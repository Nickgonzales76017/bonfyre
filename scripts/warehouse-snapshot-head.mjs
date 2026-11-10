#!/usr/bin/env bun
// Read-mostly, generation-pinned metadata within the canonical Representation Manifold.
// A head is derived metadata; it grants no source, execution, or physical-byte authority.
import {Database} from "bun:sqlite";
import {existsSync,lstatSync} from "node:fs";
import {homedir} from "node:os";
import {join,resolve} from "node:path";
import {fileURLToPath} from "node:url";

const ddl="CREATE TABLE IF NOT EXISTS warehouse_snapshot_head("
  +"semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,"
  +"generation INTEGER NOT NULL CHECK(generation>0),representation_id TEXT NOT NULL,"
  +"digest TEXT NOT NULL,frontier TEXT NOT NULL,created_at TEXT NOT NULL,changed_at TEXT NOT NULL,"
  +"PRIMARY KEY(semantic_kind,representation_kind))";
const ddlStats="CREATE TABLE IF NOT EXISTS warehouse_snapshot_stats("
  +"semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,"
  +"generation INTEGER NOT NULL,representation_id TEXT NOT NULL,digest TEXT NOT NULL,"
  +"realization_count INTEGER NOT NULL,max_realization_bytes INTEGER,frontier_rows INTEGER,"
  +"updated_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,representation_kind))";
const indexDDL=[
  "CREATE INDEX IF NOT EXISTS idx_warehouse_ri_latest ON representation_instance(schema_ref,kind,created_at DESC,id DESC)",
  "CREATE INDEX IF NOT EXISTS idx_warehouse_pr_live ON physical_realization(representation_id,available,digest)",
  "CREATE INDEX IF NOT EXISTS idx_warehouse_sb_kind ON source_binding(semantic_kind)"
];
const readSql="SELECT * FROM warehouse_snapshot_head WHERE semantic_kind=? AND representation_kind=?";
function valid(name,value){
  if(typeof value!=="string" || !/^[a-zA-Z0-9_.:/-]{1,240}$/.test(value))
    throw Error("invalid "+name);
}
function open(estate,readonly){
  const path=join(resolve(estate),"representation-manifold.db");
  if(!existsSync(path)||!lstatSync(path).isFile()||lstatSync(path).isSymbolicLink())
    throw Error("missing or unsafe manifold: "+path);
  return readonly ? new Database(path,{readonly:true,create:false}) : new Database(path);
}
function isInstalled(db){
  return !!db.query("SELECT 1 ok FROM sqlite_master WHERE type='table' AND name='warehouse_snapshot_head'").get();
}
function declaredPhysicalTarget(db,semanticKind,representationKind){
  const sources=db.query("SELECT COUNT(*) n FROM source_binding WHERE semantic_kind=?").get(semanticKind)?.n;
  const target=db.query("SELECT status FROM representation_target WHERE semantic_kind=? AND representation_kind=?")
    .get(semanticKind,representationKind);
  return sources===1 && target?.status==="physical";
}
function sourceRow(db,id,semanticKind,representationKind){
  return db.query("SELECT ri.id,ri.digest,ri.frontier,ri.created_at FROM representation_instance ri "
    +"WHERE ri.id=? AND ri.schema_ref=? AND ri.kind=? AND ri.state='physical' "
    +"AND ri.digest IS NOT NULL AND ri.digest<>'' AND ri.frontier IS NOT NULL "
    +"AND EXISTS(SELECT 1 FROM physical_realization pr WHERE pr.representation_id=ri.id "
    +"AND pr.available=1 AND pr.digest=ri.digest)").get(id,semanticKind,representationKind);
}
export function installWarehouseHead(estate){
  const db=open(estate,false);
  try{
    db.exec("BEGIN IMMEDIATE");
    try{
      const names=new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(r=>r.name));
      for(const required of ["representation_instance","physical_realization","source_binding","representation_target"])
        if(!names.has(required)) throw Error("missing manifold table: "+required);
      db.exec(ddl);
      db.exec(ddlStats);
      for(const index of indexDDL) db.exec(index);
      db.exec("COMMIT");
    }catch(e){db.exec("ROLLBACK");throw e;}
    return {state:"DERIVED_HEAD_INSTALLED",database:"representation-manifold.db",effect_authority:false};
  }finally{db.close();}
}
export function readWarehouseHead(estate,{semanticKind,representationKind}){
  valid("semanticKind",semanticKind);valid("representationKind",representationKind);
  const db=open(estate,true);
  try{
    db.exec("BEGIN");
    try{
      if(!isInstalled(db)) return {state:"NOT_INSTALLED",head:null};
      const head=db.query(readSql).get(semanticKind,representationKind);
      if(!head) return {state:"NO_HEAD",head:null};
      if(!declaredPhysicalTarget(db,semanticKind,representationKind))
        return {state:"SOURCE_OR_TARGET_AMBIGUOUS",head:null};
      const row=sourceRow(db,head.representation_id,semanticKind,representationKind);
      if(!row||row.digest!==head.digest||row.frontier!==head.frontier||row.created_at!==head.created_at)
        return {state:"STALE_HEAD_REFUSED",head:null};
      const stats=db.query("SELECT 1 ok FROM sqlite_master WHERE type=\'table\' AND name=\'warehouse_snapshot_stats\'").get() &&
        db.query("SELECT * FROM warehouse_snapshot_stats "
        +"WHERE semantic_kind=? AND representation_kind=?").get(semanticKind,representationKind);
      const currentStats=stats?.generation===head.generation &&
        stats?.representation_id===head.representation_id && stats?.digest===head.digest ? stats:null;
      return {state:"METADATA_HEAD_CURRENT",head,stats:currentStats,physical_bytes_verified:false,
        source_authority:false,execution_authority:false};
    }finally{db.exec("ROLLBACK");}
  }finally{db.close();}
}
export function bindWarehouseHead(estate,{semanticKind,representationKind,representationId,digest,expectedGeneration}){
  valid("semanticKind",semanticKind);valid("representationKind",representationKind);
  valid("representationId",representationId);
  if(typeof digest!=="string"||!/^[0-9a-f]{64}$/.test(digest)) throw Error("exact SHA256 digest required");
  if(!Number.isSafeInteger(expectedGeneration)||expectedGeneration<0)
    throw Error("nonnegative expectedGeneration required");
  const db=open(estate,false);
  try{
    db.exec("PRAGMA busy_timeout=30000");
    db.exec("BEGIN IMMEDIATE");
    try{
      if(!isInstalled(db)) throw Error("head not installed");
      if(!declaredPhysicalTarget(db,semanticKind,representationKind))
        throw Error("unique source binding and declared physical target required");
      const row=sourceRow(db,representationId,semanticKind,representationKind);
      if(!row||row.digest!==digest) throw Error("no matching live digest and representation");
      const old=db.query(readSql).get(semanticKind,representationKind);
      if(old&&old.representation_id===representationId&&old.digest===digest){
        db.exec("COMMIT");return {state:"IDEMPOTENT",head:old};
      }
      const generation=old?.generation??0;
      if(generation!==expectedGeneration) throw Error("stale head generation");
      if(old&&row.created_at<=old.created_at) throw Error("new head must advance source creation time");
      const head={semantic_kind:semanticKind,representation_kind:representationKind,
        generation:generation+1,representation_id:row.id,digest:row.digest,
        frontier:row.frontier,created_at:row.created_at,changed_at:new Date().toISOString()};
      if(old){
        const result=db.query("UPDATE warehouse_snapshot_head SET generation=?,representation_id=?,"
          +"digest=?,frontier=?,created_at=?,changed_at=? "
          +"WHERE semantic_kind=? AND representation_kind=? AND generation=? AND representation_id=?")
          .run(head.generation,head.representation_id,head.digest,head.frontier,head.created_at,
            head.changed_at,semanticKind,representationKind,generation,old.representation_id);
        if(result.changes!==1) throw Error("concurrent head conflict");
      }else{
        db.query("INSERT INTO warehouse_snapshot_head VALUES(?,?,?,?,?,?,?,?)")
          .run(semanticKind,representationKind,head.generation,row.id,row.digest,row.frontier,row.created_at,head.changed_at);
      }
      const observation=db.query("SELECT COUNT(*) n,MAX(bytes) max_bytes "
        +"FROM physical_realization WHERE representation_id=? AND available=1 AND digest=?")
        .get(representationId,digest);
      const parsed=/(?:^|;)rows:(\d+)(?:;|$)/.exec(row.frontier);
      const number=parsed?Number(parsed[1]):null;
      const frontierRows=Number.isSafeInteger(number)?number:null;
      db.query("INSERT INTO warehouse_snapshot_stats VALUES(?,?,?,?,?,?,?,?,?) "
        +"ON CONFLICT(semantic_kind,representation_kind) DO UPDATE SET "
        +"generation=excluded.generation,representation_id=excluded.representation_id,"
        +"digest=excluded.digest,realization_count=excluded.realization_count,"
        +"max_realization_bytes=excluded.max_realization_bytes,frontier_rows=excluded.frontier_rows,"
        +"updated_at=excluded.updated_at")
        .run(semanticKind,representationKind,head.generation,row.id,row.digest,
          observation.n,observation.max_bytes,frontierRows,head.changed_at);
      db.exec("COMMIT");
      return {state:"HEAD_ADVANCED",head,physical_bytes_verified:false,
        source_authority:false,execution_authority:false};
    }catch(e){db.exec("ROLLBACK");throw e;}
  }finally{db.close();}
}
export function explainWarehouseHead(estate,semanticKind,representationKind){
  valid("semanticKind",semanticKind);valid("representationKind",representationKind);
  const db=open(estate,true);
  try{
    if(!isInstalled(db)) return {state:"NOT_INSTALLED"};
    const plan=db.query("EXPLAIN QUERY PLAN "+readSql).all(semanticKind,representationKind).map(r=>r.detail);
    return {state:"EXPLAIN_ONLY",plan,indexed:plan.some(p=>/USING (?:COVERING )?INDEX/.test(p))};
  }finally{db.close();}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{
    const [command,...args]=process.argv.slice(2);
    const get=k=>{const i=args.indexOf(k);return i<0?undefined:args[i+1];};
    const estate=get("--estate")??join(homedir(),".bonfyre/estate-local");
    const kinds={semanticKind:get("--semantic-kind"),representationKind:get("--kind")};
    let result;
    if(command==="inspect") result=readWarehouseHead(estate,kinds);
    else if(command==="explain") result=explainWarehouseHead(estate,kinds.semanticKind,kinds.representationKind);
    else if(command==="install"&&args.includes("--apply")) result=installWarehouseHead(estate);
    else if(command==="bind"&&args.includes("--apply")) result=bindWarehouseHead(estate,{
      ...kinds,representationId:get("--representation-id"),digest:get("--digest"),
      expectedGeneration:Number(get("--expected-generation"))});
    else throw Error("usage: inspect|explain|install --apply|bind --apply, with --estate, "
      +"--semantic-kind, --kind, --representation-id, --digest and --expected-generation as applicable");
    console.log(JSON.stringify(result));
  }catch(e){console.error("warehouse-head: "+e.message);process.exitCode=1;}
}
