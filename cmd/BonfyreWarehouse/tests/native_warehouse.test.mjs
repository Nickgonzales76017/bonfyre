import {test,expect} from "bun:test";
import {Database} from "bun:sqlite";
import {mkdtempSync,readFileSync,rmSync,symlinkSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {installWarehouseHead,bindWarehouseHead}
  from "../../../scripts/warehouse-snapshot-head.mjs";

const schema=readFileSync(join(import.meta.dir,"../../../schemas/storage/representation-manifold.sql"),"utf8");
const binary=join(import.meta.dir,"../bonfyre-warehouse");
const hex=c=>c.repeat(64);
function create() {
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-native-warehouse-"));
  const db=new Database(join(estate,"representation-manifold.db"));
  db.exec(schema);
  db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run(
    "/tmp/s","events","example","parquet",null);
  db.query("INSERT INTO representation_instance "
    +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
    +"VALUES(?,?,?,?,?,?,?,?,?)").run(
      "current","parquet","derived","f1",hex("a"),"example","2026-11-05T00:00:00Z",1,"physical");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("body","current","file","/never-open",44,hex("a"),"warm",1,"2026-11-05");
  db.close();
  return estate;
}
function run(...args) {
  const p=Bun.spawnSync([binary,...args],{stdout:"pipe",stderr:"pipe"});
  const stdout=new TextDecoder().decode(p.stdout);
  return {code:p.exitCode,stdout,stderr:new TextDecoder().decode(p.stderr),
    data:p.exitCode===0?JSON.parse(stdout):null};
}
test("native status reads original manifold without installing a head",()=>{
  const estate=create();
  try{
    const r=run("status","--estate",estate);
    expect(r.code).toBe(0);
    expect(r.data).toMatchObject({state:"READ_ONLY",warehouse_heads_installed:false,
      representation_instances:1,source_bindings:1,execution_authority:false});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native head follows Bun's canonical head writer and rejects invalidation",()=>{
  const estate=create();
  try{
    const flags=["--estate",estate,"--semantic-kind","example","--kind","parquet"];
    expect(run("head",...flags).data.state).toBe("NOT_INSTALLED");
    installWarehouseHead(estate);
    expect(run("head",...flags).data.state).toBe("NO_HEAD");
    bindWarehouseHead(estate,{semanticKind:"example",representationKind:"parquet",
      representationId:"current",digest:hex("a"),expectedGeneration:0});
    const read=run("head",...flags);
    expect(read.code).toBe(0);
    expect(read.data).toMatchObject({state:"METADATA_HEAD_CURRENT",generation:1,
      representation_id:"current",digest:hex("a"),physical_bytes_verified:false});
    const plan=run("plan",...flags);
    expect(plan.data.state).toBe("QUERY_PLAN");
    expect(plan.data.plan).toContain("INDEX");
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    db.close();
    expect(run("head",...flags).data).toMatchObject({state:"STALE_HEAD_REFUSED",head:null});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native reader refuses missing, symlinked and malformed paths",()=>{
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-native-symlink-"));
  try{
    expect(run("status","--estate",estate).code).not.toBe(0);
    expect(run("head","--estate",estate,"--semantic-kind","';DROP","--kind","parquet").code).not.toBe(0);
    const target=join(estate,"target.db");
    const db=new Database(target);db.exec(schema);db.close();
    symlinkSync(target,join(estate,"representation-manifold.db"));
    expect(run("status","--estate",estate).code).not.toBe(0);
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("integrity classifies orphan metadata without deleting or rewriting anything",()=>{
  const estate=create();
  try{
    installWarehouseHead(estate);
    bindWarehouseHead(estate,{semanticKind:"example",representationKind:"parquet",
      representationId:"current",digest:hex("a"),expectedGeneration:0});
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("orphan","missing","file","/never-open-2",10,hex("b"),"warm",1,"2026-11-05");
    db.query("INSERT INTO representation_edge VALUES(?,?,?,?,?)")
      .run("missing","current","derived","materialized","2026-11-05");
    db.query("INSERT INTO eviction_receipt VALUES(?,?,?,?,?,?,?,?,?)")
      .run("er-missing","missing","parquet","/never-open-2",hex("b"),10,"obsolete","/nothing","2026-11-05");
    db.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    db.close();
    const report=run("integrity","--estate",estate);
    expect(report.code).toBe(0);
    expect(report.data).toMatchObject({state:"METADATA_INTEGRITY_OBSERVED",
      dangling_realizations:1,dangling_edges:1,dangling_eviction_receipts:1,
      stale_heads:1,deleted:0,safe_to_delete_inferred:false});
    const verify=new Database(join(estate,"representation-manifold.db"),{readonly:true});
    expect(verify.query("SELECT COUNT(*) n FROM eviction_receipt").get().n).toBe(1);
    expect(verify.query("SELECT COUNT(*) n FROM physical_realization").get().n).toBe(2);
    verify.close();
  }finally{rmSync(estate,{recursive:true,force:true});}
});
