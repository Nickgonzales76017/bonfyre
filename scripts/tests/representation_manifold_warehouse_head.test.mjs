import {test,expect} from "bun:test";
import {Database} from "bun:sqlite";
import {mkdtempSync,readFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {installWarehouseHead,readWarehouseHead,bindWarehouseHead,explainWarehouseHead}
  from "../warehouse-snapshot-head.mjs";
const schema=readFileSync(join(import.meta.dir,"../../schemas/storage/representation-manifold.sql"),"utf8");
const d=c=>c.repeat(64), kinds={semanticKind:"sample",representationKind:"parquet"};
function fixture(){
 const estate=mkdtempSync(join(tmpdir(),"bonfyre-wh-head-"));
 const db=new Database(join(estate,"representation-manifold.db"));db.exec(schema);
 db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run("/tmp/source","table","sample","sqlite",null);
 db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
   .run("sample","parquet","physical",null,"derived target");
 const ri=db.query("INSERT INTO representation_instance "
   +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
   +"VALUES(?,?,?,?,?,?,?,?,?)");
 const pr=db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)");
 for(let i=0;i<2000;i++) ri.run("old-"+i,"parquet","derived","f"+i,d("e"),
   "other","2026-11-01T00:00:00Z",1,"reconstructable");
 for(const [id,stamp,digest] of [
   ["v0","2026-10-31T00:00:00Z",d("c")],
   ["v1","2026-11-01T10:00:00Z",d("a")],
   ["v2","2026-11-02T10:00:00Z",d("b")]
 ]){
   ri.run(id,"parquet","derived","frontier-"+id,digest,"sample",stamp,1,"physical");
   pr.run("physical-"+id,id,"file","/never-opened/"+id,100,digest,"warm",1,stamp);
 }
 db.close();return estate;
}
const remove=estate=>rmSync(estate,{recursive:true,force:true});
test("read-only lookup uses one compound key despite 2000 unrelated generations",()=>{
 const estate=fixture();
 try{
  expect(readWarehouseHead(estate,kinds).state).toBe("NOT_INSTALLED");
  expect(installWarehouseHead(estate).state).toBe("DERIVED_HEAD_INSTALLED");
  expect(explainWarehouseHead(estate,kinds.semanticKind,kinds.representationKind).indexed).toBe(true);
  expect(bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0}).head.generation).toBe(1);
  expect(readWarehouseHead(estate,kinds).head.representation_id).toBe("v1");
  expect(readWarehouseHead(estate,kinds).stats).toMatchObject({
    generation:1,representation_id:"v1",realization_count:1,max_realization_bytes:100});
  expect(bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0}).state).toBe("IDEMPOTENT");
  expect(bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("b"),expectedGeneration:1}).head.generation).toBe(2);
  expect(readWarehouseHead(estate,kinds).head.representation_id).toBe("v2");
  expect(readWarehouseHead(estate,kinds).stats).toMatchObject({
    generation:2,representation_id:"v2",realization_count:1,max_realization_bytes:100});
  const indexes=new Database(join(estate,"representation-manifold.db"),{readonly:true});
  const plan=indexes.query("EXPLAIN QUERY PLAN SELECT id FROM representation_instance "
    +"WHERE schema_ref=? AND kind=? ORDER BY created_at DESC,id DESC LIMIT 1").all("sample","parquet");
  expect(plan.some(x=>x.detail.includes("idx_warehouse_ri_latest"))).toBe(true);
  indexes.close();
  const db=new Database(join(estate,"representation-manifold.db"),{readonly:true});
  expect(db.query("SELECT COUNT(*) n FROM representation_instance").get().n).toBe(2003);
  db.close();
 }finally{remove(estate);}
});
test("stale compare-and-swap, rewind and wrong digest never change the head",()=>{
 const estate=fixture();
 try{
  installWarehouseHead(estate);
  bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0});
  expect(()=>bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("b"),expectedGeneration:0})).toThrow("stale");
  expect(()=>bindWarehouseHead(estate,{...kinds,representationId:"v0",digest:d("c"),expectedGeneration:1})).toThrow("advance");
  expect(()=>bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("f"),expectedGeneration:1})).toThrow("digest");
  expect(readWarehouseHead(estate,kinds).head.generation).toBe(1);
 }finally{remove(estate);}
});
test("evicted physical realization invalidates the current head without silent fallback",()=>{
 const estate=fixture();
 try{
  installWarehouseHead(estate);
  bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0});
  const db=new Database(join(estate,"representation-manifold.db"));
  db.query("UPDATE physical_realization SET available=0 WHERE representation_id='v1'").run();db.close();
  expect(readWarehouseHead(estate,kinds)).toMatchObject({state:"STALE_HEAD_REFUSED",head:null});
 }finally{remove(estate);}
});
test("missing source binding or missing physical representation refuses authority",()=>{
 const estate=fixture();
 try{
  installWarehouseHead(estate);
  expect(()=>bindWarehouseHead(estate,{semanticKind:"other",representationKind:"parquet",
    representationId:"old-1",digest:d("e"),expectedGeneration:0})).toThrow("source binding");
  expect(()=>bindWarehouseHead(estate,{...kinds,
    representationId:"missing",digest:d("a"),expectedGeneration:0})).toThrow("no matching");
  expect(readWarehouseHead(estate,kinds).state).toBe("NO_HEAD");
 }finally{remove(estate);}
});
test("source identity collisions and undeclared physical target refuse heads",()=>{
 const estate=fixture();
 try{
  installWarehouseHead(estate);
  bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0});
  const db=new Database(join(estate,"representation-manifold.db"));
  db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)")
    .run("/tmp/other","other","sample","sqlite",null);
  db.close();
  expect(readWarehouseHead(estate,kinds).state).toBe("SOURCE_OR_TARGET_AMBIGUOUS");
  expect(()=>bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("b"),expectedGeneration:1}))
    .toThrow("unique source binding");
  const dbase=new Database(join(estate,"representation-manifold.db"));
  dbase.query("DELETE FROM source_binding WHERE source_db='/tmp/other'").run();
  dbase.query("UPDATE representation_target SET status='declared_target' WHERE semantic_kind='sample' AND representation_kind='parquet'").run();
  dbase.close();
  expect(readWarehouseHead(estate,kinds).state).toBe("SOURCE_OR_TARGET_AMBIGUOUS");
  expect(()=>bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("b"),expectedGeneration:1}))
    .toThrow("declared physical target");
 }finally{remove(estate);}
});
test("incremental stats parse frontier rows only from pinned physical metadata",()=>{
 const estate=fixture();
 try{
  const db=new Database(join(estate,"representation-manifold.db"));
  db.query("UPDATE representation_instance SET frontier=? WHERE id='v1'")
    .run("sqlite-table-sha256:source-digest;rows:17");
  db.close();
  installWarehouseHead(estate);
  bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0});
  const r=readWarehouseHead(estate,kinds);
  expect(r.stats).toMatchObject({generation:1,realization_count:1,frontier_rows:17});
  expect(r.physical_bytes_verified).toBe(false);
 }finally{remove(estate);}
});
