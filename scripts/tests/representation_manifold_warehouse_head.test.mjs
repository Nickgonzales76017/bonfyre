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
 db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run("/tmp/source","table","sample","parquet",null);
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
  expect(bindWarehouseHead(estate,{...kinds,representationId:"v1",digest:d("a"),expectedGeneration:0}).state).toBe("IDEMPOTENT");
  expect(bindWarehouseHead(estate,{...kinds,representationId:"v2",digest:d("b"),expectedGeneration:1}).head.generation).toBe(2);
  expect(readWarehouseHead(estate,kinds).head.representation_id).toBe("v2");
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
