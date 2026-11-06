// Antithesis/SQLancer-inspired local differential tests against the real
// Representation Manifold schema. Never opens live estate or physical parquet.
import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {installWarehouseHead,bindWarehouseHead,readWarehouseHead,explainWarehouseHead}
 from '../warehouse-snapshot-head.mjs';
const digest=x=>x.repeat(64);
const kinds=Array.from({length:16},(_,i)=>'test-kind-'+i);
function fixture(){
 const dir=mkdtempSync(join(tmpdir(),'antithesis-warehouse-sql-'));
 const db=new Database(join(dir,'representation-manifold.db'));
 db.exec(readFileSync(join(import.meta.dir,'../../schemas/storage/representation-manifold.sql'),'utf8'));
 const bind=db.query('INSERT INTO source_binding VALUES(?,?,?,?,?)');
 const target=db.query('INSERT INTO representation_target VALUES(?,?,?,?,?)');
 const instance=db.query('INSERT INTO representation_instance '+
  '(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) '+
  'VALUES(?,?,?,?,?,?,?,?,?)');
 const physical=db.query('INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)');
 for(let i=0;i<kinds.length;i++){
  const kind=kinds[i];
  bind.run('/tmp/fixture-'+i,'table',kind,'sqlite',null);
  target.run(kind,'parquet','physical',null,'test target');
  for(let gen=0;gen<2;gen++){
   const id=kind+'-v'+gen,hash=digest(gen===0?'a':'b'),when='2026-11-0'+(gen+1)+'T00:00:00Z';
   instance.run(id,'parquet','derived','rows:'+(10+i+gen),hash,kind,when,1,'physical');
   physical.run('realization-'+id,id,'file','/never-opened/'+id,100+i+gen,hash,'warm',1,when);
  }
 }
 return {dir,db};
}
test('16 native Warehouse CAS schedules, rollback and physical realization failures agree with oracle',()=>{
 const {dir,db}=fixture();
 try{
  expect(installWarehouseHead(dir).state).toBe('DERIVED_HEAD_INSTALLED');
  const get=kind=>readWarehouseHead(dir,{semanticKind:kind,representationKind:'parquet'});
  for(let i=0;i<kinds.length;i++){
   const kind=kinds[i],v0=kind+'-v0',v1=kind+'-v1';
   const bind=(id,sha,gen)=>bindWarehouseHead(dir,{
    semanticKind:kind,representationKind:'parquet',representationId:id,digest:sha,expectedGeneration:gen});
   expect(get(kind).state).toBe('NO_HEAD');
   expect(bind(v0,digest('a'),0).head.generation).toBe(1);
   expect(bind(v0,digest('a'),0).state).toBe('IDEMPOTENT');
   expect(()=>bind(v1,digest('f'),1)).toThrow(/digest/);
   expect(()=>bind(v1,digest('b'),0)).toThrow(/stale/);
   expect(get(kind).head.representation_id).toBe(v0);
   expect(bind(v1,digest('b'),1).head.generation).toBe(2);
   expect(get(kind)).toMatchObject({state:'METADATA_HEAD_CURRENT',head:{
    generation:2,representation_id:v1},stats:{
    generation:2,representation_id:v1,realization_count:1,max_realization_bytes:101+i}});
   db.query('UPDATE physical_realization SET available=0 WHERE representation_id=?').run(v1);
   expect(get(kind).state).toBe('STALE_HEAD_REFUSED');
   db.exec('BEGIN');
   db.query('UPDATE physical_realization SET available=1 WHERE representation_id=?').run(v1);
   db.exec('ROLLBACK');
   expect(get(kind).state).toBe('STALE_HEAD_REFUSED');
   db.query('UPDATE physical_realization SET available=1 WHERE representation_id=?').run(v1);
   expect(get(kind).head.representation_id).toBe(v1);
  }
  expect(db.query('SELECT COUNT(*) n FROM warehouse_snapshot_head').get().n).toBe(kinds.length);
  expect(db.query('SELECT COUNT(*) n FROM warehouse_snapshot_stats').get().n).toBe(kinds.length);
  expect(db.query('PRAGMA quick_check').get().quick_check).toBe('ok');
 }finally{db.close();rmSync(dir,{recursive:true,force:true});}
});
test('SQL differential: indexed latest physical representation equals JS oracle for 16 source kinds',()=>{
 const {dir,db}=fixture();
 try{
  installWarehouseHead(dir);
  for(const kind of kinds){
   const rows=db.query("SELECT id,created_at FROM representation_instance "+
    "WHERE schema_ref=? AND kind='parquet' AND state='physical'").all(kind);
   const oracle=rows.sort((a,b)=>b.created_at.localeCompare(a.created_at)||b.id.localeCompare(a.id))[0];
   const query=db.query("SELECT id,created_at FROM representation_instance "+
    "WHERE schema_ref=? AND kind='parquet' AND state='physical' "+
    "ORDER BY created_at DESC,id DESC LIMIT 1").get(kind);
   expect(query).toEqual(oracle);
   expect(explainWarehouseHead(dir,kind,'parquet').indexed).toBe(true);
  }
  db.query('INSERT INTO source_binding VALUES(?,?,?,?,?)').run('/tmp/collision','table',kinds[0],'sqlite',null);
  expect(()=>bindWarehouseHead(dir,{semanticKind:kinds[0],representationKind:'parquet',
    representationId:kinds[0]+'-v1',digest:digest('b'),expectedGeneration:0}))
    .toThrow(/unique source binding/);
  expect(readWarehouseHead(dir,{semanticKind:kinds[1],representationKind:'parquet'}).state).toBe('NO_HEAD');
 }finally{db.close();rmSync(dir,{recursive:true,force:true});}
});
