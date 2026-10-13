import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,existsSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {reviewPhysicalReceipts} from '../performance_physical_receipts.mjs';

const sha=c=>'sha256:'+c.repeat(64);
const keys=['bytes_read','bytes_borrowed','bytes_copied','bytes_decompressed',
  'bytes_reencoded','bytes_hydrated','allocations','serialize_count',
  'deserialize_count','materialization_count','borrowed_boundaries','fused_boundaries','useful_bytes'];
function paired(){
 const coordinate={subject:'public:one',owner:'BonfyreCompress',operation:'physical-compile',
  scope:'public',work_class:'discovery',source_digest:sha('a'),
  environment_digest:sha('b'),cohort:'same-requests',axis:'learned-embedding',k:5};
 const common={subject:coordinate.subject,owner:coordinate.owner,scope:coordinate.scope,
  source_digest:coordinate.source_digest,environment_digest:coordinate.environment_digest,
  result_digest:sha('c'),effect_digest:sha('d'),features:['intake','validation','output']};
 return {schema:'bonfyre.performance.equivalence.v1',coordinate,
  rollback:{baseline_route:'BonfyreCompress/exact',receipt_ref:'owner:rollback'},
  baseline:{...common,kind:'exact',receipt_ref:'owner:baseline',physical_receipt_id:'physical-before',
   physical_passport:'passport:before',metrics:{p95_ms:18,peak_resident_bytes:50000,
    bytes_copied:10000,bytes_hydrated:4000,allocations:100,external_calls:0}},
  candidate:{...common,kind:'exact',receipt_ref:'owner:candidate',physical_receipt_id:'physical-after',
   physical_passport:'passport:after',metrics:{p95_ms:12,peak_resident_bytes:42000,
    bytes_copied:2000,bytes_hydrated:1000,allocations:40,external_calls:0}}};
}
function fixture(){
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-native-copy-')),path=join(dir,'physical.sqlite');
 const db=new DatabaseSync(path);
 db.exec('CREATE TABLE representation_passport(handle INTEGER PRIMARY KEY,'
  +'identity TEXT,subject_family TEXT,semantic_digest TEXT,exactness_class TEXT,measurement_state TEXT);'
  +'CREATE TABLE compression_receipt(receipt_id TEXT PRIMARY KEY,passport_handle INTEGER NOT NULL,'
  +keys.map(k=>k+' INTEGER NOT NULL').join(',')+');');
 const pass=db.prepare('INSERT INTO representation_passport VALUES (?,?,?,?,?,?)');
 pass.run(1,'passport:before','occurrence','semantic:same','exact','measured');
 pass.run(2,'passport:after','occurrence','semantic:same','exact','measured');
 const insert=db.prepare('INSERT INTO compression_receipt VALUES (?, ?, '+keys.map(()=>'?').join(',')+')');
 const p=paired();
 for(const [i,side] of ['baseline','candidate'].entries()){
  const m=p[side].metrics;
  const values={bytes_read:10000,bytes_borrowed:i?8000:0,bytes_copied:m.bytes_copied,
   bytes_decompressed:0,bytes_reencoded:0,bytes_hydrated:m.bytes_hydrated,allocations:m.allocations,
   serialize_count:0,deserialize_count:0,materialization_count:0,
   borrowed_boundaries:i?1:0,fused_boundaries:i?1:0,useful_bytes:10000};
  insert.run(i?'physical-after':'physical-before',i+1,...keys.map(k=>values[k]));
 }
 db.close();return {path,dir,close:()=>rmSync(dir,{recursive:true,force:true})};
}
const hold=(p,path,re)=>{
 const result=reviewPhysicalReceipts(p,path);
 assert.equal(result.decision,'hold-baseline');assert.match(result.problems.join(' '),re);
 assert.equal(result.no_effect,true);
};
test('readback admits review, never deployment; physical counts are actual rows',()=>{
 const db=fixture();try{
  const x=reviewPhysicalReceipts(paired(),db.path);
  assert.equal(x.decision,'eligible-for-human-promotion-review');
  assert.equal(x.admitted_for_execution,false);
  assert.equal(x.physical.baseline.counters.bytes_copied,10000);
  assert.equal(x.physical.candidate.counters.bytes_borrowed,8000);
  assert.equal(x.physical.candidate.counters.useful_bytes,10000);
  assert.equal(x.financial.priced,false);
 }finally{db.close();}
});
test('missing path refuses without creating a phantom DB',()=>{
 const db=fixture();try{const p=join(db.dir,'absent.sqlite');hold(paired(),p,/native-physical-registry-unavailable/);
  assert.equal(existsSync(p),false);}finally{db.close();}
});
test('both physical receipts must actually exist',()=>{
 const db=fixture();try{const p=paired();p.candidate.physical_receipt_id='absent';
  hold(p,db.path,/candidate:native-physical-receipt-not-found/);}finally{db.close();}
});
test('physical baseline and candidate cannot reuse the same receipt',()=>{
 const db=fixture();try{const p=paired();p.candidate.physical_receipt_id=p.baseline.physical_receipt_id;
  hold(p,db.path,/distinct-native-physical-receipt-ids-required/);}finally{db.close();}
});
test('caller cannot underreport native copy/hydration/allocation cost',()=>{
 const db=fixture();try{for(const key of ['bytes_copied','bytes_hydrated','allocations']){
  const p=paired();p.candidate.metrics[key]--;
  hold(p,db.path,new RegExp('candidate:claimed-'+key+'-differs-from-native-receipt'));
 }}finally{db.close();}
});
test('different semantic bodies are not interchangeable',()=>{
 const db=fixture();try{const x=new DatabaseSync(db.path);
  x.prepare('UPDATE representation_passport SET semantic_digest=? WHERE handle=2').run('other');x.close();
  hold(paired(),db.path,/physical-passports-differ-in-semantic-family-or-digest/);
 }finally{db.close();}
});
test('truncated or empty useful work is not a saving',()=>{
 const db=fixture();try{const x=new DatabaseSync(db.path);
  x.prepare('UPDATE compression_receipt SET useful_bytes=20 WHERE receipt_id=?').run('physical-after');x.close();
  hold(paired(),db.path,/unequal-or-empty-useful-work/);
 }finally{db.close();}
});
test('baseline physical material remains exact',()=>{
 const db=fixture();try{const x=new DatabaseSync(db.path);
  x.prepare("UPDATE representation_passport SET exactness_class='latent' WHERE handle=1").run();x.close();
  hold(paired(),db.path,/baseline-physical-representation-not-exact/);
 }finally{db.close();}
});
test('latent physical candidate needs its own approximation gate',()=>{
 const db=fixture();try{const x=new DatabaseSync(db.path);
  x.prepare("UPDATE representation_passport SET exactness_class='latent' WHERE handle=2").run();x.close();
  hold(paired(),db.path,/latent-representation-without-approximation-gate/);
 }finally{db.close();}
});
test('existing output and tail-latency requirements still apply',()=>{
 const db=fixture();try{const p=paired();p.candidate.result_digest=sha('e');
  hold(p,db.path,/final-result-differs/);
  const q=paired();q.candidate.metrics.p95_ms=100;hold(q,db.path,/p95_ms-regressed/);
 }finally{db.close();}
});
test('readback leaves existing native receipt rows unchanged',()=>{
 const db=fixture();try{reviewPhysicalReceipts(paired(),db.path);
  const x=new DatabaseSync(db.path,{readOnly:true});
  assert.equal(x.prepare('SELECT COUNT(*) AS n FROM compression_receipt').get().n,2);
  assert.equal(x.prepare('SELECT COUNT(*) AS n FROM representation_passport').get().n,2);x.close();
 }finally{db.close();}
});
