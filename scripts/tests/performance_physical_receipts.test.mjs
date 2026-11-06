import {test} from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,existsSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {reviewPhysicalReceipts, reviewPhysicalReadbacks} from '../performance_physical_receipts.mjs';

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

function historicFixture(){
  const f=fixture();
  const db=new DatabaseSync(f.path);
  db.exec('CREATE TABLE physical_generation(name TEXT PRIMARY KEY,source_digest TEXT NOT NULL);');
  const former=sha('1').slice(7),present=sha('2').slice(7);
  db.prepare('INSERT INTO physical_generation(name,source_digest) VALUES (?,?)').run('passport',present);
  db.exec('CREATE TABLE compression_receipt_history(archive_row INTEGER PRIMARY KEY,'
    +'generation_digest TEXT NOT NULL,receipt_id TEXT NOT NULL,passport_identity TEXT NOT NULL,'
    +'subject_family TEXT NOT NULL,semantic_digest TEXT NOT NULL,physical_digest TEXT NOT NULL,'
    +'exactness_class TEXT NOT NULL,'+keys.map(k=>k+' INTEGER NOT NULL').join(',')+');');
  const col='generation_digest,receipt_id,passport_identity,subject_family,semantic_digest,physical_digest,exactness_class,'+keys.join(',');
  const select='SELECT ?,r.receipt_id,p.identity,p.subject_family,p.semantic_digest,?,p.exactness_class,'
    +keys.map(k=>'r.'+k).join(',')+' FROM compression_receipt r '
    +'JOIN representation_passport p ON p.handle=r.passport_handle WHERE r.receipt_id=?';
  db.prepare('INSERT INTO compression_receipt_history('+col+') '+select).run(former,'physical:before','physical-before');
  db.prepare("DELETE FROM compression_receipt WHERE receipt_id='physical-before'").run();
  db.close();
  return {...f,former,present};
}
test('historic baseline and current candidate can be compared with generation binding',()=>{
  const f=historicFixture();try{
    const p=paired();
    p.baseline.physical_source='archive';
    p.baseline.physical_generation_digest=f.former;
    p.candidate.physical_generation_digest=f.present;
    const v=reviewPhysicalReceipts(p,f.path);
    assert.equal(v.decision,'eligible-for-human-promotion-review',v.problems.join(','));
    assert.equal(v.physical.baseline.source,'archive');
    assert.equal(v.physical.baseline.generation_digest,f.former);
    assert.equal(v.physical.candidate.generation_digest,f.present);
    assert.equal(v.physical.measured_copy_delta.copied.saved_units,8000);
    assert.equal(v.admitted_for_execution,false);
  }finally{f.close();}
});
test('archived measurements require digest AND passport, no implicit historical fallback',()=>{
  const f=historicFixture();try{
    const p=paired();p.baseline.physical_source='archive';
    hold(p,f.path,/historical-generation-and-passport-required/);
    p.baseline.physical_generation_digest=f.former;
    assert.equal(reviewPhysicalReceipts(p,f.path).decision,'eligible-for-human-promotion-review');
    p.baseline.physical_generation_digest=sha('3').slice(7);
    hold(p,f.path,/native-physical-receipt-not-found/);
  }finally{f.close();}
});
test('wrong active generation and stale historical identity both refuse',()=>{
  const f=historicFixture();try{
    const p=paired();p.baseline.physical_source='archive';
    p.baseline.physical_generation_digest=f.former;
    p.candidate.physical_generation_digest=f.former;
    hold(p,f.path,/physical-generation-drift/);
    p.candidate.physical_generation_digest=f.present;
    p.baseline.physical_passport='claimed-other-passport';
    hold(p,f.path,/physical-passport-mismatch/);
  }finally{f.close();}
});
test('reused receipt IDs require separate generation identity and preserve both records',()=>{
  const f=historicFixture();try{
    const db=new DatabaseSync(f.path);
    db.prepare("UPDATE compression_receipt SET receipt_id='physical-before' WHERE receipt_id='physical-after'").run();
    db.close();
    const p=paired();
    p.baseline.physical_source='archive';
    p.baseline.physical_generation_digest=f.former;
    p.candidate.physical_generation_digest=f.present;
    p.candidate.physical_receipt_id='physical-before';
    assert.equal(reviewPhysicalReceipts(p,f.path).decision,'eligible-for-human-promotion-review');
    delete p.baseline.physical_generation_digest;
    hold(p,f.path,/historical-generation-and-passport-required/);
  }finally{f.close();}
});
test('ambiguous history for the same generation and receipt refuses silently selecting one',()=>{
  const f=historicFixture();try{
    const db=new DatabaseSync(f.path);
    db.exec('INSERT INTO compression_receipt_history(generation_digest,receipt_id,passport_identity,'
     +'subject_family,semantic_digest,physical_digest,exactness_class,'+keys.join(',')+') '
     +'SELECT generation_digest,receipt_id,passport_identity,subject_family,semantic_digest,'
     +'physical_digest,exactness_class,'+keys.join(',')+' FROM compression_receipt_history');
    db.close();
    const p=paired();p.baseline.physical_source='archive';p.baseline.physical_generation_digest=f.former;
    hold(p,f.path,/ambiguous-historical-receipt-generation/);
  }finally{f.close();}
});
test('a legacy active snapshot can still pass, but cannot forge a generation label',()=>{
  const f=fixture();try{
    assert.equal(reviewPhysicalReceipts(paired(),f.path).decision,'eligible-for-human-promotion-review');
    const p=paired();p.baseline.physical_generation_digest=sha('1').slice(7);
    hold(p,f.path,/physical-generation-drift/);
  }finally{f.close();}
});
test('history lookup neither creates a store nor writes or deletes archived rows',()=>{
  const f=historicFixture();try{
    const p=paired();p.baseline.physical_source='archive';p.baseline.physical_generation_digest=f.former;
    const x=new DatabaseSync(f.path,{readOnly:true});
    const before=x.prepare('SELECT COUNT(*) AS n FROM compression_receipt_history').get().n;
    x.close();
    reviewPhysicalReceipts(p,f.path);
    const y=new DatabaseSync(f.path,{readOnly:true});
    assert.equal(y.prepare('SELECT COUNT(*) AS n FROM compression_receipt_history').get().n,before);
    y.close();
  }finally{f.close();}
});
test('historical CopyReceipts must match the semantic family and useful-work count',()=>{
  const f=historicFixture();try{
    const p=paired();p.baseline.physical_source='archive';p.baseline.physical_generation_digest=f.former;
    const db=new DatabaseSync(f.path);
    db.prepare("UPDATE compression_receipt_history SET useful_bytes=500 WHERE receipt_id='physical-before'").run();
    db.close();
    hold(p,f.path,/unequal-or-empty-useful-work/);
  }finally{f.close();}
});

function nativeReadbacks(){
 const p=paired();
 const former=sha('1').slice(7),present=sha('2').slice(7);
 p.baseline.physical_source='archive';
 p.baseline.physical_generation_digest=former;
 p.candidate.physical_generation_digest=present;
 const mk=(side,origin,generation,passport)=>{
   const values={bytes_read:10000,bytes_borrowed:side==='candidate'?8000:0,
     bytes_copied:p[side].metrics.bytes_copied,bytes_decompressed:0,
     bytes_reencoded:0,bytes_hydrated:p[side].metrics.bytes_hydrated,
     allocations:p[side].metrics.allocations,serialize_count:0,
     deserialize_count:0,materialization_count:0,borrowed_boundaries:0,
     fused_boundaries:0,useful_bytes:10000};
   return {schema:'bonfyre.copyreceipt.readback.v1',origin,generation_digest:generation,
     receipt_id:p[side].physical_receipt_id,passport_identity:passport,
     subject_family:'occurrence',semantic_digest:'semantic:same',
     physical_digest:'physical:same',exactness_class:'exact',
     counters:values,no_effect:true};
 };
 return {p,readbacks:{
   baseline:mk('baseline','archived',former,'passport:before'),
   candidate:mk('candidate','active',present,'passport:after')}};
}
test('portable C readback pair reviews historical vs active without permitting execution',()=>{
 const {p,readbacks}=nativeReadbacks();
 const r=reviewPhysicalReadbacks(p,readbacks);
 assert.equal(r.decision,'eligible-for-human-promotion-review',r.problems.join(','));
 assert.equal(r.admitted_for_execution,false);
 assert.equal(r.physical.measured_copy_delta.copied.saved_units,8000);
 assert.equal(r.physical.baseline.generation_digest,p.baseline.physical_generation_digest);
});
test('portable readback requires exact generation, passport, and ID coordinates',()=>{
 for(const field of ['physical_generation_digest','physical_passport','physical_receipt_id']){
  const {p,readbacks}=nativeReadbacks();delete p.baseline[field];
  const r=reviewPhysicalReadbacks(p,readbacks);
  assert.equal(r.decision,'hold-baseline',field);
  assert.match(r.problems.join(' '),/not-bound/);
 }
});
test('portable C readbacks preserve no effect and cannot forge exact representation',()=>{
 const {p,readbacks}=nativeReadbacks();
 readbacks.baseline.no_effect=false;
 let r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/unrecognized-native-readback/);
 readbacks.baseline.no_effect=true;
 readbacks.baseline.exactness_class='latent';
 r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/baseline-native-representation-not-exact/);
});
test('portable readbacks refuse native counter drift and unequal useful work',()=>{
 const {p,readbacks}=nativeReadbacks();
 readbacks.candidate.counters.bytes_copied=0;
 let r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/claimed-bytes_copied-differs-from-native-readback/);
 readbacks.candidate.counters.bytes_copied=2000;
 readbacks.candidate.counters.useful_bytes=9999;
 r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/unequal-or-empty-native-useful-work/);
});
test('portable snapshots refuse changed semantic material and reused observation',()=>{
 const {p,readbacks}=nativeReadbacks();
 readbacks.candidate.semantic_digest='other';
 let r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/differ-in-semantic-material/);
 readbacks.candidate.semantic_digest='semantic:same';
 readbacks.candidate.origin=readbacks.baseline.origin;
 readbacks.candidate.generation_digest=readbacks.baseline.generation_digest;
 readbacks.candidate.receipt_id=readbacks.baseline.receipt_id;
 r=reviewPhysicalReadbacks(p,readbacks);
 assert.match(r.problems.join(' '),/not-two-distinct-native-observations/);
});
