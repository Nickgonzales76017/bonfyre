#!/usr/bin/env node
/** Generation-aware, read-only CopyReceipt readback.
 * This compares archived and active native observations without treating a
 * previous passport generation as current. It does not authenticate callers
 * or authorize any execution, provider request, resource grant or payment.
 */
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { reviewPerformance } from './performance_equivalence.mjs';

const isName = x => typeof x === 'string' && x.length > 0 && x.length <= 512 && !x.includes('\0');
const isDigest = x => typeof x === 'string' && /^[a-f0-9]{64}$/.test(x);
const counters = ['bytes_read','bytes_borrowed','bytes_copied','bytes_decompressed',
  'bytes_reencoded','bytes_hydrated','allocations','serialize_count',
  'deserialize_count','materialization_count','borrowed_boundaries','fused_boundaries','useful_bytes'];
const directMetrics = ['bytes_copied','bytes_hydrated','allocations'];
const activeSql = 'SELECT r.receipt_id,p.identity AS passport,p.subject_family,p.semantic_digest,'
  + 'p.exactness_class,p.measurement_state,'
  + counters.map(k => 'r.' + k).join(',')
  + ' FROM compression_receipt AS r'
  + ' JOIN representation_passport AS p ON p.handle=r.passport_handle'
  + ' WHERE r.receipt_id=?';
const historySql = 'SELECT receipt_id,passport_identity AS passport,subject_family,'
  + 'semantic_digest,exactness_class,generation_digest,'
  + counters.join(',') + ' FROM compression_receipt_history'
  + ' WHERE generation_digest=? AND receipt_id=? LIMIT 2';
const tableExists = (db,name) => !!db.prepare(
  "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
const relative = (a,b,denom) => denom>0 && Number.isSafeInteger(a) && Number.isSafeInteger(b)
  ? {baseline_per_useful_byte:a/denom,candidate_per_useful_byte:b/denom,
     saved_units:a-b} : null;

export function reviewPhysicalReceipts(record, dbPath) {
  const reviewed = reviewPerformance(record);
  const problems = [...(reviewed.problems ?? reviewed.errors ?? [])];
  const sides = ['baseline','candidate'];
  const choices = sides.map(side => {
    const claim=record?.[side]||{};
    return {side,id:claim.physical_receipt_id,origin:claim.physical_source??'active',
      generation:claim.physical_generation_digest,passport:claim.physical_passport};
  });
  const key = x => [x.origin,x.generation??'',x.id??''].join('|');
  if(choices.some(x=>!isName(x.id)||!['active','archive'].includes(x.origin)) ||
     key(choices[0])===key(choices[1]))
    problems.push('distinct-native-physical-receipt-ids-required');
  if (!isName(dbPath)) problems.push('physical-registry-path-required');
  for(const x of choices){
    if(x.origin==='archive' && (!isDigest(x.generation)||!isName(x.passport)))
      problems.push(x.side+':historical-generation-and-passport-required');
    if(x.generation!==undefined && !isDigest(x.generation))
      problems.push(x.side+':physical-generation-digest-invalid');
  }
  let db,rows,opened=false;
  if(!problems.includes('physical-registry-path-required') &&
     !problems.includes('distinct-native-physical-receipt-ids-required')) {
    try {
      db = new DatabaseSync(dbPath,{readOnly:true});
      db.exec('BEGIN'); opened=true; // both observations use one SQLite read snapshot
      const get = db.prepare(activeSql);
      const hasHistory=tableExists(db,'compression_receipt_history');
      const hasGeneration=tableExists(db,'physical_generation');
      const generation=hasGeneration ? db.prepare(
        "SELECT source_digest FROM physical_generation WHERE name='passport'").get()?.source_digest : null;
      const readHistory = hasHistory ? db.prepare(historySql) : null;
      rows=choices.map(x=>{
        if(x.origin==='archive'){
          if(!isDigest(x.generation)||!readHistory)return null;
          const matches=readHistory.all(x.generation,x.id);
          if(matches.length>1){
            problems.push(x.side+':ambiguous-historical-receipt-generation');
            return null;
          }
          return matches[0]?{...matches[0],source:'archive'}:null;
        }
        const active=get.get(x.id);
        return active?{...active,source:'active',generation_digest:generation??null}:null;
      });
      for(let i=0;i<2;i++){
        const x=choices[i],row=rows[i],side=x.side;
        if(!row){problems.push(side+':native-physical-receipt-not-found');continue;}
        if(x.generation!==undefined && row.generation_digest!==x.generation)
          problems.push(side+':physical-generation-drift');
        if(x.origin==='archive' && (!isDigest(row.generation_digest)||!isName(row.passport)))
          problems.push(side+':historical-coordinate-invalid');
        if(x.passport!==undefined && x.passport!==row.passport)
          problems.push(side+':physical-passport-mismatch');
        for(const metric of counters){
          if(!Number.isSafeInteger(row[metric])||row[metric]<0)
            problems.push(side+':invalid-native-counter-'+metric);
          if(record?.[side]?.physical_counters && record[side].physical_counters[metric]!==row[metric])
            problems.push(side+':physical-counter-differs-'+metric);
        }
        for(const metric of directMetrics)
          if(record?.[side]?.metrics?.[metric]!==row[metric])
            problems.push(side+':claimed-'+metric+'-differs-from-native-receipt');
      }
      if(rows.every(Boolean)){
        const [before,after]=rows;
        if(!isName(before.subject_family)||before.subject_family!==after.subject_family||
           !isName(before.semantic_digest)||before.semantic_digest!==after.semantic_digest)
          problems.push('physical-passports-differ-in-semantic-family-or-digest');
        if(before.exactness_class!=='exact')
          problems.push('baseline-physical-representation-not-exact');
        if(after.exactness_class==='latent' && record?.candidate?.kind!=='approximate-prefilter')
          problems.push('latent-representation-without-approximation-gate');
        if(!['exact','latent'].includes(after.exactness_class))
          problems.push('candidate-unknown-physical-exactness');
        if(!before.useful_bytes || before.useful_bytes!==after.useful_bytes)
          problems.push('unequal-or-empty-useful-work');
        if(before.source==='archive'&&after.source==='archive'&&
           before.generation_digest===after.generation_digest && before.receipt_id===after.receipt_id)
          problems.push('same-historical-observation');
      }
    } catch {
      problems.push('native-physical-registry-unavailable-or-schema-incompatible');
    } finally {
      if(opened){try{db.exec('ROLLBACK')}catch{problems.push('native-read-transaction-close-failed')}}
      try{db?.close()}catch{problems.push('native-registry-close-failed')}
    }
  }
  const valid=rows?.every(Boolean);
  const physical=valid?{
    source:'BonfyreCompress active/historical SQLite CopyReceipts, single read snapshot',
    baseline:{receipt_id:rows[0].receipt_id,source:rows[0].source,
      generation_digest:rows[0].generation_digest??null,passport:rows[0].passport,
      subject_family:rows[0].subject_family,semantic_digest:rows[0].semantic_digest,
      exactness_class:rows[0].exactness_class,
      counters:Object.fromEntries(counters.map(k=>[k,rows[0][k]]))},
    candidate:{receipt_id:rows[1].receipt_id,source:rows[1].source,
      generation_digest:rows[1].generation_digest??null,passport:rows[1].passport,
      subject_family:rows[1].subject_family,semantic_digest:rows[1].semantic_digest,
      exactness_class:rows[1].exactness_class,
      counters:Object.fromEntries(counters.map(k=>[k,rows[1][k]]))},
    measured_copy_delta:{
      copied:relative(rows[0].bytes_copied,rows[1].bytes_copied,rows[0].useful_bytes),
      hydrated:relative(rows[0].bytes_hydrated,rows[1].bytes_hydrated,rows[0].useful_bytes),
      allocations:relative(rows[0].allocations,rows[1].allocations,rows[0].useful_bytes),
      units:'native counters for matched useful bytes, not provider cost or authorization'
    }
  }:null;
  return {...reviewed,decision:problems.length?'hold-baseline':reviewed.decision,
    problems,physical,admitted_for_execution:false,no_effect:true,
    cannot_infer:[...reviewed.cannot_infer,
      'a caller-supplied owner observation or external trace is independently authenticated',
      'a past generation measurement describes the current live representation',
      'native counters demonstrate benefit on unmeasured cohorts or hardware',
      'native representation savings imply cash saved without priced occurrences',
      'one source family and useful-byte equality alone prove end-to-end parity'
    ]};
}

/** Portable version of the no-loss review for pairs of native read-receipt
 * JSON outputs. Unlike the SQLite readback above, supplied JSON is not an
 * authenticated owner record. Consumers must independently witness source,
 * transport, actor permission and actual effects.
 */
export function reviewPhysicalReadbacks(record, readbacks) {
  const reviewed=reviewPerformance(record);
  const problems=[...reviewed.problems];
  const sides=['baseline','candidate'];
  const validDigest=x=>typeof x==='string'&&/^[a-f0-9]{64}$/.test(x);
  const items=sides.map(side=>({claim:record?.[side]||{},raw:readbacks?.[side],side}));
  for(const {claim,raw,side} of items) {
    if(!raw||raw.schema!=='bonfyre.copyreceipt.readback.v1'||raw.no_effect!==true) {
      problems.push(side+':unrecognized-native-readback');continue;
    }
    if(!['active','archived'].includes(raw.origin)||!isName(raw.receipt_id)||
       !validDigest(raw.generation_digest)||!isName(raw.passport_identity)||
       !isName(raw.subject_family)||!isName(raw.semantic_digest)||
       !isName(raw.physical_digest)||!['exact','latent'].includes(raw.exactness_class))
      problems.push(side+':invalid-native-coordinate');
    const expectedOrigin=claim.physical_source==='archive'?'archived':'active';
    if(raw.origin!==expectedOrigin)
      problems.push(side+':native-origin-mismatch');
    if(!validDigest(claim.physical_generation_digest)||claim.physical_generation_digest!==raw.generation_digest)
      problems.push(side+':native-generation-not-bound');
    if(!isName(claim.physical_passport)||claim.physical_passport!==raw.passport_identity)
      problems.push(side+':native-passport-not-bound');
    if(!isName(claim.physical_receipt_id)||claim.physical_receipt_id!==raw.receipt_id)
      problems.push(side+':native-receipt-id-not-bound');
    for(const metric of counters) {
      const observed=raw.counters?.[metric];
      if(!Number.isSafeInteger(observed)||observed<0)
        problems.push(side+':invalid-native-counter-'+metric);
      if(claim.physical_counters && claim.physical_counters[metric]!==observed)
        problems.push(side+':counter-claim-differs-'+metric);
    }
    for(const metric of directMetrics)
      if(claim.metrics?.[metric]!==raw.counters?.[metric])
        problems.push(side+':claimed-'+metric+'-differs-from-native-readback');
  }
  const [before,after]=items.map(x=>x.raw);
  const usable=items.every(x=>x.raw?.schema==='bonfyre.copyreceipt.readback.v1');
  if(usable) {
    if(before.origin===after.origin&&before.generation_digest===after.generation_digest&&
       before.receipt_id===after.receipt_id)
      problems.push('not-two-distinct-native-observations');
    if(before.subject_family!==after.subject_family||
       before.semantic_digest!==after.semantic_digest)
      problems.push('native-readbacks-differ-in-semantic-material');
    if(before.exactness_class!=='exact')
      problems.push('baseline-native-representation-not-exact');
    if(after.exactness_class==='latent'&&record?.candidate?.kind!=='approximate-prefilter')
      problems.push('latent-native-representation-without-approximation-gate');
    if(!Number.isSafeInteger(before.counters?.useful_bytes)||before.counters.useful_bytes<=0||
       before.counters.useful_bytes!==after.counters?.useful_bytes)
      problems.push('unequal-or-empty-native-useful-work');
  }
  const physical=usable?{
    source:'caller-supplied BonfyreCompress read-receipt JSON; external provenance unverified',
    baseline:before,candidate:after,
    measured_copy_delta: {
      copied:relative(before.counters?.bytes_copied,after.counters?.bytes_copied,before.counters?.useful_bytes),
      hydrated:relative(before.counters?.bytes_hydrated,after.counters?.bytes_hydrated,before.counters?.useful_bytes),
      allocations:relative(before.counters?.allocations,after.counters?.allocations,before.counters?.useful_bytes),
      units:'same-work native counter comparison; no inferred provider prices'
    }
  }:null;
  return {...reviewed,decision:problems.length?'hold-baseline':reviewed.decision,
    problems,physical,admitted_for_execution:false,no_effect:true,
    cannot_infer:[...reviewed.cannot_infer,
      'caller-provided native readback JSON is an independently authenticated owner receipt',
      'a historical generation authorizes the current execution path',
      'equal native byte counts alone authenticate effects or guarantee future preservation'
    ]};
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  try{
    const record=JSON.parse(readFileSync(process.argv[2],'utf8'));
    let verdict;
    if(process.argv.length===4) {
      verdict=reviewPhysicalReceipts(record,process.argv[3]);
    } else if(process.argv.length===6 && process.argv[3]==='--native-readbacks') {
      verdict=reviewPhysicalReadbacks(record,{
        baseline:JSON.parse(readFileSync(process.argv[4],'utf8')),
        candidate:JSON.parse(readFileSync(process.argv[5],'utf8'))});
    } else throw Error('usage: performance_physical_receipts.mjs RECORD SQLITE | RECORD --native-readbacks BASELINE.json CANDIDATE.json');
    console.log(JSON.stringify(verdict,null,2));
    if(verdict.decision!=='eligible-for-human-promotion-review')process.exitCode=1;
  }catch(e){console.error(e.message);process.exitCode=2}
}
