import {test} from 'node:test';
import assert from 'node:assert/strict';
import {reviewPerformance} from '../performance_equivalence.mjs';
const sha=c=>'sha256:'+c.repeat(64);
function fixture(){
 const coordinate={subject:'public:item-1',owner:'BonfyreFinance',operation:'qualified-read',
   scope:'public',work_class:'discovery',source_digest:sha('a'),environment_digest:sha('b'),
   cohort:'same-input',axis:'learned-embedding',k:5};
 const common={subject:coordinate.subject,owner:coordinate.owner,scope:coordinate.scope,
   source_digest:coordinate.source_digest,environment_digest:coordinate.environment_digest,
   result_digest:sha('c'),effect_digest:sha('d'),features:['source','authority','output']};
 return {schema:'bonfyre.performance.equivalence.v1',coordinate,
   rollback:{baseline_route:'BonfyreFinance/exact-read',receipt_ref:'owner:rollback-1'},
   baseline:{...common,kind:'exact',receipt_ref:'owner:baseline-1',metrics:{
     p95_ms:20,peak_resident_bytes:8000,bytes_copied:10000,bytes_hydrated:5000,allocations:120,external_calls:3}},
   candidate:{...common,features:[...common.features],kind:'exact',receipt_ref:'owner:candidate-1',metrics:{
     p95_ms:15,peak_resident_bytes:5000,bytes_copied:1000,bytes_hydrated:200,allocations:20,external_calls:2}}};
}
const fails=(p,re)=>{const x=reviewPerformance(p);assert.equal(x.decision,'hold-baseline');assert.match(x.problems.join(' '),re);};
test('identical measured output preserves all features but requires separate human promotion',()=>{
 const r=reviewPerformance(fixture());assert.equal(r.decision,'eligible-for-human-promotion-review');
 assert.equal(r.admitted_for_execution,false);assert.equal(r.no_effect,true);
 assert.equal(r.financial.observed_cost_delta_usd,null);
});
test('remove a feature and revert to baseline',()=>{const p=fixture();p.candidate.features.pop();fails(p,/feature-lost/);});
test('different result or effect never qualifies',()=>{const p=fixture();p.candidate.result_digest=sha('e');fails(p,/final-result-differs/);p.candidate.result_digest=p.baseline.result_digest;p.candidate.effect_digest=sha('f');fails(p,/effect-or-authorization/);});
test('scope and source revisions stay exact',()=>{const p=fixture();p.candidate.scope='restricted';p.candidate.source_digest=sha('f');fails(p,/authority-or-owner-drift|source-drift/);});
test('worse tail or memory or external calls refuse even if copying improved',()=>{
 for(const [key,value] of [['p95_ms',60],['peak_resident_bytes',9000],['external_calls',4]]){
  const p=fixture();p.candidate.metrics[key]=value;fails(p,/regressed/);
 }});
test('unmeasured counters and missing observation receipts refuse',()=>{
 const p=fixture();delete p.baseline.metrics.allocations;fails(p,/missing-metric-allocations/);
 const q=fixture();delete q.candidate.receipt_ref;fails(q,/receipt-missing/);
});
test('no demonstrable gain refuses',()=>{const p=fixture();p.candidate.metrics={...p.baseline.metrics};fails(p,/no-measured-resource-gain/);});
test('approximation requires equivalent final output and an exact receiving recheck',()=>{
 const p=fixture();p.candidate.kind='approximate-prefilter';fails(p,/exact-recheck-not-witnessed/);
 p.candidate.preservation={axis:p.coordinate.axis,k:5,cohort:'same-input',min_recall:.9,observed_recall:.95,false_prunes:1,final_exact_recheck:true,recheck_receipt_ref:'owner:exact-rerank'};
 assert.equal(reviewPerformance(p).decision,'eligible-for-human-promotion-review');
 p.candidate.result_digest=sha('e');fails(p,/final-result-differs/);
});
test('approximation can never bypass exact money or identity',()=>{
 for(const kind of ['money','identity','publication','effect','fulfillment']){
  const p=fixture();p.coordinate.work_class=kind;p.candidate.kind='approximate-prefilter';
  p.candidate.preservation={axis:p.coordinate.axis,k:5,cohort:'same-input',min_recall:.9,observed_recall:1,false_prunes:0,final_exact_recheck:true,recheck_receipt_ref:'owner:exact-rerank'};
  fails(p,/loss-forbidden-for-exact-work/);
 }});
test('different axis, k or cohort never borrow each other’s evidence',()=>{
 for(const property of ['axis','k','cohort']){
  const p=fixture();p.candidate.kind='approximate-prefilter';
  p.candidate.preservation={axis:p.coordinate.axis,k:5,cohort:'same-input',min_recall:.9,observed_recall:.95,false_prunes:1,final_exact_recheck:true,recheck_receipt_ref:'owner:exact-rerank'};
  p.candidate.preservation[property]=property==='k'?7:'other';fails(p,/drift/);
 }});
test('prices are unknown until both rates are evidenced, and priced regressions hold',()=>{
 const p=fixture();p.baseline.provider_cost={amount_usd:1};p.candidate.provider_cost={amount_usd:0};
 assert.equal(reviewPerformance(p).financial.observed_cost_delta_usd,null);
 p.baseline.provider_cost.rate_receipt_ref='provider:rate-1';p.candidate.provider_cost.rate_receipt_ref='provider:rate-1';
 assert.equal(reviewPerformance(p).financial.observed_cost_delta_usd,-1);
 p.candidate.provider_cost.amount_usd=2;fails(p,/priced-provider-cost-regressed/);
});
test('missing rollback or unsupported schema refuses',()=>{
 const p=fixture();delete p.rollback;fails(p,/rollback-not-witnessed/);
 p.rollback={baseline_route:'old',receipt_ref:'owner:rollback'};p.schema='unknown';fails(p,/unsupported-schema/);
});
