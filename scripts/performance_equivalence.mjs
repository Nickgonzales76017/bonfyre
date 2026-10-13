#!/usr/bin/env node
/** Read-only review of two owner-observed performance paths.
 * This is NOT an execution, authorization, promotion, or payment carrier.
 * Caller-supplied references still require independent owner verification.
 */
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

const SHA=/^(?:sha256:)?[a-f0-9]{64}$/i;
const EXACT=new Set(['authority','identity','money','publication','effect','fulfillment','legal','privacy']);
const APPROX=new Set(['discovery','retrieval','ranking','exploration','simulation']);
const METRICS=['p95_ms','peak_resident_bytes','bytes_copied','bytes_hydrated','allocations','external_calls'];
const str=x=>typeof x==='string'&&x.trim().length>0;
const measure=x=>typeof x==='number'&&Number.isFinite(x)&&x>=0;
const features=x=>Array.isArray(x)&&x.length>0&&x.every(str)&&new Set(x).size===x.length;
const demand=(issues,ok,reason)=>{if(!ok)issues.push(reason);};

export function reviewPerformance(input) {
  const record=input||{}, scope=record.coordinate||{}, baseline=record.baseline||{}, candidate=record.candidate||{}, problems=[];
  demand(problems,record.schema==='bonfyre.performance.equivalence.v1','unsupported-schema');
  for(const name of ['subject','owner','operation','scope','work_class','source_digest','environment_digest','cohort'])
    demand(problems,str(scope[name]),'missing-coordinate:'+name);
  demand(problems,SHA.test(scope.source_digest||''),'source-digest-not-bound');
  demand(problems,SHA.test(scope.environment_digest||''),'environment-digest-not-bound');
  demand(problems,str(record.rollback?.baseline_route)&&str(record.rollback?.receipt_ref),'rollback-not-witnessed');
  demand(problems,features(baseline.features),'baseline-feature-contract-missing');
  demand(problems,features(candidate.features),'candidate-feature-contract-missing');
  if(features(baseline.features)&&features(candidate.features))
    for(const f of baseline.features)demand(problems,candidate.features.includes(f),'feature-lost:'+f);
  for(const [label,v] of [['baseline',baseline],['candidate',candidate]]) {
    demand(problems,str(v.receipt_ref),label+':receipt-missing');
    demand(problems,v.owner===scope.owner&&v.subject===scope.subject&&v.scope===scope.scope,label+':authority-or-owner-drift');
    demand(problems,v.source_digest===scope.source_digest,label+':source-drift');
    demand(problems,v.environment_digest===scope.environment_digest,label+':environment-drift');
    demand(problems,SHA.test(v.result_digest||''),label+':result-digest-invalid');
    demand(problems,SHA.test(v.effect_digest||''),label+':effect-digest-invalid');
    demand(problems,label==='baseline'?v.kind==='exact':(v.kind==='exact'||v.kind==='approximate-prefilter'),label+':unknown-kind');
    for(const metric of METRICS)demand(problems,measure(v.metrics?.[metric]),label+':missing-metric-'+metric);
  }
  demand(problems,str(baseline.receipt_ref)&&str(candidate.receipt_ref)&&baseline.receipt_ref!==candidate.receipt_ref,'candidate-reuses-baseline-receipt');
  demand(problems,baseline.result_digest===candidate.result_digest,'final-result-differs');
  demand(problems,baseline.effect_digest===candidate.effect_digest,'effect-or-authorization-differs');
  if(candidate.kind==='approximate-prefilter'){
    const p=candidate.preservation||{};
    demand(problems,APPROX.has(scope.work_class)&&!EXACT.has(scope.work_class),'loss-forbidden-for-exact-work');
    demand(problems,p.final_exact_recheck===true&&str(p.recheck_receipt_ref),'exact-recheck-not-witnessed');
    demand(problems,str(scope.axis)&&p.axis===scope.axis,'representation-axis-drift');
    demand(problems,Number.isInteger(scope.k)&&scope.k>0&&p.k===scope.k,'recall-k-drift');
    demand(problems,p.cohort===scope.cohort,'preservation-cohort-drift');
    demand(problems,measure(p.min_recall)&&p.min_recall<=1&&measure(p.observed_recall)&&p.observed_recall<=1&&p.observed_recall>=p.min_recall,'measured-recall-budget-not-met');
    demand(problems,Number.isInteger(p.false_prunes)&&p.false_prunes>=0,'false-prune-count-missing');
    demand(problems,p.false_prunes===0,'false-prune-means-functionality-loss');
  }
  const deltas={};
  let better=false;
  for(const metric of METRICS){
    if(measure(baseline.metrics?.[metric])&&measure(candidate.metrics?.[metric])){
      deltas[metric]=candidate.metrics[metric]-baseline.metrics[metric];
      if(deltas[metric]<0)better=true;
    }
  }
  demand(problems,better,'no-measured-resource-gain');
  for(const metric of ['p95_ms','peak_resident_bytes','external_calls'])
    demand(problems,measure(baseline.metrics?.[metric])&&measure(candidate.metrics?.[metric])&&candidate.metrics[metric]<=baseline.metrics[metric],metric+'-regressed');
  const before=baseline.provider_cost,after=candidate.provider_cost;
  const priced=before&&after&&measure(before.amount_usd)&&measure(after.amount_usd)&&str(before.rate_receipt_ref)&&str(after.rate_receipt_ref);
  const financial=priced?{priced:true,observed_cost_delta_usd:after.amount_usd-before.amount_usd,rate_refs:[before.rate_receipt_ref,after.rate_receipt_ref]}:
    {priced:false,observed_cost_delta_usd:null,reason:'unknown provider rates remain unknown, not free'};
  if(priced)demand(problems,after.amount_usd<=before.amount_usd,'priced-provider-cost-regressed');
  return {schema:'bonfyre.performance.equivalence-review.v1',
    decision:problems.length?'hold-baseline':'eligible-for-human-promotion-review',
    admitted_for_execution:false,no_effect:true,problems,deltas,financial,
    coordinate:{subject:scope.subject||null,owner:scope.owner||null,operation:scope.operation||null,
      work_class:scope.work_class||null,cohort:scope.cohort||null},
    baseline_route:record.rollback?.baseline_route||null,
    cannot_infer:[
      'caller-supplied receipt IDs are authentic without independent owner readback',
      'this cohort establishes all future output or behavior equivalence',
      'this review authorizes changing a runtime, grant, application or customer record',
      'approximation can replace exact authorization, identity, money or publication',
      'resource savings equal cash without actual priced provider occurrences'
    ]};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  try{
    if(process.argv.length!==3)throw Error('usage: node scripts/performance_equivalence.mjs paired-owner-receipts.json');
    const verdict=reviewPerformance(JSON.parse(readFileSync(process.argv[2],'utf8')));
    console.log(JSON.stringify(verdict,null,2));
    if(verdict.decision!=='eligible-for-human-promotion-review')process.exitCode=1;
  }catch(e){console.error(e.message);process.exitCode=2;}
}
