// Exact retained catalogue passages for the existing fixture planner.
// Never a supplier-page read, a quote, a publication or a write owner.
import {selectedRequirement} from './opportunity-selection.mjs';

const fail=message=>{throw Error(message);};
export function retainedFixtureText(product){
  if(!product||typeof product!=='object')fail('Missing retained product.');
  return ['Listing title: '+product.title,'Option: '+product.option,'Use: '+product.use,
    'Observed at: '+product.observed_at,'Source URL: '+product.source_url,
    ...(product.questions??[]).map(q=>'Recorded question: '+q)].join('\n');
}
export function selectPlannerPassage(plan,catalogue,productAlias,start,end){
  const product=catalogue?.products?.find(p=>p.id===productAlias);
  if(!product||plan?.catalogue!==catalogue.digest||typeof plan.id!=='string')
    fail('The plan and retained source release do not match.');
  const source=retainedFixtureText(product);
  const selected=selectedRequirement({[product.id]:source},product.id,start,end);
  return Object.freeze({schema:'service-site.fixture-source-selection.v1',
    project_id:plan.id,plan_fingerprint:JSON.stringify(plan),catalogue_digest:catalogue.digest,
    product:product.id,product_id:product.product_id,variant_id:product.variant_id,
    source_sha256:product.source_sha256,source_url:product.source_url,
    start:selected.start,end:selected.end,text:selected.text});
}
// Explicit user action creates an *unsaved* proposal; the existing planner owner
// validates and saves it with the existing revision conflict checks.
export function adoptPlannerPassage(plan,catalogue,selection){
  if(selection?.schema!=='service-site.fixture-source-selection.v1'||
     selection.project_id!==plan?.id||selection.plan_fingerprint!==JSON.stringify(plan)||
     selection.catalogue_digest!==catalogue?.digest)
    fail('The plan changed after source review. Start a new review.');
  const product=catalogue.products.find(p=>p.id===selection.product);
  if(!product||product.product_id!==selection.product_id||product.variant_id!==selection.variant_id||
     product.source_url!==selection.source_url||product.source_sha256!==selection.source_sha256)
    fail('The retained source identity changed after review.');
  const fresh=selectPlannerPassage(plan,catalogue,product.id,selection.start,selection.end);
  if(fresh.text!==selection.text)fail('The retained passage changed after review.');
  const annotation='[Retained catalogue '+product.id+'; snapshot '+product.source_sha256+
    '; catalogue '+catalogue.digest+']\n'+fresh.text+'\n'+product.source_url;
  const notes=(plan.notes?plan.notes+'\n\n':'')+annotation;
  if(notes.length>1500)fail('The selected passage exceeds the plan-notes limit.');
  return {...structuredClone(plan),notes};
}
