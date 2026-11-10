import {contentReferences,sourceOfferIdentity} from './contents.mjs';
import {compileReferencedDraft} from './observation.mjs';
import {basketPreview} from './recurring-commerce.mjs';
const check=(ok,message)=>{if(!ok)throw Error(message);};
export function recompileProgram(catalog,input,observations){
  const ids=new Set([...(input.browser_evidence??[]).map(r=>r.id),...contentReferences(input.terms?.contents)]);
  const selected=observations.filter(o=>ids.has(o.id));
  check(selected.length===ids.size,'A retained program source is missing.');
  return compileReferencedDraft(catalog,input,selected);
}
export function sourceUpdatePlan(catalog,records,sourceRows,oldId,newId){
  const observations=sourceRows.map(r=>r.value),before=observations.find(o=>o.id===oldId),after=observations.find(o=>o.id===newId);
  check(before&&after&&before.id!==after.id&&before.captured.url===after.captured.url,'Choose two different versions of the same source page.');
  check(Date.parse(after.captured.captured_at)>Date.parse(before.captured.captured_at),'Choose a later recorded source version.');
  const rows=[];
  for(const record of records){
    if(record.value?.schema!=='aurekai.recurring-program.v1')continue;
    try{
      if(!contentReferences(record.value.terms?.contents).includes(oldId))continue;
      const input=structuredClone(record.value),changes=[];
      for(const items of [input.terms.contents.defaults,...input.terms.contents.cycles.map(c=>c.items)])for(const item of items){
        if(item.source!==oldId)continue;
        const old=before.offers[item.offer];check(old,'An original product is missing.');
        const identity=sourceOfferIdentity(old),originals=before.offers.filter(o=>sourceOfferIdentity(o)===identity);
        const matches=after.offers.map((offer,index)=>({offer,index})).filter(r=>sourceOfferIdentity(r.offer)===identity);
        check(originals.length===1&&matches.length===1,'A product identity is missing or ambiguous. Replace that product explicitly.');
        const match=matches[0];changes.push({title:old.product_name,quantity:item.quantity,before:old.price.usd_cents,after:match.offer.price.usd_cents});
        item.source=newId;item.offer=match.index;
      }
      input.browser_evidence=(input.browser_evidence??[]).map(ref=>ref.id===oldId?{id:newId,url:after.captured.url,sha256:after.captured_body_sha256}:ref);
      input.browser_evidence=input.browser_evidence.filter((ref,n,all)=>all.findIndex(r=>r.id===ref.id)===n);
      const prior=recompileProgram(catalog,record.value,observations),next=recompileProgram(catalog,input,observations);
      const issues=next.cycle_preview.flatMap(c=>c.state==='skip_requested'?[]:c.contents?.problems??[]);
      check(!issues.length,issues.join(' '));
      const totals=d=>new Map(basketPreview([d],100000000).months.map(m=>[m.month,m.modeled_cents]));
      const a=totals(prior),b=totals(next),months=[...new Set([...a.keys(),...b.keys()])].sort().filter(m=>a.get(m)!==b.get(m)).map(month=>({month,before:a.get(month)??0,after:b.get(month)??0}));
      rows.push({key:record.key,revision:record.revision,name:next.name,changes,months,next,error:null});
    }catch(error){rows.push({key:record.key,revision:record.revision,name:record.value.name,error:error.message});}
  }
  return {before,after,rows,sourceRows};
}
export function sourceUpdateWrites(plan,selected){
  check(Array.isArray(selected)&&selected.length>0&&new Set(selected).size===selected.length,'Choose the programs to update.');
  const rows=selected.map(id=>plan.rows.find(r=>r.key===id));check(rows.every(r=>r&&!r.error),'A selected program requires manual source reconciliation.');
  const dependencies=new Set([plan.before.id,plan.after.id,...rows.flatMap(r=>r.next.browser_evidence.map(s=>s.id))]);
  const writes=[...dependencies].map(id=>{const row=plan.sourceRows.find(r=>r.key===id);check(row,'A source was not retained when this review was prepared.');return {key:id,value:row.value,expectedRevision:row.revision,checkOnly:true};});
  return [...writes,...rows.map(r=>({key:r.key,value:r.next,expectedRevision:r.revision}))];
}
export function upcomingPrograms(catalog,records,observations,current=null){
  const inputs=records.filter(r=>r.key!==current?.program_id).map(r=>r.value);if(current)inputs.push(current);
  const rows=[],errors=[];
  for(const input of inputs){try{const draft=recompileProgram(catalog,input,observations);for(const cycle of draft.cycle_preview)rows.push({program:draft.program_id,name:draft.name,cycle,pattern:draft.pattern});}catch(error){errors.push({name:input.name,error:error.message});}}
  rows.sort((a,b)=>a.cycle.due_local_date.localeCompare(b.cycle.due_local_date)||a.name.localeCompare(b.name));
  return {rows,errors};
}
