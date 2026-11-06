import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {observe,compileReferencedDraft} from '../extension/observation.mjs';
import {compileDraft,basketPreview} from '../extension/recurring-commerce.mjs';
import {replaceContents,contentReferences} from '../extension/contents.mjs';
import {moveCycle,reviseCycles,reviewText,changeFields} from '../extension/workbench-actions.mjs';
import {sourceUpdatePlan,sourceUpdateWrites,upcomingPrograms} from '../extension/contents-operations.mjs';
const catalog=JSON.parse(readFileSync(new URL('../extension/catalog.json',import.meta.url),'utf8'));
const capture=(price='12.35',date='2026-10-15T12:00:00Z',extra={})=>({url:'https://fixture.invalid/pantry',title:'Fixture pantry',fixture:true,captured_at:date,structured:[{name:'Coffee',sku:'coffee',url:'https://fixture.invalid/coffee','@type':'Product',offers:{'@type':'Offer',price,priceCurrency:'USD',availability:'https://schema.org/InStock'}},{name:'Tea',sku:'tea',url:'https://fixture.invalid/tea','@type':'Product',offers:{'@type':'Offer',price:'6.65',priceCurrency:'USD',availability:'https://schema.org/InStock'}}],...extra});
const source=await observe(capture(),catalog),laterCapture=capture('14.50','2026-10-16T12:00:00Z');laterCapture.structured.reverse();const later=await observe(laterCapture,catalog);
const sourceRows=[{key:source.id,revision:3,value:source},{key:later.id,revision:1,value:later}],observations=sourceRows.map(r=>r.value);
const input=(id='one',pattern='discovery_box')=>({program_id:'draft:contents:'+id,template_id:catalog.templates.find(t=>t.pattern===pattern).id,owner_subject:'person:test',name:'Pantry '+id,sample:true,terms:{currency:'USD',price_cents:4500,shipping_cents:210,max_members:1,quantity_per_member:1,start_date:'2026-11-01',cadence:{unit:'month',every:1},auto_renew:pattern!=='finite_gift',...(pattern==='finite_gift'?{gift_cycles:3}:{}),cancellation:'Cancel before review.',return_policy:'Review with the supplier.'}});
const lines=[{id:'line:coffee',source:source.id,offer:0,quantity:2},{id:'line:tea',source:source.id,offer:1,quantity:1}];
const program=(id='one',pricing='itemized')=>replaceContents(input(id),'defaults',lines,{pricing});
const compile=value=>compileReferencedDraft(catalog,value,observations.filter(o=>contentReferences(value.terms.contents).includes(o.id)));
const records=()=>['one','two','three'].map((id,n)=>({key:input(id).program_id,revision:n+2,value:compile(program(id,id==='three'?'fixed':'itemized'))}));
test('contents derive exact money from references rather than trusting supplied product prices',()=>{
 const value=program(),before=structuredClone(value),draft=compile(value);expect(draft.cycle_preview[0].contents.modeled_cents).toBe(3345);expect(basketPreview([draft],5000).months[0].modeled_cents).toBe(3345);expect(value).toEqual(before);expect(value.terms.contents.defaults[0].unit_cents).toBeUndefined();
 const forged=structuredClone(value);forged.terms.contents.defaults[0].unit_cents=1;expect(()=>compile(forged)).toThrow('line identity');
});
test('cycle-only contents retain their scope when recurring defaults and dates change',()=>{
 let value=replaceContents(program(),'2026-11-01',[...lines,{id:'line:extra',source:later.id,offer:1,quantity:1}]);
 value=replaceContents(value,'defaults',[{...lines[0],quantity:3},lines[1]]);const draft=compile(value);expect(draft.cycle_preview[0].contents.units).toBe(4);expect(draft.cycle_preview[1].contents.units).toBe(4);expect(draft.cycle_preview[0].contents.modeled_cents).toBe(4795);expect(draft.cycle_preview[1].contents.modeled_cents).toBe(4580);
 const moved=compile(moveCycle(catalog,value,'2026-11-01','2026-10-31'));expect(moved.cycle_preview[0].contents).toEqual(draft.cycle_preview[0].contents);expect(moved.cycle_preview[1].due_local_date).toBe('2026-12-01');
 const reverted=compile(replaceContents(value,'2026-11-01',null));expect(reverted.cycle_preview[0].contents.selection).toBe('default');expect(reverted.cycle_preview[0].contents.modeled_cents).toBe(4580);
});
test('quantity rules and missing prices are visible unresolved totals, not zero-cost approvals',async()=>{
 const over=program();over.terms.contents.maximum=2;over.terms.contents.per_product=2;expect(compile(over).cycle_preview[0].contents.problems).toContain('Choose at most 2 units.');expect(basketPreview([compile(over)],5000).months).toEqual([]);
 const minimum=program();minimum.terms.contents.minimum=4;expect(compile(minimum).readiness.unresolved).toContain('contents_selection');
 for(const quantity of [0,-1,1.5,1001])expect(()=>compile(replaceContents(program(),'defaults',[{...lines[0],quantity}]))).toThrow();
 const unknown=await observe(capture(null),catalog),value=replaceContents(program(),'defaults',[{...lines[0],source:unknown.id}]);const d=compileReferencedDraft(catalog,value,[unknown]);expect(d.cycle_preview[0].contents.modeled_cents).toBe(null);expect(basketPreview([d],5000).variable_priced_programs).toEqual([value.program_id]);
});
test('fixed price bundles and gifts keep their own economic agreement',()=>{
 const fixed=compile(program('fixed','fixed'));expect(fixed.cycle_preview[0].contents.modeled_cents).toBe(4710);
 const gift=replaceContents(input('gift','finite_gift'),'defaults',lines,{pricing:'fixed'}),first=compile(gift),skipped=compile(reviseCycles(catalog,gift,['2026-11-01'],true));expect(basketPreview([skipped],20000)).toEqual(basketPreview([first],20000));expect(basketPreview([first],20000).months[0].modeled_cents).toBe(14130);
 gift.terms.contents.pricing='itemized';expect(()=>compile(gift)).toThrow('separate price agreement');
});
test('a source update joins SKU and product addresses after source offer order changes',()=>{
 const before=records(),snapshot=structuredClone(before),plan=sourceUpdatePlan(catalog,before,sourceRows,source.id,later.id);expect(plan.rows).toHaveLength(3);expect(plan.rows.every(r=>!r.error)).toBe(true);expect(plan.rows[0].next.terms.contents.defaults[0]).toEqual({...lines[0],source:later.id,offer:1});expect(plan.rows[0].months[0]).toEqual({month:'2026-11',before:3345,after:3775});expect(plan.rows[2].months).toEqual([]);expect(before).toEqual(snapshot);
 const writes=sourceUpdateWrites(plan,[before[0].key,before[2].key]);expect(writes.filter(w=>!w.checkOnly).map(w=>w.key)).toEqual([before[0].key,before[2].key]);expect(writes.find(w=>w.key===source.id).expectedRevision).toBe(3);expect(writes.find(w=>w.key===before[0].key).expectedRevision).toBe(2);expect(writes.some(w=>w.key===before[1].key)).toBe(false);
});
test('changed identity, ambiguous variants and unavailable source products need manual reconciliation',async()=>{
 for(const kind of ['missing','ambiguous','unavailable']){const raw=capture('14.50','2026-10-17T12:00:00Z');if(kind==='missing')raw.structured[0].sku='other';if(kind==='ambiguous')raw.structured.push(structuredClone(raw.structured[0]));if(kind==='unavailable')raw.structured[0].offers.availability='https://schema.org/OutOfStock';const next=await observe(raw,catalog),p=sourceUpdatePlan(catalog,records(),[sourceRows[0],{key:next.id,revision:1,value:next}],source.id,next.id);expect(p.rows.every(r=>!!r.error)).toBe(true);expect(()=>sourceUpdateWrites(p,[p.rows[0].key])).toThrow();}
 expect(()=>sourceUpdatePlan(catalog,records(),sourceRows,later.id,source.id)).toThrow('later');expect(()=>sourceUpdateWrites(sourceUpdatePlan(catalog,records(),sourceRows,source.id,later.id),[])).toThrow();
});
test('source references stay necessary and stale cycle overrides cannot silently migrate',()=>{
 expect(()=>compileReferencedDraft(catalog,program(),[])).toThrow('source');const value=replaceContents(program(),'2026-11-01',lines);value.terms.start_date='2026-11-02';expect(()=>compile(value)).toThrow('no longer belongs');
 const duplicate=program();duplicate.terms.contents.defaults.push({...lines[0],id:'line:duplicate'});expect(()=>compile(duplicate)).toThrow('same product');
});
test('readable review shows changed products, quantities, dates and actual modeled costs',()=>{
 const prior=compile(program()),next=compile(replaceContents(program(),'2026-11-01',[{...lines[1],quantity:2}]));expect(reviewText(next)).toContain('2 \u00D7 Tea');expect(reviewText(next)).toContain('$15.40');expect(reviewText(next)).not.toContain('Terms: $45.00 per cycle');expect(changeFields(prior,next).some(r=>r.label==='Contents for 2026-11-01'&&r.after.includes('2 \u00D7 Tea'))).toBe(true);
});
test('the combined schedule replaces the open saved copy without merging programs on one date',()=>{
 const saved=records(),open=compile(program('one')),result=upcomingPrograms(catalog,saved,observations,open);expect(result.errors).toEqual([]);expect(result.rows.filter(r=>r.cycle.due_local_date==='2026-11-01')).toHaveLength(3);expect(result.rows.filter(r=>r.program===open.program_id)).toHaveLength(6);
});
