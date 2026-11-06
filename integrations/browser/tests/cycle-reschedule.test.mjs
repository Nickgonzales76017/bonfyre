import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {compileDraft,basketPreview} from '../extension/recurring-commerce.mjs';
import {moveCycle,cycleMoveImpact,reviseCycles,changeFields,reviewText} from '../extension/workbench-actions.mjs';
const catalog=JSON.parse(readFileSync(new URL('../extension/catalog.json',import.meta.url),'utf8'));
const input=(pattern='discovery_box')=>({program_id:'draft:date-review',template_id:catalog.templates.find(t=>t.pattern===pattern).id,owner_subject:'person:test',name:'Coffee proposal',sample:true,browser_evidence:[{id:'source:one',url:'https://fixture.invalid/coffee',sha256:'a'.repeat(64)}],terms:{currency:'USD',price_cents:1235,shipping_cents:210,max_members:1,quantity_per_member:1,start_date:'2026-11-01',cadence:{unit:pattern==='replenishment'?'usage':'month',every:1},auto_renew:pattern!=='finite_gift',gift_cycles:pattern==='finite_gift'?3:undefined,cancellation:'Review before renewing.',return_policy:'Confirm with the seller.'},preferences:{no_repeats:true}});
test('moving one cycle changes its month without moving the series or losing its source identity',()=>{
 const original=input(),before=structuredClone(original),next=moveCycle(catalog,original,'2026-12-01','2026-11-30'),draft=compileDraft(catalog,next);
 expect(original).toEqual(before);expect(next.browser_evidence).toEqual(original.browser_evidence);
 expect(draft.cycle_preview[1]).toMatchObject({cycle:2,due_local_date:'2026-11-30',original_local_date:'2026-12-01',effect:'none'});
 expect(draft.cycle_preview.slice(2)).toEqual(compileDraft(catalog,original).cycle_preview.slice(2));
 expect(cycleMoveImpact(catalog,original,next).months).toEqual([{month:'2026-11',before:1445,after:2890},{month:'2026-12',before:1445,after:0}]);
 expect(draft.stage_plan.every(s=>s.state==='not_executed')).toBe(true);
});
test('a skipped cycle keeps its skip through moves and restoration uses its original anchor',()=>{
 const skipped=reviseCycles(catalog,input(),['2026-12-01'],true),next=moveCycle(catalog,skipped,'2026-12-01','2026-11-30');
 expect(compileDraft(catalog,next).cycle_preview[1].state).toBe('skip_requested');
 expect(next.terms.pause_dates).toEqual(['2026-12-01']);
 expect(basketPreview([compileDraft(catalog,next)],10000)).toEqual(basketPreview([compileDraft(catalog,skipped)],10000));
 const restored=reviseCycles(catalog,next,['2026-11-30'],false);expect(compileDraft(catalog,restored).cycle_preview[1].state).toBe('preview_required');
 expect(restored.terms.pause_dates).toEqual([]);
});
test('repeated moves replace the same override and original-date restoration removes it',()=>{
 const first=moveCycle(catalog,input(),'2026-12-01','2026-11-30'),next=moveCycle(catalog,first,'2026-11-30','2026-11-29');
 expect(next.terms.cycle_moves).toEqual([{from:'2026-12-01',to:'2026-11-29'}]);
 const restored=moveCycle(catalog,next,'2026-11-29','2026-12-01');expect(restored.terms.cycle_moves).toEqual([]);
 expect(compileDraft(catalog,restored).cycle_preview).toEqual(compileDraft(catalog,input()).cycle_preview);
 expect(()=>moveCycle(catalog,next,'2026-12-01','2026-12-02')).toThrow('cycle changed');
});
test('prepaid gift movements keep the original prepaid price and timing',()=>{
 const before=input('finite_gift'),next=moveCycle(catalog,before,'2026-11-01','2026-10-31');
 expect(cycleMoveImpact(catalog,before,next)).toEqual({months:[],prepaid:true,variable:false});
 expect(basketPreview([compileDraft(catalog,next)],10000)).toEqual(basketPreview([compileDraft(catalog,before)],10000));
});
test('collision, reordered cycle, invalid date and unreasonable displacement are refused atomically',()=>{
 for(const date of ['2026-11-01','2027-01-01','2027-01-02','2026-02-30','invalid','2028-01-01'])expect(()=>moveCycle(catalog,input(),'2026-12-01',date)).toThrow();
 expect(()=>moveCycle(catalog,input('replenishment'),'2026-12-01','2026-12-02')).toThrow();
});
test('malformed and stale imported date overrides cannot silently attach to a different series',()=>{
 for(const moves of [{},[{from:'2026-12-01',to:'2026-12-02',extra:true}],[{from:'2026-12-01',to:'2026-12-02'},{from:'2026-12-01',to:'2026-12-03'}],[{from:'2028-12-01',to:'2028-12-02'}],Array(61).fill({from:'2026-12-01',to:'2026-12-02'})]){const value=input();value.terms.cycle_moves=moves;expect(()=>compileDraft(catalog,value)).toThrow();}
 const value=moveCycle(catalog,input(),'2026-12-01','2026-12-02');value.terms.start_date='2026-11-02';expect(()=>compileDraft(catalog,value)).toThrow('no longer belongs');
});
test('readable change review and proposal include old and new dates without object text',()=>{
 const before=compileDraft(catalog,input()),after=compileDraft(catalog,moveCycle(catalog,input(),'2026-12-01','2026-11-30'));
 const changes=changeFields(before,after);expect(changes.find(c=>c.label==='Moved cycle dates').after).toBe('2026-12-01 → 2026-11-30');
 expect(reviewText(after)).toContain('2026-11-30 (moved from 2026-12-01)');expect(reviewText(after)).not.toContain('[object Object]');
});
test('all dated program templates share the same per-cycle edit behavior',()=>{
 for(const template of catalog.templates){const value=input();value.template_id=template.id;if(template.pattern==='finite_gift'){value.terms.gift_cycles=3;value.terms.auto_renew=false;}if(template.pattern==='try_before_buy')value.terms.style_fee_cents=2000;
 const next=moveCycle(catalog,value,'2026-12-01','2026-11-30');expect(compileDraft(catalog,next).cycle_preview[1].due_local_date).toBe('2026-11-30');expect(value.terms.cycle_moves).toBeUndefined();}
});
