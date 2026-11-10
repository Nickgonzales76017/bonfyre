import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {prepareCaptureBatch,batchWrites,reviseCycles,adoptObservedPrice,comparisonRows,changeFields,reviewText,editHistory} from '../extension/workbench-actions.mjs';
import {observe,compileReferencedDraft} from '../extension/observation.mjs';
import {compileDraft,basketPreview} from '../extension/recurring-commerce.mjs';
const catalog=JSON.parse(readFileSync(new URL('../extension/catalog.json',import.meta.url),'utf8'));
const capture=(price='12.35',extra={})=>({url:'https://fixture.invalid/coffee?variant=a',title:'Coffee source',captured_at:'2026-10-15T00:00:00Z',fixture:true,selected_text:'Roasted on order.',term_snippets:['Delivery is quoted separately.'],structured:[{'@type':'Product',name:'Coffee',offers:{'@type':'Offer',price,priceCurrency:'USD'}}],...extra});
const input=(pattern='discovery_box')=>({program_id:'draft:behavior',template_id:catalog.templates.find(t=>t.pattern===pattern).id,owner_subject:'person:test',name:'Coffee proposal',sample:true,terms:{currency:'USD',price_cents:1235,shipping_cents:210,max_members:1,quantity_per_member:1,start_date:'2026-11-01',cadence:{unit:pattern==='replenishment'?'usage':'month',every:1},auto_renew:pattern!=='finite_gift',gift_cycles:pattern==='finite_gift'?3:undefined,cancellation:'Review before renewing.',return_policy:'Confirm with the seller.'},preferences:{no_repeats:true}});
test('batch preview separates exact duplicates, changed versions, different pages and errors without saving',async()=>{
  const old=await observe(capture(),catalog),before=structuredClone(old);
  const rows=await prepareCaptureBatch([{name:'same',value:capture()},{name:'newer date only',value:capture('12.35',{captured_at:'2026-10-16T00:00:00Z'})},{name:'change',value:capture('14.50')},{name:'same changed',value:capture('14.50')},{name:'other page',value:capture('12.35',{url:'https://fixture.invalid/other',canonical:old.captured.url})},{name:'broken',error:'Invalid file'}],catalog,[old]);
  expect(rows.map(r=>r.state)).toEqual(['unchanged','unchanged','new_version','unchanged','new_source','invalid']);
  expect(rows[1].observation.captured.captured_at).toBe(old.captured.captured_at);expect(rows[1].observation.id).toBe(old.id);expect(old).toEqual(before);
  const writes=batchWrites(rows,[2,4]);expect(writes).toHaveLength(2);expect(writes.every(w=>w.expectedRevision===0)).toBe(true);
  expect(()=>batchWrites(rows,[0])).toThrow();expect(()=>batchWrites(rows,[99])).toThrow();expect(()=>batchWrites(rows,[])).toThrow();
});
test('an observation wrapper is reparsed rather than trusting forged extracted prices',async()=>{
  const old=await observe(capture(),catalog);old.offers[0].price.usd_cents=1;
  const [row]=await prepareCaptureBatch([{value:old}],catalog);expect(row.observation.offers[0].price.usd_cents).toBe(1235);
  await expect(prepareCaptureBatch([],catalog)).rejects.toThrow();await expect(prepareCaptureBatch(Array(41).fill({}),catalog)).rejects.toThrow();
});
test('selected calendar actions alter the existing compiler and monthly budget, with immutable undo input',()=>{
  const before=input(),copy=structuredClone(before),initial=compileDraft(catalog,before),dates=initial.cycle_preview.slice(0,2).map(c=>c.due_local_date);
  const skipped=reviseCycles(catalog,before,dates,true),compiled=compileDraft(catalog,skipped);
  expect(before).toEqual(copy);expect(compiled.cycle_preview.filter(c=>c.state==='skip_requested')).toHaveLength(2);expect(compiled.cycle_preview.every(c=>c.effect==='none')).toBe(true);
  expect(basketPreview([compiled],2000).months).toHaveLength(4);expect(basketPreview([compileDraft(catalog,reviseCycles(catalog,skipped,dates,false))],2000).months).toHaveLength(6);
  expect(()=>reviseCycles(catalog,before,['2028-01-01'],true)).toThrow();expect(()=>reviseCycles(catalog,before,[],true)).toThrow();
});
test('a skipped prepaid gift retains its term cost and stock-triggered programs have no fake dates',()=>{
  const gift=input('finite_gift'),first=compileDraft(catalog,gift).cycle_preview[0].due_local_date;
  expect(basketPreview([compileDraft(catalog,reviseCycles(catalog,gift,[first],true))],10000)).toEqual(basketPreview([compileDraft(catalog,gift)],10000));
  expect(()=>reviseCycles(catalog,input('replenishment'),['2026-11-01'],true)).toThrow();
});
test('adopting a source price changes only the proposed price and refuses ranges or unknown currencies',async()=>{
  const source=await observe(capture('28.40'),catalog),original=input(),result=adoptObservedPrice(original,source,0);
  expect(result).toEqual({...original,terms:{...original.terms,price_cents:2840}});expect(original.terms.price_cents).toBe(1235);
  source.offers[0].range={};expect(()=>adoptObservedPrice(original,source,0)).toThrow();delete source.offers[0].range;source.offers[0].price.usd_cents=null;expect(()=>adoptObservedPrice(original,source,0)).toThrow();
});
test('comparison and readable output preserve sources and never claim an activated program',async()=>{
  const source=await observe(capture(),catalog),draft=compileReferencedDraft(catalog,input(),[source]),text=reviewText(draft,[source]);
  expect(comparisonRows([source])[0].offers[0].price).toBe('$12.35');expect(text).toContain('$14.45');expect(text).toContain(source.captured_body_sha256);expect(text).toContain('not an active subscription');expect(text).not.toContain('"stage_plan"');
  const newer=structuredClone(draft);newer.terms.price_cents=2000;expect(changeFields(draft,newer)).toEqual([{label:'Cycle price',before:'$12.35',after:'$20.00'}]);
});
test('undo and redo restore source selections with edits and a new branch discards redo',()=>{
  const history=editHistory({price:1,selected:['one']},3);history.record({price:2,selected:['two']});history.record({price:3,selected:['one','two']});
  expect(history.undo()).toEqual({price:2,selected:['two']});expect(history.canRedo).toBe(true);expect(history.redo().price).toBe(3);
  history.undo();history.record({price:4,selected:[]});expect(history.canRedo).toBe(false);expect(history.undo().price).toBe(2);expect(history.undo().price).toBe(1);expect(history.canUndo).toBe(false);
});
