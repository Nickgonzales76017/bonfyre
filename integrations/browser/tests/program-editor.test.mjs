import {test,expect} from 'bun:test';
import {usdCents,selectedReferences} from '../extension/program-editor.mjs';
import {compileReferencedDraft} from '../extension/observation.mjs';
import {readFileSync} from 'node:fs';
const catalog=JSON.parse(readFileSync(new URL('../extension/catalog.json',import.meta.url),'utf8'));
test('human dollar fields retain exact cents and reject ambiguous amounts',()=>{
  expect(usdCents('12.35')).toBe(1235);expect(usdCents('0.1')).toBe(10);expect(usdCents(' 100 ')).toBe(10000);
  for(const bad of ['-1','1e3','1,20','2.005','NaN','',1,Infinity])expect(()=>usdCents(bad)).toThrow();
});
test('selected sources retain references without copying their bodies',()=>{
  const a={id:'a',captured_body_sha256:'a'.repeat(64),captured:{url:'https://fixture.invalid/a',selected_text:'retained source'}},b={id:'b',captured_body_sha256:'b'.repeat(64),captured:{url:'https://fixture.invalid/b'}};
  expect(selectedReferences([{value:a},{value:b}],['b','a','b'])).toEqual([a,b]);
  expect(()=>selectedReferences([a],['missing'])).toThrow();
  const input={program_id:'draft:multi-source',template_id:catalog.templates[0].id,owner_subject:'person:test',name:'Reviewed sources',sample:true,terms:{currency:'USD',price_cents:1235,shipping_cents:0,max_members:3,quantity_per_member:1,start_date:'2026-11-01',cadence:{unit:'month',every:1},auto_renew:false,cancellation:'Cancel before renewal.',return_policy:'Review damaged goods.'},preferences:{}};
  const draft=compileReferencedDraft(catalog,input,selectedReferences([a,b],['a','b']));
  expect(draft.browser_evidence).toHaveLength(2);expect(JSON.stringify(draft.browser_evidence)).not.toContain('retained source');
  expect(draft.stage_plan.every(s=>s.state==='not_executed')).toBe(true);
});
test('source selection refuses a removed referenced capture',()=>{
  expect(()=>selectedReferences([],['observation:removed'])).toThrow('no longer available');
  expect(selectedReferences([],[])).toEqual([]);
  expect(()=>selectedReferences([{id:'bad',captured:{url:'x'}}],['bad'])).toThrow('retained identity');
});
