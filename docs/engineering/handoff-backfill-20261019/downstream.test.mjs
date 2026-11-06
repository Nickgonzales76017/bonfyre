import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {tsv,sections,programmes,buildDelivery,verifySources,sha} from './downstream.mjs';
import {createOperatorStore} from '../../../integrations/mcp/operator_store.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
test('TSV retains every field, including empty and quoted historical values',()=>{
 const rows=tsv('id\tstatus\tnote\na\t\tPARTIAL: original source\nb\t""\tretained\n');
 expect(rows[0]).toEqual({line:2,raw:'a\t\tPARTIAL: original source',fields:{id:'a',status:'',note:'PARTIAL: original source'}});
 expect(rows[1].fields.status).toBe('""');expect(()=>tsv('id\tid\na\tb')).toThrow();expect(()=>tsv('id\tstatus\na')).toThrow();
});
test('historical sections retain nested material and ignore headings inside a code fence',()=>{
 const input='# I. Source\n```text\n# fake heading\n```\n## Nested\nexact original\n# II. Receiver\nsecond';
 const rows=sections(input);expect(rows).toHaveLength(2);expect(rows[0].text).toContain('# fake heading');expect(rows[0].text).toContain('## Nested');expect(rows[1].start).toBe(7);
});
test('programme dependencies preserve all thirty identities and reject a cycle',()=>{
 const text=Array.from({length:30},(_,i)=>`maturity ${i+1} programme-${i+1}\n  class application\n  owner owner-${i+1}\n  entry -\n  depends ${i===0?'-':i}\n  stages inspect|configure|operate|correct\n  release_gate actual consequence\n  witness test-${i+1}`).join('\n');
 expect(programmes(text)).toHaveLength(30);expect(()=>programmes(text.replace('  depends -','  depends 30'))).toThrow(/cycle/);
});
test('changed source refuses the reviewed handoff',()=>{
 const dir=mkdtempSync(join(tmpdir(),'source-review-'));try{const path=join(dir,'source.md');writeFileSync(path,'first');const plan={sources:[{path,label:'fixture',sha256:sha('first')}]};verifySources(plan);writeFileSync(path,'second');expect(()=>verifySources(plan)).toThrow(/Source changed/);}finally{rmSync(dir,{recursive:true,force:true});}
});
test('current retained source batch keeps exact rows, detailed contracts and existing enrollment ownership',()=>{
 const plan=buildDelivery();expect(plan.counts).toEqual({programme:30,skill:388,platform:100,instrument:10,controller:129,'source-family':35,composition:12,'browser-compute':50,'protocol-family':43,'historical-feedback':5,'recovery-gap':1});
 expect(plan.items).toHaveLength(803);expect(new Set(plan.items.map(i=>i.goal.dedupe_key)).size).toBe(803);
 expect(plan.contracts).toContain('item x language x rendition');expect(plan.contracts).toContain('per-group/per-item/per-segment overrides');expect(plan.contracts).toContain('quantity');expect(plan.contracts).toContain('an already-populated editable receiving workspace');
 expect(plan.references.filter(r=>r.kind==='existing-enrollment-owner').map(r=>r.count)).toEqual([400,34]);
 for(const item of plan.items){const source=plan.sources.find(s=>s.label===item.source);expect(source).toBeDefined();expect(item.goal.details).toContain(item.unit_sha256);expect(item.goal.recurring).toBe(false);
  if(['skill','platform','controller','instrument'].includes(item.kind)){const raw=source.text.split('\n')[item.start-1];expect(item.goal.details).toContain(raw);}}
 const skill=plan.items.find(i=>i.kind==='skill'&&i.key==='business-growth/skills/sales-engineer/SKILL.md');expect(skill.goal.details).toContain('PARTIAL 2026-10-21');
 expect(plan.items.filter(i=>i.kind==='historical-feedback').map(i=>i.key)).toHaveLength(5);
 const bound={maxBytes:16*1024*1024},back=decode(encode(plan,bound),bound);expect(back.items.length).toBe(803);expect(back.sources.map(s=>s.sha256)).toEqual(plan.sources.map(s=>s.sha256));
 expect(sha(encode(buildDelivery(),bound))).toBe(sha(encode(plan,bound)));verifySources(plan);
});
test('full recovered batch enters the actual goal owner once, without any run or completion',()=>{
 const home=mkdtempSync(join(tmpdir(),'source-delivery-owner-')),store=createOperatorStore({home});
 try{const plan=buildDelivery(),goals=plan.items.map(i=>i.goal),first=store.importGoals(goals,{apply:true});expect(first.created).toBe(803);expect(first.goals.every(g=>g.state==='queued')).toBe(true);
 const again=store.importGoals(goals,{apply:true});expect(again.created).toBe(0);expect(again.unchanged).toBe(803);expect(again.goals.map(r=>r.id)).toEqual(first.goals.map(r=>r.id));
 }finally{store.close();rmSync(home,{recursive:true,force:true});}
});
