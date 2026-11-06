import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {buildPlan} from '../backfill_publisher_subaccounts.mjs';

const sha = (a,b) => createHash('sha256').update(a+'\0'+b).digest('hex').slice(0,20);
const host = (a,b) => 'credited-'+a.toLowerCase().replace(/[^a-z0-9]+/g,'-')+'-'+sha(a,b)+'.aurekai.work';
const row = (a,b) => ['/people?who='+encodeURIComponent(a)+'&source='+encodeURIComponent(b)+'&profile=1','hosts/'+host(a,b)+'/index.html'];
const manifest = rows => rows.flatMap(x=>row(...x)).join('\0')+'\0';
const parent = source => ({label:source,publisher_name:source,url:'https://example.test/'+source});

test('two equal printed names stay in two publisher-owned subaccounts',()=>{
 const plan=buildPlan(manifest([['Alex Rivera','publisher-one'],['Alex Rivera','publisher-two']]),parent,
  name=>name==='publisher-one'?{subject:'institution:publisher:publisher-one',url:'https://publisher-one.aurekai.life/'}:null);
 expect(plan.parents.length).toBe(2);
 expect(plan.children.length).toBe(2);
 expect(new Set(plan.children.map(x=>x.account_subject)).size).toBe(2);
 expect(new Set(plan.children.map(x=>x.publisher_account_id)).size).toBe(2);
 expect(plan.parents.find(x=>x.source==='publisher-one').publisher_subject).toBe('institution:publisher:publisher-one');
 expect(plan.parents.find(x=>x.source==='publisher-two').publisher_subject).toBe('');
 for(const child of plan.children){
  expect(child.identity_state).toBe('unclaimed');
  expect(child.claimed_actor_subject).toBe('');
  expect(child.cannot_infer).toContain('authenticated account');
  expect(child.status).toBe('published');
 }
});

test('one publisher owns multiple subordinate profiles without duplicating its account',()=>{
 const plan=buildPlan(manifest([['Alex Rivera','publisher-one'],['Robin Shah','publisher-one']]),parent,()=>null);
 expect(plan.parents.length).toBe(1);
 expect(plan.children.length).toBe(2);
 expect(new Set(plan.children.map(x=>x.publisher_account_id)).size).toBe(1);
 expect(plan.children[0].source).toBe('publisher-one');
});

test('invalid, missing and colliding bindings fail rather than inheriting identity or authority',()=>{
 expect(()=>buildPlan('/people?who=Alex&source=publisher-one&profile=1\0hosts/evil.example/index.html\0',parent,()=>null)).toThrow('Unsafe');
 expect(()=>buildPlan(manifest([['Alex Rivera','publisher-one']]),()=>null,()=>null)).toThrow('Missing published');
 expect(()=>buildPlan(manifest([['Alex Rivera','publisher-one'],['Alex Rivera','publisher-one']]),parent,()=>null)).toThrow('Duplicate');
 expect(()=>buildPlan('incomplete',parent,()=>null)).toThrow('Incomplete');
});
