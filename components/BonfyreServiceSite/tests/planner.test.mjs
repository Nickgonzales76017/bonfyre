// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
import {test,expect,afterAll} from 'bun:test';
import {readFileSync,mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir,homedir} from 'node:os';
import {plannerFiles} from '../planner-material.mjs';
import {startingPlan,validatePlan,assessPlan,footprint,planReviewForm,planRequest,planBrief} from '../planner-model.mjs';
import {workerSource} from '../lead-worker.mjs';
import {syncLeads} from '../leads.mjs';
import {onLead} from '../../../packs/aurekai-work-samples/site/lead.mjs';

const repo=resolve(import.meta.dir,'../../..'),scratch=mkdtempSync(join(tmpdir(),'fixture-planner-'));
afterAll(()=>rmSync(scratch,{recursive:true,force:true}));
const input=JSON.parse(readFileSync(join(repo,'packs/aurekai-work-samples/site/fixture-planner.json'),'utf8'));
const {catalogue}=plannerFiles(input),form='planner-red3-fixtures',page='/red3-planner/';
test('planner packages exact selection shared with Opportunity Desk rather than a second source reader',()=>{
  const packed=plannerFiles(input);
  expect(packed.files['opportunity-selection.mjs']).toBeDefined();
  expect(packed.files[packed.asset_prefix+'opportunity-selection.mjs']).toBeDefined();
  expect(Buffer.from(packed.files['planner-client.mjs']).toString()).toContain('openSourceReview');
});
const plan=()=>startingPlan(catalogue,'compact','test-plan-001');
const body=p=>planRequest(p,catalogue,{form,page,name:'Planner test',email:'planner@example.invalid'});
const request=(data,origin='https://example.test',route='/api/lead/'+form)=>new Request('https://example.test'+route,{method:'POST',headers:{origin,'content-type':'application/json'},body:data});
let serial=0;
async function fixture(){const path=join(scratch,`worker-${serial++}.mjs`);writeFileSync(path,workerSource('aurekai-work-samples',{[form]:planReviewForm(catalogue,page)}));const worker=(await import(path)).default,values=new Map();return {worker,values,env:{LEADS:{get:async k=>values.get(k)??null,put:async(k,v)=>values.set(k,v)}}};}

test('selected option prices are recomputed from retained sources and a changed source invalidates the plan',()=>{
  const p=plan(),a=assessPlan(p,catalogue);
  expect(a.subtotal).toBe(116850);expect(a.lines.map(l=>l.product.variant_id)).toEqual(['10749','20684','11740','7631']);
  const other=structuredClone(input);other.products[0].price_cents+=100;
  expect(()=>validatePlan(p,plannerFiles(other).catalogue)).toThrow('different product');
  expect(()=>validatePlan({...p,subtotal:1},catalogue)).toThrow();
  expect(()=>validatePlan({...p,items:[{...p.items[0],price_cents:1}]},catalogue)).toThrow();
});

test('geometry checks rotation, collisions and bounds without inventing missing footprints',()=>{
  const p=plan();expect(assessPlan(p,catalogue).issues).toEqual([]);
  const table=p.items.find(i=>i.product==='table'),counter=p.items.find(i=>i.product==='counter');
  expect(footprint(catalogue.products.find(p=>p.id==='rack'),p.items[0])).toBeNull();
  expect(assessPlan(p,catalogue).questions.join(' ')).toContain('depth is missing');
  counter.x=table.x;counter.y=table.y;expect(assessPlan(p,catalogue).issues.join(' ')).toContain('overlaps');
  counter.x=15;expect(assessPlan(p,catalogue).issues.join(' ')).toContain('beyond');
  table.rotation=90;expect(footprint(catalogue.products.find(p=>p.id==='table'),table)).toMatchObject({w:2.5,h:5});
  p.budget=100;expect(assessPlan(p,catalogue).issues.join(' ')).toContain('exceeds');
});

test('invalid quantities, references, dates and dimensions cannot enter a shared or submitted plan',()=>{
  const p=plan();
  for(const bad of [{...p,width:0},{...p,depth:NaN},{...p,budget:-1},{...p,budget:100.1},{...p,deadline:'2026-02-30'},{...p,notes:'x'.repeat(1501)},{...p,items:[]},{...p,items:[p.items[0],p.items[0]]},{...p,items:[{...p.items[0],product:'invented'}]},{...p,items:[{...p.items[0],rotation:45}]}])expect(()=>validatePlan(bad,catalogue)).toThrow();
  expect(planBrief(p,catalogue)).toContain('Delivery, tax and any custom work are excluded');
});

test('generated intake rejects forged plans, cross-origin posts and oversized envelopes before storing anything',async()=>{
  const {worker,env,values}=await fixture();
  for(const bad of [{...plan(),catalogue:'a'.repeat(64)},{...plan(),items:[{...plan().items[0],product:'fake'}]},{...plan(),items:[{...plan().items[0],price_cents:1}]}]){
    const data=JSON.parse(body(plan()));data.fields[2].value=JSON.stringify(bad);
    const response=await worker.fetch(request(JSON.stringify(data)),env);expect(response.status).toBe(400);expect((await response.json()).receipt).toBeUndefined();
  }
  expect((await worker.fetch(request(body(plan()),'https://other.test'),env)).status).toBe(403);
  expect((await worker.fetch(request(body(plan()),'https://example.test','/api/lead'),env)).status).toBe(400);
  expect((await worker.fetch(request('x'.repeat(24001)),env)).status).toBe(413);
  expect(values.size).toBe(0);
});

test('retry receives the same receipt and a changed layout receives a new one',async()=>{
  const {worker,env,values}=await fixture();
  expect((await worker.fetch(request(body(plan())),{})).status).toBe(503);
  const one=await worker.fetch(request(body(plan())),env),receipt=(await one.json()).receipt;expect(one.status).toBe(201);expect(receipt).toMatch(/^lead:planner:[a-f0-9]{64}$/);
  const retry=await worker.fetch(request(body(plan())),env);expect(retry.status).toBe(200);expect((await retry.json()).receipt).toBe(receipt);
  const changed=plan();changed.items[0].x=3;const two=await worker.fetch(request(body(changed)),env);expect((await two.json()).receipt).not.toBe(receipt);
  expect([...values.keys()].filter(k=>k.startsWith('lead:'))).toHaveLength(2);
});

test('the complete customer brief reaches existing native lead and held-message owners once',async()=>{
  const {worker,env,values}=await fixture(),p=plan();p.notes='Please confirm the rack depth before ordering. Café pop-up.';p.deadline='2026-12-10';
  const response=await worker.fetch(request(body(p)),env),receipt=(await response.json()).receipt;
  const schemas=join(repo,'cmd/BonfyreCMS/content-types'),db=join(scratch,'review.db');
  const cms=args=>{const run=Bun.spawnSync([join(homedir(),'.bonfyre/bin/bonfyre-cms'),...args,'--db',db,'--schemas',schemas],{stdout:'pipe',stderr:'pipe'});if(run.exitCode!==0)throw Error(run.stderr.toString());const result=JSON.parse(run.stdout.toString().trim()||"{}");if(result.error)throw Error(result.error);return result;};
  for(const type of ['service_lead','outbound_message'])cms(['schema','apply',join(schemas,type+'.json')]);
  const options={sites:[{site_id:'aurekai-work-samples',pages_project:'local-test',business:{name:'Aurekai'},contact:{email:'operator@example.invalid'}}],cms,heldLeads:async()=>[{key:receipt,...JSON.parse(values.get(receipt))}],enrich:(_,lead)=>onLead(lead)};
  const result=await syncLeads(options);expect(result.errors).toEqual([]);expect(result.brought).toBe(1);expect(result.prepared).toBe(1);
  const lead=cms(['entry','list','service_lead','--ns','service-site']).data[0].attributes;
  expect(JSON.parse(JSON.parse(lead.fields_json).find(f=>f.label==='Fixture plan').value)).toEqual(p);
  expect(JSON.parse(lead.facts_json)).toMatchObject({listed_subtotal_cents:116850,supplier_contacted:false,quote_issued:false,order_placed:false});
  expect(lead.brief).toContain(p.notes);
  const message=cms(['entry','list','outbound_message','--ns','service-site']).data[0].attributes;
  expect(message.subject_line).toContain(p.title);expect(message.body).toContain('depth is missing');expect(message.body).not.toContain('service-site.fixture-plan.v1');expect(message.state).toBe('composed');
  expect((await syncLeads(options)).prepared).toBe(0);
});
