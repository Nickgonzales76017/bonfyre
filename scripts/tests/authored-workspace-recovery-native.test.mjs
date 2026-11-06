// Real isolated Auth/CMS/WebAddress acceptance. Requires the native Mac fixture
// binaries and the retained connected-instrument material; no live account is used.
import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../materialize_authored_workspace.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {createAuthoredWorkspace,addInstrument,productionPrefix,validateAuthoredWorkspace} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
import {inspectRecoveryMaterial,previewRecoveryWorkflow,commitRecoveryWorkflow,recoverySchema} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs';
import {validateRecoveryInstrument} from '../../cmd/BonfyreSurfaceScene/runtime/authored-instrument-validation.mjs';
import {evaluateDataChecks} from '../../cmd/BonfyreSurfaceScene/runtime/authored-data-checks.mjs';
import {verifyProductionDestination} from '../../integrations/browser/workspace-destinations.mjs';
import {prepareRecord,digest} from '../../integrations/browser/workspace-value.mjs';
import {encode,decode} from '../../integrations/browser/workspace-msgpack.mjs';

const sha=value=>createHash('sha256').update(value).digest('hex');
const digestValue=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
const validation={digestValue,validateInstrument:validateRecoveryInstrument};
const previous={BONFYRE_WORKSPACE_RELEASE:process.env.BONFYRE_WORKSPACE_RELEASE,BONFYRE_WORKSPACE_PACKAGES:process.env.BONFYRE_WORKSPACE_PACKAGES};
let temp,pack,f,binding,target;
beforeAll(async()=>{
  const fixed=process.env.BONFYRE_AUTHORED_RELEASE;
  if(fixed!==undefined){
    if(!/^[a-f0-9]{64}$/.test(fixed))throw Error('Invalid immutable authoring release.');
    // The release handoff supplies the exact frozen material root; preserve it.
    pack={id:fixed};
  }else{
    temp=mkdtempSync(join(tmpdir(),'authored-recovery-native-'));
    const packages=join(temp,'packages');
    pack=await materializeAuthoredWorkspace({root:packages});
    process.env.BONFYRE_WORKSPACE_PACKAGES=packages;
  }
  process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
  f=await workspaceFixture();
  publishSetups({records:authoredWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
  binding=await f.install('experience.connected-instruments.workspace','institution:workspace-team');
  target={context:binding.context,id:binding.id,release:pack.id,entry:'instruments/production/'};
},60000);
afterAll(async()=>{
  try{await f?.close();}finally{
    if(temp)rmSync(temp,{recursive:true,force:true});
    for(const [key,value]of Object.entries(previous)){if(value===undefined)delete process.env[key];else process.env[key]=value;}
  }
});

// A new literal contract, unrelated to the five archive contracts. Tables are
// deliberately empty: this acceptance fixture never installs customer inputs.
function exampleInput(key){
  const contract={schema:'bonfyre.data-checks-contract.v1',id:'exhibit-humidity-fixture',version:'test-v1',
    sources:[{id:'limits',role:'reference',kind:'tabular',witness_requirements:[{kind:'sheet_row'}]},{id:'observations',role:'observation',kind:'tabular',witness_requirements:[{kind:'sheet_row'}]}],
    entities:[{id:'specification',source:'limits',role:'reference',key:'crate_id'},{id:'reading',source:'observations',role:'observation',key:'crate_id'}],
    fields:[{id:'spec-id',entity:'specification',name:'crate_id',type:'string',required:true},{id:'maximum',entity:'specification',name:'humidity_max',type:'float64',required:true},{id:'reading-id',entity:'reading',name:'crate_id',type:'string',required:true},{id:'observed',entity:'reading',name:'humidity',type:'float64',required:true}],
    relations:[{id:'same-crate',left_entity:'specification',right_entity:'reading',threshold:1,candidate_strategy:{kind:'key_eq',left:{entity:'specification',field:'crate_id'},right:{entity:'reading',field:'crate_id'}}}],
    predicates:[{id:'humidity-at-most-limit',kind:'lte',lhs:{entity:'reading',field:'humidity'},rhs:{entity:'specification',field:'humidity_max'}}],
    closures:[{id:'review-ready',relation:'same-crate',when_all_pass:['humidity-at-most-limit'],status_on_pass:'closed',status_on_fail:'residual',human_label_on_pass:'inspection_draft'}],labels:[]};
  const body='Isolated example specification — café 🧪.\nCompare a supplied crate reading with its supplied limit. No customer input, provider request or operational effect is included.\n'+JSON.stringify(contract)+'\n';
  let production=addInstrument(createAuthoredWorkspace('Isolated recovery '+key),'checks',{label:'Inputs and checks'});
  production=addInstrument(production,'research',{label:'Exact original specification',role:'source'});
  production.description='Native acceptance fixture with empty production tables; not a recovered customer workflow.';
  const values={checks:{kind:'checks',title:'Example exhibit humidity checks',contract,tables:[],selected:null},research:{kind:'research',title:'Example source and constraints',sources:[{id:'fixture-source',title:'Synthetic acceptance specification',body,url:'',sha256:sha(body),coordinate:'UTF-16 offsets into exact original text'}],findings:[],selectedSource:'fixture-source',selectedFinding:null,tab:'sources'}};
  return [1,recoverySchema,{archive_sha256:sha('Synthetic native acceptance archive'),workflows:[{recovery_key:key,source:{conversation_id:'isolated-native-recovery-fixture',message_ids:['session:isolated-native-recovery-fixture:line:1:block:0'],source_sha256:sha(body)},production,records:production.panels.map(p=>({key:p.recordKey,value:values[p.kind]}))}]}];
}
function options(user=f.owner,targetStore=f.store(binding,user)){
  return {...validation,target,targetStore,verifyTarget:destination=>verifyProductionDestination(destination,{request:(url,opts)=>f.raw(url,user,opts)})};
}
async function review(key){
  const raw=exampleInput(key),material=await inspectRecoveryMaterial(decode(encode(raw)),validation),workflow=material.workflows[0],opts=options();
  return {raw,workflow,opts,plan:await previewRecoveryWorkflow(workflow,opts)};
}
async function exactReadback(workflow){
  const store=f.store(binding),rows=[];
  for(const change of workflow.changes){const row=await store.readVersion(change.key);expect(row?.revision).toBe(1);expect(row?.deleted).toBe(false);expect(row?.value).toEqual(change.value);rows.push(row);}
  const composition=rows.find(r=>r.key===workflow.productionKey);validateAuthoredWorkspace(composition.value,{saved:true});
  expect(composition.value.panels.map(p=>p.revision)).toEqual([1,1]);return {composition,rows};
}

test('materialized native studio imports checks and exact research in one closure and reopens it',async()=>{
  const r=await review('exact-reopen');expect(r.plan.state).toBe('ready');
  const saved=await commitRecoveryWorkflow(r.plan,r.opts);expect(saved.state).toBe('created');expect(saved.records).toHaveLength(3);
  const reopened=await exactReadback(r.workflow),research=reopened.rows.find(row=>row.value.kind==='research'),checks=reopened.rows.find(row=>row.value.kind==='checks');
  expect(research.value.sources[0].body).toBe(r.raw[2].workflows[0].records[1].value.sources[0].body);
  expect(reopened.composition.value.recoveryOrigin.source_sha256).toBe(sha(research.value.sources[0].body));
  expect(checks.value.tables).toEqual([]);const result=await evaluateDataChecks(checks.value);expect(result.summary.state).toBe('needs_input');expect(result.execution).toBe('not_run');
  const path='/apps/'+binding.id+'/'+target.entry+'?'+new URLSearchParams({record:reopened.composition.key,revision:'1'}),page=await f.raw(path);
  expect(page.status).toBe(200);expect(await page.text()).toContain('bonfyre-workspace-context');
  const retained=await previewRecoveryWorkflow(r.workflow,options());expect(retained.state).toBe('retained');
},60000);

test('current native account and read-only grants refuse recovery without leaking or creating rows',async()=>{
  const r=await review('account-boundary');
  for(const user of [f.other,f.reader]){
    await expect(commitRecoveryWorkflow(r.plan,options(user))).rejects.toThrow();
    // Exercise the native writer as well as destination admission.
    await expect(f.store(binding,user).saveLocalBatch(r.workflow.changes)).rejects.toThrow();
  }
  for(const change of r.workflow.changes)expect(await f.store(binding).readVersion(change.key)).toBeNull();
  const saved=await commitRecoveryWorkflow(r.plan,r.opts);expect(saved.state).toBe('created');
  await expect(f.store(binding,f.other).readVersion(saved.production.key)).rejects.toThrow();
  expect((await f.raw('/apps/'+binding.id+'/'+target.entry,f.other)).status).toBe(403);
  expect((await f.store(binding,f.reader).readVersion(saved.production.key)).value).toEqual(saved.production.value);
  await exactReadback(r.workflow);
},60000);

test('a lost acknowledgement after a real native batch is recovered and never writes a second copy',async()=>{
  const r=await review('lost-acknowledgement'),store=f.store(binding);let writes=0,committedRows;
  const losingAck={readVersion:key=>store.readVersion(key),saveLocalBatch:async changes=>{writes++;committedRows=await store.saveLocalBatch(changes);throw Error('Synthetic transport loss after the native commit');}};
  const opts=options(f.owner,losingAck),saved=await commitRecoveryWorkflow(r.plan,opts);
  expect(saved.state).toBe('recovered');expect(writes).toBe(1);expect(committedRows).toHaveLength(3);await exactReadback(r.workflow);
  const inspected=await inspectRecoveryMaterial(decode(encode(r.raw)),validation),workflow=inspected.workflows[0];expect(workflow.productionKey).toBe(r.workflow.productionKey);
  const again=await previewRecoveryWorkflow(workflow,opts);expect(again.state).toBe('retained');expect((await commitRecoveryWorkflow(again,opts)).state).toBe('retained');expect(writes).toBe(1);
  await exactReadback(workflow);
},60000);

test('a later native child revision becomes a recovery conflict without overwriting that work',async()=>{
  const r=await review('later-native-edit');await commitRecoveryWorkflow(r.plan,r.opts);
  const store=f.store(binding),child=r.workflow.production.panels.find(p=>p.kind==='research'),before=await store.readVersion(child.recordKey);
  await store.saveLocal(before.key,{...before.value,title:'A later independent review'},before.revision);
  const count=(await store.readLocal(productionPrefix)).length,conflict=await previewRecoveryWorkflow(r.workflow,r.opts);
  expect(conflict.state).toBe('conflict');expect(conflict.conflicts.some(c=>c.key===child.recordKey&&c.reason==='revision')).toBe(true);
  await expect(commitRecoveryWorkflow(conflict,r.opts)).rejects.toThrow();
  expect((await store.readVersion(child.recordKey)).revision).toBe(2);expect((await store.readVersion(child.recordKey)).value.title).toBe('A later independent review');
  expect((await store.readVersion(r.workflow.productionKey)).revision).toBe(1);expect((await store.readLocal(productionPrefix)).length).toBe(count);
},60000);
