import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {operateProgramSourceWork} from '../program-source-work.mjs';
import {operate,readProgramSourceContext} from '../program-workspace-owner.mjs';
import {run as compileOwner} from '../compile-program-fabric.mjs';
import {WebWork} from '../../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {WebRefusal} from '../../cmd/BonfyreSourceFrontier/tools/web_protocol.mjs';
import {encode,decode} from '../../integrations/browser/workspace-msgpack.mjs';

const repo=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const person={actor:'user:19',subject:'user:19',scope:'self'};
const hash=value=>createHash('sha256').update(value).digest('hex');
const types=['program_template','program_instance','semantic_slot','application_binding','program_binding','source_collection','program_requirement','funding_work_package'];
const keys={program_template:'template_id',program_instance:'instance_id',semantic_slot:'slot_id',application_binding:'binding_id',program_binding:'binding_id',source_collection:'collection_id',program_requirement:'requirement_id',funding_work_package:'package_id'};
const emptyAddress=argv=>({exitCode:0,stderr:'',stdout:JSON.stringify({candidate_kind:argv[argv.indexOf('--kind')+1],truth:'eligible_for_further_consideration',considered:0,eligible:0,returned:0,has_more:false,survivors:[]})});
const admitted={admissionEnv:{BONFYRE_EXTERNAL_READS_ENABLED:'1',BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN:'10'}};
const exactItem=()=>{const body='A retained fixture source about community learning.';return {id:hash('fixture-item'),url:'https://example.org/learning',title:'Community learning source',kind:'page',author:'Fixture author',publishedAt:'2026-10-01T00:00:00Z',body,bodySHA256:hash(body),truncated:false};};
function providerReply(spec){const raw='fixture-provider-reply';return {items:[exactItem()],refused:0,requestId:'provider-request',reportedCredits:null,rawSHA256:hash(raw),raw,observedAt:'2026-10-22T01:00:00Z',provider:spec.provider,tool:spec.provider==='exa'?'web_search_advanced_exa':'firecrawl_search',notices:[],noticesTruncated:false};}

async function fixture(check){
  const estate=mkdtempSync(join(tmpdir(),'program-source-work-')),dbPath=join(estate,'cms.db'),sourcePath=join(estate,'frontier.db');
  const db=new Database(dbPath),source=new Database(sourcePath);source.close();
  for(const type of types){
    const schema=JSON.parse(readFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),'utf8'));
    const columns=new Set(['namespace','created_at','updated_at','published_at','status',...Object.keys(schema.attributes)]);
    db.exec(`CREATE TABLE "${type}"(id INTEGER PRIMARY KEY,${[...columns].map(name=>`"${name}" ${schema.attributes[name]?.type==='integer'?'INTEGER':'TEXT'}`).join(',')},UNIQUE(namespace,"${keys[type]}"))`);
  }
  const packs=join(estate,'fixture.json');
  writeFileSync(packs,JSON.stringify([{program_id:'learning',name:'Learning together',archetype:'education',description:'Research and operate a learning program.',subject_kinds:['person','institution','household'],semantic_slots:['sources','mentoring'],capability_families:['source_intake'],grant_stack:['Education stream']} ]));
  compileOwner({verb:'load',estate,packs});
  const prepare=(binding=person)=>{
    const options={...binding,db:dbPath,addressIndex:join(estate,'address.db')};
    const review=operate({...options,operation:'review'},encode([1,{expectedBinding:binding,template_id:'learning'}]),{addressRunFn:emptyAddress});
    return operate({...options,operation:'prepare'},encode([1,{expectedBinding:binding,template_id:'learning',review_digest:review.review_digest}]),{addressRunFn:emptyAddress}).instance;
  };
  const instance=prepare();let effects=0,execute=async spec=>{effects++;return providerReply(spec);};
  const create=(path,options)=>new WebWork(path,{...options,execute:(...args)=>execute(...args)});
  const call=(operation,extra={},options={})=>operateProgramSourceWork({operation,binding:person,db,sourceFrontierDB:sourcePath,webConfig:admitted,createWebWork:create,
    ...options,request:{expectedBinding:options.binding??person,instance_id:options.instance??instance.instance_id,...extra}});
  const planning=async(extra={},options={})=>{
    const status=await call('source-status',{},options);
    return {collection_id:status.collection.collection_id,collection_revision:status.collection.revision,demand_id:status.collection.demands[0].id,request_id:'research-request-1',provider:'exa',mode:'public',query:'community learning sources',limit:3,characters:5000,...extra};
  };
  const start=(plan,options={})=>call('source-start',{job_id:plan.job.id,digest:plan.job.digest,revision:plan.job.revision},options);
  const owner=(binding=person)=>new WebWork(sourcePath,{...binding,execute:async()=>{throw Error('unexpected direct effect');},config:admitted});
  try{await check({db,dbPath,sourcePath,estate,instance,prepare,call,planning,start,create,owner,effects:()=>effects,setExecute:fn=>{execute=fn;}});}
  finally{db.close();rmSync(estate,{recursive:true,force:true});}
}

test('source status uses the real Program demands and does not write an association',()=>fixture(async f=>{
  const before=readProgramSourceContext(f.db,person,f.instance.instance_id);
  const status=await f.call('source-status');
  expect(status.binding).toEqual(person);expect(status.collection.collection_id).toBe(f.instance.instance_id+'-sources');
  expect(status.collection.demands).toEqual(before.source_demands);expect(status.collection.revision).toMatch(/^[a-f0-9]{64}$/);
  expect(status.web.admission.enabled).toBe(true);expect(status.runs).toEqual([]);expect(f.effects()).toBe(0);
  expect(f.db.query("SELECT count(*) n FROM sqlite_master WHERE name='_program_web_work'").get().n).toBe(0);
  expect(readProgramSourceContext(f.db,person,f.instance.instance_id)).toEqual(before);
}));

test('source projections and acknowledgement recovery work through a readonly CMS handle',()=>fixture(async f=>{
  const request=await f.planning();
  const broken=(path,options)=>{const owner=f.create(path,options),plan=owner.plan.bind(owner);owner.plan=input=>{plan(input);throw Error('fixture lost acknowledgement');};return owner;};
  await expect(f.call('source-plan',request,{createWebWork:broken})).rejects.toThrow('lost acknowledgement');
  const readonly=new Database(f.dbPath,{readonly:true,create:false});
  try{
    const status=await f.call('source-status',{}, {db:readonly});expect(status.runs[0].job_id).toBeNull();
    const recovered=await f.call('source-recover',{request_id:request.request_id},{db:readonly});expect(recovered.job.state).toBe('planned');
    expect((await f.call('source-read',{job_id:recovered.job.id},{db:readonly})).job).toEqual(recovered.job);
    expect(readonly.query('SELECT job_id FROM _program_web_work').get().job_id).toBeNull();
  }finally{readonly.close();}
  expect(f.effects()).toBe(0);
}));

test('plan, explicit execution, retained text review and exact reopen use the native owners',()=>fixture(async f=>{
  const before=readProgramSourceContext(f.db,person,f.instance.instance_id),request=await f.planning();
  const plan=await f.call('source-plan',request);
  expect(plan.job.state).toBe('planned');expect(f.effects()).toBe(0);
  expect(plan.job.binding).toEqual(person);expect(plan.relation).toMatchObject({job_id:plan.job.id,request_id:request.request_id,collection_id:request.collection_id,demand_id:request.demand_id});
  expect(plan.relation.href).toBe('/sources/web?'+new URLSearchParams({context:'self',job:plan.job.id}));
  const result=await f.start(plan);expect(result.job.state).toBe('completed');expect(f.effects()).toBe(1);expect(result.job.items).toHaveLength(1);
  const item=await f.call('source-item',{job_id:plan.job.id,item_id:result.job.items[0].id});
  expect(item.item.body).toBe(exactItem().body);expect(item.item.bodySHA256).toBe(hash(item.item.body));expect(item.item.preview).toBeUndefined();
  const reviewed=await f.call('source-decision',{job_id:plan.job.id,item_id:item.item.id,revision:item.item.revision,decision:'shortlist',notes:'Useful for this program.'});
  expect(reviewed.job.items[0]).toMatchObject({decision:'shortlist',notes:'Useful for this program.',revision:2});
  const reopened=await f.call('source-read',{job_id:plan.job.id});expect(reopened.job).toEqual(reviewed.job);
  const recovered=await f.call('source-recover',{request_id:request.request_id});expect(recovered.job).toEqual(reviewed.job);
  expect((await f.call('source-status')).runs).toHaveLength(1);
  // The bridge owns only references; source publication and requirement acceptance stay untouched.
  expect(readProgramSourceContext(f.db,person,f.instance.instance_id)).toEqual(before);
  expect(f.db.query('PRAGMA table_info(_program_web_work)').all().some(row=>/body|payload|output|state|spec/.test(row.name))).toBe(false);
}));

test('replaying plan or start uses one retained job and one provider dispatch',()=>fixture(async f=>{
  const request=await f.planning(),plan=await f.call('source-plan',request);
  expect((await f.call('source-plan',request)).job).toEqual(plan.job);
  const result=await f.start(plan),again=await f.start(plan);expect(again.job).toEqual(result.job);expect(f.effects()).toBe(1);
  expect((await f.call('source-plan',request)).job).toEqual(result.job);
  expect(f.db.query('SELECT count(*) n FROM _program_web_work').get().n).toBe(1);
  const owner=f.owner();try{expect(owner.list().jobs).toHaveLength(1);}finally{owner.close();}
}));

test('same request identity refuses a changed query before another plan or provider effect',()=>fixture(async f=>{
  const request=await f.planning();await f.call('source-plan',request);
  await expect(f.call('source-plan',{...request,query:'different source question'})).rejects.toThrow('already names different research');
  expect(f.effects()).toBe(0);expect(f.db.query('SELECT count(*) n FROM _program_web_work').get().n).toBe(1);
}));

test('collection changes invalidate a planned request before dispatch but retain its reopening',()=>fixture(async f=>{
  const request=await f.planning(),plan=await f.call('source-plan',request);
  f.db.query("UPDATE source_collection SET updated_at='new-owner-revision' WHERE collection_id=?").run(request.collection_id);
  await expect(f.call('source-plan',{...request,request_id:'new-request-1'})).rejects.toThrow('source collection changed');
  await expect(f.start(plan)).rejects.toThrow('source collection changed');
  expect((await f.call('source-read',{job_id:plan.job.id})).job.state).toBe('planned');expect(f.effects()).toBe(0);
}));

test('native request digest and item revisions reject stale changes',()=>fixture(async f=>{
  const plan=await f.call('source-plan',await f.planning());
  await expect(f.call('source-start',{job_id:plan.job.id,digest:'a'.repeat(64),revision:plan.job.revision})).rejects.toThrow('digest changed');
  await expect(f.call('source-start',{job_id:plan.job.id,digest:plan.job.digest,revision:plan.job.revision+1})).rejects.toThrow('changed after review');
  const result=await f.start(plan),item=result.job.items[0];
  const change={job_id:plan.job.id,item_id:item.id,revision:item.revision,decision:'shortlist',notes:'First review'};
  await f.call('source-decision',change);
  await expect(f.call('source-decision',{...change,decision:'discard'})).rejects.toThrow('reviewed in another tab');expect(f.effects()).toBe(1);
}));

test('native off and quota admission refuse execution while preserving the plan',()=>fixture(async f=>{
  const plan=await f.call('source-plan',await f.planning());
  await expect(f.start(plan,{webConfig:{admissionEnv:{BONFYRE_EXTERNAL_READS_ENABLED:'0'}}})).rejects.toThrow('switched off');
  await expect(f.start(plan,{webConfig:{admissionEnv:{BONFYRE_EXTERNAL_READS_ENABLED:'1',BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN:'1'}}})).rejects.toThrow('exceeds');
  expect((await f.call('source-read',{job_id:plan.job.id})).job.state).toBe('planned');expect(f.effects()).toBe(0);
}));

test('two concurrent starts preserve one native dispatch and hold no CMS transaction during execution',()=>fixture(async f=>{
  let entered,release;const started=new Promise(r=>entered=r),finish=new Promise(r=>release=r);let effects=0;
  f.setExecute(async spec=>{effects++;entered();await finish;return providerReply(spec);});
  const plan=await f.call('source-plan',await f.planning()),first=f.start(plan);await started;
  const second=await f.start(plan);expect(second.job.state).toBe('submitting');
  const other=new Database(f.dbPath);try{other.query("UPDATE funding_work_package SET updated_at='concurrent-author'").run();}finally{other.close();}
  release();expect((await first).job.state).toBe('completed');expect(effects).toBe(1);
}));

test('uncertain provider outcome is retained and repeated start cannot dispatch again',()=>fixture(async f=>{
  let effects=0;f.setExecute(async()=>{effects++;throw new WebRefusal('Fixture transport lost after dispatch.','transport',true);});
  const plan=await f.call('source-plan',await f.planning());
  expect((await f.start(plan)).job.state).toBe('uncertain');expect((await f.start(plan)).job.state).toBe('uncertain');
  expect((await f.call('source-recover',{request_id:'research-request-1'})).job.state).toBe('uncertain');expect(effects).toBe(1);
}));

test('a missing plan acknowledgement recovers native work before any dispatch',()=>fixture(async f=>{
  const request=await f.planning();let retainedId;
  const broken=(path,options)=>{const owner=f.create(path,options),plan=owner.plan.bind(owner);owner.plan=input=>{const job=plan(input);retainedId=job.id;throw Error('fixture acknowledgement interrupted');};return owner;};
  await expect(f.call('source-plan',request,{createWebWork:broken})).rejects.toThrow('acknowledgement interrupted');
  expect(f.db.query('SELECT job_id FROM _program_web_work').get().job_id).toBeNull();expect(f.effects()).toBe(0);
  const recovered=await f.call('source-recover',{request_id:request.request_id});expect(recovered.job.id).toBe(retainedId);expect(recovered.relation.job_id).toBe(retainedId);
  expect((await f.call('source-read',{job_id:retainedId})).job).toEqual(recovered.job);
  expect((await f.start(recovered)).job.state).toBe('completed');expect(f.effects()).toBe(1);
  expect(f.db.query('SELECT job_id FROM _program_web_work').get().job_id).toBe(retainedId);
}));

test('an intent interrupted before native planning can be recovered and retried with the same input',()=>fixture(async f=>{
  const request=await f.planning();const broken=(path,options)=>{const owner=f.create(path,options);owner.plan=()=>{throw Error('fixture stopped before plan');};return owner;};
  await expect(f.call('source-plan',request,{createWebWork:broken})).rejects.toThrow('stopped before plan');
  const recovered=await f.call('source-recover',{request_id:request.request_id});expect(recovered.job).toBeNull();expect(recovered.relation.job_id).toBeNull();
  const plan=await f.call('source-plan',request);expect(plan.job.state).toBe('planned');expect(f.effects()).toBe(0);
  expect(f.db.query('SELECT count(*) n FROM _program_web_work').get().n).toBe(1);
}));

test('expected-binding checks happen before source owner construction',()=>fixture(async f=>{
  let opened=0;const createWebWork=()=>{opened++;throw Error('must not open');};
  await expect(f.call('source-status',{expectedBinding:{actor:'user:20',subject:'user:20',scope:'self'}},{createWebWork})).rejects.toThrow('destination changed');
  await expect(f.call('source-status',{expectedBinding:{...person,extra:true}},{createWebWork})).rejects.toThrow('destination changed');expect(opened).toBe(0);
}));

test('a stale account in plan or start cannot construct or dispatch its native owner',()=>fixture(async f=>{
  const request=await f.planning(),plan=await f.call('source-plan',request);
  let opened=0;const createWebWork=()=>{opened++;throw Error('must not open');};
  const changed={actor:'user:20',subject:'user:20',scope:'self'};
  await expect(f.call('source-plan',{...request,expectedBinding:person},{binding:changed,createWebWork})).rejects.toThrow('destination changed');
  await expect(f.call('source-start',{job_id:plan.job.id,digest:plan.job.digest,revision:plan.job.revision,expectedBinding:person},{binding:changed,createWebWork})).rejects.toThrow('destination changed');
  expect(opened).toBe(0);expect(f.effects()).toBe(0);
}));

test('a stale collection review retains no native plan or Program research intent',()=>fixture(async f=>{
  const request=await f.planning();
  f.db.query("UPDATE source_collection SET updated_at='changed-before-plan' WHERE collection_id=?").run(request.collection_id);
  await expect(f.call('source-plan',request)).rejects.toThrow('collection changed');
  const owner=f.owner();try{expect(owner.list().jobs).toEqual([]);}finally{owner.close();}
  expect(f.db.query("SELECT count(*) n FROM sqlite_master WHERE name='_program_web_work'").get().n).toBe(0);expect(f.effects()).toBe(0);
}));

test('personal, organization, household and unrelated native jobs cannot cross Program relations',()=>fixture(async f=>{
  const plan=await f.call('source-plan',await f.planning());
  for(const binding of [{actor:'user:20',subject:'user:20',scope:'self'},{...person,subject:'institution:studio',scope:'institution'},{...person,subject:'household:home',scope:'household'}]){
    const instance=f.prepare(binding),options={binding,instance:instance.instance_id};
    expect((await f.call('source-status',{},options)).runs).toEqual([]);
    await expect(f.call('source-read',{job_id:plan.job.id},options)).rejects.toThrow('not attached');
    expect((await f.call('source-recover',{request_id:'research-request-1'},options)).job).toBeNull();
  }
  const owner=f.owner();let unrelated;
  try{unrelated=owner.plan({requestId:'outside-program-request',title:'Unrelated work',mode:'public',spec:{provider:'exa',operation:'search',query:'other query',objective:'Unrelated task',limit:1,characters:1000}});}finally{owner.close();}
  await expect(f.call('source-read',{job_id:unrelated.id})).rejects.toThrow('not attached');
  await expect(f.call('source-start',{job_id:unrelated.id,digest:unrelated.digest,revision:unrelated.revision})).rejects.toThrow('not attached');
  expect(f.effects()).toBe(0);
}));

test('another current organization actor can reopen its actual shared Program research',()=>fixture(async f=>{
  const binding={...person,subject:'institution:studio',scope:'institution'},instance=f.prepare(binding),options={binding,instance:instance.instance_id};
  const request=await f.planning({},options),plan=await f.call('source-plan',request,options);
  const second={binding:{...binding,actor:'user:20'},instance:instance.instance_id};
  const opened=await f.call('source-read',{job_id:plan.job.id},second);expect(opened.job.binding.actor).toBe('user:20');expect(opened.job.id).toBe(plan.job.id);
  expect(opened.relation.href).toContain('context=institution%3Astudio');
}));

test('lifecycle and preparation provenance block source writes without hiding retained reads',()=>fixture(async f=>{
  const plan=await f.call('source-plan',await f.planning());
  f.db.query("UPDATE program_instance SET status='published' WHERE instance_id=?").run(f.instance.instance_id);
  await expect(f.start(plan)).rejects.toThrow('lifecycle owner');
  expect((await f.call('source-read',{job_id:plan.job.id})).job.id).toBe(plan.job.id);
  f.db.exec('DELETE FROM _program_workspace_preparation');await expect(f.call('source-read',{job_id:plan.job.id})).rejects.toThrow('original preparation');expect(f.effects()).toBe(0);
}));

test('source result hash mismatch is refused before returning text to Programs',()=>fixture(async f=>{
  const plan=await f.call('source-plan',await f.planning()),result=await f.start(plan);
  const bad=(path,options)=>{const owner=f.create(path,options),item=owner.item.bind(owner);owner.item=(...args)=>({...item(...args),body:'substituted body'});return owner;};
  await expect(f.call('source-item',{job_id:plan.job.id,item_id:result.job.items[0].id},{createWebWork:bad})).rejects.toThrow('does not match');
}));

test('unknown demands, extra principal fields and unavailable database paths fail before retention',()=>fixture(async f=>{
  const request=await f.planning();
  await expect(f.call('source-plan',{...request,demand_id:'invented-demand'})).rejects.toThrow('declared by this program');
  await expect(f.call('source-plan',{...request,subject:'institution:other'})).rejects.toThrow('unsupported');
  await expect(f.call('source-status',{}, {sourceFrontierDB:join(f.estate,'missing.db')})).rejects.toThrow('source owner is unavailable');
  expect(f.db.query("SELECT count(*) n FROM sqlite_master WHERE name='_program_web_work'").get().n).toBe(0);expect(f.effects()).toBe(0);
}));

test('CLI returns bounded native MessagePack and refuses missing source configuration',()=>fixture(async f=>{
  const body=encode([1,{expectedBinding:person,instance_id:f.instance.instance_id}]);
  const result=Bun.spawnSync([process.execPath,join(repo,'scripts/program-source-work.mjs'),'--operation','source-status','--actor',person.actor,'--subject',person.subject,'--scope',person.scope,'--db',f.dbPath,'--source-db',f.sourcePath],{stdin:body,stdout:'pipe',stderr:'pipe'});
  expect(result.exitCode).toBe(0);expect(result.stderr.toString()).toBe('');
  const packet=decode(new Uint8Array(result.stdout));expect(packet[0]).toBe(1);expect(packet[1]).toBe(true);expect(packet[2].instance_id).toBe(f.instance.instance_id);
  const absent=Bun.spawnSync([process.execPath,join(repo,'scripts/program-source-work.mjs'),'--operation','source-status','--actor',person.actor,'--subject',person.subject,'--scope',person.scope,'--db',f.dbPath],{stdin:body,stdout:'pipe',stderr:'pipe'});
  expect(decode(new Uint8Array(absent.stdout))[1]).toBe(false);
}));
