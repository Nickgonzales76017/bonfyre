import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { operate, derivedSite, readProgramSourceContext } from '../program-workspace-owner.mjs';
import { compilePrograms, run as compileOwner } from '../compile-program-fabric.mjs';
import { encode, decode } from '../../integrations/browser/workspace-msgpack.mjs';

const repo=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const binding={actor:'user:19',subject:'user:19',scope:'self'};
const hash=value=>createHash('sha256').update(value).digest('hex');
const workspace='ws-'+('a'.repeat(40)),recordNamespace='workspace-'+('c'.repeat(40));
const referenceManifest='view\tlabel\tentry\tprefix\ttitle\tquery\nplan\tWorking plan\teditor/\tdraft:\ttitle\trecord=$key\n';
const referencePage='<html><head><meta name="bonfyre-workspace-store"></head><body>Program editor</body></html>';
const packageManifest='path\tsha256\tbytes\nCONTINUATIONS.tsv\t'+hash(referenceManifest)+'\t'+Buffer.byteLength(referenceManifest)+'\neditor/index.html\t'+hash(referencePage)+'\t'+Buffer.byteLength(referencePage)+'\n';
const release=hash(packageManifest);

const types=['program_template','program_instance','semantic_slot','application_binding','program_binding','source_collection','program_requirement','funding_work_package'];
const keys={program_template:'template_id',program_instance:'instance_id',semantic_slot:'slot_id',application_binding:'binding_id',program_binding:'binding_id',source_collection:'collection_id',program_requirement:'requirement_id',funding_work_package:'package_id'};
const emptyAddress=argv=>({exitCode:0,stderr:'',stdout:JSON.stringify({candidate_kind:argv[argv.indexOf('--kind')+1],truth:'eligible_for_further_consideration',considered:0,eligible:0,returned:0,has_more:false,survivors:[]})});

function fixture(check, overrides={}) {
  const estate=mkdtempSync(join(tmpdir(),'program-workspace-')),dbPath=join(estate,'cms.db');
  const db=new Database(dbPath);
  for(const type of types){
    const schema=JSON.parse(readFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),'utf8'));
    const columns=new Set(['namespace','created_at','updated_at','published_at','status',...Object.keys(schema.attributes)]);
    db.exec(`CREATE TABLE "${type}"(id INTEGER PRIMARY KEY,${[...columns].map(name=>`"${name}" ${schema.attributes[name]?.type==='integer'?'INTEGER':'TEXT'}`).join(',')},UNIQUE(namespace,"${keys[type]}"))`);
  }
  const packs=join(estate,'fixture.json');
  writeFileSync(packs,JSON.stringify([{program_id:'learning',name:'Learning together',archetype:'education',description:'Research and operate a learning program.',
    subject_kinds:['person','institution','household'],semantic_slots:['sources','mentoring'],capability_families:['source_intake'],grant_stack:['Education stream'],...overrides}]));
  expect(compileOwner({verb:'load',estate,packs}).templates).toBe(1);
  const workspacePackages=join(estate,'packages');mkdirSync(join(workspacePackages,release,'editor'),{recursive:true});
  writeFileSync(join(workspacePackages,release,'MANIFEST.tsv'),packageManifest);writeFileSync(join(workspacePackages,release,'CONTINUATIONS.tsv'),referenceManifest);
  writeFileSync(join(workspacePackages,release,'editor/index.html'),referencePage);
  const options={...binding,db:dbPath,addressIndex:join(estate,'address.db'),cmsBinary:process.execPath,workspacePackages};
  const call=(operation,request,extra={},hooks={})=>operate({...options,operation,...extra},request===undefined?undefined:encode([1,{...request,expectedBinding:{actor:extra.actor??binding.actor,subject:extra.subject??binding.subject,scope:extra.scope??binding.scope}}]),{addressRunFn:emptyAddress,...hooks});
  const prepare=(extra={})=>{const review=call('review',{template_id:'learning'},extra);return call('prepare',{template_id:'learning',review_digest:review.review_digest},extra);};
  try { return check({db,dbPath,estate,options,call,prepare}); }
  finally { db.close();rmSync(estate,{recursive:true,force:true}); }
}

test('catalog and review read canonical templates without writing private state',()=>fixture(({db,call})=>{
  const catalog=call('catalog');expect(catalog.binding).toEqual(binding);expect(catalog.templates[0].template_id).toBe('learning');expect(catalog.instances).toEqual([]);
  const review=call('review',{template_id:'learning'});
  expect(review.existing).toBe(false);expect(review.review_digest).toMatch(/^[0-9a-f]{64}$/);
  expect(review.instance_id).toBe(derivedSite(binding)+'-learning');
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_preparation'").get().n).toBe(0);
}));

test('preparation uses the real compiler and leaves all derived rows private',()=>fixture(({db,prepare,call})=>{
  const result=prepare();expect(result.created).toBe(true);expect(result.instance.owner_subject).toBe(binding.subject);expect(result.instance.status).toBe('draft');
  expect(result.instance.bindings.map(row=>[row.program_role,row.lowered_by])).toEqual([['mentoring',''],['sources','PublicationFeed']]);
  expect(result.instance.requirements).toHaveLength(2);expect(result.instance.requirements.every(row=>row.state==='open')).toBe(true);
  expect(result.instance.funding[0]).toMatchObject({funding_program:'Education stream',scope:[],cost_codes:[],status:'draft'});
  for(const type of types.filter(type=>type!=='program_template'))expect(db.query('SELECT COUNT(*) n FROM '+type+" WHERE status<>'draft'").get().n).toBe(0);
  expect(db.query("SELECT COUNT(*) n FROM program_template WHERE status='published'").get().n).toBe(1);
  expect(JSON.parse(db.query('SELECT source_selector FROM source_collection').get().source_selector)).toMatchObject({source_refs:[],resolution_state:'no_indexed_candidate'});
  expect(call('read',undefined,{instance:result.instance.instance_id}).instances[0]).toEqual(result.instance);
}));

test('self, organization and household records remain separated without legacy adoption',()=>fixture(({db,prepare,call})=>{
  db.transaction(()=>compilePrograms(db,{subject:'person:nick',siteId:'nick',programs:['learning'],addressIndex:'/fixture/index',addressRunFn:emptyAddress}))();
  const personal=prepare(),org=prepare({subject:'institution:studio',scope:'institution'}),home=prepare({subject:'household:home',scope:'household'});
  expect(new Set([personal.instance.instance_id,org.instance.instance_id,home.instance.instance_id]).size).toBe(3);
  expect(call('read').instances.map(row=>row.instance_id)).toEqual([personal.instance.instance_id]);
  expect(call('read',undefined,{subject:'institution:studio',scope:'institution'}).instances.map(row=>row.instance_id)).toEqual([org.instance.instance_id]);
  expect(()=>call('read',undefined,{instance:org.instance.instance_id})).toThrow('unavailable in this workspace');
  expect(db.query("SELECT owner_subject,status FROM program_instance WHERE instance_id='nick-learning'").get()).toEqual({owner_subject:'person:nick',status:'published'});
}));

test('replay preserves completed requirements, funding scopes and all logical timestamps',()=>fixture(({db,prepare})=>{
  prepare();db.exec("UPDATE program_requirement SET state='met'; UPDATE funding_work_package SET scope='[\"authored scope\"]',cost_codes='[\"cost-a\"]'");
  for(const type of types)db.exec('UPDATE '+type+" SET updated_at='unchanged'");
  // The template row's changed metadata is itself part of its exact identity.
  // Restore only that row to its original capture before checking replay.
  const receipt=db.query('SELECT * FROM _program_workspace_preparation').get();
  const template=db.query('SELECT created_at FROM program_template').get();
  db.query('UPDATE program_template SET updated_at=?').run(template.created_at);
  const result=prepare();expect(result.created).toBe(false);expect(result.instance.requirements.every(row=>row.state==='met')).toBe(true);
  expect(result.instance.funding[0].scope).toEqual(['authored scope']);
  for(const type of types.filter(type=>type!=='program_template'))expect(db.query('SELECT COUNT(*) n FROM '+type+" WHERE updated_at<>'unchanged'").get().n).toBe(0);
  expect(db.query('SELECT * FROM _program_workspace_preparation').get()).toEqual(receipt);
}));

test('template drift refuses the stale review before any creation',()=>fixture(({db,call})=>{
  const review=call('review',{template_id:'learning'});db.query('UPDATE program_template SET description=?').run('Changed requirement');
  expect(()=>call('prepare',{template_id:'learning',review_digest:review.review_digest})).toThrow('template or destination changed');
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_preparation'").get().n).toBe(0);
}));

test('a changed template cannot silently recompile an existing prepared program',()=>fixture(({db,prepare,call})=>{
  const before=prepare();db.query('UPDATE program_template SET semantic_slots=?').run(JSON.stringify(['sources','work']));
  expect(()=>prepare()).toThrow('earlier template');
  expect(call('read',undefined,{instance:before.instance.instance_id}).instances[0].bindings).toEqual(before.instance.bindings);
}));

test('a second authorized actor reopens the same organization instance',()=>fixture(({db,prepare})=>{
  const a=prepare({subject:'institution:studio',scope:'institution'}),b=prepare({actor:'user:20',subject:'institution:studio',scope:'institution'});
  expect(a.instance.instance_id).toBe(b.instance.instance_id);expect(b.created).toBe(false);
  expect(db.query('SELECT actor FROM _program_workspace_preparation').get().actor).toBe('user:19');
}));

test('binding changes, unsupported fields and cross-account overrides are refused',()=>fixture(({options,call,db})=>{
  const request={template_id:'learning',expectedBinding:binding};
  expect(()=>operate({...options,operation:'review',subject:'user:20'},encode([1,request]))).toThrow('account and destination');
  expect(()=>operate({...options,operation:'review',subject:'institution:studio',scope:'institution'},encode([1,request]))).toThrow('destination changed');
  expect(()=>operate({...options,operation:'review'},encode([1,{...request,site_id:'nick'}]))).toThrow('unsupported');
  expect(()=>call('catalog',{template_id:'learning'})).toThrow('does not accept');
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
}));

test('same-scope legacy identity and missing preparation receipts are not adopted',()=>fixture(({db,prepare})=>{
  db.transaction(()=>compilePrograms(db,{subject:binding.subject,siteId:derivedSite(binding),programs:['learning'],addressIndex:'/fixture/index',addressRunFn:emptyAddress}))();
  expect(()=>prepare()).toThrow('original owner');
  expect(db.query('SELECT status FROM program_instance').get().status).toBe('published');
}));

test('orphaned child identities cannot contribute accepted state to a new program',()=>fixture(({db,prepare})=>{
  const id=derivedSite(binding)+'-learning';
  db.query("INSERT INTO program_requirement(namespace,requirement_id,instance_id,statement,state,status) VALUES('root',?,'different-program','other owner acceptance','met','published')").run(id+'-needs-source-intake');
  expect(()=>prepare()).toThrow('already occupies');
  expect(db.query('SELECT state,instance_id FROM program_requirement').get()).toEqual({state:'met',instance_id:'different-program'});
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
  db.exec('DELETE FROM program_requirement');
  db.query("INSERT INTO program_requirement(namespace,requirement_id,instance_id,statement,state,status) VALUES('root','unplanned-child',?,'retained orphan','met','published')").run(id);
  expect(()=>prepare()).toThrow('missing program');
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
}));

test('compiler failure rolls back private instance and preparation receipt together',()=>fixture(({db,prepare})=>{
  db.exec('DROP TABLE program_binding');expect(()=>prepare()).toThrow();
  expect(db.query('SELECT COUNT(*) n FROM program_instance').get().n).toBe(0);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_preparation'").get().n).toBe(0);
}));

test('declared subject kinds and unpublished templates gate preparation',()=>fixture(({db,call})=>{
  db.query('UPDATE program_template SET subject_kinds=?').run(JSON.stringify(['institution']));
  expect(()=>call('review',{template_id:'learning'})).toThrow('kind of workspace');
  db.exec("UPDATE program_template SET status='draft'");expect(call('catalog').templates).toEqual([]);
  expect(()=>call('review',{template_id:'learning'},{subject:'institution:studio',scope:'institution'})).toThrow('unavailable');
}));

test('other CMS namespaces cannot replace templates or scoped child records',()=>fixture(({db,prepare,call})=>{
  const result=prepare(),id=result.instance.instance_id;
  db.query("INSERT INTO program_requirement(namespace,requirement_id,instance_id,statement,state,status) VALUES('foreign','foreign-requirement',?,'private foreign material','met','published')").run(id);
  db.query("INSERT INTO program_instance(namespace,instance_id,template_id,owner_subject,site_id,status) VALUES('foreign','foreign-instance','learning',?,'foreign-site','published')").run(binding.subject);
  expect(call('read').instances).toHaveLength(1);
  expect(call('read',undefined,{instance:id}).instances[0].requirements.some(row=>row.statement==='private foreign material')).toBe(false);
  expect(()=>call('read',undefined,{instance:'foreign-instance'})).toThrow('unavailable');
}));

test('the legacy compiler entry still publishes declarations and preserves authored owner state',()=>fixture(({db,estate})=>{
  db.query('UPDATE program_template SET capability_families=?').run('[]');
  const args={verb:'compile',estate,subject:'person:nick',siteId:'nick',programs:['learning','missing']};
  const initial=compileOwner(args);expect(initial.compiled).toHaveLength(1);expect(initial.no_such_template).toEqual(['missing']);
  expect(db.query("SELECT status FROM program_instance WHERE instance_id='nick-learning'").get().status).toBe('published');
  db.exec("UPDATE program_requirement SET state='met'; UPDATE semantic_slot SET ordinal=33,extent='authored'; UPDATE funding_work_package SET scope='[\"preserved\"]'");
  compileOwner(args);
  expect(db.query("SELECT COUNT(*) n FROM program_requirement WHERE state='met'").get().n).toBe(1);
  expect(db.query("SELECT COUNT(*) n FROM semantic_slot WHERE ordinal=33 AND extent='authored'").get().n).toBe(2);
  expect(db.query('SELECT scope FROM funding_work_package').get().scope).toBe('["preserved"]');
}));

test('CLI accepts WebAddress empty instance argument and returns only bounded MessagePack',()=>fixture(({dbPath})=>{
  const script=join(repo,'scripts/program-workspace-owner.mjs');
  const args=[process.execPath,script,'--operation','catalog','--actor','user:19','--subject','user:19','--scope','self','--db',dbPath,'--instance',''];
  const result=Bun.spawnSync(args,{stdin:new Uint8Array()});expect(result.stderr.toString()).toBe('');expect(result.exitCode).toBe(0);
  const packet=decode(result.stdout);expect(packet[0]).toBe(1);expect(packet[1]).toBe(true);expect(packet[2].templates).toHaveLength(1);
  const refused=Bun.spawnSync([...args,'--db',dbPath],{stdin:new Uint8Array()});expect(refused.exitCode).toBe(3);
  expect(decode(refused.stdout)).toMatchObject([1,false,{code:'invalid_arguments'}]);
}));


const changeRequest=(instance,change,request_id='request-1')=>({instance_id:instance.instance_id,request_id,expected_revision:instance.revision,change});
const current=(call,id)=>call('read',undefined,{instance:id}).instances[0];
function nativeRecord({subject=binding.subject,scope=binding.scope,key='draft:learning:1',revision=3,recordBinding=workspace}={}) {
  const context=[1,recordBinding,subject,scope,'program-pack',release,'editor/','program',recordNamespace];
  return {exitCode:0,stdout:encode([1,context,[key,revision,encode([2,{title:'Workshop plan',content:'Retained editor material'},[]])],[[context,'Program editor']]])};
}
const linkChange=(instance,extra={})=>({kind:'work-link',target_kind:'requirement',target_id:instance.requirements[0].requirement_id,
  workspace_binding:workspace,record_key:'draft:learning:1',record_revision:3,label:'Workshop plan',...extra});

test('requirement planning is durable, revision checked and leaves owner acceptance unchanged',()=>fixture(({db,call,prepare})=>{
  const initial=prepare().instance,id=initial.instance_id,requirement=initial.requirements[0].requirement_id;
  db.query('UPDATE program_requirement SET state=?,evidence_ref=?,satisfied_by=? WHERE requirement_id=?').run('met','native:evidence:accepted','NativeOwner:receipt',requirement);
  const before=current(call,id),request=changeRequest(before,{kind:'requirement-plan',requirement_id:requirement,deadline:'2026-11-01'});
  const changed=call('change',request);
  expect(changed.instance.revision).not.toBe(before.revision);
  expect(changed.instance.requirements.find(row=>row.requirement_id===requirement)).toMatchObject({deadline:'2026-11-01',state:'met',evidence_ref:'native:evidence:accepted',satisfied_by:'NativeOwner:receipt'});
  expect(changed.receipt).toMatchObject({request_id:'request-1',kind:'requirement-plan',before_revision:before.revision,after_revision:changed.instance.revision});
  expect(changed.replayed).toBe(false);expect(current(call,id)).toEqual(changed.instance);
  const recovered=call('change-receipt',{instance_id:id,request_id:'request-1'});
  expect(recovered.found).toBe(true);expect(recovered.receipt).toEqual(changed.receipt);expect(recovered.instance).toEqual(changed.instance);
  const replayed=call('change',request);expect(replayed.replayed).toBe(true);expect(replayed.receipt).toEqual(changed.receipt);
  expect(db.query('SELECT COUNT(*) n FROM _program_workspace_change').get().n).toBe(1);
}));

test('stale snapshots and reused request identities cannot overwrite current configuration',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance,id=before.instance_id,requirement=before.requirements[0].requirement_id;
  const request=changeRequest(before,{kind:'requirement-plan',requirement_id:requirement,deadline:'2026-11-01'});
  const first=call('change',request),later=call('change',changeRequest(first.instance,{...request.change,deadline:'2026-11-02'},'request-2'));
  expect(()=>call('change',{...request,request_id:'request-stale'})).toThrow('changed in another view');
  expect(()=>call('change',{...request,change:{...request.change,deadline:'2026-11-03'}})).toThrow('already used');
  const replay=call('change',request);expect(replay.receipt).toEqual(first.receipt);expect(replay.instance).toEqual(later.instance);
  expect(call('change-receipt',{instance_id:id,request_id:'absent'})).toMatchObject({found:false,receipt:null,instance:later.instance});
  expect(db.query('SELECT COUNT(*) n FROM _program_workspace_change').get().n).toBe(2);
}));

test('configuration writes reject identity overrides and lifecycle owner fields',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance,requirement=before.requirements[0].requirement_id;
  for(const change of [
    {kind:'requirement-plan',requirement_id:requirement,deadline:'2026-02-30'},
    {kind:'requirement-plan',requirement_id:requirement,deadline:'2026-12-01',state:'met'},
    {kind:'requirement-plan',requirement_id:requirement,deadline:'',evidence_ref:'fabricated'},
    {kind:'complete',requirement_id:requirement},
  ])expect(()=>call('change',changeRequest(before,change))).toThrow();
  expect(()=>call('change',changeRequest(before,{kind:'requirement-plan',requirement_id:'different-program-requirement',deadline:''}))).toThrow('from this program');
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_change'").get().n).toBe(0);
  expect(current(call,before.instance_id)).toEqual(before);
}));

test('funding declarations enforce same-program distinctness without finance acceptance',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance,[first,second]=before.funding;
  const changed=call('change',changeRequest(before,{kind:'funding-plan',package_id:first.package_id,scope:['Community workshop'],cost_codes:['TRAIN-01']}));
  expect(changed.instance.funding[0]).toMatchObject({scope:['Community workshop'],cost_codes:['TRAIN-01'],status:'draft'});
  expect(changed.instance.requirements.filter(row=>row.kind==='funding_scope').every(row=>row.state==='open')).toBe(true);
  expect(()=>call('change',changeRequest(changed.instance,{kind:'funding-plan',package_id:second.package_id,scope:['community   workshop'],cost_codes:['OTHER']},'overlap-scope'))).toThrow('already belongs');
  expect(()=>call('change',changeRequest(changed.instance,{kind:'funding-plan',package_id:second.package_id,scope:['Different work'],cost_codes:['train-01']},'overlap-cost'))).toThrow('already belongs');
  expect(()=>call('change',changeRequest(changed.instance,{kind:'funding-plan',package_id:second.package_id,scope:['One','one'],cost_codes:[]},'duplicate'))).toThrow('must be distinct');
  expect(current(call,before.instance_id)).toEqual(changed.instance);
  const next=call('change',changeRequest(changed.instance,{kind:'funding-plan',package_id:second.package_id,scope:['Separate mentoring'],cost_codes:['MENTOR-01']},'funding-2'));
  expect(next.instance.funding[1].scope).toEqual(['Separate mentoring']);
  expect(db.query('SELECT COUNT(*) n FROM _program_workspace_change').get().n).toBe(2);
},{grant_stack:['Education stream','Community stream']}));

test('private local source refresh preserves authored selection and never closes requirements',()=>fixture(({db,call,prepare})=>{
  const initial=prepare().instance,id=initial.instance_id,selector={...initial.source_collection.source_selector,source_refs:['authored-source']};
  db.query('UPDATE source_collection SET source_selector=? WHERE collection_id=?').run(JSON.stringify(selector),initial.source_collection.collection_id);
  const before=current(call,id);let ownerCalls=0;
  const addressRunFn=argv=>{
    ownerCalls++;const kind=argv[argv.indexOf('--kind')+1],survivors=kind==='provider'?[{identity:'provider:available',owner_ref:'Provider:available',exact_verifier:'Provider:registry',lowerer_ref:'Provider:operation',dependency_refs:[]}]:[];
    return {exitCode:0,stdout:JSON.stringify({candidate_kind:kind,truth:'eligible_for_further_consideration',considered:survivors.length,eligible:survivors.length,returned:survivors.length,has_more:false,survivors})};
  };
  const changed=call('change',changeRequest(before,{kind:'sources-refresh'}),{},{addressRunFn});
  expect(ownerCalls).toBeGreaterThan(0);expect(changed.instance.source_collection.status).toBe('draft');
  expect(changed.instance.source_collection.source_selector).toMatchObject({source_refs:['authored-source'],candidate_refs:['provider:available'],resolution_state:'owner_verification_required'});
  expect(changed.instance.source_demands.map(row=>row.id)).toEqual(['operation:source_intake','produces:source_intake']);
  expect(changed.instance.requirements).toEqual(before.requirements);expect(changed.receipt.result.requirements_satisfied).toBe(0);
  expect(changed.receipt.result.owner).toBe('AddressPlane');
}));

test('local resolver refusal rolls back configuration and receipt atomically',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance;
  expect(()=>call('change',changeRequest(before,{kind:'sources-refresh'}),{},{addressRunFn:()=>({exitCode:1,stderr:'owner unavailable'})})).toThrow('resolution failed');
  expect(current(call,before.instance_id)).toEqual(before);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_change'").get().n).toBe(0);
}));

test('saved-work links are exact native references and unlink never removes saved content',()=>fixture(({db,call,prepare,dbPath})=>{
  const before=prepare().instance;let nativeCalls=0;
  const cmsRunFn=argv=>{
    nativeCalls++;expect(argv.slice(1,4)).toEqual(['pack-workspace','--operation','continuation']);
    for(const [flag,value]of [['--binding',workspace],['--actor',binding.actor],['--subject',binding.subject],['--scope',binding.scope],['--db',dbPath],['--prefix','draft:learning:1']])expect(argv[argv.indexOf(flag)+1]).toBe(value);
    return nativeRecord();
  };
  const request=changeRequest(before,linkChange(before)),linked=call('change',request,{},{cmsRunFn});
  expect(nativeCalls).toBe(1);expect(linked.instance.work_links).toHaveLength(1);
  const link=linked.instance.work_links[0];
  expect(link).toMatchObject({target_kind:'requirement',target_id:before.requirements[0].requirement_id,workspace_binding:workspace,record_key:'draft:learning:1',record_revision:3,release,label:'Workshop plan',linked_by:binding.actor});
  expect(link.record_digest).toMatch(/^[a-f0-9]{64}$/);expect(link.href).toContain('/_views?key=draft%3Alearning%3A1&revision=3&release=');
  expect(linked.instance.requirements).toEqual(before.requirements);expect(linked.receipt.result.views).toEqual(['plan']);
  expect(call('change',request,{},{cmsRunFn}).replayed).toBe(true);expect(nativeCalls).toBe(1);
  const unlinked=call('change',changeRequest(linked.instance,{kind:'work-unlink',link_id:link.link_id},'unlink-1'));
  expect(unlinked.instance.work_links).toEqual([]);expect(nativeCalls).toBe(1);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name LIKE '_workspace_%'").get().n).toBe(0);
}));

test('saved-work attachment rejects another destination, stale revisions and unowned targets',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance,request=changeRequest(before,linkChange(before));
  expect(()=>call('change',request,{},{cmsRunFn:()=>nativeRecord({subject:'user:20'})})).toThrow('different workspace');
  expect(()=>call('change',request,{},{cmsRunFn:()=>nativeRecord({revision:4})})).toThrow('saved record changed');
  expect(()=>call('change',request,{},{cmsRunFn:()=>({exitCode:4,stdout:new Uint8Array()})})).toThrow('could not reopen');
  expect(()=>call('change',{...request,change:{...request.change,target_id:'unowned'}},{},{cmsRunFn:()=>{throw Error('must not call native owner');}})).toThrow('from this program');
  expect(current(call,before.instance_id)).toEqual(before);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_link' OR name='_program_workspace_change'").get().n).toBe(0);
}));

test('change readback and receipts stay within current account, organization and actor',()=>fixture(({db,call,prepare})=>{
  const personal=prepare().instance,organization=prepare({subject:'institution:studio',scope:'institution'}).instance;
  const changed=call('change',changeRequest(personal,{kind:'requirement-plan',requirement_id:personal.requirements[0].requirement_id,deadline:'2026-12-01'}));
  expect(()=>call('change-receipt',{instance_id:personal.instance_id,request_id:'request-1'},{subject:'institution:studio',scope:'institution'})).toThrow('unavailable');
  expect(()=>call('change',changeRequest(organization,{kind:'sources-refresh'}))).toThrow('unavailable');
  const orgRequest=changeRequest(organization,{kind:'requirement-plan',requirement_id:organization.requirements[0].requirement_id,deadline:'2026-12-02'});
  call('change',orgRequest,{subject:'institution:studio',scope:'institution'});
  expect(()=>call('change-receipt',{instance_id:organization.instance_id,request_id:'request-1'},{actor:'user:20',subject:'institution:studio',scope:'institution'})).toThrow('another account actor');
  expect(current(call,personal.instance_id)).toEqual(changed.instance);
}));

test('reads and missing receipt lookup do not create schema, and legacy instances cannot mutate',()=>fixture(({db,call,prepare,options})=>{
  const prepared=prepare().instance;
  expect(call('change-receipt',{instance_id:prepared.instance_id,request_id:'unknown'})).toMatchObject({found:false,receipt:null});
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name IN ('_program_workspace_change','_program_workspace_link')").get().n).toBe(0);
  db.exec('DELETE FROM _program_workspace_preparation');
  expect(()=>call('change',changeRequest(prepared,{kind:'requirement-plan',requirement_id:prepared.requirements[0].requirement_id,deadline:''}))).toThrow('original preparation');
  expect(current(call,prepared.instance_id)).toEqual(prepared);
}));


test('the shared source bridge guard reuses private preparation and current template ownership',()=>fixture(({db,call,prepare})=>{
  const before=prepare().instance;
  const read=readProgramSourceContext(db,binding,before.instance_id,{write:true});
  expect(read.instance).toEqual(before);expect(read.source_collection).toEqual(before.source_collection);expect(read.source_demands).toEqual(before.source_demands);
  expect(()=>readProgramSourceContext(db,{actor:'user:20',subject:'user:20',scope:'self'},before.instance_id)).toThrow('unavailable');
  db.query('UPDATE program_template SET description=?').run('A changed source contract');
  const changed=current(call,before.instance_id);expect(changed.revision).not.toBe(before.revision);
  expect(()=>readProgramSourceContext(db,binding,before.instance_id,{write:true})).toThrow('template changed');
  expect(readProgramSourceContext(db,binding,before.instance_id).instance).toEqual(changed);
  db.exec('DELETE FROM _program_workspace_preparation');
  expect(()=>readProgramSourceContext(db,binding,before.instance_id)).toThrow('original preparation');
}));

test('an unavailable pinned saved-work executable refuses without retaining a reference',()=>fixture(({db,call,prepare,estate})=>{
  const before=prepare().instance;
  expect(()=>call('change',changeRequest(before,linkChange(before)),{cmsBinary:join(estate,'not-installed')},{cmsRunFn:()=>{throw Error('must not run');}})).toThrow('saved-work owner is unavailable');
  expect(current(call,before.instance_id)).toEqual(before);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_link'").get().n).toBe(0);
}));


test('server work attachment requires a retained matching view independently of the browser',()=>fixture(({db,call,prepare,options})=>{
  const before=prepare().instance;
  const nonmatching=linkChange(before,{record_key:'other:record'});
  expect(()=>call('change',changeRequest(before,nonmatching),{},{cmsRunFn:()=>nativeRecord({key:'other:record'})})).toThrow('verified editor view');
  writeFileSync(join(options.workspacePackages,release,'editor/index.html'),'changed package body');
  expect(()=>call('change',changeRequest(before,linkChange(before)),{},{cmsRunFn:()=>nativeRecord()})).toThrow('verified editor view');
  expect(current(call,before.instance_id)).toEqual(before);
  expect(db.query("SELECT COUNT(*) n FROM sqlite_master WHERE name='_program_workspace_link'").get().n).toBe(0);
}));
