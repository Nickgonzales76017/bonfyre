import {test,expect,afterAll} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync,writeFileSync,mkdirSync,symlinkSync,lstatSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {connectionUpgradeTarget as target,reviewConnectionStudioUpgrade,applyConnectionStudioUpgrade,upgradeJournal,parseConnectionLifecycle,connectionEntrySnapshot} from '../connection_studio_upgrade.mjs';
import {connectionWorkspaceMaterial,connectionModules} from '../../materialize_connections_workspace.mjs';
import {verifyWorkspacePackage,readWorkspaceFile} from '../../workspace_material.mjs';
import {freeze} from '../connections_rollout.mjs';

// Isolated owner replies model the readback states in pack_lifecycle.h and
// workspace_store.h. No production process, account, CMS database or browser runs.
const hash=text=>createHash('sha256').update(text).digest('hex'),homes=[],candidate=hash('reviewed fixture package'),base=hash('installed fixture package');
const clone=value=>structuredClone(value),stages=['version','head','demand','install','activate'];
afterAll(()=>{for(const home of homes)rmSync(home,{recursive:true,force:true});});
test('staging replaces only the candidate catalog link without modifying its live target',()=>{
 const home=mkdtempSync(join(tmpdir(),'connection-stage-'));homes.push(home);
 const live=join(home,'live-catalog'),candidate=join(home,'candidate');mkdirSync(join(candidate,'bin'),{recursive:true});
 writeFileSync(live,'signed live catalog');const dest=join(candidate,'bin/bonfyre-connection-catalog');symlinkSync(live,dest);
 const record=freeze(candidate,'README.md','bin/bonfyre-connection-catalog',0o755);
 expect(readFileSync(live,'utf8')).toBe('signed live catalog');expect(lstatSync(dest).isSymbolicLink()).toBe(false);
 expect(hash(readFileSync(dest))).toBe(record.material.sha256);expect(record.input.sha256).toBe(record.material.sha256);
 const other=join(candidate,'bin/other');symlinkSync(live,other);
 expect(()=>freeze(candidate,'README.md','bin/other')).toThrow('An existing frozen release member differs');
 expect(readFileSync(live,'utf8')).toBe('signed live catalog');
});
function fixture(){
 const home=mkdtempSync(join(tmpdir(),'connection-upgrade-'));homes.push(home);
 const oldDigest=hash('old native closure'),planDigest=hash('new native closure'),version='connections-'+candidate.slice(0,40),oldVersion='connections-old';
 const common={namespace:'root',created_at:'retained',updated_at:'retained',published_at:null,status:'published'};
 const state={
  binding:{binding_id:target.binding,cms_id:12,installation_id:7,family:'connections',release:base,entry:'connections/'},
  installation:{...common,id:7,namespace:'pack-use-fixture',pack:target.pack,installed_for:target.subject,channel:'stable',pinned_version:oldVersion,installed_by:target.subject},
  installationState:{installation_id:7,revision:4,source_digest:oldDigest,last_operation:'activate',last_actor:target.subject},
  activation:{...common,id:8,namespace:'pack-use-fixture',pack:target.pack,activated_for:target.subject,scope:'self',activated_by:target.subject,conditions:'pack-lock:'+oldDigest},
  application:{...common,id:12,namespace:'pack-use-fixture',binding_id:target.binding,status:'active',site_id:target.subject,material_query:'{}'},
  head:{...common,id:20,pack:target.pack,channel:'stable',version:oldVersion,advanced_by:'existing-owner'},
  demand:{...common,id:21,pack:target.pack,demand_kind:'workspace',needs:'connections',preferred_sources:base,fields:'connections/'},
  baseVersion:{...common,id:22,pack:target.pack,version:oldVersion,root:base},candidateVersion:null,
  preserved:{owner:{serviceSha:hash('service'),operatorPause:hash('STOP')},definitions:{count:7,sha256:hash('other declarations')},records:{count:3,sha256:hash('original saved records')},otherBindings:{count:9,sha256:hash('other account editors')}}
 };
 const calls=[],control={source:'reviewed',missingPackage:false,failBefore:null,loseReply:null,loseRead:null,readFailed:false,competingPlan:false,recordsAfterInstall:false};
 const current=()=>{
  const next=state.installation.pinned_version===version;
  return {schema:'bonfyre.pack.lifecycle.v1',pack:target.pack,state:next?(state.activation.status==='published'?'active':'installed'):(state.demand.preferred_sources===base?'active':'stale'),revision:String(state.installationState.revision),installation:'7',pinned_digest:state.installationState.source_digest,committed:'0',pins:[[target.pack,next?version:oldVersion,next?candidate:base],['foundation','1',hash('foundation:1')]],nodes:[[target.pack,next?version:oldVersion,next?candidate:base,'official','application','Connection Studio'],['foundation','1',hash('foundation:1'),'official','application','Foundation']]};
 };
 const stage=action=>action.operation==='create'?'version':action.operation==='update'?(action.table==='pack_head'?'head':'demand'):action.operation;
 const adapter={
  journalRoot:join(home,'journal'),inputs:()=>[{path:'fixture.mjs',sha256:hash(control.source)}],
  materialize:async()=>({id:candidate}),verify:async id=>{if(control.missingPackage)throw Error('Package unavailable');expect(id).toBe(candidate);},
  inspect:async()=>{if(control.readFailed){control.readFailed=false;throw Error('Fixture lost native readback');}return {...clone(state),headDigest:hash(JSON.stringify(state.head)),demandDigest:hash(JSON.stringify(state.demand)),native:current()};},
  plan:async()=>({...current(),digest:planDigest,version,nodes:[[target.pack,version,candidate,'official','application','Connection Studio'],['foundation',control.competingPlan?'2':'1',hash(control.competingPlan?'foundation:2':'foundation:1'),'official','application','Foundation']]}),
  mutate:async action=>{
   const name=stage(action);calls.push(clone(action));if(control.failBefore===name)return {exitCode:1};
   if(name==='version')state.candidateVersion={...common,id:23,...action.value};
   else if(name==='head'){expect(action.id).toBe(20);expect(action.ifDigest).toBe(hash(JSON.stringify(state.head)));Object.assign(state.head,action.value,{updated_at:'changed'});}
   else if(name==='demand'){expect(action.id).toBe(21);expect(action.ifDigest).toBe(hash(JSON.stringify(state.demand)));Object.assign(state.demand,action.value,{updated_at:'changed'});}
   else{
    expect(action.revision).toBe(state.installationState.revision);expect(action.digest).toBe(planDigest);expect(action.version).toBe(version);
    Object.assign(state.installationState,{revision:state.installationState.revision+1,source_digest:planDigest,last_operation:name,last_actor:target.subject});
    Object.assign(state.installation,{pinned_version:version,updated_at:'changed',published_at:'native publication time'});
    Object.assign(state.binding,{release:candidate});Object.assign(state.application,{status:name==='activate'?'active':'draft',updated_at:'changed'});
    Object.assign(state.activation,{conditions:'pack-lock:'+planDigest,status:name==='activate'?'published':'withdrawn',updated_at:'changed',...(name==='activate'?{published_at:'native publication time'}:{})});
    if(name==='install'&&control.recordsAfterInstall)state.preserved.records.sha256=hash('concurrent saved edit');
   }
   if(control.loseRead===name)control.readFailed=true;
   if(control.loseReply===name)throw Error('Fixture lost command acknowledgement');
   return {exitCode:0};
  }
 };
 return {home,state,calls,control,adapter,journal:()=>upgradeJournal(adapter.journalRoot,candidate)};
}

test('review freezes a current account binding and package without invoking a mutation',async()=>{
 const f=fixture(),review=await reviewConnectionStudioUpgrade(f.adapter);expect(review.state).toBe('reviewed');expect(review.initialRevision).toBe(4);expect(review.binding).toBe(target.binding);expect(f.calls).toHaveLength(0);
 const retained=f.journal().read('review');expect(retained.review.before.preserved.records.count).toBe(3);expect(retained.review.candidate).toBe(candidate);
 expect((await reviewConnectionStudioUpgrade(f.adapter)).state).toBe('review_retained');expect(f.calls).toHaveLength(0);
});

test('native version, head, demand, install and activation keep the existing editor and saved work',async()=>{
 const f=fixture(),before=clone(f.state.preserved);await reviewConnectionStudioUpgrade(f.adapter);
 const result=await applyConnectionStudioUpgrade(f.adapter,candidate);expect(result.state).toBe('installed_verified');expect(result.revision).toBe(6);expect(result.savedRecordsUnchanged).toBe(true);
 expect(f.state.binding.binding_id).toBe(target.binding);expect(f.state.binding.release).toBe(candidate);expect(f.state.installation.id).toBe(7);expect(f.state.application.id).toBe(12);expect(f.state.preserved).toEqual(before);
 expect(f.calls.map(action=>action.operation)).toEqual(['create','update','update','install','activate']);
 const count=f.calls.length;expect((await applyConnectionStudioUpgrade(f.adapter,candidate)).state).toBe('already_verified');expect(f.calls).toHaveLength(count);
 expect(f.journal().read('complete').otherBindingsUnchanged).toBe(true);
});

for(const stage of stages)test('a lost '+stage+' acknowledgement is reconciled from native state without duplicate dispatch',async()=>{
 const f=fixture();f.control.loseReply=stage;await reviewConnectionStudioUpgrade(f.adapter);
 expect((await applyConnectionStudioUpgrade(f.adapter,candidate)).state).toBe('installed_verified');expect(f.calls).toHaveLength(5);expect(f.journal().read(stage+'-done').reconciled).toBe(true);
 expect((await applyConnectionStudioUpgrade(f.adapter,candidate)).state).toBe('already_verified');expect(f.calls).toHaveLength(5);
});

for(const stage of stages)test('a process interrupted after '+stage+' resumes from its durable intent and exact owner readback',async()=>{
 const f=fixture();f.control.loseRead=stage;await reviewConnectionStudioUpgrade(f.adapter);
 await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('Unconfirmed '+stage+' write');
 expect(f.journal().read(stage+'-intent')).not.toBeNull();expect(f.journal().read(stage+'-done')).toBeNull();
 const before=f.calls.length;f.control.loseRead=null;expect((await applyConnectionStudioUpgrade(f.adapter,candidate)).state).toBe('installed_verified');
 expect(f.calls).toHaveLength(5);expect(before).toBe(stages.indexOf(stage)+1);expect(f.journal().read(stage+'-done').reconciled).toBe(true);
});

test('an uncertain intent with unchanged pre-state stops on every resume without replaying the write',async()=>{
 const f=fixture();f.control.failBefore='head';await reviewConnectionStudioUpgrade(f.adapter);
 await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('Unconfirmed head write');expect(f.calls).toHaveLength(2);
 f.control.failBefore=null;await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('no native write was replayed');expect(f.calls).toHaveLength(2);expect(f.state.binding.release).toBe(base);
});

test('changed source, missing package, changed service or another account binding refuse before native dispatch',async()=>{
 for(const change of [f=>f.control.source='changed',f=>f.control.missingPackage=true,f=>f.state.preserved.owner.serviceSha=hash('new service'),f=>f.state.binding.binding_id='ws-'+hash('other account').slice(0,40)]){
  const f=fixture();await reviewConnectionStudioUpgrade(f.adapter);change(f);await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow();expect(f.calls).toHaveLength(0);
 }
});

test('a competing head or workspace declaration is never overwritten',async()=>{
 for(const change of [f=>f.state.head.version='someone-elses-version',f=>f.state.demand.preferred_sources=hash('other package')]){
  const f=fixture();await reviewConnectionStudioUpgrade(f.adapter);change(f);await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow();expect(f.calls).toHaveLength(0);
 }
});

test('an unintended dependency upgrade refuses native installation',async()=>{
 const f=fixture();await reviewConnectionStudioUpgrade(f.adapter);f.control.competingPlan=true;
 await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('another version pin');expect(f.calls).toHaveLength(3);expect(f.state.binding.release).toBe(base);expect(f.state.installationState.revision).toBe(4);
});

test('a changed saved record after installation stops before activation and is never restored or overwritten',async()=>{
 const f=fixture();await reviewConnectionStudioUpgrade(f.adapter);f.control.recordsAfterInstall=true;
 await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('Saved records');expect(f.calls).toHaveLength(4);expect(f.state.application.status).toBe('draft');
 const changed=f.state.preserved.records.sha256;await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('no native write was replayed');expect(f.calls).toHaveLength(4);expect(f.state.preserved.records.sha256).toBe(changed);
});

test('a changed review receipt or absent exact package review cannot authorize native writes',async()=>{
 const f=fixture();await reviewConnectionStudioUpgrade(f.adapter);const path=join(f.journal().dir,'review.json'),packet=JSON.parse(readFileSync(path,'utf8'));packet.review.target.subject='user:20';writeFileSync(path,JSON.stringify(packet));
 await expect(applyConnectionStudioUpgrade(f.adapter,candidate)).rejects.toThrow('No intact review');await expect(applyConnectionStudioUpgrade(f.adapter,hash('unreviewed'))).rejects.toThrow('No intact review');expect(f.calls).toHaveLength(0);
});

test('lifecycle parsing preserves repeated pin/node rows and refuses duplicate or malformed identity fields',()=>{
 const raw='schema\tbonfyre.pack.lifecycle.v1\npack\t'+target.pack+'\nstate\tactive\nrevision\t4\ninstallation\t7\npinned_digest\t'+hash('closure')+'\npin\t'+target.pack+'\told\t'+base+'\npin\tfoundation\t1\t'+hash('foundation')+'\n';
 expect(parseConnectionLifecycle(raw).pins).toHaveLength(2);expect(()=>parseConnectionLifecycle(raw+'revision\t5\n')).toThrow('Duplicate');expect(()=>parseConnectionLifecycle(raw.replace('revision\t4','revision\tunknown'))).toThrow('unavailable');
});

test('the entry baseline uses native typed MessagePack column order and binary text',()=>{
 const value={id:20,namespace:'root',status:'published',optional:null},columns=[['id','integer'],['namespace','text'],['status','text'],['optional','null']];
 // Hand-written native row envelope: [[name, SQLite kind, typed value], ...].
 const nativeBytes=Buffer.from('9493a26964011493a96e616d65737061636503c404726f6f7493a673746174757303c4097075626c697368656493a86f7074696f6e616c05c0','hex');
 expect(connectionEntrySnapshot(value,columns)).toBe(hash(nativeBytes));expect(connectionEntrySnapshot(value,[...columns].reverse())).not.toBe(hash(nativeBytes));
 expect(()=>connectionEntrySnapshot({...value,id:20.5},columns)).toThrow();expect(()=>connectionEntrySnapshot(value,[['id','real']])).toThrow('Unsupported');
});

test('the retained Connection Studio package contains the current card, UI and complete bridge closure',async()=>{
 const home=mkdtempSync(join(tmpdir(),'connection-package-'));homes.push(home);const packages=join(home,'packages'),material=await connectionWorkspaceMaterial({root:packages});
 expect((await verifyWorkspacePackage(packages,material.id)).id).toBe(material.id);expect((await connectionWorkspaceMaterial({root:packages})).id).toBe(material.id);
 const html=(await readWorkspaceFile(packages,material.id,'connections/index.html')).toString();expect(html).toContain('../bridge/connection-studio.mjs');expect(html).toContain('bonfyre-workspace-store');
 const repo=resolve(import.meta.dir,'../../..');for(const name of connectionModules)expect(Buffer.from(await readWorkspaceFile(packages,material.id,'bridge/'+name))).toEqual(readFileSync(join(repo,'integrations/browser',name)));
 expect((await readWorkspaceFile(packages,material.id,'bridge/connection-studio.mjs')).toString()).toContain('fieldsCollapsed:true');
});
