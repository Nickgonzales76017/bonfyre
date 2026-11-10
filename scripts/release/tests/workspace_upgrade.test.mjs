import {test,expect,afterAll} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,upgradeJournal,parseWorkspaceLifecycle} from '../workspace_upgrade.mjs';
import {researchUpgradeSpec} from '../research_workspace_upgrade.mjs';
const target=researchUpgradeSpec.target;
const spec={...researchUpgradeSpec,baseRoot:release=>hash('wrapped:'+release),versionFor:(candidate,base)=>({pack:target.pack,version:'research-'+candidate.slice(0,24),root:hash('wrapped:'+candidate),status:'published'})};
// Isolated owner replies model the readback states in pack_lifecycle.h and
// workspace_store.h. No production process, account, CMS database or browser runs.
const hash=text=>createHash('sha256').update(text).digest('hex'),homes=[],candidate=hash('reviewed fixture package'),base=hash('installed fixture package');
const clone=value=>structuredClone(value),stages=['version','head','demand','install','activate'];
afterAll(()=>{for(const home of homes)rmSync(home,{recursive:true,force:true});});
function fixture(){
 const home=mkdtempSync(join(tmpdir(),'connection-upgrade-'));homes.push(home);
 const oldDigest=hash('old native closure'),planDigest=hash('new native closure'),version=spec.versionFor(candidate,base).version,oldVersion='connections-old';
 const common={namespace:'root',created_at:'retained',updated_at:'retained',published_at:null,status:'published'};
 const state={
  binding:{binding_id:target.binding,cms_id:12,installation_id:7,family:target.family,release:base,entry:target.entry},
  installation:{...common,id:7,namespace:'pack-use-fixture',pack:target.pack,installed_for:target.subject,channel:'stable',pinned_version:oldVersion,installed_by:target.subject},
  installationState:{installation_id:7,revision:4,source_digest:oldDigest,last_operation:'activate',last_actor:target.subject},
  activation:{...common,id:8,namespace:'pack-use-fixture',pack:target.pack,activated_for:target.subject,scope:'self',activated_by:target.subject,conditions:'pack-lock:'+oldDigest},
  application:{...common,id:12,namespace:'pack-use-fixture',binding_id:target.binding,status:'active',site_id:target.subject,material_query:'{}'},
  head:{...common,id:20,pack:target.pack,channel:'stable',version:oldVersion,advanced_by:'existing-owner'},
  demand:{...common,id:21,pack:target.pack,demand_kind:'workspace',needs:target.family,preferred_sources:base,fields:target.entry},
  baseVersion:{...common,id:22,pack:target.pack,version:oldVersion,root:spec.baseRoot(base)},candidateVersion:null,
  preserved:{owner:{serviceSha:hash('service'),operatorPause:hash('STOP')},definitions:{count:7,sha256:hash('other declarations')},records:{count:3,sha256:hash('original saved records')},otherBindings:{count:9,sha256:hash('other account editors')}}
 };
 const calls=[],control={source:'reviewed',missingPackage:false,failBefore:null,loseReply:null,loseRead:null,readFailed:false,competingPlan:false,recordsAfterInstall:false,initialStale:false,initialError:'resolution_refused'};
 const current=()=>{
  const next=state.installation.pinned_version===version;
  const stale=!next&&(control.initialStale||state.demand.preferred_sources!==base);
  return {schema:'bonfyre.pack.lifecycle.v1',pack:target.pack,state:next?(state.activation.status==='published'?'active':'installed'):(stale?'stale':'active'),revision:String(state.installationState.revision),installation:'7',pinned_digest:state.installationState.source_digest,committed:'0',...(control.initialStale&&!next?{error:control.initialError}:{}),pins:[[target.pack,next?version:oldVersion,next?spec.versionFor(candidate,base).root:spec.baseRoot(base)],['foundation','1',hash('foundation:1')]],nodes:[[target.pack,next?version:oldVersion,next?spec.versionFor(candidate,base).root:spec.baseRoot(base),'official','application','Connection Studio'],['foundation','1',hash('foundation:1'),'official','application','Foundation']]};
 };
 const stage=action=>action.operation==='create'?'version':action.operation==='update'?(action.table==='pack_head'?'head':'demand'):action.operation;
 const adapter={
  journalRoot:join(home,'journal'),inputs:()=>[{path:'fixture.mjs',sha256:hash(control.source)}],
  materialize:async()=>({id:candidate}),verify:async id=>{if(control.missingPackage)throw Error('Package unavailable');expect(id).toBe(candidate);},
  inspect:async()=>{if(control.readFailed){control.readFailed=false;throw Error('Fixture lost native readback');}return {...clone(state),headDigest:hash(JSON.stringify(state.head)),demandDigest:hash(JSON.stringify(state.demand)),native:current()};},
  plan:async()=>{const planned=current();delete planned.error;return {...planned,digest:planDigest,version,nodes:[[target.pack,version,spec.versionFor(candidate,base).root,'official','application','Connection Studio'],['foundation',control.competingPlan?'2':'1',hash(control.competingPlan?'foundation:2':'foundation:1'),'official','application','Foundation']]};},
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

test('a second workspace preserves the account binding and uses declared version roots, not asset digests',async()=>{
 const f=fixture();const review=await reviewWorkspaceUpgrade(f.adapter,spec);expect(review.binding).toBe(target.binding);expect(f.calls).toHaveLength(0);
 const result=await applyWorkspaceUpgrade(f.adapter,candidate,spec);expect(result.state).toBe('installed_verified');expect(result.editorURL).toEndWith('/instruments/');expect(f.state.binding.release).toBe(candidate);expect(f.state.candidateVersion.root).toBe(hash('wrapped:'+candidate));expect(f.calls).toHaveLength(5);
 expect((await applyWorkspaceUpgrade(f.adapter,candidate,spec)).state).toBe('already_verified');expect(f.calls).toHaveLength(5);
});
for(const stage of stages)test('second-family interrupted '+stage+' resumes without repeating the effect',async()=>{
 const f=fixture();f.control.loseRead=stage;await reviewWorkspaceUpgrade(f.adapter,spec);
 await expect(applyWorkspaceUpgrade(f.adapter,candidate,spec)).rejects.toThrow('Unconfirmed '+stage+' write');
 f.control.loseRead=null;expect((await applyWorkspaceUpgrade(f.adapter,candidate,spec)).state).toBe('installed_verified');expect(f.calls).toHaveLength(5);
});
test('declaration-only stale closure requires explicit opt-in and can upgrade without changing unrelated pins',async()=>{
 const denied=fixture();denied.control.initialStale=true;
 await expect(reviewWorkspaceUpgrade(denied.adapter,spec)).rejects.toThrow('resolution-refused declaration-stale closure');expect(denied.calls).toHaveLength(0);
 const f=fixture();f.control.initialStale=true;const staleSpec={...spec,id:'research-declaration-stale-upgrade',schema:'fixture.declaration-stale.v1',allowInitialStale:true};
 const review=await reviewWorkspaceUpgrade(f.adapter,staleSpec);expect(review.state).toBe('reviewed');expect(f.calls).toHaveLength(0);
 const result=await applyWorkspaceUpgrade(f.adapter,candidate,staleSpec);expect(result.state).toBe('installed_verified');expect(f.state.binding.release).toBe(candidate);expect(f.calls).toHaveLength(5);
});

test('declaration-stale opt-in refuses stale states with any error other than resolution_refused',async()=>{
 const f=fixture();f.control.initialStale=true;f.control.initialError='storage_refused';
 const staleSpec={...spec,id:'research-declaration-stale-error-refusal',schema:'fixture.declaration-stale-error.v1',allowInitialStale:true};
 await expect(reviewWorkspaceUpgrade(f.adapter,staleSpec)).rejects.toThrow('resolution-refused declaration-stale closure');expect(f.calls).toHaveLength(0);
});

test('declaration-stale opt-in still refuses a mismatched stable head',async()=>{
 const f=fixture();f.control.initialStale=true;f.state.head.version='foreign-version';
 const staleSpec={...spec,id:'research-declaration-stale-head-refusal',schema:'fixture.declaration-stale-head.v1',allowInitialStale:true};
 await expect(reviewWorkspaceUpgrade(f.adapter,staleSpec)).rejects.toThrow('separately prepared head');expect(f.calls).toHaveLength(0);
});

test('another family or account cannot consume a retained package review',async()=>{
 const f=fixture();await reviewWorkspaceUpgrade(f.adapter,spec);
 await expect(applyWorkspaceUpgrade(f.adapter,candidate,{...spec,target:{...target,subject:'user:20'}})).rejects.toThrow('reviewed target changed');expect(f.calls).toHaveLength(0);
});
test('native lifecycle replies must name the selected pack',()=>{
 const raw='schema\tbonfyre.pack.lifecycle.v1\npack\t'+target.pack+'\nrevision\t4\ninstallation\t7\npinned_digest\t'+hash('closure')+'\n';
 expect(parseWorkspaceLifecycle(raw,target.pack).pack).toBe(target.pack);expect(()=>parseWorkspaceLifecycle(raw,'experience.other.workspace')).toThrow('Invalid native');
});
