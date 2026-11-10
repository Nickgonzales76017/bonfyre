import {test,expect,afterAll} from 'bun:test';
import {mkdtempSync,mkdirSync,readFileSync,writeFileSync,copyFileSync,rmSync,chmodSync,realpathSync,symlinkSync,readlinkSync,lstatSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname,join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {encode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {readProgramServicePolicy,renderProgramServicePolicy} from '../../project-program-service-policy.mjs';
import {freezeBrowser,freezePrograms,uniqueInputs,configurePrograms,inspectRelease,freezeSiteAlias,terminalBin,verifyInputs,verifyBrowser,browserReadback,programReadback,retryReadbackTransport,acceptanceEnvironment,acceptanceIdentity,requireAcceptance,requireFrozenAcceptance,verifyPromotionReceipt,findExtensionChromium} from '../public_surface_rollout.mjs';

const projectRoot=resolve(import.meta.dir,'../../..');
const homes=[];
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const closure=['scripts/program-workspace-owner.mjs','scripts/program-source-work.mjs','scripts/program-work-reference.mjs','scripts/project-program-service-policy.mjs','cmd/BonfyreInteractionGrammar/src/main.c','scripts/workspace_material.mjs','integrations/browser/workspace-continuation.mjs','scripts/compile-program-fabric.mjs','scripts/program-fabric.cue','integrations/browser/workspace-msgpack.mjs','integrations/browser/provider-contract.mjs','cmd/BonfyreSourceFrontier/tools/web_work.mjs','cmd/BonfyreSourceFrontier/tools/web_protocol.mjs','cmd/BonfyreSourceFrontier/tools/source_material.mjs','cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs','cmd/BonfyreExternalEstate/bonfyre-external-estate','cmd/BonfyreCMS/bonfyre-cms','cmd/BonfyreCredentials/provider-credentials.json'];
const browserAssets=['aurekai-activity.mjs','aurekai-horde.mjs','aurekai-activity-source.mjs','aurekai-inbox-source.mjs','aurekai-activity.css','workspace-msgpack.mjs','extension/foreign-sign-in.mjs','extension/foreign-sign-in.css','aurekai-push.mjs','aurekai-push-sw.mjs','provider-contract.mjs','aurekai-model.mjs','aurekai-model.css','program-workspace.mjs','program-service-continuations.mjs','program-service-policy.mjs','program-source-work.mjs','program-workspace.css','interaction-collection.mjs','record-comparison.mjs','workspace-store.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-destinations.mjs','reference-picker.mjs','account-choices.mjs','account-choices.css','invitation-choice.mjs','connection-studio.mjs','connection-studio.css','authentication-capability-surface.mjs','provider-auth-capability.mjs','connection-source.mjs','connection-lab.mjs','publication-picker.mjs','publication-research.mjs','source-desk.mjs','source-desk.css','source-publication.mjs','source-publication.css','web-workbench.mjs','web-workbench.css','extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs'];
function put(path,body,mode=0o644){mkdirSync(dirname(path),{recursive:true});writeFileSync(path,body,{mode});chmodSync(path,mode);}
function fixture(){
 const home=realpathSync(mkdtempSync(join(tmpdir(),'program-rollout-')));homes.push(home);
 const source=join(home,'source'),release=join(home,'release'),bin=join(home,'tools');mkdirSync(release);mkdirSync(bin);
 for(const path of closure)put(join(source,path),'fixture '+path+'\n',/\/bonfyre-(external-estate|cms)$/.test(path)?0o755:0o644);
 for(const name of browserAssets)put(join(source,'integrations/browser',name),'fixture integrations/browser/'+name+'\n');
 put(join(bin,'cue'),'#!/bin/sh\nexit 0\n',0o755);
 const bun=join(bin,'bun');put(bun,'#!/bin/sh\nexit 0\n',0o755);
 const cms=join(home,'canonical-cms.db');put(cms,'existing canonical database\n');
 const control=join(home,'control.db');put(control,'existing canonical control database\n');
 const frontier=join(home,'canonical-frontier.db');put(frontier,'existing source database\n');
 for(const name of ['cms','auth'])put(join(bin,name),'#!/bin/sh\nexit 0\n',0o755);
 const before={Label:'com.bonfyre.public-web-address',ProgramArguments:['/existing/web-address','serve','--db','/canonical/matrix.db','--auth-db','/canonical/auth.db','--site-root','/existing/site','--port','28901'],EnvironmentVariables:{BONFYRE_AUTH_BINARY:join(bin,'auth'),BONFYRE_CMS_BINARY:join(bin,'cms'),BONFYRE_CMS_DB:cms,BONFYRE_CONTROL_DB:control,BONFYRE_SOURCE_FRONTIER_DB:frontier,BONFYRE_WORKSPACE_CLIENT:'/existing/browser',BONFYRE_BIN_DIR:'/existing/bin',BONFYRE_ADDRESS_INDEX:'/canonical/address.db',UNCHANGED_SETTING:'retained'}};
 return {home,source,release,bin,bun,before};
}
function stageFixture(f){
 put(join(f.release,'bin/bonfyre-web-address'),'fixture frozen resolver\n',0o755);put(join(f.release,'bin/bonfyre-meter'),'fixture frozen meter\n',0o755);
 freezeSiteAlias(f.release);
 const frozen=freezePrograms(f.source,f.release),configured=configurePrograms(f.before,f.release,{bun:f.bun,cuePath:f.bin});
 const browserInputs=freezeBrowser(f.source,f.release);
 const backup=Buffer.from('fixture original service plist\n');put(join(f.release,'previous-service.plist'),backup);
 put(join(f.release,'candidate-service.plist'),'fixture candidate service plist\n');
 const members=[...closure.map(path=>'native/'+path),...browserAssets.map(name=>'browser/'+name),'runtime/bin/cue','bin/bonfyre-web-address','bin/bonfyre-site','bin/bonfyre-meter','candidate-service.plist','previous-service.plist'];
 const material=members.map(path=>({path,sha256:sha(readFileSync(join(f.release,path)))}));
 const stage={kind:'public-surface-release-v2',directory:f.release,previousHash:sha(backup),inputs:uniqueInputs([...frozen.inputs,...browserInputs]),material,runtimes:configured.runtimes};
 put(join(f.release,'stage.mpk'),encode(stage),0o600);
 return {stage,frozen,configured};
}
afterAll(()=>{for(const path of homes)rmSync(path,{recursive:true,force:true});});

test('frozen site uses the exact release resolver, never the stale managed site',()=>{
 const f=fixture();stageFixture(f);
 const web=join(f.release,'bin/bonfyre-web-address'),site=join(f.release,'bin/bonfyre-site');
 expect(lstatSync(site).isSymbolicLink()).toBe(true);
 expect(readlinkSync(site)).toBe('bonfyre-web-address');expect(readFileSync(site)).toEqual(readFileSync(web));
 expect(inspectRelease(f.release).material.some(row=>row.path==='bin/bonfyre-site')).toBe(true);
 rmSync(site);put(site,readFileSync(web),0o755);
 expect(()=>inspectRelease(f.release)).toThrow('relative alias');
 rmSync(site);symlinkSync('/old/managed/bonfyre-site',site);
 expect(()=>inspectRelease(f.release)).toThrow('relative alias');
 rmSync(site);symlinkSync('bonfyre-web-address',site);
 expect(inspectRelease(f.release).directory).toBe(f.release);
});

test('release links resolve long chains and reject cycles',()=>{const f=fixture(),base=join(f.home,'target');put(base,'x');let path=base;for(let i=0;i<70;i++){const next=join(f.home,'link'+i);symlinkSync(path,next);path=next;}expect(terminalBin(path)).toBe(base);const a=join(f.home,'cycle-a'),b=join(f.home,'cycle-b');symlinkSync(b,a);symlinkSync(a,b);expect(()=>terminalBin(a)).toThrow('Cyclic');});
test('review manifest coalesces shared sources but refuses conflicting identities',()=>{
 const rows=uniqueInputs([{path:'integrations/browser/workspace-msgpack.mjs',sha256:'a'.repeat(64)},{path:'integrations/browser/workspace-msgpack.mjs',sha256:'a'.repeat(64)},{path:'scripts/program-fabric.cue',sha256:'b'.repeat(64)}]);
 expect(rows).toEqual([{path:'integrations/browser/workspace-msgpack.mjs',sha256:'a'.repeat(64)},{path:'scripts/program-fabric.cue',sha256:'b'.repeat(64)}]);
 expect(()=>uniqueInputs([{path:'same',sha256:'a'.repeat(64)},{path:'same',sha256:'b'.repeat(64)}])).toThrow('cannot attest two different byte identities');
});

test('Program service browser policy is the exact generated projection of ProgramFabric CUE',()=>{
 const servicePolicy=readProgramServicePolicy(join(projectRoot,'scripts/program-fabric.cue'));
 expect(readFileSync(join(projectRoot,'integrations/browser/program-service-policy.mjs'),'utf8')).toBe(renderProgramServicePolicy(servicePolicy));
 expect(servicePolicy.grammar_basis.correspondence).toEqual({owner_families:['Correspondence'],state:'pending_exact_owner'});
 expect(servicePolicy.grammar_basis.community).toEqual({continuation:'community',owner_families:['Community','RelationshipGraph'],state:'typed'});
});

test('upgrades an old browser tree with the complete hashed surface and refuses a missing checkout asset',()=>{
 const f=fixture();
 for(const name of ['aurekai-activity.mjs','extension/foreign-sign-in.mjs','program-workspace.mjs','extension/workbench-shell.mjs'])put(join(f.release,'browser',name),'old deployed '+name+'\n');
 put(join(f.release,'browser','unrelated-page.mjs'),'retained existing page\n');
 const inputs=freezeBrowser(f.source,f.release);
 expect(inputs.map(row=>row.path).sort()).toEqual(browserAssets.map(name=>'integrations/browser/'+name).sort());
 for(const name of browserAssets){
  const source=readFileSync(join(f.source,'integrations/browser',name));
  expect(readFileSync(join(f.release,'browser',name))).toEqual(source);
  expect(inputs.find(row=>row.path==='integrations/browser/'+name)?.sha256).toBe(sha(source));
 }
 expect(readFileSync(join(f.release,'browser','unrelated-page.mjs'),'utf8')).toBe('retained existing page\n');
 for(const name of ['provider-contract.mjs','aurekai-push-sw.mjs','aurekai-model.css','program-service-policy.mjs','connection-studio.mjs','connection-source.mjs','web-workbench.css','extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs']){
  const missing=fixture();put(join(missing.release,'browser',name),'stale deployed fallback\n');rmSync(join(missing.source,'integrations/browser',name));
  expect(()=>freezeBrowser(missing.source,missing.release)).toThrow();
 }
});

test('frozen clone inputs preserve bytes and modes without aliasing the checkout',()=>{
 const f=fixture(),name='aurekai-activity.mjs';
 const source=join(f.source,'integrations/browser',name),target=join(f.release,'browser',name);
 put(source,Buffer.alloc(256*1024+17,0x61));
 const original=readFileSync(source);
 put(target,'stale prior browser material',0o755);
 const inputs=freezeBrowser(f.source,f.release);
 expect(readFileSync(target)).toEqual(original);
 expect(lstatSync(target).mode & 0o777).toBe(0o644);
 expect(inputs.find(row=>row.path==='integrations/browser/'+name)?.sha256).toBe(sha(original));
 writeFileSync(source,'modified checkout browser');
 expect(readFileSync(target)).toEqual(original);
 writeFileSync(target,'modified frozen browser');
 expect(readFileSync(source,'utf8')).toBe('modified checkout browser');
 const program=freezePrograms(f.source,f.release);
 const binName='cmd/BonfyreCMS/bonfyre-cms';
 const programSource=join(f.source,binName),programTarget=join(f.release,'native',binName);
 const programOriginal=readFileSync(programSource);
 expect(readFileSync(programTarget)).toEqual(programOriginal);
 expect(lstatSync(programTarget).mode & 0o777).toBe(0o755);
 expect(program.inputs.find(row=>row.path===binName)?.sha256).toBe(sha(programOriginal));
 writeFileSync(programTarget,'changed staged executable');
 expect(readFileSync(programSource)).toEqual(programOriginal);
});

test('frozen CUE runtime is a content-matched independent clone',()=>{
 const f=fixture(),source=join(f.bin,'cue'),original=Buffer.alloc(256*1024+13,0x78);
 put(source,original,0o755);
 const {runtimes}=configurePrograms(f.before,f.release,{bun:f.bun,cuePath:f.bin});
 const target=join(f.release,'runtime/bin/cue');
 expect(readFileSync(target)).toEqual(original);
 expect(lstatSync(target).mode&0o777).toBe(0o755);
 expect(runtimes.find(row=>row.role==='cue')?.sha256).toBe(sha(original));
 writeFileSync(source,'changed checkout CUE');
 expect(readFileSync(target)).toEqual(original);
 writeFileSync(target,'changed frozen CUE');
 expect(readFileSync(source,'utf8')).toBe('changed checkout CUE');
 expect(()=>configurePrograms(f.before,f.release,{bun:f.bun,cuePath:f.bin})).toThrow();
});

test('freezes the complete compiler-relative Programs closure with matching input bytes',()=>{
 const f=fixture(),frozen=freezePrograms(f.source,f.release);
 expect(frozen.nativeRoot).toBe(join(f.release,'native'));
 expect(frozen.owner).toBe(join(f.release,'native/scripts/program-workspace-owner.mjs'));
 for(const path of closure){
  expect(readFileSync(join(f.release,'native',path))).toEqual(readFileSync(join(f.source,path)));
  const row=frozen.inputs.find(row=>row.path===path);expect(row?.sha256).toBe(sha(readFileSync(join(f.source,path))));
 }
 put(join(f.source,'scripts/program-workspace-owner.mjs'),'changed checkout owner\n');
 expect(readFileSync(frozen.owner,'utf8')).toBe('fixture scripts/program-workspace-owner.mjs\n');
});

test('binds Programs explicitly while preserving canonical databases, args and unrelated environment',()=>{
 const f=fixture(),original=structuredClone(f.before);
 const {configuration,runtimes}=configurePrograms(f.before,f.release,{bun:process.execPath,cuePath:f.bin});
 expect(f.before).toEqual(original);
 expect(configuration.ProgramArguments).toEqual(original.ProgramArguments);
 for(const [key,value]of Object.entries(original.EnvironmentVariables))expect(configuration.EnvironmentVariables[key]).toBe(value);
 expect(configuration.EnvironmentVariables.BONFYRE_PROGRAM_OWNER).toBe(join(f.release,'native/scripts/program-workspace-owner.mjs'));
 expect(configuration.EnvironmentVariables.BONFYRE_PROGRAM_SOURCE_OWNER).toBe(join(f.release,'native/scripts/program-source-work.mjs'));
 expect(configuration.EnvironmentVariables.BONFYRE_PROGRAM_CMS_BINARY).toBe(join(f.release,'native/cmd/BonfyreCMS/bonfyre-cms'));
 expect(configuration.EnvironmentVariables.BONFYRE_BUN_BINARY).toBe(realpathSync(process.execPath));
 const cue=join(f.release,'runtime/bin/cue');
 expect(configuration.EnvironmentVariables.PATH.split(':')[0]).toBe(dirname(cue));
 expect(readFileSync(cue)).toEqual(readFileSync(join(f.bin,'cue')));
 expect(runtimes.some(row=>row.path===realpathSync(process.execPath))).toBe(true);
 expect(runtimes.some(row=>row.path===cue&&row.role==='cue')).toBe(true);
 put(join(f.bin,'cue'),'#!/bin/sh\nexit 9\n',0o755);
 expect(readFileSync(cue,'utf8')).toBe('#!/bin/sh\nexit 0\n');
 expect(readFileSync(f.before.EnvironmentVariables.BONFYRE_CMS_DB,'utf8')).toBe('existing canonical database\n');
});

test('account Work is wired for signed-in reads even without a dispatcher owner',()=>{
 const f=fixture(),index=join(f.home,'.bonfyre/state/account-work.db'),operator=join(f.home,'.bonfyre/operator/operator.sqlite3');
 put(index,'retained account work fixture');put(operator,'retained operator fixture');
 const read=configurePrograms(f.before,f.release,{bun:process.execPath,cuePath:f.bin,accountHome:f.home}).configuration.EnvironmentVariables;
 expect(read.BONFYRE_ACCOUNT_WORK_DB).toBe(index);expect(read.BONFYRE_ACCOUNT_WORK_SOURCES).toBe('operator='+operator);
 expect(read.BONFYRE_ACCOUNT_WORK_OWNER).toBeUndefined();const meter=join(f.home,'.local/share/bonfyre/state/bonfyre-meter.db');put(meter,'meter fixture');const meteredRelease=join(f.home,'metered-release');mkdirSync(meteredRelease);const metered=configurePrograms(f.before,meteredRelease,{bun:process.execPath,cuePath:f.bin,accountHome:f.home}).configuration.EnvironmentVariables;expect(metered.BONFYRE_METER_DB).toBe(meter);expect(metered.BONFYRE_ACCOUNT_WORK_SOURCES).toBe('operator='+operator+';meter='+meter);
 const withOwner=structuredClone(f.before);withOwner.EnvironmentVariables.BONFYRE_ACCOUNT_WORK_OWNER='user:19';
 const ownerRelease=join(f.home,'owner-release');mkdirSync(ownerRelease);const owned=configurePrograms(withOwner,ownerRelease,{bun:process.execPath,cuePath:f.bin,accountHome:f.home}).configuration.EnvironmentVariables;
 expect(owned.BONFYRE_ACCOUNT_WORK_OWNER).toBe('user:19');expect(owned.BONFYRE_ACCOUNT_WORK_DB).toBe(index);
 const missing=fixture();const unset=configurePrograms(missing.before,missing.release,{bun:process.execPath,cuePath:missing.bin,accountHome:missing.home}).configuration.EnvironmentVariables;
 expect(unset.BONFYRE_ACCOUNT_WORK_DB).toBeUndefined();expect(unset.BONFYRE_ACCOUNT_WORK_SOURCES).toBeUndefined();
 const misconfigured=structuredClone(missing.before);misconfigured.EnvironmentVariables.BONFYRE_ACCOUNT_WORK_DB=join(missing.home,'missing-account.db');
 const explicitRelease=join(missing.home,'explicit-release');mkdirSync(explicitRelease);expect(()=>configurePrograms(misconfigured,explicitRelease,{bun:process.execPath,cuePath:missing.bin,accountHome:missing.home})).toThrow('unavailable');
 const noIndexOwner=structuredClone(missing.before);noIndexOwner.EnvironmentVariables.BONFYRE_ACCOUNT_WORK_OWNER='user:19';
 const noIndexRelease=join(missing.home,'owner-without-index');mkdirSync(noIndexRelease);expect(()=>configurePrograms(noIndexOwner,noIndexRelease,{bun:process.execPath,cuePath:missing.bin,accountHome:missing.home})).toThrow('requires an existing');
});

test('uses the installed CUE path and refuses missing canonical CMS or usable CUE',()=>{
 const f=fixture(),fallback=join(f.home,'fallback');mkdirSync(fallback);put(join(fallback,'cue'),'#!/bin/sh\nexit 1\n',0o755);
 f.before.EnvironmentVariables.PATH=f.bin;
 const {configuration,runtimes}=configurePrograms(f.before,f.release,{bun:process.execPath,cuePath:fallback});
 expect(configuration.EnvironmentVariables.PATH.split(':')[1]).toBe(f.bin);
 expect(readFileSync(join(f.release,'runtime/bin/cue'))).toEqual(readFileSync(join(f.bin,'cue')));
 expect(runtimes.some(row=>row.path===join(f.release,'runtime/bin/cue')&&row.sha256===sha(readFileSync(join(f.bin,'cue'))))).toBe(true);
 expect(runtimes.some(row=>row.path===realpathSync(join(fallback,'cue')))).toBe(false);
 const noCms=structuredClone(f.before);delete noCms.EnvironmentVariables.BONFYRE_CMS_DB;
 expect(()=>configurePrograms(noCms,f.release,{bun:process.execPath,cuePath:fallback})).toThrow();
 const noFrontier=structuredClone(f.before);delete noFrontier.EnvironmentVariables.BONFYRE_SOURCE_FRONTIER_DB;
 expect(()=>configurePrograms(noFrontier,f.release,{bun:process.execPath,cuePath:fallback})).toThrow();
 const noCue=structuredClone(f.before);noCue.EnvironmentVariables.PATH=join(f.home,'empty-path');mkdirSync(noCue.EnvironmentVariables.PATH);
 expect(()=>configurePrograms(noCue,f.release,{bun:process.execPath,cuePath:noCue.EnvironmentVariables.PATH})).toThrow();
});

test('replaces only stale Program executables and preserves source admission and canonical work owners',()=>{
 const f=fixture();Object.assign(f.before.EnvironmentVariables,{
  BONFYRE_PROGRAM_OWNER:'/old/program-owner.mjs',BONFYRE_PROGRAM_SOURCE_OWNER:'/old/source-bridge.mjs',
  BONFYRE_PROGRAM_CMS_BINARY:'/old/program-cms',BONFYRE_WEB_WORK_OWNER:'/canonical/web_work.mjs',
  BONFYRE_EXTERNAL_READS_ENABLED:'0',BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN:'7',BONFYRE_WEB_CREDENTIALS_DB:'/canonical/credentials.db'
 });
 const {configuration}=configurePrograms(f.before,f.release,{bun:process.execPath,cuePath:f.bin}),env=configuration.EnvironmentVariables;
 expect(env.BONFYRE_PROGRAM_OWNER).toBe(join(f.release,'native/scripts/program-workspace-owner.mjs'));
 expect(env.BONFYRE_PROGRAM_SOURCE_OWNER).toBe(join(f.release,'native/scripts/program-source-work.mjs'));
 expect(env.BONFYRE_PROGRAM_CMS_BINARY).toBe(join(f.release,'native/cmd/BonfyreCMS/bonfyre-cms'));
 for(const name of ['BONFYRE_CMS_BINARY','BONFYRE_CMS_DB','BONFYRE_SOURCE_FRONTIER_DB','BONFYRE_WEB_WORK_OWNER','BONFYRE_WEB_CREDENTIALS_DB','BONFYRE_EXTERNAL_READS_ENABLED','BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN'])expect(env[name]).toBe(f.before.EnvironmentVariables[name]);
});

test('refuses changed frozen closure, rollback bytes, runtime bytes and checkout inputs',()=>{
 const f=fixture(),{stage,frozen}=stageFixture(f);
 expect(inspectRelease(f.release).directory).toBe(f.release);
 verifyInputs(stage,f.source);
 const owner=readFileSync(frozen.owner);put(frozen.owner,'changed frozen owner\n');
 expect(()=>inspectRelease(f.release)).toThrow();put(frozen.owner,owner);
 const backup=readFileSync(join(f.release,'previous-service.plist'));put(join(f.release,'previous-service.plist'),'changed rollback\n');
 expect(()=>inspectRelease(f.release)).toThrow();put(join(f.release,'previous-service.plist'),backup);
 const runtime=readFileSync(f.bun);put(f.bun,'#!/bin/sh\nexit 2\n',0o755);
 expect(()=>inspectRelease(f.release)).toThrow();put(f.bun,runtime,0o755);
 for(const name of ['aurekai-push.mjs','aurekai-push-sw.mjs','provider-contract.mjs','aurekai-model.mjs','aurekai-model.css']){
  const incomplete=structuredClone(stage);incomplete.material=incomplete.material.filter(row=>row.path!=='browser/'+name);
  put(join(f.release,'stage.mpk'),encode(incomplete),0o600);expect(()=>inspectRelease(f.release)).toThrow();
 }
 const unhashed=structuredClone(stage);unhashed.inputs=unhashed.inputs.filter(row=>row.path!=='integrations/browser/provider-contract.mjs');
 put(join(f.release,'stage.mpk'),encode(unhashed),0o600);expect(()=>inspectRelease(f.release)).toThrow();
 put(join(f.release,'stage.mpk'),encode(stage),0o600);
 put(join(f.source,'scripts/program-fabric.cue'),'changed compiler policy\n');
 expect(()=>verifyInputs(stage,f.source)).toThrow();
});

test('frozen release verification is bound to its own accepted stage instead of later source inventory growth',()=>{
 const f=fixture(),{stage}=stageFixture(f);
 const historical=structuredClone(stage);
 historical.inputs=historical.inputs.filter(row=>row.path!=='cmd/BonfyreInteractionGrammar/src/main.c');
 historical.material=historical.material.filter(row=>row.path!=='native/cmd/BonfyreInteractionGrammar/src/main.c');
 put(join(f.release,'stage.mpk'),encode(historical),0o600);
 expect(()=>inspectRelease(f.release)).toThrow();
 expect(inspectRelease(f.release,{requireCurrentInventory:false}).directory).toBe(f.release);

 const stageHash=sha(readFileSync(join(f.release,'stage.mpk'))),acceptanceKey='a'.repeat(64),groupKey='b'.repeat(64),group=['fixture.test.mjs'];
 put(join(f.release,'acceptance-0.log'),'historical acceptance passed\n',0o600);
 const logSha256=sha(readFileSync(join(f.release,'acceptance-0.log')));
 put(join(f.release,'acceptance-0.mpk'),encode({stageHash,acceptanceKey,groupKey,group,logSha256,accepted:'2026-10-24T00:00:00Z'}),0o600);
 put(join(f.release,'accepted.mpk'),encode({stageHash,acceptanceKey,logs:[{path:'acceptance-0.log',sha256:logSha256}],accepted:'2026-10-24T00:00:00Z'}),0o600);
 expect(requireFrozenAcceptance(f.release,stageHash)).toMatchObject({stageHash,acceptanceKey,logs:[{path:'acceptance-0.log',sha256:logSha256}]});

 put(join(f.release,'acceptance-0.log'),'tampered\n',0o600);expect(()=>requireFrozenAcceptance(f.release,stageHash)).toThrow();
 put(join(f.release,'acceptance-0.log'),'historical acceptance passed\n',0o600);
 const wrong=structuredClone(historical);wrong.created='changed';put(join(f.release,'stage.mpk'),encode(wrong),0o600);
 expect(()=>requireFrozenAcceptance(f.release,sha(readFileSync(join(f.release,'stage.mpk'))))).toThrow();
});

test('promotion receipt proves the exact frozen route set and refuses altered activation evidence',()=>{
 const f=fixture(),{stage}=stageFixture(f),stageHash=sha(readFileSync(join(f.release,'stage.mpk'))),material=new Map(stage.material.map(row=>[row.path,row.sha256]));
 const companions=new Set(['extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs']);
 const programs=['program-workspace.mjs','program-service-continuations.mjs','program-service-policy.mjs','program-source-work.mjs','program-workspace.css'];
 const base=[
  {route:'/',status:200,sha256:'c'.repeat(64)},
  ...browserAssets.filter(name=>!companions.has(name)).map(name=>({route:'/apps/'+name,status:200,sha256:material.get('browser/'+name)})),
  {route:'/apps/_activity',status:403},
  ...programs.map(name=>({route:'/apps/'+name,status:200,sha256:material.get('browser/'+name)})),
  {route:'/apps/_programs',status:403},
  {route:'/programs?context=self',status:303,location:'/login?next=%2Fprograms%3Fcontext%3Dself'},
 ];
 const hosts=['aurekai.work','aurekai.live','aurekai.life','aurekai.shop'],routes=hosts.flatMap(host=>base.map(row=>({host,...row})));
 const promoted={activated:'2026-10-24T00:00:00Z',stageHash,local:structuredClone(routes),publicRoutes:structuredClone(routes),rollback:join(f.release,'previous-service.plist'),databaseWrites:false};
 put(join(f.release,'promoted.mpk'),encode(promoted),0o600);
 expect(verifyPromotionReceipt(f.release,stage,stageHash)).toMatchObject({stageHash,databaseWrites:false});
 const altered=structuredClone(promoted);altered.publicRoutes.find(row=>row.route.startsWith('/apps/')&&row.sha256).sha256='0'.repeat(64);put(join(f.release,'promoted.mpk'),encode(altered),0o600);
 expect(()=>verifyPromotionReceipt(f.release,stage,stageHash)).toThrow();
 const wrongStage=structuredClone(promoted);wrongStage.stageHash='f'.repeat(64);put(join(f.release,'promoted.mpk'),encode(wrongStage),0o600);
 expect(()=>verifyPromotionReceipt(f.release,stage,stageHash)).toThrow();
});

test('browser readback requires every public asset, serializes the edge load and refuses semantic mismatches without retry',async()=>{
 const f=fixture();freezeBrowser(f.source,f.release);
 const calls=[];let active=0,maxActive=0;
 const request=async(base,path,host)=>{
  calls.push({base,path,host});active++;maxActive=Math.max(maxActive,active);await Bun.sleep(1);active--;
  const name=path.slice('/apps/'.length);
  expect(path.startsWith('/apps/')&&browserAssets.includes(name)).toBe(true);
  return {status:200,type:name.endsWith('.css')?'text/css; charset=utf-8':'application/javascript',sha256:sha(readFileSync(join(f.release,'browser',name)))};
 };
 const rows=await browserReadback('http://127.0.0.1:28901','aurekai.work',f.release,request);
 const publicAssets=browserAssets.filter(name=>!['extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs'].includes(name));
 expect(rows.map(row=>row.route).sort()).toEqual(publicAssets.map(name=>'/apps/'+name).sort());expect(calls.length).toBe(publicAssets.length);expect(maxActive).toBe(1);
 expect(calls.every(call=>call.base==='http://127.0.0.1:28901'&&call.host==='aurekai.work')).toBe(true);
 for(const [path,reply]of [['/apps/aurekai-push.mjs',{status:404}],['/apps/aurekai-push-sw.mjs',{status:200,type:'text/javascript',sha256:'stale-worker'}],['/apps/provider-contract.mjs',{status:200,type:'application/javascript',sha256:'stale-contract'}],['/apps/aurekai-model.css',{status:404}],['/apps/aurekai-model.mjs',{status:200,type:'text/html',sha256:sha(readFileSync(join(f.release,'browser/aurekai-model.mjs')))}]]){
  let badCalls=0;
  await expect(browserReadback('http://127.0.0.1:28901','aurekai.work',f.release,(base,current,host)=>{if(current===path){badCalls++;return Promise.resolve(reply);}return request(base,current,host);})).rejects.toThrow();
  expect(badCalls).toBe(1);
 }
});

test('public readback transport retries only thrown transport failures within its bound',async()=>{
 let calls=0;
 const recovered=await retryReadbackTransport(async(base,path,host)=>{calls++;expect(base).toBe('https://aurekai.live');expect(path).toBe('/apps/connection-studio.mjs');expect(host).toBeUndefined();if(calls<3)throw Error('transient timeout');return{status:200};},'https://aurekai.live','/apps/connection-studio.mjs',undefined,{attempts:3,delayMs:0});
 expect(recovered).toEqual({status:200});expect(calls).toBe(3);
 calls=0;
 await expect(retryReadbackTransport(async()=>{calls++;throw Error('persistent timeout');},'https://aurekai.live','/apps/connection-studio.mjs',undefined,{attempts:3,delayMs:0})).rejects.toThrow('persistent timeout');
 expect(calls).toBe(3);
});

test('browser verification compiles every frozen module and refuses missing imports or companion entries',async()=>{
 const f=fixture();
 for(const name of browserAssets)copyFileSync(join(projectRoot,'integrations/browser',name),join(f.source,'integrations/browser',name));
 freezeBrowser(f.source,f.release);
 await expect(verifyBrowser(f.release)).resolves.toBeUndefined();
 for(const name of ['aurekai-model.mjs','program-service-policy.mjs','connection-source.mjs','publication-picker.mjs','source-publication.mjs','extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs']){
  const model=join(f.release,'browser',name),bytes=readFileSync(model);rmSync(model);
  try{await expect(verifyBrowser(f.release)).rejects.toThrow();}finally{put(model,bytes);}
 }
 await expect(verifyBrowser(f.release)).resolves.toBeUndefined();
});

function readbackFixture(f,overrides={}){
 const paths=['/apps/program-workspace.mjs','/apps/program-service-continuations.mjs','/apps/program-service-policy.mjs','/apps/program-source-work.mjs','/apps/program-workspace.css'];
 for(const path of paths)put(join(f.release,'browser',path.split('/').at(-1)),'fixture '+path+'\n');
 const calls=[];
 const request=async(base,path,host)=>{calls.push({base,path,host});if(overrides[path])return overrides[path];
  if(paths.includes(path)){const bytes=readFileSync(join(f.release,'browser',path.split('/').at(-1)));return {status:200,type:path.endsWith('.css')?'text/css':'text/javascript; charset=utf-8',sha256:sha(bytes),text:bytes.toString()};}
  if(path==='/apps/_programs?context=self&operation=catalog')return {status:403,text:'Sign in'};
  if(path==='/programs?context=self')return {status:303,location:'/login?next=%2Fprograms%3Fcontext%3Dself',text:''};
  throw Error('Unexpected request '+path);
 };return {calls,request};
}
test('Programs readback binds exact assets, refuses anonymous API and preserves sign-in return',async()=>{
 const f=fixture(),{calls,request}=readbackFixture(f);
 const rows=await programReadback('http://127.0.0.1:28901','aurekai.work',f.release,request);
 expect(rows.length).toBe(7);expect(calls.length).toBe(7);
 expect(calls.every(call=>call.base==='http://127.0.0.1:28901'&&call.host==='aurekai.work')).toBe(true);
 for(const [path,reply]of [['/apps/program-workspace.mjs',{status:200,type:'text/javascript',sha256:'wrong'}],['/apps/program-workspace.css',{status:200,type:'text/html',sha256:sha(readFileSync(join(f.release,'browser/program-workspace.css')))}],['/apps/_programs?context=self&operation=catalog',{status:200}],['/programs?context=self',{status:303,location:'/login?next=%2Fother'}]]){
  const bad=readbackFixture(f,{[path]:reply});await expect(programReadback('http://127.0.0.1:28901','aurekai.work',f.release,bad.request)).rejects.toThrow();
 }
});

test('real owners import the frozen compiler, source protocol, shared helpers and native readers',()=>{
 const f=fixture();
 for(const path of closure.filter(path=>!/\/bonfyre-(external-estate|cms)$/.test(path))){const target=join(f.source,path);mkdirSync(dirname(target),{recursive:true});copyFileSync(join(projectRoot,path),target);}
 freezePrograms(f.source,f.release);
 const {configuration}=configurePrograms(f.before,f.release,{bun:process.execPath,cuePath:process.env.PATH});
 const env={...configuration.EnvironmentVariables,HOME:f.home};
 const child=Bun.spawnSync([env.BONFYRE_BUN_BINARY,'--no-env-file','--no-install','--eval',"const owner=await import(process.argv[1]),sources=await import(process.argv[2]),reference=await import(process.argv[3]);if(typeof owner.operate!=='function'||typeof owner.admittedBinding!=='function'||typeof sources.operateProgramSourceWork!=='function'||typeof reference.inspectProgramWorkReference!=='function')throw Error('Missing native owner exports');console.log('frozen-owners-imported');",env.BONFYRE_PROGRAM_OWNER,env.BONFYRE_PROGRAM_SOURCE_OWNER,join(f.release,'native/scripts/program-work-reference.mjs')],{cwd:f.release,env,stdout:'pipe',stderr:'pipe',timeout:15000});
 expect(child.stderr.toString()).toBe('');expect(child.exitCode).toBe(0);expect(child.stdout.toString().trim()).toBe('frozen-owners-imported');
 expect(readFileSync(env.BONFYRE_CMS_DB,'utf8')).toBe('existing canonical database\n');
});

test('candidate acceptance pins retained public owners and detects their changed bytes',()=>{
 const f=fixture(),{configured}=stageFixture(f),env=acceptanceEnvironment(f.release,configured.configuration.EnvironmentVariables,'fixture-playwright',f.bun);
 expect(env.BONFYRE_TEST_CMS_BINARY).toBe(f.before.EnvironmentVariables.BONFYRE_CMS_BINARY);expect(env.BONFYRE_TEST_AUTH_BINARY).toBe(f.before.EnvironmentVariables.BONFYRE_AUTH_BINARY);expect(env.BONFYRE_TEST_SITE_ROOT).toBe(join(f.release,'site'));
 expect(configured.runtimes.map(row=>row.role).sort()).toEqual(['bun','cue','public-auth','public-cms']);
 put(f.before.EnvironmentVariables.BONFYRE_CMS_BINARY,'changed public CMS',0o755);expect(()=>inspectRelease(f.release)).toThrow();
});
test('acceptance children omit changing launcher identity while retaining every other ambient setting',()=>{
 const ambient={CODEX_TOOL_CALL_ID:'first-command',SEMANTIC_SETTING:'candidate',BONFYRE_EXTENSION_CHROMIUM_BINARY:'/reviewed/chrome',PATH:'/reviewed/bin'},args=['/release',{BONFYRE_BUN_BINARY:'/reviewed/bun'},'playwright','/chrome'];
 const first=acceptanceEnvironment(...args,ambient),next=acceptanceEnvironment(...args,{...ambient,CODEX_TOOL_CALL_ID:'next-command'});
 expect(first).toEqual(next);expect(first).not.toHaveProperty('CODEX_TOOL_CALL_ID');expect(ambient.CODEX_TOOL_CALL_ID).toBe('first-command');
 expect(first.SEMANTIC_SETTING).toBe('candidate');expect(first.BONFYRE_EXTENSION_CHROMIUM_BINARY).toBe('/reviewed/chrome');expect(first.PATH).toBe('/release/runtime/bin:/reviewed/bin');
 expect(acceptanceEnvironment(...args,{...ambient,SEMANTIC_SETTING:'changed'})).not.toEqual(first);
});
test('installed-extension acceptance prefers explicit or cached Chrome for Testing instead of implicit branded Chrome',()=>{
 const f=fixture(),explicit=join(f.home,'explicit-chrome'),cache=join(f.home,'.cache/puppeteer/chrome/999999/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing');
 put(explicit,'#!/bin/sh\nexit 0\n',0o755);put(cache,'#!/bin/sh\nexit 0\n',0o755);
 expect(findExtensionChromium({BONFYRE_EXTENSION_CHROMIUM_BINARY:explicit},{homeDir:f.home,arch:'arm64'})).toBe(realpathSync(explicit));
 expect(findExtensionChromium({},{homeDir:f.home,arch:'arm64'})).toBe(realpathSync(cache));
 rmSync(explicit);rmSync(cache);expect(findExtensionChromium({},{homeDir:f.home,arch:'arm64'})).toBeNull();
});
test('acceptance identity covers imported fixtures, verifier, groups, environment and runtime bytes',async()=>{
 const f=fixture(),sourceRoot=f.source;put(join(sourceRoot,'gate.mjs'),'export const gate = 1;');put(join(sourceRoot,'fixture.mjs'),'export const value = 1;');put(join(sourceRoot,'test.mjs'),'import {value} from "./fixture.mjs";console.log(value);');
 const args={sourceRoot,verifier:'gate.mjs',groups:[['test.mjs']],env:{MODE:'candidate'},runtimes:[f.bun]},initial=await acceptanceIdentity(args);
 put(join(sourceRoot,'fixture.mjs'),'export const value = 2;');expect(await acceptanceIdentity(args)).not.toBe(initial);put(join(sourceRoot,'fixture.mjs'),'export const value = 1;');
 expect(await acceptanceIdentity({...args,env:{MODE:'checkout'}})).not.toBe(initial);expect(await acceptanceIdentity({...args,groups:[['test.mjs'],['fixture.mjs']]})).not.toBe(initial);
 put(join(sourceRoot,'gate.mjs'),'export const gate = 2;');expect(await acceptanceIdentity(args)).not.toBe(initial);put(join(sourceRoot,'gate.mjs'),'export const gate = 1;');
 put(f.bun,'changed runtime',0o755);expect(await acceptanceIdentity(args)).not.toBe(initial);
 put(join(sourceRoot,'native.c'),'int main(void){return 0;}');put(join(sourceRoot,'fixture.json'),'{}');
 const nativeArgs={...args,groups:[['native.c','fixture.json']]},nativeKey=await acceptanceIdentity(nativeArgs);
 put(join(sourceRoot,'native.c'),'int main(void){return 1;}');expect(await acceptanceIdentity(nativeArgs)).not.toBe(nativeKey);put(join(sourceRoot,'native.c'),'int main(void){return 0;}');
 put(join(sourceRoot,'fixture.json'),'{"changed":true}');expect(await acceptanceIdentity(nativeArgs)).not.toBe(nativeKey);
});
test('activation requires all current acceptance groups and rejects obsolete or altered evidence',()=>{
 const f=fixture(),groups=[['one.test.mjs'],['two.test.mjs']],stageHash='stage',acceptanceKey='current';
 const logs=groups.map((group,i)=>{const path='acceptance-'+i+'.log';put(join(f.release,path),'passed '+i);const logSha256=sha(readFileSync(join(f.release,path)));put(join(f.release,'acceptance-'+i+'.mpk'),encode({stageHash,acceptanceKey,group,logSha256}));return{path,sha256:logSha256};});
 const accepted={stageHash,acceptanceKey,logs};put(join(f.release,'accepted.mpk'),encode(accepted));expect(requireAcceptance(f.release,stageHash,acceptanceKey,groups)).toEqual(accepted);
 expect(()=>requireAcceptance(f.release,stageHash,'new-verifier',groups)).toThrow();expect(()=>requireAcceptance(f.release,stageHash,acceptanceKey,[...groups,['new.test.mjs']])).toThrow();
 put(join(f.release,'accepted.mpk'),encode({...accepted,logs:logs.slice(0,1)}));expect(()=>requireAcceptance(f.release,stageHash,acceptanceKey,groups)).toThrow();put(join(f.release,'accepted.mpk'),encode(accepted));
 put(join(f.release,'acceptance-1.log'),'altered output');expect(()=>requireAcceptance(f.release,stageHash,acceptanceKey,groups)).toThrow();
});
