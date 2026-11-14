// Freeze and promote only the public WebAddress service. Operator admission,
// Auth/CMS owners, application records and other services remain unchanged.
import assert from 'node:assert/strict';
import {constants as fsConstants,readFileSync,readSync,writeFileSync,copyFileSync,cpSync,mkdirSync,mkdtempSync,readdirSync,lstatSync,readlinkSync,realpathSync,symlinkSync,renameSync,rmSync,chmodSync,accessSync,openSync,closeSync,existsSync,constants} from 'node:fs';
import {homedir} from 'node:os';
import {join,resolve,relative,dirname,isAbsolute} from 'node:path';
import {createHash} from 'node:crypto';
import {encode,decode} from '../../integrations/browser/workspace-msgpack.mjs';import {frontIntegrity} from './front_integrity.mjs';
import {selectMemberJournalCanaries,verifyMemberJournalReadback} from './member_journal_canary.mjs';
import {inspectApex,inspectStylesheet} from '../native_apex_live_readback.mjs';
const root=resolve(import.meta.dir,'../..'),home=homedir(),releases=join(home,'.bonfyre/releases');
const label='com.bonfyre.public-web-address',domain='gui/'+process.getuid(),plist=join(home,'Library/LaunchAgents',label+'.plist');
// A carried program whose link target was removed (a self-updating CLI such as
// claude replaces its versioned file) is re-resolved from PATH, never dropped.
export function carriedBin(path,name){try{return terminalBin(path);}catch(error){if(error?.code!=='ENOENT')throw error;const found=Bun.which(name);assert(found,'Carried program '+name+' no longer resolves and is not on PATH.');return terminalBin(found);}}
const digest=bytes=>createHash('sha256').update(bytes).digest('hex'),hash=file=>digest(readFileSync(file));export function terminalBin(path){const seen=new Set();let p=path;while(lstatSync(p).isSymbolicLink()){assert(!seen.has(p)&&seen.size<1024,'Cyclic or excessive binary link chain.');seen.add(p);p=resolve(dirname(p),readlinkSync(p));}assert(lstatSync(p).isFile(),'Binary target is not a file.');return p;}
function run(argv,{env=process.env,allowed=[0],timeout=180000}={}){
 const result=Bun.spawnSync(argv,{cwd:root,env,stdout:'pipe',stderr:'pipe',timeout});
 assert(allowed.includes(result.exitCode),argv[0]+' failed: '+result.stderr.toString().slice(-3000));return result.stdout.toString();
}
const parsePlist=file=>JSON.parse(run(['/usr/bin/plutil','-convert','json','-o','-',file]));
function save(file,value){writeFileSync(file,encode(value),{mode:0o600,flag:'wx'});}
const load=file=>decode(readFileSync(file));
function selectedDirectory(value){const dir=realpathSync(value);assert(relative(releases,dir)&&!relative(releases,dir).startsWith('..')&&lstatSync(dir).isDirectory(),'Choose an existing release directory.');return dir;}
function filesUnder(dir,prefix=''){
 const result=[];for(const entry of readdirSync(join(dir,prefix),{withFileTypes:true})){
  const name=join(prefix,entry.name),path=join(dir,name);assert(!lstatSync(path).isSymbolicLink(),'Release material cannot contain symlinks.');
  if(entry.isDirectory())result.push(...filesUnder(dir,name));else if(entry.isFile())result.push(name);else throw Error('Unexpected release member.');
 }return result.sort();
}
const programSources=['scripts/program-workspace-owner.mjs','scripts/program-source-work.mjs','scripts/program-work-reference.mjs','scripts/project-program-service-policy.mjs','cmd/BonfyreInteractionGrammar/src/main.c','scripts/workspace_material.mjs','integrations/browser/workspace-continuation.mjs','scripts/compile-program-fabric.mjs','scripts/program-fabric.cue','integrations/browser/workspace-msgpack.mjs','integrations/browser/provider-contract.mjs','cmd/BonfyreSourceFrontier/tools/web_work.mjs','cmd/BonfyreSourceFrontier/tools/web_protocol.mjs','cmd/BonfyreSourceFrontier/tools/source_material.mjs','cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs','cmd/BonfyreExternalEstate/bonfyre-external-estate','cmd/BonfyreCMS/bonfyre-cms','cmd/BonfyreCredentials/provider-credentials.json'];
const programAssets=['program-workspace.mjs','program-service-continuations.mjs','program-service-policy.mjs','program-source-work.mjs','program-workspace.css'];
const companionSources=['extension/workbench-shell.mjs','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs'];
const browserSources=['aurekai-activity.mjs','aurekai-horde.mjs','aurekai-activity-source.mjs','aurekai-inbox-source.mjs','aurekai-activity.css','workspace-msgpack.mjs','extension/foreign-sign-in.mjs','extension/foreign-sign-in.css','aurekai-push.mjs','aurekai-push-sw.mjs','provider-contract.mjs','aurekai-model.mjs','aurekai-model.css','program-workspace.mjs','program-service-continuations.mjs','program-service-policy.mjs','program-source-work.mjs','program-workspace.css','interaction-collection.mjs','record-comparison.mjs','workspace-store.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-destinations.mjs','reference-picker.mjs','account-choices.mjs','account-choices.css','invitation-choice.mjs','connection-studio.mjs','connection-studio.css','authentication-capability-surface.mjs','provider-auth-capability.mjs','connection-source.mjs','connection-lab.mjs','publication-picker.mjs','publication-research.mjs','source-desk.mjs','source-desk.css','source-publication.mjs','source-publication.css','web-workbench.mjs','web-workbench.css',...companionSources];
function sha256File(file){
 const fd=openSync(file,'r'),buf=Buffer.allocUnsafe(64*1024),h=createHash('sha256');
 try{
  for(let count;(count=readSync(fd,buf,0,buf.length,null))>0;)h.update(buf.subarray(0,count));
  return h.digest('hex');
 }finally{closeSync(fd);}
}
function frozenInput(projectRoot,dir,sourcePath,targetPath,mode=0o644){
 const source=join(projectRoot,sourcePath),target=join(dir,targetPath);
 assert(lstatSync(source).isFile(),'A release input must be a regular file.');
 const sourceSha256=sha256File(source);mkdirSync(dirname(target),{recursive:true});
 rmSync(target,{force:true});copyFileSync(source,target,fsConstants.COPYFILE_FICLONE);
 chmodSync(target,mode);assert.equal(sha256File(target),sourceSha256,'Frozen input changed during staging: '+sourcePath);
 return{path:sourcePath,sha256:sourceSha256};
}
export function freezeSiteAlias(dir){
 const web=join(dir,'bin/bonfyre-web-address'),site=join(dir,'bin/bonfyre-site');
 assert(lstatSync(web).isFile(),'The staged resolver must be a regular file.');
 symlinkSync('bonfyre-web-address',site);
 return {path:'bin/bonfyre-site',sha256:hash(site)};
}
export function uniqueInputs(rows){
 const byPath=new Map();
 for(const row of rows){const prior=byPath.get(row.path);if(prior){assert.equal(prior.sha256,row.sha256,'One source path cannot attest two different byte identities: '+row.path);continue;}byPath.set(row.path,row);}
 return [...byPath.values()];
}
export function freezePrograms(projectRoot,dir){
 const inputs=programSources.map(path=>frozenInput(projectRoot,dir,path,join('native',path),/\/bonfyre-(external-estate|cms)$/.test(path)?0o755:0o644));
 return{inputs,nativeRoot:join(dir,'native'),owner:join(dir,'native/scripts/program-workspace-owner.mjs')};
}
export function freezeBrowser(projectRoot,dir){
 return browserSources.map(name=>frozenInput(projectRoot,dir,join('integrations/browser',name),join('browser',name)));
}
export async function verifyBrowser(dir,sources=browserSources){
 const built=await Bun.build({entrypoints:sources.filter(name=>name.endsWith('.mjs')).map(name=>join(dir,'browser',name)),target:'browser',write:false});
 assert(built.success,'The frozen browser dependency closure is incomplete: '+built.logs.map(log=>log.message).join('\n'));
}
function executable(path){
 if(typeof path!=='string'||!isAbsolute(path))return null;
 try{const resolved=realpathSync(path);assert(lstatSync(resolved).isFile());accessSync(resolved,constants.X_OK);return resolved;}catch{return null;}
}
function findCue(path){
 for(const directory of String(path||'').split(':')){if(!isAbsolute(directory))continue;const found=executable(join(directory,'cue'));if(found)return found;}return null;
}
export function findExtensionChromium(ambient=process.env,{homeDir=home,arch=process.arch}={}){
 const explicit=executable(ambient.BONFYRE_EXTENSION_CHROMIUM_BINARY);if(explicit)return explicit;
 const cache=join(homeDir,'.cache/puppeteer/chrome');
 if(existsSync(cache)){
  const preferred=arch==='arm64'?['chrome-mac-arm64','chrome-mac-x64']:['chrome-mac-x64','chrome-mac-arm64'];
  for(const version of readdirSync(cache).sort().reverse())for(const folder of preferred){
   const found=executable(join(cache,version,folder,'Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing'));if(found)return found;
  }
 }
 const playwrightCaches=[
  ...(typeof ambient.PLAYWRIGHT_BROWSERS_PATH==='string'&&isAbsolute(ambient.PLAYWRIGHT_BROWSERS_PATH)?[ambient.PLAYWRIGHT_BROWSERS_PATH]:[]),
  join(homeDir,'Library/Caches/ms-playwright'),join(homeDir,'.cache/ms-playwright')
 ];
 for(const playwrightCache of [...new Set(playwrightCaches)]){
  if(!existsSync(playwrightCache))continue;
  const folders=arch==='arm64'?['chrome-mac-arm64','chrome-mac','chrome-mac-x64']:['chrome-mac-x64','chrome-mac','chrome-mac-arm64'];
  for(const version of readdirSync(playwrightCache).sort().reverse()){
   if(!/^chromium-[0-9]+$/.test(version))continue;
   for(const folder of folders)for(const [app,binary] of [['Google Chrome for Testing.app','Google Chrome for Testing'],['Chromium.app','Chromium']]){
    const found=executable(join(playwrightCache,version,folder,app,'Contents/MacOS',binary));if(found)return found;
   }
  }
 }
 for(const candidate of ['/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing','/Applications/Chromium.app/Contents/MacOS/Chromium']){
  const found=executable(candidate);if(found)return found;
 }
 for(const name of ['chromium','chromium-browser']){const candidate=Bun.which(name),found=candidate&&executable(candidate);if(found)return found;}
 return null;
}
export function configurePrograms(before,dir,{bun=process.execPath,cuePath=process.env.PATH,accountHome=home}={}){
 const configuration=structuredClone(before),env=configuration.EnvironmentVariables||{};
 assert(typeof env.BONFYRE_CMS_DB==='string'&&isAbsolute(env.BONFYRE_CMS_DB),'The installed service must name its canonical CMS database.');
 assert(lstatSync(realpathSync(env.BONFYRE_CMS_DB)).isFile(),'The canonical CMS database is unavailable.');
 const control=env.BONFYRE_CONTROL_DB||join(home,'.local/share/bonfyre/control.db'); // BonfyreControl's own default
 assert(typeof control==='string'&&isAbsolute(control),'The installed service must name its canonical control database.');
 assert(lstatSync(realpathSync(control)).isFile(),'The canonical control database is unavailable.');
 env.BONFYRE_CONTROL_DB=control;
 assert(typeof env.BONFYRE_SOURCE_FRONTIER_DB==='string'&&isAbsolute(env.BONFYRE_SOURCE_FRONTIER_DB),'The installed service must name its canonical SourceFrontier database.');
 assert(lstatSync(realpathSync(env.BONFYRE_SOURCE_FRONTIER_DB)).isFile(),'The canonical SourceFrontier database is unavailable.');
 const bunBinary=executable(env.BONFYRE_BUN_BINARY||bun);assert(bunBinary,'The Bun runtime must be an absolute executable.');
 const servicePath=env.PATH||'/usr/bin:/bin:/usr/sbin:/sbin',cueSource=findCue(servicePath)||findCue(cuePath);assert(cueSource,'CUE is unavailable for the program owner.');
 const runtimeDirectory=join(dir,'runtime/bin'),cue=join(runtimeDirectory,'cue'),sourceSha256=sha256File(cueSource);
 mkdirSync(runtimeDirectory,{recursive:true});
 copyFileSync(cueSource,cue,fsConstants.COPYFILE_FICLONE|fsConstants.COPYFILE_EXCL);
 chmodSync(cue,0o755);assert.equal(sha256File(cue),sourceSha256,'Frozen CUE runtime differs from its attested source.');
 // Only cue lives here; resolving it cannot shadow unrelated service commands.
 env.PATH=runtimeDirectory+':'+servicePath;
 assert.equal(findCue(env.PATH),cue,'The deployed service must resolve the frozen CUE executable.');
 env.BONFYRE_BUN_BINARY=bunBinary;env.BONFYRE_PROGRAM_OWNER=join(dir,'native/scripts/program-workspace-owner.mjs');
 env.BONFYRE_PROGRAM_SOURCE_OWNER=join(dir,'native/scripts/program-source-work.mjs');
 env.BONFYRE_PROGRAM_CMS_BINARY=join(dir,'native/cmd/BonfyreCMS/bonfyre-cms');env.BONFYRE_WEB_WORK_OWNER=env.BONFYRE_WEB_WORK_OWNER||join(dir,'native/cmd/BonfyreSourceFrontier/tools/web_work.mjs');
 const accountIndex=env.BONFYRE_ACCOUNT_WORK_DB||join(accountHome,'.bonfyre/state/account-work.db');
 assert(!env.BONFYRE_ACCOUNT_WORK_DB||isAbsolute(accountIndex),'Account Work database must use an absolute path.');
 if(env.BONFYRE_ACCOUNT_WORK_DB)assert(existsSync(accountIndex),'Explicit account Work database is unavailable.');
 if(existsSync(accountIndex)){
  assert(lstatSync(accountIndex).isFile(),'Account Work index must be a regular file.');
  env.BONFYRE_ACCOUNT_WORK_DB=accountIndex;
  env.BONFYRE_METER_DB??=join(accountHome,'.local/share/bonfyre/state/bonfyre-meter.db');if(!existsSync(env.BONFYRE_METER_DB))delete env.BONFYRE_METER_DB;else assert(isAbsolute(env.BONFYRE_METER_DB)&&lstatSync(env.BONFYRE_METER_DB).isFile(),'Meter database must be an existing absolute file.');env.BONFYRE_ACCOUNT_WORK_SOURCES=[... [['operator','operator/operator.sqlite3'],['fabric','dev-fabric/fabric.sqlite3'],['workgraph','edge-workgraph/workgraph.db'],['workgraph','estate/native-architecture-workgraph.db'],['workgraph','estate-fabric/fabric.db']].map(([kind,path])=>[kind,join(accountHome,'.bonfyre',path)]),... (env.BONFYRE_METER_DB?[['meter',env.BONFYRE_METER_DB]]:[])].filter(([,path])=>existsSync(path)).map(([kind,path])=>kind+'='+path).join(';');
 }
 const workOwner=env.BONFYRE_ACCOUNT_WORK_OWNER||process.env.BONFYRE_ACCOUNT_WORK_OWNER;
 if(workOwner){
  assert(/^user:[1-9][0-9]{0,17}$/.test(workOwner),'The account work owner must be one user subject.');
  assert(env.BONFYRE_ACCOUNT_WORK_DB,'An account work owner requires an existing account Work database.');
  env.BONFYRE_ACCOUNT_WORK_OWNER=workOwner;
 }
 configuration.EnvironmentVariables=env;
 const owners=['CMS','AUTH'].map(name=>{const path=executable(env['BONFYRE_'+name+'_BINARY']);assert(path,'The retained public '+name+' owner is unavailable.');return{role:'public-'+name.toLowerCase(),path,sha256:hash(path)};});
 return{configuration,runtimes:[{role:'bun',path:bunBinary,sha256:hash(bunBinary)},{role:'cue',path:cue,sha256:hash(cue)},...owners]};
}
export function verifyInputs(stage,sourceRoot=root){
 for(const row of stage.inputs)assert.equal(hash(join(sourceRoot,row.path)),row.sha256,'Source changed before activation: '+row.path);
}
export function inspectRelease(dir,{requireCurrentInventory=true}={}){
 const stage=load(join(dir,'stage.mpk'));assert.equal(stage.kind,'public-surface-release-v2','Stage this Programs release before acceptance.');assert.equal(stage.directory,dir);
 assert.deepEqual(stage.runtimes?.map(row=>row.role).sort(),['bun','cue','public-auth','public-cms'],'Program runtimes and retained public owners must be attested.');
 assert(Array.isArray(stage.inputs)&&stage.inputs.length,'Release inputs must be a non-empty manifest.');const reviewedInputs=uniqueInputs(stage.inputs);if(requireCurrentInventory)assert.equal(reviewedInputs.length,stage.inputs.length,'Current release inputs must be unique.');
 assert(Array.isArray(stage.material)&&stage.material.length&&new Set(stage.material.map(row=>row.path)).size===stage.material.length,'Release material must be a non-empty unique manifest.');
 for(const row of [...stage.inputs,...stage.material])assert(typeof row.path==='string'&&row.path&&!isAbsolute(row.path)&&!row.path.split('/').some(part=>!part||part==='.'||part==='..')&&/^[a-f0-9]{64}$/.test(row.sha256),'Release manifest row is malformed.');
 if(requireCurrentInventory){
  for(const path of [...programSources.map(path=>'native/'+path),...browserSources.map(path=>'browser/'+path),'runtime/bin/cue','bin/bonfyre-site','bin/bonfyre-meter','candidate-service.plist','previous-service.plist'])assert(stage.material.some(row=>row.path===path),'Missing frozen release member: '+path);
  for(const path of [...programSources,...browserSources.map(path=>'integrations/browser/'+path)])assert(stage.inputs.some(row=>row.path===path),'Missing reviewed release input: '+path);
 }
 if(requireCurrentInventory){
  const site=join(dir,'bin/bonfyre-site');
  assert(lstatSync(site).isSymbolicLink()&&readlinkSync(site)==='bonfyre-web-address',
   'Frozen bonfyre-site must be a relative alias to frozen WebAddress.');
  assert.equal(hash(site),hash(join(dir,'bin/bonfyre-web-address')),
   'Frozen site product differs from the resolver.');
 }
 assert.equal(hash(join(dir,'previous-service.plist')),stage.previousHash,'The retained rollback configuration changed.');
 for(const row of stage.material)assert.equal(hash(join(dir,row.path)),row.sha256,'Changed frozen material: '+row.path);
 for(const row of stage.runtimes||[]){assert.equal(executable(row.path),row.path,'The reviewed runtime is unavailable: '+row.role);assert.equal(hash(row.path),row.sha256,'The reviewed runtime changed: '+row.role);}
 return stage;
}
async function stage(){
 run([process.execPath,'scripts/project-program-service-policy.mjs','--check']);
 run(['node','cmd/BonfyreWebAddress/tools/compile_brand_cuts.mjs','--check']);
 run(['make','-C','cmd/BonfyreCMS','bonfyre-cms']);run(['make','-C','cmd/BonfyreMeter','bonfyre-meter']);run(['make','-C','cmd/BonfyreQueue','bonfyre-queue']);run(['make','-C','cmd/BonfyreExternalEstate','bonfyre-external-estate']);run(['make','-C','cmd/BonfyreProcurement','bonfyre-procurement']);run(['make','-C','cmd/BonfyreWebAddress','bonfyre-web-address','campaign-workspace-witness','native-publications-query-witness','people-projection-witness','people-static-manifest-witness']);
 const previousBytes=readFileSync(plist),previousHash=digest(previousBytes);
 mkdirSync(releases,{recursive:true});const dir=mkdtempSync(join(releases,'activity-public-20261021-'));chmodSync(dir,0o700);
 writeFileSync(join(dir,'previous-service.plist'),previousBytes,{mode:0o600,flag:'wx'});
 const before=parsePlist(join(dir,'previous-service.plist')),args=before.ProgramArguments,env=before.EnvironmentVariables||{};
 assert.equal(before.Label,label);assert(args.includes('--site-root')&&args.includes('--port'));assert(env.BONFYRE_WORKSPACE_CLIENT&&env.BONFYRE_BIN_DIR);
 cpSync(join(root,'site/matrix'),join(dir,'site'),{recursive:true,dereference:true,mode:fsConstants.COPYFILE_FICLONE,filter:p=>!p.split('/').includes('node_modules')}); // from the repo: the running release's site carried edits nowhere
 assert.equal(hash(join(dir,'site/front-brand.css')),hash(join(root,'site/matrix/front-brand.css')),'Staged native brand CSS differs from source bytes.');
 cpSync(env.BONFYRE_WORKSPACE_CLIENT,join(dir,'browser'),{recursive:true,dereference:true,mode:fsConstants.COPYFILE_FICLONE});mkdirSync(join(dir,'bin')); // APFS clones: releases share unchanged site/browser blocks
 const inputs=freezeBrowser(root,dir);
 for(const name of readdirSync(env.BONFYRE_BIN_DIR)){if(name==='bonfyre-site'||name==='bonfyre-meter'||name==='bonfyre-queue'||name==='bonfyre-web-address'||name==='bonfyre-procurement'||name==='bonfyre-credentials'||name==='bonfyre-provider-estate')continue;symlinkSync(carriedBin(join(env.BONFYRE_BIN_DIR,name),name),join(dir,'bin',name));}
 for(const [family,name]of [['BonfyreQueue','bonfyre-queue'],['BonfyreMeter','bonfyre-meter'],['BonfyreProcurement','bonfyre-procurement'],['BonfyreWebAddress','bonfyre-web-address'],['BonfyreCredentials','bonfyre-credentials'],['BonfyreProviderEstate','bonfyre-provider-estate']])inputs.push(frozenInput(root,dir,join('cmd',family,name),join('bin',name),0o755));
 const siteAlias=freezeSiteAlias(dir);
 inputs.push(...freezePrograms(root,dir).inputs);
 const {configuration:candidate,runtimes}=configurePrograms(before,dir);candidate.ProgramArguments[0]=join(dir,'bin/bonfyre-web-address');
 {const w=candidate.ProgramArguments.indexOf('--workers'),want=Math.min(16,Math.max(+(process.env.BONFYRE_PUBLIC_WORKERS||6),w>=0?+candidate.ProgramArguments[w+1]||1:1));if(w>=0)candidate.ProgramArguments[w+1]=String(want);else candidate.ProgramArguments.push('--workers',String(want));}
 // Two workers queued every home behind crawler item pages; a cold release then failed its readback.
 candidate.ProgramArguments[candidate.ProgramArguments.indexOf('--site-root')+1]=join(dir,'site');candidate.EnvironmentVariables.BONFYRE_WORKSPACE_CLIENT=join(dir,'browser');candidate.EnvironmentVariables.BONFYRE_BIN_DIR=join(dir,'bin');candidate.EnvironmentVariables.BONFYRE_PROCUREMENT_BINARY=join(dir,'bin/bonfyre-procurement');candidate.EnvironmentVariables.BONFYRE_CREDENTIALS_BINARY=join(dir,'bin/bonfyre-credentials');candidate.EnvironmentVariables.BONFYRE_PROVIDER_ESTATE_BINARY=join(dir,'bin/bonfyre-provider-estate');candidate.EnvironmentVariables.BONFYRE_CMS_BINARY=join(dir,'native/cmd/BonfyreCMS/bonfyre-cms');candidate.EnvironmentVariables.BONFYRE_CREDENTIALS_PROVIDERS=join(dir,'native/cmd/BonfyreCredentials/provider-credentials.json');// CMS and the provider list ship with the pages; database paths stay pinnedcandidate.EnvironmentVariables.BONFYRE_DETAIL_DEPTH_DB??=join(process.env.HOME,'.bonfyre/estate-local/detail-depth.db');
 // plutil is the existing system format owner; the intermediate stays private.
 const temporary=join(dir,'.candidate');writeFileSync(temporary,JSON.stringify(candidate),{mode:0o600});run(['/usr/bin/plutil','-convert','xml1','-o',join(dir,'candidate-service.plist'),temporary]);rmSync(temporary);
 const material=[...filesUnder(join(dir,'site')).map(p=>'site/'+p),...filesUnder(join(dir,'browser')).map(p=>'browser/'+p),...filesUnder(join(dir,'native')).map(p=>'native/'+p),...filesUnder(join(dir,'runtime')).map(p=>'runtime/'+p),'bin/bonfyre-queue','bin/bonfyre-meter','bin/bonfyre-procurement','bin/bonfyre-web-address','bin/bonfyre-credentials','bin/bonfyre-provider-estate','candidate-service.plist','previous-service.plist'].map(path=>({path,sha256:hash(join(dir,path))})).concat(siteAlias);
 assert.equal(hash(plist),previousHash,'The current service changed during staging.');
 save(join(dir,'stage.mpk'),{kind:'public-surface-release-v2',directory:dir,previousHash,inputs:uniqueInputs(inputs),material,runtimes,created:new Date().toISOString()});
 console.log('STAGED '+dir+'\nWebAddress '+hash(join(dir,'bin/bonfyre-web-address'))+'\nQueue '+hash(join(dir,'bin/bonfyre-queue')));
 return dir;
}
export function acceptanceEnvironment(dir,runtimeEnv,playwrightModule,chromeBinary,ambient=process.env){
 const env={...ambient};delete env.CODEX_TOOL_CALL_ID; // Per-command launcher identity is not passed to acceptance children.
 return {...env,PATH:join(dir,'runtime/bin')+':'+(ambient.PATH||'/usr/bin:/bin:/usr/sbin:/sbin'),BONFYRE_PLAYWRIGHT_MODULE:playwrightModule,BONFYRE_CHROMIUM_BINARY:chromeBinary,BONFYRE_EXTENSION_CHROMIUM_BINARY:ambient.BONFYRE_EXTENSION_CHROMIUM_BINARY||chromeBinary,BONFYRE_TEST_BROWSER:ambient.BONFYRE_TEST_BROWSER||chromeBinary,BONFYRE_BUN_BINARY:runtimeEnv.BONFYRE_BUN_BINARY,BONFYRE_TEST_CMS_BINARY:runtimeEnv.BONFYRE_CMS_BINARY,BONFYRE_TEST_AUTH_BINARY:runtimeEnv.BONFYRE_AUTH_BINARY,BONFYRE_TEST_PUBLIC_BROWSER:join(dir,'browser'),BONFYRE_TEST_WORKSPACE_CLIENT:join(dir,'browser'),BONFYRE_TEST_SITE_ROOT:join(dir,'site'),BONFYRE_TEST_WEB_BINARY:join(dir,'bin/bonfyre-web-address'),BONFYRE_TEST_QUEUE:join(dir,'bin/bonfyre-queue'),BONFYRE_TEST_PROGRAM_CMS_BINARY:runtimeEnv.BONFYRE_PROGRAM_CMS_BINARY,BONFYRE_TEST_PACK_ESTATE_BINARY:join(dir,"native/cmd/BonfyreExternalEstate/bonfyre-external-estate")};
}
export async function acceptanceIdentity({groups,env,sourceRoot=root,verifier='scripts/release/public_surface_rollout.mjs',runtimes=[]}){
 const built=await Bun.build({entrypoints:[verifier,...groups.flat().filter(path=>path.endsWith('.mjs'))].map(path=>resolve(sourceRoot,path)),root:sourceRoot,target:'bun',packages:'external',external:['/apps/*','https://*','http://*'],metafile:true,write:false});
 assert(built.success,'Acceptance dependency scan failed: '+built.logs.map(log=>log.message).join('\n'));
 const inputs=[...new Set([...Object.keys(built.metafile.inputs).map(path=>resolve(path)),...groups.flat().filter(path=>!path.endsWith('.mjs')).map(path=>resolve(sourceRoot,path))])].sort().map(path=>({path:relative(sourceRoot,path),sha256:hash(path)}));
 return digest(Buffer.from(JSON.stringify({groups,inputs,env:Object.entries(env).filter(([,v])=>v!==undefined).sort(([a],[b])=>a.localeCompare(b)),runtimes:runtimes.map(path=>({path,sha256:hash(path)})),scope:'static local imports and declared native inputs; computed imports, undeclared runtime fixtures and external packages are not attested'})));
}
export function requireAcceptance(dir,stageHash,acceptanceKey,groups){
 const accepted=load(join(dir,'accepted.mpk'));assert.equal(accepted.stageHash,stageHash);assert.equal(accepted.acceptanceKey,acceptanceKey,'Acceptance dependencies or environment changed; verify again.');
 const logs=groups.map((group,i)=>{const path='acceptance-'+i+'.log',receipt=load(join(dir,'acceptance-'+i+'.mpk'));assert.equal(receipt.stageHash,stageHash);assert.equal(receipt.acceptanceKey,acceptanceKey);assert.deepEqual(receipt.group,group);assert.equal(hash(join(dir,path)),receipt.logSha256);return{path,sha256:receipt.logSha256};});
 assert.deepEqual(accepted.logs,logs,'Every current acceptance group is required.');return accepted;
}
export function requireFrozenAcceptance(dir,stageHash){
 const accepted=load(join(dir,'accepted.mpk'));assert.equal(accepted.stageHash,stageHash);assert(/^[a-f0-9]{64}$/.test(accepted.acceptanceKey||''),'Frozen acceptance identity is malformed.');
 assert(Array.isArray(accepted.logs)&&accepted.logs.length>0&&accepted.logs.length<=64,'Frozen acceptance must retain bounded logs.');
 const seen=new Set();
 for(const row of accepted.logs){
  assert(/^acceptance-[0-9]+\.log$/.test(row.path)&&/^[a-f0-9]{64}$/.test(row.sha256),'Frozen acceptance log row is malformed.');
  assert(!seen.has(row.path),'Frozen acceptance log is duplicated.');seen.add(row.path);
  assert.equal(hash(join(dir,row.path)),row.sha256,'Frozen acceptance log changed: '+row.path);
  const receipt=load(join(dir,row.path.replace(/\.log$/,'.mpk')));
  assert.equal(receipt.stageHash,stageHash);assert.equal(receipt.acceptanceKey,accepted.acceptanceKey);assert.equal(receipt.logSha256,row.sha256);
  assert(/^[a-f0-9]{64}$/.test(receipt.groupKey||'')&&Array.isArray(receipt.group)&&receipt.group.length>0,'Frozen acceptance receipt is malformed.');
 }
 return accepted;
}
export function acceptanceGroups(){
 const groups=[['cmd/BonfyreWebAddress/tests/public_surface_assets.test.mjs','cmd/BonfyreWebAddress/tests/cms_editor_journey.test.mjs','integrations/browser/tests/native-workspace.test.mjs','cmd/BonfyreWebAddress/tests/account_activity_web.test.mjs','cmd/BonfyreWebAddress/tests/account_inbox_web.test.mjs','integrations/browser/tests/aurekai-inbox-source.test.mjs','cmd/BonfyreWebAddress/tests/account_activity_chrome.test.mjs'],['cmd/BonfyreWebAddress/tests/public_code_test.mjs','cmd/BonfyreWebAddress/tests/public_audio_deck_test.mjs','cmd/BonfyreWebAddress/tests/session_handoff_web.test.mjs']];
 /* Browser-backed suites must not share one Bun test process: each opens a
  * separate native CMS and Chrome fixture, and parallel file execution can
  * terminate another file's browser before its continuation assertions. */
 for(const suite of ['cmd/BonfyreWebAddress/tests/page_preview_diversity.test.mjs','cmd/BonfyreWebAddress/tests/community_journey.test.mjs','cmd/BonfyreWebAddress/tests/work_entrance_test.mjs'])groups.push([suite]);
 groups.push(['scripts/tests/source-desk-browser.test.mjs']);groups.push(['scripts/tests/campaign-workspace.test.mjs']);groups.push(['cmd/BonfyreWebAddress/tests/web_workbench.browser.test.mjs']);groups.push(['scripts/tests/interaction-components-browser.test.mjs','cmd/BonfyreWebAddress/tests/account_choice_boundaries.test.mjs','cmd/BonfyreWebAddress/tests/account_choices.test.mjs']);
 groups.push(['integrations/browser/tests/account-connections.test.mjs','integrations/browser/tests/enrollment-continuation.test.mjs','integrations/browser/tests/enrollment-integrity.test.mjs','integrations/browser/tests/provider-continuation.test.mjs']);
 groups.push(['cmd/BonfyreWebAddress/tests/program_workspace.test.mjs','scripts/tests/program-workspace-owner.test.mjs','scripts/tests/program-source-work.test.mjs','integrations/browser/tests/program-service-continuations.test.mjs','integrations/browser/tests/program-workspace.browser.test.mjs','cmd/BonfyreWebAddress/tests/installed_subtools_test.mjs','scripts/release/tests/public_surface_rollout.test.mjs']);
 groups.push(['scripts/release/tests/member_journal_all_sites.test.mjs']);
 groups.push(['cmd/BonfyreCMS/tests/program_continuation_test.c','cmd/BonfyreCMS/tests/program_continuation_test.mk','cmd/BonfyreCMS/bonfyre-cms','cmd/BonfyreExternalEstate/bonfyre-external-estate','cmd/BonfyreExternalEstate/src/main.c',...['bf_json.c','bf_jsonw.c','bf_msgpack.c','bf_utf8.c','src/bf_sha256.c','include/bf_json.h','include/bf_jsonw.h','include/bf_msgpack.h','include/bf_utf8.h','include/bonfyre.h','include/bonfyre/bf_discipl.h'].map(path=>'lib/libbonfyre/'+path),...['pack_artifact','pack_version','pack_head','pack_dependency','pack_overlay','pack_demand','pack_installation','pack_activation','application_binding'].map(name=>'cmd/BonfyreCMS/content-types/'+name+'.json')]);
 // Each browser fixture owns its Playwright transport in a separate test process.
 for(const file of ['integrations/browser/tests/foreign-sign-in.test.mjs','integrations/browser/tests/foreign-sign-in.browser.test.mjs','integrations/browser/tests/foreign-sign-in-work.browser.test.mjs','integrations/browser/tests/connection-studio-signin.browser.test.mjs','integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs','integrations/browser/tests/aurekai-activity.browser.test.mjs','integrations/browser/tests/provider-continuation.browser.test.mjs','integrations/browser/tests/enrollment-live.test.mjs'])groups.push([file]);
 return groups;
}
// Group receipts carry across candidates when the release content, the group's tests and their imports, and the BONFYRE_* environment are identical.
const carryEnv=(env,dir)=>Object.fromEntries(Object.entries(env).filter(([k])=>k.startsWith('BONFYRE_')||k==='PATH').map(([k,v])=>[k,String(v).replaceAll(dir,'<release>')]));
async function carryKeys(dir,stage,groups,env,runtimes){const content=digest(Buffer.from(JSON.stringify({inputs:stage.inputs,material:stage.material.filter(r=>!/candidate-service\.plist$/.test(r.path||'')),runtimes:stage.runtimes}).replaceAll(dir,'<release>')));return Promise.all(groups.map(async group=>digest(Buffer.from(content+await acceptanceIdentity({groups:[group],env:carryEnv(env,dir),runtimes})))));}
function carriedLog(dir,carryKey){for(const name of readdirSync(releases)){const other=join(releases,name);if(other===dir||!name.startsWith('activity-public-')||!existsSync(join(other,'stage.mpk')))continue;for(const file of readdirSync(other).filter(n=>/^acceptance-[0-9]+\.mpk$/.test(n))){try{const r=load(join(other,file)),log=join(other,file.replace(/\.mpk$/,'.log'));if(r.carryKey===carryKey&&hash(log)===r.logSha256)return log;}catch{}}}return null;}
async function verify(dir){
 const stage=inspectRelease(dir);verifyInputs(stage);await verifyBrowser(dir);
 const candidate=parsePlist(join(dir,'candidate-service.plist')),runtimeEnv={...candidate.EnvironmentVariables,HOME:home};
 const previousEnvironment=parsePlist(join(dir,'previous-service.plist')).EnvironmentVariables;
 run([runtimeEnv.BONFYRE_BUN_BINARY,'--no-env-file','--no-install',join(dir,'native/scripts/project-program-service-policy.mjs'),'--check','--cue',join(dir,'native/scripts/program-fabric.cue'),'--out',join(dir,'browser/program-service-policy.mjs')],{env:runtimeEnv,timeout:30000});
 for(const name of ['BONFYRE_CMS_DB','BONFYRE_SOURCE_FRONTIER_DB'])assert.equal(runtimeEnv[name],previousEnvironment[name],'A canonical database changed: '+name);
 const previousControl=previousEnvironment.BONFYRE_CONTROL_DB||join(home,'.local/share/bonfyre/control.db');
 assert.equal(runtimeEnv.BONFYRE_CONTROL_DB,previousControl,'The canonical control database changed.');
 for(const owner of [runtimeEnv.BONFYRE_PROGRAM_OWNER,runtimeEnv.BONFYRE_PROGRAM_SOURCE_OWNER,join(dir,'native/scripts/program-work-reference.mjs')])run([runtimeEnv.BONFYRE_BUN_BINARY,'--no-env-file','--no-install','-e',"await import(process.argv[1]);",owner],{env:runtimeEnv,timeout:30000});
 const playwrightModule=process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test',chromeBinary=process.env.BONFYRE_CHROMIUM_BINARY||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';assert(executable(chromeBinary),'A full installed Chrome is required for browser acceptance.');
 const extensionChrome=findExtensionChromium(process.env);assert(extensionChrome,'Chrome for Testing or Chromium is required for installed-extension acceptance; branded Chrome is not used as the implicit unpacked-extension runner.');
 const env=acceptanceEnvironment(dir,runtimeEnv,playwrightModule,chromeBinary,{...process.env,BONFYRE_EXTENSION_CHROMIUM_BINARY:extensionChrome});
 const groups=acceptanceGroups();
 const acceptanceKey=await acceptanceIdentity({groups,env,runtimes:[chromeBinary,env.BONFYRE_EXTENSION_CHROMIUM_BINARY,env.BONFYRE_TEST_BROWSER]}),stageHash=hash(join(dir,'stage.mpk')),onlyGroup=arguments.length>1&&Number.isInteger(arguments[1])?arguments[1]:null,logs=[];
 if(onlyGroup!==null)assert(onlyGroup>=0&&onlyGroup<groups.length,'Choose an existing acceptance group.');
 const carry=await carryKeys(dir,stage,groups,env,[chromeBinary,env.BONFYRE_EXTENSION_CHROMIUM_BINARY,env.BONFYRE_TEST_BROWSER]);
 for(let i=0;i<groups.length;i++){
  const path='acceptance-'+i+'.log',logPath=join(dir,path),receiptPath=join(dir,'acceptance-'+i+'.mpk');
  const groupKey=digest(Buffer.from(JSON.stringify({stageHash,acceptanceKey,group:groups[i]})));
  if(existsSync(receiptPath)&&existsSync(logPath)){
   const prior=load(receiptPath);
   if(prior?.stageHash===stageHash&&prior?.acceptanceKey===acceptanceKey&&prior?.groupKey===groupKey&&prior?.logSha256===hash(logPath)){logs.push({path,sha256:prior.logSha256});if(onlyGroup===i)console.log('ACCEPTANCE_GROUP_REUSED '+i);continue;}
  }
  if(onlyGroup!==null&&onlyGroup!==i)continue;
  rmSync(receiptPath,{force:true});rmSync(logPath,{force:true});
  const carriedFrom=onlyGroup===null?carriedLog(dir,carry[i]):null;if(carriedFrom){copyFileSync(carriedFrom,logPath);const logSha256=hash(logPath);save(receiptPath,{stageHash,acceptanceKey,groupKey,carryKey:carry[i],carriedFrom,group:groups[i],logSha256,accepted:new Date().toISOString()});logs.push({path,sha256:logSha256});console.log('ACCEPTANCE_GROUP_CARRIED '+i+' '+carriedFrom);continue;}
  const fd=openSync(logPath,'w',0o600);let timer,code,timedOut=false;
  try{
   const command=groups[i][0]==='cmd/BonfyreCMS/tests/program_continuation_test.c'?['make','-B','-C','cmd/BonfyreCMS','-f','tests/program_continuation_test.mk','test']:[runtimeEnv.BONFYRE_BUN_BINARY,'test',...groups[i]];
   const child=Bun.spawn(command,{cwd:root,env,stdout:fd,stderr:fd});
   timer=setTimeout(()=>{timedOut=true;child.kill();},180000);code=await child.exited;
  }finally{clearTimeout(timer);closeSync(fd);}
  console.log(readFileSync(logPath,'utf8').slice(-9000));assert(!timedOut,'Acceptance timed out; no activation.');assert.equal(code,0,'Acceptance failed; no activation.');
  const logSha256=hash(logPath);save(receiptPath,{stageHash,acceptanceKey,groupKey,carryKey:carry[i],group:groups[i],logSha256,accepted:new Date().toISOString()});logs.push({path,sha256:logSha256});if(onlyGroup===i)console.log('ACCEPTANCE_GROUP '+i);
 }
 if(onlyGroup!==null)return;
 assert.equal(logs.length,groups.length,'Every acceptance group must have a current receipt.');
 verifyInputs(stage);inspectRelease(dir);
 assert.equal(await acceptanceIdentity({groups,env,runtimes:[chromeBinary,env.BONFYRE_EXTENSION_CHROMIUM_BINARY,env.BONFYRE_TEST_BROWSER]}),acceptanceKey,'Acceptance dependencies changed during verification.');rmSync(join(dir,'accepted.mpk'),{force:true});
 save(join(dir,'accepted.mpk'),{stageHash,acceptanceKey,logs,accepted:new Date().toISOString()});console.log('ACCEPTED '+dir);
}
async function request(base,path,host){
 try{const response=await fetch(base+path,{redirect:'manual',headers:host?{Host:host}:undefined,signal:AbortSignal.timeout(10000)});const bytes=Buffer.from(await response.arrayBuffer());return{status:response.status,type:response.headers.get('content-type'),location:response.headers.get('location'),sha256:digest(bytes),text:bytes.toString()};}
 catch(error){throw Error('Readback '+base+path+(host?' (Host '+host+')':'')+' failed: '+error.message,{cause:error});}
}
export async function retryReadbackTransport(requestFn,base,path,host,{attempts=3,delayMs=250}={}){
 assert.equal(typeof requestFn,'function');assert(Number.isInteger(attempts)&&attempts>=1&&attempts<=5);assert(Number.isInteger(delayMs)&&delayMs>=0&&delayMs<=2000);
 let last;
 for(let attempt=1;attempt<=attempts;attempt++){try{return await requestFn(base,path,host);}catch(error){last=error;if(attempt<attempts&&delayMs)await Bun.sleep(delayMs*attempt);}}
 throw last;
}
// Release acceptance is about the composed experience, not merely a 200 or JS mount.
export function assertEstateShell(html,host){
 assert(/<body\b[^>]*\bclass=["'][^"']*\bak-app\b/i.test(html),host+' lost the shared app shell');
 assert(/<header\b[^>]*\bclass=["'][^"']*\bak-bar\b/i.test(html),host+' lost the territory navigation');
 assert(/<main\b/i.test(html),host+' lost the composed main content');
 assert(/<footer\b[^>]*\bclass=["'][^"']*\bak-foot\b/i.test(html),host+' lost the shared footer');
 assert(/\bdata-ak-player\b/i.test(html),host+' lost the persistent narration player');
 for(const old of ['Explore the rest of Aurekai','Conversations to take part in','Your work, in one place'])assert(!html.includes(old),host+' still renders the obsolete demo body: '+old);
}
const requiredEstateStyles=Object.freeze(['aurekai-bar.css','composition.css','aurekai-native.css','profile-journal.css','authoring-reader.css']);
export function assertStyleAsset(asset,dir,name){
 const route='/assets/'+name;
 assert.equal(asset.status,200,route+' is unavailable');
 assert.equal(String(asset.type||'').split(';')[0].trim().toLowerCase(),'text/css',route+' must return CSS');
 assert.equal(asset.sha256,hash(join(dir,'site',name)),route+' differs from the frozen mature stylesheet');
}
function browserAsset(name,asset,dir){
 const route='/apps/'+name,type=String(asset.type||'').split(';')[0].trim().toLowerCase();assert.equal(asset.status,200,route);
 assert((name.endsWith('.css')?['text/css']:['text/javascript','application/javascript']).includes(type),route+' content type');
 assert.equal(asset.sha256,hash(join(dir,'browser',name)),route+' frozen bytes');return{route,status:200,type,sha256:asset.sha256};
}
export async function browserReadback(base,host,dir,requestFn=request,sources=browserSources){
 const results=[];
 for(const name of sources.filter(name=>!companionSources.includes(name)))results.push(browserAsset(name,await requestFn(base,'/apps/'+name,host),dir));
 return results;
}
export async function programReadback(base,host,dir,requestFn=request,assets=programAssets){
 const results=[];
 for(const name of assets)results.push(browserAsset(name,await requestFn(base,'/apps/'+name,host),dir));
 const api=await requestFn(base,'/apps/_programs?context=self&operation=catalog',host);assert.equal(api.status,403,'Anonymous Programs must be refused');results.push({route:'/apps/_programs',status:403});
 const page=await requestFn(base,'/programs?context=self',host);assert.equal(page.status,303,'Programs must retain its sign-in continuation');
 const location=new URL(page.location,base);assert.equal(location.origin,new URL(base).origin);assert.equal(location.pathname,'/login');assert.equal(location.searchParams.get('next'),'/programs?context=self');
 results.push({route:'/programs?context=self',status:303,location:location.pathname+location.search});return results;
}
export function nativeBrandReleaseWitness(host,home,css,expected){
 assert(/^aurekai\.(work|life|live|shop)$/.test(host),'Only the four exact owned apices may be branded.');
 assert.equal(home.status,200,'Native front must answer before admission');
 const inspected=inspectApex(host.slice('aurekai.'.length),home.text);
 assert.equal(css.status,200,'Frozen brand CSS must return HTTP 200');
 assert.equal(String(css.type||'').split(';')[0].trim().toLowerCase(),'text/css','Frozen CSS must have text/css content type');
 const digest=inspectStylesheet(Buffer.from(css.text||''),expected);
 assert.equal(css.sha256,digest,'Public CSS response bytes differ from frozen source');
 return {route:'/assets/front-brand.css',status:200,sha256:digest,version:inspected.version};
}
async function smoke(dir,{publicRead=false,stage=null}={}){
 const conf=parsePlist(join(dir,'candidate-service.plist')),port=conf.ProgramArguments[conf.ProgramArguments.indexOf('--port')+1],results=[];
 const read=publicRead?(base,path,host)=>retryReadbackTransport(request,base,path,host,{attempts:3,delayMs:250}):request;
 const frozenBrowser=stage?stage.inputs.filter(row=>row.path.startsWith('integrations/browser/')).map(row=>row.path.slice('integrations/browser/'.length)).filter(name=>stage.material.some(row=>row.path==='browser/'+name)):browserSources;
 const frozenPrograms=stage?programAssets.filter(name=>frozenBrowser.includes(name)):programAssets;
 for(const host of ['aurekai.work','aurekai.live','aurekai.life','aurekai.shop']){
  const base=publicRead?'https://'+host:'http://127.0.0.1:'+port,header=publicRead?undefined:host;
  const home=await read(base,'/',header);assert.equal(home.status,200,host+' home');assert(home.text.includes('aurekai-activity.mjs'),host+' activity mounting');assertEstateShell(home.text,host);
  await Promise.all(requiredEstateStyles.map(async name=>assertStyleAsset(await read(base,'/assets/'+name,header),dir,name)));
  const edition=inspectApex(host.slice('aurekai.'.length),home.text);
  const asset=await read(base,'/assets/front-brand.css?v='+edition.version,header);
  const witness=nativeBrandReleaseWitness(host,home,asset,readFileSync(join(dir,'site/front-brand.css')));
  results.push({host,route:'/',status:200,sha256:home.sha256},{host,...witness});
  results.push(...(await browserReadback(base,header,dir,read,frozenBrowser)).map(result=>({host,...result})));
  const privateReply=await read(base,'/apps/_activity?context=self&operation=list',header);assert.equal(privateReply.status,403,'Anonymous activity must be refused');results.push({host,route:'/apps/_activity',status:403});
  results.push(...(await programReadback(base,header,dir,read,frozenPrograms)).map(result=>({host,...result})));
 }
 return results;
}
function frozenSurface(stage){const browser=uniqueInputs(stage.inputs).filter(row=>row.path.startsWith('integrations/browser/')).map(row=>row.path.slice('integrations/browser/'.length)).filter(name=>stage.material.some(row=>row.path==='browser/'+name));return{browser,programs:programAssets.filter(name=>browser.includes(name))};}
function expectedPromotionRows(stage){
 const {browser,programs}=frozenSurface(stage),material=new Map(stage.material.map(row=>[row.path,row.sha256]));
 assert(material.get('site/front-brand.css'),'A frozen release without the branded stylesheet cannot be promoted.');
 return[{route:'/',status:200,sha256:material.get('site/index.html')||null},{route:'/assets/front-brand.css',status:200,sha256:material.get('site/front-brand.css')},...browser.filter(name=>!companionSources.includes(name)).map(name=>({route:'/apps/'+name,status:200,sha256:material.get('browser/'+name)})),{route:'/apps/_activity',status:403},...programs.map(name=>({route:'/apps/'+name,status:200,sha256:material.get('browser/'+name)})),{route:'/apps/_programs',status:403},{route:'/programs?context=self',status:303,location:'/login?next=%2Fprograms%3Fcontext%3Dself'}];
}
const promotionRowKey=row=>JSON.stringify([row.route,row.status,row.route==='/'?null:row.sha256||null,row.location||null]);
export function verifyPromotionReceipt(dir,stage,stageHash){
 const promoted=load(join(dir,'promoted.mpk')),expected=expectedPromotionRows(stage).map(promotionRowKey).sort(),hosts=['aurekai.work','aurekai.live','aurekai.life','aurekai.shop'];assert.equal(promoted.stageHash,stageHash,'Promotion belongs to a different frozen stage.');assert.equal(promoted.databaseWrites,false,'Promotion must not claim database writes.');assert.equal(promoted.rollback,join(dir,'previous-service.plist'),'Promotion rollback path changed.');assert(Number.isFinite(Date.parse(promoted.activated)),'Promotion timestamp is malformed.');
 for(const [name,rows]of [['local',promoted.local],['public',promoted.publicRoutes]]){assert(Array.isArray(rows),'Promotion '+name+' readback is missing.');assert(rows.filter(row=>row.route==='/').every(row=>/^[a-f0-9]{64}$/.test(row.sha256||'')),'Promotion '+name+' home digest is malformed.');for(const host of hosts)assert.deepEqual(rows.filter(row=>row.host===host).map(promotionRowKey).sort(),expected,'Promotion '+name+' readback changed for '+host);assert(rows.every(row=>hosts.includes(row.host)),'Promotion '+name+' readback contains an unknown host.');}
 return promoted;
}
async function currentPublicSample(dir,stage){
 assert.equal(hash(plist),hash(join(dir,'candidate-service.plist')),'This frozen release is no longer the active public service configuration.');const {browser}=frozenSurface(stage),sample=['aurekai-activity.mjs','source-desk.mjs','program-workspace.mjs'].filter(name=>browser.includes(name)),material=new Map(stage.material.map(row=>[row.path,row.sha256])),rows=[];
 await Promise.all(['aurekai.work','aurekai.live','aurekai.life','aurekai.shop'].map(async host=>{
  const base='https://'+host,read=path=>retryReadbackTransport(request,base,path,undefined,{attempts:3,delayMs:250}),[homeReply,...assetReplies]=await Promise.all([read('/'),...sample.map(name=>read('/apps/'+name))]);assert.equal(homeReply.status,200,host+' home');assert(homeReply.text.includes('aurekai-activity.mjs'),host+' activity mounting');assertEstateShell(homeReply.text,host);
  await Promise.all(requiredEstateStyles.map(async name=>assertStyleAsset(await read('/assets/'+name),dir,name)));
  const edition=inspectApex(host.slice('aurekai.'.length),homeReply.text);
  const cssReply=await read('/assets/front-brand.css?v='+edition.version);
  const brand=nativeBrandReleaseWitness(host,homeReply,cssReply,readFileSync(join(dir,'site/front-brand.css')));
  rows.push({host,route:'/'},{host,...brand});
  for(let i=0;i<sample.length;i++){const name=sample[i],asset=browserAsset(name,assetReplies[i],dir);assert.equal(asset.sha256,material.get('browser/'+name));rows.push({host,...asset});}
  const [activity,programs,page]=await Promise.all([read('/apps/_activity?context=self&operation=list'),read('/apps/_programs?context=self&operation=catalog'),read('/programs?context=self')]),location=new URL(page.location,base);assert.equal(activity.status,403);assert.equal(programs.status,403);assert.equal(page.status,303);assert.equal(location.pathname,'/login');assert.equal(location.searchParams.get('next'),'/programs?context=self');rows.push({host,route:'/apps/_activity',status:403},{host,route:'/apps/_programs',status:403},{host,route:'/programs?context=self',status:303});
 }));return rows;
}
async function verifyFrozen(dir){
 const stage=inspectRelease(dir,{requireCurrentInventory:false}),stageHash=hash(join(dir,'stage.mpk')),accepted=requireFrozenAcceptance(dir,stageHash),promoted=verifyPromotionReceipt(dir,stage,stageHash),{browser:frozenBrowser}=frozenSurface(stage);await verifyBrowser(dir,frozenBrowser);
 const candidate=parsePlist(join(dir,'candidate-service.plist')),runtimeEnv={...candidate.EnvironmentVariables,HOME:home},previousEnvironment=parsePlist(join(dir,'previous-service.plist')).EnvironmentVariables;for(const name of ['BONFYRE_CMS_DB','BONFYRE_SOURCE_FRONTIER_DB'])assert.equal(runtimeEnv[name],previousEnvironment[name],'A canonical database changed: '+name);for(const owner of [runtimeEnv.BONFYRE_PROGRAM_OWNER,runtimeEnv.BONFYRE_PROGRAM_SOURCE_OWNER,join(dir,'native/scripts/program-work-reference.mjs')])run([runtimeEnv.BONFYRE_BUN_BINARY,'--no-env-file','--no-install','-e',"await import(process.argv[1]);",owner],{env:runtimeEnv,timeout:30000});
 const memberCanaries=selectMemberJournalCanaries(runtimeEnv.BONFYRE_CMS_DB);
 // Older single-site receipts remain verifiable only if that was the entire
 // published member journal census at activation. No new host can disappear.
 const retained=promoted.memberJournals??(promoted.memberJournal
  ?{local:[promoted.memberJournal.local],public:[promoted.memberJournal.public]}:null);
 if(memberCanaries.length){
  assert(retained,'The activated member-journal receipts are missing.');
  for(const scope of ['local','public']){
   const receipts=retained[scope];
   assert(Array.isArray(receipts)&&receipts.length===memberCanaries.length,
    scope+' member journal coverage changed.');
   for(let i=0;i<memberCanaries.length;i++){
    const receipt=receipts[i],canary=memberCanaries[i];
    assert.equal(receipt?.site_id,canary.site_id,scope+' member journal site identity drifted.');
    assert.equal(receipt?.host,canary.host,scope+' member journal host drifted.');
    assert.equal(receipt?.total,canary.total,scope+' member journal census drifted.');
    assert.equal(receipt?.route_sha256,canary.route_sha256,scope+' member journal routes drifted.');
    assert.equal(receipt?.routes_verified,canary.total,scope+' member journal has unchecked articles.');
   }
  }
  for(const canary of memberCanaries)
   await verifyMemberJournalReadback(canary,(base,route,host)=>retryReadbackTransport(request,base,route,host,{attempts:3,delayMs:250}),{publicRead:true});
 }else assert(!retained,'The previously activated member journal disappeared from CMS.');
 const sample=await currentPublicSample(dir,stage);console.log('FROZEN_VERIFIED '+dir+'\n'+JSON.stringify({stage_sha256:stageHash,acceptance_key:accepted.acceptanceKey,acceptance_logs:accepted.logs.length,promoted_at:promoted.activated,current_sample_routes:sample.length}));
}

async function activate(dir){
 const stage=inspectRelease(dir),conf=parsePlist(join(dir,'candidate-service.plist')),chrome=process.env.BONFYRE_CHROMIUM_BINARY||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',extensionChrome=findExtensionChromium(process.env);assert(extensionChrome,'Chrome for Testing or Chromium is required for installed-extension acceptance.');
 const env=acceptanceEnvironment(dir,conf.EnvironmentVariables,process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test',chrome,{...process.env,BONFYRE_EXTENSION_CHROMIUM_BINARY:extensionChrome}),groups=acceptanceGroups();
 const accepted=requireAcceptance(dir,hash(join(dir,'stage.mpk')),await acceptanceIdentity({groups,env,runtimes:[chrome,extensionChrome,env.BONFYRE_TEST_BROWSER]}),groups);verifyInputs(stage);
 for(const log of accepted.logs)assert.equal(hash(join(dir,log.path)),log.sha256);assert.equal(hash(plist),stage.previousHash,'Service changed since review.');
 const before=parsePlist(plist),candidate=parsePlist(join(dir,'candidate-service.plist'));assert.equal(candidate.EnvironmentVariables.BONFYRE_AUTH_BINARY,before.EnvironmentVariables.BONFYRE_AUTH_BINARY);assert(candidate.EnvironmentVariables.BONFYRE_CMS_BINARY===before.EnvironmentVariables.BONFYRE_CMS_BINARY||candidate.EnvironmentVariables.BONFYRE_CMS_BINARY===join(dir,'native/cmd/BonfyreCMS/bonfyre-cms'),'CMS must be unchanged or this release\'s own frozen copy.');assert.equal(candidate.EnvironmentVariables.BONFYRE_CMS_DB,before.EnvironmentVariables.BONFYRE_CMS_DB);
 const candidateHash=hash(join(dir,'candidate-service.plist')),temporary=plist+'.activity-candidate';let stopped=false;
 try{
  copyFileSync(join(dir,'candidate-service.plist'),temporary);run(['/usr/bin/plutil','-lint',temporary]);assert.equal(hash(temporary),candidateHash);assert.equal(hash(plist),stage.previousHash);
  run(['/bin/launchctl','bootout',domain,plist]);stopped=true;renameSync(temporary,plist);run(['/bin/launchctl','bootstrap',domain,plist]);
   {const port=candidate.ProgramArguments[candidate.ProgramArguments.indexOf('--port')+1];console.log('WARM_BEGIN');for(const host of ['aurekai.work','aurekai.live','aurekai.life','aurekai.shop']){let warmed=false,lastError='';for(let i=0;i<8;i++){try{const r=await fetch('http://127.0.0.1:'+port+'/',{headers:{Host:host,Accept:'text/html'},signal:AbortSignal.timeout(90000)});await r.arrayBuffer();if(r.status===200){console.log('WARM '+host+' '+r.status);warmed=true;break;}lastError='status '+r.status;}catch(e){lastError=e.message||String(e);}await Bun.sleep(1000);}assert(warmed,'Warm-up failed for '+host+(lastError?': '+lastError:''));}}
   // Every publisher-approved member journal must warm and pass a complete
   // local article census before any public acceptance can be claimed.
   const memberCanaries=selectMemberJournalCanaries(candidate.EnvironmentVariables.BONFYRE_CMS_DB);
   const memberLocal=[];
   for(const memberCanary of memberCanaries){
    const port=candidate.ProgramArguments[candidate.ProgramArguments.indexOf('--port')+1];
    let warmed=false;
    for(let attempt=0;attempt<8;attempt++){
     try{const response=await fetch('http://127.0.0.1:'+port+'/blog/',{headers:{Host:memberCanary.host,Accept:'text/html'},signal:AbortSignal.timeout(90000)});await response.arrayBuffer();if(response.status===200){warmed=true;break;}}catch{}
     await Bun.sleep(1000);
    }
    assert(warmed,'The member publication did not warm on '+memberCanary.host);
    const verified=await verifyMemberJournalReadback(memberCanary,(base,route,host)=>request('http://127.0.0.1:'+port,route,host));
    memberLocal.push(verified);
    console.log('LOCAL_MEMBER_JOURNAL_OK '+memberCanary.host+' '+verified.routes_verified+'/'+memberCanary.total);
   }
  console.log('LOCAL_READBACK_BEGIN '+dir);let local,error;for(let i=0;i<8;i++){try{local=await smoke(dir);break;}catch(e){error=e;console.log('LOCAL_READBACK_RETRY '+(i+1)+' '+e.message);await Bun.sleep(500);}}assert(local,error?.message||'Local readback unavailable.');console.log('LOCAL_READBACK_OK '+local.length);
  console.log('RENDERED_FRONTS_BEGIN');const front=await frontIntegrity({chrome,port:candidate.ProgramArguments[candidate.ProgramArguments.indexOf('--port')+1]}),broken=front.filter(f=>f.hard);assert(!broken.length,'Rendered fronts failed: '+broken.slice(0,6).map(f=>f.host+' '+f.width+' '+f.kind+' '+f.detail).join('; '));
  console.log('RENDERED_FRONTS_OK '+front.length+' notes');console.log('PUBLIC_READBACK_BEGIN');
   const publicRoutes=await smoke(dir,{publicRead:true});
   const memberPublic=[];
   for(const memberCanary of memberCanaries){
    const verified=await verifyMemberJournalReadback(memberCanary,(base,route,host)=>retryReadbackTransport(request,base,route,host,{attempts:3,delayMs:250}),{publicRead:true});
    memberPublic.push(verified);
    console.log('PUBLIC_MEMBER_JOURNAL_OK '+verified.host+' '+verified.routes_verified+'/'+memberCanary.total);
   }
   inspectRelease(dir);assert.equal(hash(plist),candidateHash,'The active service configuration changed during readback.');
   const memberJournals=memberCanaries.length?{local:memberLocal,public:memberPublic}:null;
   save(join(dir,'promoted.mpk'),{activated:new Date().toISOString(),stageHash:hash(join(dir,'stage.mpk')),local,publicRoutes,front,memberJournals,
    memberJournal:memberCanaries.length===1?{local:memberLocal[0],public:memberPublic[0]}:null,
    rollback:join(dir,'previous-service.plist'),databaseWrites:false});
  console.log('LIVE '+dir+'\n'+publicRoutes.map(r=>r.host+r.route+' '+r.status).join('\n'));
 }catch(error){
  rmSync(temporary,{force:true});if(stopped){
   try{assert.equal(hash(join(dir,'previous-service.plist')),stage.previousHash);assert([stage.previousHash,candidateHash].includes(hash(plist)),'A different service configuration now owns this path.');}
   catch{throw Error('Activation failed and the service or rollback configuration changed concurrently. No newer configuration was overwritten; inspect the retained release before recovery. Cause: '+error.message);}
   run(['/bin/launchctl','bootout',domain,plist],{allowed:[0,3,5,113]});copyFileSync(join(dir,'previous-service.plist'),temporary);renameSync(temporary,plist);run(['/bin/launchctl','bootstrap',domain,plist]);
  }
  throw Error('Activation failed; '+(stopped?'previous service configuration restored without restoring databases':'current service configuration retained')+': '+error.message);
 }
}
function status(){
 const conf=parsePlist(plist),env=conf.EnvironmentVariables||{},binary=executable(conf.ProgramArguments?.[0]);assert.equal(conf.Label,label);
 const result=Bun.spawnSync(['/bin/launchctl','print',domain+'/'+label],{stdout:'pipe',stderr:'pipe',timeout:10000});
 const text=result.stdout.toString(),state=text.match(/^\s*state = (.+)$/m)?.[1]||'unavailable',pid=text.match(/^\s*pid = (\d+)$/m)?.[1]||'unavailable';
 const probe=path=>{try{return Boolean(path&&lstatSync(realpathSync(path)).isFile());}catch{return false;}};
 const owner=env.BONFYRE_PROGRAM_OWNER||(env.BONFYRE_WORKSPACE_CLIENT?join(env.BONFYRE_WORKSPACE_CLIENT,'../../scripts/program-workspace-owner.mjs'):null);
 console.log('SERVICE '+label+'\nSTATE '+state+'\nPID '+pid+'\nBINARY '+(binary||'unavailable')+'\nBINARY_SHA256 '+(binary?hash(binary):'unavailable')+'\nPROGRAM_OWNER '+(probe(owner)?realpathSync(owner):'unavailable')+'\nBUN '+(executable(env.BONFYRE_BUN_BINARY)||'unavailable')+'\nCUE '+(findCue(env.PATH||'/usr/bin:/bin:/usr/sbin:/sbin')||'unavailable')+'\nCMS_DATABASE '+(probe(env.BONFYRE_CMS_DB)?'configured':'unavailable')+'\nPROGRAM_ASSETS '+(programAssets.every(name=>probe(env.BONFYRE_WORKSPACE_CLIENT&&join(env.BONFYRE_WORKSPACE_CLIENT,name)))?'present':'unavailable'));
}
if(import.meta.main){const [operation,path,arg]=process.argv.slice(2);if(operation==='stage')await stage();else if(operation==='rollout'){const dir=await stage();await verify(dir);await activate(dir);}else if(operation==='status')status();else{const dir=selectedDirectory(path);if(operation==='verify')await verify(dir);else if(operation==='verify-frozen')await verifyFrozen(dir);else if(operation==='verify-group'){const index=Number(arg);assert(Number.isInteger(index),'Acceptance group index is required.');await verify(dir,index);}else if(operation==='activate')await activate(dir);else if(operation==='readback')console.log(await smoke(dir,{publicRead:true,stage:inspectRelease(dir,{requireCurrentInventory:false})}));else throw Error('Use status, stage, verify, verify-frozen, verify-group, activate, readback or rollout.');}}
