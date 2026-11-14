#!/usr/bin/env bun
// Forward-only nightly release: full native build -> managed image -> public four-apex.
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync, renameSync, statSync, unlinkSync, rmdirSync, appendFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const home=homedir(),checkout=resolve(process.env.BONFYRE_NIGHTLY_CHECKOUT||fileURLToPath(new URL('../..',import.meta.url)));
const stateRoot=join(home,'.bonfyre/state'),logRoot=join(home,'.bonfyre/logs');
const stateFile=join(stateRoot,'nightly-checkout.json'),lockDir=join(stateRoot,'nightly-checkout.lock');
const bun=process.env.BONFYRE_NIGHTLY_BUN||join(home,'.bun/bin/bun');
const releaseScript='scripts/release/public_surface_rollout.mjs';
const now=()=>new Date().toISOString(),logFile=join(logRoot,'nightly-checkout-'+new Date().toISOString().slice(0,10)+'.log');
mkdirSync(stateRoot,{recursive:true,mode:0o700});mkdirSync(logRoot,{recursive:true,mode:0o700});
const line=message=>{const msg=now()+' '+message+'\n';process.stdout.write(msg);appendFileSync(logFile,msg,{mode:0o600});};
function lock(){
 try{mkdirSync(lockDir,{mode:0o700});}
 catch(error){
  if(error.code!=='EEXIST')throw error;
  const pid=Number((()=>{try{return readFileSync(join(lockDir,'pid'),'utf8')}catch{return '0'}})());
  if(pid>0){try{process.kill(pid,0);throw Error('Another nightly checkout release is active (pid '+pid+').');}
   catch(e){if(e.code!=='ESRCH')throw e;}}
  try{unlinkSync(join(lockDir,'pid'));rmdirSync(lockDir);mkdirSync(lockDir,{mode:0o700});}
  catch{throw Error('A nightly checkout lock still exists: '+lockDir);}
 }
 writeFileSync(join(lockDir,'pid'),String(process.pid),{mode:0o600});
}
function unlock(){try{unlinkSync(join(lockDir,'pid'));rmdirSync(lockDir);}catch{}}
function loadState(){try{return JSON.parse(readFileSync(stateFile,'utf8'))}catch{return {checkout,phase:'new'};}}
function saveState(next){
 const data={...next,checkout,updated_at:now()},file=stateFile+'.'+process.pid;
 writeFileSync(file,JSON.stringify(data,null,2)+'\n',{mode:0o600});renameSync(file,stateFile);return data;
}
function stableEnvironment(){
 const keep=new Set(['HOME','USER','LOGNAME','PATH','SHELL','TMPDIR','LANG','LC_ALL','TZ',
 'PLAYWRIGHT_BROWSERS_PATH','ZIG_GLOBAL_CACHE_DIR','DYLD_LIBRARY_PATH',
 'BONFYRE_CHROMIUM_BINARY','BONFYRE_EXTENSION_CHROMIUM_BINARY','BONFYRE_TEST_BROWSER',
 'BONFYRE_PLAYWRIGHT_MODULE','BONFYRE_PUBLIC_WORKERS']);
 const env={};
 for(const [key,value] of Object.entries(process.env))if(keep.has(key)||key.startsWith('BONFYRE_'))env[key]=value;
 delete env.BONFYRE_NIGHTLY_CHECKOUT;delete env.BONFYRE_NIGHTLY_BUN;
 env.HOME=home;env.PATH=process.env.PATH||'/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin';
 env.LANG=env.LANG||'en_US.UTF-8';env.TMPDIR='/private/tmp/';
 return env;
}
function newerPromotedRelease(candidateDir){
 const plist=join(home,'Library/LaunchAgents/com.bonfyre.public-web-address.plist');
 if(!existsSync(plist))throw Error('Current public service plist unavailable; refusing unverified candidate resume.');
 const result=spawnSync('/usr/bin/plutil',['-convert','json','-o','-',plist],{encoding:'utf8',timeout:10000});
 if(result.status!==0)throw Error('Cannot read installed public service configuration.');
 let installed;
 try{installed=JSON.parse(result.stdout);}
 catch{throw Error('Installed service configuration is not parseable.');}
 const binary=installed.ProgramArguments?.[0]||'';
 const prefix=join(home,'.bonfyre/releases')+'/';
 const suffix='/bin/bonfyre-web-address';
 if(!binary.startsWith(prefix)||!binary.endsWith(suffix))
  throw Error('Installed service is not an identified managed WebAddress release.');
 const active=binary.slice(0,-suffix.length);
 if(active===candidateDir)return null;
 const promoted=join(active,'promoted.mpk'),staged=join(candidateDir,'stage.mpk');
 if(!existsSync(promoted)||!existsSync(staged))return null;
 return statSync(promoted).mtimeMs>statSync(staged).mtimeMs ? active : null;
}
function checkedRepo(){
 if(!existsSync(join(checkout,releaseScript)))throw Error('Integration checkout has no native rollout owner: '+checkout);
 const git=spawnSync('git',['rev-parse','--show-toplevel'],{cwd:checkout,encoding:'utf8'});
 if(git.status!==0||resolve(git.stdout.trim())!==checkout)throw Error('Refusing a non-checkout release root: '+checkout);
 const head=spawnSync('git',['rev-parse','HEAD'],{cwd:checkout,encoding:'utf8'});if(head.status!==0)throw Error('Cannot identify integration source revision.');
 return head.stdout.trim();
}
const env=stableEnvironment();
if(process.argv[2]==='--failures'){
 const body=readFileSync(process.argv[3]||logFile,'utf8'),a=body.lastIndexOf('BEGIN full-native-build'),b=body.indexOf('BLOCKED full-native-build',a);
 if(a<0)throw Error('No native build log found');const s=body.slice(a,b<0?undefined:b).replace(/\u001b\[[0-9;]*[A-Za-z]/g,'');
 const rows=[...s.matchAll(/^[ \t]*\[\s*(\d+)\]\s+(.+?)(✓|✗)[ \t\r]*$/gm)],failed=rows.filter(x=>x[3]==='✗'),expected=Number([...s.matchAll(/===\s*\d+\/\d+\s+built\s*\((\d+)\s+failed\)/g)].at(-1)?.[1]??-1);
 console.log('NATIVE_FAILED_OWNERS '+failed.length+' EXPECTED '+expected);
 for(let i=0;i<rows.length;i++){const m=rows[i];if(m[3]!=='✗')continue;const chunk=s.slice(m.index+m[0].length,rows[i+1]?.index??s.length),diagnosis=/native ratchet:/i.test(chunk)?'RATCHET':/error:|undefined reference|ld: |clang:/i.test(chunk)?'COMPILER':/No such file|not found|permission denied/i.test(chunk)?'DEPENDENCY':/FAIL|not ok|AssertionError|Error [0-9]/i.test(chunk)?'TEST':'UNKNOWN';console.log(m[1]+' '+m[2].trim()+' ['+diagnosis+']');for(const line of chunk.split('\n').filter(l=>/native ratchet:|error:|FAIL|AssertionError|No such file|Error [0-9]/i.test(l)).slice(0,2))console.log('    '+line.trim().slice(0,260));}
 if(expected!==failed.length)console.log('COUNT_MISMATCH '+expected+' vs '+failed.length+'; UNPARSED_ROWS\n'+s.split('\n').filter(l=>/^\s*\[\s*\d+\]\s+/.test(l)&&!/[✓✗]\s*$/.test(l)).slice(0,20).join('\n'));
 console.log('RATCHET_MEASURED '+([...s.matchAll(/measured: (\{[^\n]+\})/g)].at(-1)?.[1]||'not in log'));process.exit(expected!==failed.length?2:0);
}
async function run(label,argv,{timeoutMs=75*60*1000}={}){
 line('BEGIN '+label+' '+argv.join(' '));
 let tail='',partial='',lastStage='';
 const proc=spawn(argv[0],argv.slice(1),{cwd:checkout,env,stdio:['ignore','pipe','pipe']});
 let expired=false;
 const clock=timeoutMs>0?setTimeout(()=>{expired=true;proc.kill('SIGTERM');},timeoutMs):null;
 const feed=data=>{
  const chunk=data.toString();appendFileSync(logFile,chunk);
  tail=(tail+chunk).slice(-18000);
  partial+=chunk;
  const segments=partial.split(/\r?\n/);partial=segments.pop()||'';
  for(const part of segments){
   const match=part.match(/^STAGED (\/\S+)/);if(match)lastStage=match[1];
   if(/^(STAGED |ACCEPTED |WARM |LOCAL_|PUBLIC_|LIVE |FROZEN_VERIFIED |RENDERED_FRONTS_|ACCEPTANCE_GROUP_|ACCEPTANCE_GROUP_CARRIED |\s*=== Native build)/.test(part))process.stdout.write(part+'\n');
  }
 };
 proc.stdout.on('data',feed);proc.stderr.on('data',feed);
 let code;
 try{code=await new Promise((done,reject)=>{proc.on('error',reject);proc.on('close',done);});}
 finally{if(clock)clearTimeout(clock);}
 line((code===0&&!expired?'PASS ':'BLOCKED ')+label+' exit='+code+(expired?' timeout':''));
 if(code!==0||expired){
  const detail=tail.split('\n').filter(Boolean).slice(-32).join('\n').slice(-7000);
  if(detail)line('FAILURE_DETAIL '+label+'\n'+detail);
 }
 return {ok:code===0&&!expired,code,tail,stage:lastStage,expired};
}
const reason=x=>x.tail.split('\n').filter(Boolean).slice(-18).join('\n').slice(-3500);
function receipt(root,phase,extra={}){return saveState({checkout,stage_dir:root,phase,...extra});}
async function publicReadback(dir){
 const readback=await run('public-readback',[bun,releaseScript,'readback',dir],{timeoutMs:25*60*1000});
 if(!readback.ok){receipt(dir,'public_readback_blocked',{error:reason(readback)});return false;}
 receipt(dir,'live');line('LIVE_VERIFIED '+dir);return true;
}
async function candidate(dir){
 if(!dir||!existsSync(join(dir,'stage.mpk')))throw Error('Missing frozen release candidate: '+dir);
 if(existsSync(join(dir,'promoted.mpk')))return publicReadback(dir);
 const replaced=newerPromotedRelease(dir);
 if(replaced){line('SUPERSEDED '+dir+' by newer live '+replaced+'; activation refused');return false;}
 const verify=await run('public-verify',[bun,releaseScript,'verify',dir]);if(!verify.ok){receipt(dir,'verify_blocked',{error:reason(verify)});return false;}
 receipt(dir,'accepted');
 const replacedAfterVerify=newerPromotedRelease(dir);
 if(replacedAfterVerify){line('SUPERSEDED '+dir+' by newer live '+replacedAfterVerify+'; activation refused');return false;}
 let activate=await run('public-activate',[bun,releaseScript,'activate',dir],{timeoutMs:0});
 if(!activate.ok&&/ForeignTwin source release has zero or inconsistent source contracts/.test(activate.tail)&&
    /previous service configuration restored/.test(activate.tail)){
  line('COLD_SOURCE_RETRY retrying accepted candidate once after confirmed rollback');
  await new Promise(done=>setTimeout(done,3000));
  activate=await run('public-activate-retry',[bun,releaseScript,'activate',dir],{timeoutMs:0});
 }
 if(!activate.ok){receipt(dir,'activation_blocked',{error:reason(activate)});return false;}
 receipt(dir,'promoted');
 return publicReadback(dir);
}
async function fresh(head){
 receipt(null,'building',{head});
 const build=await run('full-native-build',['make','all'],{timeoutMs:3*60*60*1000});
 let nativeOk=false,nativeError='';
 if(build.ok){
  receipt(null,'native_built',{head});
  const promoted=await run('managed-native-image',['make','runtime-image-promote'],{timeoutMs:30*60*1000});
  nativeOk=promoted.ok;if(!nativeOk)nativeError=reason(promoted);
 }else nativeError=reason(build);
 if(!nativeOk){
  line('NATIVE_BLOCKED independent public promotion will use fresh core builds and its own acceptance.');
  for(const [owner,target] of [
   ['BonfyreCMS','bonfyre-cms'],['BonfyreQueue','bonfyre-queue'],
   ['BonfyreWebAddress','bonfyre-web-address']]){
   const core=await run('public-core-'+owner,['make','-C','cmd/'+owner,target],{timeoutMs:40*60*1000});
   if(!core.ok){receipt(null,'public_core_blocked',{head,native_error:nativeError,error:reason(core)});return false;}
  }
 }
 receipt(null,nativeOk?'native_promoted':'public_ready',{head,native_status:nativeOk?'promoted':'blocked',native_error:nativeError});
 const staged=await run('public-stage',[bun,releaseScript,'stage'],{timeoutMs:20*60*1000});
 if(!staged.ok||!staged.stage){receipt(null,'stage_blocked',{head,native_error:nativeError,error:reason(staged)});return false;}
 receipt(staged.stage,'staged',{head,native_status:nativeOk?'promoted':'blocked',native_error:nativeError});
 const publicOk=await candidate(staged.stage);
 if(publicOk&&!nativeOk){
  saveState({...loadState(),native_status:'blocked',native_error:nativeError});
  line('PUBLIC_LIVE_NATIVE_BLOCKED: keep current public release, repair native build independently.');
  return false;
 }
 return publicOk;
}
lock();
try{
 const head=checkedRepo(),state=loadState();
 line('CHECKOUT '+checkout+' HEAD '+head);
 if(state.phase==='activation_blocked'&&state.error)line('PREVIOUS_ACTIVATION_FAILURE\n'+String(state.error).slice(-6000));
 let success;
 if(state.checkout===checkout&&state.stage_dir&&state.phase!=='live'&&existsSync(join(state.stage_dir,'stage.mpk'))){
  const newer=newerPromotedRelease(state.stage_dir);
  if(newer){
   line('SUPERSEDED '+state.stage_dir+' by newer live '+newer+'; refusing stale activation');
   receipt(null,'superseded',{held_candidate:state.stage_dir,live_release:newer,head});
   success=await fresh(head);
  }else{
   line('RESUME '+state.stage_dir+' (never silently return to an older checkout)');
   success=await candidate(state.stage_dir);
   if(!success&&/Source changed before activation|Source changed|release input.*changed/i.test(loadState().error||'')){
    line('SOURCE_MOVED building latest checkout in the same nightly run');
    success=await fresh(head);
   }else if(!success)line('HOLD existing candidate; keep its acceptance and rollback state for correction.');
  }
 }else success=await fresh(head);
 process.exitCode=success?0:1;
 line('NIGHTLY_FINISHED '+(success?'OK':'BLOCKED')+' state='+loadState().phase+' log='+logFile);
}catch(error){line('REFUSED '+String(error?.stack||error));process.exitCode=1;}
finally{unlock();}
