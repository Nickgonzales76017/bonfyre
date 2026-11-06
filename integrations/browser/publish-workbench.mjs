#!/usr/bin/env bun
// Publish the existing web projection through the existing ServiceSite account.
import {mkdtempSync,readFileSync,writeFileSync,mkdirSync,renameSync,rmSync} from 'node:fs';
import {join,resolve,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {createHash,randomUUID} from 'node:crypto';
import {buildWebWorkbench} from './web-package.mjs';
import {account,publish} from '../../components/BonfyreServiceSite/cloud.mjs';
export const PUBLIC_ORIGIN='https://browser-workbench.aurekai-work-samples.pages.dev';
const here=dirname(fileURLToPath(import.meta.url)),project='aurekai-work-samples',branch='browser-workbench';
const sha=b=>createHash('sha256').update(b).digest('hex');
export async function publishWorkbench({apply=false,cloud=account(),publisher=publish,request=fetch,receiptDirectory=join(here,'build')}={}){
 const temp=mkdtempSync(join(tmpdir(),'workbench-publish-'));
 try{
  const built=buildWebWorkbench(join(temp,'site'));
  const get=async path=>{const r=await request(PUBLIC_ORIGIN+'/'+path,{signal:AbortSignal.timeout(15000)});return {ok:r.ok,bytes:Buffer.from(await r.arrayBuffer())};};
  const prior=await get('release.tsv');
  const same=prior.ok&&sha(prior.bytes)===built.release;
  if(!same&&!apply)return {state:'READY_TO_PUBLISH',release:built.release,address:PUBLIC_ORIGIN+'/workbench.html',published:false};
  const before=(await cloud.api('GET','/pages/projects/'+project)).result.canonical_deployment?.id;
  if(!same)await publisher(built.directory,project,undefined,{previewBranch:branch});
  const current=await get('release.tsv');
  if(!current.ok||sha(current.bytes)!==built.release)throw Error('Published release manifest differs from the source projection.');
  for(const file of built.files){
   const remote=await get(file.name);
   if(!remote.ok||sha(remote.bytes)!==file.sha256)throw Error('Published workbench file differs: '+file.name);
   if(sha(readFileSync(join(here,'extension',file.name)))!==file.sha256)throw Error('Workbench source changed during publication.');
  }
  const after=(await cloud.api('GET','/pages/projects/'+project)).result.canonical_deployment?.id;
  if(before!==after)throw Error('Production changed during preview publication; reconcile before recording success.');
  const result={state:same?'ALREADY_PUBLISHED':'PUBLISHED',release:built.release,address:PUBLIC_ORIGIN+'/workbench.html',published:true,files:built.files.length,productionUnchanged:true};
  mkdirSync(receiptDirectory,{recursive:true});
  const path=join(receiptDirectory,'workbench-publication.yaff'),temporary=path+'.'+randomUUID();
  try{writeFileSync(temporary,'workbench-publication '+result.release+'\n  state '+result.state+'\n  address '+result.address+'\n  checked_at '+new Date().toISOString()+'\n  source_files '+result.files+'\n  production_unchanged true\n  native_effects false\n');renameSync(temporary,path);}finally{rmSync(temporary,{force:true});}
  return result;
 }finally{rmSync(temp,{recursive:true,force:true});}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{
  const args=process.argv.slice(2);if(args.length!==1||!['--check','--publish'].includes(args[0]))throw Error('usage: publish-workbench.mjs --check|--publish');
  const r=await publishWorkbench({apply:args[0]==='--publish'});console.log('state '+r.state+'\nrelease '+r.release+'\naddress '+r.address);
  if(!r.published)process.exitCode=3;
 }catch(error){console.error(error.message);process.exitCode=1;}
}

// No Queue, workflow schedule, browser host registration or merchant action is
// created by this publisher. Browser native operations remain extension-only.
