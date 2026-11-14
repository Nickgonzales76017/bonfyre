// Read-only release inventory. A local accepted package is not a live release.
import {readdirSync,lstatSync,statSync,existsSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {publicAcceptanceReceipts} from './public_acceptance_receipts.mjs';
import {homedir} from 'node:os';
import {inspectRelease,verifyInputs} from './public_surface_rollout.mjs';
import {createHash} from 'node:crypto';
import {decode} from '../../integrations/browser/workspace-msgpack.mjs';
const home=homedir(),base=join(home,'.bonfyre');
const safe=name=>/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(name);
const entries=dir=>{try{return readdirSync(dir).filter(safe).sort();}catch{return [];}};
const isFile=path=>{try{const s=lstatSync(path);return s.isFile()&&!s.isSymbolicLink();}catch{return false;}};
const isDir=path=>{try{const s=lstatSync(path);return s.isDirectory()&&!s.isSymbolicLink();}catch{return false;}};
const reviewedTarget=path=>{
 if(!isFile(path))return {};
 try{if(lstatSync(path).size>1048576)return {};
  const v=JSON.parse(readFileSync(path,'utf8')).review||{},target=v.target||{},base=v.before?.binding?.release;
  return {pack:/^experience\.[a-z0-9.-]+\.workspace$/.test(target.pack||'')?target.pack:null,
   previousRelease:typeof base==='string'&&/^[a-f0-9]{64}$/.test(base)?base:null};
 }catch{return {};}
};
export function candidateInventory(){
 const releaseRoot=join(base,'releases'),approvalRoot=join(base,'state/approvals');
 const releases=entries(releaseRoot).filter(name=>isDir(join(releaseRoot,name))).map(name=>{
  const dir=join(releaseRoot,name);
  const accepted=isFile(join(dir,'accepted.mpk'));
  let seal={validAcceptance:false,promotedReceipt:false,verifiedGroups:0,totalGroups:0};
  try{const progress=publicAcceptanceReceipts(dir);seal={validAcceptance:progress.accepted,promotedReceipt:progress.promoted,verifiedGroups:progress.verified,totalGroups:progress.total};}catch{}
  let lineage={stageCreatedMs:null,webAddressSha256:null,inputFingerprint:null};
  try{const file=join(dir,'stage.mpk');if(isFile(file)){const stage=decode(readFileSync(file));const inputs=(stage.inputs||[]).map(v=>[v.path,v.sha256]).sort((a,b)=>a[0].localeCompare(b[0]));lineage={stageCreatedMs:Math.round(statSync(file).mtimeMs),webAddressSha256:stage.inputs?.find(v=>v.path==='cmd/BonfyreWebAddress/bonfyre-web-address')?.sha256||null,inputFingerprint:createHash('sha256').update(JSON.stringify(inputs)).digest('hex')};}}catch{}
  return {name,accepted,...seal,...lineage,hasManifest:isFile(join(dir,'manifest.mpk'))||
    isFile(join(dir,'MANIFEST.tsv')),files:entries(dir).length};
 });
 const approved=entries(approvalRoot).filter(name=>name.endsWith('.approved')&&isFile(join(approvalRoot,name)))
  .map(name=>{
   const text=readFileSync(join(approvalRoot,name),'utf8').slice(0,4096),release=/^release\t([A-Za-z0-9-]+)$/m.exec(text);
   return {receipt:name,release:release?.[1]||null,accepted:Boolean(releases.find(r=>r.name===release?.[1])?.accepted)};
  });
 const handoffRoot=join(base,'release-handoff');
 const handoffs=entries(handoffRoot).filter(name=>isDir(join(handoffRoot,name))).map(name=>{
  const dir=join(handoffRoot,name);
  const candidates=entries(dir).filter(hash=>/^[a-f0-9]{64}$/.test(hash)&&isDir(join(dir,hash))).map(hash=>{
   const p=join(dir,hash),stages=['review','plan','version-done','head-done','demand-done','install-done','activate-done','complete'];
   return {candidate:hash,stages:stages.filter(s=>isFile(join(p,s+'.json'))),...reviewedTarget(join(p,'review.json'))};
  });
  return {name,candidates};
 }).filter(r=>r.candidates.length);
 return {schema:'bonfyre.local-candidate-inventory.readonly.v1',
   public:{total:releases.length,accepted:releases.filter(r=>r.accepted).length,
    acceptedReleases:releases.filter(r=>r.accepted).slice(-80),
    awaitingAcceptance:releases.filter(r=>!r.accepted).slice(-40),
    approved,done:entries(join(approvalRoot,'done')).length,
    failed:entries(join(approvalRoot,'failed')).length},
   workspaces:handoffs};
}
function candidateCurrentState(name){
 const dir=join(base,'releases',name);
 try{const stage=inspectRelease(dir);verifyInputs(stage);return {state:'current_source_and_frozen_material',pendingGroups:publicAcceptanceReceipts(dir).groups.filter(g=>g.state!=='verified').map(g=>g.index)};}
 catch(error){return {state:'blocked',reason:String(error?.message||error).slice(0,220)};}
}
export function convergenceSummary(data,liveName){
 const rows=[...data.public.acceptedReleases,...data.public.awaitingAcceptance],live=rows.find(x=>x.name===liveName)||null;
 const older=x=>live?.stageCreatedMs!=null&&x.stageCreatedMs!=null&&x.stageCreatedMs<live.stageCreatedMs;
 const newer=x=>live?.stageCreatedMs!=null&&x.stageCreatedMs!=null&&x.stageCreatedMs>live.stageCreatedMs;
 const unpromoted=rows.filter(x=>x.validAcceptance&&!x.promotedReceipt),unaccepted=rows.filter(x=>!x.accepted);
 return {schema:'bonfyre.release-convergence.v1',live:live?{name:live.name,accepted:live.validAcceptance,promoted:live.promotedReceipt,groups:live.verifiedGroups,binary:live.webAddressSha256}:null,
 acceptedUnpromotedOlder:unpromoted.filter(older).map(x=>({name:x.name,binary:x.webAddressSha256,sameInputs:x.inputFingerprint===live?.inputFingerprint})),
 acceptedUnpromotedNewer:unpromoted.filter(newer).map(x=>({name:x.name,binary:x.webAddressSha256})),
 acceptedUnpromotedUnknown:unpromoted.filter(x=>!older(x)&&!newer(x)).map(x=>x.name),
 unacceptedNewer:unaccepted.filter(newer).map(x=>({name:x.name,groups:x.verifiedGroups,createdMs:x.stageCreatedMs,binary:x.webAddressSha256,inputFingerprint:x.inputFingerprint,...candidateCurrentState(x.name)})),
 unacceptedOlder:unaccepted.filter(older).length,unacceptedNoStage:unaccepted.filter(x=>x.stageCreatedMs==null).map(x=>({name:x.name,files:x.files})),
 explicitApprovals:data.public.approved.length,workspaceJournalComplete:data.workspaces.reduce((n,w)=>n+w.candidates.filter(c=>c.stages.includes('complete')).length,0),
 workspaceReviewedNotComplete:data.workspaces.flatMap(w=>w.candidates.filter(c=>c.stages.includes('review')&&!c.stages.includes('complete')).map(c=>({owner:w.name,candidate:c.candidate,pack:c.pack??null,previousRelease:c.previousRelease??null}))).slice(-40)};
}
function currentLiveRelease(){
 try{const path=join(home,'Library/LaunchAgents/com.bonfyre.public-web-address.plist');
  const p=Bun.spawnSync(['/usr/bin/plutil','-convert','json','-o','-',path],{stdout:'pipe',stderr:'pipe',timeout:6000});
  if(p.exitCode!==0)return null;
  const bin=JSON.parse(p.stdout.toString()).ProgramArguments?.[0]||'';
  return /\/releases\/([A-Za-z0-9-]+)\/bin\/bonfyre-web-address$/.exec(bin)?.[1]||null;
 }catch{return null;}
}
if(import.meta.main){const data=candidateInventory();console.log(JSON.stringify(process.argv.includes('--full')?data:convergenceSummary(data,currentLiveRelease())));}
