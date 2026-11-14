// Read-only, checksum-verified progress for the frozen public release.
import assert from 'node:assert/strict';
import {readFileSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {decode} from '../../integrations/browser/workspace-msgpack.mjs';
import {acceptanceGroups} from './public_surface_rollout.mjs';
const hash=file=>createHash('sha256').update(readFileSync(file)).digest('hex');
export function publicAcceptanceReceipts(directory){
 const dir=resolve(directory),stagePath=join(dir,'stage.mpk'),groups=acceptanceGroups();
 const stage=decode(readFileSync(stagePath)),stageHash=hash(stagePath);
 assert.equal(stage.kind,'public-surface-release-v2');
 assert.equal(stage.directory,dir,'Inspect the exact staged directory.');
 const records=groups.map((group,index)=>{
  const receiptPath=join(dir,'acceptance-'+index+'.mpk');
  const logPath=join(dir,'acceptance-'+index+'.log');
  if(!existsSync(receiptPath))return {index,state:existsSync(logPath)?'unfinished':'pending',tests:group.length};
  try{
   const receipt=decode(readFileSync(receiptPath));
   const valid=existsSync(logPath)&&receipt.stageHash===stageHash&&
    receipt.logSha256===hash(logPath)&&JSON.stringify(receipt.group)===JSON.stringify(group);
   return {index,state:valid?'verified':'stale_or_corrupt',tests:group.length};
  }catch{return {index,state:'stale_or_corrupt',tests:group.length};}
 });
 let accepted=false,promoted=false;
 try{
  const a=decode(readFileSync(join(dir,'accepted.mpk')));
  accepted=a.stageHash===stageHash&&Array.isArray(a.logs)&&a.logs.length===groups.length;
 }catch{}
 try{const p=decode(readFileSync(join(dir,'promoted.mpk')));promoted=p.stageHash===stageHash;}catch{}
 return {schema:'bonfyre.public-acceptance-progress.v1',candidate:dir.split('/').at(-1),
   groups:records,verified:records.filter(r=>r.state==='verified').length,total:records.length,
   accepted,promoted};
}
if(import.meta.main){
 assert(process.argv.length===3,'Give one exact staged public release path.');
 console.log(JSON.stringify(publicAcceptanceReceipts(process.argv[2])));
}
