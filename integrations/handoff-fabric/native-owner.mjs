import {execFileSync} from 'node:child_process';
import {resolve} from 'node:path';

// Existing BonfyreQueue remains the only WorkGraph job and completion owner.
export function nativeHandoffQueue(database,{binary=resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue')}={}){
 function run(...args){
  try{return JSON.parse(execFileSync(binary,[...args,'--db',resolve(database)],{encoding:'utf8',timeout:10000,maxBuffer:128*1024,stdio:['ignore','pipe','pipe']}));}
  catch(error){let native;try{native=JSON.parse(String(error.stdout));}catch{}
   const status=['conflict','stale_claim','not_eligible'].includes(native?.result)?409:503;
   throw Object.assign(Error('Native WorkGraph refused the handoff.'),{status,code:native?.result});
  }
 }
 return {
  inspect:mission=>run('mission',mission),
  status:mission=>run('status',mission,'job'),
  enqueue:(mission,family,type,uri)=>run('enqueue',type,uri,'--mission',mission,'--family',family,'--if-absent','--retries','100'),
  claim:(mission,family,worker)=>run('claim',mission,'job',worker,'--family',family,'--lease-ms','30000'),
  ack:(mission,worker,token,output)=>run('ack',mission,'job',worker,token,'--output-uri',output),
  reap:mission=>run('reap-expired',mission)
 };
}
