import {decode} from './workspace-msgpack.mjs';
import {parseContinuationManifest} from './workspace-continuation.mjs';
const contextPattern=/^(?:self|(?:institution|household):[A-Za-z0-9][A-Za-z0-9._-]{0,95})$/;
const bindingPattern=/^ws-[a-f0-9]{40}$/;
const releasePattern=/^[a-f0-9]{64}$/;
export const productionPack='experience.connected-instruments.workspace';
export async function readWorkspacePacket(url,{request=globalThis.fetch.bind(globalThis),signal,timeoutMs=15000,limit=512*1024,type='application/msgpack'}={}){
  const deadline=signal?AbortSignal.any([signal,AbortSignal.timeout(timeoutMs)]):AbortSignal.timeout(timeoutMs),response=await request(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal:deadline,headers:{Accept:type}});
  if(!response.ok||deadline.aborted){try{await response.body?.cancel();}catch{}deadline.throwIfAborted();throw Object.assign(Error(response.status===403?'Your current account cannot access this destination.':'The destination owner could not complete this read.'),{status:response.status});}
  if(type==='text/plain'?!['text/plain','application/octet-stream'].includes(response.headers.get('content-type')?.split(';')[0]):response.headers.get('content-type')?.split(';')[0]!==type){try{await response.body?.cancel();}catch{}throw Error('The destination returned an unexpected response.');}
  const reader=response.body?.getReader();if(!reader)throw Error('The destination returned no body.');const parts=[];let length=0;
  try{for(;;){const part=await reader.read();deadline.throwIfAborted();if(part.done)break;length+=part.value.length;if(length>limit)throw Error('The destination response exceeds its bound.');parts.push(part.value);}}finally{try{await reader.cancel();}catch{}finally{reader.releaseLock();}}
  const data=new Uint8Array(length);let at=0;for(const part of parts){data.set(part,at);at+=part.length;}return type==='application/msgpack'?decode(data):type==='application/json'?JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(data)):new TextDecoder('utf-8',{fatal:true}).decode(data);
}
export async function readWorkspaceDestinations(options={}){
  const packet=await readWorkspacePacket('/apps/_destinations',options);
  if(!Array.isArray(packet)||packet.length!==2||packet[0]!==1||!Array.isArray(packet[1])||packet[1].length>201)throw Error('The destination list is incomplete.');
  const seen=new Set();return packet[1].map(row=>{
    if(!Array.isArray(row)||row.length!==3||!contextPattern.test(row[0])||seen.has(row[0])||typeof row[1]!=='string'||!row[1].trim()||row[1].length>319||typeof row[2]!=='boolean')throw Error('The destination list contains an invalid identity.');
    seen.add(row[0]);return {context:row[0],label:row[1],canWrite:row[2]};
  });
}
export async function readProductionBindings(context,options={}){
  if(!contextPattern.test(context))throw Error('Choose an admitted destination.');
  const packet=await readWorkspacePacket('/apps/_bindings?'+new URLSearchParams({context,pack:productionPack}),options);
  const expectedScope=context==='self'?'self':context.split(':')[0];
  if(!Array.isArray(packet)||packet.length!==4||packet[0]!==1||packet[2]!==expectedScope||!(context==='self'?/^user:[1-9][0-9]*$/.test(packet[1]):packet[1]===context)||!Array.isArray(packet[3])||packet[3].length>64)throw Error('The installed editor list belongs to a different destination.');
  const seen=new Set();return packet[3].map(row=>{
    if(!Array.isArray(row)||row.length!==6||!bindingPattern.test(row[0])||seen.has(row[0])||row[1]!==productionPack||typeof row[2]!=='string'||!/^([A-Za-z0-9_-]+\/)+$/.test(row[2])||!releasePattern.test(row[3])||typeof row[4]!=='string'||row[4].length>255||typeof row[5]!=='boolean')throw Error('The installed editor list is incomplete.');
    seen.add(row[0]);return {context,id:row[0],pack:row[1],entry:row[2],release:row[3],label:row[4],ready:row[5]};
  });
}
export async function verifyProductionDestination(target,options={}){
  if(!target||!bindingPattern.test(target.id)||!releasePattern.test(target.release)||!contextPattern.test(target.context))throw Error('Review a current receiving workspace.');
  const destinations=await readWorkspaceDestinations(options),destination=destinations.find(d=>d.context===target.context);
  if(!destination?.canWrite)throw Error('Current write access is required in the receiving destination.');
  const bindings=await readProductionBindings(target.context,options),binding=bindings.find(b=>b.id===target.id);
  if(!binding?.ready||binding.release!==target.release)throw Error('The receiving editor changed. Review its current configuration before preparing again.');
  const manifest=await readWorkspacePacket('/apps/'+binding.id+'/CONTINUATIONS.tsv',{...options,type:'text/plain',limit:32768});
  const view=parseContinuationManifest(manifest).find(v=>v.view==='production');
  if(!view||view.entry!=='instruments/production/'||view.prefix!=='draft:instruments:composition:'||view.query!=='record=$key')throw Error('Enable the current Production Studio in this destination before preparing its work.');
  return {...binding,entry:view.entry,destinationLabel:destination.label};
}
