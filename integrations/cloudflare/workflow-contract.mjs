import {canonical,sign,verify} from './workgraph-envelope.mjs';
const enc=new TextEncoder();
const hex=b=>Array.from(new Uint8Array(b),x=>x.toString(16).padStart(2,'0')).join('');
export const hash=async text=>hex(await crypto.subtle.digest('SHA-256',enc.encode(text)));
export const workflowId=async x=>'w-'+(await hash([x.family,x.mission,x.digest].join('\n'))).slice(0,48);
export const queueMission=async x=>'edge-'+await hash([x.family,x.mission,x.digest].join('\n'));
export async function packetFor({mission,digest},secret,now=Date.now()) {
  const base={version:1,mission,family:'source-sync',digest,issued:now};
  const id=await workflowId(base),intent={...base,nonce:'workflow_'+id};
  return {id,packet:{intent,signature:await sign(intent,secret)}};
}
export async function checkPacket(packet,secret,{now=Date.now(),maxAgeMs=86400000}={}) {
  if(!packet||Object.keys(packet).sort().join(',')!=='intent,signature')throw Error('invalid activation packet');
  const intent=await verify(packet.intent,packet.signature,secret,now,maxAgeMs);
  if(intent.family!=='source-sync'||intent.nonce!=='workflow_'+await workflowId(intent))throw Error('invalid activation identity');
  return intent;
}
function completionText(x) {
  const fields=['version','workflow','mission','digest','nativeMission','node','receipt','output','status','at'];
  if(!x||Object.keys(x).sort().join(',')!==[...fields].sort().join(','))throw Error('invalid completion fields');
  if(x.version!==1||x.node!=='job'||x.status!=='complete'||!Number.isSafeInteger(x.at)||!/^[a-f0-9]{64}$/.test(x.digest))throw Error('invalid completion');
  for(const name of fields.filter(k=>!['version','at'].includes(k)))if(typeof x[name]!=='string'||!x[name]||x[name].length>1024||/[\r\n\0]/.test(x[name]))throw Error('invalid completion coordinate');
  return fields.map(k=>x[k]).join('\n');
}
async function completionKey(secret) {
  if(typeof secret!=='string'||secret.length<32)throw Error('missing transport secret');
  return crypto.subtle.importKey('raw',enc.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);
}
export async function signCompletion(result,secret) {return {result,signature:hex(await crypto.subtle.sign('HMAC',await completionKey(secret),enc.encode('completion/v1\n'+completionText(result))))};}
export async function checkCompletion(packet,intent,secret) {
  if(!packet||Object.keys(packet).sort().join(',')!=='result,signature'||!/^[a-f0-9]{64}$/.test(packet.signature??''))throw Error('invalid completion envelope');
  const r=packet.result,text=completionText(r),bytes=Uint8Array.from(packet.signature.match(/../g),x=>parseInt(x,16));
  if(!await crypto.subtle.verify('HMAC',await completionKey(secret),bytes,enc.encode('completion/v1\n'+text)))throw Error('invalid completion signature');
  if(r.workflow!==await workflowId(intent)||r.mission!==intent.mission||r.digest!==intent.digest||r.nativeMission!==await queueMission(intent))throw Error('completion does not match activation');
  return r;
}
