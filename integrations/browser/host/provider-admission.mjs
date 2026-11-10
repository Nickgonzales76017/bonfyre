import {createHash} from 'node:crypto';
import {encode,decode} from '../workspace-msgpack.mjs';
import {PROVIDER_ADMISSION_BYTES,validateProviderAdmission} from '../provider-contract.mjs';
import {AccountConnections} from '../onboarding/account-connections.mjs';

// The registered extension obtains this packet in the pinned Aurekai document's
// isolated world. BrowserHost carries no cookie and creates no session or work.
export function admitProviderPacket(message,{now=Date.now()}={}){
 const bytes=message.packet;
 if(!Array.isArray(bytes)||bytes.length<1||bytes.length>PROVIDER_ADMISSION_BYTES||!bytes.every(n=>Number.isInteger(n)&&n>=0&&n<=255))throw Error('Use one bounded native provider packet.');
 const p=validateProviderAdmission(decode(Uint8Array.from(bytes)),{origin:message.origin,review:message.review,challenge:message.challenge,now});
 const owner=createHash('sha256').update(encode([p.binding.subject,p.binding.scope])).digest('hex');
 if(owner!==p.work.scope)throw Error('The provider work belongs to another native destination.');
 if(p.work.owner==='BonfyreEnrollment'){
  const current=new AccountConnections(p.binding).inspect(p.work.identity,p.work.workId);
  if(current.review.jobId!==p.work.id||current.review.revision!==p.work.revision||!current.providerReady||current.entry.url!==p.provider.url||current.entry.name!==p.provider.name)
   throw Error('The assigned provider setup changed. No browser tab was admitted.');
 }
 return p;
}
