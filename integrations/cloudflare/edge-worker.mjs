// Cloudflare Cron -> Queue -> native WorkGraph intent transport.
// Deploy only after configuring EDGEPATH, source digest, Queue binding and origin auth.
import {boundedBody} from './bounded-body.mjs';
import {canonical,sign} from './workgraph-envelope.mjs';
const enc=new TextEncoder();
const sha=async bytes=>Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
const assertEnv=env=>{
 if(typeof env.EDGE_SHARED_SECRET!=='string'||env.EDGE_SHARED_SECRET.length<32)throw Error('missing signing secret');
 const u=new URL(env.NATIVE_INGRESS_URL);
 if(u.protocol!=='https:'||u.username||u.password||u.pathname!=='/workgraph/intake'||u.search||u.hash)throw Error('invalid ingress destination');
 if(!/^[-a-z0-9.:]{3,120}$/.test(env.SOURCE_MISSION??'')||!(/^[a-f0-9]{64}$/.test(env.SOURCE_DIGEST??'')))throw Error('source snapshot must be pinned');
 return u;
};
export default {
 async scheduled(controller,env,ctx){
  assertEnv(env);
  if(!env.WORKGRAPH_QUEUE?.send)throw Error('Cloudflare Queue binding missing');
  const id=controller.scheduledTime;
  if(!Number.isSafeInteger(id)||id<0)throw Error('invalid scheduled time');
  // Scheduled time defines the idempotent identity for a single clock occurrence.
  ctx.waitUntil(env.WORKGRAPH_QUEUE.send({version:1,mission:env.SOURCE_MISSION,family:'source-sync',digest:env.SOURCE_DIGEST,issued:id,nonce:'scheduled_'+String(id).padStart(13,'0')}));
 },
 async queue(batch,env){
  const endpoint=assertEnv(env);
  for(const message of batch.messages){
   try {
    canonical(message.body);
    const x={...message.body,issued:Date.now()}; // refresh short-lived transport validity; keep stable occurrence nonce
    if(x?.version!==1||x?.family!=='source-sync'||x?.mission!==env.SOURCE_MISSION||x?.digest!==env.SOURCE_DIGEST||!Number.isSafeInteger(x?.issued)||!/^scheduled_[0-9]{13,}$/.test(x?.nonce??''))throw Error('invalid source occurrence');
    const signature=await sign(x,env.EDGE_SHARED_SECRET);
    const response=await fetch(endpoint,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({intent:x,signature}),signal:AbortSignal.timeout(10000),redirect:'error'});
    if(![200,202].includes(response.status)||response.headers.get('content-type')?.split(';')[0]!=='application/json')throw Error('native admission unavailable');
    const result=JSON.parse(await boundedBody(response,16384));
    const expected='edge-'+await sha(await crypto.subtle.digest('SHA-256',enc.encode([x.family,x.mission,x.digest].join('\n'))));
    if(result.status!==(response.status===202?'admitted':'duplicate')||result.mission!==x.mission||result.receipt?.mission!==expected||result.receipt?.node!=='job'||result.receipt?.sourceMission!==x.mission||result.receipt?.digest!==x.digest)throw Error('native admission receipt mismatch');
    message.ack();
   } catch(error){message.retry();}
  }
 }
};
