import {verify} from './workgraph-envelope.mjs';

// A duplicate is acknowledged only when durable state retains the native admission receipt.
export function admission({secret,replay,enqueue,clock=()=>Date.now(),maxAgeMs=300000,retentionMs=172800000}) {
  if(!replay?.begin||!replay?.accept||!replay?.release||typeof enqueue!=='function') throw Error('durable native adapters required');
  return async function receive(input) {
    if(typeof input!=='string'||new TextEncoder().encode(input).length>4096) throw Error('oversized intent');
    let packet;try {packet=JSON.parse(input);} catch {throw Error('invalid transport input');}
    if(!packet||Object.keys(packet).sort().join(',')!=='intent,signature') throw Error('unexpected transport fields');
    const now=clock(),intent=await verify(packet.intent,packet.signature,secret,now,maxAgeMs);
    if(intent.family!=='source-sync') throw Error('unapproved work family');
    const replayKey=intent.family+':'+intent.mission+':'+intent.nonce;
    const claim=await replay.begin(replayKey,intent.digest,now+retentionMs);
    if(claim.state==='accepted') return {status:'duplicate',mission:intent.mission,receipt:claim.receipt};
    if(claim.state==='busy') return {status:'pending',mission:intent.mission,retryAfterMs:claim.retryAfterMs};
    if(claim.state!=='claimed'||!claim.token) throw Error('invalid replay result');
    try {
      const result=await enqueue({mission:intent.mission,family:intent.family,digest:intent.digest,ifAbsent:true});
      if(result?.accepted!==true||!result.receipt) throw Error('native admission refused');
      await replay.accept(replayKey,claim.token,result.receipt);
      return {status:'admitted',mission:intent.mission,receipt:result.receipt};
    } catch(error) {
      await replay.release(replayKey,claim.token);
      throw error;
    }
  };
}
