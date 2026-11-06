import {boundedBody} from './bounded-body.mjs';
import {admission} from './workgraph-admission.mjs';
import {nativeEnqueue} from './native-queue-adapter.mjs';
import {nonceStore} from './native-nonce-store.mjs';
// An origin listener, never a public unauthenticated command endpoint.
export function serveIngress({secret,queueBinary,queueDb,nonceDb,hostname='127.0.0.1',port=4387}){
 if(typeof secret!=='string'||secret.length<32)throw Error('transport secret not configured');
 if(!['127.0.0.1','::1','localhost'].includes(hostname))throw Error('public binding refused');
 const registry=nonceStore(nonceDb);
 const receive=admission({secret,replay:registry,enqueue:nativeEnqueue({binary:queueBinary,db:queueDb})});
 const server=Bun.serve({hostname,port,async fetch(request){
  const url=new URL(request.url);
  if(!['127.0.0.1','localhost','[::1]'].includes(url.hostname))return new Response('Unknown host',{status:421});
  if(url.pathname==='/health'&&request.method==='GET')return new Response('ready',{headers:{'cache-control':'no-store'}});
  if(url.pathname!=='/workgraph/intake'||request.method!=='POST')return new Response('Not found',{status:404});
  if(request.headers.get('content-type')?.split(';')[0]!=='application/json')return new Response('Unsupported media type',{status:415});
  const length=Number(request.headers.get('content-length')??0);if(length>4096)return new Response('Too large',{status:413});
  if(request.headers.get('content-encoding'))return new Response('Encoded bodies are refused',{status:415});
  try{
   const result=await receive(await boundedBody(request));
   return Response.json(result,{status:result.status==='pending'?503:result.status==='duplicate'?200:202,headers:{'cache-control':'no-store'}});
  }catch(error){
   const rejected=/invalid|expired|unexpected|oversized|stale|signature|secret/i.test(error.message);
   return Response.json({status:'refused'}, {status:rejected?403:503,headers:{'cache-control':'no-store'}});
  }
 }});
 return {server,close:()=>{server.stop(true);registry.close()}};
}
