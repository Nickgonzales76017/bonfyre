// Reusable loopback, MessagePack and real Chromium test infrastructure.
import {encode} from '../workspace-msgpack.mjs';
export const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
export const gate=()=>({entered:deferred(),release:deferred(),finished:deferred()});
export const packet=(value,status=200)=>new Response(encode(value),{status,headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
export const loopbackServer=options=>Bun.serve({hostname:'127.0.0.1',port:0,...options});
export async function launchFixtureChromium({module='@playwright/test',noSandbox=false}={}){
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||module);
 return chromium.launch({headless:true,...noSandbox?{args:['--no-sandbox']}:{},...process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{}});
}
