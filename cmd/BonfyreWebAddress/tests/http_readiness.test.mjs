import {test,expect} from 'bun:test';
import {connect} from 'node:net';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_WEB_ADDRESS_TEST_BINARY||join(root,'cmd/BonfyreWebAddress/bonfyre-web-address');
async function fixture(fn){
 const home=mkdtempSync(join(tmpdir(),'http-ready-')),db=join(home,'matrix.db'),sockets=[];
 const reservation=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});const port=reservation.port;await reservation.stop(true);
 const env={...process.env,BONFYRE_CMS_DB:join(home,'absent.db')};
 const boot=Bun.spawnSync([binary,'bootstrap-owned','--db',db],{env});expect(boot.exitCode).toBe(0);
 const server=Bun.spawn([binary,'serve','--db',db,'--port',String(port),'--site-root',join(root,'site/matrix')],{env,stdout:'ignore',stderr:'ignore'});
 const base=`http://127.0.0.1:${port}`;
 const socket=()=>new Promise((resolve,reject)=>{const s=connect({host:'127.0.0.1',port});sockets.push(s);s.on('error',reject);s.once('connect',()=>resolve(s));});
 const reply=(s,timeout=2500)=>new Promise((resolve,reject)=>{let data='';s.setTimeout(timeout,()=>reject(Error('socket response timed out')));s.on('data',b=>data+=b);s.once('end',()=>resolve(data));s.once('error',reject);});
 const health=async()=>{const start=performance.now();const r=await fetch(base+'/healthz',{signal:AbortSignal.timeout(1500)});expect(r.status).toBe(200);expect((await r.json()).verdict).toBe('healthy');return performance.now()-start;};
 try{
  for(let i=0;i<60;i++){try{const r=await fetch(base+'/healthz',{signal:AbortSignal.timeout(300)});await r.text();if(r.ok)break;}catch{}await Bun.sleep(50);}
  await fn({socket,reply,health,base});
 }finally{for(const s of sockets)s.destroy();server.kill();await server.exited;rmSync(home,{recursive:true,force:true});}
}

test('idle browser connections do not occupy the only request worker',()=>fixture(async({socket,reply,health})=>{
 const idle=await socket();await Bun.sleep(40);
 expect(await health()).toBeLessThan(1500);
 const response=reply(idle);idle.write('GET /healthz HTTP/1.1\r\nHost: aurekai.work\r\n\r\n');
 expect(await response).toContain('"verdict":"healthy"');
}));

test('split headers and an unfinished body wait without blocking another request',()=>fixture(async({socket,reply,health})=>{
 const partial=await socket(),response=reply(partial);let earlyBytes=0;partial.on('data',b=>earlyBytes+=b.length);
 partial.write('GET /healthz HTTP/1.1\r\nHo');await Bun.sleep(30);expect(await health()).toBeLessThan(1500);expect(earlyBytes).toBe(0);
 partial.write('st: aurekai.work\r\nContent-Length: 4\r\n\r\nab');await Bun.sleep(30);expect(await health()).toBeLessThan(1500);expect(earlyBytes).toBe(0);
 partial.write('cd');expect(await response).toContain('"verdict":"healthy"');
}));

test('malformed framing is refused before a handler and later requests still work',()=>fixture(async({socket,reply,health})=>{
 for(const [headers,code] of [['Content-Length: 999999999\r\n',413],['Content-Length: 1\r\nContent-Length: 1\r\n',400],['Transfer-Encoding: chunked\r\n',400],['Content-Length: -1\r\n',400]]){
  const s=await socket(),response=reply(s);s.write('GET /healthz HTTP/1.1\r\nHost: aurekai.work\r\n'+headers+'\r\n');
  expect(await response).toStartWith(`HTTP/1.1 ${code}`);await health();
 }
}));


test('a burst of preconnects remains bounded and leaves room for a complete request',()=>fixture(async({socket,health})=>{
 let refused=0;
 for(let i=0;i<80;i++){const s=await socket();s.on('data',b=>{if(b.toString().startsWith('HTTP/1.1 503'))refused++;});s.resume();}
 expect(await health()).toBeLessThan(1500);expect(refused).toBeGreaterThanOrEqual(16);
}));

test('body text cannot supply a missing Host header',()=>fixture(async({socket,reply})=>{
 const s=await socket(),response=reply(s),body='Host: aurekai.work\r\nAccept: text/html\r\n';
 s.write(`GET /_surface/intake?url=https%3A%2F%2Fexample.test%2F HTTP/1.1\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
 expect(await response).toStartWith('HTTP/1.1 404');
}));


test('an unused preconnection expires while the listener stays available',()=>fixture(async({socket,reply,health})=>{
 const idle=await socket(),expired=reply(idle,12000);
 await health();
 expect(await expired).toStartWith('HTTP/1.1 408');
 await health();
}),15000);


test('headers that exhaust the request bound are refused without waiting for another byte',()=>fixture(async({socket,reply,health})=>{
 const s=await socket(),response=reply(s);
 const prefix='GET /healthz HTTP/1.1\r\nHost: aurekai.work\r\nX-Padding: ';
 s.write(prefix+'x'.repeat(256*1024-1-Buffer.byteLength(prefix)));
 expect(await response).toStartWith('HTTP/1.1 413');await health();
}));
