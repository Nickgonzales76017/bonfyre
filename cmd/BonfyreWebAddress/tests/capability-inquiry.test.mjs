import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(import.meta.dir,'../../..');
const binary=join(root,'cmd/BonfyreWebAddress/bonfyre-web-address');
async function withEdge(inquiry,run){
  const dir=mkdtempSync(join(tmpdir(),'capability-inquiry-'));
  const env={...process.env,BONFYRE_CMS_DB:'',BONFYRE_FINANCE_DB:join(dir,'finance.db'),BONFYRE_CAPABILITY_INQUIRY_URL:inquiry};
  const db=join(dir,'matrix.db');
  const invoke=args=>{const r=Bun.spawnSync([binary,...args],{env,stdout:'pipe',stderr:'pipe'});expect(r.exitCode).toBe(0);};
  invoke(['bootstrap-owned','--db',db]);
  invoke(['domain','--db',db,'--host','aurekai.work','--territory','work','--scope','institution','--world','live','--repr','HTML','--evidence','receipt:inquiry:test']);
  const probe=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});
  const port=probe.port;probe.stop(true);
  const server=Bun.spawn([binary,'serve','--db',db,'--port',String(port),'--site-root',join(root,'site/matrix')],{env,stdout:'ignore',stderr:'pipe'});
  const request=(path,accept='application/json',extra={})=>fetch(`http://127.0.0.1:${port}${path}`,{headers:{host:'aurekai.work',accept,...extra},signal:AbortSignal.timeout(5000)});
  try{
    let ready=false;
    for(let i=0;i<60;i++){
      if(server.exitCode!==null)throw Error('edge exited: '+await new Response(server.stderr).text());
      try{if((await request('/healthz')).ok){ready=true;break;}}catch{}
      await Bun.sleep(50);
    }
    expect(ready).toBe(true);await run(request);
  }finally{server.kill();await server.exited;rmSync(dir,{recursive:true,force:true});}
}
test('configured inquiry is shared by human and machine discovery without enabling purchases',async()=>{
  const url='https://example.test/work?source=capability&scope=review#ask';
  await withEdge(url,async request=>{
    const catalogue=await (await request('/capability')).json();
    expect(catalogue.inquiry.url).toBe(url);expect(catalogue.inquiry.grants_execution).toBe(false);
    expect(catalogue.capabilities.every(x=>x.priced===false)).toBe(true);
    const html=await (await request('/capability','text/html')).text();
    expect(html).toContain('https://example.test/work?source=capability&amp;scope=review#ask');
    expect(html).toContain('Discuss your workflow');
    const refusal=await request('/capability/estate-coverage','application/json',{'X-PAYMENT':'unverified'});
    expect(refusal.status).toBe(503);const body=await refusal.json();
    expect(body.error).toBe('not_for_sale');expect(body.inquiry.url).toBe(url);expect(body.inquiry.grants_execution).toBe(false);
    expect((await request('/capability/unknown')).status).toBe(404);
  });
},15000);
test('missing or unsafe configuration publishes no inquiry destination',async()=>{
  for(const url of ['', 'javascript:alert(1)','https://user:secret@example.test/','https://example.test/\"bad','https://example.test/\nheader']){
    await withEdge(url,async request=>{
      expect((await (await request('/capability')).json()).inquiry).toBeUndefined();
      expect(await (await request('/capability','text/html')).text()).not.toContain('Discuss your workflow');
      const response=await request('/capability/estate-coverage');expect(response.status).toBe(503);
      expect((await response.json()).inquiry).toBeUndefined();
    });
  }
},30000);
