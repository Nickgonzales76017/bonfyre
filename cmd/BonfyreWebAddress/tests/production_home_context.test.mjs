import {test,expect,beforeAll,afterAll} from 'bun:test';
import {copyFileSync,writeFileSync,rmSync,mkdirSync,mkdtempSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {fixture,root} from './auth_continuation_fixture.mjs';
import {publicationPayload,publicationRoute} from '../../BonfyreSurfaceScene/runtime/authored-publication.mjs';
// Actual native anonymous pages and CMS publication records, transported to a
// disposable browser at four fixture hosts. No customer session or external read.
let f,browser;const ns='home-context-fixture',records=[],errors=[];
const proof=mkdtempSync(join(tmpdir(),'bonfyre-home-context-'));
const design={schema:'bonfyre.public-production-design.v1',title:'An authored source investigation',description:'Inspect this source and continue the reviewed finding into product work.',layout:'columns',appearance:'paper',panels:[{kind:'research',label:'Read the source',role:'source'},{kind:'product',label:'Review the opportunity',role:'output'}],connections:[{from:0,to:1,operation:'research-opportunity'}]};
function command(op,value){const packet=join(f.home,'home-page.packet');try{if(op==='page-write')writeFileSync(packet,value,{mode:0o600});return JSON.parse(f.run(f.cms,['entry',op,op==='page-write'?'@'+packet:value,'--ns',ns,'--db',f.db,'--schemas',join(f.home,'schemas')]));}finally{if(op==='page-write')rmSync(packet,{force:true});}}
beforeAll(async()=>{
 f=await fixture();copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(f.home,'schemas/site_fragment.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 for(let i=0;i<4;i++){const route=await publicationRoute('fixture:home-production-'+i);records.push(route);command('page-write',JSON.stringify(publicationPayload({...design,title:design.title+' '+i},route,command('page-read',route),true)));}
 const pp=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await pp.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
},30000);
afterAll(async()=>{await browser?.close();await f?.close();writeFileSync(join(proof,'scope.tsv'),'scope\tactual isolated CMS/native pages, transparent fixture host transport\nweb_sha256\t'+(f?.fingerprint||'')+'\n');console.log('HOME_CONTEXT_PROOF '+proof);});
async function open(host,width=1440,mode={}){
 const context=await browser.createBrowserContext(),page=await context.newPage(),requests=[];page.setDefaultTimeout(12000);await page.setViewport({width,height:950});
 page.on('pageerror',e=>errors.push(e.message));await page.setRequestInterception(true);
 page.on('request',async request=>{try{
  const url=new URL(request.url());if(url.protocol==='data:')return request.continue();if(url.hostname!==host)return request.abort();
  if(request.method()!=='GET')throw Error('Unexpected mutation in public page test.');requests.push(url.pathname);
  if(url.pathname==='/_surface/productions'&&mode.fail)return request.respond({status:503,contentType:'text/plain',body:'Explicit read failure'});
  const response=await fetch(f.base+url.pathname+url.search,{headers:{Host:host,Accept:request.headers().accept||'*/*'},redirect:'manual',signal:AbortSignal.timeout(10000)}),body=Buffer.from(await response.arrayBuffer()),headers={};
  for(const[k,v]of response.headers)if(!['connection','content-length','transfer-encoding','content-encoding'].includes(k))headers[k]=v;
  await request.respond({status:response.status,headers,body});
 }catch(e){errors.push(e.message);if(!request.isInterceptResolutionHandled())await request.abort();}});
 await page.goto('https://'+host+'/',{waitUntil:'networkidle0'});return {page,requests,close:()=>context.close()};
}
for(const territory of ['work','life','live','shop'])test(territory+' front retains its content without loading the production catalogue',async()=>{
 for(const width of [1440,390]){const p=await open('aurekai.'+territory,width);try{
  expect(await p.page.$('main h1')).not.toBeNull();
  expect(await p.page.$('[data-home-showcase],[data-home-production-slot]')).toBeNull();
  expect(p.requests).not.toContain('/_surface/production-home.js');
  expect(p.requests).not.toContain('/_surface/productions');
  expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await p.close();}}
},45000);
test('published designs remain available and public reading creates no account',async()=>{
 const body=(await f.request('/productions')).body;expect(body.match(/<article[^>]*data-production-design/g).length).toBe(4);
 for(const route of records)expect((await f.request('/productions/'+ns+route)).status).toBe(200);
 expect(f.run(f.auth,['--db',f.authdb,'status'])).toContain('0 total');expect(errors).toEqual([]);
});
