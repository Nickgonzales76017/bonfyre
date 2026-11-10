// Public-only acceptance. No account creation, private session or user material.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {copyFileSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {fixture,root} from './auth_continuation_fixture.mjs';
import {publicationPayload,publicationRoute,designFromPacket} from '../../BonfyreSurfaceScene/runtime/authored-publication.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';
let f,route,path,browser;const namespace='public-design-fixture';
const design={schema:'bonfyre.public-production-design.v1',title:'An unfamiliar workshop investigation',description:'Read public material and prepare reviewed product opportunities. Supply your own inputs.',layout:'columns',appearance:'paper',panels:[{kind:'research',label:'Read source material',role:'source'},{kind:'product',label:'Evaluate opportunities',role:'output'}],connections:[{from:0,to:1,operation:'research-opportunity'}]};
const command=(op,value)=>{const packet=join(f.home,'reviewed-page.packet');try{if(op==='page-write')writeFileSync(packet,value,{mode:0o600});return JSON.parse(f.run(f.cms,['entry',op,op==='page-write'?'@'+packet:value,'--ns',namespace,'--db',f.db,'--schemas',join(f.home,'schemas')]));}finally{if(op==='page-write')rmSync(packet,{force:true});}};
const read=()=>command('page-read',route);
const save=(value=design,published=true)=>command('page-write',JSON.stringify(publicationPayload(value,route,read(),published)));
beforeAll(async()=>{f=await fixture();copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(f.home,'schemas/site_fragment.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','CANDIDATE','--repr','HTML','--evidence','fixture:public-production']);route=await publicationRoute('fixture:public-design');path='/productions/'+namespace+route;save();},30000);
afterAll(async()=>{await browser?.close();await f?.close();},30000);
test('all territories expose the exact published design and public MessagePack only',async()=>{
  for(const host of ['aurekai.work','aurekai.life','aurekai.live','aurekai.shop']){const r=await f.request(path,{host});expect(r.status).toBe(200);expect(r.body).toContain(design.title);expect(r.body).toContain('data-production-reuse');expect(r.headers.get('cache-control')).toBe('no-store');}
  for(const host of ['aurekai.work','aurekai.life','aurekai.live','aurekai.shop']){const r=await f.request('/',{host});expect(r.status).toBe(200);expect(r.body).not.toContain('/_surface/production-home.js');expect(r.body).not.toContain('data-home-showcase');}
  const script=await f.request('/_surface/production-home.js');expect(script.status).toBe(200);expect(script.headers.get('content-type')).toContain('application/javascript');expect(script.body).toContain("fetch('/_surface/productions'");expect(script.body).not.toContain('<script>');
  expect((await f.request('/_surface/production-home.js?actor=other')).status).toBe(400);
  const r=await fetch(f.base+path+'/design');expect(r.status).toBe(200);expect(r.headers.get('content-type')).toBe('application/msgpack');expect(designFromPacket(decode(new Uint8Array(await r.arrayBuffer())))).toEqual(design);
  expect((await f.request('/productions')).body).toContain(design.title);expect((await f.request('/_surface/productions')).body).toContain(design.title);expect((await f.request(path+'?actor=other')).status).toBe(400);expect((await f.request('/productions/root/design-'+'0'.repeat(64))).status).toBe(404);
  const next='/cms/productions?context=self&design='+namespace+route;const anonymous=await f.request(next);expect(anonymous.status).toBe(303);expect(decodeURIComponent(anonymous.headers.get('location'))).toContain(next);
});
test('unknown fields, extra fragments and out-of-range handoffs are withheld',async()=>{
  for(const change of [p=>{p.fragments[0].projection_contract.design.panels[0].recordKey='not-a-public-field';},p=>{p.fragments[0].projection_contract.design.connections[0].from=4294967296;},p=>{p.fragments.push({...p.fragments[0],coordinate:'duplicate-design'});}]){const p=publicationPayload(design,route,read());change(p);command('page-write',JSON.stringify(p));expect((await f.request(path)).status).toBe(404);expect((await f.request(path+'/design')).status).toBe(404);save();}
});
test('withdrawal and stale aggregate refusal preserve the exact native lifecycle',async()=>{
  const before=read();save({...design,title:'A later reviewed title'});expect(()=>command('page-write',JSON.stringify(publicationPayload(design,route,before)))).toThrow();expect(read().snapshot.page.title).toBe('A later reviewed title');
  save(design,false);expect((await f.request(path)).status).toBe(404);expect((await f.request(path+'/design')).status).toBe(404);expect((await f.request('/productions')).body).not.toContain(design.title);save();
});
test('public design cards remain readable and navigable without loading editors',async()=>{
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});const tab=await browser.newPage(),errors=[];tab.on('pageerror',e=>errors.push(e.message));
  try{for(const width of [1440,390]){await tab.setViewport({width,height:950});await tab.goto(f.base+'/productions',{waitUntil:'networkidle0'});await tab.waitForSelector('[data-production-design]');expect(await tab.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(await tab.$eval('.production-outline-preview',n=>n.children.length)).toBe(2);expect(await tab.$eval('[data-production-inspect]',n=>n.getAttribute('href'))).toBe(path);}
    await tab.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);expect(await tab.$eval('.production-edition',n=>getComputedStyle(n).animationName)).toBe('none');expect(await tab.evaluate(()=>performance.getEntriesByType('resource').every(r=>!/(sql-wasm|three\.module|production-client|research-view)/.test(r.name)))).toBe(true);
    await Promise.all([tab.waitForNavigation({waitUntil:'networkidle0'}),tab.click('[data-production-inspect]')]);expect(new URL(tab.url()).pathname).toBe(path);expect(await tab.$eval('[data-production-reuse]',n=>n.getAttribute('href'))).toBe('/cms/productions?context=self&design='+namespace+route);
    const proof='/private/tmp/bonfyre-public-production-proof';mkdirSync(proof,{recursive:true});await tab.screenshot({path:join(proof,'detail-mobile.png'),fullPage:true});await tab.setViewport({width:1440,height:950});await tab.goto(f.base+'/productions',{waitUntil:'networkidle0'});await tab.screenshot({path:join(proof,'gallery-desktop.png'),fullPage:true});expect(errors).toEqual([]);
  }finally{await tab.close();}
},30000);
test('public reads never create an authenticated account',()=>{
  const result=f.run(f.auth,['--db',f.authdb,'status']);expect(result).toContain('0 total');
});
// The separate authenticated publication-to-reuse browser journey is not covered
// by this public-only test. Existing native editor and authority suites remain.
