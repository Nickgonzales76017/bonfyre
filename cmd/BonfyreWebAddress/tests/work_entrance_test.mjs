import {test,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync,writeFileSync,mkdtempSync,mkdirSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {packSetupFixture,root} from './pack_setup_fixture.mjs';
let f,browser;
const out=process.env.BONFYRE_QA_OUTPUT||mkdtempSync(join(tmpdir(),'bonfyre-work-entrance-'));
const image='/apps/material/'+'a'.repeat(64)+'/assets/work.png';
const base={publisher:'fixture-publisher',layer:'official',kind:'application',purpose:'An editable source-backed test application.',governed_by:'fixture-owner',cannot_infer:'Test only; no deployment or user installation.',status:'published'};
const load=()=>f.request('/',{host:'aurekai.work'});
beforeAll(async()=>{
 f=await packSetupFixture();
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_door.json'),join(f.home,'schemas/site_door.json'));
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/work_example.json'),join(f.home,'schemas/work_example.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 f.add('pack_artifact',{...base,pack:'test-finance',canonical_name:'Finance & review'});
 f.add('pack_artifact',{...base,pack:'test-research',canonical_name:'Research studio'});
 f.add('pack_artifact',{...base,pack:'experience.connection-studio.workspace',canonical_name:'Connection Studio',kind:'base'});
 f.add('pack_artifact',{...base,pack:'test-draft',canonical_name:'Unpublished customer application',status:'draft'});
 f.add('pack_artifact',{...base,pack:'test-private',canonical_name:'Private client portfolio'},'qa-private');
 f.add('work_example',{example_id:'finance-finished',pack:'test-finance',territories:'work',position:1,kicker:'Finished work',headline:'Finance review finished',dek:'A first-party finished example.',made_with:'Fixture material',observed_at:'2026-10-24T12:00:00Z',picture:image,picture_alt:'Fixture preview',routes:'/examples/finance-finished',status:'published'});
 f.add('pack_demand',{pack:'test-finance',demand_kind:'preview-media',needs:image,fields:'image',preferred_sources:'Actual test preview identity',status:'published'});
 f.add('pack_demand',{pack:'test-finance',demand_kind:'public-preview',needs:'https://example.test/finance/',fields:'demo',status:'published'});
 f.add('pack_demand',{pack:'test-research',demand_kind:'public-preview',needs:'https://example.test/guide/',fields:'guide',status:'published'});
 f.add('pack_demand',{pack:'test-research',demand_kind:'public-preview',needs:'https://user:secret@example.test/unsafe',fields:'demo',status:'published'});
 f.add('site_door',{page:'front-work',position:1,eyebrow:'Existing entry',heading:'Open your native work',body:'A retained navigation contribution.',ways:[{label:'Installed tools',href:'https://aurekai.work/apps?context=self'}],status:'published'});
 f.sql.exec("DELETE FROM internet_source");
 mkdirSync(out,{recursive:true});
},30000);
afterAll(async()=>{await browser?.close();await f?.close();console.log('WORK_ENTRANCE_EVIDENCE '+out);});
test('published applications and navigation survive an empty news reservoir',async()=>{
 const page=await load();expect(page.status).toBe(200);
 expect(page.body).toContain('data-work-entrance');expect(page.body).toContain('Open your native work');
 expect(page.body).toContain('data-work-open href="/apps?context=self"');expect(page.body).toContain('data-work-setup href="/packs/setup"');
 expect(page.body).not.toContain('Finance &amp; review');expect(page.body).not.toContain('data-arrival-templates');expect(page.body).not.toContain('data-public-tool-open');expect(page.body).not.toContain('data-public-tool-demo');
 expect(page.body).not.toContain('user:secret');expect(page.body).not.toContain('Private client portfolio');expect(page.body).not.toContain('Unpublished customer application');
 writeFileSync(join(out,'work-empty-reservoir.html'),page.body);
});
test('all 21 canonical PackGraph identities have unique licensed semantic mappings',()=>{
 const mapping=readFileSync(join(root,'cmd/BonfyreWebAddress/src/pack_icon_map.h'),'utf8');
 const renderer=readFileSync(join(root,'cmd/BonfyreWebAddress/src/page_core_rendering.h'),'utf8');
 const rows=[...mapping.matchAll(/\{"(experience\.[^"]+)","([a-z-]+)"\}/g)];
 expect(rows.length).toBe(21);
 expect(new Set(rows.map(r=>r[1])).size).toBe(21);
 for(const [,pack,glyph] of rows){expect(renderer).toContain('{"'+glyph+'",');expect(pack.length).toBeLessThan(96);}
});
test('WORK-specific application entrance is not projected onto another territory or personal host',async()=>{
 for(const host of ['aurekai.life','aurekai.live','aurekai.shop']){
  const page=await f.request('/',{host});expect(page.status).toBe(200);expect(page.body).not.toContain('data-work-entrance');
 }
 f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','reader.aurekai.work','--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:work-entry']);
 const page=await f.request('/',{host:'reader.aurekai.work'});expect(page.body).not.toContain('data-work-entrance');expect(page.body).not.toContain('Open your native work');
});
test('new RSS material does not displace the application entrance or destroy its source identity',async()=>{
 f.add('internet_source',{family:'today',source:'work-news',label:'Work news fixture',says:'Explicit generated RSS test input',url:'https://newsdesk.test/work-rss',twin:'work-news-twin',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'post'});
 const path=join(f.home,'foreign/bodies/work-news.xml'),now=Date.now();
 const items=[['Earlier retained report',now-7200000],['Newest retained report',now-60000]].map(([title,at],i)=>`<item><title>${title}</title><link>https://newsdesk.test/work-${i}</link><guid>work-${i}</guid><pubDate>${new Date(at).toUTCString()}</pubDate><description>Original fixture body ${i}</description><media:thumbnail url="https://newsdesk.test/work-${i}.jpg"/></item>`).join('');
 writeFileSync(path,'<rss xmlns:media="http://search.yahoo.com/mrss/"><channel>'+items+'</channel></rss>');writeFileSync(join(f.home,'foreign/receipts/work-news-twin.json'),JSON.stringify({body_path:path}));
 const page=await load();expect(page.status).toBe(200);expect(page.body).toContain('data-work-entrance');expect(page.body).toContain('Newest retained report');
 expect(page.body).toMatch(/href="\/o\/work-news\/[a-f0-9]{24}"/);
 expect(page.body.indexOf('data-work-entrance')).toBeLessThan(page.body.indexOf('Newest retained report'));expect(page.body).toContain('class="wf-beat-photo"');
 writeFileSync(join(out,'work-with-rss.html'),page.body);
});
test('emitted entrance retains responsive geometry, focus motion and reduced motion',async()=>{
 f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:work-entry-browser']);
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 const page=await browser.newPage();page.setDefaultTimeout(12000);await page.setRequestInterception(true);page.on('request',r=>r.url().startsWith(f.base)||r.url().startsWith('data:')?r.continue():r.abort());
 await page.goto(f.base+'/healthz',{waitUntil:'domcontentloaded',timeout:12000});const response=await load();
 // The complete body is emitted by native CMS/WebAddress. This fixture checks layout, not public media delivery.
 for(const width of [1440,390]){
  console.log('WORK_LAYOUT',width,'mounting emitted body');
  await page.setViewport({width,height:1000});await page.setContent(response.body,{waitUntil:'domcontentloaded',timeout:12000});await page.waitForSelector('[data-work-entrance]');
  expect(await page.$('[data-work-open]')).not.toBeNull();expect(await page.$('[data-work-setup]')).not.toBeNull();
  expect(await page.$('.wf-tools .pack-tool')).toBeNull();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  await page.focus('[data-work-open]');expect(await page.$eval('[data-work-open]',n=>document.activeElement===n)).toBe(true);
  await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);
  await page.screenshot({path:join(out,'work-entry-fixture-'+width+'.png'),fullPage:true});
  await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'no-preference'}]);
 }
 await page.close();
},30000);


test('a failed automatic continuation leaves the ordinary link and modified clicks usable',async()=>{
 const page=await browser.newPage(),response=await load();
 const marker='<script>(function(){var foot=document.currentScript.previousElementSibling';
 expect(response.body).toContain(marker);
 const inject='<script>window.__continuationAttempted=false;window.fetch=()=>{window.__continuationAttempted=true;return Promise.reject(new Error("fixture offline"))};window.IntersectionObserver=class{constructor(cb){this.cb=cb}observe(){setTimeout(()=>this.cb([{isIntersecting:true}]),0)}}</script>';
 const footer='<nav class="keep-reading" data-keep-reading>';expect(response.body).toContain(footer);
 await page.setContent(response.body.replace(footer,inject+footer),{waitUntil:'domcontentloaded'});
 await page.waitForFunction(()=>window.__continuationAttempted&&!document.querySelector('[data-keep-reading]').hasAttribute('data-reading'));
 const result=await page.evaluate(()=>{const link=document.querySelector('[data-keep-reading] a'),events=[];document.addEventListener('click',e=>{events.push(e.defaultPrevented);e.preventDefault()},{once:false});link.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,button:0,ctrlKey:true}));link.dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,button:0}));return {events,href:link.getAttribute('href')};});
 expect(result.events).toEqual([false,false]);expect(result.href).toBe('/2');await page.close();
},30000);

test('WORK preserves distinct same-headline sources below the lead instead of merging by text',async()=>{
 for(const[name,age]of [['work-equal-a',120000],['work-equal-b',130000]]){
  f.add('internet_source',{family:'today',source:name,label:name,says:'Distinct generated RSS fixture',url:'https://'+name+'.test/feed',twin:name,state:'observed',extract:'rss:item',title_field:'title',link_field:'feed:auto',body_regime:'retained-public',front_door:'yes',status:'published',tier:'warm',kind:'post'});
  const path=join(f.home,'foreign/bodies',name+'.xml');writeFileSync(path,'<rss><channel><item><title>Identical headline, independent source</title><link>https://'+name+'.test/notice</link><guid>'+name+'-notice</guid><pubDate>'+new Date(Date.now()-age).toUTCString()+'</pubDate><description>Retained words from '+name+'.</description></item></channel></rss>');writeFileSync(join(f.home,'foreign/receipts',name+'.json'),JSON.stringify({body_path:path}));
 }
 const page=await load();expect(page.status).toBe(200);expect(page.body).toMatch(/href="\/o\/work-equal-a\/[a-f0-9]{24}"/);expect(page.body).toMatch(/href="\/o\/work-equal-b\/[a-f0-9]{24}"/);expect(page.body).toContain('data-work-entrance');
});
test('a future-dated RSS post is not described as a currently published WORK story',async()=>{
 const name='work-future',path=join(f.home,'foreign/bodies',name+'.xml');f.add('internet_source',{family:'today',source:name,label:name,says:'Generated future-dated fixture',url:'https://work-future.test/feed',twin:name,state:'observed',extract:'rss:item',title_field:'title',link_field:'feed:auto',body_regime:'retained-public',front_door:'yes',status:'published',tier:'warm',kind:'post'});
 writeFileSync(path,'<rss><channel><item><title>Not released yet fixture</title><link>https://work-future.test/notice</link><guid>future-notice</guid><pubDate>'+new Date(Date.now()+86400000).toUTCString()+'</pubDate><description>Future-dated publication test.</description></item></channel></rss>');writeFileSync(join(f.home,'foreign/receipts',name+'.json'),JSON.stringify({body_path:path}));
 const page=await load();expect(page.status).toBe(200);expect(page.body).not.toContain('Not released yet fixture');expect(page.body).toContain('Newest retained report');expect(page.body).toContain('data-work-entrance');
});


test('public reading routes retain usable layouts across desktop and phone',async()=>{
 for(const [family,source,kind,items] of [
  ['video','fixture-videos','video',[{name:'A playable fixture video',url:'https://tube.example.test/w/video123',thumb:'https://fixture-media.test/video.jpg'}]],
  ['commons','fixture-pictures','image',[{name:'A retained fixture picture',url:'https://pictures.example.test/picture',thumb:'https://fixture-media.test/picture.jpg'}]],
  ['news','fixture-discover','post',[{name:'A retained fixture story',url:'https://news.example.test/story',thumb:'https://fixture-media.test/story.jpg'}]]
 ]){
  f.add('internet_source',{family,source,label:source,says:'Fixture retained material',url:'https://'+source+'.test/feed',twin:source,state:'observed',extract:'items',title_field:'name',link_field:'url',image_field:'thumb',image_base:'',body_regime:'retained-public',front_door:'yes',status:'published',tier:'warm',kind});
  const path=join(f.home,'foreign/bodies',source+'.json');writeFileSync(path,JSON.stringify({items}));writeFileSync(join(f.home,'foreign/receipts',source+'.json'),JSON.stringify({body_path:path}));
 }
 const page=await browser.newPage();await page.setRequestInterception(true);const pixel=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=','base64');
 page.on('request',r=>r.url().startsWith('https://fixture-media.test/')?r.respond({status:200,contentType:'image/png',body:pixel}):r.url().startsWith(f.base)||r.url().startsWith('data:')?r.continue():r.abort());
 await page.goto(f.base+'/healthz',{waitUntil:'domcontentloaded'});
 for(const route of ['/discover','/watch','/board']){
  const response=await f.request(route,{host:'aurekai.work'});expect(response.status).toBe(200);
  for(const width of [1440,390]){await page.setViewport({width,height:900});await page.setContent(response.body,{waitUntil:'domcontentloaded'});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(await page.$('.hero .reading-mark svg')).not.toBeNull();expect(await page.$eval('.hero h1',n=>getComputedStyle(n).animationName)).toBe('none');await page.screenshot({path:join(out,'reading-'+route.slice(1)+'-'+width+'.png'),fullPage:true});}
 }
 await page.close();
},60000);
