import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {packSetupFixture,root} from './pack_setup_fixture.mjs';
let f,origin='publisher.aurekai.work',destination='writer.aurekai.work',sourceId='publisher-editorial-site',destinationId='profile-writer-work',route='/blog/one-publisher-story/',digest='';
const read=(host,path)=>f.request(path,{host});
 // The exposed MCP test profile can be invoked without Makefile prerequisites.
 // Never validate a new C route against a stale checkout binary.
 execFileSync('make',['-C',join(root,'cmd/BonfyreWebAddress'),'bonfyre-web-address'],{timeout:120000,stdio:'pipe'});
beforeAll(async()=>{
 f=await packSetupFixture();
 const schemas=join(f.home,'schemas');
 for(const name of ['site_presentation','site_fragment','site_publication_placement'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',name+'.json'),join(schemas,name+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const owner=await f.login('placement-owner@example.test');
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(owner.id),'--actor','person:writer']);
 const matrix=join(f.home,'matrix.db');
 for(const [actor,host,id] of [['person:publisher',origin,sourceId],['person:writer',destination,destinationId]]){
  const institution='inst:'+actor.replace(/:/g,'-');
  f.run(f.web,['subject','--db',matrix,'--id',actor,'--kind','Person','--digest','a'.repeat(64),'--evidence','fixture:placement']);
  f.run(f.web,['institution','--db',matrix,'--id',institution,'--kind','Person','--world','live','--evidence','fixture:placement']);
  f.run(f.web,['domain','--db',matrix,'--host',host,'--institution',institution,'--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:placement']);
  f.run(f.web,['presence','--db',matrix,'--id','presence:'+id,'--subject',actor,'--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:placement']);
  f.add('site_presentation',{site_id:id,host,subject_address:actor,territory:'work',profile:'member-profile-v1',navigation:[],status:'published'});
  f.add('site_page',{route:'/',title:id,heading:id,lede:'A published person site.',status:'published'},id);
 }
 f.add('site_page',{route,title:'Original publisher story',heading:'Original publisher story',lede:'Written and governed by the publisher.',eyebrow:'Publishing',editorial_kind:'article',machine_view:'authored',author_credit:'Publisher Editorial',author_subject_ref:'author:publisher-editorial',status:'published'},sourceId);
 f.add('site_fragment',{coordinate:'publisher-story-body',page:route,slot:'showcase',audience:'public',kind:'profile-story',position:0,heading:'Original publisher story',body:'The publisher owns this complete authored body.',semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},projection_contract:{html:true},status:'published'},sourceId);
 const original=await read(origin,route);expect(original.status).toBe(200);
 const marker='<div class="ak-zen-body ak-zen-text">',start=original.body.indexOf(marker)+marker.length,stop=original.body.indexOf('</div></article><script>',start);
 if(start<marker.length||stop<=start)throw Error('Source native composition unavailable for digest witness');
 digest=createHash('sha256').update(original.body.slice(start,stop)).digest('hex');
},60000);
afterAll(async()=>{await f?.close()},30000);
const placement=()=>({placement_id:'publisher-to-writer-article-1',origin_site:sourceId,origin_route:route,origin_page_digest:digest,destination_site:destinationId,destination_route:route,publisher_subject:'person:publisher',destination_subject:'person:writer',review_request_ref:'review:source-and-destination-fixture',cannot_infer:'Placement gives neither authorship nor editor access.',status:'published'});
test('destination approval alone never publishes the original source',async()=>{
 expect((await read(destination,route)).status).toBe(404);
 f.add('site_publication_placement',placement(),destinationId);
 expect((await read(destination,route)).status).toBe(404);
 expect((await read(destination,'/?tab=posts')).body).not.toContain('Original publisher story');
});
test('dual exact approvals render the canonical native article and preserve publisher credit',async()=>{
 f.add('site_publication_placement',placement(),sourceId);
 const article=await read(destination,route),posts=await read(destination,'/?tab=posts');
 expect(article.status).toBe(200);
 expect(article.body).toContain('The publisher owns this complete authored body.');
 expect(article.body).toContain('data-publisher-origin');
 expect(article.body).toContain('Publisher Editorial');
 expect(article.body).toContain('https://'+origin+route);
 expect(article.body).toContain('data-ak-track data-ak-default data-narrate=".ak-zen-body"');
 expect(posts.body).toContain('data-publisher-credit');
 expect(posts.body).toContain('Original publisher story');
 expect(f.sql.query('SELECT count(*) n FROM site_page WHERE namespace=? AND route=?').get(destinationId,route).n).toBe(0);
});
test('Chrome shows publisher-approved subjects as filters on the real profile journal',async()=>{
 const browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({
  headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  args:['--disable-background-networking','--no-proxy-server',
   '--host-resolver-rules=MAP writer.aurekai.work 127.0.0.1']});
 try {
  const page=await browser.newPage();
  const url=f.base.replace('127.0.0.1',destination)+'/?tab=posts';
  const res=await page.goto(url,{waitUntil:'domcontentloaded',timeout:15000});
  expect(res.status()).toBe(200);
  const probe=await page.evaluate(()=>({journal:!!document.querySelector("[data-profile-journal]"),
   cards:[...document.querySelectorAll("[data-article]")].map(x=>x.dataset.category),
   categories:[...document.querySelectorAll("[data-journal-category]")].map(x=>x.dataset.journalCategory),
   clientScript:[...document.scripts].some(x=>x.textContent.includes("filterBar")),
   filters:!!document.querySelector(".ph-journal-filters")}));
  if(!probe.categories.includes("Publishing"))throw Error("Publisher filter not composed: "+JSON.stringify(probe));
  await page.click('[data-journal-category="Publishing"]');
  const result=await page.evaluate(()=>({
   active:[...document.querySelectorAll('[data-journal-category][aria-pressed="true"]')].map(x=>x.dataset.journalCategory),
   articles:[...document.querySelectorAll('[data-article]:not([hidden])')].map(x=>x.dataset.category),
   count:document.querySelector('[data-journal-count]')?.textContent||''
  }));
  expect(result.active).toEqual(['Publishing']);
  expect(result.articles).toEqual(['Publishing']);
  expect(result.count).toContain('Showing 1 of 1 articles');
 } finally {await browser.close();}
},90000);
test('native editorial CSS and SurfaceScene runtime retain their independent source contracts',()=>{
 const script=join(root,'cmd/BonfyreWebAddress/tests/page_surface_scene_test.sh');
 const binary=join(root,'cmd/BonfyreWebAddress/bonfyre-web-address');
 execFileSync('sh',[script,binary],{timeout:60000,stdio:'pipe'});
},90000);
test('source revision invalidates placement, without erasing source ownership',async()=>{
 f.sql.query('UPDATE site_fragment SET body=? WHERE namespace=? AND page=?').run('Changed publisher copy.',sourceId,route);
 expect((await read(destination,route)).status).toBe(404);
 expect((await read(destination,'/?tab=posts')).body).not.toContain('Original publisher story');
 expect((await read(origin,route)).status).toBe(200);
});
