import {test as runTest,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {Database} from 'bun:sqlite';
import {packSetupFixture,root,decode} from './pack_setup_fixture.mjs';
const test=(name,fn,timeout=30000)=>runTest(name,fn,timeout);let f,owner,other,browser,matrix,workIndex;
const sites=['work','life','live','shop'].map(t=>({id:'profile-avery-'+t,host:'avery.aurekai.'+t,territory:t}));
const hidden=body=>Object.fromEntries([...body.matchAll(/<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));
async function request(path,{site='work',user=owner,form,headers={}}={}){return f.request(path,{host:'aurekai.'+site,cookie:user?.cookie||'',form,headers:{...(form!==undefined?{Origin:'https://aurekai.'+site}:{}),...headers}});}
async function publicPage(site='work',path='/',user=null){return f.request(path,{host:'avery.aurekai.'+site,cookie:user?.cookie||''});}
function post(site,title,body,extra={}){return f.add('community_post',{post:site+':'+title,community:site,author_subject:'person:avery',title,body,subject_address:'',published_at:'2026-10-21T05:00:00Z',visibility:'public',moderation_state:'accepted',cannot_infer:'Isolated authored fixture.',status:'published',...extra},site);}
beforeAll(async()=>{f=await packSetupFixture({beforeServe:({home,env})=>{workIndex=join(home,'account-work.db');env.BONFYRE_ACCOUNT_WORK_DB=workIndex;env.BONFYRE_ITEM_ARCHIVE_DB=join(home,"profile-items.db");const w=new Database(workIndex,{create:true});w.exec("CREATE TABLE account_work(id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,title TEXT NOT NULL,state TEXT NOT NULL,updated_ms INTEGER NOT NULL)");w.close();}});const schemas=join(f.home,'schemas');for(const type of ['site_presentation','site_fragment','community_post','community_body','saved_item','work_example'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);owner=await f.login('profile-owner@example.test');other=await f.login('profile-other@example.test');f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Private Account Name',owner.id);const db=join(f.home,'matrix.db');f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(owner.id),'--actor','person:avery']);f.run(f.web,['subject','--db',db,'--id','person:avery','--kind','Person','--digest','a'.repeat(64),'--evidence','fixture:profile']);f.run(f.web,['institution','--db',db,'--id','inst:person-avery','--kind','Person','--world','live','--evidence','fixture:profile']);
 const w=new Database(workIndex),insert=w.query('INSERT INTO account_work VALUES(?,?,?,?,?,?)');
 insert.run('account-operator-owned-1','user:'+owner.id,'self','Review the source-backed owner decision','blocked',1793175000000);
 insert.run('account-operator-owned-2','user:'+owner.id,'self','Continue an admitted native workflow','ready',1793174000000);
 insert.run('account-operator-other-1','user:'+other.id,'self','Other person private work','blocked',1793173000000);w.close();
 for(const s of sites){f.run(f.web,['domain','--db',db,'--host',s.host,'--institution','inst:person-avery','--territory',s.territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);f.run(f.web,['presence','--db',db,'--id','presence:avery:'+s.territory,'--subject','person:avery','--territory',s.territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);f.add('site_presentation',{site_id:s.id,host:s.host,subject_address:'person:avery',territory:s.territory,profile:'member-profile-v1',navigation:[],status:'published'});f.add('site_page',{route:'/',title:s.territory+' profile',heading:'Avery '+s.territory,lede:s.territory+' introduction',status:'published'},s.id);post(s.id,s.territory+'-only-public-post',s.territory+' public words');}
 post(sites[0].id,'private-post-marker','Never show this',{visibility:'self'});post(sites[0].id,'draft-post-marker','Never show draft',{status:'draft'});post(sites[0].id,'wrong-author-marker','Never show another author',{author_subject:'person:someone-else'});f.add('work_example',{example_id:'profile-work-release',site:'profile-avery-work',pack:'profile-workflow',program_template:'fixture-program',territories:'work',position:1,kicker:'Client release',headline:'A site-bound work release',dek:'A fixture release belongs only to this profile site.',made_with:'Fixture material',made_with_href:'https://example.test/material',observed_at:'2026-10-25T12:00:00Z',picture:'/assets/world-land.svg',picture_alt:'Fixture work',status:'published'});f.add('work_example',{example_id:'unbound-work-release',pack:'profile-workflow',territories:'work',position:2,kicker:'Unbound',headline:'Never publish this globally',dek:'This release has no profile binding.',made_with:'Fixture material',observed_at:'2026-10-25T12:00:00Z',picture:'/assets/world-land.svg',picture_alt:'Unbound work',status:'published'});
 f.add('saved_item',{saved:'owner-save',actor:'user:'+owner.id,item_ref:'newsdesk/'+'1'.repeat(24),title:'Owner private bookmark',href:'https://example.test/original',publisher:'Source publisher',summary:'Private saved excerpt MUST NOT be published',saved_at:'2026-10-21T05:00:00Z',collection:'Reading',cannot_infer:'Private fixture.',status:'published'});f.add('saved_item',{saved:'other-save',actor:'user:'+other.id,item_ref:'newsdesk/'+'2'.repeat(24),title:'Other account private bookmark',href:'https://example.test/other',summary:'Other private body',collection:'Other reading',saved_at:'2026-10-21T05:00:00Z',cannot_infer:'Private fixture.',status:'published'});
 matrix=new Database(db);browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking','--no-proxy-server','--host-resolver-rules=MAP aurekai.life 127.0.0.1']});
},60000);
afterAll(async()=>{await browser?.close();matrix?.close();await f?.close();},30000);
test('dating is a private profile composition with exact owner and origin boundaries',async()=>{
 const discover=await request('/profile?tab=dating');
 expect(discover.status).toBe(200);
 expect(discover.body).toContain('data-native-dating');
 expect(discover.body).toContain('data-bf-stitch="dating-editorial"');
 expect(discover.body).toContain('Meet beyond the profile');
 expect(discover.body).toContain('The private Participation owner is not installed');
 const schemaDir=join(f.home,'schemas');
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(schemaDir,'site_fragment.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemaDir]);
 f.add('site_fragment',{coordinate:'private-life-discover-copy',page:'/profile/dating',slot:'discover',
  audience:'self',kind:'dating-view',position:1,eyebrow:'Aurekai Life',
  heading:'An authored introduction',body:'Private CMS editorial text.',status:'published'},'root');
 const customized=await request('/profile?tab=dating');
 expect(customized.body).toContain('An authored introduction');
 expect(customized.body).toContain('Private CMS editorial text.');
 expect((await publicPage('life')).body).not.toContain('Private CMS editorial text.');
 expect((await request('/profile?tab=dating',{user:null})).status).toBe(303);
 const studio=await request('/profile?tab=dating&view=studio');
 expect(studio.status).toBe(200);
 expect(studio.body).toContain('Your authored Life profile');
 expect(studio.body).toContain('/cms/compose/advanced?site=profile-avery-life');
 expect((await request('/profile?tab=dating&view=matches')).status).toBe(200);
 expect((await request('/profile?tab=dating&view=chat')).status).toBe(200);
 expect((await request('/profile?tab=dating&view=chat&peer=person:avery&before=123')).status).toBe(200);
 expect((await request('/profile?tab=dating&view=chat&peer=person:avery&before=0')).status).toBe(400);
 expect((await request('/profile?tab=dating&view=chat&peer=person:avery&before=9223372036854775808')).status).toBe(400);
 expect((await request('/profile?tab=dating&view=matches&before=123')).status).toBe(400);
 expect((await request('/profile?tab=dating&view=chat&before=123')).status).toBe(400);
 expect((await request('/profile?tab=dating&view=discover&peer=person:other')).status).toBe(400);
 expect((await publicPage('life','/?tab=dating')).status).toBe(400);
 expect((await request('/profile?tab=dating',{user:null})).status).toBe(303);
 const form={rev:'0',enable:'1',ack:'1'};
 expect((await request('/profile/dating/request',{form,headers:{Origin:'https://foreign.test'}})).status).toBe(403);
 expect((await request('/profile/dating/request',{form})).status).toBe(503);
});
test('Stitch member feed filters only actually published site posts and keeps private data out',async()=>{
 const work=await publicPage('work'),life=await publicPage('life');
 expect(work.body).toContain('/assets/profile-journal.css');
 expect(work.body).not.toContain('<style data-bf-journal>');
 const journalStyle=await f.request('/assets/profile-journal.css',{host:'avery.aurekai.work'});
 expect(journalStyle.status).toBe(200);expect(journalStyle.body).toContain('.ph-journal');
 const barStyle=await f.request('/assets/aurekai-bar.css',{host:'avery.aurekai.work'});
 expect(barStyle.status).toBe(200);
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage(),errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 try{
  await page.setViewport({width:1440,height:920});await page.setContent(work.body.replace('</head>','<style>'+barStyle.body+'</style></head>'),{waitUntil:'domcontentloaded'});
  const icon=await page.$eval('.ak-bar .ak-icon',n=>({width:n.getBoundingClientRect().width,font:parseFloat(getComputedStyle(n).fontSize)}));
  expect(icon.width).toBeGreaterThan(0);expect(icon.width).toBeLessThanOrEqual(icon.font*1.1);
  expect(await page.$$eval('article[data-public-post]',nodes=>nodes.length)).toBe(1);
  expect(await page.$eval('article[data-public-post]',n=>n.textContent)).toContain('work-only-public-post');
  expect(await page.$eval('body',n=>n.textContent)).not.toContain('Other account private bookmark');
  expect(work.body).not.toContain('life-only-public-post');
  expect(life.body).not.toContain('work-only-public-post');
  await page.setViewport({width:390,height:900});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  expect(await page.$eval('.ak-places a span',n=>getComputedStyle(n).position)).toBe('absolute');
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
});
test('profile component styles load from the compiled asset at desktop and phone widths',async()=>{
 const own=await request('/profile?site=profile-avery-work');
 const css=await f.request('/assets/aurekai-native.css',{host:'avery.aurekai.work'});
 expect(css.status).toBe(200);expect(own.body).not.toContain('<style>.ph-realm-grid');
 const context=await browser.createBrowserContext(),page=await context.newPage();
 try{
  await page.setViewport({width:1440,height:900});
  await page.setContent(own.body.replace('</head>','<style>'+css.body+'</style></head>'),{waitUntil:'domcontentloaded'});
  expect(await page.$eval('.ph-realm-grid',n=>getComputedStyle(n).gridTemplateColumns.split(' ').length)).toBe(2);
  await page.setViewport({width:390,height:900});
  expect(await page.$eval('.ph-realm-grid',n=>getComputedStyle(n).gridTemplateColumns.split(' ').length)).toBe(1);
 }finally{await context.close();}
});
test('People resolves a published person directly to their existing public Work profile',async()=>{
 const exact=await request('/people/avery');
 expect(exact.status).toBe(303);
 expect(exact.headers.get('location')).toBe('https://avery.aurekai.work/');
 const subject=await request('/people?subject=person:avery');
 expect(subject.status).toBe(303);
 expect(subject.headers.get('location')).toBe('https://avery.aurekai.work/');
 const published=await publicPage('work');
 expect(published.status).toBe(200);
 expect(published.body).toContain('work-only-public-post');
 for(const unknown of ['/people/unknown','/people/avery%2Fevil','/people?subject=person:unknown','/people?subject=person:avery/other']){
   const missing=await request(unknown);expect(missing.status).toBe(404);
   expect(missing.headers.get('location')).toBeNull();
 }
},30000);
test('unclaimed credited profile dogfoods exact retained publications without becoming a member',async()=>{
 const archive=new Database(join(f.home,'profile-items.db'),{create:true});
 try{
  archive.exec('CREATE TABLE IF NOT EXISTS item(source TEXT,key TEXT,kind TEXT,family TEXT,title TEXT,body TEXT,href TEXT,image TEXT,by TEXT,publisher TEXT,ident TEXT,at INTEGER,first_read TEXT,last_read TEXT)');
  const item=archive.query('INSERT INTO item(source,by,key,title,kind,publisher,at,family) VALUES(?,?,?,?,?,?,?,?)');
  item.run('source-a','Avery Observer','aaaaaaaaaaaaaaaaaaaaaaaa','A source A authored report','article','Independent Publisher A',1792972800,'news');
  item.run('source-b','Avery Observer','bbbbbbbbbbbbbbbbbbbbbbbb','A source B separate report','article','Independent Publisher B',1792886400,'news');
  item.run('source-a','Avery Observer','cccccccccccccccccccccccc','Future schedule not a published credit','article','Independent Publisher A',2019686400,'news');
  item.run('source-a','Avery Observer','eeeeeeeeeeeeeeeeeeeeeeee','Jobs listing not a byline publication','article','Independent Publisher A',1792886400,'jobs');
 }finally{archive.close();}
 const url='/people?who=Avery%20Observer&source=source-a&profile=1&tab=posts';
 const sourceA=await request(url,{user:null});
 expect(sourceA.status).toBe(200);
 expect(sourceA.body).toContain('data-credit-observation');
  const canonical=await request('/people?who=Avery%20Observer&source=source-a',{user:null});
  expect(canonical.status).toBe(200);
  expect(canonical.body).toContain('data-credit-observation');
  expect(canonical.body).toContain('data-profile-template="organization-subprofile-v1"');
  expect(canonical.body).toContain('A source A authored report');
  expect(canonical.body).not.toContain('A source B separate report');
 expect(sourceA.body).toContain('data-profile-page');
 expect(sourceA.body).toContain('data-profile-template="organization-subprofile-v1"');
 expect(sourceA.body).toContain('A source A authored report');
 expect(sourceA.body).toContain('/o/source-a/aaaaaaaaaaaaaaaaaaaaaaaa');
 expect(sourceA.body).toContain("id='posts'");
 expect(sourceA.body).toContain('href="#posts"');
 for(const foreign of ['A source B separate report','Future schedule not a published credit','Jobs listing not a byline publication','Owner private bookmark','Other account private body','data-public-post','Write a post','Manage this post'])
  expect(sourceA.body).not.toContain(foreign);
 expect((await request('/people?who=Avery%20Observer&source=source-b&profile=1&tab=posts',{user:null})).body).toContain('A source B separate report');
 expect((await request('/people?who=Avery%20Observer&source=source-a&profile=1&tab=organizations',{user:null})).body).toContain('not evidence of organizational membership');
 for(const invalid of ['/people?who=Avery%20Observer&source=unknown&profile=1','/people?who=Anonymous&source=source-a&profile=1','/people?who=Avery%20Observer&source=source-a&profile=1&tab=compose'])
  expect((await request(invalid,{user:null})).status).toBe(404);
});


test('a published cover remains bound to its exact site and current source rights',async()=>{
 const {createHash}=await import('node:crypto');
 const {encode}=await import('../../../integrations/browser/workspace-msgpack.mjs');
 const {writeFileSync}=await import('node:fs');
 const schemas=join(f.home,'schemas');
 for(const type of ['internet_source','site_fragment'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const source='fixture-art',title='Fixture artwork',maker='A Maker',href='https://art.example.test/work/1',image='https://images.example.test/art.jpg';
 const key=createHash('sha256').update(source+'\n'+href).digest('hex').slice(0,24);
 const body=rights=>JSON.stringify({data:[{title,url:href,image,maker,rights}]});
 const path=join(f.home,'foreign/bodies',source+'.body');
 writeFileSync(path,body('CC0'));writeFileSync(join(f.home,'foreign/receipts',source+'-twin.json'),JSON.stringify({body_path:path}));
 f.add('internet_source',{family:'museums',source,label:'Fixture museum',says:'Fixture source',url:'https://art.example.test/api',twin:source+'-twin',state:'observed',extract:'data',title_field:'title',link_field:'url',image_field:'image',sub_field:'maker',rights_field:'rights',body_regime:'retained-public',status:'published',tier:'warm',kind:'image'});
 const evidence=createHash('sha256').update(encode(['bonfyre.profile-cover-review.v1',source,key,title,maker,href,image,'CC0',50,50])).digest('hex');
 f.add('site_fragment',{coordinate:'profile-cover-'+sites[0].id,page:'/',slot:'cover',audience:'public',kind:'profile-cover',position:5100,source_ref:source+'/'+key,evidence_ref:evidence,status:'published'},sites[0].id);
 f.add('site_fragment',{coordinate:'profile-cover-'+sites[1].id,page:'/',slot:'cover',audience:'public',kind:'profile-cover',position:5100,source_ref:source+'/'+key,evidence_ref:evidence,status:'draft'},sites[1].id);
 const work=await publicPage('work'),life=await publicPage('life');
 expect(work.body).toContain('data-profile-cover');expect(work.body).toContain(image);expect(work.body).toContain('Fixture artwork');expect(work.body).toContain('CC0');
 const wildcard=await f.request('/',{host:sites[0].host,headers:{Accept:'*/*'}});expect(wildcard.status).toBe(200);expect(wildcard.body).toContain('data-profile-cover');expect(wildcard.body).toContain('Fixture artwork');
 const json=await f.request('/',{host:sites[0].host,headers:{Accept:'application/json, */*'}});expect(json.body).not.toContain('data-profile-cover');
 expect((await f.request('/',{host:sites[1].host,headers:{Accept:'*/*'}})).body).not.toContain('data-profile-cover');
 const nativeStyle=await f.request('/assets/aurekai-native.css',{host:sites[0].host});
 expect(nativeStyle.status).toBe(200);
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage();
 try{for(const width of [1440,390]){await page.setViewport({width,height:900});await page.setContent(work.body.replace('</head>','<style>'+nativeStyle.body+'</style></head>'),{waitUntil:'domcontentloaded'});
  const geometry=await page.$eval('[data-profile-cover]',n=>({width:n.getBoundingClientRect().width,height:n.getBoundingClientRect().height,overflow:document.documentElement.scrollWidth-innerWidth}));
  expect(geometry.height).toBeGreaterThanOrEqual(180);expect(geometry.width).toBeLessThanOrEqual(width);expect(geometry.overflow).toBeLessThanOrEqual(1);
  expect(await page.$eval('[data-profile-cover] img',n=>n.getAttribute('alt'))).toBe(title);
 }}finally{await ctx.close();}
 expect(life.body).not.toContain('data-profile-cover');expect(life.body).not.toContain(image);
 const self=await request('/profile?site='+sites[0].id);
 const form=self.body.match(/<form action="\/profile\/cover\/remove"[^>]*>([\s\S]*?)<\/form>/);
 expect(form).not.toBeNull();const reviewed=hidden(form[1]);
 expect((await request('/profile/cover/remove',{user:other,form:reviewed})).status).toBe(404);
 f.sql.query("UPDATE site_fragment SET position=5101 WHERE namespace=? AND kind='profile-cover'").run(sites[0].id);
 expect((await request('/profile/cover/remove',{form:reviewed})).status).toBe(409);
 expect((await publicPage('work')).body).not.toContain('data-profile-cover');
 f.sql.query("UPDATE site_fragment SET position=5100 WHERE namespace=? AND kind='profile-cover'").run(sites[0].id);
 expect((await request('/profile/cover/remove',{form:reviewed})).status).toBe(303);
 expect((await publicPage('work')).body).not.toContain('data-profile-cover');
 f.sql.query("UPDATE site_fragment SET status='published' WHERE namespace=? AND kind='profile-cover'").run(sites[0].id);
 f.sql.query("UPDATE site_fragment SET status='draft' WHERE namespace=? AND kind='profile-cover'").run(sites[0].id);
 expect((await publicPage('work')).body).not.toContain('data-profile-cover');
 f.sql.query("UPDATE site_fragment SET status='published' WHERE namespace=? AND kind='profile-cover'").run(sites[0].id);
 writeFileSync(path,body('restricted'));
 expect((await publicPage('work')).body).not.toContain('data-profile-cover');
});
test('private profile shows real account work without publishing work to a site',async()=>{
 const own=await request('/profile');expect(own.status).toBe(200);
 expect(own.body).toContain('data-private-work');expect(own.body).toContain('data-work-total="2"');expect(own.body).toContain('data-work-attention="1"');
 expect(own.body).toContain('data-profile-site-overview');expect((own.body.match(/1 published post<\/p>/g)||[]).length).toBe(4);
 for(const t of ['work','life','live','shop'])expect(own.body).toContain('data-profile-territory="'+t+'"');
 expect((own.body.match(/class="ph-realm-icon"/g)||[]).length).toBe(4);
 for(const label of ['Edit introduction','View public site','Manage site'])expect(own.body).toContain(label);
 expect((own.body.match(/class="ph-tab-icon"/g)||[]).length).toBe(10);
 const ownSites=await request('/profile?tab=sites');expect(ownSites.status).toBe(200);
 expect(ownSites.body).toContain('data-profile-site-overview');
 for(const t of ['work','life','live','shop'])expect(ownSites.body).toContain('data-profile-territory="'+t+'"');
 expect(ownSites.body).not.toContain('Other account private bookmark');
 const ownActivity=await request('/profile?tab=activity');expect(ownActivity.status).toBe(200);
 expect(ownActivity.body).toContain('data-private-work');
 expect(ownActivity.body).toContain('Review the source-backed owner decision');
 expect(ownActivity.body).not.toContain('Other person private work');
 for(const x of ['Review the source-backed owner decision','Continue an admitted native workflow','/inbox?context=self','/work-board?context=self','/apps?context=self'])expect(own.body).toContain(x);
 expect(own.body).not.toContain('Other person private work');
 const foreign=await request('/profile',{user:other});expect(foreign.body).toContain('data-work-total="1"');expect(foreign.body).toContain('Other person private work');expect(foreign.body).not.toContain('Review the source-backed owner decision');expect(foreign.body).not.toContain('data-profile-site-overview');
 for(const t of ['work','life','live','shop']){const publicBody=(await publicPage(t)).body;expect(publicBody).not.toContain('data-private-work');expect(publicBody).not.toContain('data-profile-site-overview');expect(publicBody).not.toContain('Review the source-backed owner decision');}
});
test('a missing Life presence stays unbound without inventing a published site',async()=>{
 matrix.query("UPDATE matrix_presences SET active=0 WHERE presence_id='presence:avery:life'").run();
 try{
  const response=await request('/profile?tab=sites');
  expect(response.status).toBe(200);
  expect(response.body).toContain('data-profile-unbound="life"');
  expect(response.body).not.toContain('data-profile-territory="life"');
  for(const territory of ['work','live','shop'])expect(response.body).toContain('data-profile-territory="'+territory+'"');
  expect(response.body).not.toContain('life-only-public-post');
 }finally{matrix.query("UPDATE matrix_presences SET active=1 WHERE presence_id='presence:avery:life'").run();}
});
test('four sites share one template but never share introductions or posts implicitly',async()=>{for(const t of ['work','life','live','shop']){const r=await publicPage(t);expect(r.status).toBe(200);expect(r.body).toContain('data-profile-template="member-v1"');expect(r.body).toContain('data-profile-site="profile-avery-'+t+'"');expect(r.body).toContain(t+'-only-public-post');for(const wrong of ['work','life','live','shop'].filter(x=>x!==t))expect(r.body).not.toContain(wrong+'-only-public-post');for(const secret of ['Owner private bookmark','Other account private bookmark','Private Account Name','private-post-marker','draft-post-marker','wrong-author-marker','Write a post','Manage this post'])expect(r.body).not.toContain(secret);expect(r.headers.get('cache-control')).toContain('no-store');}});
test('released work stays with its bound account profile',async()=>{const work=await publicPage('work');const life=await publicPage('life');expect(work.body).toContain('A site-bound work release');expect(work.body).toContain('data-profile-work');expect(life.body).not.toContain('A site-bound work release');expect(work.body).not.toContain('Never publish this globally');expect((await request('/examples')).status).toBe(410);expect(work.body).toContain('href="/programs?context=self&amp;template=fixture-program">Start this as a program</a>');expect(work.body).toContain('href="/packs/setup?pack=profile-workflow&amp;context=self"');expect(work.body).not.toContain('Set up this workflow');const programs=await request('/programs?context=self&template=fixture-program');expect(programs.status).toBe(200);expect(programs.body).toContain('data-template="fixture-program"');expect(programs.body).toContain('<input type="hidden" name="template" value="fixture-program">');for(const bad of ['template=fixture%3Cprogram','template=fixture-program&instance=pws-x','template=a&template=b','template=-lead','template='+'a'.repeat(161),'context=self&operation=catalog&template=fixture-program'])expect((await request('/programs?context=self&'+bad)).status).toBe(400);expect((await request('/apps/_programs?context=self&operation=catalog&template=fixture-program')).status).toBe(400);const anonymous=await request('/programs?context=self&template=fixture-program',{user:null});expect(anonymous.status).toBe(303);expect(decodeURIComponent(anonymous.headers.get('location'))).toBe('/login?next=/programs?context=self&template=fixture-program');});
test('Profile opens the current territory; account saves remain private across all four territories',async()=>{for(const t of ['work','life','live','shop']){const r=await request('/profile',{site:t});expect(r.status).toBe(200);expect(r.body).toContain('data-profile-site="profile-avery-'+t+'"');expect(r.body).toContain('Owner private bookmark');expect(r.body).not.toContain('Other account private bookmark');expect(r.body).toContain('noindex,nofollow');const nav=await request('/account/navigation',{site:t});expect(nav.body).toContain('data-profile-home href="/profile"');expect(nav.body).toContain('https://avery.aurekai.'+t+'/');expect(nav.body).not.toContain('/signup');}});
test('People resolves an exact published member to their existing public profile rather than a second detail page',async()=>{
 const direct=await request('/people?subject=person:avery',{user:null});
 expect(direct.status).toBe(303);expect(direct.headers.get('location')).toBe('https://avery.aurekai.work/');
 const legacy=await request('/people?who=person%3Aavery',{user:null});
 expect(legacy.status).toBe(303);expect(legacy.headers.get('location')).toBe('https://avery.aurekai.work/');
 for(const who of ['person:unknown','person:../avery','person:avery.example','person:other']){
  const r=await request('/people?subject='+encodeURIComponent(who),{user:null});
  expect(r.status).toBe(404);expect(r.headers.get('location')).toBeNull();
 }
 const posts=await publicPage('work','/?tab=posts');
 expect(posts.status).toBe(200);expect(posts.body).toContain('work-only-public-post');
 expect(posts.body).not.toContain('Owner private bookmark');
});
test('public member buckets reuse exact published community owners without exposing private organization access',async()=>{
 const fields=(community,title,actor,status='published',visibility='public')=>({community,subject_address:'person:avery',title,description:'A public community with its own conversations',visibility,governance_scope:'community',moderation_policy:'aurekai-member-conversation-v1',created_by_subject:actor,cannot_infer:'Public source visibility does not disclose private membership.',status});
 f.add('community_body',fields('avery-circle','Avery Public Circle','user:'+owner.id),'root');
 f.add('community_body',fields('other-circle','Someone Else Circle','user:'+other.id),'root');
 f.add('community_body',fields('avery-draft','Never Display Draft','user:'+owner.id,'draft'),'root');
 f.add('community_body',fields('avery-private','Never Display Private','user:'+owner.id,'published','self'),'root');
 const current=await publicPage('work','/?tab=communities');
 expect(current.status).toBe(200);expect(current.body).toContain('data-profile-communities');
 expect(current.body).toContain('Avery Public Circle');expect(current.body).toContain('/community?community=avery-circle');
 for(const secret of ['Someone Else Circle','Never Display Draft','Never Display Private','Other account private bookmark','Private Account Name'])expect(current.body).not.toContain(secret);
 const orgs=await publicPage('work','/?tab=organizations');expect(orgs.status).toBe(200);
 expect(orgs.body).toContain('data-profile-organizations');expect(orgs.body).toContain('href="/organisations"');
 expect(orgs.body).not.toContain('Other account private bookmark');
 const home=await publicPage('work');
 for(const tab of ['overview','gallery','posts','about','communities','organizations'])expect(home.body).toContain('/?tab='+tab);
 const alias=await request('/people/avery',{user:null});expect(alias.status).toBe(303);expect(alias.headers.get('location')).toBe('https://avery.aurekai.work/');
 const sub=await request('/people/avery/communities',{user:null});expect(sub.status).toBe(303);expect(sub.headers.get('location')).toBe('https://avery.aurekai.work/?tab=communities');
 const postAlias=await request('/people/person:avery/posts',{user:null});expect(postAlias.status).toBe(303);expect(postAlias.headers.get('location')).toBe('https://avery.aurekai.work/?tab=posts');
 const invalid=await request('/people/avery/saved',{user:null});expect(invalid.status).toBe(404);expect(invalid.headers.get('location')).toBeNull();
});
test('Profile Discover Keep Create links are native, icon-mapped, private and site-scoped',async()=>{
 for(const site of ['work','life','live','shop']){
  const own=await request('/profile',{site});
  expect(own.status).toBe(200);
  expect((own.body.match(/data-pathway-stage="/g)||[]).length).toBe(3);
  expect((own.body.match(/data-pathway-link="/g)||[]).length).toBe(9);
  for(const title of ['Sources','Recipes','Feeds','Profile','Collections','Posts','Worlds','Programs'])
   expect(own.body).toContain('data-pathway-link="'+title+'"');
  expect(own.body).toContain('One retained source identity, multiple account-owned continuations, explicit publication review.');
  expect(own.body).toContain('href="/sources"');
  expect(own.body).toContain('href="/recipes"');
  expect(own.body).toContain('href="/channels"');
  expect(own.body).toContain('href="/profile?tab=saved"');
  expect(own.body).toContain('href="/programs?context=self"');
  expect(own.body).toContain('href="/profile?site=profile-avery-'+site+'&amp;tab=compose"');
  expect(own.body).toContain('href="/workspace/worlds?site=profile-avery-'+site+'"');
  const stageMarkup=own.body.match(/<section class="ph-pathway"[^>]*>[\s\S]*?<\/nav><\/section>/g)||[];
  expect(stageMarkup).toHaveLength(3);
  for(const stage of stageMarkup){expect((stage.match(/<svg /g)||[]).length).toBeGreaterThanOrEqual(3);}
  const outside=await publicPage(site);
  expect(outside.body).not.toContain('data-profile-pathways');
 }
 const visitor=await request('/profile',{user:other});
 expect(visitor.body).not.toContain('profile-avery-work&amp;tab=compose');
});
test('one exact retained source opens a reviewed, site-scoped profile post without copying source text',async()=>{
 const source='fixture-profile-jobs',key='a'.repeat(24),ref=source+'/'+key;
 const schemas=join(f.home,'schemas');
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/internet_source.json'),join(schemas,'internet_source.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.add('internet_source',{family:'jobs',source,label:'Fixture profile source',says:'Retained public job',url:'https://jobs.example.test/feed',twin:'fixture-profile-twin',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'text',front_door:'yes'});
 const archive=join(f.home,'profile-items.db');
 f.run('/usr/bin/sqlite3',[archive,"CREATE TABLE IF NOT EXISTS item(source TEXT,key TEXT,kind TEXT,family TEXT,title TEXT,body TEXT,href TEXT,image TEXT,by TEXT,publisher TEXT,ident TEXT,at INTEGER,first_read TEXT); INSERT INTO item(source,key,kind,family,title,body,href,image,by,publisher,ident,at,first_read) VALUES('fixture-profile-jobs','aaaaaaaaaaaaaaaaaaaaaaaa','publication','jobs','Fixture retained opportunity','PRIVATE RETAINED BODY MUST NOT BE REPOSTED','https://jobs.example.test/opportunity','','','Fixture publisher','fixture-profile-job',1792930001,'2026-11-01');"]);
 const publicLink='https://aurekai.work/o/'+ref;
 const exact=await request('/profile?site=profile-avery-life&tab=compose&ref='+encodeURIComponent(ref));
 expect(exact.status).toBe(200);
 expect(exact.body).toContain('value="Fixture retained opportunity"');
 expect(exact.body).toContain('name="ref" value="'+ref+'"');
 expect(exact.body).toContain(publicLink);
 expect(exact.body).not.toContain('PRIVATE RETAINED BODY MUST NOT BE REPOSTED');
 const form={...hidden(exact.body),title:'Fixture retained opportunity',text:'My original interpretation of the retained source.'};
 const before=f.sql.query("SELECT count(*) AS n FROM community_post WHERE namespace='profile-avery-life'").get().n;
 const tamper=await request('/profile/preview',{form:{...form,href:'https://example.test/different'}});
 expect(tamper.status).toBe(409);
 const preview=await request('/profile/preview',{form});
 expect(preview.status).toBe(200);
 expect(preview.body).toContain('Review before publishing');
 expect(preview.body).not.toContain('PRIVATE RETAINED BODY MUST NOT BE REPOSTED');
 expect(f.sql.query("SELECT count(*) AS n FROM community_post WHERE namespace='profile-avery-life'").get().n).toBe(before);
 const committed=hidden(preview.body);
 expect(committed.ref).toBe(ref);
 f.sql.query("UPDATE internet_source SET status='draft' WHERE source=?").run(source);
 try{expect((await request('/profile/publish',{form:committed})).status).toBe(409);}finally{f.sql.query("UPDATE internet_source SET status='published' WHERE source=?").run(source);}
 expect((await request('/profile/publish',{form:committed})).status).toBe(303);
 const life=await publicPage('life'),work=await publicPage('work');
 expect(life.body).toContain('Fixture retained opportunity');
 expect(life.body).toContain(publicLink);
 expect(life.body).not.toContain('PRIVATE RETAINED BODY MUST NOT BE REPOSTED');
 expect(work.body).not.toContain('My original interpretation of the retained source.');
 const row=f.sql.query("SELECT subject_address,body FROM community_post WHERE title='Fixture retained opportunity' AND namespace='profile-avery-life'").get();
 expect(row).toEqual({subject_address:publicLink,body:form.text});
 expect((await request('/profile/publish',{form:committed})).status).toBe(303);
 expect(f.sql.query("SELECT count(*) AS n FROM community_post WHERE namespace='profile-avery-life'").get().n).toBe(before+1);
 const foreign=await request('/profile?site=profile-avery-life&tab=compose&ref='+encodeURIComponent(ref),{user:other});
 expect(foreign.status).toBe(404);
},60000);
test('profile parameters cannot select another account or widen a public site',async()=>{expect((await request('/profile?site=profile-avery-work',{user:other})).status).toBe(404);expect((await request('/profile',{user:null})).status).toBe(303);for(const q of ['tab=saved','site=profile-avery-life','actor=person:avery','viewer=self','tab=posts&tab=saved']){const r=await publicPage('work','/?'+q);expect(r.status).toBe(400);expect(r.body).not.toContain('Private saved excerpt');}const r=await request('/profile?tab=saved',{user:other});expect(r.body).toContain('Other account private bookmark');expect(r.body).not.toContain('Owner private bookmark');});
test('site-specific profile edits preserve exact long text and reject stale or foreign changes',async()=>{const form=hidden((await request('/profile?tab=edit')).body),text='A professional introduction.\n'+('Exact text with quotes " and caf\u00E9. '.repeat(155));const changed=await request('/profile/edit',{form:{...form,title:'Avery at work',text}});expect(changed.status).toBe(303);expect((await publicPage('work')).body).toContain('Avery at work');expect((await publicPage('life')).body).not.toContain('Avery at work');expect(f.sql.query("SELECT lede FROM site_page WHERE namespace='profile-avery-work' AND route='/'").get().lede).toBe(text);expect((await request('/profile/edit',{form:{...form,title:'Stale name',text:'Stale'}})).status).toBe(409);expect((await request('/profile/edit',{user:other,form:{...form,title:'Foreign',text:'No'}})).status).toBe(404);});
test('public posting requires review, remains site-scoped and retries never duplicate it',async()=>{const form=hidden((await request('/profile?tab=compose')).body),value={...form,title:'Work publication witness',text:'Authored work post <script>window.bad=1</script>',href:'https://example.test/work'};const before=f.sql.query('SELECT count(*) AS n FROM community_post').get().n;const preview=await request('/profile/preview',{form:value});expect(preview.status).toBe(200);expect(preview.body).toContain('avery.aurekai.work');expect(f.sql.query('SELECT count(*) AS n FROM community_post').get().n).toBe(before);expect(preview.body).not.toContain('<script>window.bad');const commit=hidden(preview.body);expect((await request('/profile/publish',{form:commit})).status).toBe(303);expect((await request('/profile/publish',{form:commit})).status).toBe(303);expect(f.sql.query('SELECT count(*) AS n FROM community_post').get().n).toBe(before+1);expect((await publicPage('work')).body).toContain('Work publication witness');expect((await publicPage('life')).body).not.toContain('Work publication witness');expect((await request('/profile/publish',{form:{...commit,text:'Different bytes'}})).status).toBe(409);});
test('sharing a saved link does not copy the private excerpt or change another site',async()=>{const saved=f.sql.query("SELECT id FROM saved_item WHERE actor=? AND saved='owner-save'").get('user:'+owner.id);const form=hidden((await request('/profile?site=profile-avery-life&tab=compose&item='+saved.id)).body);const value={...form,title:'Life reading recommendation',text:'My own introduction to this source.'};expect((await request('/profile/publish',{form:value})).status).toBe(303);const life=await publicPage('life');expect(life.body).toContain('Life reading recommendation');expect(life.body).toContain('https://example.test/original');expect(life.body).not.toContain('Private saved excerpt');expect((await publicPage('work')).body).not.toContain('Life reading recommendation');expect(f.sql.query('SELECT summary FROM saved_item WHERE id=?').get(saved.id).summary).toBe('Private saved excerpt MUST NOT be published');const foreign=f.sql.query("SELECT id FROM saved_item WHERE actor=?").get('user:'+other.id);expect((await request('/profile/publish',{form:{...value,item:String(foreign.id),nonce:'f'.repeat(32)}})).status).toBe(409);});
test('withdrawal is specific to the chosen site and does not erase private saved material',async()=>{const row=f.sql.query("SELECT id,post FROM community_post WHERE title='Work publication witness'").get(),form={site:'profile-avery-work',id:String(row.id),expected:row.post};expect((await request('/profile/withdraw',{form:{...form,site:'profile-avery-life'}})).status).toBe(404);expect((await request('/profile/withdraw',{user:other,form})).status).toBe(404);expect((await request('/profile/withdraw',{form})).status).toBe(303);expect((await request('/profile/withdraw',{form})).status).toBe(303);expect((await publicPage('work')).body).not.toContain('Work publication witness');expect((await request('/profile?tab=saved')).body).toContain('Owner private bookmark');expect((await publicPage('life')).body).toContain('Life reading recommendation');});
test('foreign or missing origins, duplicate fields and unbound site claims cannot write',async()=>{const form=hidden((await request('/profile?tab=compose')).body);for(const origin of ['https://attacker.test','null',''])expect((await request('/profile/publish',{form:{...form,title:'bad',text:'no'},headers:{Origin:origin}})).status).toBe(403);const values=new URLSearchParams({...form,title:'bad',text:'no'});values.append('site','profile-avery-life');expect((await request('/profile/publish',{form:values.toString()})).status).toBe(403);const count=f.sql.query('SELECT count(*) AS n FROM community_post').get().n;matrix.query("UPDATE matrix_presences SET active=0 WHERE presence_id='presence:avery:shop'").run();try{expect((await request('/profile?site=profile-avery-shop')).status).toBe(404);expect((await request('/profile/publish',{form:{...form,site:'profile-avery-shop',title:'bad',text:'no'}})).status).toBe(404);expect(f.sql.query('SELECT count(*) AS n FROM community_post').get().n).toBe(count);}finally{matrix.query("UPDATE matrix_presences SET active=1 WHERE presence_id='presence:avery:shop'").run();}});
test('an unrelated moderator grant never exposes another person\u2019s private profile controls',async()=>{f.grant(other.id,'unrelated-community','admin');const r=await publicPage('life','/',other);expect(r.body).not.toContain('Manage this post');expect(r.body).not.toContain('Write a post');expect(r.body).not.toContain('data-private-saved');expect(r.body).toContain('Life reading recommendation');});
test('real Chrome follows profile sections, edits one site and preserves responsive boundaries',async()=>{const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[];p.on('pageerror',e=>errors.push(e.message));p.setDefaultTimeout(15000);const[name,...value]=owner.cookie.split('=');await p.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});try{await p.setViewport({width:1440,height:1000});await p.goto(f.base+'/profile?site=profile-avery-life',{waitUntil:'networkidle0'});expect(await p.$eval('[data-profile-page]',n=>n.dataset.profileSite)).toBe('profile-avery-life');await p.click('.ph-tabs a[href*="tab=saved"]');await p.waitForSelector('.ph-search input');await p.type('.ph-search input','Owner private');await Promise.all([p.waitForNavigation({waitUntil:'networkidle0'}),p.click('.ph-search button')]);expect(await p.$$eval('[data-private-saved]',rows=>rows.length)).toBe(1);await Promise.all([p.waitForNavigation({waitUntil:'networkidle0'}),p.click('.ph-sites a[href*="profile-avery-work"]')]);await Promise.all([p.waitForNavigation({waitUntil:'networkidle0'}),p.click('.ph-actions a[href*="tab=edit"]')]);await p.$eval('textarea[name=text]',n=>n.value='Browser-edited WORK introduction.');const [submitted]=await Promise.all([p.waitForNavigation({waitUntil:'networkidle0'}),p.click('.ph-form button')]);if(!await p.$('[data-profile-page]'))throw Error('Profile form response '+submitted?.status()+': '+await p.$eval('body',n=>n.innerText.slice(0,700)));expect((await publicPage('work')).body.includes('Browser-edited WORK introduction.')).toBe(true);expect((await publicPage('life')).body).not.toContain('Browser-edited WORK introduction.');for(const width of [1440,390]){await p.setViewport({width,height:1000});expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);}expect(errors).toEqual([]);const out=process.env.BONFYRE_PROFILE_SCREENSHOTS;if(out){mkdirSync(out,{recursive:true});await p.screenshot({path:join(out,'profile-mobile.png'),fullPage:true});await p.setViewport({width:1440,height:1000});await p.goto(f.base+'/profile?site=profile-avery-life',{waitUntil:'networkidle0'});await p.screenshot({path:join(out,'profile-desktop.png'),fullPage:true});}}finally{await ctx.close();}});

test('cross-territory public preview uses the existing session handoff with the exact profile return',async()=>{const r=await request('/profile?site=profile-avery-life');const href=decode(r.body.match(/data-profile-preview href="([^"]+)"/)[1]),u=new URL(href);expect(u.origin).toBe('https://aurekai.work');expect(u.pathname).toBe('/session/switch');expect(u.searchParams.get('to')).toBe('life');expect(u.searchParams.get('next')).toBe('https://avery.aurekai.life/');expect([...u.searchParams.keys()].sort()).toEqual(['next','to']);});
test('public profile has no automatic installed-tool shelf for a signed-in visitor',async()=>{const r=await publicPage('life','/',other);expect(r.body).not.toContain('ak-home-continuation-grid');expect(r.body).not.toContain('Pick up what you made yours');expect(r.body).not.toContain('data-private-saved');expect(r.body.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g,'')).not.toContain('Create and publish');});

test('withdrawing a registered profile cannot fall through to a legacy public renderer',async()=>{matrix.query("UPDATE matrix_presences SET active=0 WHERE presence_id='presence:avery:live'").run();try{const r=await publicPage('live');expect(r.status).toBe(404);expect(r.body).not.toContain('live-only-public-post');}finally{matrix.query("UPDATE matrix_presences SET active=1 WHERE presence_id='presence:avery:live'").run();}});
test('custom presentation selection is not silently replaced by the shared member template',async()=>{f.sql.query("UPDATE site_presentation SET profile='custom-independent-fixture' WHERE site_id='profile-avery-shop'").run();try{const r=await publicPage('shop');expect(r.body).not.toContain('data-profile-template="member-v1"');expect((await request('/profile?site=profile-avery-shop')).status).toBe(404);}finally{f.sql.query("UPDATE site_presentation SET profile='member-profile-v1' WHERE site_id='profile-avery-shop'").run();}});

runTest('an exact CC0 artwork becomes one site header and a reviewed gallery piece without copying its source',async()=>{
 const initialHeading=(await publicPage('work')).body.match(/<h1>[^<]+<\/h1>/)?.[0];expect(initialHeading).toBeTruthy();
 const {createHash}=await import('node:crypto'),{encode}=await import('../../../integrations/browser/workspace-msgpack.mjs'),{writeFileSync,readFileSync}=await import('node:fs');
 const schemas=join(f.home,'schemas');for(const type of ['internet_source','site_fragment'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const source='cleveland-artworks',retained=JSON.parse(readFileSync(join(import.meta.dir,'fixtures/cleveland-cc0-artworks.json'),'utf8')),path=join(f.home,'foreign/bodies',source+'.body');
 const keyOf=r=>createHash('sha256').update(source+'\n#'+r.id).digest('hex').slice(0,24),stag=retained.data.find(r=>r.id===92937),key='a47812567fb8271d725264a7',ref=source+'/'+key;expect(keyOf(stag)).toBe(key);
 const write=(rights='CC0')=>writeFileSync(path,JSON.stringify({...retained,data:retained.data.map(r=>r.id===92937?{...r,share_license_status:rights}:r)}));write();writeFileSync(join(f.home,'foreign/receipts',source+'.json'),JSON.stringify({body_path:path}));
 f.add('internet_source',{family:'museums',source,label:'Cleveland Museum of Art',says:'open-access artworks at the Cleveland Museum of Art',url:'https://openaccess-api.clevelandart.org/api/artworks/?limit=60&has_image=1&cc0=1',twin:source,state:'observed',extract:'data',title_field:'title',link_field:'url',image_field:'images.web.url',maker_field:'creators.0.description',summary_field:'tombstone',rights_field:'share_license_status',key_field:'id',body_regime:'REMOTE',status:'published',tier:'COLD',kind:'image'});
 const review=(label,r,x,y)=>createHash('sha256').update(encode([label,source,keyOf(r),r.title,r.creators[0]?.description||'',r.url,r.images.web.url,'CC0',x,y])).digest('hex');
 const forms=(body,action)=>[...body.matchAll(/<form[^>]*action="([^"]+)"[^>]*>([\s\S]*?)<\/form>/g)].filter(m=>m[1]===action).map(m=>hidden(m[2]));
 const rows=()=>f.sql.query("SELECT kind,status,audience,source_ref,position,evidence_ref FROM site_fragment WHERE namespace='profile-avery-work' AND source_ref LIKE 'cleveland-artworks/%' ORDER BY id").all();
 // The item page offers the profile action only to a signed-in reader.
 const item=await request('/o/'+ref,{site:'work'});expect(item.status).toBe(200);expect(item.body).toContain('href="/profile?tab=artwork&amp;art=cleveland-artworks%2F'+key+'"');expect((await request('/o/'+ref,{user:null})).body).not.toContain('Use on my profile');
 const reviewPage=await request('/profile?site=profile-avery-work&tab=artwork&art='+encodeURIComponent(ref));expect(reviewPage.status).toBe(200);expect(reviewPage.body).toContain('data-artwork-review');expect(reviewPage.body).toContain("Stag at Sharkey");expect(reviewPage.body).toContain('Public domain (CC0)');expect(rows()).toEqual([]);
 for(const bad of ['/?tab=artwork','/?art='+ref,'/?tab=gallery&fx=1'])expect((await publicPage('work',bad)).status).toBe(400);
 expect((await request('/profile?site=profile-avery-work&tab=artwork&art=fixture-art/'+'0'.repeat(24))).body).toContain('data-artwork-refused');
 // Header: the exact item, chosen focus, this site only; the avatar keeps its initial.
 const [cover]=forms(reviewPage.body,'/profile/cover/set');expect(cover.art).toBe(ref);
 expect((await request('/profile/cover/set',{user:other,form:{...cover,fx:'30',fy:'20'}})).status).toBe(404);
 expect((await request('/profile/cover/set',{form:{...cover,fx:'30',fy:'20'},headers:{Origin:'https://attacker.test'}})).status).toBe(403);
 expect((await request('/profile/cover/set',{form:{...cover,fx:'101',fy:'20'}})).status).toBe(400);
 expect((await request('/profile/cover/set',{form:{...cover,fx:'30',fy:'20'}})).status).toBe(303);
 expect((await request('/profile/cover/set',{form:{...cover,fx:'30',fy:'20'}})).status).toBe(409);
 let pub=(await publicPage('work')).body;expect(pub).toContain('data-profile-cover');expect(pub).toContain(stag.images.web.url);expect(pub).toContain('object-position:30% 20%');expect(pub).toContain('href="/o/'+ref+'"');expect(pub).toContain(initialHeading);
 expect((await publicPage('life')).body).not.toContain(stag.images.web.url);
 expect(rows()).toEqual([{kind:'profile-cover',status:'published',audience:'public',source_ref:ref,position:30*101+20,evidence_ref:review('bonfyre.profile-cover-review.v1',stag,30,20)}]);
 // Recrop through the owner's cover options, with the stored focus prefilled.
 const self=(await request('/profile?site=profile-avery-work')).body,adjust=decode(self.match(/data-cover-adjust href="([^"]+)"/)[1]);expect(adjust).toContain('fx=30&fy=20');
 const again=(await request(adjust)).body;expect(again).toContain('Save the focus point');expect(again).toContain('value="30" data-art-focus="x"');
 expect((await request('/profile/cover/set',{form:{...forms(again,'/profile/cover/set')[0],fx:'70',fy:'20'}})).status).toBe(303);expect((await publicPage('work')).body).toContain('object-position:70% 20%');
 // Gallery: adding saves a private draft; nothing is public until reviewed.
 const pieces=[stag,...retained.data.filter(r=>r.id!==92937).slice(0,5)];
 for(const r of pieces){const page=(await request('/profile?site=profile-avery-work&tab=artwork&art='+encodeURIComponent(source+'/'+keyOf(r)))).body;expect((await request('/profile/gallery/add',{form:forms(page,'/profile/gallery/add')[0]})).status).toBe(303);}
 expect((await request('/profile/gallery/add',{form:{site:'profile-avery-work',art:ref}})).status).toBe(303);
 expect(rows().filter(r=>r.kind==='profile-gallery').map(r=>[r.status,r.audience])).toEqual(pieces.map(()=>['draft','self']));
 pub=(await publicPage('work')).body;expect(pub).not.toContain('data-gallery-item');for(const r of pieces.slice(1))expect(pub).not.toContain(r.images.web.url);
 expect((await publicPage('work','/?tab=gallery')).body).toContain('data-profile-gallery-empty');
 let mine=(await request('/profile?site=profile-avery-work&tab=gallery')).body;expect((mine.match(/data-gallery-state="private"/g)||[]).length).toBe(pieces.length);
 for(let i=0;i<5;i++){const form=forms(mine,'/profile/gallery/publish')[0];expect((await request('/profile/gallery/publish',{user:other,form})).status).toBe(404);expect((await request('/profile/gallery/publish',{form})).status).toBe(303);expect((await request('/profile/gallery/publish',{form})).status).toBe(409);mine=(await request('/profile?site=profile-avery-work&tab=gallery')).body;}
 pub=(await publicPage('work','/?tab=gallery')).body;expect((pub.match(/data-gallery-item/g)||[]).length).toBe(5);expect(pub).toContain('data-span="wide"');expect(pub).not.toContain(pieces[5].images.web.url);expect(pub).not.toContain('Show in public gallery');
 const published=f.sql.query("SELECT source_ref,evidence_ref FROM site_fragment WHERE namespace='profile-avery-work' AND kind='profile-gallery' AND source_ref=?").get(ref);expect(published.evidence_ref).toBe(review('bonfyre.profile-gallery-review.v1',stag,50,50));
 expect((await publicPage('life','/?tab=gallery')).body).not.toContain('data-gallery-item');
 const hide=forms(mine,'/profile/gallery/hide')[1];expect((await request('/profile/gallery/hide',{form:hide})).status).toBe(303);expect(((await publicPage('work','/?tab=gallery')).body.match(/data-gallery-item/g)||[]).length).toBe(4);
 mine=(await request('/profile?site=profile-avery-work&tab=gallery')).body;const last=forms(mine,'/profile/gallery/remove').at(-1);expect((await request('/profile/gallery/remove',{form:last})).status).toBe(303);
 expect(((await request('/profile?site=profile-avery-work&tab=gallery')).body.match(/data-gallery-item/g)||[]).length).toBe(pieces.length-1);
 // One source, many placements: header and gallery both point at the exact item; no copy exists.
 pub=(await publicPage('work')).body;expect((pub.match(new RegExp('href="/o/'+ref+'"','g'))||[]).length).toBeGreaterThanOrEqual(3);expect(rows().filter(r=>r.source_ref===ref).map(r=>r.kind).sort()).toEqual(['profile-cover','profile-gallery']);
 // A rights change hides both placements publicly and flags the owner's piece.
 write('Copyrighted');pub=(await publicPage('work')).body;expect(pub).not.toContain(stag.images.web.url);expect(pub).not.toContain('data-profile-cover');
 expect((await request('/profile?site=profile-avery-work&tab=gallery')).body).toContain('data-gallery-stale');
 expect((await request('/profile/gallery/add',{form:{site:'profile-avery-life',art:ref}})).status).toBe(409);write();
 // Real Chrome: guest gallery and owner review at desktop and phone widths.
 const nativeStyle=await f.request('/assets/aurekai-native.css',{host:sites[0].host});expect(nativeStyle.status).toBe(200);
 const out=process.env.BONFYRE_ART_SCREENSHOTS,ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[];p.on('pageerror',e=>errors.push(e.message));
 const [name,...value]=owner.cookie.split('=');await p.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 try{for(const width of [1440,390]){await p.setViewport({width,height:1000});
  await p.setContent((await publicPage('work')).body.replace('<head>','<head><base href="'+f.base+'/"><style>'+nativeStyle.body+'</style>'),{waitUntil:'load',timeout:90000});
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  expect(await p.$eval('[data-profile-cover] img',n=>n.complete&&n.naturalWidth>0)).toBe(true);
  for(const img of await p.$$('[data-gallery-item] img')){await img.evaluate(n=>n.scrollIntoView({block:'center'}));await p.waitForFunction(n=>n.complete&&n.naturalWidth>0,{timeout:15000},img);}const tiles=await p.$$eval('[data-gallery-item]',n=>n.map(x=>{const b=x.getBoundingClientRect();return {w:Math.round(b.width),img:x.querySelector('img').naturalWidth>0};}));
  expect(tiles.length).toBe(4);expect(tiles.every(t=>t.img)).toBe(true);expect(tiles[0].w).toBeGreaterThan(tiles[1].w);
  if(out){mkdirSync(out,{recursive:true});await p.screenshot({path:join(out,'art-public-'+width+'.png'),fullPage:true});}
  await p.goto(f.base+'/profile?site=profile-avery-work&tab=gallery',{waitUntil:'load',timeout:90000});await p.addStyleTag({content:nativeStyle.body});expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  if(out)await p.screenshot({path:join(out,'art-owner-gallery-'+width+'.png'),fullPage:true});
  await p.goto(f.base+'/profile?site=profile-avery-work&tab=artwork&art='+encodeURIComponent(ref),{waitUntil:'load',timeout:90000});await p.addStyleTag({content:nativeStyle.body});
  await p.focus('[data-art-focus=x]');for(let i=0;i<5;i++)await p.keyboard.press('ArrowLeft');expect(await p.$eval('[data-art-preview]',n=>n.style.objectPosition)).toBe('65% 20%');
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  if(out)await p.screenshot({path:join(out,'art-owner-review-'+width+'.png'),fullPage:true});}
 await p.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);await p.setJavaScriptEnabled(false);await p.goto(f.base+'/profile?site=profile-avery-work&tab=gallery',{waitUntil:'load',timeout:90000});expect(await p.$$eval('[data-gallery-item] img',n=>n.length)).toBe(pieces.length-1);
 expect(errors.filter(e=>!e.includes('about:blank'))).toEqual([]);}finally{await ctx.close();}
},180000);
test('site-owned draft pages have a visible private review path, never a public post',async()=>{
 const route='/blog/a-story-under-review/';
 f.add('site_page',{route,title:'A Story Under Review',heading:'A Story Under Review',lede:'Private until released',status:'draft'},sites[0].id);
  f.add('site_page',{route:'/about-update/',title:'Uncomposed site page',heading:'Uncomposed site page',status:'draft'},sites[0].id);
  f.add('site_fragment',{coordinate:'under-review-body',page:route,slot:'showcase',audience:'public',kind:'profile-story',position:0,heading:'A Story Under Review',body:'Composed original editorial work.',semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},projection_contract:{html:true},status:'draft'},sites[0].id);
 const mine=await request('/profile?site=profile-avery-work&tab=posts');
 expect(mine.status).toBe(200);
 expect(mine.body).toContain('Pages awaiting your review');
 expect(mine.body).toContain('data-review-queue');expect(mine.body).toContain('composed sections');expect(mine.body).toMatch(/\d+ pages? awaiting composition and review/);
 expect(mine.body).toContain('A Story Under Review');
 const reviewStart=mine.body.indexOf('data-review-queue');
 const privateDesk=mine.body.slice(reviewStart,mine.body.indexOf('</section>',reviewStart));
 expect(privateDesk).toContain("A Story Under Review");
 expect(privateDesk).toContain("Uncomposed site page");
 expect(mine.body).toContain('data-publication-state="draft"');
 expect(mine.body).toContain('/cms/compose/advanced?site=profile-avery-work&amp;route=');
 const publicWork=await publicPage('work','/?tab=posts');
 const otherSite=await request('/profile?site=profile-avery-life&tab=posts');
 const otherUser=await request('/profile?tab=posts',{user:other});
 for(const page of [publicWork,otherSite,otherUser]){expect(page.body).not.toContain('A Story Under Review');expect(page.body).not.toContain('data-publication-state="draft"');}
});
test('profile primary actions open the unified article editor without losing site scope',async()=>{
 const own=await request('/profile?site=profile-avery-work&tab=posts');
 expect(own.body).toContain('/cms/compose/advanced?site=profile-avery-work&amp;new=article');
 expect(own.body).toContain('Write an article');
});
test('site owner releases an exact reviewed journal composition through the authenticated CMS publisher',async()=>{
 const site=sites[0].id,route='/blog/released-in-browser/';
 f.add('site_page',{route,title:'Released browser journey',heading:'Released browser journey',lede:'Full editorial article.',
  editorial_kind:'article',machine_view:'authored',author_credit:'Avery',author_subject_ref:'person:avery',status:'draft'},site);
 f.add('site_fragment',{coordinate:'release-browser-story',page:route,slot:'showcase',audience:'public',kind:'profile-story',
  position:0,heading:'Opening',body:'This article is available after review.',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},
  projection_contract:{html:true},status:'draft'},site);
 const qs='?site='+encodeURIComponent(site)+'&route='+encodeURIComponent(route);
 const snapshot=await request('/cms/compose/page'+qs);
 expect(snapshot.status).toBe(200);
 const saved=JSON.parse(snapshot.body);
 expect(saved.snapshot.page.status).toBe('draft');
 const body={expected_id:saved.id,expected_digest:saved.digest,rights_confirmed:true};
 const publish=(payload=body,user=owner,origin='http://aurekai.work',siteName=site)=>
  request('/cms/compose/publish?site='+encodeURIComponent(siteName)+'&route='+encodeURIComponent(route),{
   user,form:JSON.stringify(payload),headers:{Origin:origin,'Content-Type':'application/json'}});
 expect((await publish({...body,rights_confirmed:false})).status).toBe(400);
 expect((await publish(body,other)).status).toBe(403);
 expect((await publish(body,owner,'https://other.test')).status).toBe(403);
 expect((await publish(body,owner,'http://aurekai.work',sites[1].id)).status).toBe(409);
 expect((await publish({...body,expected_digest:'0'.repeat(64)})).status).toBe(409);
 expect((await publicPage('work',route)).status).toBe(404);
 const accepted=await publish();
 expect(accepted.status).toBe(200);
 const committed=JSON.parse(accepted.body);
 expect(committed).toMatchObject({committed:true,id:saved.id,released:1});
 const published=await publicPage('work',route);
 expect(published.status).toBe(200);
 expect(published.body).toContain('This article is available after review.');
 expect((await publicPage('life',route)).status).toBe(404);
 expect((await publish()).status).toBe(409);
 const next=JSON.parse((await request('/cms/compose/page'+qs)).body);
 expect(next.snapshot.page.status).toBe('published');
 expect(next.snapshot.fragments[0].status).toBe('published');
});
test('Chrome publishes a saved article from the same visual editor and returns to its profile',async()=>{
 const site=sites[0].id,route='/blog/editor-to-site-release/';
 f.add('site_page',{route,title:'Editor to public website',heading:'Editor to public website',
  lede:'A complete original article',editorial_kind:'article',media_subject_ref:'article:editor-to-site-release',machine_view:'authored',
  media_subject_ref:'article:editor-to-site-release',author_credit:'Avery',author_subject_ref:'person:avery',status:'draft'},site);
 f.add('site_fragment',{coordinate:'editor-to-site-release-prose',page:route,slot:'showcase',
  audience:'public',kind:'profile-story',position:0,heading:'The finished story',
  body:'Saved through the existing CMS and released through the same visual workspace.',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},
  projection_contract:{html:true},status:'draft'},site);
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage();
 try{
  const [name,...secret]=owner.cookie.split('=');
  await page.setCookie({name,value:secret.join('='),url:f.base.replace('127.0.0.1','aurekai.life'),httpOnly:true,sameSite:'Lax'});
  const url=f.base.replace('127.0.0.1','aurekai.life')+'/cms/compose/advanced?site='+encodeURIComponent(site)+'&route='+encodeURIComponent(route);
  const res=await page.goto(url,{waitUntil:'domcontentloaded',timeout:15000});
  expect(res.status()).toBe(200);
  await page.waitForFunction(()=>document.querySelector(".authoring-modes button")||document.querySelector(".authoring-error"),{timeout:15000});
  const mount=await page.evaluate(()=>({error:document.querySelector(".authoring-error")?.textContent||"",modes:document.querySelectorAll(".authoring-modes button").length,publication:!!document.querySelector("[data-article-publication] .authoring-publication-open")}));
  if(mount.error||mount.modes!==3||!mount.publication)throw Error("CMS publication workspace not mounted: "+JSON.stringify(mount));
  expect(await page.$eval('[data-article-publication] .authoring-publication-open',x=>x.disabled)).toBe(false);
  await page.click('[data-article-publication] .authoring-publication-open');
  await page.waitForSelector('dialog.authoring-publication-review[open]');
  expect(await page.$eval('.authoring-publication-summary',n=>n.textContent)).toContain('Editor to public website');
  const returnToProfile=page.waitForNavigation({waitUntil:"domcontentloaded",timeout:15000});
  await page.click("dialog.authoring-publication-review .authoring-publication-confirm");
  await returnToProfile;
  expect(new URL(page.url()).pathname).toBe("/profile");
  expect(new URL(page.url()).searchParams.get("tab")).toBe("posts");
  const profileDesk=await page.$eval("[data-profile-page]",node=>node.textContent);
  if(!profileDesk.includes("Editor to public website"))throw Error("Released article missing from profile: "+profileDesk.slice(0,450));
  const publicStory=await publicPage('work',route);
  expect(publicStory.status).toBe(200);
  expect(publicStory.body).toContain('Saved through the existing CMS');
  expect((await publicPage('life',route)).status).toBe(404);
 }finally{await ctx.close();}
},90000);
test('native authored articles share the public profile journal and exact site route',async()=>{
 const route='/blog/profile-journal-acceptance/';
 f.add('site_page',{route,title:'A published journal article',heading:'A published journal article',
  lede:'A long-form original from the account publication.',eyebrow:'Research',editorial_kind:'article',
  media_subject_ref:'article:profile-journal-acceptance',editorial_published_on:'2026-11-07',status:'published'},sites[0].id);
 f.add('site_fragment',{coordinate:'profile-journal-acceptance',page:route,slot:'showcase',audience:'public',
  kind:'profile-story',position:0,heading:'A published journal article',body:'Native journal body proof, not a duplicated community post.',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},projection_contract:{html:true},status:'published'},sites[0].id);
 const work=await publicPage('work','/?tab=posts'),article=await publicPage('work',route);
 expect(article.status).toBe(200);expect(article.body).toContain('Native journal body proof');
 expect(article.body).toContain('class="ak-bar"');
 expect(article.body).toContain('data-ak-track data-ak-default data-narrate=".ak-zen-body"');
 expect(article.body).toContain('data-ak-player');
 expect(work.status).toBe(200);expect(work.body).toContain('data-profile-journal');
 expect(work.body).toContain('A published journal article');expect(work.body).toContain(route);
 f.add('site_fragment',{coordinate:'journal-evidence-section',page:route,slot:'showcase',audience:'public',
  kind:'profile-story',position:1,heading:'The evidence',body:'A second native section with its own reading anchor.',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},projection_contract:{html:true},status:'published'},sites[0].id);
 f.add('site_fragment',{coordinate:'journal-evidence-layout',page:route,slot:'showcase',audience:'public',
  kind:'ui:columns',position:2,heading:'Evidence and interpretation',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{columns:2,gap:20},responsive:{desktop:{layout:'columns',columns:2},phone:{layout:'stack'}}}},projection_contract:{html:true},status:'published'},sites[0].id);
 f.add('site_fragment',{coordinate:'journal-evidence-card',page:route,slot:'showcase',audience:'public',
  kind:'ui:card',position:0,heading:'Owned continuation',
  semantic_tags:{authoring_v1:{version:1,parent:'journal-evidence-layout',payload:{title:'Nested native article card',body:'A typed continuation is part of its publication, not a copy.',image:{src:'',alt:''},actions:[]},responsive:{}}},projection_contract:{html:true},status:'published'},sites[0].id);
 const enrichedArticle=await publicPage('work',route);
 expect(enrichedArticle.status).toBe(200);expect(enrichedArticle.body).toContain('Nested native article card');
 const index=await publicPage('work','/blog/'),foreignIndex=await publicPage('life','/blog/');
 expect(index.status).toBe(200);expect(index.body).toContain('class="fd ak-journal-list"');
 expect(index.body).toContain('class="ak-bar"');expect(index.body).toContain('data-ak-player');
 expect(index.body).toContain('class="ak-journal-rows"');expect(index.body).toContain('name="q"');
 expect(index.body).toContain('href="/blog/?subject=Research"');
 expect(index.body).toContain('A published journal article');
 expect(index.body).not.toContain('data-public-post');
 expect(foreignIndex.status).toBe(200);expect(foreignIndex.body).not.toContain('A published journal article');
 const subjectIndex=await publicPage('work','/blog/?subject=Research');
 expect(subjectIndex.status).toBe(200);expect(subjectIndex.body).toContain('A published journal article');
 expect(subjectIndex.body).toContain('aria-current="page">Research</a>');
 const searched=await publicPage('work','/blog/?q=published%20journal');
 expect(searched.status).toBe(200);expect(searched.body).toContain('A published journal article');
 const noResults=await publicPage('work','/blog/?q=unfindable%20phrase');
 expect(noResults.status).toBe(200);expect(noResults.body).toContain('No articles match');
 expect(noResults.body).not.toContain('A published journal article');
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage();
 try{
  await page.setContent(index.body,{waitUntil:'domcontentloaded'});
  expect(await page.$eval('.ak-journal-list',root=>!!(root.querySelector('.fd-lead')&&root.querySelector('.fd-find input[name="q"]')))).toBe(true);
  await page.setViewport({width:1440,height:920});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  await page.setViewport({width:390,height:820});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  await page.evaluate(()=>{
   window.__spoken=[];window.__utterance=null;
   Object.defineProperty(window,'speechSynthesis',{configurable:true,value:{cancel(){},speak(u){window.__spoken.push(u.text);window.__utterance=u;}}});
   window.SpeechSynthesisUtterance=function(text){this.text=text;};
   window.Audio=function(){window.__audio=this;this.pause=()=>{};this.play=()=>window.__allowAudio?Promise.resolve():Promise.reject(new Error('unavailable'));};
  });
  await page.setContent(enrichedArticle.body.replace(' data-ak-default',''),{waitUntil:'domcontentloaded'});
  await page.evaluate(()=>{window.__playerEvents=0;document.addEventListener('bonfyre:public-audio-play',()=>window.__playerEvents++);});
  await page.$eval('.ak-zen-body',n=>{n.innerHTML='<blockquote><p>Quoted once.</p></blockquote><ul><li>First item. <ul><li>Nested item.</li></ul></li></ul><p hidden>Hidden text.</p><p>Final paragraph.<span aria-hidden="true">Invisible text.</span></p>';});
  await page.click('[data-ak-track]');
  await page.evaluate(()=>{for(let i=0;i<3;i++)window.__utterance.onend();});
  expect(await page.evaluate(()=>window.__spoken)).toEqual(['Quoted once.','First item. Nested item.','Final paragraph.']);
  expect(await page.$eval('[data-ak-player]',n=>n.hidden)).toBe(false);
  await page.click('[data-ak-track]');
  expect(await page.evaluate(()=>window.__playerEvents)).toBe(2);
  await page.evaluate(()=>document.dispatchEvent(new CustomEvent('bonfyre:public-audio-play',{detail:{}})));
  expect(await page.$eval('.ak-play',n=>n.getAttribute('aria-label'))).toBe('Play');
  await page.click('[data-ak-track]');
  await page.evaluate(()=>window.__utterance.onerror());
  expect(await page.$eval('[data-ak-status]',n=>n.textContent)).toBe('Narration could not play. Try again.');
  expect(await page.$eval('.ak-play',n=>n.getAttribute('aria-label'))).toBe('Play');
  await page.$eval('main',n=>{n.insertAdjacentHTML('beforeend','<button type="button" data-ak-track data-src="/missing.mp3">Play recording</button>');});
  await page.click('[data-src="/missing.mp3"]');
  expect(await page.$eval('[data-ak-status]',n=>n.textContent)).toBe('Recording could not play. Try again.');
  expect(await page.$eval('.ak-play',n=>n.getAttribute('aria-label'))).toBe('Play');
  await page.$eval('main',n=>{n.insertAdjacentHTML('beforeend','<div hidden id="queue-text-one"><h2>First title</h2><p>First story.</p></div><div hidden id="queue-text-two"><h2>Second title</h2><p>Second story.</p></div><button type="button" data-ak-track data-ak-queue="listen" data-narrate="#queue-text-one" data-title="First">First</button><button type="button" data-ak-track data-ak-queue="listen" data-narrate="#queue-text-two" data-title="Second">Second</button>');});
  await page.evaluate(()=>{window.__spoken=[];});
  await page.click('[data-title="First"]');
  expect(await page.$eval('.ak-prev',n=>n.disabled)).toBe(true);
  expect(await page.$eval('.ak-next',n=>n.disabled)).toBe(false);
  await page.evaluate(()=>{window.__utterance.onend();window.__utterance.onend();});
  expect(await page.evaluate(()=>window.__spoken)).toEqual(['First title','First story.','Second title']);
  expect(await page.$eval('[data-ak-title]',n=>n.textContent)).toBe('Second');
  expect(await page.$eval('.ak-next',n=>n.disabled)).toBe(true);
  await page.evaluate(()=>{window.__utterance.onend();window.__utterance.onend();});
  expect(await page.$eval('[data-ak-status]',n=>n.textContent)).toBe('End of queue.');
  await page.click('.ak-prev');
  expect(await page.$eval('[data-ak-title]',n=>n.textContent)).toBe('First');
  await page.$eval('main',n=>{n.insertAdjacentHTML('beforeend','<button type="button" data-ak-track data-ak-queue="recordings" data-src="/one.mp3" data-title="One">One</button><button type="button" data-ak-track data-ak-queue="recordings" data-src="/two.mp3" data-title="Two">Two</button>');});
  await page.evaluate(()=>{window.__allowAudio=true;});
  await page.click('[data-title="One"]');
  await page.evaluate(()=>window.__audio.onended());
  expect(await page.$eval('[data-ak-title]',n=>n.textContent)).toBe('Two');
  expect(await page.evaluate(()=>window.__audio.src)).toBe('/two.mp3');
 }finally{await ctx.close();}
 expect(enrichedArticle.body).toContain('id="journal-evidence-section"');
 expect(enrichedArticle.body).toContain('A second native section with its own reading anchor.');
 const other=await publicPage('life','/?tab=posts');
 expect(other.body).toContain('A published journal article');
 expect(other.body).toContain('https://'+sites[0].host+route);
});


test('public journal keeps long source slugs within the native route bound',async()=>{
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(f.home,'schemas/site_fragment.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 const slug='an-electric-vehicle-can-be-a-mobile-battery-but-availability-is-the-product';
 const route='/blog/'+slug+'/';
 f.add('site_page',{route,title:'A long source slug',heading:'A long source slug',lede:'A retained source.',editorial_kind:'article',media_subject_ref:'article:'+slug,status:'published'},sites[0].id);
 f.add('site_fragment',{coordinate:'long-slug-story',page:route,slot:'showcase',audience:'public',kind:'profile-story',position:0,heading:'A long source slug',body:'The complete long-slug article is composed.',semantic_tags:{authoring_v1:{version:1,payload:{}}},projection_contract:{html:true},status:'published'},sites[0].id);
 const article=await publicPage('work',route);
 expect(article.status).toBe(200);expect(article.body).toContain('The complete long-slug article is composed.');
 expect(article.body).toContain('https://avery.aurekai.work'+route);
 const index=await publicPage('work','/blog/');
 expect(index.status).toBe(200);expect(index.body).toContain('A long source slug');expect(index.body).toContain(route);
 const profile=await publicPage('work','/?tab=posts');
 expect(profile.body).toContain('A long source slug');expect(profile.body).toContain(route);
 expect((await publicPage('life',route)).status).toBe(404);
 expect((await publicPage('work','/blog/'+slug.slice(0,71)+'/')).status).toBe(404);
 expect((await publicPage('work','/blog/'+'a'.repeat(97)+'/')).status).toBe(404);
});


test('article narration revision binds the signed-in profile and saved content',async()=>{
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(f.home,'schemas/site_fragment.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 const route='/blog/narration-owner/',site=sites[0].id;
 f.add('site_page',{route,title:'A source article',heading:'A source article',lede:'A saved source.',editorial_kind:'article',media_subject_ref:'article:narration-owner',status:'draft'},site);
 f.add('site_fragment',{coordinate:'narration-owner-story',page:route,slot:'showcase',audience:'public',kind:'profile-story',position:0,heading:'Opening',body:'A saved passage for the narration owner.',semantic_tags:{authoring_v1:{version:1,payload:{}}},projection_contract:{html:true},status:'draft'},site);
 const path='/cms/compose/narration-revision?site='+encodeURIComponent(site)+'&route='+encodeURIComponent(route);
 const first=await request(path),second=await request(path);expect(first.status).toBe(200);expect(second.status).toBe(200);
 const revision=JSON.parse(first.body);expect(revision.schema).toBe('bonfyre.article-revision.v2');
 expect(revision.namespace).toBe(site);expect(revision.route).toBe(route);expect(revision.subject).toBe('article:narration-owner');expect(revision.status).toBe('draft');
 expect(revision.sha256).toMatch(/^[a-f0-9]{64}$/);expect(JSON.parse(second.body).sha256).toBe(revision.sha256);
 expect((await request(path,{user:null})).status).toBe(403);
 expect((await request(path,{user:other})).status).toBe(403);
 expect((await request('/cms/compose/narration-revision?route='+encodeURIComponent(route))).status).toBe(403);
 expect((await request('/cms/compose/page?site='+encodeURIComponent(site)+'&route='+encodeURIComponent(route))).status).toBe(200);
 f.sql.query("UPDATE site_fragment SET body=? WHERE namespace=? AND coordinate=?").run('Changed retained passage.',site,'narration-owner-story');
 const changed=await request(path);expect(changed.status).toBe(200);expect(JSON.parse(changed.body).sha256).not.toBe(revision.sha256);
});

test('private journal drafts use the native composed cards without publishing member material',async()=>{
 const site=sites[0].id,route='/blog/private-journal-draft-view/';
 f.add('site_page',{route,title:'Private draft library proof',heading:'Private draft library proof',
  editorial_kind:'article',media_subject_ref:'article:private-journal-draft-view',status:'draft'},site);
 f.add('site_fragment',{coordinate:'private-journal-draft-view-story',page:route,slot:'showcase',
  audience:'self',kind:'profile-story',position:0,heading:'A private draft section',
  body:'PRIVATE-DRAFT-CONTENT-MUST-NOT-LEAK',
  semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},
  projection_contract:{html:true},status:'draft'},site);
 const owner=await request('/profile?site='+encodeURIComponent(site)+'&tab=posts');
 expect(owner.status).toBe(200);
 const grid=owner.body.match(/<section class="ph-journal" data-profile-journal>[\s\S]*?<\/section>/)?.[0]||'';
 expect(grid).toContain('Your journal');
 expect(grid).toContain('Private draft library proof');
 expect(grid).toContain('data-publication-state="draft"');
 expect(grid).toContain('/cms/compose/advanced?site=');
 expect(grid).toContain('data-journal-batch');
 expect(grid).toContain('data-journal-select-all');
 expect(grid).toContain('data-journal-select');
 expect(grid).toContain('data-journal-rights');
 expect(grid).toContain('data-journal-publish');
 expect(grid).not.toContain('PRIVATE-DRAFT-CONTENT-MUST-NOT-LEAK');
 expect((await request('/profile?site='+encodeURIComponent(site)+'&tab=posts',{user:null})).status).toBe(303);
 for(const url of ['/?tab=posts','/blog/']){
  const publicVersion=await publicPage('work',url);
  expect(publicVersion.status).toBe(200);
  expect(publicVersion.body).not.toContain('Private draft library proof');
  expect(publicVersion.body).not.toContain('PRIVATE-DRAFT-CONTENT-MUST-NOT-LEAK');
 }
 expect((await publicPage('work',route)).status).toBe(404);
});

test('published journal articles join the same owner-composed grid without exposing drafts',async()=>{
 const site=sites[0].id,route='/blog/published-owner-journal-proof/';
 f.add('site_page',{route,title:'Published owner journal proof',heading:'Published owner journal proof',
  editorial_kind:'article',machine_view:'authored',status:'published'},site);
 f.add('site_fragment',{coordinate:'published-owner-journal-proof-story',page:route,slot:'showcase',
  audience:'public',kind:'profile-story',position:0,heading:'An authored published section',
  body:'Composed public journal article.',semantic_tags:{authoring_v1:{version:1,parent:null,payload:{},responsive:{}}},
  projection_contract:{html:true},status:'published'},site);
 const owner=await request('/profile?site='+encodeURIComponent(site)+'&tab=posts');
 expect(owner.status).toBe(200);
 const grid=owner.body.match(/<section class="ph-journal" data-profile-journal>[\s\S]*?<\/section>/)?.[0]||'';
 expect(grid).toContain('Your journal');
 expect(grid).toContain('Published owner journal proof');
 expect(grid).toContain('data-publication-state="published"');
 expect(grid).toContain(route);
 expect(grid).toContain('Private draft library proof');
 expect(grid).toContain('data-publication-state="draft"');
 const index=await publicPage('work','/blog/');
 expect(index.status).toBe(200);
 expect(index.body).toContain('Published owner journal proof');
 expect(index.body).not.toContain('Private draft library proof');
 expect((await publicPage('work',route)).status).toBe(200);
});
