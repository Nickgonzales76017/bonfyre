import {test as runTest,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {Database} from 'bun:sqlite';
import {packSetupFixture,root,decode} from './pack_setup_fixture.mjs';
const test=(name,fn)=>runTest(name,fn,30000);let f,owner,other,browser,matrix,workIndex,datingDb;
const sites=['work','life','live','shop'].map(t=>({id:'profile-avery-'+t,host:'avery.aurekai.'+t,territory:t}));
const hidden=body=>Object.fromEntries([...body.matchAll(/<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));
async function request(path,{site='work',user=owner,form,headers={}}={}){return f.request(path,{host:'aurekai.'+site,cookie:user?.cookie||'',form,headers:{...(form!==undefined?{Origin:'https://aurekai.'+site}:{}),...headers}});}
async function publicPage(site='work',path='/',user=null){return f.request(path,{host:'avery.aurekai.'+site,cookie:user?.cookie||''});}
function post(site,title,body,extra={}){return f.add('community_post',{post:site+':'+title,community:site,author_subject:'person:avery',title,body,subject_address:'',published_at:'2026-10-21T05:00:00Z',visibility:'public',moderation_state:'accepted',cannot_infer:'Isolated authored fixture.',status:'published',...extra},site);}
beforeAll(async()=>{f=await packSetupFixture({beforeServe:({home,env})=>{workIndex=join(home,'account-work.db');datingDb=join(home,'private-dating.db');
env.BONFYRE_DATING_DB=datingDb;env.BONFYRE_ACCOUNT_WORK_DB=workIndex;env.BONFYRE_ITEM_ARCHIVE_DB=join(home,"profile-items.db");const w=new Database(workIndex,{create:true});w.exec("CREATE TABLE account_work(id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,title TEXT NOT NULL,state TEXT NOT NULL,updated_ms INTEGER NOT NULL)");w.close();}});const setup=join(root,'cmd/BonfyreParticipation/bonfyre-participation');
f.run(setup,['dating-init','--db',datingDb]);
const schemas=join(f.home,'schemas');for(const type of ['site_presentation','community_post','community_body','saved_item','work_example'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);owner=await f.login('profile-owner@example.test');other=await f.login('profile-other@example.test');f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Private Account Name',owner.id);const db=join(f.home,'matrix.db');f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(owner.id),'--actor','person:avery']);f.run(f.web,['subject','--db',db,'--id','person:avery','--kind','Person','--digest','a'.repeat(64),'--evidence','fixture:profile']);f.run(f.web,['institution','--db',db,'--id','inst:person-avery','--kind','Person','--world','live','--evidence','fixture:profile']);
 const w=new Database(workIndex),insert=w.query('INSERT INTO account_work VALUES(?,?,?,?,?,?)');
 insert.run('account-operator-owned-1','user:'+owner.id,'self','Review the source-backed owner decision','blocked',1793175000000);
 insert.run('account-operator-owned-2','user:'+owner.id,'self','Continue an admitted native workflow','ready',1793174000000);
 insert.run('account-operator-other-1','user:'+other.id,'self','Other person private work','blocked',1793173000000);w.close();
 for(const s of sites){f.run(f.web,['domain','--db',db,'--host',s.host,'--institution','inst:person-avery','--territory',s.territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);f.run(f.web,['presence','--db',db,'--id','presence:avery:'+s.territory,'--subject','person:avery','--territory',s.territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);f.add('site_presentation',{site_id:s.id,host:s.host,subject_address:'person:avery',territory:s.territory,profile:'member-profile-v1',navigation:[],status:'published'});f.add('site_page',{route:'/',title:s.territory+' profile',heading:'Avery '+s.territory,lede:s.territory+' introduction',status:'published'},s.id);post(s.id,s.territory+'-only-public-post',s.territory+' public words');}
 post(sites[0].id,'private-post-marker','Never show this',{visibility:'self'});post(sites[0].id,'draft-post-marker','Never show draft',{status:'draft'});post(sites[0].id,'wrong-author-marker','Never show another author',{author_subject:'person:someone-else'});f.add('work_example',{example_id:'profile-work-release',site:'profile-avery-work',pack:'profile-workflow',program_template:'fixture-program',territories:'work',position:1,kicker:'Client release',headline:'A site-bound work release',dek:'A fixture release belongs only to this profile site.',made_with:'Fixture material',made_with_href:'https://example.test/material',observed_at:'2026-10-25T12:00:00Z',picture:'/assets/world-land.svg',picture_alt:'Fixture work',status:'published'});f.add('work_example',{example_id:'unbound-work-release',pack:'profile-workflow',territories:'work',position:2,kicker:'Unbound',headline:'Never publish this globally',dek:'This release has no profile binding.',made_with:'Fixture material',observed_at:'2026-10-25T12:00:00Z',picture:'/assets/world-land.svg',picture_alt:'Unbound work',status:'published'});
 f.add('saved_item',{saved:'owner-save',actor:'user:'+owner.id,item_ref:'newsdesk/'+'1'.repeat(24),title:'Owner private bookmark',href:'https://example.test/original',publisher:'Source publisher',summary:'Private saved excerpt MUST NOT be published',saved_at:'2026-10-21T05:00:00Z',collection:'Reading',cannot_infer:'Private fixture.',status:'published'});f.add('saved_item',{saved:'other-save',actor:'user:'+other.id,item_ref:'newsdesk/'+'2'.repeat(24),title:'Other account private bookmark',href:'https://example.test/other',summary:'Other private body',collection:'Other reading',saved_at:'2026-10-21T05:00:00Z',cannot_infer:'Private fixture.',status:'published'});
 matrix=new Database(db);browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();matrix?.close();await f?.close();},30000);
test('Stitch member feed filters only actually published site posts and keeps private data out',async()=>{
 const work=await publicPage('work'),life=await publicPage('life');
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage(),errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 try{
  await page.setViewport({width:1440,height:920});await page.setContent(work.body,{waitUntil:'domcontentloaded'});
  await page.waitForSelector('.ph-post-tools');
  expect(await page.$$eval('article[data-public-post]',nodes=>nodes.length)).toBe(1);
  expect(await page.$eval('.ph-post-tool-status',n=>n.textContent)).toContain('1 of 1');
  await page.click('[data-post-filter=linked]');
  expect(await page.$eval('article[data-public-post]',n=>n.hidden)).toBe(true);
  await page.click('[data-post-filter=original]');
  expect(await page.$eval('article[data-public-post]',n=>n.hidden)).toBe(false);
  await page.type('input[aria-label="Find in published posts"]','not this post');
  expect(await page.$eval('article[data-public-post]',n=>n.hidden)).toBe(true);
  await page.$eval('input[aria-label="Find in published posts"]',n=>{n.value='work-only-public-post';n.dispatchEvent(new Event('input',{bubbles:true}));});
  expect(await page.$eval('article[data-public-post]',n=>n.hidden)).toBe(false);
  expect(await page.$eval('[data-post-filter=original]',n=>n.getAttribute('aria-pressed'))).toBe('true');
  expect(await page.$eval('body',n=>n.textContent)).not.toContain('Other account private bookmark');
  expect(work.body).not.toContain('life-only-public-post');
  expect(life.body).not.toContain('work-only-public-post');
  await page.setViewport({width:390,height:900});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
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
 const ctx=await browser.createBrowserContext(),page=await ctx.newPage();
 try{for(const width of [1440,390]){await page.setViewport({width,height:900});await page.setContent(work.body,{waitUntil:'domcontentloaded'});
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
 expect((own.body.match(/class="ph-tab-icon"/g)||[]).length).toBe(9);
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
  for(const title of ['Sources','Recipes','Feeds','Profile','Collections','Operating roles','Posts','Worlds','Programs'])
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
 let pub=(await publicPage('work')).body;expect(pub).toContain('data-profile-cover');expect(pub).toContain(stag.images.web.url);expect(pub).toContain('object-position:30% 20%');expect(pub).toContain('href="/o/'+ref+'"');expect(pub).toMatch(/<span class="ph-avatar" aria-hidden="true">A<\/span>/);
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
 const out=process.env.BONFYRE_ART_SCREENSHOTS,ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[];p.on('pageerror',e=>errors.push(e.message));
 const [name,...value]=owner.cookie.split('=');await p.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 try{for(const width of [1440,390]){await p.setViewport({width,height:1000});
  await p.setContent((await publicPage('work')).body.replace('<head>','<head><base href="'+f.base+'/">'),{waitUntil:'load',timeout:90000});
   await p.waitForFunction(()=>[...document.querySelectorAll('[data-profile-cover] img,[data-gallery-item] img')].every(n=>n.complete&&n.naturalWidth>0),{timeout:20000}).catch(async error=>{
    const images=await p.$$eval('[data-profile-cover] img,[data-gallery-item] img',nodes=>nodes.map(n=>({src:n.currentSrc||n.src,complete:n.complete,width:n.naturalWidth})));
    throw Error('Profile artwork did not load: '+JSON.stringify(images)+' '+error.message);
   })
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  expect(await p.$eval('[data-profile-cover] img',n=>n.complete&&n.naturalWidth>0)).toBe(true);
  const tiles=await p.$$eval('[data-gallery-item]',n=>n.map(x=>{const b=x.getBoundingClientRect();return {w:Math.round(b.width),img:x.querySelector('img').naturalWidth>0};}));
  expect(tiles.length).toBe(4);expect(tiles.every(t=>t.img)).toBe(true);expect(tiles[0].w).toBeGreaterThan(tiles[1].w);
  if(out){mkdirSync(out,{recursive:true});await p.screenshot({path:join(out,'art-public-'+width+'.png'),fullPage:true});}
  await p.goto(f.base+'/profile?site=profile-avery-work&tab=gallery',{waitUntil:'load',timeout:90000});expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  if(out)await p.screenshot({path:join(out,'art-owner-gallery-'+width+'.png'),fullPage:true});
  await p.goto(f.base+'/profile?site=profile-avery-work&tab=artwork&art='+encodeURIComponent(ref),{waitUntil:'load',timeout:90000});
  await p.focus('[data-art-focus=x]');for(let i=0;i<5;i++)await p.keyboard.press('ArrowLeft');expect(await p.$eval('[data-art-preview]',n=>n.style.objectPosition)).toBe('65% 20%');
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  if(out)await p.screenshot({path:join(out,'art-owner-review-'+width+'.png'),fullPage:true});}
 await p.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);await p.setJavaScriptEnabled(false);await p.goto(f.base+'/profile?site=profile-avery-work&tab=gallery',{waitUntil:'load',timeout:90000});expect(await p.$$eval('[data-gallery-item] img',n=>n.length)).toBe(pieces.length-1);
 expect(errors.filter(e=>!e.includes('about:blank'))).toEqual([]);}finally{await ctx.close();}
},180000);
test('profile page composition stays within the selected owner site',async()=>{
 const schemas=join(f.home,'schemas');copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(schemas,'site_fragment.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const site=sites[0].id,route='/journal/fixture',q='?site='+site;expect(f.authsql.query('SELECT scope FROM sessions WHERE user_id=? ORDER BY id DESC LIMIT 1').get(owner.id).scope).toBe('self');
 const profile=await request('/profile?site='+site);
  const sitePicker=await request('/profile?tab=sites');
  expect(sitePicker.status).toBe(200);
  expect(sitePicker.body).toContain('data-profile-edit-pages href="/cms/compose/advanced?site='+site+'"');
  expect(profile.body).toContain('/cms/compose/advanced?site='+site+'&amp;new=article');
  expect(profile.body).toContain('Write an article');
  expect(profile.body).toContain('Edit pages');
   expect(profile.body).toContain('data-profile-edit-pages href="/cms/compose/advanced?site='+site+'"');
  expect(profile.body).toContain('Write a post');
 expect((await request('/cms/compose/advanced'+q,{user:other})).status).toBe(403);
 expect((await request('/cms/compose/advanced'+q,{user:null})).status).toBe(403);
 const opened=await request('/cms/compose/page'+q+'&route='+encodeURIComponent(route));expect(opened.status,opened.body).toBe(200);const snapshot=JSON.parse(opened.body);expect(snapshot.snapshot.namespace).toBe(site);
 const input={operation:'create',route,expected_id:0,expected_digest:snapshot.digest,page:{route,title:'Journal fixture',heading:'Journal fixture',status:'draft'},fragments:[]};
 const options={form:JSON.stringify(input),headers:{'Content-Type':'application/json'}};
 const publishNow={...input,page:{...input.page,status:'published'}};
 expect((await request('/cms/compose/page'+q,{form:JSON.stringify(publishNow),headers:{'Content-Type':'application/json'}})).status).toBe(409);
 const rootPage=JSON.parse((await request('/cms/compose/page'+q+'&route=%2F')).body);
 const overwrite={operation:'replace',route:'/',expected_id:rootPage.id,expected_digest:rootPage.digest,page:{route:'/',title:'Overwrite',heading:'Overwrite',status:'draft'},fragments:[]};
 expect((await request('/cms/compose/page'+q,{form:JSON.stringify(overwrite),headers:{'Content-Type':'application/json'}})).status).toBe(409);
 expect((await request('/cms/compose/page'+q,{user:other,...options})).status).toBe(403);
 expect((await request('/cms/compose/page'+q,{...options,headers:{...options.headers,Origin:'https://foreign.example'}})).status).toBe(403);
 expect((await request('/cms/compose/page'+q,options)).status).toBe(200);
 const saved=JSON.parse((await request('/cms/compose/page'+q+'&route='+encodeURIComponent(route))).body);expect(saved.snapshot.page.title).toBe('Journal fixture');
 const otherSite=JSON.parse((await request('/cms/compose/page?site='+sites[1].id+'&route='+encodeURIComponent(route))).body);expect(otherSite.id).toBe(0);
 expect((await request('/cms/compose/page?site='+site+'&site='+sites[1].id+'&route='+encodeURIComponent(route))).status).toBe(400);
});
test('all journal sources round-trip as profile-owned editable article drafts',async()=>{
 const {readFileSync}=await import('node:fs');const {createHash}=await import('node:crypto');const {treeFromNativeSnapshot}=await import('../../../components/BonfyreServiceSite/authoring/native-cms.mjs');
 const schemas=join(f.home,'schemas');copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(schemas,'site_fragment.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 const rows=readFileSync(join(root,'site/blog/post-index.tsv'),'utf8').trim().split('\n').slice(1);expect(rows.length).toBe(16);
 for(const entry of rows){
  const [subject,slug,,,source]=entry.split('\t'),original=readFileSync(join(root,source),'utf8'),parts=original.split('---');
  const title=parts[1].match(/^title: (.+)$/m)?.[1],body=parts.slice(2).join('---').trim(),route='/blog/'+slug+'/',site=sites[0].id,q='?site='+site;
  expect(subject).toBe('post:'+slug);expect(title).toBeTruthy();
  const baseline=JSON.parse((await request('/cms/compose/page'+q+'&route='+encodeURIComponent(route))).body);expect(baseline.id).toBe(0);
  const article={route,title,heading:title,editorial_kind:'article',media_subject_ref:'article:'+slug,status:'draft'};
  const fragment={coordinate:'journal-'+slug,page:route,slot:'showcase',audience:'public',kind:'profile-story',position:0,heading:title,body,status:'draft',source_ref:subject,source_digest:createHash('sha256').update(original).digest('hex'),semantic_tags:{authoring_v1:{version:1,parent:null,responsive:{}}}};
  const input={operation:'create',route,expected_id:0,expected_digest:baseline.digest,page:article,fragments:[fragment]};
  const saved=await request('/cms/compose/page'+q,{form:JSON.stringify(input),headers:{'Content-Type':'application/json'}});expect(saved.status,saved.body).toBe(200);
  const reopened=JSON.parse((await request('/cms/compose/page'+q+'&route='+encodeURIComponent(route))).body);
  expect(reopened.snapshot.namespace).toBe(site);expect(reopened.snapshot.page.media_subject_ref).toBe('article:'+slug);expect(reopened.snapshot.fragments[0].body).toBe(body);
  const identityPath='/cms/compose/narration-revision'+q+'&route='+encodeURIComponent(route);
  const identity=await request(identityPath);expect(identity.status,identity.body).toBe(200);
  const revision=JSON.parse(identity.body);expect(revision.schema).toBe('bonfyre.article-revision.v2');
  expect(revision.namespace).toBe(site);expect(revision.route).toBe(route);expect(revision.subject).toBe('article:'+slug);
  expect(revision.status).toBe('draft');expect(revision.sha256).toMatch(/^[a-f0-9]{64}$/);
  expect((await request(identityPath,{user:other})).status).toBe(403);
  expect((await request(identityPath,{user:null})).status).toBe(403);
  if(entry===rows[0]){
   const clean=x=>Object.fromEntries(Object.entries(x).filter(([k])=>!new Set(['id','namespace','created_at','updated_at','published_at']).has(k)));
   const edited=clean(reopened.snapshot.fragments[0]);edited.semantic_tags={authoring_v1:{version:1,parent:null,responsive:{},editorial:[{type:'paragraph',data:{text:'A changed editorial passage.'}}]}};
   const update={operation:'replace',route,expected_id:reopened.id,expected_digest:reopened.digest,page:clean(reopened.snapshot.page),fragments:[edited]};
   const result=await request('/cms/compose/page'+q,{form:JSON.stringify(update),headers:{'Content-Type':'application/json'}});expect(result.status,result.body).toBe(200);
   const changed=await request(identityPath);expect(changed.status,changed.body).toBe(200);expect(JSON.parse(changed.body).sha256).not.toBe(revision.sha256);
  }
  const tree=treeFromNativeSnapshot(reopened);expect(tree.blocks[0].props.body).toBe(body);
 }
});

test('the real profile dating route retains private opt-in, blocks self-certification and does not publish it',async()=>{
 const noAuth=await request('/profile?tab=dating',{user:null});
 expect([302,303]).toContain(noAuth.status);
 const own=await request('/profile?tab=dating',{site:'life'});
 expect(own.status).toBe(200);
 expect(own.body).toContain('data-native-dating');
 expect(own.body).toContain('Your authored Life profile');
 expect(own.body).toContain('/cms/compose/advanced?site=profile-avery-life');
 expect(own.body).toContain('Participation is off');
 expect(own.body).not.toContain('data-dating-candidate');
 expect(own.body).not.toContain('identity:verified');
 const publicLife=await publicPage('life');
 expect(publicLife.body).not.toContain('data-native-dating');
 expect(publicLife.body).not.toContain('Dating</a>');
 expect((await publicPage('life','/?tab=dating')).status).toBe(400);
 const foreign=await request('/profile?tab=dating',{user:other});
 expect(foreign.body).not.toContain('Your authored Life profile');
 expect(foreign.body).not.toContain('profile-avery-life');
 const blocked=await request('/profile/dating/request',{
   site:'life',form:{rev:'0',enable:'1',ack:'1'},
   headers:{Origin:'https://another.example.test'}});
 expect(blocked.status).toBe(403);
 const withoutAck=await request('/profile/dating/request',{site:'life',form:{rev:'0',enable:'1'}});
 expect(withoutAck.status).toBe(400);
 const started=await request('/profile/dating/request',{site:'life',form:{rev:'0',enable:'1',ack:'1'}});
 expect(started.status).toBe(303);
 expect(started.headers.get('location')).toContain('/profile?tab=dating');
 const pending=await request('/profile?tab=dating',{site:'life'});
 expect(pending.status).toBe(200);
 expect(pending.body).toContain('Review requested');
 expect(pending.body).not.toContain('data-dating-candidate');
 const retry=await request('/profile/dating/request',{site:'life',form:{rev:'0',enable:'1',ack:'1'}});
 expect(retry.status).toBe(409);
 const stranger=await request('/profile/dating/request',{site:'life',user:other,
   form:{rev:'0',enable:'1',ack:'1'}});
 expect(stranger.status).toBe(403);
 const active=new Database(datingDb);
 try{
  const state=active.query("SELECT subject,revision,state FROM dating_request WHERE subject='person:avery'").get();
  expect(state).toEqual({subject:'person:avery',revision:1,state:'requested'});
  expect(active.query("SELECT count(*) AS n FROM dating_person").get().n).toBe(0);
  expect(active.query("SELECT count(*) AS n FROM dating_verification").get().n).toBe(0);
 }finally{active.close();}
 const withdrawn=await request('/profile/dating/request',{site:'life',
   form:{rev:'1',enable:'0'}});
 expect(withdrawn.status).toBe(303);
 const after=await request('/profile?tab=dating',{site:'life'});
 expect(after.body).toContain('Participation is off');
});

test('two real account sessions use one Life profile, native private inbox, preferences, reviewed plan and instant block',async()=>{
 const second='profile-blake-life',host='blake.aurekai.life',path=join(f.home,'matrix.db');
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(other.id),'--actor','person:blake']);
 f.run(f.web,['subject','--db',path,'--id','person:blake','--kind','Person','--digest','b'.repeat(64),'--evidence','fixture:blake']);
 f.run(f.web,['institution','--db',path,'--id','inst:person-blake','--kind','Person','--world','live','--evidence','fixture:blake']);
 f.run(f.web,['domain','--db',path,'--host',host,'--institution','inst:person-blake','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:blake']);
 f.run(f.web,['presence','--db',path,'--id','presence:blake:life','--subject','person:blake','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:blake']);
 f.add('site_presentation',{site_id:second,host,subject_address:'person:blake',
   territory:'life',profile:'member-profile-v1',navigation:[],status:'published'});
 f.add('site_page',{route:'/',title:'Blake life',heading:'Blake',lede:'A short public introduction',
   status:'published'},second);
 const ownerOn=await request('/profile/dating/request',{site:'life',form:{rev:'2',enable:'1',ack:'1'}});
 const otherOn=await request('/profile/dating/request',{site:'life',user:other,
   form:{rev:'0',enable:'1',ack:'1'}});
 expect(ownerOn.status).toBe(303);expect(otherOn.status).toBe(303);
 const db=new Database(datingDb);
 try{
  // Test-only issuer fixture: production has no actor-facing route to create
  // verified age or moderation records, and must use its independent owners.
  db.exec("INSERT INTO dating_verification"
   +"(subject,age,eligibility_witness,moderation_witness,issued_by,state,valid_until) VALUES"
   +"('person:avery',30,'identity:avery','media:avery','BonfyreAuth','verified',"
   +"CAST(strftime('%s','now') AS INTEGER)+3600),"
   +"('person:blake',31,'identity:blake','media:blake','BonfyreAuth','verified',"
   +"CAST(strftime('%s','now') AS INTEGER)+3600)");
  db.exec("INSERT INTO dating_person"
   +"(subject,revision,age,min_age,max_age,active,intro,eligibility_witness,moderation_witness) VALUES"
   +"('person:avery',1,30,25,40,1,1,'identity:avery','media:avery'),"
   +"('person:blake',1,31,25,40,1,1,'identity:blake','media:blake')");
 }finally{db.close();}
 const initial=await request('/profile?tab=dating',{site:'life'});
 expect(initial.status).toBe(200);
 expect(initial.body).toContain('data-dating-candidate');
 expect(initial.body).toContain('person:blake');
 expect(initial.body).toContain('Private preferences');
 expect(initial.body).not.toContain('identity:blake');
 const interest=await request('/profile/dating/choice',{site:'life',
   form:{other:'person:blake',choice:'interest',rev:'0'}});
 expect(interest.status).toBe(303);
 const introduction=await request('/profile?tab=dating',{site:'life',user:other});
 expect(introduction.body).toContain('Accept introduction');
 expect(introduction.body).not.toContain('Good to meet you through Aurekai');
 const otherAccept=await request('/profile/dating/choice',{site:'life',user:other,
   form:{other:'person:avery',choice:'accept',rev:'0'}});
 expect(otherAccept.status).toBe(303);
 const ownerAccept=await request('/profile/dating/choice',{site:'life',
   form:{other:'person:blake',choice:'accept',rev:'1'}});
 expect(ownerAccept.status).toBe(303);
 const agreed=await request('/profile?tab=dating',{site:'life'});
 expect(agreed.body).toContain('Both of you have agreed');
 const selected={rev:'1',p1:'1',p2:'-1',p3:'1',p4:'1',i1:'5',i2:'3',i3:'4',i4:'1'};
 const counterpart={...selected,p2:'1'};
 expect((await request('/profile/dating/preferences',{site:'life',form:selected})).status).toBe(303);
 expect((await request('/profile/dating/preferences',{site:'life',user:other,form:counterpart})).status).toBe(303);
 const comparison=await request('/profile?tab=dating',{site:'life'});
 expect(comparison.body).toContain('shared preference agreement');
 const nonce='a'.repeat(32),idea='Museum walk and coffee';
 expect((await request('/profile/dating/message',{site:'life',
   form:{other:'person:blake',nonce,body:'Good to meet you through Aurekai'}})).status).toBe(303);
 const reply=await request('/profile?tab=dating',{site:'life',user:other});
 expect(reply.body).toContain('Good to meet you through Aurekai');
 const selfInbox=await request('/inbox?context=self',{site:'life'});
 const otherInbox=await request('/inbox?context=self',{site:'life',user:other});
 expect(selfInbox.body).toContain('data-private-dating');
 expect(otherInbox.body).toContain('Open conversations');
 const planKey='b'.repeat(32);
 const proposal=await request('/profile/dating/plan',{site:'life',
   form:{other:'person:blake',nonce:planKey,activity:idea,
     context:'A mutually chosen public venue',estimate:'35.00',budget:'60.00',confirm:'1'}});
 expect(proposal.status).toBe(303);
 const proposed=await request('/profile?tab=dating',{site:'life',user:other});
 expect(proposed.body).toContain(idea);
 expect(proposed.body).toContain('Approve this proposal');
 expect((await request('/profile/dating/plan-approve',{site:'life',user:other,
   form:{plan:planKey,rev:'1'}})).status).toBe(303);
 const approved=await request('/profile?tab=dating',{site:'life'});
 expect(approved.body).toContain('Both approved');
 const blocked=await request('/profile/dating/choice',{site:'life',user:other,
   form:{other:'person:avery',choice:'block',rev:'1'}});
 expect(blocked.status).toBe(303);
 const inaccessible=await request('/profile?tab=dating',{site:'life'});
 expect(inaccessible.body).not.toContain('Good to meet you through Aurekai');
 expect(inaccessible.body).not.toContain(idea);
 expect((await request('/profile/dating/message',{site:'life',
   form:{other:'person:blake',nonce:'c'.repeat(32),body:'No consent after block'}})).status).toBe(409);
 expect((await publicPage('life')).body).not.toContain('data-private-dating');
 expect((await publicPage('life')).body).not.toContain(idea);
});

test('contextual participation shares one profile and feed without sharing consent, dating or civic authority',async()=>{
 const peer=await f.login('profile-morgan-context@example.test');
 const matrixPath=join(f.home,'matrix.db');
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(peer.id),'--actor','person:morgan']);
 f.run(f.web,['subject','--db',matrixPath,'--id','person:morgan','--kind','Person','--digest','c'.repeat(64),'--evidence','fixture:morgan']);
 f.run(f.web,['institution','--db',matrixPath,'--id','inst:person-morgan','--kind','Person','--world','live','--evidence','fixture:morgan']);
 f.run(f.web,['domain','--db',matrixPath,'--host','morgan.aurekai.life','--institution','inst:person-morgan',
   '--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:morgan']);
 f.run(f.web,['presence','--db',matrixPath,'--id','presence:morgan:life','--subject','person:morgan',
   '--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:morgan']);
 f.add('site_presentation',{site_id:'profile-morgan-life',host:'morgan.aurekai.life',
   subject_address:'person:morgan',territory:'life',profile:'member-profile-v1',
   navigation:[],status:'published'});
 f.add('site_page',{route:'/',title:'Morgan life',heading:'Morgan',lede:'Making and sharing',
   status:'published'},'profile-morgan-life');
 const direct=await request('/profile?tab=connections',{site:'life'});
 expect(direct.status).toBe(200);
 expect(direct.body).toContain('data-native-peer-context');
 expect(direct.body).toContain('Meet through what matters');
 for(const name of ['People & friendships','Art & making','Civic & community','Projects & work','Dating'])
  expect(direct.body).toContain(name.replaceAll('&','&amp;'));
 expect(direct.body).toContain('Not participating in this context');
 const art=await request('/profile?tab=connections&facet=art',{site:'life'});
 expect(art.status).toBe(200);
 expect(art.body).toContain('Art &amp; making');
 expect(art.body).not.toContain('data-peer-candidate');
 expect((await request('/profile?tab=connections&facet=dating',{site:'life'})).body).toContain('data-native-dating');
 expect((await publicPage('life','/?tab=connections')).status).toBe(400);
 expect((await request('/profile?tab=connections&facet=art',{user:null})).status).toBe(303);
 const db=new Database(datingDb),datingCount=db.query("SELECT count(*) AS n FROM dating_person").get().n;
 const initialBlocks=db.query("SELECT count(*) AS n FROM peer_block").get().n;
 db.close();
 const sourceSite='profile-morgan-life',published='Creative exchange from Morgan';
 f.add('community_post',{post:'morgan-context-art',community:sourceSite,
  author_subject:'person:morgan',title:published,body:'A published arts discussion',
  published_at:'2026-11-10T20:30:00Z',visibility:'public',moderation_state:'accepted',
  cannot_infer:'Public art writing does not confer licensing or private relationship authority.',
  status:'published'},sourceSite);
 const artJoin={facet:'art',rev:'0',enable:'1',ack:'1'};
 expect((await request('/profile/connections/join',{site:'life',form:artJoin})).status).toBe(303);
 expect((await request('/profile/connections/join',{site:'life',user:peer,form:artJoin})).status).toBe(303);
 expect((await request('/profile/connections/join',{site:'life',form:artJoin})).status).toBe(409);
 const interests={facet:'art',epoch:'1',rev:'0',topics:'Hiking, Pottery'};
 expect((await request('/profile/connections/interests',{site:'life',form:interests,headers:{Origin:'https://elsewhere.example.test'}})).status).toBe(403);
 expect((await request('/profile/connections/interests',{site:'life',form:interests})).status).toBe(303);
 expect((await request('/profile/connections/interests',{site:'life',form:interests})).status).toBe(409);
 expect((await request('/profile/connections/interests',{site:'life',user:peer,form:{...interests,topics:'hiking, painting'}})).status).toBe(303);
 expect((await request('/profile/connections/interests',{site:'life',form:{...interests,facet:'civic',rev:'0'}})).status).toBe(409);
 const context=await browser.createBrowserContext(),tab=await context.newPage(),browserErrors=[];
 tab.on('pageerror',error=>browserErrors.push(error.message));
 const [cookieName,...cookieValue]=owner.cookie.split('=');
 await tab.setCookie({name:cookieName,value:cookieValue.join('='),url:f.base,httpOnly:true});
 try{
  await tab.goto(f.base+'/profile?tab=connections&facet=art',{waitUntil:'networkidle0'});
  await tab.waitForSelector('.ph-connect-interests input[name=topics]');
  await tab.$eval('.ph-connect-interests input[name=topics]',input=>{input.value='hiking, pottery, cycling';input.dispatchEvent(new Event('input',{bubbles:true}));});
  await Promise.all([tab.waitForNavigation(),tab.click('.ph-connect-interests button')]);
  expect(await tab.$eval('.ph-connect-interests input[name=topics]',input=>input.value)).toBe('hiking,pottery,cycling');
  for(const width of [1440,390]){await tab.setViewport({width,height:900});expect(await tab.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);}
  expect(browserErrors).toEqual([]);
 }finally{await context.close();}
 const feed=await request('/profile?tab=connections&facet=art',{site:'life'});
 expect(feed.body).toContain('data-peer-feed');
 expect(feed.body).toContain('data-peer-candidate');
 expect(feed.body).toContain('Shared interests: hiking');
 expect(feed.body).not.toContain('Shared interests: painting');
 expect((await publicPage('life')).body).not.toContain('Shared interests: hiking');
 expect(feed.body).toContain(published);
 expect(feed.body).toContain('https://morgan.aurekai.life/?tab=posts');
 const cross=await request('/profile/connections/choice',{site:'life',
  form:{facet:'art',other:'person:morgan',choice:'interest',rev:'0'},
  headers:{Origin:'https://elsewhere.example.test'}});
 expect(cross.status).toBe(403);
 expect((await request('/profile/connections/choice',{site:'life',
  form:{facet:'art',other:'person:morgan',choice:'interest',rev:'0'}})).status).toBe(303);
 const message={facet:'art',other:'person:morgan',nonce:'a'.repeat(32),body:'A private note about making art.'};
 expect((await request('/profile/connections/message',{site:'life',form:message})).status).toBe(409);
 const inbound=await request('/profile?tab=connections&facet=art',{site:'life',user:peer});
 expect(inbound.body).toContain('Accept introduction');
 expect(inbound.body).not.toContain('You both agreed to connect in this context');
 const pending=await request('/inbox?context=self',{site:'life',user:peer});
 expect(pending.body).toContain('data-private-peer-facets');
 expect(pending.body).toContain('Art &amp; making');
 expect((await request('/profile/connections/choice',{site:'life',user:peer,
  form:{facet:'art',other:'person:avery',choice:'accept',rev:'0'}})).status).toBe(303);
 expect((await request('/profile/connections/choice',{site:'life',
  form:{facet:'art',other:'person:morgan',choice:'accept',rev:'1'}})).status).toBe(303);
 expect((await request('/profile?tab=connections&facet=art',{site:'life'})).body)
  .toContain('You both agreed to connect in this context');
 expect((await request('/profile/connections/message',{site:'life',form:message,headers:{Origin:'https://elsewhere.example.test'}})).status).toBe(403);
 expect((await request('/profile/connections/message',{site:'life',form:message})).status).toBe(303);
 expect((await request('/profile/connections/message',{site:'life',form:message})).status).toBe(303);
 expect((await request('/profile/connections/message',{site:'life',form:{...message,body:'Changed text'}})).status).toBe(409);
 const thread=await request('/profile?tab=connections&facet=art',{site:'life',user:peer});
 expect(thread.body).toContain('A private note about making art.');
 expect((await publicPage('life')).body).not.toContain('A private note about making art.');
 const joinCivic={facet:'civic',rev:'0',enable:'1',ack:'1'};
 expect((await request('/profile/connections/join',{site:'life',form:joinCivic})).status).toBe(303);
 expect((await request('/profile/connections/join',{site:'life',user:peer,form:joinCivic})).status).toBe(303);
 const civic=await request('/profile?tab=connections&facet=civic',{site:'life'});
 expect(civic.body).toContain('Civic &amp; community');
 expect(civic.body).not.toContain('You both agreed to connect in this context');
 expect((await request('/profile/connections/choice',{site:'life',user:peer,
  form:{facet:'civic',other:'person:avery',choice:'block',rev:'0'}})).status).toBe(303);
 const after=await request('/profile?tab=connections&facet=art',{site:'life'});
 expect(after.body).not.toContain('data-peer-candidate');
 expect(after.body).not.toContain('A private note about making art.');
 expect((await request('/profile/connections/message',{site:'life',form:{...message,nonce:'b'.repeat(32)}})).status).toBe(409);
 expect((await publicPage('life')).body).not.toContain('data-native-peer-context');
 const final=new Database(datingDb);
 expect(final.query("SELECT count(*) AS n FROM dating_person").get().n).toBe(datingCount);
 expect(final.query("SELECT count(*) AS n FROM peer_block").get().n).toBe(initialBlocks+1);
 final.close();
});
test('authored CMS articles appear on the same profile as full site pages, with private drafts isolated',async()=>{
 const site=sites[0].id;
 f.add('site_page',{route:'/journal/native-profile-story/',title:'Native profile story',heading:'Native profile story',
  lede:'A complete page whose layout belongs to the native authoring owner.',
  editorial_kind:'article',media_subject_ref:'article:native-profile-story',editorial_published_on:'2026-11-10',status:'published'},site);
 f.add('site_page',{route:'/journal/private-profile-story/',title:'Private authored revision',heading:'Private authored revision',
  lede:'Unreleased source owned only by the editor.',editorial_kind:'article',
  media_subject_ref:'article:private-profile-story',status:'draft'},site);
 const published=await publicPage('work','/?tab=posts');
 expect(published.status).toBe(200);
 expect(published.body).toContain('data-authored-articles');
 expect(published.body).toContain('Native profile story');
 expect(published.body).toContain('href="/journal/native-profile-story/"');
 expect(published.body).not.toContain('Private authored revision');
 expect(published.body).not.toContain('Make this space yours');
 const privateView=await request('/profile?site='+site+'&tab=posts');
 expect(privateView.status).toBe(200);
 expect(privateView.body).toContain('Native profile story');
 expect(privateView.body).toContain('Private authored revision');
 expect(privateView.body).toContain('Compose article');
 expect(privateView.body).toContain('Bring in journal');
 expect(privateView.body).toContain('/cms/compose/advanced?site='+site+'&amp;route=%2Fjournal%2Fprivate-profile-story%2F');
 expect((await publicPage('life','/?tab=posts')).body).not.toContain('Native profile story');
 expect((await request('/profile?site='+site+'&tab=posts',{user:other})).status).toBe(404);
});
