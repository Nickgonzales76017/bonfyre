// Full native integration coverage. Release receipts retain actual execution results.
// Uses actual isolated Auth/CMS/Matrix/WebAddress fixture owners, not live users.
import {test as testCase,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {packSetupFixture,root,decode} from './pack_setup_fixture.mjs';
const test=(name,run)=>testCase(name,run,45000);
let f,owner,other,browser;
const hidden=html=>Object.fromEntries([...html.matchAll(/<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));
async function request(path,{user=owner,host='aurekai.work',form,headers={}}={}){
 return f.request(path,{host,cookie:user?.cookie||'',form,headers:{Accept:'text/html',...(form!==undefined?{Origin:'https://'+host}:{}),...headers}});
}
async function detail(kind='occurrence',id='journey-lecture',user=owner){
 const response=await request('/participation?'+new URLSearchParams({kind,id}),{user});
 expect(response.status).toBe(200);return {response,form:hidden(response.body)};
}
function occurrence(id,title,extra={}){
 f.add('publication_occurrence',{occurrence_id:id,source_twin:'source:journey',source_ref:'journey-source',canonical_href:'https://example.test/'+id,title,summary:'An isolated public fixture with no reservation or external effect.',observed_at:'2026-10-21T00:00:00Z',receipt_ref:'fixture:journey',rights_basis:'fixture',retention:'metadata',visibility:'public',assertion_state:'OBSERVED',cannot_infer:'Generated fixture, not a real event.',status:'published',...extra});
}
beforeAll(async()=>{
 f=await packSetupFixture();const schemas=join(f.home,'schemas');
 for(const type of ['site_presentation','site_page','community_body','community_membership','publication_occurrence','occurrence_follow','community_post','saved_item']){
  copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 }
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 owner=await f.login('participant-return-owner@example.test');other=await f.login('participant-return-other@example.test');
 occurrence('journey-lecture','Open lecture');
 f.add('community_body',{community:'journey-repair',subject_address:'community:journey-repair',title:'Community repair',description:'Bring a repair question.',visibility:'public',cannot_infer:'Generated local community fixture.',status:'published'});
 const matrix=join(f.home,'matrix.db');
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(owner.id),'--actor','person:journeytester']);
 f.run(f.web,['subject','--db',matrix,'--id','person:journeytester','--kind','Person','--digest','b'.repeat(64),'--evidence','fixture:participant']);
 f.run(f.web,['institution','--db',matrix,'--id','inst:person-journeytester','--kind','Person','--world','live','--evidence','fixture:participant']);
 for(const territory of ['work','life','live','shop']){
  const host='journeytester.aurekai.'+territory;
  f.run(f.web,['domain','--db',matrix,'--host',host,'--institution','inst:person-journeytester','--territory',territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:participant']);
  f.run(f.web,['presence','--db',matrix,'--id','presence:journey:'+territory,'--subject','person:journeytester','--territory',territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:participant']);
  f.add('site_presentation',{site_id:'journey-'+territory,host,subject_address:'person:journeytester',territory,profile:'member-profile-v1',navigation:[],status:'published'});
  f.add('site_page',{route:'/',title:'Fixture profile',heading:'Fixture '+territory,lede:'Independent '+territory+' introduction',status:'published'},'journey-'+territory);
 }
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();},30000);
test('guest source and selected profile sections preserve exact sign-in continuation without performing a write',async()=>{
 const before=f.sql.query('SELECT count(*) AS n FROM occurrence_follow').get().n;
 const guest=await detail('occurrence','journey-lecture',null);
 expect(guest.response.body).not.toContain('action="/participation/change"');
 const href=decode(guest.response.body.match(/data-participant-signin href="([^"]+)"/)[1]);
 expect(new URL(href,'https://aurekai.work').searchParams.get('next')).toBe('/participation?kind=occurrence&id=journey-lecture');
 for(const target of ['/profile?site=journey-life&tab=saved&q=repair%20%26%20reuse','/profile?tab=activity&kind=community&site=journey-work']){
  const r=await request(target,{user:null});expect(r.status).toBe(303);
  expect(new URL(r.headers.get('location'),'https://aurekai.work').searchParams.get('next')).toBe(target);
 }
 expect(f.sql.query('SELECT count(*) AS n FROM occurrence_follow').get().n).toBe(before);
});
test('normal participation appears privately across territories, then supports deliberate withdrawal and re-follow',async()=>{
 let {form}=await detail();expect(form.action).toBe('follow');
 expect((await request('/participation/change',{form})).status).toBe(303);
 expect((await request('/participation/change',{form})).status).toBe(303);
 const rows=f.sql.query('SELECT * FROM occurrence_follow WHERE follower=?').all('user:'+owner.id);expect(rows.length).toBe(1);
 for(const territory of ['work','life','live','shop']){
  const r=await request('/profile?tab=activity',{host:'aurekai.'+territory});expect(r.status).toBe(200);expect(r.body).toContain('Open lecture');expect(r.body).toContain('data-private-participation');
  const publicPage=await request('/',{host:'journeytester.aurekai.'+territory,user:null});expect(publicPage.status).toBe(200);expect(publicPage.body).not.toContain('data-private-participation');expect(publicPage.body).not.toContain('Open lecture');
  expect((await request('/?tab=activity',{host:'journeytester.aurekai.'+territory,user:owner})).status).toBe(400);
 }
 expect((await request('/profile?tab=activity',{user:other})).body).not.toContain('Open lecture');
 ({form}=await detail());expect(form.action).toBe('unfollow');expect((await request('/participation/change',{form})).status).toBe(303);
 const off=await detail();expect(off.form.action).toBe('follow');
 expect((await request('/govern/occurrence-follow',{form:{id:'journey-lecture',return:'/subjects'}})).status).toBe(409);
 expect((await request('/participation/change',{form:off.form})).status).toBe(303);
 expect(f.sql.query('SELECT id FROM occurrence_follow WHERE follower=?').get('user:'+owner.id).id).toBe(rows[0].id);
});
test('community retries preserve active roles and withdrawn requests require a fresh explicit review',async()=>{
 const first=await detail('community','journey-repair');expect((await request('/participation/change',{form:first.form})).status).toBe(303);
 let pending=await detail('community','journey-repair');expect(pending.response.body).toContain('Request saved');expect(pending.form.action).toBe('withdraw');
 expect((await request('/participation/change',{form:pending.form})).status).toBe(303);
 expect((await request('/govern/community-join',{form:{id:'journey-repair'}})).status).toBe(409);
 const again=await detail('community','journey-repair');expect((await request('/participation/change',{form:again.form})).status).toBe(303);
 // An existing owner decision is a fixture input, not an implemented moderation journey.
 f.sql.query("UPDATE community_membership SET state='active',role='moderator',authority_ref='fixture:grant' WHERE community=? AND member_subject=?").run('journey-repair','user:'+owner.id);
 const before=f.sql.query('SELECT * FROM community_membership WHERE member_subject=?').get('user:'+owner.id);
 expect((await request('/govern/community-join',{form:{id:'journey-repair',return:'/subjects'}})).status).toBe(303);
 expect(f.sql.query('SELECT * FROM community_membership WHERE member_subject=?').get('user:'+owner.id)).toEqual(before);
 expect((await detail('community','journey-repair')).response.body).not.toContain('name="action" value="withdraw"');
});
test('source withdrawal hides content while its own private relation remains reversible',async()=>{
 occurrence('journey-withdrawn','Former public item');const first=await detail('occurrence','journey-withdrawn');await request('/participation/change',{form:first.form});
 f.sql.query("UPDATE publication_occurrence SET visibility='self',title='Restricted corrected title',summary='Restricted body' WHERE occurrence_id='journey-withdrawn'").run();
 const own=await detail('occurrence','journey-withdrawn');expect(own.response.body).not.toContain('Restricted corrected');expect(own.response.body).not.toContain('Restricted body');expect(own.form.action).toBe('unfollow');
 expect((await request('/participation?kind=occurrence&id=journey-withdrawn',{user:other})).status).toBe(404);
 expect((await request('/participation/change',{form:own.form})).status).toBe(303);
});
test('origins, actor injection, duplicate keys and stale preimages refuse without changing records',async()=>{
 occurrence('journey-boundary','Boundary source');const {form}=await detail('occurrence','journey-boundary');
 for(const origin of ['', 'null','https://another.example','http://aurekai.work'])expect((await request('/participation/change',{form,headers:{Origin:origin}})).status).toBe(403);
 expect((await request('/participation/change',{form:{...form,actor:'user:'+other.id}})).status).toBe(400);
 expect((await request('/participation/change',{form:new URLSearchParams(form).toString()+'&id=journey-lecture'})).status).toBe(400);
 expect((await request('/participation/change',{user:other,form})).status).toBe(409);
 expect(f.sql.query("SELECT count(*) AS n FROM occurrence_follow WHERE occurrence_id='journey-boundary'").get().n).toBe(0);
});
test('a failed owner write returns an error rather than a success redirect',async()=>{
 occurrence('journey-write-failure','Retained failure fixture');const {form}=await detail('occurrence','journey-write-failure');
 f.sql.exec("CREATE TRIGGER participant_write_refusal BEFORE INSERT ON occurrence_follow WHEN NEW.occurrence_id='journey-write-failure' BEGIN SELECT RAISE(ABORT,'test refusal');END");
 try{const r=await request('/participation/change',{form});expect(r.status).toBe(503);expect(r.headers.get('location')).toBeNull();expect(f.sql.query("SELECT count(*) AS n FROM occurrence_follow WHERE occurrence_id='journey-write-failure'").get().n).toBe(0);}finally{f.sql.exec('DROP TRIGGER participant_write_refusal');}
});
test('native browser forms return to the same private activity and work at phone width',async()=>{
 occurrence('journey-browser','Browser participant source');
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];
 page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(15000);
 const [name,...value]=owner.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 try{
  await page.setViewport({width:1440,height:1000});
  await page.goto(f.base+'/participation?kind=occurrence&id=journey-browser',{waitUntil:'networkidle0'});
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('form[action="/participation/change"] button')]);
  expect(await page.$eval('.pj-state h2',n=>n.textContent)).toBe('Following locally');
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('.pj-state a[href*="tab=activity"]')]);
  await page.type('input[aria-label="Search my activity"]','Browser participant');
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('.pj-activity form button')]);
  expect(await page.$$eval('.pj-row',rows=>rows.length)).toBe(1);
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('.pj-row h3 a')]);
  expect(new URL(page.url()).searchParams.get('id')).toBe('journey-browser');
  for(const width of [1440,390]){await page.setViewport({width,height:1000});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);}
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('form[action="/participation/change"] button')]);
  expect(await page.$eval('.pj-state h2',n=>n.textContent)).toBe('No longer following');expect(errors).toEqual([]);
 }finally{await context.close();}
});

test('a newly declared community owner cannot be bypassed through the legacy participant path',async()=>{
 f.add('community_body',{community:'journey-managed',subject_address:'community:journey-managed',title:'Managed local community',description:'Separate native community owner.',visibility:'public',moderation_policy:'aurekai-member-conversation-v1',created_by_subject:'user:'+owner.id,cannot_infer:'Synthetic local owner fixture.',status:'published'});
 const page=await detail('community','journey-managed',other);
 expect(page.response.body).toContain('href="/community?community=journey-managed"');
 expect(page.response.body).not.toContain('action="/participation/change"');
 expect(f.sql.query("SELECT count(*) AS n FROM community_membership WHERE community='journey-managed'").get().n).toBe(0);
 const before=await detail('community','journey-repair',other);
 f.sql.query("UPDATE community_body SET moderation_policy='new-policy',created_by_subject=? WHERE community='journey-repair'").run('user:'+owner.id);
 try{expect((await request('/participation/change',{user:other,form:before.form})).status).toBe(409);}
 finally{f.sql.query("UPDATE community_body SET moderation_policy='',created_by_subject='' WHERE community='journey-repair'").run();}
 expect(f.sql.query("SELECT count(*) AS n FROM community_membership WHERE community='journey-repair' AND member_subject=?").get('user:'+other.id).n).toBe(0);
});
