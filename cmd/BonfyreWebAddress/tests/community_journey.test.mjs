import {test as runTest,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync,mkdirSync} from 'node:fs';
import {join as pathJoin} from 'node:path';
import {createRequire} from 'node:module';
import {Database} from 'bun:sqlite';
import {packSetupFixture,root,decode,password} from './pack_setup_fixture.mjs';
const test=(name,fn,timeout=45000)=>runTest(name,fn,timeout);
let f,owner,member,outsider,browser;let serial=0;
beforeAll(async()=>{
 f=await packSetupFixture({beforeServe:({home,env})=>{env.BONFYRE_PUBLIC_PROJECTION_DB=pathJoin(home,'public-projection.db');}});
 for(const type of ['community_body','community_membership','community_post','community_reaction','community_presence','site_presentation','program_template','program_instance','program_requirement'])copyFileSync(pathJoin(root,'cmd/BonfyreCMS/content-types',type+'.json'),pathJoin(f.home,'schemas',type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',pathJoin(f.home,'schemas')]);
 owner=await f.login('community-owner@example.test');member=await f.login('community-member@example.test');outsider=await f.login('community-outsider@example.test');
 browser=await createRequire(pathJoin(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},45000);
afterAll(async()=>{await browser?.close();await f?.close();},15000);
const path=(key,tab='conversations',post='')=>'/community?'+new URLSearchParams({community:key,tab,...post?{post}:{}});
const get=(url,user=owner,options={})=>f.request(url,{cookie:user?.cookie||'',...options});
const send=(form,user=owner,url='/community/action',headers={})=>f.request(url,{cookie:user?.cookie||'',form,headers:{Origin:'https://aurekai.life',...headers}});
function form(html,operation){const matched=[...html.matchAll(/<form\b[^>]*>([\s\S]*?)<\/form>/g)].find(m=>m[1].includes('name="operation" value="'+operation+'"'));if(!matched)throw Error('Missing '+operation+' form: '+html.slice(-2000));const values=Object.fromEntries([...matched[1].matchAll(/<input\b[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));for(const m of matched[1].matchAll(/<textarea\b[^>]*name="([^"]+)"[^>]*>([\s\S]*?)<\/textarea>/g))values[m[1]]=decode(m[2]);return values;}
async function create(name='Fixture community',user=owner){const page=await get('/community/new',user);expect(page.status).toBe(200);const values={...form(page.body,'create'),title:name+' '+(++serial),text:'A shared workshop for useful public discussion. Everyone keeps their own contributions.'};const preview=await send(values,user,'/community/preview');expect(preview.status).toBe(200);const reviewed=form(preview.body,'create'),r=await send(reviewed,user);if(r.status!==303)throw Error('Create refused '+r.status+': '+r.body.slice(-2000));return {key:new URL(r.headers.get('location'),'https://aurekai.life').searchParams.get('community'),reviewed};}
async function join(key,user=member){const p=await get(path(key,'join'),user),r=await send(form(p.body,'request'),user);expect(r.status).toBe(303);return r;}
async function accept(key,user=member,steward=owner){const p=await get(path(key,'members'),steward),forms=[...p.body.matchAll(/<form\b[^>]*>[\s\S]*?<\/form>/g)];const selected=forms.find(m=>m[0].includes('value="accept"')&&m[0].includes('value="user:'+user.id+'"'));if(!selected)throw Error('Exact membership request missing');const r=await send(form(selected[0],'accept'),steward);expect(r.status).toBe(303);}
async function publish(key,user=owner,{text='Useful original contribution.',title='A workshop question',parent=''}={}){const p=await get(path(key,parent?'reply':'write',parent),user);if(p.status!==200)throw Error('Editor refused '+p.status);const op=parent?'reply':'post',input={...form(p.body,op),title:parent?'':title,text},review=await send(input,user,'/community/preview');expect(review.status).toBe(200);const approved=form(review.body,op),r=await send(approved,user);if(r.status!==303)throw Error('Publish refused '+r.status+': '+r.body.slice(-2000));return {key:new URL(r.headers.get('location'),'https://aurekai.life').searchParams.get('post'),reviewed:approved};}
const row=(table,key,value)=>f.sql.query('SELECT * FROM '+table+' WHERE '+key+'=?1 AND namespace=\'root\'').get(value);
test('an explicitly created local guild is a real stewarded community, not inferred apprenticeship or financial authority',async()=>{
 const starting=await get('/guild/new');expect(starting.status).toBe(200);
 const draft={...form(starting.body,'create-guild'),title:'Independent Repair Guild',text:'A public circle for craft learning, repair stories and stewardship.'};
 const preview=await send(draft,owner,'/community/preview');expect(preview.status).toBe(200);expect(preview.body).toContain('Create this guild');
 const before=f.sql.query('SELECT count(*) n FROM community_body').get().n;
 const created=await send(form(preview.body,'create-guild'));expect(created.status).toBe(303);
 const key=new URL(created.headers.get('location'),'https://aurekai.life').searchParams.get('community');
 expect(key).toMatch(/^guild-[a-f0-9]{64}$/);
 const saved=f.sql.query('SELECT governance_scope,status,moderation_policy FROM community_body WHERE community=?').get(key);
 expect(saved).toEqual({governance_scope:'local-guild',status:'published',moderation_policy:'aurekai-member-conversation-v1'});
 expect(f.sql.query('SELECT count(*) n FROM community_body').get().n).toBe(before+1);
 const replay=await send(form(preview.body,'create-guild'));expect(replay.status).toBe(303);
 expect(f.sql.query('SELECT count(*) n FROM community_body').get().n).toBe(before+1);
 const guilds=await get('/guilds',null),general=await get('/communities',null);
 expect(guilds.status).toBe(200);expect(guilds.body).toContain('Independent Repair Guild');
 expect(guilds.body).toContain('action="/guilds"');expect(general.body).toContain('Independent Repair Guild');
 expect((await get('/guilds?q=unmatched',null)).body).not.toContain('Independent Repair Guild');
 expect((await get('/guilds?cursor=invalid',null)).status).toBe(400);
 const ordinary=await create('Ordinary walking community');
 expect((await get('/guilds',null)).body).not.toContain('Ordinary walking community');
 expect((await get(path(key),null)).status).toBe(200);
 expect((await get(path(key,'write'),member)).status).toBe(403);
 await join(key);expect((await get(path(key,'write'),member)).status).toBe(403);
 await accept(key);expect((await get(path(key,'write'),member)).status).toBe(200);
 const notice=await publish(key,member,{title:'Bench check',text:'Bring tools and a safe inspection plan.'});
 expect((await get(path(key,'thread',notice.key),null)).body).toContain('Bench check');
 const membershipPage=await get(path(key,'members'));expect(membershipPage.status).toBe(200);
 const revocation=form(membershipPage.body,'revoke');expect((await send(revocation)).status).toBe(303);
 expect((await get(path(key,'write'),member)).status).toBe(403);
 expect(f.sql.query('SELECT count(*) n FROM pack_installation').get().n).toBe(0);
 expect(f.authsql.query('SELECT count(*) n FROM institution_grants').get().n).toBe(0);
});
test('guild programmes bind exact ProgrammeFabric records to a reviewed guild and preserve membership and evidence boundaries',async()=>{
 const {key}=await (async()=>{const p=await get('/guild/new');const preview=await send({...form(p.body,'create-guild'),title:'Precision Craft Studio',text:'Document craft safety, shared observations and the work people want to learn.'},owner,'/community/preview');
  const created=await send(form(preview.body,'create-guild'));expect(created.status).toBe(303);return {key:new URL(created.headers.get('location'),'https://aurekai.life').searchParams.get('community')};})();
 const root=f.sql.query('SELECT governance_scope FROM community_body WHERE community=?').get(key);expect(root.governance_scope).toBe('local-guild');
 f.add('program_template',{template_id:'craft-inspection-v1',name:'Workshop Inspection and Learning',archetype:'guild-practice',
  subject_kinds:['community_body'],semantic_slots:['workshop','observations'],capability_families:['education','documentation'],
  owner_bindings:{program:'BonfyreProgramFabric'},surface_policy:'member-only',page_policy:'published',
  description:'A supervised sequence of observations for the workshop. No credentials are awarded.',
  cannot_infer:'Template alone does not enroll members or grant access.',status:'published'});
 const linked={instance_id:'craft-sessions-'+key.slice(0,15),template_id:'craft-inspection-v1',
  owner_subject:'community_body:'+key,site_id:key,archetype:'guild-practice',opened_at:'2026-11-05T00:00:00Z',
  cannot_infer:'No enrollment or skill validation',status:'published'};
 f.add('program_instance',linked);
 f.add('program_requirement',{requirement_id:'inspect-bench-'+key.slice(0,15),instance_id:linked.instance_id,
  kind:'evidence',statement:'Document bench inspection before supervised work',owner:'community_body:'+key,
  satisfied_by:'claimed-by-operator',evidence_ref:'PRIVATE-UNVERIFIED-REFERENCE',deadline:'2026-11-25',state:'open',
  cannot_infer:'An unverified reference does not prove work completed.',status:'published'});
 const other=await create('Unrelated community');
 f.add('program_instance',{...linked,instance_id:'unrelated-program-'+key.slice(0,15),owner_subject:'community_body:'+other.key,site_id:other.key});
 const route=path(key,'programs');
 expect((await get(route,null)).status).toBe(303);expect((await get(route,outsider)).status).toBe(403);
 expect((await get(route,member)).status).toBe(403);
 expect((await get(route,owner)).body).toContain('Workshop Inspection and Learning');
 const steward=await get(route,owner);expect(steward.status).toBe(200);
 expect(steward.body).toContain('data-community-programmes');expect(steward.body).toContain('Document bench inspection before supervised work');
 expect(steward.body).toContain('data-guild-role-bridge');
 expect(steward.body).toContain('/roles?scope=community%3A'+key);
 expect(steward.body).toContain('they do not enroll members, grant authority or execute guild operations');
 expect(steward.body).toContain('Retained evidence reference is present');
 expect(steward.body).not.toContain('PRIVATE-UNVERIFIED-REFERENCE');
 expect(steward.body).not.toContain('unrelated-program-');
 expect((await get(path(other.key,'programs'))).status).toBe(400);
 await join(key);expect((await get(route,member)).status).toBe(403);await accept(key);
 const participant=await get(route,member);expect(participant.status).toBe(200);
 expect(participant.body).toContain('Document bench inspection before supervised work');
 expect(participant.body).not.toContain('Retained evidence reference is present');
 expect(participant.body).toContain('Document a workshop observation');
 const before=f.sql.query('SELECT state,satisfied_by,evidence_ref FROM program_requirement WHERE requirement_id=?').get('inspect-bench-'+key.slice(0,15));
 const note=await publish(key,member,{title:'Bench inspection observation',text:'Visual check completed. Review by workshop steward is still needed.'});
 expect((await get(path(key,'thread',note.key),null)).body).toContain('Visual check completed');
 const foreign=await publish(other.key,owner,{title:'Other workshop evidence',text:'A separate community retains its own observation and details.'});
 f.add('program_requirement',{requirement_id:'guild-source-confirmed-'+key.slice(0,15),instance_id:linked.instance_id,
  kind:'evidence',statement:'A workshop note requires independent steward review',owner:'community_body:'+key,
  evidence_ref:'community_post:'+note.key,state:'open',cannot_infer:'A visible note is not proof of competency.',status:'published'});
 f.add('program_requirement',{requirement_id:'guild-source-wrong-'+key.slice(0,15),instance_id:linked.instance_id,
  kind:'evidence',statement:'Cross-community evidence must not be linked',owner:'community_body:'+key,
  evidence_ref:'community_post:'+foreign.key,state:'open',cannot_infer:'Evidence does not cross community authority.',status:'published'});
 const evidencePage=await get(route,member);expect(evidencePage.status).toBe(200);
 expect(evidencePage.body).toContain('data-requirement-source-note');
 expect((evidencePage.body.match(/data-requirement-source-note/g)||[])).toHaveLength(1);
 expect(evidencePage.body).toContain('post='+note.key);
 expect(evidencePage.body).not.toContain(foreign.key);
 expect(evidencePage.body).not.toContain('A separate community retains its own observation');
 expect(evidencePage.body).not.toContain('PRIVATE-UNVERIFIED-REFERENCE');
 expect(evidencePage.body).toContain('completion is not verified');
 const after=f.sql.query('SELECT state,satisfied_by,evidence_ref FROM program_requirement WHERE requirement_id=?').get('inspect-bench-'+key.slice(0,15));
 expect(after).toEqual(before);
 const members=await get(path(key,'members'));expect((await send(form(members.body,'revoke'))).status).toBe(303);
 expect((await get(route,member)).status).toBe(403);
 expect((await get(route,owner)).status).toBe(200);
 expect(f.sql.query('SELECT count(*) n FROM pack_installation').get().n).toBe(0);
});


test('ordinary creation previews before writes, commits founding membership atomically, and replays exactly',async()=>{
 const p=await get('/community/new'),before=f.sql.query('SELECT count(*) AS n FROM community_body').get().n,input={...form(p.body,'create'),title:'Unfamiliar repair circle',text:'Share repair questions. Café & community. <script>not executable</script>'};
 const preview=await send(input,owner,'/community/preview');expect(preview.status).toBe(200);expect(f.sql.query('SELECT count(*) AS n FROM community_body').get().n).toBe(before);expect(preview.body).toContain('&lt;script&gt;');const approved=form(preview.body,'create'),r=await send(approved);expect(r.status).toBe(303);const key=new URL(r.headers.get('location'),'https://aurekai.life').searchParams.get('community'),membership=f.sql.query('SELECT * FROM community_membership WHERE community=?1').all(key);expect(membership).toHaveLength(1);expect(membership[0].member_subject).toBe('user:'+owner.id);expect(membership[0].state).toBe('active');expect((await send(approved)).headers.get('location')).toBe(r.headers.get('location'));expect((await send({...approved,title:'Conflicting use of the same request'})).status).toBe(409);expect(f.sql.query('SELECT count(*) AS n FROM community_body').get().n).toBe(before+1);
 const publicPage=await get(path(key),null);expect(publicPage.body).toContain('Unfamiliar repair circle');expect(publicPage.body).not.toContain('data-community-action="accept"');expect(publicPage.body).not.toContain('community-owner@example.test');
});
test('requests, profile continuation and acceptance concern the exact community without global grants',async()=>{
 const {key}=await create('Participation flow');await join(key);const pending=f.sql.query('SELECT * FROM community_membership WHERE community=?1 AND member_subject=?2').get(key,'user:'+member.id);expect(pending.state).toBe('requested');expect((await get(path(key,'write'),member)).status).toBe(403);
 const mine=await get('/profile?tab=communities',member),other=await get('/profile?tab=communities',outsider);expect(mine.body).toContain(key);expect(mine.body).toContain('request is awaiting review');expect(other.body).not.toContain(key);await accept(key);expect((await get(path(key,'write'),member)).status).toBe(200);expect((await get('/profile?tab=communities',member)).body).toContain('You can take part.');
 const before=row('community_membership','membership',pending.membership);expect((await send({id:key,return:'/community'},member,'/govern/community-join')).status).toBe(303);expect(row('community_membership','membership',pending.membership).state).toBe(before.state);
});
test('conversation replies, revision conflicts and removal preserve other authors and the exact discussion',async()=>{
 const {key}=await create('Conversation flow');await join(key);await accept(key);const rootPost=await publish(key,owner,{text:'Original public question.',title:'What should we repair first?'}),reply=await publish(key,member,{parent:rootPost.key,text:'I can bring a broken lamp.'});
 const page=await get(path(key,'thread',rootPost.key),null);expect(page.body).toContain('Original public question.');expect(page.body).toContain('I can bring a broken lamp.');expect(page.body).not.toContain('community-member@example.test');
 const editPage=await get(path(key,'edit',reply.key),member),edit={...form(editPage.body,'edit'),text:'I can bring a lamp and a safe inspection checklist.'};expect((await send(edit,member)).status).toBe(303);expect((await send(edit,member)).status).toBe(303);expect((await send({...edit,text:'Stale replacement'},member)).status).toBe(409);expect(row('community_post','post',reply.key).body).toBe(edit.text);
 const rootPage=await get(path(key,'thread',rootPost.key)),remove=form(rootPage.body,'remove');expect((await send(remove)).status).toBe(303);const withdrawn=await get(path(key,'thread',rootPost.key),null);expect(withdrawn.body).not.toContain('Original public question.');expect(withdrawn.body).toContain('no longer public');expect(withdrawn.body).toContain(edit.text);expect((await get(path(key,'reply',reply.key),member)).status).toBe(409);expect((await send(rootPost.reviewed)).status).toBe(409);
});
test('membership revocation cannot be escaped through leave/rejoin or unrelated administrator privileges',async()=>{
 const {key}=await create('Scoped authority');await join(key);await accept(key);const plannedPage=await get(path(key,'write'),member),planned={...form(plannedPage.body,'post'),title:'Before access changed',text:'This must not be published.'};const members=await get(path(key,'members'));expect((await send(form(members.body,'revoke'))).status).toBe(303);
 expect((await send(planned,member)).status).toBe(403);const latest=f.sql.query('SELECT * FROM community_membership WHERE community=?1 AND member_subject=?2').get(key,'user:'+member.id);expect(latest.state).toBe('revoked');expect((await send({operation:'leave',community:key,expected:'absent'},member)).status).not.toBe(303);expect((await send({operation:'request',community:key,expected:'absent'},member)).status).not.toBe(303);expect(f.sql.query('SELECT count(*) AS n FROM community_post WHERE community=?1').get(key).n).toBe(0);
 f.grant(outsider.id,'unrelated-test-organization','admin');expect((await get(path(key,'members'),outsider)).status).toBe(403);expect((await send({operation:'accept',community:key,target:'user:'+member.id,expected:'absent'},outsider)).status).toBe(403);
});
test('stewards can hide but cannot rewrite another author’s words; another community stays independent',async()=>{
 const first=await create('First independent community'),second=await create('Second independent community',outsider);await join(first.key);await accept(first.key);const post=await publish(first.key,member,{text:'Member authored text.'});expect((await get(path(first.key,'edit',post.key))).status).toBe(403);const hidden=await get(path(first.key,'thread',post.key));expect((await send(form(hidden.body,'hide'))).status).toBe(303);expect((await get(path(first.key,'thread',post.key),null)).body).not.toContain('Member authored text.');expect(row('community_post','post',post.key).body).toBe('Member authored text.');expect((await get(path(second.key,'members'))).status).toBe(403);expect((await get(path(second.key,'thread',post.key),null)).status).toBe(404);
});
test('origin, fields, private drafts, namespace identity and parent corrections are enforced at commit',async()=>{
 const {key}=await create('Integrity boundaries'),writePage=await get(path(key,'write')),p={...form(writePage.body,'post'),title:'Bounded request',text:'Do not publish on malformed input.'};
 for(const origin of ['null','','https://foreign.example','http://aurekai.life'])expect((await send(p,owner,'/community/action',{Origin:origin})).status).toBe(403);
 for(const extra of [{actor:'user:'+outsider.id},{role:'steward'},{namespace:'other'}])expect((await send({...p,...extra})).status).toBe(403);
 expect((await send(new URLSearchParams(p).toString()+'&community=other')).status).toBe(403);expect((await send(p,null)).status).toBe(401);
 const parent=await publish(key),replyPage=await get(path(key,'reply',parent.key)),reply={...form(replyPage.body,'reply'),text:'Reply to the old version.'},editPage=await get(path(key,'edit',parent.key));expect((await send({...form(editPage.body,'edit'),text:'The parent changed.'})).status).toBe(303);expect((await send(reply)).status).toBe(409);
 f.add('community_post',{post:'private-parent-fixture',community:key,author_subject:'user:'+owner.id,title:'Private draft',body:'PRIVATE-DRAFT-MUST-STAY-HIDDEN',published_at:'2026-10-21T00:00:00Z',visibility:'private',moderation_state:'accepted',cannot_infer:'fixture',status:'published'});expect((await get(path(key,'thread','private-parent-fixture'),null)).status).toBe(404);
});
test('an interrupted creation rolls back membership and does not leave a half-created community',async()=>{
 const page=await get('/community/new'),input={...form(page.body,'create'),title:'Rollback fixture',text:'A complete creation or no creation.'},before=f.sql.query('SELECT count(*) AS n FROM community_body').get().n;
 f.sql.exec("CREATE TRIGGER refuse_fixture_membership BEFORE INSERT ON community_membership BEGIN SELECT RAISE(ABORT,'fixture failure'); END;");try{expect((await send(input)).status).toBe(503);expect(f.sql.query('SELECT count(*) AS n FROM community_body').get().n).toBe(before);}finally{f.sql.exec('DROP TRIGGER refuse_fixture_membership');}
 expect((await send(input)).status).toBe(303);expect(f.sql.query('SELECT count(*) AS n FROM community_body').get().n).toBe(before+1);
});
test('public reads, profile entry and guest sign-in preserve the exact requested continuation',async()=>{
 const {key}=await create('Guest continuation'),target=path(key,'join'),guest=await get(target,null);expect(guest.status).toBe(303);expect(new URL(guest.headers.get('location'),'https://aurekai.life').searchParams.get('next')).toBe(target);
 const profile=await get('/profile?tab=communities',null);expect(new URL(profile.headers.get('location'),'https://aurekai.life').searchParams.get('next')).toBe('/profile?tab=communities');
 const result=await get('/communities?q=Guest',null);expect(result.status).toBe(200);expect(result.body).toContain(key);expect(result.body).not.toContain('Configuration user');
});
async function browserPage(user=null){const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(12000);await page.evaluateOnNewDocument(()=>{window.communityEvents=[];for(const event of ['click','submit','invalid'])document.addEventListener(event,e=>{window.communityEvents.push({event,target:e.target.tagName,text:e.target.textContent.slice(0,90),x:e.clientX,y:e.clientY,action:e.target.action||e.target.closest('form')?.action});window.communityEvents=window.communityEvents.slice(-12);},true);});await page.setViewport({width:1440,height:1000});if(user){const[name,...value]=user.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});}return {context,page,errors};}
const clickText=async(page,text,scope='main')=>{await page.evaluate(({text,scope})=>{const n=[...document.querySelector(scope).querySelectorAll('a,button,summary')].find(n=>n.textContent.trim()===text&&n.getClientRects().length&&!n.disabled);if(!n)throw Error('Missing visible action '+text);n.click();},{text,scope});};
const navigate=async(page,fn)=>{try{await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),fn()]);}catch(error){const state=await page.evaluate(()=>{const n=document.querySelector('[data-community-publish], [data-community-action=request]'),r=n?.getBoundingClientRect(),hit=r?document.elementFromPoint(r.x+r.width/2,r.y+r.height/2):null;return {url:location.href,heading:document.querySelector('main h1')?.textContent,events:window.communityEvents,publish:r?{x:r.x,y:r.y,width:r.width,height:r.height,disabled:n.disabled,hit:hit?.outerHTML.slice(0,300)}:null};}).catch(()=>null);throw Error(error.message+'; community navigation '+JSON.stringify(state));}};
// Locators wait for stable button geometry before dispatching the real pointer action.
test('two real browsers create, discover, join, approve, converse, revise and return to private profiles',async()=>{
 const a=await browserPage(owner),b=await browserPage(member);try{
  await a.page.goto(f.base+'/community/new',{waitUntil:'domcontentloaded'});await a.page.type('input[name=title]','The neighborhood repair circle');await a.page.type('textarea[name=text]','Bring your questions, share what worked, and plan a careful repair together.');await navigate(a.page,()=>a.page.click('form.cj-form button[type=submit]'));expect(await a.page.$eval('main h1',n=>n.textContent)).toBe('Review your public contribution');await navigate(a.page,()=>a.page.locator('[data-community-publish]').click());const community=new URL(a.page.url()).searchParams.get('community');expect(community).toMatch(/^community-[a-f0-9]{64}$/);
  await b.page.goto(f.base+'/communities?q=neighborhood',{waitUntil:'domcontentloaded'});await navigate(b.page,()=>b.page.click('[data-community-listing] h2 a'));await navigate(b.page,()=>clickText(b.page,'Request to join'));await navigate(b.page,()=>b.page.click('[data-community-action=request]'));
  await b.page.goto(f.base+'/profile?tab=communities',{waitUntil:'domcontentloaded'});expect(await b.page.$eval('main',n=>n.textContent)).toContain('Your request is awaiting review.');
  await a.page.goto(f.base+path(community,'members'),{waitUntil:'domcontentloaded'});await navigate(a.page,()=>a.page.locator('[data-community-action=accept]').click());
  await b.page.goto(f.base+path(community),{waitUntil:'domcontentloaded'});await navigate(b.page,()=>clickText(b.page,'Start a conversation'));await b.page.type('input[name=title]','Can we diagnose this lamp together?');await b.page.type('textarea[name=text]','I have not powered it on. What should I inspect first?');await navigate(b.page,()=>b.page.click('.cj-form button[type=submit]'));await navigate(b.page,()=>b.page.locator('[data-community-publish]').click());const post=new URL(b.page.url()).searchParams.get('post');
  await a.page.goto(f.base+path(community,'thread',post),{waitUntil:'domcontentloaded'});await navigate(a.page,()=>clickText(a.page,'Reply'));await a.page.type('textarea[name=text]','Start with a description and photos before anyone proposes electrical work.');await navigate(a.page,()=>a.page.click('.cj-form button[type=submit]'));await navigate(a.page,()=>a.page.locator('[data-community-publish]').click());const reply=new URL(a.page.url()).searchParams.get('post');
  await b.page.reload({waitUntil:'domcontentloaded'});expect(await b.page.$eval('main',n=>n.textContent)).toContain('Start with a description and photos');await navigate(b.page,()=>clickText(b.page,'Edit my contribution'));await b.page.$eval('textarea[name=text]',n=>n.value='I have not powered it on. I will add a clear description first.');await navigate(b.page,()=>b.page.click('.cj-form button[type=submit]'));await navigate(b.page,()=>b.page.locator('[data-community-publish]').click());
  expect(row('community_post','post',post).body).toBe('I have not powered it on. I will add a clear description first.');
  expect(row('community_post','post',reply).body).toBe('Start with a description and photos before anyone proposes electrical work.');
  for(const width of [1440,390]){await b.page.setViewport({width,height:1000});expect(await b.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(await b.page.$$eval('main',n=>n.length)).toBe(1);}
  const output=process.env.BONFYRE_COMMUNITY_SCREENSHOTS;if(output){mkdirSync(output,{recursive:true});await b.page.screenshot({path:pathJoin(output,'community-phone.png'),fullPage:true});await b.page.setViewport({width:1440,height:1000});await b.page.screenshot({path:pathJoin(output,'community-desktop.png'),fullPage:true});}
  await navigate(b.page,()=>clickText(b.page,'Return through your profile'));expect(new URL(b.page.url()).searchParams.get('tab')).toBe('communities');expect(await b.page.$eval('[data-private-community]',n=>n.textContent)).toContain('You can take part.');expect(a.errors).toEqual([]);expect(b.errors).toEqual([]);
 }finally{await a.context.close();await b.context.close();}
},90000);
async function isolatedHTTPS(page,errors){
 // Chrome retains the real Secure/HttpOnly cookies. All four HTTPS hosts
 // terminate at isolated native owners; no fixture credentials leave the test.
 await page.setRequestInterception(true);
 page.on('request',async request=>{try{
  const u=new URL(request.url());if(!/^aurekai\.(work|life|live|shop)$/.test(u.hostname)){await request.respond({status:204,body:''});return;}
  const headers={...request.headers(),Host:u.hostname};delete headers.host;delete headers['content-length'];delete headers['accept-encoding'];
  const result=await fetch(f.base+u.pathname+u.search,{method:request.method(),headers,body:request.postData(),redirect:'manual',signal:AbortSignal.timeout(10000)}),responseHeaders={};
  for(const[key,value]of result.headers)if(!['content-length','content-encoding','transfer-encoding','connection','set-cookie'].includes(key))responseHeaders[key]=value;
  const cookies=result.headers.getSetCookie();if(cookies.length)responseHeaders['set-cookie']=cookies;
  await request.respond({status:result.status,headers:responseHeaders,body:Buffer.from(await result.arrayBuffer())});
 }catch(error){errors.push('Isolated HTTPS transport: '+error.name);await request.abort().catch(()=>{});}});
}
test('real sign-in from a community request returns to that community without creating a membership',async()=>{
 const {key}=await create('Sign-in destination'),{page,context,errors}=await browserPage();try{
  await isolatedHTTPS(page,errors);await page.goto('https://aurekai.life'+path(key,'join'),{waitUntil:'domcontentloaded'});await page.waitForSelector('form[action="/login"]');
  await page.type('form[action="/login"] input[name=email]','community-outsider@example.test');await page.type('form[action="/login"] input[name=password]',password);await navigate(page,()=>page.click('form[action="/login"] button[type=submit]'));
  expect(new URL(page.url()).hostname).toBe('aurekai.life');expect(new URL(page.url()).searchParams.get('community')).toBe(key);expect(new URL(page.url()).searchParams.get('tab')).toBe('join');expect(await page.$('[data-community-action=request]')).not.toBeNull();
  expect(f.sql.query('SELECT count(*) AS n FROM community_membership WHERE community=?1 AND member_subject=?2').get(key,'user:'+outsider.id).n).toBe(0);expect(errors).toEqual([]);
 }finally{await context.close();}
});

test('lost publication acknowledgement is reconciled by the same reviewed request without a second post',async()=>{
 const {key}=await create('Lost reply'),{page,context}=await browserPage(member);let cdp,reviewed,postKey,writes=0;
 try{await join(key);await accept(key);await page.goto(f.base+path(key,'write'),{waitUntil:'domcontentloaded'});await page.type('input[name=title]','One original contribution');await page.type('textarea[name=text]','This text must be retained exactly once.');await navigate(page,()=>page.click('.cj-form button[type=submit]'));reviewed=await page.$eval('form[action="/community/action"]',n=>Object.fromEntries(new FormData(n)));
  cdp=await page.createCDPSession();await cdp.send('Fetch.enable',{patterns:[{urlPattern:'*/community/action',requestStage:'Response'}]});
  cdp.on('Fetch.requestPaused',async e=>{try{if(e.request.method==='POST'&&e.responseStatusCode===303){writes++;await cdp.send('Fetch.failRequest',{requestId:e.requestId,errorReason:'ConnectionClosed'});}else await cdp.send('Fetch.continueResponse',{requestId:e.requestId});}catch{}});
  await Promise.allSettled([page.waitForNavigation({waitUntil:'domcontentloaded',timeout:6000}),page.locator('[data-community-publish]').click()]);await cdp.detach();cdp=null;
  const retained=f.sql.query('SELECT * FROM community_post WHERE community=?1 AND author_subject=?2').all(key,'user:'+member.id);expect(writes).toBe(1);expect(retained).toHaveLength(1);postKey=retained[0].post;
  const repeated=await send(reviewed,member);expect(repeated.status).toBe(303);expect(new URL(repeated.headers.get('location'),'https://aurekai.life').searchParams.get('post')).toBe(postKey);expect(f.sql.query('SELECT count(*) AS n FROM community_post WHERE community=?1').get(key).n).toBe(1);
  await page.goto(f.base+path(key,'thread',postKey),{waitUntil:'domcontentloaded'});expect(await page.$eval('main',n=>n.textContent)).toContain('This text must be retained exactly once.');
 }finally{await cdp?.detach();await context.close();}
});
test('withdrawal before approval and expired membership reject operations prepared under older access',async()=>{
 const {key}=await create('Participation changes');await join(key);const oldReview=await get(path(key,'members')),acceptForm=form(oldReview.body,'accept'),memberPage=await get(path(key),member);expect((await send(form(memberPage.body,'leave'),member)).status).toBe(303);expect((await send(acceptForm)).status).toBe(409);
 await join(key);await accept(key);const editor=await get(path(key,'write'),member),prepared={...form(editor.body,'post'),title:'Expired membership',text:'Must not become public.'};
 const membership=f.sql.query('SELECT * FROM community_membership WHERE community=?1 AND member_subject=?2').get(key,'user:'+member.id);
 f.run(f.cms,['entry','update','community_membership',String(membership.id),JSON.stringify({expires_at:'2020-01-01T00:00:00Z'}),'--ns','root','--db',f.db,'--schemas',pathJoin(f.home,'schemas')]);
 expect((await send(prepared,member)).status).toBe(403);expect(f.sql.query('SELECT count(*) AS n FROM community_post WHERE community=?1').get(key).n).toBe(0);
});
test('publicly flagged drafts and foreign-namespace twins cannot become conversation ancestors',async()=>{
 const {key}=await create('Exact source standing');const base={post:'same-key',community:key,author_subject:'user:'+owner.id,title:'Draft',body:'UNRELEASED-TEXT',published_at:'2026-10-21T00:00:00Z',visibility:'public',moderation_state:'accepted',cannot_infer:'Isolated native fixture.',status:'draft'};f.add('community_post',base);
 f.add('community_post',{...base,body:'FOREIGN-NAMESPACE-TEXT',status:'published'},'other-test');
 expect((await get(path(key,'thread','same-key'),null)).status).toBe(404);
 f.add('community_post',{...base,post:'reply-under-draft',reply_to:'same-key',body:'CHILD-OF-DRAFT',status:'published'});
 expect((await get(path(key,'thread','reply-under-draft'),null)).status).toBe(404);expect((await get(path(key),null)).body).not.toContain('UNRELEASED-TEXT');
});
test('bounded conversation pages preserve exact membership and UTF-8 previews',async()=>{
 const {key}=await create('Paged conversation');for(let i=0;i<27;i++)f.add('community_post',{post:'page-item-'+i,community:key,author_subject:'user:'+owner.id,title:'Conversation '+i,body:'a'.repeat(599)+'é end of excerpt '+i,published_at:'2026-10-21T00:00:00Z',visibility:'public',moderation_state:'accepted',cannot_infer:'Isolated pagination fixture.',status:'published'});
 const first=await get(path(key),null),ids=html=>[...html.matchAll(/data-community-post="([^"]+)"/g)].map(m=>m[1]);expect(ids(first.body)).toHaveLength(24);expect(first.body).not.toContain('\ufffd');const next=decode(first.body.match(/class="cj-next" href="([^"]+)"/)?.[1]||'');expect(next).toContain('cursor=');const second=await get(next,null);expect(ids(second.body)).toHaveLength(3);expect(new Set([...ids(first.body),...ids(second.body)]).size).toBe(27);
});
test('the existing public community entrance links to participant discovery without changing observed records',async()=>{
 f.add('community_body',{community:'legacy-reference',subject_address:'community_body:legacy-reference',title:'An observed group',description:'Retained public source.',visibility:'public',governance_scope:'external-reference',moderation_policy:'external-read-only',created_by_subject:'foreign:publisher',cannot_infer:'No local control or external membership.',status:'published'});
 const before=row('community_body','community','legacy-reference'),page=await get('/community?community=legacy-reference',null);expect(page.status).toBe(200);expect(page.body).toContain('href="/communities"');expect(page.body).toContain('href="/community/new"');expect(row('community_body','community','legacy-reference')).toEqual(before);expect((await get('/communities?q=observed',null)).body).not.toContain('An observed group');
});

test('four territory homes discover exact public conversations without private membership or source leakage',async()=>{
 const {key}=await create('Homepage workshop'),post=await publish(key,owner,{title:'A shared repair question',text:'A public starting point. <script>not executable</script> '+ 'é'.repeat(160)});
 const before=f.sql.query('SELECT * FROM community_post WHERE community=?').all(key);
 const starter={community:key,author_subject:'user:'+owner.id,published_at:new Date(Date.now()-1000).toISOString(),visibility:'public',moderation_state:'accepted',cannot_infer:'Isolated home discovery fixture.',status:'published'};
 for(const [id,patch,ns]of [['hidden',{moderation_state:'hidden'}],['private',{visibility:'private'}],['draft',{status:'draft'}],['future',{published_at:'2099-01-01T00:00:00Z'}],['foreign',{},'other-test'],['reply',{reply_to:post.key}]])f.add('community_post',{...starter,post:'home-'+id,title:'HOME-EXCLUDED-'+id,body:'HOME-EXCLUDED-'+id,...patch},ns||'root');
 for(const host of ['aurekai.life','aurekai.work','aurekai.live','aurekai.shop']){
  const page=await f.request('/',{host});expect(page.status).toBe(200);expect(page.body).toContain('data-community-front');expect(page.headers.get('cache-control')).toContain('must-revalidate');
  const section=page.body.match(/<section class="community-front"[\s\S]*?<\/section>/)?.[0]||'';
  expect(section).toContain('A shared repair question');expect(section).toContain('&lt;script&gt;');expect(section).not.toContain('\ufffd');expect(section).not.toContain('HOME-EXCLUDED-');
  expect(section).not.toContain('community-owner@example.test');expect(section).not.toContain('user:'+owner.id);
  expect((section.match(/data-community-discovery=/g)||[]).length).toBeLessThanOrEqual(3);
  const links=[...section.matchAll(/data-community-thread href="([^"]+)"/g)].map(m=>decode(m[1]));
  const target=links.find(href=>new URL(href,'https://'+host).searchParams.get('community')===key);expect(target).toBeTruthy();
  const thread=await f.request(target,{host});expect(thread.status).toBe(200);expect(thread.body).toContain('A shared repair question');
 }
 expect(f.sql.query('SELECT * FROM community_post WHERE post=? AND community=?').all(post.key,key)).toEqual(before);
 const cached=await f.request('/',{host:'aurekai.life'}),etag=cached.headers.get('etag');expect(etag).toBeTruthy();
 const cache=new Database(pathJoin(f.home,'public-projection.db'),{readonly:true});try{expect(cache.query("SELECT count(*) AS n FROM public_projection WHERE closure_key LIKE 'public-page|aurekai.life|/%'").get().n).toBeGreaterThan(0);}finally{cache.close();}
 const unchanged=await f.request('/',{host:'aurekai.life',headers:{'If-None-Match':etag}});expect(unchanged.status).toBe(304);expect(unchanged.headers.get('cache-control')).toContain('must-revalidate');
 expect((await send(form((await get(path(key,'thread',post.key))).body,'remove'))).status).toBe(303);
 expect((await f.request('/',{host:'aurekai.life',headers:{'If-None-Match':etag}})).status).toBe(200);
 expect((await f.request('/',{host:'aurekai.life'})).body).not.toContain('A shared repair question');
 f.run(f.web,['domain','--db',pathJoin(f.home,'matrix.db'),'--host','home-reader.aurekai.life','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence','fixture:community-home']);
 expect((await f.request('/',{host:'home-reader.aurekai.life'})).body).not.toContain('data-community-front');
});

test('homepage conversation discovery works without script and continues through the real community journey',async()=>{
 const {key}=await create('Home discovery browser'),post=await publish(key,owner,{title:'Plan a shared workshop',text:'Choose a project together and keep the discussion with its community.'});
 const {page,context,errors}=await browserPage();try{
  await isolatedHTTPS(page,errors);await page.setJavaScriptEnabled(false);
  for(const width of [1440,390]){
   await page.setViewport({width,height:1000});await page.goto('https://aurekai.life/',{waitUntil:'domcontentloaded'});await page.waitForSelector('[data-community-front]');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   expect(await page.$$eval('[data-community-discovery]',nodes=>nodes.length)).toBeLessThanOrEqual(3);
   const output=process.env.BONFYRE_COMMUNITY_SCREENSHOTS;if(output){mkdirSync(output,{recursive:true});await page.$eval('[data-community-front]',n=>n.scrollIntoView());await page.screenshot({path:pathJoin(output,'community-home-'+width+'.png')});}
  }
  await navigate(page,()=>page.click('[data-community-discovery="'+key+'"] [data-community-thread]'));
  expect(new URL(page.url()).searchParams.get('post')).toBe(post.key);expect(await page.$eval('main',n=>n.textContent)).toContain('Plan a shared workshop');
  await navigate(page,()=>clickText(page,'Sign in to take part'));
  expect(new URL(page.url()).pathname).toBe('/login');
  await page.type('form[action="/login"] input[name=email]','community-outsider@example.test');await page.type('form[action="/login"] input[name=password]',password);await navigate(page,()=>page.click('form[action="/login"] button[type=submit]'));
  expect(new URL(page.url()).hostname).toBe('aurekai.life');expect(new URL(page.url()).searchParams.get('community')).toBe(key);expect(new URL(page.url()).searchParams.get('tab')).toBe('join');
  expect(f.sql.query('SELECT count(*) AS n FROM community_membership WHERE community=?').get(key).n).toBe(1);expect(errors).toEqual([]);
 }finally{await context.close();}
});

test('a community made private withdraws its conversation from a warm homepage',async()=>{
 const {key}=await create('Discovery privacy change');await publish(key,owner,{title:'Privacy-change conversation',text:'Words withdrawn with their parent community.'});
 const first=await f.request('/',{host:'aurekai.life'});expect(first.body).toContain('Privacy-change conversation');
 const community=row('community_body','community',key);
 f.run(f.cms,['entry','update','community_body',String(community.id),JSON.stringify({visibility:'private'}),'--ns','root','--db',f.db,'--schemas',pathJoin(f.home,'schemas')]);
 for(const headers of [{},{'If-None-Match':first.headers.get('etag')}]){
  const result=await f.request('/',{host:'aurekai.life',headers});expect(result.status).toBe(200);expect(result.body).not.toContain('Privacy-change conversation');expect(result.body).not.toContain('Words withdrawn with their parent community.');
 }
 expect((await get(path(key),null)).status).toBe(404);
});
