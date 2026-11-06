// The same retained ApplicationBinding supplies rich cards and its exact sub-editors.
// Uses isolated native CMS/Auth/Matrix owners, never writes the public installation.
import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {projectInstrumentsWorkspaceSetup,projectCreatorWorkspaceSetup,publishSetups} from '../../../scripts/publish_experience_setups.mjs';import {materializeCreatorWorkspace} from '../../../scripts/materialize_creator_workspace.mjs';import {materializeSourceLibrary,sourceLibrarySetup,sourceLibraryPack} from '../../../scripts/materialize_source_library.mjs';import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../../../scripts/materialize_authored_workspace.mjs';
import {instrumentsPackage,INSTRUMENTS} from '../../../components/BonfyreServiceSite/instruments-package.mjs';

const previous={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
let home,f,release,binding,browser,creatorPackage,sourcePackage,researchPackage,audioBinding,sourceBinding,researchBinding;
const context='institution:workspace-team',pack='experience.skill-instruments.workspace';
const COLLECTIONS=['process-model:','review-project:','schema-plan:','plan:','price-work:','opportunity:'];
const restore=(key,value)=>{if(value===undefined)delete process.env[key];else process.env[key]=value;};

beforeAll(async()=>{
 home=mkdtempSync(join(tmpdir(),'bonfyre-subtools-'));
 const packageRoot=join(home,'packages'),retained=await instrumentsPackage({packageRoot});creatorPackage=await materializeCreatorWorkspace({root:packageRoot});sourcePackage=await materializeSourceLibrary({root:packageRoot});researchPackage=await materializeAuthoredWorkspace({root:packageRoot});
 release=retained.id;process.env.BONFYRE_WORKSPACE_RELEASE=release;process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;
 f=await workspaceFixture({beforeServe:({home,env,run,cms,db})=>{const archive=join(home,'items.db'),schemas=join(home,'schemas');run('/usr/bin/sqlite3',[archive,"CREATE TABLE item(source TEXT,key TEXT,kind TEXT,family TEXT,title TEXT,body TEXT,href TEXT,image TEXT,by TEXT,publisher TEXT,ident TEXT,at INTEGER,first_read INTEGER); INSERT INTO item VALUES('source-one','bbbbbbbbbbbbbbbbbbbbbbbb','publication','people','Alex mention','Fixture source-bound observation.','https://source-one.test/item','','Alex','Source One','source-one-alex',1792930000,1792930000); INSERT INTO item VALUES('feed-jobs-fixture','aaaaaaaaaaaaaaaaaaaaaaaa','publication','jobs','Lithic: Senior Software Engineer, Product Platform','Lithic is hiring a senior product-platform software engineer.','https://jobs.example.test/lithic','','','Fixture Jobs','lithic-job',1792930001,1792930001);"]);run(cms,['entry','create','internet_source',JSON.stringify({family:'jobs',source:'feed-jobs-fixture',label:'Fixture jobs feed',says:'Fixture job listings',url:'https://jobs.example.test/feed',twin:'fixture-jobs-twin',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'text',front_door:'yes'}),'--ns','root','--db',db,'--schemas',schemas]);env.BONFYRE_ITEM_ARCHIVE_DB=archive;env.BONFYRE_NARRATE_BINARY=join(root,'cmd/BonfyreNarrate/bonfyre-narrate');}});
 f.pack('experience.native-creator-studio','Audio Studio');const args={records:[...projectInstrumentsWorkspaceSetup(release),...projectCreatorWorkspaceSetup(creatorPackage.id),...sourceLibrarySetup(sourcePackage.id),...authoredWorkspaceSetup(researchPackage.id)],binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true};
 expect(publishSetups(args).created).toBe(37);
 expect(publishSetups(args).created).toBe(0);
 binding=await f.install(pack,context);
},120000);

afterAll(async()=>{
 await browser?.close();await f?.close();
 restore('BONFYRE_WORKSPACE_RELEASE',previous.release);restore('BONFYRE_WORKSPACE_PACKAGES',previous.packages);
 if(home)rmSync(home,{recursive:true,force:true});
});

test('private cards reuse the verified editor release and expose six exact sub-editor addresses',async()=>{
 const uri='/apps?context=institution%3Aworkspace-team';
 const work=await f.request(uri,{cookie:f.owner.cookie});
 const cards=await f.request('/apps/_cards?context=institution%3Aworkspace-team',{cookie:f.owner.cookie});
 expect(work.status).toBe(200);expect(cards.status).toBe(200);
 expect(cards.headers.get('cache-control')).toContain('private, no-store');
 for(const response of [work,cards]){
  for(const needle of ['class="pack-subtools"','Continue saved work','Explore 6 working tools','class="pack-subtool-list"','class="pack-collections"','Workspace family <code>skill-instruments</code>'])expect(response.body).toContain(needle);
  const hrefs=[...response.body.matchAll(/data-instrument-entry href="([^"]+)"/g)].map(m=>m[1]);
  expect(hrefs.length).toBe(INSTRUMENTS.length);
  expect(hrefs).toEqual(INSTRUMENTS.map(it=>'/apps/'+binding.id+'/'+it.page+'/'));
  const collections=[...response.body.matchAll(/data-work-collection href="([^"]+)"/g)].map(m=>m[1]);
  expect(collections.length).toBe(COLLECTIONS.length);for(const prefix of COLLECTIONS)expect(collections).toContain('/apps/'+binding.id+'/_work?prefix='+encodeURIComponent(prefix));
  expect(response.body).not.toContain('__WORKSPACE__');
 }
 expect((await f.request('/apps/_cards?context=institution%3Aworkspace-team',{cookie:f.other.cookie})).status).toBe(403);
 const collection=await f.request('/apps/'+binding.id+'/_work?prefix=opportunity%3A',{cookie:f.owner.cookie});expect(collection.status).toBe(200);expect(collection.body).toContain('data-work-collection-scope');expect(collection.body).toContain('<code>opportunity:</code>');const relationship=await f.request('/relationships?context=institution%3Aworkspace-team&who=Alex&source=source-one',{cookie:f.owner.cookie});expect(relationship.status).toBe(200);for(const needle of ['Source-bound observation','This does not assert that equal names identify one person','data-relationship-collection','/apps/'+binding.id+'/_work?prefix=opportunity%3A','/s/apps/crm','Compiled semantics, not this account'])expect(relationship.body).toContain(needle);expect((await f.request('/relationships?context=institution%3Aworkspace-team',{cookie:f.owner.cookie})).status).toBe(200);expect((await f.request('/relationships?context=institution%3Aworkspace-team&who=Nobody&source=source-one',{cookie:f.owner.cookie})).status).toBe(404);expect((await f.request('/relationships?context=institution%3Aworkspace-team&who=Alex&source=source-one')).status).toBe(303);
 expect((await f.request('/apps/'+binding.id+'/_work?prefix=not-declared%3A',{cookie:f.owner.cookie})).status).toBe(400);
 audioBinding=await f.install('experience.native-creator-studio.workspace',context);sourceBinding=await f.install(sourceLibraryPack,context);researchBinding=await f.install('experience.connected-instruments.workspace',context);const allCards=await f.request('/apps/_cards?context='+encodeURIComponent(context),{cookie:f.owner.cookie}),incoming=await f.request('/apps/'+researchBinding.id+'/_work?prefix=draft%3Ainstruments%3Aincoming%3A',{cookie:f.owner.cookie});expect(allCards.body).toContain('href="/apps/'+researchBinding.id+'/instruments/" data-collection-kind="supporting"');expect(allCards.body).toContain('<code>draft:instruments:incoming:</code><small>Open owning editor</small>');expect(allCards.body).toContain('href="/apps/'+audioBinding.id+'/_work?prefix=draft%3Acreator%3A" data-collection-kind="saved-work"');expect(allCards.body).toContain('href="/apps/'+researchBinding.id+'/_work?prefix=draft%3Ainstruments%3Aresearch%3A" data-collection-kind="saved-work"');expect(incoming.status).toBe(200);expect(incoming.body).toContain('data-work-supporting');expect(incoming.body).toContain('data-work-owner href="/apps/'+researchBinding.id+'/instruments/"');expect(incoming.body).not.toContain('No saved items');expect((await f.request('/apps/'+researchBinding.id+'/_work?prefix=draft%3Ainstruments%3Aincoming%3A',{cookie:f.other.cookie})).status).toBe(403);expect((await f.request('/apps/_cards?context=institution%3Aworkspace-team')).status).toBe(303);const board=await f.request('/work-board?context=institution%3Aworkspace-team&task=account-fixture',{cookie:f.owner.cookie});expect(board.status).toBe(200);expect(board.body).toMatch(/import\{[^}]*\breadTasks\b[^}]*\}from['\"]\/apps\/aurekai-activity-source\.mjs['\"]/);for(const needle of ['data-work-board','data-context="institution:workspace-team"','data-task="account-fixture"','data-board-phase="pending"','data-board-phase="active"','data-board-phase="attention"','data-board-phase="completed"',"/apps/aurekai-activity-source.mjs",'/board','Public boards'])expect(board.body).toContain(needle);expect((await f.request('/work-board?context=institution%3Aworkspace-team')).status).toBe(303);const item=await f.request('/o/feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa',{cookie:f.owner.cookie});expect(item.status).toBe(200);expect(item.body).toContain('Use in your work');const feed=await f.request('/feed-use?context=institution%3Aworkspace-team&item=feed-jobs-fixture%2Faaaaaaaaaaaaaaaaaaaaaaaa',{cookie:f.owner.cookie});expect(feed.status).toBe(200);for(const needle of ['data-feed-use','Lithic: Senior Software Engineer, Product Platform','data-mode="relationship"','data-mode="source-library"','data-mode="narration"','data-mode="research"','Review relationship observation','Save source material','Render narration','Prepare reviewed research','data-prefix="opportunity:"','data-prefix="source-material:"','data-prefix="draft:creator:"','data-prefix="draft:instruments:research:"','context=institution:workspace-team','binding='+researchBinding.id])expect(feed.body).toContain(needle);expect((await f.request('/feed-use?context=institution%3Aworkspace-team&item=feed-jobs-fixture%2Faaaaaaaaaaaaaaaaaaaaaaaa')).status).toBe(303);expect((await f.request('/feed-use?context=institution%3Aworkspace-team&item=feed-jobs-fixture%2Faaaaaaaaaaaaaaaaaaaaaaaa',{cookie:f.other.cookie})).status).toBe(403);
 const iconByMode=Object.fromEntries([...feed.body.matchAll(/<article class="feed-use-card" data-feed-target data-mode="([^"]+)"[^>]*><h2>(<svg[\s\S]*?<\/svg>)/g)].map(m=>[m[1],m[2]]));
 expect(Object.keys(iconByMode).sort()).toEqual(['narration','relationship','research','source-library']);
 expect(new Set(Object.values(iconByMode)).size).toBe(4);
},60000);

test('every source-qualified maker profile uses the shared public member shell without fabricated Auth ownership',async()=>{
 const by='Alex Rivera',source='maker-source-fixture',key='dddddddddddddddddddddddd';
 f.run('/usr/bin/sqlite3',[join(f.home,'items.db'),"INSERT INTO item VALUES('maker-source-fixture','dddddddddddddddddddddddd','publication','news','A credited article','Public source item.','https://publisher.example.test/story','https://publisher.example.test/art.jpg','Alex Rivera','News Fixture','credit-fixture',1792930000,1792930000);"]);
 const {createHash}=await import('node:crypto');
 const suffix=createHash('sha256').update(by+'\u0000'+source).digest('hex').slice(0,20);
 const host='credited-alex-rivera-'+suffix+'.aurekai.work';
 const uri='/people?who='+encodeURIComponent(by)+'&source='+encodeURIComponent(source)+'&profile=1';
 const response=await f.request(uri,{host:'aurekai.work'});
 expect(response.status).toBe(200);expect(response.body).toContain('data-profile-template="organization-subprofile-v1"');
 expect(response.body).toContain('data-profile-site="'+host+'"');
 for(const id of ['overview','posts','gallery','about','communities','organizations']){
   expect(response.body.includes('id="'+id+'"') || response.body.includes("id='"+id+"'")).toBe(true);expect(response.body).toContain('href="#'+id+'"');
 }
 expect(response.body).toContain('data-credit-posts');expect(response.body).toContain('A credited article');
 expect(response.body).toContain('data-credit-gallery');expect(response.body).toContain('data-credit-publication');
  expect(response.body).toContain('class="ph-credit-media" src="https://publisher.example.test/art.jpg"');
  expect(response.body).toContain('data-publication-continuation');
  expect(response.body).toContain('href="https://signin.aurekai.work/feed-use?context=self&amp;item=maker-source-fixture%2Fdddddddddddddddddddddddd"');
  expect(response.body).toContain('Use in installed apps');
  expect(response.body).toContain('href="https://signin.aurekai.work/relationships?context=self&amp;who=Alex%20Rivera&amp;source=maker-source-fixture"');
  expect(response.body).toContain('Keep track of this source credit');
 expect(response.body).toContain('not a verified person, employment record, login, consent or editorial permission');
 expect(response.body).toContain('data-subprofile-parent-subject="institution:aurekai:work"');
 expect(response.body).toContain('data-subprofile-parent-host="aurekai.work"');
 for(const privateText of ['Edit this profile','Write a post','Owner private bookmark','Your communities'])expect(response.body).not.toContain(privateText);
 expect((await f.request('/people?who=Alex%20Rivera&source=other&profile=1',{host:'aurekai.work'})).status).toBe(404);
 f.run('/usr/bin/sqlite3',[join(f.home,'items.db'),"INSERT INTO item VALUES('feed-jobs-fixture','ffffffffffffffffffffffff','publication','news','Publisher self credit','Source description is not a person.','https://jobs.example.test/own','','Fixture jobs feed','Fixture jobs feed','self-credit',1792930003,1792930003);"]);
 const sourceSelf=await f.request('/people?who=Fixture%20jobs%20feed&source=feed-jobs-fixture&profile=1',{host:'aurekai.work'});
 expect(sourceSelf.status).toBe(404);
 f.run('/usr/bin/sqlite3',[join(f.home,'items.db'),"INSERT INTO item VALUES('maker-source-fixture','eeeeeeeeeeeeeeeeeeeeeeee','publication','news','Publisher named as author','Do not infer a maker.','https://publisher.example.test/own','','News Fixture','News Fixture','global-self-credit',1792930004,1792930004);"]);
 const publisherSelf=await f.request('/people?who=News%20Fixture&source=maker-source-fixture&profile=1',{host:'aurekai.work'});
 expect(publisherSelf.status).toBe(404);
});
test('relationship intake preserves exact source evidence under current workspace authority',async()=>{
 const ref='source-one/bbbbbbbbbbbbbbbbbbbbbbbb',path='/apps/'+binding.id+'/_relationship-observation',archive=join(f.home,'items.db');
 f.add('internet_source',{family:'people',source:'source-one',label:'Source One',says:'Retained credits',url:'https://source-one.test/feed',twin:'source-one-twin',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'text',front_door:'yes'});
 const review=async(user=f.owner)=>f.request(path+'?item='+encodeURIComponent(ref),{cookie:user.cookie});
 const fields=html=>Object.fromEntries([...html.matchAll(/<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g)].map(m=>[m[1],m[2]]));
 const save=(form,user=f.owner,headers={})=>f.request(path,{cookie:user.cookie,form,headers:{Origin:'https://aurekai.life',...headers}});
 const opened=await review(),form=fields(opened.body);expect(opened.status).toBe(200);expect(opened.body).toContain('Source credit: <strong>Alex');expect(form.snapshot).toMatch(/^[a-f0-9]{64}$/);
 const rel=await f.request('/relationships?context='+encodeURIComponent(context)+'&who=Alex&source=source-one',{cookie:f.owner.cookie});expect(rel.body).toContain('data-relationship-review');
 expect((await save({...form,who:'Someone else'})).status).toBe(400);expect((await save({...form,item:'source-one/cccccccccccccccccccccccc'})).status).toBe(404);
 expect((await save({...form,snapshot:'0'.repeat(64)})).status).toBe(409);expect((await save({...form,actor:'user:other'})).status).toBe(409);expect((await save({...form,release:'0'.repeat(64)})).status).toBe(409);
 expect((await save(form,f.other)).status).toBe(403);expect((await save(form,f.reader)).status).toBe(403);expect((await save(form,f.owner,{Origin:'https://foreign.test'})).status).toBe(403);
 const writerForm=fields((await review(f.writer)).body);f.grant(f.writer.id,'workspace-team','read');expect((await save(writerForm,f.writer)).status).toBe(403);f.grant(f.writer.id,'workspace-team','write');
 f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET by='Someone else' WHERE source='source-one'"]);expect((await save(form)).status).toBe(409);f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET by='Alex' WHERE source='source-one'"]);
 f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET body='A changed excerpt.' WHERE source='source-one'"]);expect((await save(form)).status).toBe(409);f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET body='Fixture source-bound observation.' WHERE source='source-one'"]);
 const declaration=f.sql.query("SELECT id,transition_conditions FROM capability_program WHERE program_id='feed.jobs.relationship'").get();
 f.sql.query('UPDATE capability_program SET transition_conditions=? WHERE id=?').run('{}',declaration.id);expect((await review()).status).toBe(409);expect((await save(form)).status).toBe(409);f.sql.query('UPDATE capability_program SET transition_conditions=? WHERE id=?').run(declaration.transition_conditions,declaration.id);
 expect(await f.store(binding).readLocal('opportunity:actor:')).toHaveLength(0);const saved=await save(form);expect(saved.status).toBe(200);expect(saved.body).toContain('data-observation-saved');
 let rows=await f.store(binding).readLocal('opportunity:actor:');expect(rows).toHaveLength(1);expect(rows[0].revision).toBe(1);expect(rows[0].value.state.relationship).toMatchObject({actor:{state:'unresolved',subject:null},source:{itemRef:ref,authorName:'Alex',source:'source-one',snapshot:form.snapshot,evidenceKind:'retained-source-credit'}});
 expect((await save(form)).status).toBe(200);expect((await f.store(binding).readLocal('opportunity:actor:'))[0].revision).toBe(1);
 f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET body='A later source excerpt.' WHERE source='source-one'"]);const newer=await review();expect(newer.status).toBe(200);expect(newer.body).toContain('The excerpt below is the current retained source item.');expect(newer.body).toContain(form.snapshot);expect(newer.body).toContain('Its saved evidence and later edits are unchanged.');expect((await save(form)).status).toBe(409);f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET body='Fixture source-bound observation.' WHERE source='source-one'"]);
 rows=await f.store(binding).readLocal('opportunity:actor:');expect(rows[0].value.state.relationship.source.summary).toBe('Fixture source-bound observation.');f.run('/usr/bin/sqlite3',[archive,"UPDATE item SET by='Fixture Publisher' WHERE source='feed-jobs-fixture'"]);const job=await f.request(path+'?item='+encodeURIComponent('feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa'),{cookie:f.owner.cookie});expect(job.status).toBe(200);expect(job.body).not.toContain('&amp;who=Fixture');const jobSaved=await save(fields(job.body));expect(jobSaved.status).toBe(200);const jobRows=await f.store(binding).readLocal('opportunity:feed:');expect(jobRows).toHaveLength(1);expect(jobRows[0].value.state.relationship.source.evidenceKind).toBe('retained-source-item');
},60000);

test('public retained source A2A results continue into current installed application admission',async()=>{
 const archive=join(f.home,'items.db');
 f.run('/usr/bin/sqlite3',[archive,"INSERT INTO item SELECT source,'legacy-key','publication',family,title,body,href,image,by,publisher,ident,at+100,first_read FROM item WHERE source='feed-jobs-fixture' LIMIT 1"]);
 const invoke=async(skill,params={})=>{const response=await f.raw('/a2a',null,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:'intake',method:'message/send',params:{skill,...params}})});expect(response.status).toBe(200);const body=await response.json();expect(body.result.execution).toBe('free');return body.result.data;};
 const listed=await invoke('retained-sources');expect(listed.sources.map(x=>x.source)).toContain('feed-jobs-fixture');
 const latest=await invoke('retained-latest',{source:'feed-jobs-fixture'});expect(latest.items.length).toBeGreaterThan(0);expect(latest.items.every(it=>/^[0-9a-f]{24}$/.test(it.key))).toBe(true);expect(Number(latest.next_cursor)).toBeGreaterThanOrEqual(latest.items.length+1);
 const row=latest.items.find(it=>it.key==='aaaaaaaaaaaaaaaaaaaaaaaa');expect(row).toBeDefined();expect(row.continuePath).toBe('/feed-use?context=self&item=feed-jobs-fixture%2Faaaaaaaaaaaaaaaaaaaaaaaa');
 const exact=await invoke('retained-item',{source:'feed-jobs-fixture',key:row.key});expect(exact.continuePath).toBe(row.continuePath);expect(exact.title).toBe(row.title);
 const browser=await f.request(row.continuePath,{cookie:f.owner.cookie});expect(browser.status).toBe(200);expect(browser.body).toContain('data-feed-use');
},60000);

test('Chrome opens the real planner from the sub-editor menu on desktop and mobile',async()=>{
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 const contextChrome=await browser.createBrowserContext(),page=await contextChrome.newPage();
 page.setDefaultTimeout(20000);
 const [name,...token]=f.owner.cookie.split('=');
 await page.setCookie({name,value:token.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 try{
  for(const width of [1440,390]){
   await page.setViewport({width,height:920});
   await page.goto(f.base+'/apps?context=institution%3Aworkspace-team',{waitUntil:'networkidle0'});const skill='.pack-tool:has(a[data-work-editor][href="'+binding.path+'"])';
   await page.waitForSelector(skill+' .pack-subtools');
   await page.waitForSelector(skill+' .pack-collections');
   expect(await page.$eval('.pack-cards',node=>getComputedStyle(node).display)).toBe('grid');
   await page.click(skill+' .pack-subtools summary');
   expect(await page.$eval(skill+' .pack-subtools',n=>n.open)).toBe(true);
   expect(await page.$$eval(skill+' [data-instrument-entry]',n=>n.length)).toBe(INSTRUMENTS.length);
   await page.click(skill+' .pack-collections summary');expect(await page.$eval(skill+' .pack-collections',n=>n.open)).toBe(true);
   expect(await page.$$eval(skill+' [data-work-collection]',n=>n.length)).toBe(COLLECTIONS.length);
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await page.goto(f.base+'/apps/'+binding.id+'/_work?prefix=opportunity%3A',{waitUntil:'domcontentloaded'});
   await page.waitForSelector('[data-work-collection-scope]');expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await page.waitForSelector('[data-work-collection-scope]');expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.goto(f.base+'/work-board?context=institution%3Aworkspace-team&task=account-fixture',{waitUntil:'domcontentloaded'});await page.waitForSelector('[data-work-board]');expect(await page.$$eval('[data-board-phase]',n=>n.length)).toBe(5);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const feedPath='/feed-use?context=institution%3Aworkspace-team&item=feed-jobs-fixture%2Faaaaaaaaaaaaaaaaaaaaaaaa';await page.goto(f.base+feedPath,{waitUntil:'networkidle0'});await page.waitForSelector('[data-feed-target][data-mode="relationship"]');expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.click('[data-feed-target][data-mode="relationship"] [data-feed-save]');await page.waitForSelector('[data-observation-form], [data-observation-saved]');if(await page.$('[data-observation-form]')){await page.setViewport({width:390,height:920});await page.screenshot({path:join(home,'relationship-review-phone.png'),fullPage:true});await page.setViewport({width,height:920});await page.click('[data-observation-form] button');await page.waitForSelector('[data-observation-saved]');}await page.goto(f.base+feedPath,{waitUntil:'networkidle0'});if(width===1440){await page.click('[data-feed-target][data-mode="source-library"] [data-feed-save]');await page.waitForFunction(()=>/Saved to the source library|already in the source library/.test(document.querySelector('[data-feed-target][data-mode="source-library"] [data-feed-result]')?.textContent||''));f.run('/usr/bin/sqlite3',[join(f.home,'items.db'),"INSERT INTO item VALUES('feed-jobs-fixture','cccccccccccccccccccccccc','publication','jobs','Lithic: Senior Backend Engineer','Another retained item sharing the same canonical publisher URL.','https://jobs.example.test/lithic','','','Fixture Jobs','lithic-job-two',1792930002,1792930002);"]);await page.goto(f.base+'/feed-use?context=institution%3Aworkspace-team&item=feed-jobs-fixture%2Fcccccccccccccccccccccccc',{waitUntil:'networkidle0'});await page.click('[data-feed-target][data-mode="source-library"] [data-feed-save]');await page.waitForFunction(()=>/Saved another feed item to this source/.test(document.querySelector('[data-feed-target][data-mode="source-library"] [data-feed-result]')?.textContent||''));await page.goto(f.base+feedPath,{waitUntil:'networkidle0'});await page.click('[data-feed-target][data-mode="narration"] [data-feed-narrate]');await page.waitForFunction(()=>/WAV rendered/.test(document.querySelector('[data-feed-target][data-mode="narration"] [data-feed-result]')?.textContent||''),{timeout:30000});expect(await page.$eval('[data-feed-target][data-mode="narration"] [data-feed-audio]',n=>!n.hidden&&n.src.startsWith('blob:'))).toBe(true);const researchHref=await page.$eval('[data-feed-target][data-mode="research"] [data-feed-research]',n=>n.getAttribute('href'));expect(researchHref).toContain('context=institution:workspace-team');expect(researchHref).toContain('binding='+researchBinding.id);await page.goto(f.base+researchHref,{waitUntil:'domcontentloaded'});await page.waitForSelector('dialog[data-source-work][data-reading="true"]',{timeout:30000});expect(await page.$eval('dialog[data-source-work] select[aria-label="Work belongs to"]',n=>n.value)).toBe(context);expect(await page.$eval('dialog[data-source-work] select[aria-label="Receiving research workspace"]',n=>n.value)).toBe(researchBinding.id);await page.waitForSelector('dialog[data-source-work] input[type="checkbox"]:checked');}await page.goto(f.base+'/relationships?context=institution%3Aworkspace-team',{waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('[data-relationship-records]')?.textContent.includes('Lithic: Senior Software Engineer, Product Platform'));expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.goto(f.base+'/relationships?context=institution%3Aworkspace-team&who=Alex&source=source-one',{waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('[data-relationship-records]')?.textContent.includes('Alex'));expect(await page.$eval('[data-relationship-records]',n=>n.textContent)).not.toContain('Lithic:');await page.goto(f.base+'/apps/'+binding.id+'/plan/',{waitUntil:'domcontentloaded'});
   await page.waitForSelector('[data-plan]');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  }
 const rels=await f.store(binding).readLocal('opportunity:feed:'),sources=await f.store(sourceBinding).readLocal('source-material:'),audio=await f.store(audioBinding).readLocal('draft:creator:'),research=await f.store(researchBinding).readLocal('draft:instruments:research:');expect(rels).toHaveLength(1);expect(rels[0].value.state.relationship).toMatchObject({schema:'bonfyre.relationship.v1',state:'observed',label:'Lithic: Senior Software Engineer, Product Platform',actor:{state:'unresolved',subject:null},source:{itemRef:'feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa',family:'jobs',kind:'text',evidenceKind:'retained-source-item'},opportunity:{kind:'job-opening',stage:'observed'}});expect(sources).toHaveLength(1);expect(sources[0].revision).toBe(2);expect(sources[0].value.feedOrigins).toHaveLength(1);expect(sources[0].value.feedOrigins[0].itemRef).toBe('feed-jobs-fixture/cccccccccccccccccccccccc');expect(sources[0].value.provenance).toMatchObject({schema:'bonfyre.feed-handoff.v1',itemRef:'feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa',source:'feed-jobs-fixture',family:'jobs',kind:'text'});expect(audio).toHaveLength(0);expect(research).toHaveLength(0);}finally{await contextChrome.close();}
},90000);
