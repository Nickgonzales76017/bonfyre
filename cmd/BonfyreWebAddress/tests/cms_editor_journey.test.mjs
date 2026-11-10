import {test,expect} from 'bun:test';
import {copyFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {packSetupFixture,root,password} from './pack_setup_fixture.mjs';

async function setup(){
 const f=await packSetupFixture(),schemas=join(f.home,'schemas');
 for(const type of ['wiki_page','course_lesson'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 f.run(f.auth,['--db',f.authdb,'signup','--email','cms-editor@example.test','--name','Editorial fixture','--password',password,'--tier','pro']);
 const writer=await f.login('cms-editor@example.test',{signup:false});
 f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','CANDIDATE','--repr','HTML','--evidence','fixture:cms-browser']);
 return {...f,writer,schemas};
}

test('native CMS editor snapshots cover every field and stale writes cannot replace a later save',async()=>{
 const f=await setup();try{
  const fresh=await f.request('/cms/parts/edit?type=wiki_page&id=new',{cookie:f.writer.cookie});
  expect(fresh.body).toContain('name="title" value="" required');expect(fresh.body).toContain('name="content" required');
  const saved=await f.request('/cms/v1/api/wiki_page',{cookie:f.writer.cookie,form:{title:'A real second input',route:'notes/cms-review',content:'Original text',status:'draft'}});
  expect(saved.body).toContain('cms-status ok');
  const ns='actor-'+f.writer.id,row=f.sql.query('SELECT * FROM wiki_page WHERE namespace=?').get(ns);
  expect(row.content).toBe('Original text');
  const opened=await f.request('/cms/parts/edit?type=wiki_page&id='+row.id,{cookie:f.writer.cookie});
  const digest=opened.body.match(/name="_reviewed_digest" value="([a-f0-9]{64})"/)?.[1];expect(digest).toHaveLength(64);
  f.run(f.cms,['entry','update','wiki_page',String(row.id),JSON.stringify({meta_description:'Changed by another editor'}),'--ns',ns,'--db',f.db,'--schemas',f.schemas]);
  const stale=await f.request('/cms/v1/api/wiki_page/'+row.id,{cookie:f.writer.cookie,method:'PUT',form:{content:'Stale replacement',_reviewed_digest:digest}});
  expect(stale.body).toContain('Someone changed this record');expect(f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(row.id).content).toBe('Original text');
  const other=await f.login('cms-reader-other@example.test');
  expect((await f.request('/cms/parts/edit?type=wiki_page&id='+row.id,{cookie:other.cookie})).body).not.toContain('Original text');
  expect((await f.request('/cms/parts/edit?type=wiki_page&id=999999',{cookie:f.writer.cookie})).body).not.toContain('<form');
  const native=Bun.spawnSync([f.cms,'entry','update','wiki_page',String(row.id),'{"content":"old native writer"}','--ns',ns,'--db',f.db,'--schemas',f.schemas,'--if-digest',digest]);
  expect(native.exitCode).not.toBe(0);expect(f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(row.id).content).toBe('Original text');
 }finally{await f.close();}
},30000);

test('Chrome edits paragraphs, preserves exact source text, saves and reopens the same native page',async()=>{
 const f=await setup();let browser;
 try{
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
  browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
  const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(15000);
  const errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.dismiss());
  const [name,...value]=f.writer.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.setViewport({width:1440,height:1000});await page.goto(f.base+'/cms/?type=wiki_page&id=new',{waitUntil:'networkidle0'});
  await page.waitForSelector('.cms-authoring');
  await page.type('[name=title]','A workshop publication');
  const first='First paragraph with café and <script>literal text</script>.',second='Second paragraph with an unfamiliar requirement.';
  await page.type('[name=content]',first+'\n\n'+second);
  expect(await page.$eval('.cms-document-body',n=>n.textContent)).toBe(first+'\n\n'+second);
  await page.evaluate(()=>[...document.querySelectorAll('.cms-editor-button')].find(n=>n.textContent==='Paragraphs').click());
  expect(await page.$$eval('.cms-paragraph',n=>n.length)).toBe(2);
  await page.click('[aria-label="Move paragraph 2 up"]');
  expect(await page.$eval('[name=content]',n=>n.value)).toBe(second+'\n\n'+first);
  await page.click('.cms-document-settings summary');await page.type('[name=route]','notes/authoring-fixture');
  await page.click('.cms-form button[type=submit]');
  await page.waitForSelector('.cms-form[hx-put]');
  const path=await page.$eval('.cms-form',n=>n.getAttribute('hx-put')),id=path.split('/').at(-1);
  expect(f.sql.query('SELECT COUNT(*) AS n FROM wiki_page WHERE namespace=?').get('actor-'+f.writer.id).n).toBe(1);
  expect(f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(id).content).toBe(second+'\n\n'+first);
  expect(page.url()).toContain('id='+id);
  await page.type('[name=content]',' Added without duplicating the page.');await page.click('.cms-form button[type=submit]');
  await page.waitForFunction(()=>document.querySelector('.cms-form')?.dataset.dirty==='false'&&!document.querySelector('[data-saving=true]'));
  expect(f.sql.query('SELECT COUNT(*) AS n FROM wiki_page WHERE namespace=?').get('actor-'+f.writer.id).n).toBe(1);
  for(const width of [1440,390]){
   await page.setViewport({width,height:1000});await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('.cms-authoring');
   expect(await page.$eval('[name=title]',n=>n.value)).toBe('A workshop publication');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   if(process.env.BONFYRE_CMS_SCREENSHOTS){mkdirSync(process.env.BONFYRE_CMS_SCREENSHOTS,{recursive:true});await page.screenshot({path:join(process.env.BONFYRE_CMS_SCREENSHOTS,'cms-'+width+'.png'),fullPage:true});}
  }
  await page.type('[name=content]',' Unsaved edit.');await page.click('.cms-form-head button[hx-get]');
  expect(await page.$eval('[name=content]',n=>n.value)).toContain('Unsaved edit.');
  expect(errors).toEqual([]);expect(await page.evaluate(()=>localStorage.length)).toBe(0);
  if(process.env.BONFYRE_CMS_SCREENSHOTS){mkdirSync(process.env.BONFYRE_CMS_SCREENSHOTS,{recursive:true});await page.screenshot({path:join(process.env.BONFYRE_CMS_SCREENSHOTS,'cms-phone.png'),fullPage:true});}
  await context.close();
 }finally{await browser?.close();await f.close();}
},60000);


test('every admitted 64 KiB field reopens without HTML truncation; decoded and wire overflows keep the record unchanged',async()=>{
 const f=await setup();let browser;
 try{
  const tail='END OF COMPLETE FIELD',content="&".repeat(65535-tail.length)+tail;
  expect(Buffer.byteLength(content)).toBe(65535);
  const saved=await f.request('/cms/v1/api/wiki_page',{cookie:f.writer.cookie,form:{title:'Complete large source',route:'notes/complete-large-source',content,status:'draft'}});
  expect(saved.body).toContain('cms-status ok');
  const row=f.sql.query('SELECT * FROM wiki_page WHERE namespace=?').get('actor-'+f.writer.id);expect(row.content).toBe(content);
  const opened=await f.request('/cms/parts/edit?type=wiki_page&id='+row.id,{cookie:f.writer.cookie});expect(opened.body).toContain(tail);expect(opened.body).not.toContain('Nothing was shortened');expect(opened.body.length).toBeGreaterThan(262144);
  browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
  const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  const[name,...v]=f.writer.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.goto(f.base+'/cms/?type=wiki_page&id='+row.id,{waitUntil:'networkidle0'});
  expect(await page.$eval('[name=content]',n=>n.value)).toBe(content);
  await page.$eval('[name=content]',n=>{n.value='Edited beginning'+n.value.slice(16);n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('.cms-form button[type=submit]');
  await page.waitForFunction(()=>document.querySelector('.cms-form')?.dataset.dirty==='false'&&!document.querySelector('[data-saving=true]'));
  const kept=f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(row.id).content;expect(kept.startsWith('Edited beginning')).toBe(true);expect(kept.endsWith(tail)).toBe(true);expect(kept.length).toBe(65535);
  await page.reload({waitUntil:'networkidle0'});expect(await page.$eval('[name=content]',n=>n.value)).toBe(kept);
  for(const invalid of ['x'.repeat(65536),'é'.repeat(32768),'"'.repeat(65535)]){
   const result=await f.request('/cms/v1/api/wiki_page/'+row.id,{cookie:f.writer.cookie,method:'PUT',form:{content:invalid}});
   expect(result.body).toContain('could not be validated');expect(f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(row.id).content).toBe(kept);
  }
  const tooLarge=await fetch(f.base+'/cms/v1/api/wiki_page/'+row.id,{method:'PUT',headers:{cookie:f.writer.cookie,'content-type':'application/x-www-form-urlencoded'},body:'content='+'x'.repeat(256*1024)});
  expect(tooLarge.status).toBe(413);expect(f.sql.query('SELECT content FROM wiki_page WHERE id=?').get(row.id).content).toBe(kept);expect(errors).toEqual([]);
  await context.close();
 }finally{await browser?.close();await f.close();}
},60000);
