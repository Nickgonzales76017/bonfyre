import {test,beforeAll,afterAll,expect} from 'bun:test';
import {readAccountChoices} from '../../../integrations/browser/account-choices.mjs';
import {encode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {packSetupFixture,root} from './pack_setup_fixture.mjs';
import {createRequire} from 'node:module';
import {join} from 'node:path';

test('refused and incompatible choice responses release their body rather than stalling navigation',async()=>{
 for(const [status,type]of [[503,'text/plain'],[403,'application/msgpack'],[200,'text/html']]){
  let cancelled=false;const response=new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array([1,2]));},cancel(){cancelled=true;}}),{status,headers:{'content-type':type}});
  await expect(readAccountChoices('/fixture',undefined,async()=>response)).rejects.toThrow();expect(cancelled).toBe(true);
 }
});
test('choice transport stays bounded and never follows a login redirect into data',async()=>{
 let options;const expected=[1,[['self','My personal workspace',true]]];
 expect(await readAccountChoices('/fixture',undefined,async(_,o)=>{options=o;return new Response(encode(expected),{headers:{'content-type':'application/msgpack'}});})).toEqual(expected);
 expect(options.redirect).toBe('error');expect(options.credentials).toBe('same-origin');expect(options.cache).toBe('no-store');
 let cancelled=false;const large=new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(129*1024));},cancel(){cancelled=true;}}),{headers:{'content-type':'application/msgpack'}});
 await expect(readAccountChoices('/fixture',undefined,async()=>large)).rejects.toThrow('bound');expect(cancelled).toBe(true);
});

let f,browser,user;const previous=process.env.BONFYRE_WORKSPACE_CLIENT;
beforeAll(async()=>{
 process.env.BONFYRE_WORKSPACE_CLIENT=process.env.BONFYRE_TEST_WORKSPACE_CLIENT||join(root,'integrations/browser');
 f=await packSetupFixture();user=await f.login('account-reference-reader@example.test');f.grant(user.id,'readers-team','read');
 f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','CANDIDATE','--repr','HTML','--evidence','fixture:account-references']);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},45000);
afterAll(async()=>{await browser?.close();await f?.close();if(previous===undefined)delete process.env.BONFYRE_WORKSPACE_CLIENT;else process.env.BONFYRE_WORKSPACE_CLIENT=previous;});
test('a named read-only space offers only applicable actions and refreshes access within the same account',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(12000);const errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  const[name,...value]=user.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.goto(f.base+'/account',{waitUntil:'networkidle0'});await page.waitForSelector('[data-space-picker] [role=option]');
  await page.type('[data-space-picker] input','readers-team');await page.waitForFunction(()=>document.querySelectorAll('[data-space-picker] [role=option]').length===1);
  await page.click('[data-space-picker] [role=option]');expect(await page.$('[data-space-picker] .reference-selected a[href^="/apps?"]')).not.toBeNull();
  expect(await page.$('[data-space-picker] .reference-selected a[href^="/packs/setup?"]')).toBeNull();
  f.authsql.query("UPDATE institution_grants SET active=0 WHERE user_id=? AND institution='readers-team'").run(user.id);
  await page.click('[data-space-picker] form button');await page.waitForFunction(()=>document.querySelector('[data-space-picker] [role=status]')?.textContent.includes('No matching'));
  expect(await page.$('[data-space-picker] .reference-selected a')).toBeNull();expect(await page.$$eval('[data-space-picker] [role=option]',nodes=>nodes.length)).toBe(0);
  expect(await page.$eval('#signed',n=>n.hidden)).toBe(false);expect(await page.$eval('#identity',n=>n.textContent)).toContain('account-reference-reader@example.test');
  expect(errors).toEqual([]);
 }finally{await context.close();}
},30000);

// Usernames, IDs and provider keys are never a fallback for a failed choice read.
