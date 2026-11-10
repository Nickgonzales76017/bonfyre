import {test,beforeAll,afterAll,expect} from 'bun:test';
import {invitationReference,invitationDisplay} from '../../../integrations/browser/invitation-choice.mjs';
import {packSetupFixture,root,decode} from './pack_setup_fixture.mjs';
import {join} from 'node:path';import {createRequire} from 'node:module';
const secret='a'.repeat(64);
test('invitation links resolve only exact Aurekai review addresses and never imply acceptance',()=>{
 for(const host of ['work','life','live','shop'])expect(invitationReference('https://aurekai.'+host+'/organizations/join#'+secret,'https://aurekai.work').secret).toBe(secret);
 expect(invitationReference('/organizations/join#'+secret,'https://aurekai.work').secret).toBe(secret);
 expect(JSON.stringify(invitationDisplay('https://aurekai.work/organizations/join#'+secret))).not.toContain(secret);
 for(const value of [secret,'https://evil.test/organizations/join#'+secret,'https://u:p@aurekai.work/organizations/join#'+secret,'https://aurekai.work/organizations/join?accept=1#'+secret,'https://aurekai.work/organizations#'+secret,'http://aurekai.work/organizations/join#'+secret])expect(()=>invitationReference(value,'https://aurekai.work')).toThrow();
});
let f,browser,owner,recipient,invite;const previous=process.env.BONFYRE_WORKSPACE_CLIENT;
const hidden=(html,marker)=>{const form=[...html.matchAll(/<form\b([^>]+)>([\s\S]*?)<\/form>/g)].find(m=>m[1].includes(marker));if(!form)throw Error('Missing native form '+marker);return Object.fromEntries([...form[2].matchAll(/<input\b[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));};
beforeAll(async()=>{
 process.env.BONFYRE_WORKSPACE_CLIENT=process.env.BONFYRE_TEST_WORKSPACE_CLIENT||join(root,'integrations/browser');f=await packSetupFixture();
 owner=await f.login('invite-owner@example.test');recipient=await f.login('invite-recipient@example.test');
 const page=await f.request('/organizations',{cookie:owner.cookie}),created=await f.request('/organizations',{cookie:owner.cookie,form:{...hidden(page.body,'data-org-create'),name:'Repair learning cooperative'},headers:{Origin:'https://aurekai.life'}});
 const issue=await f.request('/organizations',{cookie:owner.cookie,form:{...hidden(created.body,'data-org-invite'),email:'invite-recipient@example.test',role:'read',hours:'24'},headers:{Origin:'https://aurekai.life'}});
 invite=decode(issue.body.match(/data-org-invitation href="([^"]+)"/)?.[1]||'');if(!invite.startsWith('/organizations/join#'))throw Error('Native owner did not issue an invitation');
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},45000);
afterAll(async()=>{await browser?.close();await f?.close();if(previous===undefined)delete process.env.BONFYRE_WORKSPACE_CLIENT;else process.env.BONFYRE_WORKSPACE_CLIENT=previous;});
async function open(user=recipient){const context=await browser.createBrowserContext(),page=await context.newPage(),posts=[],requests=[],errors=[];page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{requests.push(r.url());if(r.method()==='POST')posts.push(r.postData());});await page.setViewport({width:1440,height:1000});const[name,...v]=user.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});await page.goto(f.base+'/organizations/join',{waitUntil:'networkidle0'});await page.waitForSelector('[data-link-choice]');return {context,page,posts,requests,errors};}
test('the recipient pastes a link, reviews a real organization, then explicitly accepts without typing an ID',async()=>{
 const p=await open();try{
  p.page.on('request',r=>{if(r.method()==='POST')console.log('INVITATION_ORIGIN '+JSON.stringify({origin:r.headers().origin,type:r.headers()['content-type']}));});
  expect(await p.page.$eval('#org-invite-code',n=>n.type)).toBe('hidden');expect(p.posts).toHaveLength(0);
  await p.page.type('input[aria-label="Invitation link"]','https://aurekai.work'+invite);expect(p.posts).toHaveLength(0);
  await Promise.all([p.page.waitForNavigation({waitUntil:'networkidle0'}),p.page.click('#org-invite-review button')]);
  if(!await p.page.$('h2'))throw Error('Native invitation review: '+await p.page.$eval('body',n=>n.innerText.slice(0,1200))+'; fields '+JSON.stringify(p.posts.map(body=>[...new URLSearchParams(body).keys()])));
  expect(new URLSearchParams(p.posts[0]).get('secret')).toBe(invite.split('#')[1]);
  expect(await p.page.$eval('h2',n=>n.textContent)).toBe('Repair learning cooperative');expect(p.posts).toHaveLength(1);
  expect(f.authsql.query('SELECT count(*) AS n FROM institution_grants WHERE user_id=?').get(recipient.id).n).toBe(0);
  await Promise.all([p.page.waitForNavigation({waitUntil:'networkidle0'}),p.page.click('[data-org-accept] button')]);
  expect(f.authsql.query('SELECT role FROM institution_grants WHERE user_id=?').get(recipient.id).role).toBe('read');
  expect(p.posts).toHaveLength(2);expect(p.requests.every(url=>!url.includes(invite.split('#')[1]))).toBe(true);expect(p.errors).toEqual([]);
 }finally{await p.context.close();}
},30000);
test('changed or malformed invitation text clears identity without sending a review request',async()=>{
 const p=await open();try{
  await p.page.type('input[aria-label="Invitation link"]','https://aurekai.work'+invite);
  await p.page.$eval('input[aria-label="Invitation link"]',n=>{n.value='unknown account';n.dispatchEvent(new Event('input',{bubbles:true}));});
  expect(await p.page.$eval('#org-invite-code',n=>n.value)).toBe('');expect(await p.page.$eval('#org-invite-review button',n=>n.disabled)).toBe(true);
  await p.page.setViewport({width:390,height:900});expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  expect(p.posts).toHaveLength(0);expect(p.errors).toEqual([]);
 }finally{await p.context.close();}
},30000);

// Production accounts and invitation tokens are never accessed by this fixture.
