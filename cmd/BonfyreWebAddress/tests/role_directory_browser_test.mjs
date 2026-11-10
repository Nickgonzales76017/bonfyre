import assert from 'node:assert/strict';
import {createRequire} from 'node:module';

const base=process.env.BONFYRE_ROLE_BROWSER_BASE;
const session=process.env.BONFYRE_ROLE_BROWSER_SESSION;
assert(/^http:\/\/127\.0\.0\.1:\d+$/.test(base||''),'isolated native listener required');
assert(/^[A-Za-z0-9._~-]+$/.test(session||''),'isolated signed-in session required');
const puppeteer=createRequire(new URL('../../../tools/creator-ingest/package.json',import.meta.url))('puppeteer');
const browser=await puppeteer.launch({
  headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  args:['--disable-background-networking','--disable-extensions']
});
const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];
page.on('pageerror',e=>errors.push(e.message));
page.setDefaultTimeout(10000);
await page.setRequestInterception(true);
page.on('request',async request=>{
  try{
    const url=new URL(request.url());
    if(url.origin!=='https://aurekai.life'){
      await request.respond({status:204,body:''});return;
    }
    const headers={...request.headers(),host:'aurekai.life',cookie:'bonfyre_session='+session};
    delete headers['content-length'];delete headers['accept-encoding'];
    const response=await fetch(base+url.pathname+url.search,{
      method:request.method(),headers,body:request.postData()||undefined,
      redirect:'manual',signal:AbortSignal.timeout(10000)
    });
    const reply={};
    for(const[k,v] of response.headers)
      if(!['content-length','content-encoding','connection','transfer-encoding','set-cookie'].includes(k))reply[k]=v;
    await request.respond({status:response.status,headers:reply,body:Buffer.from(await response.arrayBuffer())});
  }catch(e){errors.push('Role fixture transport: '+e.message);await request.abort().catch(()=>{});}
});
await page.setCookie({name:'bonfyre_session',value:session,url:'https://aurekai.life',httpOnly:true,secure:true,sameSite:'Lax'});
const scope='community:browser-role-smoke';
async function open(path){await page.goto('https://aurekai.life'+path,{waitUntil:'domcontentloaded'});}
async function move(f){await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),f()]);}
try{
  await page.setViewport({width:390,height:844});
  await open('/roles?scope='+encodeURIComponent(scope));
  assert.equal(await page.$$eval('[data-role-program]',n=>n.length),3);
  assert.equal(await page.$eval('[name=scope]',n=>n.value),scope);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px role directory overflow');
  await page.type('.role-search [name=q]','internet');
  await move(()=>page.click('.role-search button'));
  assert.equal(await page.$$eval('[data-role-program]',n=>n.length),1);
  assert.match(page.url(),/scope=community%3Abrowser-role-smoke/);
  assert.equal(await page.$eval('[data-role-program] [name=scope]',n=>n.value),scope);
  await move(()=>page.click('[data-role-program] button[type=submit]'));
  assert.equal(new URL(page.url()).pathname,'/roles');
  assert.match(await page.$eval('[data-role-assignments]',n=>n.textContent),/community:browser-role-smoke/);
  const links=await page.$$eval('[data-role-assignment] a',n=>n.map(x=>x.getAttribute('href')));
  const detail=links.find(x=>x?.startsWith('/roles?assignment='));
  assert(detail,'new assignment must be discoverable');
  await open(detail);
  assert.equal(await page.$$eval('[data-role-posture]',n=>n.length),1);
  const copy=await page.$eval('[data-role-posture]',n=>n.textContent);
  for(const label of ['Work queues','Authority still required','Operations not yet bound'])
    assert(copy.includes(label),'missing '+label);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px role detail overflow');
  await page.setViewport({width:1365,height:900});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'desktop role detail overflow');
  await move(()=>page.click('.role-end button[type=submit]'));
  assert.equal(new URL(page.url()).pathname,'/roles');
  assert(!((await page.$eval('[data-role-assignments]',n=>n.textContent)).includes(scope)),'ended assignment remains active');
  assert.deepEqual(errors,[]);
  console.log('Stitch operating roles: PASS Chrome 390px/desktop, search, scoped assume, private queues, end, no script exceptions');
}finally{await page.close();await context.close();await browser.close();}
