import {test,expect} from 'bun:test';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {workspaceFixture,root} from './workspace_fixture.mjs';

// Real Chrome retains Secure/HttpOnly cookies for the four HTTPS origins.
// A transparent test transport sends each request to isolated native owners;
// no request or fixture credential reaches a production Aurekai endpoint.
test('Chrome carries one account and its installed workspace through all four territory links',async()=>{
 const f=await workspaceFixture();let browser;
 try{
  const binding=await f.install('catalogue','self',f.owner);
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
  browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
  const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(15000);
  const errors=[];page.on('pageerror',e=>errors.push(e.message));let handoffs=0;
  await page.setRequestInterception(true);
  page.on('request',async request=>{
   try{
    const u=new URL(request.url());
    if(!/^aurekai\.(work|life|live|shop)$/.test(u.hostname)){await request.respond({status:204,body:''});return;}
    const headers={...request.headers(),Host:u.hostname};delete headers.host;delete headers['content-length'];delete headers['accept-encoding'];
    const result=await fetch(f.base+u.pathname+u.search,{method:request.method(),headers,body:request.postData(),redirect:'manual',signal:AbortSignal.timeout(10000)});
    const responseHeaders={};for(const [key,value]of result.headers)if(!['content-length','content-encoding','transfer-encoding','connection','set-cookie'].includes(key))responseHeaders[key]=value;
    const cookies=result.headers.getSetCookie();if(cookies.length)responseHeaders['set-cookie']=cookies;
    if(u.pathname==='/session/complete')handoffs++;
    await request.respond({status:result.status,headers:responseHeaders,body:Buffer.from(await result.arrayBuffer())});
   }catch(error){errors.push('Isolated transport: '+error.name);await request.abort().catch(()=>{});}
  });
  const [name,...value]=f.owner.cookie.split('=');
  await context.setCookie({name,value:value.join('='),domain:'.aurekai.life',path:'/',secure:true,httpOnly:true,sameSite:'Lax'});
  await page.setViewport({width:1440,height:1000});await page.goto('https://aurekai.life/',{waitUntil:'networkidle0'});
  await page.waitForSelector('[data-account-continuation]');
  for(const destination of ['work','shop','live','life']){
   await page.waitForSelector('.ak-places a[href*="to='+destination+'"]');
   await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('.ak-places a[href*="to='+destination+'"]')]);
   expect(new URL(page.url()).hostname).toBe('aurekai.'+destination);
   await page.waitForSelector('.ak-session[data-session-state="signed-in"]');
   await page.waitForSelector('[data-account-continuation]');
   expect(await page.$$eval('.ak-session a',nodes=>nodes.some(n=>/^(Sign in|Create account)$/.test(n.textContent.trim())))).toBe(false);
  }
  expect(handoffs).toBe(3); // Returning LIFE already holds the initiating account.
  const sessions=(await context.cookies()).filter(c=>c.name==='bonfyre_session');
  expect(sessions.length).toBe(4);expect(new Set(sessions.map(c=>c.value)).size).toBe(4);
  expect(sessions.every(c=>c.httpOnly&&c.secure)).toBe(true);
  await page.goto('https://aurekai.work/apps?context=self',{waitUntil:'networkidle0'});
  expect(await page.$eval('[data-work-editor]',n=>n.getAttribute('href'))).toBe(binding.path);
  expect(errors).toEqual([]);await context.close();
 }finally{await browser?.close();await f.close();}
},60000);
