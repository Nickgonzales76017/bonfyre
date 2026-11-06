// A local HTTPS test carrier around the native HTTP owner. No public host is contacted.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:https';
import {request as nativeRequest} from 'node:http';
import {execFileSync} from 'node:child_process';
import {join} from 'node:path';
import {mkdirSync,readFileSync,writeFileSync} from 'node:fs';
import {Database} from 'bun:sqlite';
import {fixture,root,password,pause} from './auth_continuation_fixture.mjs';
const pressure=execFileSync('/usr/bin/memory_pressure',['-Q'],{encoding:'utf8'});
const free=Number(pressure.match(/free percentage: (\d+)%/)?.[1]);
if(!(free>=18))throw Error('Browser verification requires at least 18% available memory; no browser launched.');
const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
const f=await fixture(),checks=[],errors=[];
const check=(label,value)=>{assert.ok(value,label);checks.push(label);console.log('PASS '+label);};
const out=join(root,'cmd/BonfyreWebAddress/tests/artifacts/auth-continuation',Date.now()+'-'+f.fingerprint.slice(0,12));
mkdirSync(out,{recursive:true});
let browser,proxy;
try{
  const key=join(f.home,'local-test.key'),cert=join(f.home,'local-test.crt');
  f.run('/usr/bin/openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',key,'-out',cert,'-subj','/CN=aurekai.life','-days','1']);
  proxy=createServer({key:readFileSync(key),cert:readFileSync(cert)},(req,res)=>{
    if(!/^aurekai\.life:\d+$/.test(req.headers.host??'')){res.writeHead(403);res.end();return;}
    const upstream=nativeRequest({hostname:'127.0.0.1',port:f.port,method:req.method,path:req.url,headers:req.headers},answer=>{res.writeHead(answer.statusCode,answer.headers);answer.pipe(res);});
    upstream.on('error',()=>{if(!res.headersSent)res.writeHead(502);res.end();});req.pipe(upstream);
  });
  await new Promise((resolve,reject)=>{proxy.once('error',reject);proxy.listen(0,'127.0.0.1',resolve);});
  const origin='https://aurekai.life:'+proxy.address().port;
  browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--ignore-certificate-errors','--no-proxy-server','--host-resolver-rules=MAP aurekai.life 127.0.0.1']});
  const prior=[];
  for(const [index,mode] of ['fallback-no-script','fallback-script','cms-authored-phone'].entries()){
    if(index===2)f.authored();
    const context=await browser.createBrowserContext(),page=await context.newPage();prior.push({context,page});
    page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(mode+': '+error.message));
    await page.setViewport({width:index===2?390:1440,height:900});
    await page.setRequestInterception(true);
    page.on('request',req=>{const u=new URL(req.url());if(u.origin===origin||['data:','about:'].includes(u.protocol))req.continue();else req.abort();});
    if(index===0)await page.setJavaScriptEnabled(false);
    const destination=f.path+'?view=reading&section=source#review';
    await page.goto(origin+'/login?next='+encodeURIComponent(destination),{waitUntil:'domcontentloaded'});
    await page.waitForSelector('form[action="/login"] input[name="next"]');
    check(mode+' renders the exact hidden continuation',await page.$eval('form[action="/login"] input[name="next"]',node=>node.value)===destination);
    await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('a[href^="/signup?next="]')]);
    await page.waitForSelector('form[action="/signup"] input[name="name"]');
    check(mode+' retains continuation when choosing account creation',await page.$eval('form[action="/signup"] input[name="next"]',node=>node.value)===destination);
    await page.type('input[name="name"]','Reader '+index);await page.type('input[name="email"]','browser-reader-'+index+'@example.test');await page.type('input[name="password"]',password);
    await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('form[action="/signup"] button[type="submit"]')]);
    await page.waitForSelector('form[action="/login"] input[name="email"]');
    check(mode+' registration reaches sign-in with the selected item',new URL(page.url()).searchParams.get('next')===destination);
    check(mode+' registration did not mint a browser session',!(await context.cookies()).some(c=>c.name==='bonfyre_session'));
    await page.type('input[name="email"]','browser-reader-'+index+'@example.test');await page.type('input[name="password"]','wrong-continuation-passphrase');
    await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('form[action="/login"] button[type="submit"]')]);
    await page.waitForSelector('form[action="/login"] input[name="email"]');
    check(mode+' failed sign-in retains the selected item',new URL(page.url()).searchParams.get('next')===destination);
    await page.type('input[name="email"]','browser-reader-'+index+'@example.test');await page.type('input[name="password"]',password);
    await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('form[action="/login"] button[type="submit"]')]);
    await page.waitForSelector('form[action="/o/save"]');
    const landed=new URL(page.url());
    check(mode+' returns to the real item with query and fragment',landed.pathname+landed.search+landed.hash===destination);
    check(mode+' native session is secure and HttpOnly',(await context.cookies()).some(c=>c.name==='bonfyre_session'&&c.httpOnly&&c.secure));
    check(mode+' item was not saved by registration',await page.$('form[action="/o/unsave"]')===null);
    await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('form[action="/o/save"] button')]);
    await page.waitForSelector('form[action="/o/unsave"]');
    await page.goto(origin+'/saved',{waitUntil:'domcontentloaded'});
    check(mode+' explicit save appears in the person’s native collection',(await page.content()).includes('City opens a new library'));
    await page.screenshot({path:join(out,mode+'.png'),fullPage:false});
    check(mode+' layout fits its viewport',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  }
  {const db=new Database(f.db,{readonly:true});try{const rows=db.query('SELECT actor,item_ref FROM saved_item').all();check('separate browser contexts retain three independent native saved records',rows.length===3&&new Set(rows.map(r=>r.actor)).size===3&&rows.every(r=>r.item_ref===f.item));}finally{db.close();}}
  const first=prior[0].page,last=prior.at(-1).page;
  await first.goto(origin+f.path,{waitUntil:'domcontentloaded'});
  await Promise.all([first.waitForNavigation({waitUntil:'domcontentloaded'}),first.click('form[action="/o/unsave"] button')]);
  await last.reload({waitUntil:'domcontentloaded'});
  check('removing the first person’s copy leaves the recipient’s collection intact',(await last.content()).includes('City opens a new library'));
  check('browser runtime reports no JavaScript exceptions',errors.length===0);
  const report=['scope\tlocal HTTPS test carrier; native Auth/CMS/WebAddress; no public deployment','native_sha256\t'+f.fingerprint,'browser\t'+await browser.version(),...checks.map(name=>'passed\t'+name)].join('\n')+'\n';
  writeFileSync(join(out,'verification.tsv'),report);
  console.log(`AUTH_CONTINUATION_BROWSER_PASS\t${checks.length}\tevidence\t${out}`);
}catch(error){writeFileSync(join(out,'failed.txt'),String(error)+'\n'+checks.join('\n')+'\n');throw error;}
finally{await browser?.close();if(proxy)await new Promise(resolve=>{proxy.close(resolve);proxy.closeAllConnections?.();});await f.close();}
