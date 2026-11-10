// Real isolated Auth/CMS/WebAddress and Chrome. Network fault cases alter only
// delivery of the existing navigation fragment; no customer account is used.
import { test, expect, beforeAll, afterAll } from 'bun:test';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createServer } from 'node:https';
import { request as httpRequest } from 'node:http';
import { createHash, X509Certificate } from 'node:crypto';
import { packSetupFixture, root, password } from './pack_setup_fixture.mjs';
let f, browser, owner, tls, origin;
const proof = join(root, 'cmd/BonfyreExperienceStage/verification/navigation-refresh-20261020');
beforeAll(async () => {
  f = await packSetupFixture();
  f.run(f.web, ['domain', '--db', join(f.home, 'matrix.db'), '--host', '127.0.0.1', '--territory', 'work', '--scope', 'public', '--world', 'CANDIDATE', '--repr', 'HTML', '--evidence', 'fixture:navigation-refresh']);
  owner = await f.login('navigation-refresh@example.test');
  f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Avery Navigation', owner.id);
  const actor = 'person:navigation-test', matrix = join(f.home, 'matrix.db');
  f.run(f.auth, ['--db', f.authdb, 'actor-bind', '--user-id', String(owner.id), '--actor', actor]);
  f.run(f.web, ['subject', '--db', matrix, '--id', actor, '--kind', 'Person', '--digest', 'a'.repeat(64), '--evidence', 'fixture:profile']);
  f.run(f.web, ['institution', '--db', matrix, '--id', 'inst:person-navigation-test', '--kind', 'Person', '--world', 'live', '--evidence', 'fixture:profile']);
  f.run(f.web, ['domain', '--db', matrix, '--host', 'navigation-test.aurekai.life', '--institution', 'inst:person-navigation-test', '--territory', 'life', '--scope', 'public', '--world', 'live', '--repr', 'HTML', '--evidence', 'fixture:profile']);
  f.run(f.web, ['presence', '--db', matrix, '--id', 'presence:navigation-test:life', '--subject', actor, '--territory', 'life', '--scope', 'public', '--world', 'live', '--repr', 'HTML', '--evidence', 'fixture:profile']);
  f.run(f.web,['domain','--db',matrix,'--host','navigation-test.aurekai.work','--institution','inst:person-navigation-test','--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);
  f.run(f.web,['presence','--db',matrix,'--id','presence:navigation-test:work','--subject',actor,'--territory','work','--scope','public','--world','live','--repr','HTML','--evidence','fixture:profile']);
  // The production transport only emits Secure, territory-scoped cookies.
  // Use a real local TLS origin instead of weakening that rule for loopback.
  const host = 'account-navigation.aurekai.work';
  f.run(f.web, ['domain', '--db', matrix, '--host', host, '--territory', 'work', '--scope', 'public', '--world', 'CANDIDATE', '--repr', 'HTML', '--evidence', 'fixture:tls']);
  const key = join(f.home, 'tls.key'), cert = join(f.home, 'tls.crt');
  f.run('/usr/bin/openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', cert, '-days', '1', '-subj', '/CN='+host]);
  const spki = createHash('sha256').update(new X509Certificate(readFileSync(cert)).publicKey.export({type:'spki',format:'der'})).digest('base64');
  tls = createServer({key:readFileSync(key),cert:readFileSync(cert)}, (incoming, outgoing) => {
    const request = httpRequest({hostname:'127.0.0.1',port:f.port,path:incoming.url,method:incoming.method,headers:incoming.headers}, response => {
      outgoing.writeHead(response.statusCode, response.headers); response.pipe(outgoing);
    });
    request.on('error', () => { if (!outgoing.headersSent) outgoing.writeHead(502); outgoing.end(); });
    incoming.pipe(request);
  });
  await new Promise((resolve,reject) => { tls.once('error',reject); tls.listen(0,'127.0.0.1',resolve); });
  origin = 'https://'+host+':'+tls.address().port;
  const puppeteer = createRequire(join(root, 'tools/creator-ingest/package.json'))('puppeteer');
  browser = await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--host-resolver-rules=MAP '+host+' 127.0.0.1','--ignore-certificate-errors-spki-list='+spki]});
  mkdirSync(proof, {recursive:true});
}, 30000);
afterAll(async () => { await browser?.close(); tls?.closeAllConnections(); if(tls)await new Promise(r=>tls.close(r)); await f?.close(); });
async function pageFor(user) {
  const context = await browser.createBrowserContext(), page = await context.newPage();
  page.setDefaultTimeout(15000);
  const errors = [], net = {mode:'normal', calls:0, held:[]};
  page.on('pageerror', e => errors.push(e.message));
  if (user) {
    const [name, ...value] = user.cookie.split('=');
    await page.setCookie({name,value:value.join('='),url:origin,secure:true,httpOnly:true,sameSite:'Lax'});
  }
  await page.setRequestInterception(true);
  page.on('request', request => {
    void (async () => {
      if (new URL(request.url()).origin !== origin) return request.abort();
      if (new URL(request.url()).pathname !== '/account/navigation') return request.continue();
      net.calls++;
      if (net.mode === 'hold') { net.held.push(request); return; }
      if (net.mode === 'outage') return request.respond({status:503,contentType:'text/html',body:'Unavailable'});
      if (net.mode === 'oversize') return request.respond({status:200,contentType:'text/html',body:'x'.repeat(32769)});
      if (net.mode === 'wrong-type') return request.respond({status:200,contentType:'application/json',body:'{}'});
      if (net.mode === 'redirect') return request.respond({status:302,headers:{Location:'/login'},body:''});
      if (net.mode === 'wrong-shape') return request.respond({status:200,contentType:'text/html',body:'<h1>Another page</h1>'});
      if (net.mode === 'duplicate') return request.respond({status:200,contentType:'text/html',body:'<div class="ak-session" data-session-state="anonymous"></div>'.repeat(2)});
      if (net.mode === 'script') return request.respond({status:200,contentType:'text/html',body:'<div class="ak-session" data-session-state="signed-in"><script>window.bad=1</script></div>'});
      return request.continue();
    })().catch(() => {}); // Aborted test deliveries cannot dispatch another request.
  });
  return {page,context,errors,net};
}
async function settled(page, state) {
  await page.waitForFunction(state => {
    const slot = document.querySelector('[data-account-slot]');
    return slot?.dataset.accountRefresh === '2' && !slot.hasAttribute('aria-busy') && slot.querySelector('.ak-session')?.dataset.sessionState === state;
  }, {}, state);
}
const refresh = page => page.evaluate(() => window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));
const selected = '/packs/setup?context=self#selected-source';
test('shared account navigation loads its module and CSS once, without a copied inline script',async()=>{
 const {page,context,errors,net}=await pageFor(owner);
 try{
  await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
  expect(await page.$eval('.ak-bar',node=>node.dataset.bfNavMounted)).toBe('1');
  expect(await page.$$eval('script[src="/apps/aurekai-activity.mjs"]',nodes=>nodes.length)).toBe(1);
  expect(await page.$$eval('link[href="/apps/aurekai-activity.css"]',nodes=>nodes.length)).toBe(1);
  expect(await page.$eval('.ak-user summary',node=>getComputedStyle(node).minHeight)).toBe('44px');
  expect(await page.$eval('[data-account-slot]',node=>node.dataset.accountRefresh)).toBe('2');
  expect(net.calls).toBe(1);expect(errors).toEqual([]);
 }finally{await context.close();}
},30000);
test('native header keeps exact continuation through the actual login form and owner profile', async () => {
  const {page,context,net,errors} = await pageFor();
  try {
    await page.goto(origin+selected, {waitUntil:'networkidle0'}); await settled(page,'anonymous');
    expect(net.calls).toBe(1);
    expect(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next'))).toBe(selected);
    await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('#session-link')]);
    expect(await page.$eval('form input[name=next]',n=>n.value)).toBe(selected);
    expect(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next'))).toBe(selected);
    await page.type('input[name=email]','navigation-refresh@example.test');
    await page.type('input[name=password]',password);
    await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.click('form button[type=submit]')]);
    await settled(page,'signed-in'); expect(page.url()).toBe(origin+selected);
    expect(await page.$eval('[data-profile-home]',a=>a.getAttribute('href'))).toBe('/profile');
    expect(await page.$eval('[data-public-profile-menu]',a=>a.href)).toBe('https://navigation-test.aurekai.work/');
    expect(await page.$$eval('[data-account-slot] a',nodes=>nodes.some(a=>/^\/(login|signup)(\?|$)/.test(a.getAttribute('href'))))).toBe(false);
    expect(await page.$eval('[data-account-profile]',a=>a.getAttribute('href'))).toBe('/profile?tab=sites');
    expect(errors).toEqual([]);
  } finally { await context.close(); }
}, 30000);
test('unchanged native fragment preserves expanded menu and focused profile action',async()=>{
  const {page,context,errors}=await pageFor(owner);
  try {
    await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    await page.click('.ak-user summary');await page.focus('[data-public-profile-menu]');
    await refresh(page);await settled(page,'signed-in');
    expect(await page.$eval('.ak-user',n=>n.open)).toBe(true);
    expect(await page.evaluate(()=>document.activeElement.hasAttribute('data-public-profile-menu'))).toBe(true);
    await page.keyboard.press('Escape');expect(await page.$eval('.ak-user',n=>n.open)).toBe(false);
    expect(errors).toEqual([]);
  }finally{await context.close();}
},30000);
test('unavailable and malformed fragments never become false sign-out, and retry reuses Auth',async()=>{
  const {page,context,net,errors}=await pageFor(owner);
  try{
    await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    for(const mode of ['outage','oversize','wrong-type','redirect','wrong-shape','duplicate','script']){
      net.mode=mode;await refresh(page);await settled(page,'unavailable');
      expect(await page.$eval('[data-account-slot]',n=>n.textContent)).not.toContain('Avery');
      expect(await page.$('#session-link')).toBeNull();expect(await page.$('[data-session-retry]')).not.toBeNull();
      net.mode='normal';await page.click('[data-session-retry]');await settled(page,'signed-in');
    }
    expect(await page.evaluate(()=>window.bad)).toBeUndefined();expect(errors).toEqual([]);
  }finally{await context.close();}
},30000);
test('a held request times out, clears busy state and permits one user retry',async()=>{
  const {page,context,net}=await pageFor(owner);
  try{
    await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    net.mode='hold';const before=net.calls;await refresh(page);await settled(page,'unavailable');
    expect(net.calls).toBe(before+1);expect(await page.$eval('[data-account-slot]',n=>n.hasAttribute('aria-busy'))).toBe(false);
    net.mode='normal';await page.click('[data-session-retry]');await settled(page,'signed-in');expect(net.calls).toBe(before+2);
  }finally{await context.close();}
},30000);
test('history suspension clears private names and rejects the old delivery after real revocation',async()=>{
  const user=await f.login('navigation-history@example.test');
  const {page,context,net}=await pageFor(user);
  try{
    await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    const previous=await f.request('/account/navigation',{cookie:user.cookie});
    net.mode='hold';await refresh(page);await page.waitForFunction(()=>document.querySelector('[data-account-slot]').getAttribute('aria-busy')==='true');
    while(!net.held.length)await new Promise(r=>setTimeout(r,20));
    await page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true})));
    expect(await page.$('.ak-user')).toBeNull();
    expect((await f.request('/logout',{cookie:user.cookie,form:{}})).status).toBe(200);
    expect((await f.request('/account/navigation',{cookie:user.cookie})).body).toContain('data-session-state="anonymous"');
    net.mode='normal';await refresh(page);await settled(page,'anonymous');
    for(const request of net.held)await request.respond({status:200,contentType:'text/html',body:previous.body}).catch(()=>{});
    await new Promise(r=>setTimeout(r,100));expect(await page.$('.ak-user')).toBeNull();
    expect(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next'))).toBe(selected);
  }finally{await context.close();}
},30000);
test('actual Back and native logout recheck identity without losing the selected address',async()=>{
  const user=await f.login('navigation-back@example.test'),{page,context}=await pageFor(user);
  try{
    await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    await page.goto(origin+'/packs/setup?pack=creator-work',{waitUntil:'networkidle0'});await settled(page,'signed-in');
    expect((await f.request('/logout',{cookie:user.cookie,form:{}})).status).toBe(200);
    expect((await f.request('/account/navigation',{cookie:user.cookie})).body).toContain('data-session-state="anonymous"');
    await page.goBack({waitUntil:'networkidle0'});await settled(page,'anonymous');
    expect(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next'))).toBe(selected);
  }finally{await context.close();}
},30000);
test('native phone menu stays within the viewport, reduced motion and storage remain unchanged',async()=>{
  const {page,context}=await pageFor(owner);
  try{
    await page.setViewport({width:390,height:900});await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    await page.click('.ak-user summary');
    const rect=await page.$eval('.ak-user-links',n=>({left:n.getBoundingClientRect().left,right:n.getBoundingClientRect().right}));
    expect(rect.left).toBeGreaterThanOrEqual(0);expect(rect.right).toBeLessThanOrEqual(390);
    await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);
    expect(await page.$eval('.ak-user summary',n=>getComputedStyle(n).transitionDuration)).toBe('0s');
    expect(await page.evaluate(()=>({local:localStorage.length,session:sessionStorage.length}))).toEqual({local:0,session:0});
    await page.screenshot({path:join(proof,'native-isolated-navigation-390.png'),fullPage:true});
  }finally{await context.close();}
},30000);
test('account menu stays usable on a short phone viewport',async()=>{
  const {page,context}=await pageFor(owner);
  try{
    await page.setViewport({width:390,height:320});await page.goto(origin+selected,{waitUntil:'networkidle0'});await settled(page,'signed-in');
    await page.click('.ak-user summary');
    const menu=await page.$eval('.ak-user-links',n=>({bottom:n.getBoundingClientRect().bottom,height:n.clientHeight,content:n.scrollHeight}));
    expect(menu.bottom).toBeLessThanOrEqual(320);expect(menu.content).toBeGreaterThan(menu.height);
    await page.focus('.ak-user-links a:last-child');
    const last=await page.$eval('.ak-user-links a:last-child',n=>({top:n.getBoundingClientRect().top,bottom:n.getBoundingClientRect().bottom,menuTop:n.parentElement.getBoundingClientRect().top,menuBottom:n.parentElement.getBoundingClientRect().bottom,href:n.getAttribute('href')}));
    expect(last.top).toBeGreaterThanOrEqual(last.menuTop);expect(last.bottom).toBeLessThanOrEqual(last.menuBottom);expect(last.href).toBe('/account#logout');
  }finally{await context.close();}
},30000);
test('embedded refresh remains safe inside the existing printf format carrier',()=>{
  const source=readFileSync(join(root,'cmd/BonfyreWebAddress/src/account_navigation_markup.h'),'utf8');
  expect(source.split('#define BF_ACCOUNT_REFRESH')[1]).not.toContain('%');
});
