// The signed-in inbox over isolated native owners: a seeded operator store and
// its real account work index, real Auth organizations and invitations, and a
// temporary approvals directory. Nothing here reads ~/.bonfyre.
import {afterAll,beforeAll,expect,test} from 'bun:test';
import {mkdtempSync,mkdirSync,readFileSync,existsSync,rmSync} from 'node:fs';
import {createServer as createTLS} from 'node:https';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {packSetupFixture,root,decode} from './pack_setup_fixture.mjs';
import {freePort} from './auth_continuation_fixture.mjs';
import {decode as decodePacket} from '../../../integrations/browser/workspace-msgpack.mjs';

const temp=mkdtempSync(join(tmpdir(),'aurekai-inbox-')),state=join(temp,'state'),index=join(state,'account-work.db'),operator=join(temp,'operator.sqlite3'),approvals=join(state,'approvals');
const queue=process.env.BONFYRE_TEST_QUEUE??join(root,'cmd/BonfyreQueue/bonfyre-queue'),proof=process.env.BONFYRE_INBOX_PROOF;
const release='fixture-release-AB12',now=Date.now()/1000,saved={};
let f,owner,colleague,stranger,ids={};
const fields=(html,marker)=>{const form=[...html.matchAll(/<form\b([^>]*)>([\s\S]*?)<\/form>/g)].find(m=>(m[1]+m[2]).includes(marker));if(!form)throw Error('Missing form '+marker);return Object.fromEntries([...form[2].matchAll(/<input\b[^>]*name="([^"]*)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));};
const secure={Origin:'https://aurekai.life'};
function seedOperator(){
 const db=new Database(operator,{create:true});
 db.exec(`CREATE TABLE goals(id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);
  CREATE TABLE runs(id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,state TEXT NOT NULL,summary TEXT NOT NULL DEFAULT '',started_at REAL NOT NULL,completed_at REAL,usage_json TEXT);
  CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT NOT NULL,tool TEXT NOT NULL,state TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL DEFAULT 0);`);
 const goal=db.query('INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)');
 goal.run('g-release','release-approval:'+release,'Approve release '+release,'Release '+release+' passed acceptance.\nWhat it changes: Organizations get provider accounts and the inbox shows what needs you.\nApprove puts it live.',90,'blocked','release-approval','Activation did not go live; the previous release kept serving. error: rendered fronts failed',now-7200,now-3600);
 goal.run('g-old-release','release-approval:fixture-release-OLD1','Approve release fixture-release-OLD1','Old release.',80,'superseded','release-approval','',now-90000,now-86000);
 goal.run('g-twilio','provider-estate:twilio','Connect Twilio for customer subaccounts','Make an API key at https://console.twilio.com/us1/account/keys-credentials/api-keys then paste it into the provider estate.',70,'blocked','provider-estate','',now-5000,now-4000);
 goal.run('provider-estate-checklist','provider-estate:checklist','Customer accounts: 2 of 5 providers usable by customers, 3 to connect',['Customers can use these now (connect, check access, revoke from Aurekai):','GitHub: customers get organization. https://aurekai.work/connect/github-app','Neon: customers get project per tenant','','Not connected yet. Each link opens where the key is made; sign in with GitHub or Google if asked:','Twilio (gives customers subaccount under a parent account): https://console.twilio.com/us1/account/keys-credentials/api-keys','Shopify (gives customers client transfer store; collaborator access): https://aurekai.work/apps/connections?context=self&identity=gmail&work=051-gmail','Postmark (gives customers server per client, own token): https://account.postmarkapp.com/api_tokens'].join('\n'),60,'blocked','provider-estate-checklist','',now-6000,now-3000);
 goal.run('g-running','agent:crawl','Refresh the Dallas permit feed','Agent work.',50,'retry','agent-run:fixture',"RateLimitError: insufficient_quota; You have no credits remaining.",now-900,now-60);
 goal.run('g-stopped','mcp-event:1','Rebuild the search index after a schema change','Agent work.',40,'blocked','mcp-event','Index writer refused the old schema',now-20000,now-10000);
 goal.run('g-queued','seed:1','Summarize this week\'s permits','Agent work.',30,'queued','seed','',now-800,now-800);
 db.query('INSERT INTO runs(id,goal_id,state,summary,started_at,completed_at) VALUES(?,?,?,?,?,?)').run('r-running','g-running','running','',now-60,null);
 db.query('INSERT INTO runs(id,goal_id,state,summary,started_at,completed_at) VALUES(?,?,?,?,?,?)').run('r-stopped','g-stopped','failed','schema refused',now-10000,now-9990);
 db.query('UPDATE runs SET usage_json=? WHERE id=?').run(JSON.stringify({input_tokens:300,output_tokens:30}),'r-stopped');
 db.close();
}
function reindex(user){const r=Bun.spawnSync([queue,'account-work','index','--db',index,'--subject','user:'+user,'--scope','self','--owner','user:'+user,'--source','operator='+operator]);if(r.exitCode)throw Error(r.stderr.toString());}
beforeAll(async()=>{
 if(!existsSync(queue))throw Error('Build the real BonfyreQueue owner before this test.');
 mkdirSync(state,{recursive:true});seedOperator();
 for(const [k,v] of Object.entries({BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_SOURCES:'operator='+operator,BONFYRE_WORKSPACE_CLIENT:join(root,'integrations/browser'),BONFYRE_BIN_DIR:join(queue,'..')})){saved[k]=process.env[k];process.env[k]=v;}
 f=await packSetupFixture();
 owner=await f.login('inbox-owner@example.test');colleague=await f.login('inbox-colleague@example.test');stranger=await f.login('inbox-stranger@example.test');
 reindex(owner.id);{const estimates=new Database(index);estimates.query('INSERT INTO account_work_estimate VALUES(?,?,?,?,?,?)').run('account-operator-g-stopped','r-stopped',300,30,1250000,'fixture');estimates.close();}reindex(owner.id);ids={release:'account-operator-g-release',twilio:'account-operator-g-twilio',checklist:'account-operator-provider-estate-checklist'};
 // Real organizations: the owner runs one with an open invitation; a colleague's invites the owner.
 const create=async(user,name)=>{const page=await f.request('/organizations',{cookie:user.cookie});const form=fields(page.body,'data-org-create');form.name=name;const r=await f.request('/organizations',{cookie:user.cookie,form,headers:secure});if(r.status!==200)throw Error('Organization not created: '+r.status+r.body.slice(-400));return f.authsql.query('SELECT institution FROM institution_origins WHERE label=?').get(name).institution;};
 const invite=async(user,org,email)=>{const page=await f.request('/organizations?organization='+org,{cookie:user.cookie});const form={...fields(page.body,'data-org-invite'),email,role:'write',hours:'72'};const r=await f.request('/organizations',{cookie:user.cookie,form,headers:secure});if(r.status!==200)throw Error('Invitation not prepared: '+r.status+r.body.slice(-400));};
 ids.mine=await create(owner,'Harbor Cooperative');await invite(owner,ids.mine,'new-member@example.test');
 ids.theirs=await create(colleague,'River Guild');await invite(colleague,ids.theirs,'inbox-owner@example.test');
},60000);
afterAll(async()=>{await f?.close();for(const [k,v] of Object.entries(saved)){if(v===undefined)delete process.env[k];else process.env[k]=v;}rmSync(temp,{recursive:true,force:true});});

test('unconnected Inbox work sources are not reported as zero customer work',async()=>{
 const isolated=await packSetupFixture({beforeServe:({env})=>{delete env.BONFYRE_ACCOUNT_WORK_DB;delete env.BONFYRE_ACCOUNT_WORK_SOURCES;}});
 try{const login=await isolated.login('source-unavailable@example.test'),r=await isolated.request('/inbox',{cookie:login.cookie});
  expect(r.status).toBe(200);expect(r.body).toContain('data-account-inbox');
  expect(r.body).toContain('data-inbox-source-unavailable');expect(r.body).toContain('Inbox needs a source connection');
  expect(r.body).not.toContain('Nothing needs you right now');
  expect(r.body).toContain('href="/work-board?context=self"');
 }finally{await isolated.close();}
});

test('a signed-out reader keeps the public inbox surface',async()=>{
 const r=await f.request('/inbox');expect(r.body).not.toContain('data-account-inbox');
 expect((await f.request('/inbox?view=public',{cookie:owner.cookie})).body).not.toContain('data-account-inbox');
});
test('tracking links retain the signed-in native Inbox without changing account authority',async()=>{
 const url='/inbox?context=self&utm_source=chatgpt.com&utm_medium=chat';
 const own=await f.request(url,{cookie:owner.cookie});
 expect(own.status).toBe(200);expect(own.body).toContain('data-account-inbox');
 expect(own.body).toContain('data-context="self"');expect(own.body).toContain('Connect Twilio');
 const guest=await f.request(url);expect(guest.body).not.toContain('data-account-inbox');
 const foreign=await f.request(url,{cookie:stranger.cookie});expect(foreign.body).not.toContain('Connect Twilio');
 const unsupported=await f.request('/inbox?context=self&view=public',{cookie:owner.cookie});
 expect(unsupported.body).not.toContain('data-account-inbox');
});
test('the inbox separates what needs the person from agent work, with each one next step',async()=>{
 const r=await f.request('/inbox',{cookie:owner.cookie});expect(r.status).toBe(200);const b=r.body;
 for(const needle of ['data-account-inbox','data-needs-count="5"','data-agent-count="0"','Approve release '+release,'What it changes: Organizations get provider accounts','Last try:','name="action" value="approve"','name="back" value="inbox"',
  'Connect Twilio for customer subaccounts','href="https://console.twilio.com/us1/account/keys-credentials/api-keys" target="_blank"','value="email"','value="review"',
  'data-inbox-kind="provider-checklist"','Postmark','href="https://account.postmarkapp.com/api_tokens"','href="/apps/connections?context=self&amp;identity=gmail&amp;work=051-gmail"','href="/connect/github-app"',
  'data-inbox-kind="invitation"','Join River Guild','href="/organizations/join"','Harbor Cooperative','1 invitation not accepted yet','href="/organizations?organization='+ids.mine+'"',
  'data-inbox-kind="execution-credit"','OpenAI API credits are exhausted. Add credits before retrying this work.','Add OpenAI API credits','href="https://platform.openai.com/settings/organization/billing/"','Agent stopped','Rebuild the search index after a schema change'])expect(b).toContain(needle);
 expect(b).not.toContain('fixture-release-OLD1');
 const needsYou=b.slice(b.indexOf('id="needs-you"'),b.indexOf('id="sub-accounts"')),agents=b.slice(b.indexOf('id="agents"'));
 expect(needsYou).toContain('Refresh the Dallas permit feed');expect(needsYou).not.toContain('insufficient_quota');expect(needsYou).not.toContain('Retry');expect(needsYou).not.toContain('Rebuild the search index');expect(agents).not.toContain('Approve release');
 expect((await f.request('/inbox?context=institution%3Anot-mine',{cookie:owner.cookie})).status).toBe(403);
 const other=await f.request('/inbox',{cookie:stranger.cookie});expect(other.status).toBe(200);expect(other.body).toContain('Nothing needs you right now');expect(other.body).not.toContain('Approve release');
});
test('the rail reads real account Inbox action owners even when recipient notifications have no messages',async()=>{
 const read=async(cookie,context='self')=>{
  const response=await fetch(f.base+'/inbox/_summary?'+new URLSearchParams({context}),{
   headers:{Host:'aurekai.life',Accept:'application/msgpack',...(cookie?{Cookie:cookie}:{})}
  });
  return {status:response.status,value:response.status===200?decodePacket(new Uint8Array(await response.arrayBuffer())):null};
 };
 const mine=await read(owner.cookie);expect(mine.status).toBe(200);
 expect(mine.value[0]).toBe(1);expect(mine.value[1]).toBe(true);
 const value=mine.value[2];expect(value.context).toBe('self');expect(value.binding.actor).toBe('user:'+owner.id);
 expect(value.items).toHaveLength(5);
 for(const [kind,title] of [
  ['release','Approve release '+release],['provider','Connect Twilio for customer subaccounts'],
  ['provider-checklist','Customer accounts: 2 of 5 providers usable by customers, 3 to connect'],
  ['invitation','Join River Guild'],['execution-credit','Refresh the Dallas permit feed']
 ])expect(value.items.some(row=>row.kind===kind&&row.title===title)).toBe(true);
 const strangerRead=await read(stranger.cookie);expect(strangerRead.status).toBe(200);
 expect(strangerRead.value[2].items).toHaveLength(0);
 expect((await read('', 'self')).status).toBe(403);
 expect((await read(owner.cookie,'institution:not-mine')).status).toBe(403);
 const malformed=await fetch(f.base+'/inbox/_summary?context=self&done=approve',{headers:{Host:'aurekai.life',Cookie:owner.cookie}});
 expect(malformed.status).toBe(400);
});
test('the work board keeps every task and groups only person steps as needing the person',async()=>{
 const bare=await f.request('/work-board',{cookie:owner.cookie});expect(bare.status).toBe(200);expect(bare.body).toContain('data-context="self"');const r=await f.request('/work-board?context=self',{cookie:owner.cookie});expect(r.status).toBe(200);
 const people=decode(r.body.match(/data-person-tasks="([^"]*)"/)[1]).split(' ').sort();expect(people).toEqual([ids.checklist,ids.release,ids.twilio].sort());
 for(const phase of ['attention','active','stopped','pending','completed'])expect(r.body).toContain('data-board-phase="'+phase+'"');expect(r.body).toContain('href="/inbox?context=self"');expect(r.body).toContain('href="/s/models/all?context=self"');
});
test('task focus contains long diagnostics and labels progress as an update',async()=>{
 const db=new Database(operator),long='A retained detail <script>\n'+'evidence '.repeat(80),summary='Attempt details\n'+'result '.repeat(80);
 db.query("UPDATE goals SET state='resolved',last_error=? WHERE id='g-stopped'").run(long);db.query("UPDATE runs SET summary=? WHERE id='r-stopped'").run(summary);db.close();
 try{const r=await f.request('/work-board?context=self&task=account-operator-g-stopped',{cookie:owner.cookie});expect(r.status).toBe(200);
  expect(r.body).toContain('<h3>Latest update</h3>');expect(r.body).not.toContain('<h3>Why it stopped last time</h3>');
  expect((r.body.match(/<details class="work-note">/g)||[]).length).toBe(2);expect(r.body).toContain('A retained detail &lt;script&gt;');expect(r.body).toContain('Attempt details');expect(r.body).not.toContain('A retained detail <script>');
 }finally{const restore=new Database(operator);restore.query("UPDATE goals SET state='blocked',last_error='Index writer refused the old schema' WHERE id='g-stopped'").run();restore.query("UPDATE runs SET summary='schema refused' WHERE id='r-stopped'").run();restore.close();}
});
test('changing workspaces from a task opens the destination board without carrying the task',async()=>{
 const focused=await f.request('/work-board?context=self&task='+ids.twilio,{cookie:owner.cookie});expect(focused.status).toBe(200);
 const picker=focused.body.match(/<form class="work-board-picker"[\s\S]*?<\/form>/)?.[0]??'';
 expect(picker).toContain('name="context"');expect(picker).toContain('institution:'+ids.mine);
 expect(picker).not.toContain('name="task"');
 const destination=await f.request('/work-board?'+new URLSearchParams({context:'institution:'+ids.mine}),{cookie:owner.cookie});
 expect(destination.status).toBe(200);expect(destination.body).toContain('data-context="institution:'+ids.mine+'"');
 expect(destination.body).toContain('data-task=""');
});
test('Approve from the inbox writes the release approval once and returns to the inbox',async()=>{
 const post=(form,cookie=owner.cookie)=>f.request('/work-board/act',{cookie,form,headers:secure});
 expect((await post({context:'self',task:ids.release,action:'approve',back:'elsewhere'})).status).toBe(400);
 expect((await post({context:'self',task:ids.release,action:'approve',back:'inbox'},stranger.cookie)).status).toBe(404);
 expect(existsSync(join(approvals,release+'.approved'))).toBe(false);
 if(proof)await browserProof();else{const r=await post({context:'self',task:ids.release,action:'approve',back:'inbox'});expect(r.status).toBe(303);expect(r.headers.get('location')).toBe('/inbox?context=self&done=approve#'+ids.release);}
 const approval=readFileSync(join(approvals,release+'.approved'),'utf8');expect(approval).toContain('release\t'+release+'\n');expect(approval).toContain('actor\tuser:'+owner.id+'\n');
 expect((await post({context:'self',task:ids.release,action:'approve',back:'inbox'})).status).toBe(409);
 const after=await f.request('/inbox',{cookie:owner.cookie});expect(after.body).toContain('Approved by you');expect(after.body).toContain('Going live now');expect(after.body).not.toContain('Approve and put live');
},90000);
test('resolved release reports its real activation receipt without inviting another approval',async()=>{
 const db=new Database(operator);db.query('UPDATE goals SET state=?,last_error=?,updated_at=? WHERE id=?').run('resolved','Live since Thu 29 Oct 2026 17:47',Date.now()/1000,'g-release');db.close();
 reindex(owner.id);
 const r=await f.request('/work-board?context=self&task='+ids.release,{cookie:owner.cookie});expect(r.status).toBe(200);
 const focus=r.body.match(/<section class="work-focus"[\s\S]*?<\/section>/)?.[0]??'';
 expect(focus).toContain('<h2 id="work-focus-title">Release live: '+release+'</h2>');
 expect(focus).toContain('<summary>Original release request</summary>');
 expect(focus).toContain('<h3>Activation receipt</h3>');expect(focus).toContain('Live since Thu 29 Oct 2026 17:47');
 expect(focus).not.toContain('name="action" value="approve"');
 expect(r.body).toContain('Release completed: ');
 expect(decode(r.body.match(/data-person-tasks="([^"]*)"/)[1]).split(' ')).not.toContain(ids.release);
 const other=await f.request('/work-board?context=self&task='+ids.release,{cookie:stranger.cookie});expect(other.body).not.toContain('Release live: '+release);
});
// Real Chrome over TLS (the session cookie is Secure): screenshots at both
// widths, then the Approve click itself.
async function browserProof(){
 mkdirSync(proof,{recursive:true});
 const port=await freePort(),origin='https://aurekai.life:'+port,cert=Bun.spawnSync(['/usr/bin/openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',join(temp,'key.pem'),'-out',join(temp,'cert.pem'),'-days','1','-subj','/CN=aurekai.life'],{stdout:'pipe',stderr:'pipe'});if(cert.exitCode)throw Error('No TLS fixture');
 const server=createTLS({key:readFileSync(join(temp,'key.pem')),cert:readFileSync(join(temp,'cert.pem'))},async(req,res)=>{try{const parts=[];for await(const c of req)parts.push(c);const headers=new Headers();for(const [k,v] of Object.entries(req.headers))if(v!==undefined&&!['connection','transfer-encoding','content-length'].includes(k))headers.set(k,Array.isArray(v)?v.join(','):v);headers.set('Host','aurekai.life');if(headers.get('origin')===origin)headers.set('origin','https://aurekai.life');
  const r=await fetch(f.base+req.url,{method:req.method,headers,body:parts.length?Buffer.concat(parts):undefined,redirect:'manual'});const out=Object.fromEntries([...r.headers].filter(([k])=>!['connection','transfer-encoding','set-cookie','content-length','content-encoding'].includes(k)));res.writeHead(r.status,out);res.end(Buffer.from(await r.arrayBuffer()));}catch{res.statusCode=502;res.end();}});
 await new Promise(r=>server.listen(port,'127.0.0.1',r));
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 const browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,userDataDir:join(temp,'chrome'),args:['--ignore-certificate-errors','--host-resolver-rules=MAP aurekai.life 127.0.0.1','--no-proxy-server','--disable-gpu','--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));const [name,value]=owner.cookie.split('=');
  await page.setCookie({name,value,domain:'aurekai.life',path:'/',secure:true,httpOnly:true});
  for(const width of [1440,390]){
   await page.setViewport({width,height:width>500?1000:900});await page.goto(origin+'/inbox',{waitUntil:'networkidle2'});await page.waitForSelector('[data-account-inbox]');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   const colours=await page.evaluate(()=>[getComputedStyle(document.querySelector('[data-needs=you] .ak-inbox-who')).backgroundColor,getComputedStyle(document.querySelector('[data-needs=agent] .ak-inbox-who')).backgroundColor]);expect(colours[0]).not.toBe(colours[1]);
   await page.screenshot({path:join(proof,'inbox-'+width+'.png'),fullPage:true});
   await page.goto(origin+'/work-board?context=self',{waitUntil:'networkidle2'});await page.waitForFunction(()=>/Showing/.test(document.querySelector('[data-board-status]')?.textContent||''),{timeout:20000});
   expect(await page.$('[data-board-collection] .ac-collection')).not.toBeNull();expect(await page.$('[data-board-collection] .ac-row-icon svg')).not.toBeNull();
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await page.type('[data-board-collection] input[type=search]','Approve release');
   expect(await page.$$eval('[data-board-collection] .ac-row',rows=>rows.every(row=>row.textContent.includes('Approve release')))).toBe(true);
   await page.click('[data-board-collection] .ac-row .ac-title');await page.waitForSelector('[data-board-collection] .work-inspect-mark svg');
   expect(await page.$eval('[data-board-collection] .ac-inspector a',a=>a.textContent)).toBe('Open full task');
   await page.click('[data-board-collection] [data-inspect-back]');
   await page.$eval('[data-board-collection] input[type=search]',input=>{input.value='';input.dispatchEvent(new Event('input',{bubbles:true}));});
   await page.type('[data-board-collection] input[type=search]','Rebuild the search index');await page.click('[data-board-collection] .ac-row .ac-title');
   await page.waitForFunction(()=>document.querySelector('[data-board-collection] .ac-inspector')?.textContent.includes('Estimated cost: $1.25 for 1 of 1 runs (not an invoice)'));
   expect(await page.$$eval('[data-board-phase=attention] .work-board-row',n=>n.map(x=>x.dataset.taskId).sort())).toEqual([ids.checklist,ids.release,ids.twilio].sort());
   expect(await page.$$eval('[data-board-phase=stopped] .work-board-row',n=>n.map(x=>x.querySelector('a').textContent))).not.toContain('Approve release '+release);
   await page.screenshot({path:join(proof,'inbox-workboard-'+width+'.png'),fullPage:true});
  }
  await page.setViewport({width:1440,height:1000});await page.goto(origin+'/inbox',{waitUntil:'networkidle2'});
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle2'}),page.click('[data-inbox-kind=release] [data-inbox-act=approve]')]);
  expect(new URL(page.url()).pathname).toBe('/inbox');await page.waitForSelector('[data-inbox-receipt]');
  await page.screenshot({path:join(proof,'inbox-approved-1440.png'),fullPage:true});
  await page.setViewport({width:390,height:900});await page.reload({waitUntil:'networkidle2'});await page.screenshot({path:join(proof,'inbox-approved-390.png'),fullPage:true});
  expect(errors).toEqual([]);
 }finally{await browser.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
}
