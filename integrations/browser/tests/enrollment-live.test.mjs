// Current enrollment UI, installed extension, full native-messaging host,
// Queue and Credentials in a throwaway home. Provider HTTP is a local fixture.
// This does not sign into a provider or establish verified provider access.
import {test,expect} from 'bun:test';
import {existsSync,mkdirSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import * as enrollment from '../onboarding/enrollment.mjs';

const repo=resolve(import.meta.dir,'../../..'),extension=join(repo,'integrations/browser/extension');
const quote=value=>"'"+String(value).replaceAll("'","'\\''")+"'";
const workId='002-gmail',mission='enroll-'+workId,providerOrigin='https://127.0.0.1';

test('current enrollment card retains exact native work through browser close, stale review, lease expiry and waiting',async()=>{
 const chromePath=process.env.BONFYRE_EXTENSION_CHROMIUM_BINARY||process.env.BONFYRE_TEST_BROWSER;
 if(!chromePath)throw Error('Set BONFYRE_EXTENSION_CHROMIUM_BINARY to full Chrome; this native lifecycle witness must not silently skip.');
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');
 const home=mkdtempSync(join(tmpdir(),'enrollment-live-current-')),root=join(home,'enrollment'),profile=join(home,'browser'),providers=join(home,'providers.json'),credentialKey=join(home,'credentials.key'),launcher=join(home,'native-host.sh');
 mkdirSync(root,{recursive:true});mkdirSync(profile,{recursive:true});
 const env={HOME:home,PATH:process.env.PATH||'/usr/bin:/bin',LANG:'en_US.UTF-8',BONFYRE_ENROLLMENT_HOME:root,BONFYRE_ENROLLMENT_DB:join(root,'queue.db'),BONFYRE_QUEUE_BINARY:process.env.BONFYRE_QUEUE_BINARY||join(repo,'cmd/BonfyreQueue/bonfyre-queue'),BONFYRE_CREDENTIALS_BINARY:process.env.BONFYRE_CREDENTIALS_BINARY||join(repo,'cmd/BonfyreCredentials/bonfyre-credentials'),BONFYRE_CREDENTIAL_PROVIDERS:providers,BONFYRE_CREDENTIALS_PROVIDERS:providers,BONFYRE_CREDENTIALS_DB:join(home,'credentials.db'),BONFYRE_CREDENTIALS_KEY:credentialKey,BONFYRE_FOREIGN_ROOT:join(home,'foreign'),BONFYRE_PROJECT_BINARY:'/usr/bin/false'};
 const config=enrollment.config(env),view=()=>enrollment.inspect('gmail',workId,config),review=()=>({work_id:workId,revision:view().revision});
 const sql=(statement,...args)=>{const db=new Database(config.db);try{return db.query(statement).all(...args);}finally{db.close();}};
 const state=()=>sql('SELECT mission_id,status,attempt FROM workgraph_nodes ORDER BY mission_id');
 const held=()=>JSON.parse(readFileSync(join(root,'leases/gmail.json'),'utf8'));
 const run=(binary,args)=>{const result=Bun.spawnSync([binary,...args],{env,stdout:'pipe',stderr:'pipe',timeout:20000});if(result.exitCode!==0)throw Error('The isolated native fixture failed: '+result.stderr.toString().slice(0,500));return result.stdout.toString();};
 let browser;const errors=[],unexpected=[];
 try{
  expect(existsSync(config.queue)).toBe(true);expect(existsSync(config.credentials)).toBe(true);
  writeFileSync(providers,JSON.stringify([{provider:'fixture',name:'Fixture',purpose:'isolated lifecycle witness',get_it:providerOrigin+'/login',fields:[{field:'token',label:'Fixture token',kind:'secret',sensitive:true,required:true,generator:'',help:'Synthetic fixture value only'}],apply:{headers:[],query:[]}}]),{mode:0o600});
  const rows=[];for(let n=1;n<=3;n++)for(const identity of ['gmail','proton']){const id=String(n).padStart(3,'0'),item={work_id:id+'-'+identity,identity,account:enrollment.IDENTITIES[identity],platform:'Fixture '+id,signup_url:providerOrigin+'/login',credential_provider:'fixture'};rows.push(['enroll-'+item.work_id,'account.enrollment',JSON.stringify(item),'enrollment-'+identity,500,40,600000].join('\t'));}
  const batch=join(home,'seed.tsv');writeFileSync(batch,rows.join('\n')+'\n');run(config.queue,['enqueue-batch',batch,'--db',config.db]);run(config.credentials,['init-key','--key',credentialKey]);
  // Run the production host CLI and its framing loop. The launcher only gives
  // that process temporary database, key, Queue and provider declaration paths.
  // Unrelated page-history reads have no Project backend in this fixture.
  writeFileSync(launcher,'#!/bin/sh\nexec /usr/bin/env -i '+Object.entries(env).map(([name,value])=>name+'='+quote(value)).join(' ')+' '+quote(process.execPath)+' --no-env-file --no-install '+quote(join(repo,'integrations/browser/host/bonfyre-browser-host.mjs'))+'\n',{mode:0o700});
  browser=await chromium.launchPersistentContext(profile,{headless:true,executablePath:chromePath,args:['--no-sandbox','--disable-background-networking','--disable-extensions-except='+extension,'--load-extension='+extension],ignoreDefaultArgs:['--disable-extensions']});
  const worker=browser.serviceWorkers()[0]||await browser.waitForEvent('serviceworker',{timeout:15000}),extensionId=new URL(worker.url()).hostname;
  console.log('ENROLLMENT_EXTENSION '+JSON.stringify({extensionId,extension,chromePath}));
  mkdirSync(join(profile,'NativeMessagingHosts'),{recursive:true});writeFileSync(join(profile,'NativeMessagingHosts/com.bonfyre.browser_host.json'),JSON.stringify({name:'com.bonfyre.browser_host',description:'Temporary full-host enrollment witness',path:launcher,type:'stdio',allowed_origins:['chrome-extension://'+extensionId+'/']}));
  await browser.route('**/*',async route=>{const address=new URL(route.request().url());if(address.protocol==='chrome-extension:'&&address.hostname===extensionId)return route.continue();if(address.origin===providerOrigin)return route.fulfill({status:200,contentType:'text/html',body:'<!doctype html><meta charset="utf-8"><h1>Enrollment provider fixture</h1><a href="/mfa">Continue to MFA fixture</a><label>Unsubmitted fixture value<input aria-label="Unsubmitted fixture value"></label>'});if(['http:','https:'].includes(address.protocol))unexpected.push(address.origin);return route.abort();});
  const panelAt=async()=>{const page=await browser.newPage(),failures=[],consoleErrors=[];page.setDefaultTimeout(10000);page.on('pageerror',error=>errors.push(error.message));page.on('requestfailed',request=>{if(failures.length<12)failures.push({url:request.url(),failure:request.failure()?.errorText});});page.on('console',message=>{if(message.type()==='error'&&consoleErrors.length<12)consoleErrors.push(message.text().slice(0,300));});await page.goto('chrome-extension://'+extensionId+'/panel.html');try{await page.waitForFunction(()=>typeof window.aurekaiAsk==='function'&&document.querySelector('#enroll .enroll-actions button'));}catch(error){const dom=await page.evaluate(()=>({url:location.href,ask:typeof window.aurekaiAsk,ready:document.readyState,scripts:[...document.scripts].map(script=>script.src),enroll:document.getElementById('enroll')?.textContent.slice(0,1000),said:document.getElementById('enroll-said')?.textContent}));throw Error(error.message+' '+JSON.stringify({errors,failures,consoleErrors,dom}));}return page;};
  const said=(page,text)=>page.locator('#enroll-said').filter({hasText:text}).waitFor();
  const expand=async page=>{const button=page.getByRole('button',{name:'Expand setup details',exact:true});if(await button.count())await button.click();};
  const before=state();let panel=await panelAt();expect(state()).toEqual(before);
  await panel.getByRole('button',{name:'Find a setup',exact:true}).click();await panel.getByRole('button',{name:'Review Fixture 002 · '+workId,exact:true}).click();await said(panel,'Saved setup opened for review.');expect(state()).toEqual(before);expect(view()).toMatchObject({status:'ready',revision:0,read_only:true});
  await panel.getByRole('button',{name:'Use this account',exact:true}).click();await said(panel,'This reviewed setup is held.');expect(view()).toMatchObject({status:'running',revision:0});const originalLease=held();expect(originalLease.mission).toBe(mission);expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
  await expand(panel);await panel.getByRole('button',{name:'Checked for existing access',exact:true}).click();await said(panel,'Recorded as your observation');expect(view().revision).toBe(1);expect(view().stages.existing_access_checked.evidence_kind).toBe('human_assertion');
  const synthetic='synthetic-enrollment-ui-key-7931';await panel.getByLabel('Fixture token',{exact:true}).fill(synthetic);await panel.getByRole('button',{name:'Seal this credential',exact:true}).click();await said(panel,'Sealed by BonfyreCredentials.');expect(view().revision).toBe(2);expect(view().stages.credential_sealed.evidence_kind).toBe('native_seal');expect(JSON.stringify(view())).not.toContain(synthetic);expect(readFileSync(config.db).includes(Buffer.from(synthetic))).toBe(false);
  const credentials=new Database(config.credentialsDb,{readonly:true});try{expect(credentials.query('SELECT account,provider,field FROM sealed_field').all()).toEqual([{account:enrollment.IDENTITIES.gmail,provider:'fixture',field:'token'}]);}finally{credentials.close();}
  expect(await panel.getByRole('button',{name:'Complete verified capability',exact:true}).isDisabled()).toBe(true);
  const opened=browser.waitForEvent('page',{timeout:10000});await panel.getByRole('button',{name:'Take over to sign in',exact:true}).click();let provider=await opened;await provider.waitForURL(providerOrigin+'/login');await provider.getByRole('link',{name:'Continue to MFA fixture',exact:true}).click();await provider.waitForURL(providerOrigin+'/mfa');await provider.getByLabel('Unsubmitted fixture value').fill('public fixture text');
  await panel.bringToFront();await panel.getByRole('button',{name:'Take over to sign in',exact:true}).click();await said(panel,'Returned to the existing provider page.');expect(provider.url()).toBe(providerOrigin+'/mfa');expect(await provider.getByLabel('Unsubmitted fixture value').inputValue()).toBe('public fixture text');expect(view().revision).toBe(2);
  await panel.close();expect(provider.isClosed()).toBe(false);panel=await panelAt();await panel.getByRole('button',{name:'Continue native enrollment',exact:true}).click();await said(panel,'Setup restored for review.');expect(view()).toMatchObject({status:'running',revision:2,item:{work_id:workId}});expect(held().token).toBe(originalLease.token);
  await expand(panel);expect(await panel.locator('#enroll').textContent()).toContain(workId+' · revision 2');expect(await panel.getByLabel('Fixture token',{exact:true}).count()).toBe(0);await panel.getByRole('button',{name:'Use this account',exact:true}).click();await said(panel,'This reviewed setup is held.');expect(held().token).toBe(originalLease.token);
  // A second native actor advances the real checkpoint while this card still
  // holds revision 2. Its next write must be refused by the production host.
  enrollment.record('gmail','account_bound','Competing fixture observation',config,review());expect(view().revision).toBe(3);await panel.getByRole('button',{name:'Confirmed project / zone / scope',exact:true}).click();await said(panel,/stale|changed/i);expect(view().revision).toBe(3);expect(view().stages.scope_confirmed).toBeUndefined();
  await panel.getByRole('button',{name:'Reload native item',exact:true}).click();await said(panel,'Setup restored for review.');expect(await panel.locator('#enroll').textContent()).toContain(workId+' · revision 3');
  // Expire only this disposable Queue lease; reclamation still goes through
  // the native UI, host, renew refusal, reap and exact-mission claim.
  sql('UPDATE workgraph_nodes SET lease_expires_at_ms=1 WHERE mission_id=?',mission);await panel.getByRole('button',{name:'Use this account',exact:true}).click();await said(panel,'This reviewed setup is held.');expect(held().mission).toBe(mission);expect(held().token).not.toBe(originalLease.token);expect(view()).toMatchObject({status:'running',revision:3});expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
  await expand(panel);await panel.getByRole('button',{name:'Waiting for email',exact:true}).click();await said(panel,'Waiting saved.');expect(view()).toMatchObject({status:'retry_wait',revision:4,resumable:false,item:{work_id:workId}});expect(provider.isClosed()).toBe(false);expect(await panel.getByRole('button',{name:'Use this account',exact:true}).isDisabled()).toBe(true);expect(state().filter(row=>row.mission_id!==mission).every(row=>row.status==='ready'&&row.attempt===0)).toBe(true);
  await provider.close();await panel.close();panel=await panelAt();await panel.getByRole('button',{name:'Continue native enrollment',exact:true}).click();await said(panel,'Setup restored for review.');expect(view()).toMatchObject({status:'retry_wait',revision:4,item:{work_id:workId}});await expand(panel);expect(await panel.locator('#enroll').textContent()).toContain(workId+' · revision 4');expect(view().missing).toContain('credential_verified');expect(view().missing).toContain('consumer_bound');expect(state().filter(row=>row.mission_id!==mission).every(row=>row.status==='ready'&&row.attempt===0)).toBe(true);
  expect(unexpected).toEqual([]);expect(errors).toEqual([]);
 }finally{await browser?.close();rmSync(home,{recursive:true,force:true});}
},120000);
