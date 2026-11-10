import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync,mkdirSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {workbookSnapshot} from '../../components/BonfyreServiceSite/workbook-model.mjs';
import {calculateWorkbook} from '../../components/BonfyreServiceSite/workbook-calculation.mjs';
import {workbookReviewValue,readWorkbookReview} from '../../components/BonfyreServiceSite/workbook-record.mjs';
import {projectFinanceWorkspaceSetup,publishSetups} from '../publish_experience_setups.mjs';
import {createWorkspaceMcp} from '../../integrations/mcp/workspace_mcp.mjs';
import * as XLSX from '../../components/BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
let f,binding,browser,temp,source,call;const key='finance-workbook:account-browser';
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'account-browser-'));const pack=await workbookPackage({packageRoot:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');f=await workspaceFixture();
 publishSetups({records:projectFinanceWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});binding=await f.install('experience.finance-workbooks.workspace','institution:workspace-team');
 const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[100],[60],[{t:'n',f:'A1-A2',v:40}],[2],[{t:'n',f:'A3/A4',v:20}]]),'Model');
 XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([['Description','Amount','Account'],['Supplies',20,'Office']]),'Transactions');
 const calculated=await calculateWorkbook(workbookSnapshot(book,{name:'Operating model'}));source={name:'original.xlsx',original:new Blob([XLSX.write(book,{type:'array',bookType:'xlsx'})],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'})};
 await f.store(binding).saveLocal(key,workbookReviewValue(calculated.snapshot,source),0);
 call=createWorkspaceMcp({ownerUrl:f.base,publicOrigin:f.base});
 writeFileSync(join(temp,'original.xlsx'),new Uint8Array(await source.original.arrayBuffer()));
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{try{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});}finally{for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;}});
async function open(width=1440){
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(25000);await page.setViewport({width,height:1000});
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});await page.goto(f.base+binding.path+'?workbook='+encodeURIComponent(key),{waitUntil:'networkidle0'});await page.waitForSelector('[data-workbook-editor][data-ready=true]');await page.click('[data-workbook-action=scenarios]');return{context,page};
}
const fill=(page,label,value)=>page.$eval('[aria-label="'+label+'"]',(n,value)=>{n.value=value;},String(value));
const act=(page,name)=>page.click('[data-scenario-action='+name+']');
const notice=(page,text)=>page.waitForFunction(text=>document.querySelector('[data-scenario-status]').textContent.includes(text),{},text);
const read=async()=>readWorkbookReview(await f.store(binding).readVersion(key));
async function save(page){await page.click('[data-workbook-action=save]');await page.waitForFunction(()=>document.querySelector('.finance-workbook-status').textContent.startsWith('Saved review'));}

async function blank(){
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(20000);
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});return{context,page};
}
const closed=page=>page.waitForSelector('[data-workbook-editor][data-closed=true]');
test('an account change clears the loaded private view and held inputs without saving or enabling old controls',async()=>{
 const{context,page}=await open(),before=await read(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  await fill(page,'Driver name','Private Sales');await fill(page,'Driver cell','A1');await act(page,'add-driver');await notice(page,'Driver added');
  await page.evaluate(()=>{window.oldPrivateInput=document.querySelector('[aria-label="Private Sales value"]');window.oldSave=document.querySelector('[data-workbook-action=save]');window.dispatchEvent(new Event('bonfyre:account-changed'));});
  await closed(page);expect(await page.evaluate(()=>oldPrivateInput.value)).toBe('');expect(await page.evaluate(()=>oldSave.disabled)).toBe(true);
  expect(await page.$eval('[data-workbook-editor]',n=>n.textContent)).not.toContain('Operating model');expect(await page.$('[data-workbook-action=export]')).toBeNull();
  await page.evaluate(()=>oldSave.click());expect((await read()).revision).toBe(before.revision);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
test('an account change during the initial native read prevents delayed material from reopening the editor',async()=>{
 const{context,page}=await blank();let held,first=true;const captured=new Promise(resolve=>held=resolve);
 await page.setRequestInterception(true);page.on('request',request=>{const u=new URL(request.url());if(first&&u.pathname.endsWith('/_records')&&u.searchParams.get('operation')==='read'&&u.searchParams.get('prefix')===key){first=false;held(request);}else request.continue().catch(()=>{});});
 try{
  const navigation=page.goto(f.base+binding.path+'?workbook='+encodeURIComponent(key),{waitUntil:'domcontentloaded'}),request=await captured;
  await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));await request.continue().catch(()=>{});await navigation;await closed(page);
  expect(await page.$('[data-workbook-editor][data-ready=true]')).toBeNull();expect(await page.$eval('[data-workbook-editor]',n=>n.textContent)).not.toContain('Operating model');
 }finally{await context.close();}
},40000);
test('page lifecycle invalidation remains closed on return and native reload checks the new account',async()=>{
 const{context,page}=await open(),before=await read();
 try{
  await page.evaluate(()=>{window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));});await closed(page);
  expect(await page.$('[data-workbook-action=save]')).toBeNull();
  const[name,...v]=f.other.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  const navigation=page.waitForNavigation({waitUntil:'domcontentloaded'});await page.click('[data-workbook-editor] button');const response=await navigation;
  expect(response.status()).toBe(403);expect((await read()).revision).toBe(before.revision);
 }finally{await context.close();}
},40000);
test('an account change while the import worker is loading clears the chosen file and creates no native record',async()=>{
 const{context,page}=await blank();let held;const captured=new Promise(resolve=>held=resolve);const posts=[];
 await page.setRequestInterception(true);page.on('request',request=>{if(request.method()==='POST')posts.push(request.url());if(request.url().endsWith('/workbook-import-worker.js'))held(request);else request.continue().catch(()=>{});});
 try{
  await page.goto(f.base+binding.path,{waitUntil:'networkidle0'});const input=await page.$('[data-workbook-file]');await input.uploadFile(join(temp,'original.xlsx'));await page.click('[data-workbook-open] button');const request=await captured;
  await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));await request.continue().catch(()=>{});await closed(page);
  expect(await page.$eval('[data-workbook-file]',n=>n.value)).toBe('');expect(await page.$('[data-workbook-editor][data-ready=true]')).toBeNull();expect(posts).toEqual([]);
 }finally{await context.close();}
},40000);
test('closing during a real native save never retries or presents its delayed acknowledgement as current',async()=>{
 const{context,page}=await blank();
 await page.evaluateOnNewDocument(()=>{
  const request=window.fetch.bind(window);window.nativeWrites=0;
  window.fetch=async(url,options)=>{const response=await request(url,options);if(new URL(url,location.href).searchParams.get('operation')==='write'){window.nativeWrites++;window.nativeWriteCompleted=true;await new Promise(resolve=>window.releaseSaveReply=resolve);}return response;};
 });
 try{
  await page.goto(f.base+binding.path+'?workbook='+encodeURIComponent(key),{waitUntil:'networkidle0'});await page.waitForSelector('[data-workbook-editor][data-ready=true]');await page.click('[data-workbook-action=scenarios]');
  await fill(page,'Driver name','Sales');await fill(page,'Driver cell','A1');await act(page,'add-driver');await notice(page,'Driver added');const before=await read();
  await page.click('[data-workbook-action=save]');await page.waitForFunction(()=>window.nativeWriteCompleted===true);
  await page.evaluate(()=>{window.dispatchEvent(new Event('bonfyre:account-changed'));window.releaseSaveReply();});await closed(page);
  const after=await read();expect(after.revision).toBe(before.revision+1);expect(after.scenarios.drivers[0].name).toBe('Sales');expect(await page.evaluate(()=>nativeWrites)).toBe(1);
  expect(await page.$eval('[data-workbook-editor]',n=>n.textContent)).not.toContain('Saved review');expect(await after.source.original.arrayBuffer()).toEqual(await source.original.arrayBuffer());
 }finally{await context.close();}
},40000);

test('account invalidation cancels a pending analysis worker without adopting late output',async()=>{
 const{context,page}=await open(),before=await read();let held;const captured=new Promise(resolve=>held=resolve);
 await page.setRequestInterception(true);page.on('request',request=>{if(request.url().endsWith('/workbook-scenario-worker.js'))held(request);else request.continue().catch(()=>{});});
 try{
  if(!before.scenarios?.drivers.length){await fill(page,'Driver name','Sales');await fill(page,'Driver cell','A1');await act(page,'add-driver');await notice(page,'Driver added');}await page.click('.finance-scenarios summary');await fill(page,'Analysis result cell','A5');await page.click('[data-analysis-action=sensitivity]');const request=await captured;
  await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));await request.continue().catch(()=>{});await closed(page);expect(await page.$('[data-analysis-results]')).toBeNull();expect((await read()).revision).toBe(before.revision);
 }finally{await context.close();}
},40000);
