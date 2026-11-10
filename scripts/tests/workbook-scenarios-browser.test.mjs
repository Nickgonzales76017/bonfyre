import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync,mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {workbookSnapshot} from '../../components/BonfyreServiceSite/workbook-model.mjs';
import {calculateWorkbook} from '../../components/BonfyreServiceSite/workbook-calculation.mjs';
import {workbookReviewValue,readWorkbookReview} from '../../components/BonfyreServiceSite/workbook-record.mjs';
import {projectFinanceWorkspaceSetup,publishSetups} from '../publish_experience_setups.mjs';
import {createWorkspaceMcp} from '../../integrations/mcp/workspace_mcp.mjs';
import * as XLSX from '../../components/BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
let f,binding,browser,temp,source,call;const key='finance-workbook:scenarios-browser';
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'scenarios-browser-'));const pack=await workbookPackage({packageRoot:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');f=await workspaceFixture();
 publishSetups({records:projectFinanceWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});binding=await f.install('experience.finance-workbooks.workspace','institution:workspace-team');
 const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[100],[60],[{t:'n',f:'A1-A2',v:40}],[2],[{t:'n',f:'A3/A4',v:20}]]),'Model');
 XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([['Description','Amount','Account'],['Supplies',20,'Office']]),'Transactions');
 const calculated=await calculateWorkbook(workbookSnapshot(book,{name:'Operating model'}));source={name:'original.xlsx',original:new Blob([XLSX.write(book,{type:'array',bookType:'xlsx'})],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'})};
 await f.store(binding).saveLocal(key,workbookReviewValue(calculated.snapshot,source),0);
 call=createWorkspaceMcp({ownerUrl:f.base,publicOrigin:f.base});
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
test('native assumption sets preview real worker results, refuse formula errors and save only an explicitly applied candidate',async()=>{
 const{page,context}=await open(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  for(const[name,cell]of [['Sales','A1'],['Costs','A2'],['Months','A4']]){await fill(page,'Driver name',name);await fill(page,'Driver cell',cell);await act(page,'add-driver');await notice(page,'Driver added');}
  await fill(page,'Sales value',140);await fill(page,'Costs value',65);await fill(page,'Assumption set name','Growth');await act(page,'save-set');await notice(page,'Assumption set added');
  await save(page);const retained=await read();expect(retained.revision).toBe(2);expect(retained.scenarios.sets[0].name).toBe('Growth');expect(retained.snapshot.sheets['sheet-0'].cellData[0][0].v).toBe(100);
  await act(page,'preview');await notice(page,'Preview ready');
  const summary=await page.$eval('[data-scenario-results]',n=>n.textContent);expect(summary).toContain('37.5');expect(summary).toContain('Model!A3');expect((await read()).revision).toBe(2);
  for(const width of [1440,390]){await page.setViewport({width,height:1000});await page.waitForFunction(()=>document.documentElement.scrollHeight<5000);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const out=process.env.BONFYRE_SCENARIO_PROOF;if(out){mkdirSync(out,{recursive:true});await page.screenshot({path:join(out,'scenario-'+width+'.png'),fullPage:true});}}
  await page.$eval('[aria-label="Sales value"]',n=>{n.value='150';n.dispatchEvent(new Event('input',{bubbles:true}));});
  expect(await page.$eval('[data-scenario-action=apply]',n=>n.hidden)).toBe(true);expect(await page.$eval('[data-scenario-action=preview]',n=>n.disabled)).toBe(true);
  await page.$eval('[aria-label="Saved assumption set"]',n=>n.dispatchEvent(new Event('change',{bubbles:true})));
  expect(await page.$eval('[aria-label="Sales value"]',n=>n.value)).toBe('140');
  await act(page,'preview');await notice(page,'Preview ready');
  await act(page,'apply');await notice(page,'Applied Growth');await save(page);const applied=await read();expect(applied.revision).toBe(3);expect(applied.snapshot.sheets['sheet-0'].cellData[4][0].v).toBe(37.5);expect(applied.snapshot.sheets['sheet-0'].cellData[2][0].f).toBe('=A1-A2');expect(await applied.source.original.arrayBuffer()).toEqual(await source.original.arrayBuffer());
  await fill(page,'Months value',0);await fill(page,'Assumption set name','Invalid duration');await act(page,'save-set');await notice(page,'Assumption set added');await act(page,'preview');await notice(page,'scenario calculation errors');
  expect(await page.$eval('[data-scenario-action=apply]',n=>n.hidden)).toBe(true);expect((await read()).revision).toBe(3);expect((await read()).snapshot.sheets['sheet-0'].cellData[3][0].v).toBe(2);
  expect(errors).toEqual([]);
 }finally{page.on('dialog',d=>d.accept());await context.close();}
},90000);
test('saved sets reopen on a phone, survive exact MCP transaction edits and stale browser saves refuse replacement',async()=>{
 const{page,context}=await open(390);
 try{
  expect(await page.$eval('[aria-label="Saved assumption set"]',n=>n.textContent)).toContain('Growth');
  const before=await read(),session=decodeURIComponent(f.owner.cookie.slice(f.owner.cookie.indexOf('=')+1)),identity={binding:binding.id,key};
  const current=await call('workbook_review',identity,session),args={...identity,expected_revision:current.revision,sha256:current.sha256,edits:[{row:1,role:'amount',value:25}]};
  const preview=await call('workbook_preview',args,session);await call('workbook_update',{...args,review_time:preview.review_time,preview_sha256:preview.preview_sha256,request_id:'preserve-scenarios'},session);
  const updated=await read();expect(updated.scenarios).toEqual(before.scenarios);expect(updated.snapshot.sheets['sheet-1'].cellData[1][1].v).toBe(25);
  await fill(page,'Assumption set name','Browser draft');await act(page,'save-set');await notice(page,'Assumption set added');
  await page.click('[data-workbook-action=save]');await page.waitForFunction(()=>!document.querySelector('[data-workbook-action=save]').disabled);expect(await page.$eval('.finance-workbook-status',n=>n.textContent)).not.toContain('Saved review');
  const after=await read();expect(after.revision).toBe(updated.revision);expect(after.scenarios).toEqual(updated.scenarios);
  expect(await page.$eval('[aria-label="Saved assumption set"]',n=>n.textContent)).toContain('Browser draft');
  page.on('dialog',d=>d.accept());await page.click('[data-workbook-action=reload]');await page.waitForFunction(()=>document.querySelector('.finance-workbook-status').textContent.startsWith('Opened revision'));
  expect(await page.$eval('[aria-label="Saved assumption set"]',n=>n.textContent)).not.toContain('Browser draft');
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await context.close();}
},90000);
