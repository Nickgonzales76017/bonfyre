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
let f,binding,browser,temp,source,call;const key='finance-workbook:analysis-browser';
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'analysis-browser-'));const pack=await workbookPackage({packageRoot:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');f=await workspaceFixture();
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

const analyze=(page,name)=>page.click('[data-analysis-action='+name+']');
const analysisNotice=(page,text)=>page.waitForFunction(text=>document.querySelector('[data-analysis-status]').textContent.includes(text),{},text);
test('sensitivity and a goal result use the real native worker and only enter the workbook through reviewed assumption sets',async()=>{
 const{page,context}=await open(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  await fill(page,'Driver name','Sales');await fill(page,'Driver cell','A1');await act(page,'add-driver');await notice(page,'Driver added');
  await page.click('.finance-scenarios summary');await fill(page,'Analysis result cell','A5');await fill(page,'Minimum input',80);await fill(page,'Maximum input',140);await fill(page,'Sensitivity samples',3);
  await page.focus('[data-analysis-action=sensitivity]');await page.keyboard.press('Enter');await analysisNotice(page,'Compared 3 inputs');
  const rows=await page.$$eval('[data-analysis-results] tbody tr',rows=>rows.map(r=>[...r.cells].map(c=>c.textContent)));expect(rows).toEqual([['80','10','-10'],['110','25','5'],['140','40','20']]);expect((await read()).revision).toBe(1);
  await fill(page,'Target result',40);await fill(page,'Maximum input',200);await analyze(page,'goal');await analysisNotice(page,'met the result tolerance');
  expect(await page.$eval('[data-analysis-results]',n=>n.textContent)).toContain('140');expect((await read()).snapshot.sheets['sheet-0'].cellData[0][0].v).toBe(100);
  for(const width of [1440,390]){await page.setViewport({width,height:1000});await page.waitForFunction(()=>document.documentElement.scrollHeight<6000);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const out=process.env.BONFYRE_ANALYSIS_PROOF;if(out){mkdirSync(out,{recursive:true});await page.screenshot({path:join(out,'analysis-'+width+'.png'),fullPage:true});}}
  await page.$eval('[aria-label="Target result"]',n=>{n.value='45';n.dispatchEvent(new Event('input',{bubbles:true}));});expect(await page.$eval('[data-analysis-results]',n=>n.childElementCount)).toBe(0);expect(await page.$eval('[data-analysis-action=save-goal]',n=>n.parentElement.hidden)).toBe(true);
  await fill(page,'Target result',40);await analyze(page,'goal');await analysisNotice(page,'met the result tolerance');await fill(page,'Goal assumption set name','Sales target');await analyze(page,'save-goal');await analysisNotice(page,'Set added');
  expect((await read()).revision).toBe(1);await save(page);const set=await read();expect(set.revision).toBe(2);expect(set.scenarios.sets[0].values[0].value).toBe(140);expect(set.snapshot.sheets['sheet-0'].cellData[0][0].v).toBe(100);
  await act(page,'preview');await notice(page,'Preview ready');await act(page,'apply');await notice(page,'Applied Sales target');await save(page);
  const after=await read();expect(after.revision).toBe(3);expect(after.snapshot.sheets['sheet-0'].cellData[4][0].v).toBe(40);expect(await after.source.original.arrayBuffer()).toEqual(await source.original.arrayBuffer());expect(errors).toEqual([]);
 }finally{page.on('dialog',d=>d.accept());await context.close();}
},90000);
test('phone analysis rejects a nonformula output and an unbracketed target without touching saved records',async()=>{
 const{page,context}=await open(390);
 try{
  await page.click('.finance-scenarios summary');await fill(page,'Analysis result cell','A1');await analyze(page,'sensitivity');await analysisNotice(page,'formula cell');
  const before=await read();await fill(page,'Analysis result cell','A5');await fill(page,'Minimum input',80);await fill(page,'Maximum input',140);await fill(page,'Target result',999);await analyze(page,'goal');await analysisNotice(page,'do not bracket');
  expect(await page.$eval('[data-analysis-results]',n=>n.childElementCount)).toBe(0);expect(await page.$eval('[data-analysis-action=save-goal]',n=>n.parentElement.hidden)).toBe(true);const after=await read();expect(after.revision).toBe(before.revision);expect(after.scenarios).toEqual(before.scenarios);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await context.close();}
},60000);
