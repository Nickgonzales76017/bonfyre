import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync,mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {workbookSnapshot} from '../../components/BonfyreServiceSite/workbook-model.mjs';
import {calculateWorkbook} from '../../components/BonfyreServiceSite/workbook-calculation.mjs';
import {workbookReviewValue} from '../../components/BonfyreServiceSite/workbook-record.mjs';
import {projectFinanceWorkspaceSetup,publishSetups} from '../publish_experience_setups.mjs';
import * as XLSX from '../../components/BonfyreDecision/vendor/sheetjs-0.20.3/xlsx.mjs';
let f,binding,browser,temp,before;const key='finance-workbook:lineage-browser';
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'lineage-browser-'));const pack=await workbookPackage({packageRoot:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');f=await workspaceFixture();
 publishSetups({records:projectFinanceWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});binding=await f.install('experience.finance-workbooks.workspace','institution:workspace-team');
 const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[20,{t:'n',f:'A1*2',v:40},{t:'n',f:'B1+5',v:45}]]),'Drivers');XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[{t:'n',f:'Drivers!C1*2',v:90}]]),'Results');
 const result=await calculateWorkbook(workbookSnapshot(book,{name:'Lineage browser fixture'})),original=new Blob([XLSX.write(book,{type:'array',bookType:'xlsx'})],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
 before=await f.store(binding).saveLocal(key,workbookReviewValue(result.snapshot,{name:'original.xlsx',original}),0);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{try{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});}finally{for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;}});
test('native finance lineage inspects dependencies, follows references and preserves the saved workbook at desktop and phone widths',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(20000);
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 try{
  await page.setViewport({width:1440,height:1000});await page.goto(f.base+binding.path+'?workbook='+encodeURIComponent(key),{waitUntil:'networkidle0'});await page.waitForSelector('[data-workbook-editor][data-ready=true]');
  await page.click('[data-workbook-action=lineage]');await page.click('[data-lineage-inspect]');await page.waitForFunction(()=>document.querySelector('[data-lineage-status]').textContent.startsWith('Inspected'));
  expect(await page.$eval('[data-lineage-dependents=direct]',n=>n.textContent)).toContain('Drivers!B1');expect(await page.$eval('[data-lineage-dependents=indirect]',n=>n.textContent)).toContain('Results!A1');
  await page.$eval('[aria-label="Lineage cell"]',n=>{n.value='C1';});await page.click('[data-lineage-inspect]');await page.waitForFunction(()=>document.querySelector('[data-lineage-formula]')?.textContent==='=B1+5');expect(await page.$eval('[data-lineage-inputs]',n=>n.textContent)).toBe('Drivers!B1');
  for(const width of [1440,390]){await page.setViewport({width,height:1000});await page.waitForFunction(()=>document.documentElement.scrollHeight<4000);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const out=process.env.BONFYRE_LINEAGE_PROOF;if(out){mkdirSync(out,{recursive:true});await page.screenshot({path:join(out,'lineage-'+width+'.png'),fullPage:true});}}
  await page.click('[data-lineage-inputs] button');expect(await page.$eval('.finance-workbook',n=>n.dataset.view)).toBe('workbook');
  await page.click('[data-workbook-action=lineage]');await page.$eval('[aria-label="Lineage cell"]',n=>{n.value='A0';});await page.click('[data-lineage-inspect]');await page.waitForFunction(()=>document.querySelector('[data-lineage-status]').textContent.includes('Choose one cell'));expect(await page.$eval('[data-lineage-results]',n=>n.children.length)).toBe(0);
  const after=await f.store(binding).readVersion(key);expect(after.revision).toBe(before.revision);expect(await after.value.snapshot.text()).toBe(await before.value.snapshot.text());expect(await after.value.source.original.arrayBuffer()).toEqual(await before.value.source.original.arrayBuffer());expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
