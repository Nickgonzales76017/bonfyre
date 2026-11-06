// Real isolated WebAddress/Auth/CMS + installed CRM and Finance editors.
// This is an account-scoped browser witness, never a live customer record.
import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {workspaceFixture,root} from '../../BonfyreWebAddress/tests/workspace_fixture.mjs';
import {projectFinanceWorkspaceSetup,projectInstrumentsWorkspaceSetup,publishSetups} from '../../../scripts/publish_experience_setups.mjs';
import {workbookPackage} from '../../../components/BonfyreServiceSite/workbook-package.mjs';
import {instrumentsPackage} from '../../../components/BonfyreServiceSite/instruments-package.mjs';

const old={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
const key='opportunity:feed:'+'b'.repeat(64),context='institution:workspace-team';
let home,f,browser,finance,crm,foreign;
const source={name:'Fixture job',saved_at:'2026-11-02T12:00:00Z',save_id:'a'.repeat(64),
 state:{relationship:{schema:'bonfyre.relationship.v1',id:'a'.repeat(64),state:'observed',label:'Fixture reviewed job opening',createdAt:'2026-11-02T12:00:00Z',
 actor:{state:'unresolved',subject:null},source:{itemRef:'fixture-jobs/bbbbbbbbbbbbbbbbbbbbbbbb',publicPath:'/o/fixture-jobs/bbbbbbbbbbbbbbbbbbbbbbbb',source:'fixture-jobs',family:'jobs',kind:'publication',title:'Fixture reviewed job opening',summary:'Fixture only',canonicalHref:'https://source.example.test/item',authorName:'',snapshot:'c'.repeat(64),evidenceKind:'retained-source-item'},opportunity:{kind:'job-opening',stage:'observed'}}}};
async function pageFor(target){
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[];
 p.on('pageerror',e=>errors.push(e.message));p.setDefaultTimeout(16000);
 const [name,...parts]=f.owner.cookie.split('=');
 await p.setCookie({name,value:parts.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await p.goto(f.base+target,{waitUntil:'domcontentloaded'});
 return {ctx,p,errors};
}
beforeAll(async()=>{
 home=mkdtempSync(join(tmpdir(),'bonfyre-finance-browser-'));
 const packages=join(home,'packages'),[fin,ins]=await Promise.all([
   workbookPackage({packageRoot:packages}),instrumentsPackage({packageRoot:packages})
 ]);
 process.env.BONFYRE_WORKSPACE_RELEASE=fin.id;process.env.BONFYRE_WORKSPACE_PACKAGES=packages;
 f=await workspaceFixture();
 const records=[...projectFinanceWorkspaceSetup(fin.id),...projectInstrumentsWorkspaceSetup(ins.id)];
 const published=publishSetups({records,binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
 expect(published.created).toBeGreaterThan(0);
 finance=await f.install('experience.finance-workbooks.workspace',context);
 crm=await f.install('experience.skill-instruments.workspace',context);
 await f.store(crm).saveLocal(key,source,0);
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
},120000);
afterAll(async()=>{
 await browser?.close();await f?.close();if(home)rmSync(home,{recursive:true,force:true});
 for(const [k,v]of [['BONFYRE_WORKSPACE_RELEASE',old.release],['BONFYRE_WORKSPACE_PACKAGES',old.packages]])if(v===undefined)delete process.env[k];else process.env[k]=v;
},20000);

test('a browser takes one saved current CRM observation into Finance, saves once and reopens with no money effect',async()=>{
 const uri='/apps/'+finance.id+'/finance/crm/?'+new URLSearchParams({record:key,crm:crm.id,revision:'1'});
 const {ctx,p,errors}=await pageFor(uri);
 try{
  await p.waitForSelector('[data-crm-finance-handoff][data-ready=true]');
  expect(await f.store(finance).readLocal('crm-finance:')).toHaveLength(0);
  await p.type('[aria-label="Proposed amount in USD"]','123.45');
  await p.type('[aria-label="Estimate assumptions"]','Review only; no agreed price');
  await p.click('button[type=submit]');
  await p.waitForSelector('a[href*="record=crm-finance%3A"]');
  const rows=await f.store(finance).readLocal('crm-finance:');
  expect(rows).toHaveLength(1);expect(rows[0].revision).toBe(1);
  expect(rows[0].value).toMatchObject({schema:'bonfyre.crm-finance-review.v1',status:'review-draft',source:{binding:crm.id,key,revision:1,snapshot:source.state.relationship.source.snapshot},
   estimate:{amount_cents:12345,currency:'USD',approval:'not-requested'},effects:{invoice:false,ledger_posted:false,payment:false,contract:false,commitment:false}});
  await p.click('a[href*="record=crm-finance%3A"]');
  await p.waitForSelector('[data-crm-finance-handoff][data-ready=true]');
  expect(await p.$eval('[data-crm-finance-handoff]',x=>x.textContent)).toContain('Review draft; no economic effect');
  expect((await f.store(finance).readLocal('crm-finance:'))[0].revision).toBe(1);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},65000);

test('an actor with access to two institutions cannot import a CRM observation from the other one',async()=>{
 f.grant(f.owner.id,'other-organization','admin');
 foreign=await f.install('experience.skill-instruments.workspace','institution:other-organization');
 await f.store(foreign).saveLocal(key,source,0);
 const uri='/apps/'+finance.id+'/finance/crm/?'+new URLSearchParams({record:key,crm:foreign.id,revision:'1'});
 const {ctx,p}=await pageFor(uri);
 try{
  await p.waitForSelector('[data-crm-finance-handoff][data-failed=true]');
  const text=await p.$eval('[data-crm-finance-handoff]',x=>x.textContent);
  expect(text).toContain('unavailable');
  expect((await f.store(finance).readLocal('crm-finance:'))[0].revision).toBe(1);
 }finally{await ctx.close();}
},65000);

test('an exact stale CRM revision is refused before creating a second Finance record',async()=>{
 const staleKey='opportunity:feed:'+'f'.repeat(64);
 await f.store(crm).saveLocal(staleKey,source,0);
 const uri='/apps/'+finance.id+'/finance/crm/?'+new URLSearchParams({record:staleKey,crm:crm.id,revision:'2'});
 const {ctx,p}=await pageFor(uri);
 try{
  await p.waitForSelector('[data-crm-finance-handoff][data-failed=true]');
  expect(await p.$eval('[data-crm-finance-handoff]',x=>x.textContent)).toContain('changed');
  expect(await f.store(finance).readLocal('crm-finance:')).toHaveLength(1);
 }finally{await ctx.close();}
},65000);
