import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {verifySameCrmFinanceDestination,validateCrmSource,crmFinanceDraft,validateCrmFinanceDraft} from '../../../integrations/browser/crm-finance-handoff.mjs';
import {workbookPackage} from '../../../components/BonfyreServiceSite/workbook-package.mjs';
import {readWorkspaceFile} from '../../../scripts/workspace_material.mjs';

const finance='ws-'+'a'.repeat(40),crm='ws-'+'b'.repeat(40),release='c'.repeat(64);
function reader({subject='user:19',crmSubject=subject,scope='self',financeRelease=release,crmReady=true,financeReady=true}={}){
 return async url=>{
  const parsed=new URL(url,'https://aurekai.work'),pack=parsed.searchParams.get('pack');
  if(parsed.pathname!=='/apps/_bindings'||parsed.searchParams.get('context')!=='self')throw Error('Wrong native destination');
  if(pack==='experience.finance-workbooks.workspace')return [1,subject,scope,[[finance,pack,'finance/',financeRelease,'Financial workbook review',financeReady]]];
  if(pack==='experience.skill-instruments.workspace')return [1,crmSubject,scope,[[crm,pack,'instruments/','d'.repeat(64),'Planning instruments',crmReady]]];
  throw Error('Unexpected provider');
 };
}
const verify=(request,opts={})=>verifySameCrmFinanceDestination({context:'self',financeBinding:finance,crmBinding:crm,financeRelease:release,request,...opts});

test('Finance and CRM must independently resolve one exact current native destination',async()=>{
 const proved=await verify(reader());expect(proved).toEqual({subject:'user:19',scope:'self',financeRelease:release,crmRelease:'d'.repeat(64)});
 for(const bad of [
  reader({crmSubject:'user:20'}),reader({scope:'institution'}),
  reader({financeRelease:'e'.repeat(64)}),reader({crmReady:false}),reader({financeReady:false})
 ])await expect(verify(bad)).rejects.toThrow();
 await expect(verify(reader(),{context:'institution:outside'})).rejects.toThrow();
 await expect(verify(reader(),{financeBinding:crm})).rejects.toThrow();
 await expect(verify(reader(),{financeRelease:'not-a-release'})).rejects.toThrow();
});

test('A reviewed opportunity can be saved as an estimate with exact source lineage but no economic effects',()=>{
 const sourceKey='opportunity:feed:'+'a'.repeat(64),relationship={
  schema:'bonfyre.relationship.v1',id:'b'.repeat(64),state:'observed',label:'Observed role with source credit',
  actor:{state:'unresolved',subject:null},
  source:{itemRef:'feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa',snapshot:'d'.repeat(64),publicPath:'/o/feed-jobs-fixture/aaaaaaaaaaaaaaaaaaaaaaaa'},
  opportunity:{kind:'job-opening',stage:'observed'}
 };
 const source={key:sourceKey,revision:3,value:{state:{relationship}}};
 expect(validateCrmSource(source,sourceKey,3)).toEqual(relationship);
 for(const row of [{...source,revision:2},{...source,value:{state:{relationship:{...relationship,actor:{state:'verified',subject:'person:else'}}}}}])
  expect(()=>validateCrmSource(row,sourceKey,3)).toThrow();
 const prepared=crmFinanceDraft(crm,sourceKey,3,relationship,'42.85','Unpriced job observation; human proposes estimate');
 const financeKey='crm-finance:'+'e'.repeat(64);
 expect(validateCrmFinanceDraft({key:financeKey,revision:1,value:prepared},financeKey).estimate.amount_cents).toBe(4285);
 expect(prepared.source).toMatchObject({binding:crm,key:sourceKey,revision:3,snapshot:relationship.source.snapshot});
 expect(Object.values(prepared.effects).every(v=>v===false)).toBe(true);
 expect(prepared.estimate.approval).toBe('not-requested');
 expect(()=>validateCrmFinanceDraft({key:financeKey,revision:1,value:{...prepared,effects:{...prepared.effects,invoice:true}}},financeKey)).toThrow();
 expect(()=>crmFinanceDraft(crm,sourceKey,3,relationship,'NaN','')).toThrow();
});

test('Finance package contains the current-account CRM review continuation and its binding reader',async()=>{
 const root=mkdtempSync(join(tmpdir(),'bonfyre-crm-finance-'));
 try{
  const pack=await workbookPackage({packageRoot:root});
  const manifest=(await readWorkspaceFile(root,pack.id,'CONTINUATIONS.tsv')).toString();
  expect(manifest).toContain('crm-finance\tCRM finance review\tfinance/crm/\tcrm-finance:');
  const html=(await readWorkspaceFile(root,pack.id,'finance/crm/index.html')).toString();
  expect(html).toContain('bonfyre-workspace-context');
  expect(html).toContain('crm-finance-handoff.mjs');
  const js=(await readWorkspaceFile(root,pack.id,'bridge/crm-finance-handoff.mjs')).toString();
  expect(js).toContain('verifySameCrmFinanceDestination(');
  expect(js).toContain('await verifySameCrmFinanceDestination(');
  expect(js).toContain('saveLocal(key,prepared,0)');
  expect((await readWorkspaceFile(root,pack.id,'bridge/workspace-destinations.mjs')).length).toBeGreaterThan(100);
  expect(pack.records).toContain('crm-finance:');
 }finally{rmSync(root,{recursive:true,force:true});}
});
