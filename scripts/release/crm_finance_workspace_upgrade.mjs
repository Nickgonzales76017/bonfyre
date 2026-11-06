// Governed current-account Finance package upgrade. One exact currently installed
// Finance editor, one immutable package, retained review and staged native writes.
// Does not create source opportunities, CRM deals, prices, invoices or cash.
import assert from 'node:assert/strict';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {projectFinanceWorkspaceSetup} from '../publish_experience_setups.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';

const pack='experience.finance-workbooks.workspace';
const target=Object.freeze({
 pack,subject:'user:19',scope:'self',
 binding:'ws-7420da15754d3b1a70b6a07213ea37254bb17393',
 family:'financial-workbooks',entry:'finance/'
});
const releaseVersion=release=>{
 const row=projectFinanceWorkspaceSetup(release).find(x=>x.type==='pack_version'&&x.value.pack===pack);
 assert(row?.value?.root,'A pinned Finance release version is required.');return row.value;
};
export const financeCrmUpgradeSpec=Object.freeze({
 id:'crm-finance-review-upgrade',
 schema:'bonfyre.crm-finance-review-upgrade.v1',
 target,
 sourcePaths:[
  'scripts/release/crm_finance_workspace_upgrade.mjs',
  'scripts/release/workspace_upgrade.mjs',
  'scripts/workspace_material.mjs',
  'scripts/publish_experience_setups.mjs',
  'components/BonfyreServiceSite/workbook-package.mjs',
  'components/BonfyreServiceSite/workbook-workspace-client.mjs',
  'integrations/browser/crm-finance-handoff.mjs',
  'integrations/browser/workspace-destinations.mjs',
  'integrations/browser/workspace-store.mjs',
  'cmd/BonfyreFinance/tests/crm_finance_auth.test.mjs',
  'cmd/BonfyreFinance/tests/crm_finance_browser.test.mjs'
 ],
 baseRoot:release=>releaseVersion(release).root,
 versionFor:(candidate,base)=>({
  ...releaseVersion(candidate),
  parent_root:base?releaseVersion(base).root:null,
  changed:'Same-destination CRM observation to reviewed Finance draft; exact source revision and account grants required, no inferred revenue or spend. Package '+candidate,
  proof:'cmd/BonfyreFinance/tests/crm_finance_auth.test.mjs; cmd/BonfyreFinance/tests/crm_finance_browser.test.mjs'
 }),
 materialize:({root})=>workbookPackage({packageRoot:root}),
 verify:async({root,id})=>{
  const manifest=(await readWorkspaceFile(root,id,'CONTINUATIONS.tsv')).toString();
  assert(manifest.includes('crm-finance\tCRM finance review\tfinance/crm/\tcrm-finance:'),
   'Finance package lacks the CRM review continuation.');
  const html=(await readWorkspaceFile(root,id,'finance/crm/index.html')).toString();
  assert(html.includes('data-crm-finance-handoff')&&html.includes('bonfyre-workspace-context')&&html.includes('crm-finance-handoff.mjs'),
   'The current-account CRM Finance receiver is not mounted.');
  const js=(await readWorkspaceFile(root,id,'bridge/crm-finance-handoff.mjs')).toString();
  assert(js.includes('await verifySameCrmFinanceDestination(')&&js.includes('saveLocal(key,prepared,0)'),
   'Same-destination or revision-bound native save is missing.');
  await readWorkspaceFile(root,id,'bridge/workspace-destinations.mjs');
 }
});
export const reviewFinanceCrmUpgrade=adapter=>reviewWorkspaceUpgrade(adapter,financeCrmUpgradeSpec);
export const applyFinanceCrmUpgrade=(adapter,candidate)=>applyWorkspaceUpgrade(adapter,candidate,financeCrmUpgradeSpec);
export const financeCrmUpgradeAdapter=(options={})=>nativeWorkspaceUpgradeAdapter({...options,spec:financeCrmUpgradeSpec});
if(import.meta.main){
 let adapter;
 try{
  const [mode,candidate,...extra]=process.argv.slice(2);
  assert(extra.length===0&&['review','apply'].includes(mode)&&
   (mode==='review'?!candidate:/^[a-f0-9]{64}$/.test(candidate||'')),
   'Use review, then apply EXACT_REVIEWED_PACKAGE_SHA256.');
  adapter=financeCrmUpgradeAdapter();
  const result=mode==='review'?await reviewFinanceCrmUpgrade(adapter):await applyFinanceCrmUpgrade(adapter,candidate);
  console.log(JSON.stringify(result));
 }catch(error){
  console.log(JSON.stringify({state:'stopped',reason:error.message,
   next_step:'Keep the immutable review journal; read native lifecycle before replaying an uncertain step.'}));
  process.exitCode=1;
 }finally{adapter?.close();}
}
