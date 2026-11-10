import assert from 'node:assert/strict';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {shipmentWorkspaceMaterial,shipmentWorkspaceSetup} from '../materialize_shipment_workspace.mjs';
import {projectFinanceWorkspaceSetup} from '../publish_experience_setups.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';

const targets=Object.freeze({
 finance:Object.freeze({
  pack:'experience.finance-workbooks.workspace',
  subject:'user:19',scope:'self',
  binding:'ws-7420da15754d3b1a70b6a07213ea37254bb17393',
  family:'financial-workbooks',entry:'finance/',
  baseRelease:'64bb4fba0cc9658f7a3aa5824f393d1de4b26f475ec76c0771622785077a01f9'
 }),
 shipment:Object.freeze({
  pack:'experience.shipment-archive.workspace',
  subject:'user:19',scope:'self',
  binding:'ws-c3afe5c33bc94b005feaad691a60a16a459de8b5',
  family:'shipment-review',entry:'shipments/',
  baseRelease:'ebfd306ef1d231176904272078d91ced6005ee9236ef63cc59b8bac1df5754f1'
 })
});

const sources=[
 'integrations/browser/procurement-order-intent-handoff.mjs',
 'components/BonfyreServiceSite/workbook-package.mjs',
 'scripts/materialize_shipment_workspace.mjs',
 'scripts/publish_experience_setups.mjs',
 'scripts/tests/procurement-downstream-workspaces.test.mjs',
 'cmd/BonfyreProcurement/Makefile',
 'scripts/release/procurement_downstream_workspace_upgrade.mjs',
 'scripts/release/workspace_upgrade.mjs'
];

function versionFor(which,release){
 const rows=which==='finance'?projectFinanceWorkspaceSetup(release):shipmentWorkspaceSetup(release);
 const row=rows.find(item=>item.type==='pack_version'&&item.value.pack===targets[which].pack);
 assert(row,'Workspace version projection is missing for '+which);
 return row.value;
}

export function downstreamUpgradeSpec(which){
 const target=targets[which];assert(target,'Use finance or shipment.');
 return Object.freeze({
  id:'procurement-downstream-'+which+'-workspace-upgrade',
  schema:'bonfyre.procurement-downstream-'+which+'-workspace-upgrade.v1',
  target,
  allowInitialStale:true,
  sourcePaths:sources,
  baseRoot:release=>{
   assert.equal(release,target.baseRelease,'The installed '+which+' release changed; review the exact current package.');
   return versionFor(which,release).root;
  },
  versionFor:(candidate,base)=>({
   ...versionFor(which,candidate),
   parent_root:base?versionFor(which,base).root:null,
   changed:'Read-only procurement order-intent continuation into '+which+'; exact saved procurement bytes and revision remain canonical, while ledger posting, payment, shipment and fulfillment effects remain separately owned; package '+candidate,
   proof:'cmd/BonfyreProcurement/Makefile; scripts/tests/procurement-downstream-workspaces.test.mjs; integrations/browser/procurement-order-intent-handoff.mjs'
  }),
  materialize:({root})=>which==='finance'?workbookPackage({packageRoot:root}):shipmentWorkspaceMaterial({root}),
  verify:async({root,id})=>{
   const continuation=(await readWorkspaceFile(root,id,'CONTINUATIONS.tsv')).toString();
   const html=(await readWorkspaceFile(root,id,which==='finance'?'finance/procurement/index.html':'shipments/procurement/index.html')).toString();
   assert(continuation.includes('procurement-order-intent:'),'The downstream package does not declare the procurement order-intent continuation.');
   assert(html.includes('data-procurement-order-intent-handoff')&&html.includes('procurement-order-intent-handoff.mjs'),'The downstream package does not mount the read-only procurement handoff.');
  }
 });
}

if(import.meta.main){
 let adapter;
 try{
  const [mode,which,...extra]=process.argv.slice(2);
  assert(!extra.length&&['review','advance'].includes(mode)&&targets[which],'Use review|advance finance|shipment.');
  const spec=downstreamUpgradeSpec(which);adapter=nativeWorkspaceUpgradeAdapter({spec});
  const reviewed=await reviewWorkspaceUpgrade(adapter,spec);
  const result=mode==='review'||reviewed.state==='already_verified'?reviewed:await applyWorkspaceUpgrade(adapter,reviewed.candidate,spec);
  console.log(JSON.stringify(result));
 }catch(error){
  console.log(JSON.stringify({state:'stopped',reason:error.message,next_step:'Keep the retained upgrade journal and reconcile native readback before any further write.'}));
  process.exitCode=1;
 }finally{adapter?.close();}
}
