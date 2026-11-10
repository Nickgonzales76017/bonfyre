import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {materializeRoomProcurement} from '../materialize_room_procurement.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {portfolio,projectWorkspaceSetups} from '../publish_experience_setups.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';

export const procurementBaseRelease='4cc9ac94af8c8e7ddedc5c7c33fdb7091d50c0f4f58fe4271d0b2829d05f9d10';

const rows=portfolio(readFileSync(join(import.meta.dir,'../../cmd/BonfyreExperienceStage/experience-maturities.yaff'),'utf8'));
const targets=Object.freeze({
 room:Object.freeze({
  pack:'experience.retail-procurement.workspace',
  subject:'user:19',scope:'self',
  binding:'ws-52728ca8c36af03b4f56c1ed55495ee00d1bc784',
  family:'spaces',entry:'product-room/'
 }),
 catalogue:Object.freeze({
  pack:'experience.multi-merchant-catalogue.workspace',
  subject:'user:19',scope:'self',
  binding:'ws-d1935babcf32fdc4c97fc25936d4ef8ed9b49935',
  family:'spaces',entry:'catalogue/'
 })
});

const sources=[
 'components/BonfyreServiceSite/room-procurement.mjs',
 'components/BonfyreServiceSite/room-procurement-view.mjs',
 'components/BonfyreServiceSite/room-procurement.css',
 'components/BonfyreServiceSite/planner-model.mjs',
 'components/BonfyreServiceSite/catalogue-release.mjs',
 'components/BonfyreServiceSite/catalogue-model.mjs',
 'components/BonfyreServiceSite/catalogue-preparation.mjs',
 'components/BonfyreServiceSite/workspace-lifetime.mjs',
 'integrations/browser/procurement-wasm.mjs',
 'integrations/browser/procurement-rfq-workbench.mjs',
 'integrations/browser/procurement-enactment-client.mjs',
 'integrations/browser/workspace-store.mjs',
 'integrations/browser/workspace-continuation.mjs',
 'integrations/browser/workspace-msgpack.mjs',
 'integrations/browser/workspace-value.mjs',
 'integrations/browser/tests/procurement-wasm.test.mjs',
 'integrations/browser/tests/procurement-enactment-client.test.mjs',
 'cmd/BonfyreProcurement/Makefile',
 'cmd/BonfyreProcurement/src/procurement.c',
 'cmd/BonfyreProcurement/src/procurement.h',
 'cmd/BonfyreProcurement/src/main.c',
 'cmd/BonfyreProcurement/src/effect.c',
 'cmd/BonfyreProcurement/src/effect.h',
 'cmd/BonfyreProcurement/wasm/export.c',
 'cmd/BonfyreProcurement/wasm/procurement.wasm',
 'cmd/BonfyreProcurement/contracts/procurement.yaff',
 'cmd/BonfyreProcurement/contracts/procurement.cue',
 'cmd/BonfyreProcurement/tests/procurement_test.c',
 'cmd/BonfyreProcurement/tests/effect_test.sh',
 'cmd/BonfyreProcurement/tests/wasm_import_test.c',
 'cmd/BonfyreWebAddress/src/procurement_approval_status.h',
 'cmd/BonfyreWebAddress/tests/procurement_approval_status.test.mjs',
 'cmd/BonfyreWebAddress/src/procurement_downstream_status.h',
 'cmd/BonfyreWebAddress/src/procurement_enactment.h',
 'cmd/BonfyreWebAddress/src/procurement_enactment_effect.h',
 'cmd/BonfyreWebAddress/src/application_workspace.h',
 'cmd/BonfyreWebAddress/src/main.c',
 'cmd/BonfyreWebAddress/Makefile',
 'cmd/BonfyreWebAddress/tests/procurement_downstream_status.test.mjs',
 'cmd/BonfyreWebAddress/tests/procurement_enactment_readiness.test.mjs',
 'cmd/BonfyreWebAddress/tests/procurement_enactment_effect.test.mjs',
 'cmd/BonfyreWebAddress/tests/auth_continuation_fixture.mjs',
 'cmd/BonfyreWebAddress/tests/pack_setup_fixture.mjs',
 'cmd/BonfyreWebAddress/tests/workspace_fixture.mjs',
 'scripts/materialize_room_procurement.mjs',
 'scripts/materialize_procurement_continuation.mjs',
 'scripts/tests/room-procurement.test.mjs',
 'scripts/tests/room-procurement-browser.test.mjs',
 'scripts/publish_experience_setups.mjs',
 'scripts/release/procurement_workspace_upgrade.mjs',
 'scripts/release/tests/workspace_upgrade.test.mjs'
];

function versionFor(pack,release){
 const row=projectWorkspaceSetups(rows,release).find(item=>item.type==='pack_version'&&item.value.pack===pack);
 assert(row,'Workspace version projection is missing for '+pack);
 return row.value;
}

export function procurementUpgradeSpec(which){
 const target=targets[which];assert(target,'Use room or catalogue.');
 return Object.freeze({
  id:'procurement-'+which+'-workspace-upgrade-v3',
  schema:'bonfyre.procurement-'+which+'-workspace-upgrade.v1',
  target,
  allowInitialStale:true,
  sourcePaths:sources,
  baseRoot:release=>{
   assert.equal(release,procurementBaseRelease,'The installed procurement release changed; review the exact current package.');
   return versionFor(target.pack,release).root;
  },
  versionFor:(candidate,base)=>({
   ...versionFor(target.pack,candidate),
   parent_root:base?versionFor(target.pack,base).root:null,
   changed:'Authority-bound order intent now imports a detached cryptographic supplier acceptance only for exact retained ed25519-bound supplier lots, enacts ProtocolContract commitments through the native owner, stamps exact order-intent provenance for lost-ack recovery, and renders per-exchange CommitmentGraph evidence; purchase order, payable, payment, settlement, fulfillment, shipment and receipt remain separate effects; package '+candidate,
   proof:'cmd/BonfyreProcurement/Makefile; cmd/BonfyreProcurement/tests/effect_test.sh; cmd/BonfyreWebAddress/tests/procurement_approval_status.test.mjs; cmd/BonfyreWebAddress/tests/procurement_downstream_status.test.mjs; cmd/BonfyreWebAddress/tests/procurement_enactment_readiness.test.mjs; cmd/BonfyreWebAddress/tests/procurement_enactment_effect.test.mjs; integrations/browser/tests/procurement-wasm.test.mjs; integrations/browser/tests/procurement-enactment-client.test.mjs; scripts/tests/room-procurement-browser.test.mjs; scripts/release/tests/workspace_upgrade.test.mjs'
  }),
  materialize:({root})=>materializeRoomProcurement({root}),
  verify:async({root,id})=>{
   const html=(await readWorkspaceFile(root,id,'procurement/index.html')).toString();
   const continuations=(await readWorkspaceFile(root,id,'CONTINUATIONS.tsv')).toString();
   assert(html.includes('procurement-rfq-workbench.mjs')&&html.includes('data-purchase-lists'),'The package does not mount the integrated procurement continuation.');
   for(const prefix of ['procurement-draft:','procurement-rfq:','procurement-quote:','procurement-comparison:','procurement-allocation:','procurement-approval:','procurement-order-intent:'])assert(continuations.includes(prefix),'Missing retained continuation '+prefix);
  }
 });
}

if(import.meta.main){
 let adapter;
 try{
  const [mode,which,candidate,...extra]=process.argv.slice(2);
  assert(!extra.length&&['review','apply','advance'].includes(mode)&&targets[which]&&(mode==='review'?!candidate:/^[a-f0-9]{64}$/.test(candidate)),'Use review room|catalogue, apply room|catalogue EXACT_REVIEWED_PACKAGE_SHA256, or advance room|catalogue EXACT_CANDIDATE_SHA256 twice.');
  const spec=procurementUpgradeSpec(which);
  adapter=nativeWorkspaceUpgradeAdapter({spec});
  let result;
  if(mode==='review')result=await reviewWorkspaceUpgrade(adapter,spec);
  else if(mode==='apply')result=await applyWorkspaceUpgrade(adapter,candidate,spec);
  else {
   const reviewed=await reviewWorkspaceUpgrade(adapter,spec);
   assert.equal(reviewed.candidate,candidate,'The materialized candidate differs from the exact advance digest.');
   result=reviewed.state==='reviewed'?reviewed:await applyWorkspaceUpgrade(adapter,candidate,spec);
  }
  console.log(JSON.stringify(result));
 }catch(error){
  console.log(JSON.stringify({state:'stopped',reason:error.message,next_step:'Keep the retained upgrade journal and reconcile native readback before any further write.'}));
  process.exitCode=1;
 }finally{adapter?.close();}
}
