import assert from 'node:assert/strict';
import {materializeResearchRepair,researchRepairSources} from '../materialize_research_repair.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {projectInstrumentsWorkspaceSetup} from '../publish_experience_setups.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';
export const researchUpgradeTarget=Object.freeze({pack:'experience.skill-instruments.workspace',subject:'user:19',scope:'self',binding:'ws-e67ee1d400acf6ecdcf7ee3d385b45521159ed10',family:'skill-instruments',entry:'instruments/'});
export const researchBaseRelease='6dbdf9f01d53e83cbdc6252f34da9afffae6cdb78b80e547034a9b7ed9ede60f';
const version=release=>projectInstrumentsWorkspaceSetup(release).find(row=>row.type==='pack_version').value;
export const researchUpgradeSpec=Object.freeze({
 id:'research-workspace-upgrade',schema:'bonfyre.research-workspace-upgrade.v1',target:researchUpgradeTarget,
 sourcePaths:[...researchRepairSources,'scripts/release/research_workspace_upgrade.mjs','scripts/publish_experience_setups.mjs','scripts/release/tests/workspace_upgrade.test.mjs','scripts/tests/research-repair-package.test.mjs'],
 baseRoot:release=>{assert.equal(release,researchBaseRelease,'The installed planning release changed; review its exact package.');return version(release).root;},
 versionFor:(candidate,base)=>({...version(candidate),parent_root:base?version(base).root:null,changed:'Research campaign persistence, evidence validity, import and search isolation, and export fitting; package '+candidate,proof:'scripts/tests/research-repair-package.test.mjs; components/BonfyreServiceSite/tests/campaign-persistence-native.mjs'}),
 materialize:({root})=>materializeResearchRepair({root,sourceRelease:researchBaseRelease}),
 verify:async({root,id})=>{
  const html=(await readWorkspaceFile(root,id,'research/index.html')).toString();
  assert(html.includes('data-ui="campaigns"')&&html.includes('bonfyre-workspace-store'),'The package does not mount the retained research editor.');
 }
});
if(import.meta.main){
 let adapter;try{
  const [mode,candidate,...extra]=process.argv.slice(2);assert(!extra.length&&['review','apply'].includes(mode)&&(mode==='review'?!candidate:/^[a-f0-9]{64}$/.test(candidate)),'Use review, then apply EXACT_REVIEWED_PACKAGE_SHA256.');
  adapter=nativeWorkspaceUpgradeAdapter({spec:researchUpgradeSpec});
  const result=mode==='review'?await reviewWorkspaceUpgrade(adapter,researchUpgradeSpec):await applyWorkspaceUpgrade(adapter,candidate,researchUpgradeSpec);
  console.log(JSON.stringify(result));
 }catch(error){console.log(JSON.stringify({state:'stopped',reason:error.message,next_step:'Keep the review and journal; reconcile native state before any further write.'}));process.exitCode=1;}finally{adapter?.close();}
}
