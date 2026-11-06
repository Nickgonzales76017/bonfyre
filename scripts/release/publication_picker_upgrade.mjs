import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {loadWorkspaceFiles,retainWorkspaceMaterial,readWorkspaceFile} from '../workspace_material.mjs';
import {projectConnectedInstrumentsSetup} from '../publish_experience_setups.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';
const repo=resolve(import.meta.dir,'../..');
export const pickerBaseRelease='6f13a12c3a36da030f67b13c84a5c69ca763c82860745c2432707b837fb60b3b';
export const pickerUpgradeTarget=Object.freeze({pack:'experience.connected-instruments.workspace',subject:'user:19',scope:'self',binding:'ws-bd08c6776460a11c6d1e970b01f53cc15b65a37a',family:'connected-instruments',entry:'instruments/'});
export const pickerDependencies=['publication-research.mjs','workspace-msgpack.mjs'];
export async function materializePickerRepair({root=join(homedir(),'.bonfyre/workspace-packages'),sourceRelease=pickerBaseRelease}={}){
 assert(/^[a-f0-9]{64}$/.test(sourceRelease),'Choose an exact retained source package.');
 const files=await loadWorkspaceFiles(root,sourceRelease),before=new Map(files);
 assert(files.get('instruments/author.mjs')?.toString().includes('mountPublicationPicker')&&files.has('instruments/production/index.html'),'The retained production editor is unavailable.');
 for(const name of pickerDependencies)assert(files.get('bridge/'+name)?.equals(await readFile(join(repo,'integrations/browser',name))),'The installed picker dependency changed: '+name);
 assert(files.has('bridge/publication-picker.mjs'),'The retained picker is unavailable.');
 files.set('bridge/publication-picker.mjs',await readFile(join(repo,'integrations/browser/publication-picker.mjs')));
 const changedFiles=[...before].filter(([name,body])=>!body.equals(files.get(name))).map(([name])=>name);
 assert.deepEqual(changedFiles,['bridge/publication-picker.mjs'],'Only the source picker must change.');
 return {...await retainWorkspaceMaterial(files,{root}),base:sourceRelease,changedFiles,preservedFiles:files.size-1};
}
const version=release=>projectConnectedInstrumentsSetup(release).find(row=>row.type==='pack_version').value;
export const pickerUpgradeSpec=Object.freeze({
 id:'publication-picker-upgrade',schema:'bonfyre.publication-picker-upgrade.v1',target:pickerUpgradeTarget,
 sourcePaths:['scripts/release/publication_picker_upgrade.mjs','scripts/workspace_material.mjs','scripts/publish_experience_setups.mjs','scripts/tests/publication-picker-release.test.mjs','scripts/tests/authored-workspace-sources.test.mjs',...['publication-picker.mjs',...pickerDependencies].map(name=>'integrations/browser/'+name)],
 baseRoot:release=>{assert.equal(release,pickerBaseRelease,'The installed connected editor changed; review its exact package.');return version(release).root;},
 versionFor:(candidate,base)=>({...version(candidate),parent_root:base?version(base).root:null,changed:'Recheck exact retained passages and receipts when adding selected sources; package '+candidate,proof:'scripts/tests/authored-workspace-sources.test.mjs; integrations/browser/tests/publication-research.test.mjs'}),
 materialize:materializePickerRepair,
 verify:async({root,id})=>assert((await readWorkspaceFile(root,id,'bridge/publication-picker.mjs')).equals(await readFile(join(repo,'integrations/browser/publication-picker.mjs'))),'The retained picker differs from its reviewed source.')
});
if(import.meta.main){
 let adapter;try{
  const [mode,candidate,...extra]=process.argv.slice(2);assert(!extra.length&&['review','apply'].includes(mode)&&(mode==='review'?!candidate:/^[a-f0-9]{64}$/.test(candidate)),'Use review, then apply EXACT_REVIEWED_PACKAGE_SHA256.');
  adapter=nativeWorkspaceUpgradeAdapter({spec:pickerUpgradeSpec});
  console.log(JSON.stringify(mode==='review'?await reviewWorkspaceUpgrade(adapter,pickerUpgradeSpec):await applyWorkspaceUpgrade(adapter,candidate,pickerUpgradeSpec)));
 }catch(error){console.log(JSON.stringify({state:'stopped',reason:error.message,next_step:'Keep the review and journal; reconcile native state before any further write.'}));process.exitCode=1;}finally{adapter?.close();}
}
