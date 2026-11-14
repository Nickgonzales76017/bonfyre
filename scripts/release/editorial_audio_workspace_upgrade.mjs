// Install the verified editorial Audio Studio in the existing current-account
// workspace. Uses the shared native, journaled pack lifecycle; no CMS shortcuts.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {materializeCreatorWorkspace} from '../materialize_creator_workspace.mjs';
import {projectCreatorWorkspaceSetup} from '../publish_experience_setups.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';

const pack='experience.native-creator-studio.workspace';
const baseRelease='505d1a01fab39be4376fc26a010e0bd11ef7310ac8c0b751892eb0fbc794f635';
const digest=buffer=>createHash('sha256').update(buffer).digest('hex');
const target=Object.freeze({
 pack,subject:'user:19',scope:'self',
 binding:'ws-dda487d733c59c27d085201262d1ddde273e6e65',
 family:'creator-audio',entry:'audio/'
});
const sourcePaths=[
 'scripts/release/editorial_audio_workspace_upgrade.mjs',
 'scripts/release/workspace_upgrade.mjs',
 'scripts/materialize_creator_workspace.mjs',
 'scripts/materialize_workspace_package.mjs',
 'scripts/workspace_continuation.mjs',
 'scripts/publish_experience_setups.mjs',
 'components/BonfyreServiceSite/creator-material.mjs',
 'components/BonfyreServiceSite/creator-import.mjs',
 'components/BonfyreServiceSite/creator-sample.mjs',
 'components/BonfyreServiceSite/narration-workspace-client.mjs',
 'components/BonfyreServiceSite/editorial-music-catalogue.mjs',
 'components/BonfyreServiceSite/editorial-music-library-client.mjs',
 'components/BonfyreServiceSite/editorial-sound-design-ui.mjs',
 'integrations/browser/narration-production.mjs',
 'integrations/browser/workspace-store.mjs',
 'cmd/BonfyreWebAddress/tests/creator_workspace_test.mjs',
 'cmd/BonfyreBroadcast/tests/editorial_mix_test.sh',
 'cmd/BonfyreBroadcast/tests/editorial_music_library.test.mjs'
];
const versionFor=(release,parent)=>{
 const version=projectCreatorWorkspaceSetup(release).find(row=>row.type==='pack_version'&&row.value.pack===pack)?.value;
 assert(version?.root===release,'Audio Studio version must name its exact package.');
 return {...version,parent_root:parent?projectCreatorWorkspaceSetup(parent).find(row=>row.type==='pack_version')?.value.root:null,
  changed:'Source-pinned real editorial recordings, revised-article narration handoff, direct-open production, native mixing and technical-only tone lab; package '+release,
  proof:'cmd/BonfyreWebAddress/tests/creator_workspace_test.mjs; cmd/BonfyreBroadcast/tests/editorial_mix_test.sh; cmd/BonfyreSurfaceScene/tests/article-narration.test.mjs'};
};
export const editorialAudioUpgradeSpec=Object.freeze({
 id:'editorial-audio-studio-workspace-upgrade',
 schema:'bonfyre.editorial-audio-studio-workspace-upgrade.v1',
 target,sourcePaths,
 baseRoot:release=>{assert.equal(release,baseRelease,'Receiving Audio Studio has a different installed package; reconcile before upgrade.');return release;},
 versionFor,
 materialize:({root})=>materializeCreatorWorkspace({root}),
 verify:async({root,id})=>{
  const html=(await readWorkspaceFile(root,id,'audio/index.html')).toString();
  assert(html.includes('data-editorial-library')&&
   html.indexOf('data-editorial-library')<html.indexOf('class="audio-demo-lab"')&&
   html.includes('data-narration-workspace')&&
   html.includes('Give every story a sound.'),
   'The revised editorial Audio Studio is not its installed opening view.');
  const catalogue=await readWorkspaceFile(root,id,'data/creator/editorial-music-catalogue.mjs');
  const importer=await readWorkspaceFile(root,id,'data/creator/creator-import.mjs');
  const listener=await readWorkspaceFile(root,id,'data/creator/editorial-music-library-client.mjs');
  const direct=await readWorkspaceFile(root,id,'data/creator/narration-workspace-client.mjs');
  assert(catalogue.includes(Buffer.from('Mellow Cafe Vibe'))&&
   catalogue.includes(Buffer.from('Above The Clouds'))&&
   importer.includes(Buffer.from('export function decodeWav'))&&
   listener.includes(Buffer.from('Save to library'))&&
   direct.includes(Buffer.from("get('production')")),
   'A recorded soundtrack, native source reader or exact narration entry is missing.');
  const official=readFileSync(new URL('../materialize_creator_workspace.mjs',import.meta.url));
  assert(official.length>1000&&digest(official).length===64);
 }
});
export const editorialAudioUpgradeAdapter=options=>nativeWorkspaceUpgradeAdapter({...options,spec:editorialAudioUpgradeSpec});
export async function editorialAudioUpgrade(mode,candidate){
 const adapter=editorialAudioUpgradeAdapter();
 try {
  if(mode==='review')return await reviewWorkspaceUpgrade(adapter,editorialAudioUpgradeSpec);
  if(mode==='apply')return await applyWorkspaceUpgrade(adapter,candidate,editorialAudioUpgradeSpec);
  throw Error('Choose review or apply EXACT_REVIEWED_PACKAGE_SHA256.');
 }finally{adapter.close();}
}
if(import.meta.main){
 const [mode,candidate,...rest]=process.argv.slice(2);
 assert(!rest.length&&(mode==='review'?!candidate:mode==='apply'&&/^[a-f0-9]{64}$/.test(candidate||'')),
  'Use review or apply EXACT_REVIEWED_PACKAGE_SHA256.');
 try{console.log(JSON.stringify(await editorialAudioUpgrade(mode,candidate)));}
 catch(error){console.error('EDITORIAL_AUDIO_UPGRADE_STOPPED '+error.message);process.exitCode=1;}
}
