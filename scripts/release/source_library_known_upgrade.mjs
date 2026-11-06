// Source Library + ForeignTwin catalogue lane through the existing governed
// native workspace upgrade owner. No implicit private record creation.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {materializeSourceLibrary,sourceLibraryPack} from '../materialize_source_library.mjs';
import {manifestRows,readWorkspaceFile,retainWorkspaceMaterial} from '../workspace_material.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';

const repository=resolve(import.meta.dir,'../..');
const sha=value=>createHash('sha256').update(value).digest('hex');
const lanePath='integrations/browser/source-library-known.mjs';
export const sourceLibraryKnownTarget=Object.freeze({
 pack:sourceLibraryPack,subject:'user:19',scope:'self',
 binding:'ws-263b80ea03eb96c910a5422a190b34bc63e09ed2',
 family:'source-materials',entry:'sources/'
});
export async function materializeKnownSourceLibrary({root=join(homedir(),'.bonfyre/workspace-packages')}={}){
 const base=await materializeSourceLibrary({root});
 const manifest=readFileSync(join(root,base.id,'MANIFEST.tsv'),'utf8');
 const entries=manifestRows(manifest),files=new Map();
 for(const row of entries)files.set(row.path,await readWorkspaceFile(root,base.id,row.path));
 const html=files.get('sources/index.html')?.toString('utf8');
 const original='<script type="module" src="./library.mjs"></script>';
 assert(html&&html.includes(original)&&html.indexOf(original)===html.lastIndexOf(original),'The existing Source Library entrypoint changed.');
 files.set('sources/index.html',Buffer.from(html.replace(original,original+'<script type="module" src="./known-sources.mjs"></script>')));
 const lane=readFileSync(join(repository,lanePath));
 assert(lane.length>0&&lane.length<128*1024,'The known-source module exceeds its bound.');
 files.set('sources/known-sources.mjs',lane);
 const source=files.get('SOURCE.tsv')?.toString('utf8');
 assert(source?.startsWith('source\tsha256\n')&&!source.includes(lanePath+'\t'),'Source manifest is not the expected original.');
 files.set('SOURCE.tsv',Buffer.from(source+lanePath+'\t'+sha(lane)+'\n'));
 return retainWorkspaceMaterial(files,{root});
}
export const sourceLibraryKnownUpgradeSpec=Object.freeze({
 id:'source-library-known-upgrade',schema:'bonfyre.source-library-known-upgrade.v1',
 target:sourceLibraryKnownTarget,
 sourcePaths:['scripts/release/source_library_known_upgrade.mjs','scripts/release/workspace_upgrade.mjs',
  'scripts/materialize_source_library.mjs','scripts/workspace_material.mjs','scripts/workspace_continuation.mjs',
  lanePath,'cmd/BonfyreSourceFrontier/tools/source_material.mjs',
  'integrations/browser/source-library.mjs','integrations/browser/workspace-store.mjs',
  'integrations/browser/workspace-destinations.mjs',
  'scripts/warehouse_aurekai_page.mjs',
  'integrations/browser/warehouse-portfolio.mjs',
  'integrations/browser/repository-cuts.mjs',
  'integrations/browser/repository-cut-intake.mjs',
  'integrations/browser/repository-pack-intake.mjs',
  'integrations/browser/repository-fpq-review.mjs',
  'integrations/browser/repository-cut-studio.mjs',
  'integrations/browser/warehouse-studio.mjs',
  'integrations/browser/program-service-continuations.mjs',
  'integrations/browser/program-service-policy.mjs',
  'cmd/BonfyreWarehouse/src/warehouse_portfolio.h'],
 baseRoot:release=>release,
 versionFor:(candidate,base)=>({
  pack:sourceLibraryPack,version:'source-known-'+candidate.slice(0,40),
  root:candidate,parent_root:base,changed:'Known Sources, Warehouse Intelligence, reviewed Git/JOSH original retention, source-bound FPQ review and PackGraph drafts with installed-app continuations; package '+candidate,
  proof:'scripts/release/tests/source_library_known_upgrade.test.mjs',status:'published'
 }),
 materialize:materializeKnownSourceLibrary,
 verify:async({root,id})=>{
  const html=(await readWorkspaceFile(root,id,'sources/index.html')).toString();
  assert(html.includes('<script type="module" src="./known-sources.mjs"></script>'),'Known source panel was not mounted.');
  const lane=await readWorkspaceFile(root,id,'sources/known-sources.mjs');
  assert.equal(sha(lane),sha(readFileSync(join(repository,lanePath))),'Known source module changed.');
  const material=await readWorkspaceFile(root,id,'sources/source-material.mjs');
  assert(material.includes(Buffer.from('export function knownSourcePage(')),'Retained-source contract is missing.');
  const warehouse=(await readWorkspaceFile(root,id,'sources/warehouse/index.html')).toString();
  assert(warehouse.includes('data-warehouse-studio')&&warehouse.includes('bonfyre-workspace-store'),
    'Warehouse intelligence must be bound inside this source-library workspace.');
  assert((await readWorkspaceFile(root,id,'sources/index.html')).toString().includes('./warehouse/'),
    'Warehouse entry is missing from Source Library.');
  const repositoryPage=(await readWorkspaceFile(root,id,'sources/warehouse/index.html')).toString();
  assert(repositoryPage.includes('data-repository-cut-studio')&&
    repositoryPage.includes('repository-cut-studio.mjs'),
    'Repository cuts must be inside the existing Source Library successor.');
  const cutClient=await readWorkspaceFile(root,id,'sources/repository-cut-studio.mjs');
  const cutModel=await readWorkspaceFile(root,id,'bridge/repository-cuts.mjs');
  const intake=await readWorkspaceFile(root,id,'bridge/repository-cut-intake.mjs');
  const packIntake=await readWorkspaceFile(root,id,'bridge/repository-pack-intake.mjs');
  const fpq=await readWorkspaceFile(root,id,'bridge/repository-fpq-review.mjs');
  assert(cutClient.includes(Buffer.from('readRepositoryCutCollection'))&&
    cutModel.includes(Buffer.from('repositoryCutContinuations'))&&
    intake.includes(Buffer.from('saveReviewedRepositoryCut'))&&
    packIntake.includes(Buffer.from('repositoryCutPackDemand'))&&
    fpq.includes(Buffer.from('reviewRepositoryFPQGeneration'))&&
    cutClient.includes(Buffer.from('Prepare local FPQ review')),
    'Repository cut continuation must retain the exact native source-cut digest.');
  const client=await readWorkspaceFile(root,id,'sources/warehouse-studio.mjs');
  const contract=await readWorkspaceFile(root,id,'bridge/warehouse-portfolio.mjs');
  assert(client.includes(Buffer.from('warehouseInstalledTools'))&&
    contract.includes(Buffer.from('programServiceContinuations')),
    'Warehouse continuation must use account-owned installed workspaces.');
 }
});
export const reviewSourceLibraryKnownUpgrade=adapter=>reviewWorkspaceUpgrade(adapter,sourceLibraryKnownUpgradeSpec);
export const applySourceLibraryKnownUpgrade=(adapter,candidate)=>applyWorkspaceUpgrade(adapter,candidate,sourceLibraryKnownUpgradeSpec);
export const nativeSourceLibraryKnownUpgradeAdapter=(options={})=>nativeWorkspaceUpgradeAdapter({...options,spec:sourceLibraryKnownUpgradeSpec});
if(import.meta.main){
 let adapter;
 try{
  const [mode,candidate,...extra]=process.argv.slice(2);
  assert(extra.length===0&&['build','review','apply'].includes(mode)&&
   (mode==='apply'?/^[a-f0-9]{64}$/.test(candidate||''):!candidate),
   'Use build, review, or apply EXACT_REVIEWED_PACKAGE_SHA256.');
  if(mode==='build'){
   const pack=await materializeKnownSourceLibrary();
   await sourceLibraryKnownUpgradeSpec.verify({root:join(homedir(),'.bonfyre/workspace-packages'),id:pack.id});
   console.log(JSON.stringify({state:'materialized_uninstalled',candidate:pack.id,files:pack.files}));
  }else{
   adapter=nativeSourceLibraryKnownUpgradeAdapter();
   console.log(JSON.stringify(mode==='review'?await reviewSourceLibraryKnownUpgrade(adapter):await applySourceLibraryKnownUpgrade(adapter,candidate)));
  }
 }catch(e){
  console.log(JSON.stringify({state:'stopped',reason:e.message,
   next_step:'Keep the reviewed package and journal; reconcile the exact native owner before any replay.'}));
  process.exitCode=1;
 }finally{adapter?.close();}
}
