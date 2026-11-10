// Extend the existing connected-instrument package through its ordinary material
// and PackGraph owners. Existing domain controllers remain reachable unchanged.
import {readFile} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
import {projectConnectedInstrumentMaterial,connectedInstrumentViews} from './materialize_workspace_package.mjs';
import {projectConnectedInstrumentsSetup} from './publish_experience_setups.mjs';
import {materializeReferenceRepair} from './materialize_reference_repair.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from './workspace_continuation.mjs';
const repo=join(dirname(fileURLToPath(import.meta.url)),'..');
export const authoredSource={id:'fad60f319bc25583dce678605babb7c4946c52d343ae8c005458a0c163b7acfb',commit:'396bcaf02f24bd96c1789aee5410d111edbf0818',tree:'e27b1a7640d51bbace199793471a271674513f43'};
export const authoredView=Object.freeze({view:'production',label:'Production studio',entry:'instruments/production/',prefix:'draft:instruments:composition:',title:'title',query:'record=$key'});
export const checksView=Object.freeze({view:'checks',label:'Data checks',entry:'instruments/',prefix:'draft:instruments:checks:',title:'title',query:'tool=@checks&record=$key'});
export async function materializeAuthoredWorkspace({root=join(homedir(),'.bonfyre/workspace-packages'),sourceRoot=join(homedir(),'.bonfyre/workspace-packages')}={}){
  const source=await loadWorkspaceFiles(sourceRoot,authoredSource.id);
  const original=await projectConnectedInstrumentMaterial(source,{commit:authoredSource.commit,sourceTree:authoredSource.tree,root});
  const files=await loadWorkspaceFiles(root,original.id),inputs=[];
  for(const [path,target] of [
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs','instruments/production-runtime/authored-workspace.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-publication.mjs','instruments/production-runtime/authored-publication.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace-transfer.mjs','instruments/production-runtime/authored-workspace-transfer.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs','instruments/production-runtime/authored-workspace-recovery.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-instrument-validation.mjs','instruments/production-runtime/authored-instrument-validation.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-data-checks.mjs','instruments/production-runtime/authored-data-checks.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-data-checks-view.mjs','instruments/checks-view.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery-view.mjs','instruments/production-recovery.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace-destination.mjs','instruments/production-destination.mjs'],
    ['integrations/browser/workspace-destinations.mjs','bridge/workspace-destinations.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/renderers/workspace-composition.mjs','instruments/production-runtime/renderers/workspace-composition.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace-view.mjs','instruments/author.mjs'],
    ['cmd/BonfyreSurfaceScene/runtime/authored-workspace.css','instruments/author.css'],
    ['integrations/browser/publication-picker.mjs','bridge/publication-picker.mjs']]){
    const body=await readFile(join(repo,path));files.set(target,body);inputs.push(path+'\t'+createHash('sha256').update(body).digest('hex'));
  }
  files.set('instruments/production/index.html',Buffer.from(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Production studio · Aurekai</title>
<meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records">
<meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&family=Instrument+Serif:ital@0;1&display=swap">
<link rel="stylesheet" href="../instruments.css"><link rel="stylesheet" href="../author.css"><style>${workspaceNavigationCSS}</style>
</head><body class="production-application">${workspaceNavigation}<main id="production-authoring"><noscript>This workspace needs JavaScript to operate its editors. The native saved records remain intact.</noscript></main>
<script type="module">import('../author.mjs').catch(error=>{const p=document.createElement('p');p.setAttribute('role','alert');p.textContent='The production editor could not start: '+error.message;document.querySelector('#production-authoring').append(p);});</script>
</body></html>`));
  // A reused instrument's worker belongs to its module, not to the mounting page.
  const ui=files.get('instruments/ui.mjs').toString();
  if((ui.match(/new Worker\(url,/g)||[]).length!==2)throw Error('Review the changed native worker carrier.');
  files.set('instruments/ui.mjs',Buffer.from(ui.replaceAll('new Worker(url,','new Worker(new URL(url,import.meta.url),')));
  inputs.push('worker-url-binding\tmodule-relative; normal start and timeout restart');
  const replaceOnce=(path,before,after)=>{const source=files.get(path)?.toString();if(!source||source.split(before).length!==2)throw Error('Review the changed native instrument carrier: '+path);files.set(path,Buffer.from(source.replace(before,after)));};
  replaceOnce('instruments/working-record.mjs',"product:'product-view.mjs'}", "product:'product-view.mjs', checks:'checks-view.mjs'}");
  replaceOnce('instruments/app.mjs',"product:'Product story map'}", "product:'Product story map',checks:'Data checks'}");
  replaceOnce('instruments/app.mjs',"status('Working example loaded. Imports and saves stay in this destination; source searches run only when requested.');", "status(kind==='checks'?'Data checks needs a contract and reviewed input tables. Save working records in this destination.':'Working example loaded. Imports and saves stay in this destination; source searches run only when requested.');");
  replaceOnce('instruments/index.html','<div class="rail-bottom">','<a href="?tool=checks" data-tool="checks"><b>✓</b><span>Data checks</span></a><div class="rail-bottom">');
  inputs.push('checks-instrument-binding\tShared working record, standalone editor, production adapter and exact continuation');
  const profile=[['schema','bonfyre.authored-workspace-recovery.v1']];
  for(const [name,path] of [
    ['compiler_sha256','cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs'],
    ['validator_sha256','cmd/BonfyreSurfaceScene/runtime/authored-instrument-validation.mjs'],
    ['checks_sha256','cmd/BonfyreSurfaceScene/runtime/authored-data-checks.mjs'],
    ['composition_sha256','cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs'],
    ['renderer_sha256','cmd/BonfyreSurfaceScene/runtime/renderers/workspace-composition.mjs'],
    ['value_codec_sha256','integrations/browser/workspace-value.mjs'],
    ['msgpack_codec_sha256','integrations/browser/workspace-msgpack.mjs']])profile.push([name,createHash('sha256').update(await readFile(join(repo,path))).digest('hex')]);
  files.set('instruments/RECOVERY.tsv',Buffer.from(profile.map(row=>row.join('\t')).join('\n')+'\n'));
  files.set('CONTINUATIONS.tsv',continuationManifest([...connectedInstrumentViews,checksView,authoredView],files));
  const entry=files.get('instruments/index.html').toString();
  if(!entry.includes('</head>')||!entry.includes('<body>'))throw Error('Existing editor entry changed.');
  files.set('instruments/index.html',Buffer.from(entry.replace('<body>','<body><nav aria-label="Production authoring" style="padding:12px 20px;background:#e8eedc"><a href="./production/">Compose these tools in Production studio ↗</a></nav>')));
  const retained=files.get('SOURCE.tsv').toString();
  files.set('SOURCE.tsv',Buffer.from(retained+'\nauthored_projection\tNative SurfaceScene canvas; existing editor controllers, CMS record transactions and reviewed research handoff.\n'+inputs.join('\n')+'\n'));
  const authored=await retainWorkspaceMaterial(files,{root});
  // The ordinary production build owns reference reading too; future authored
  // releases must not silently resurrect the pre-repair research renderer.
  const readable=await materializeReferenceRepair({sourceRelease:authored.id,root});
  return {...readable,base:original.id,authoredBase:authored.id,entry:authoredView.entry};
}
export function authoredWorkspaceSetup(release){
  const rows=projectConnectedInstrumentsSetup(release),pack='experience.connected-instruments.workspace';
  for(const view of [authoredView,checksView])rows.splice(rows.length-1,0,{type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:'connected-instruments',fields:view.prefix},value:{pack,status:'published',demand_kind:'workspace-record',needs:'connected-instruments',fields:view.prefix,body_regime:'LOCAL',reheat_when:'explicit authored production save or continuation',optional:'false'}});
  return rows;
}
if(import.meta.main){const result=await materializeAuthoredWorkspace();for(const [key,value] of Object.entries(result))console.log(key+'\t'+value);}
