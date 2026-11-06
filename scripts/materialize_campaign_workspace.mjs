// Source-to-campaign uses the original linked-artboard arithmetic and the
// existing source reader, account record and blob owners. No social publisher.
import {readFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {retainWorkspaceFiles} from './materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from './workspace_continuation.mjs';
const root=resolve(import.meta.dir,'..');
export const campaignPack='experience.source-campaign.workspace';
export const campaignSources=Object.freeze(['integrations/browser/campaign-native-model.mjs','integrations/browser/campaign-workspace.mjs','integrations/browser/campaign-workspace.css','integrations/browser/interaction-collection.mjs','integrations/browser/publication-picker.mjs','integrations/browser/publication-research.mjs','integrations/browser/workspace-msgpack.mjs','components/BonfyreServiceSite/campaign-composer.mjs','components/BonfyreServiceSite/instrument-store.mjs']);
export async function campaignWorkspaceMaterial(options={}){
 const files=new Map(),inputs=[];for(const path of campaignSources){const bytes=await readFile(join(root,path));files.set(path,bytes);inputs.push([path,createHash('sha256').update(bytes).digest('hex')]);}
 for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs'])files.set('components/bridge/'+name,await readFile(join(root,'integrations/browser',name)));
 files.set('components/BonfyreServiceSite/instrument-local-store.mjs',await readFile(join(root,'integrations/browser/extension/store.mjs')));
 files.set('campaigns/index.html',Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Source-to-campaign studio · Aurekai</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__CONTEXT__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}</style><link rel="stylesheet" href="../integrations/browser/campaign-workspace.css"></head><body>${workspaceNavigation}<main data-campaign-workspace class="campaign-workspace"></main><script type="module" src="../integrations/browser/campaign-workspace.mjs"></script></body></html>`));
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'source-campaign',label:'Source campaign',entry:'campaigns/',prefix:'source-campaign:',title:'name',query:'record=$key'}],files));
 files.set('SOURCE.tsv',Buffer.from('source_path\tsha256\n'+inputs.map(r=>r.join('\t')).join('\n')+'\n'));
 return{...await retainWorkspaceFiles(files,options),files:files.size,pack:campaignPack,family:'source-campaign',entry:'campaigns/',prefix:'source-campaign:'};
}
export function campaignWorkspaceSetup(release){
 if(!/^[a-f0-9]{64}$/.test(release))throw Error('Use the verified campaign package digest.');const pack=campaignPack,version='recovered-20261022-'+release.slice(0,16),rows=[],add=(type,key,values)=>rows.push({type,key,value:{...values,status:'published'}});
 add('pack_artifact',{pack},{pack,canonical_name:'Source-to-campaign studio',publisher:'aurekai',layer:'official',kind:'base',purpose:'Retain a source and citation, compose three linked artboards, and save actual image and motion outputs in the current account.',governed_by:'Original campaign arithmetic and native source/workspace owners',cannot_infer:'A matching quotation or a rendered output does not grant reuse rights, verify a claim, or publish to a social account.'});
 add('pack_version',{pack,version},{pack,version,root:release,changed:'Recovered source-linked artboard workflow with current account storage and verifiable renderings.'});add('pack_head',{pack,channel:'stable'},{pack,channel:'stable',version,advanced_by:'aurekai-workflow-recovery'});
 add('pack_demand',{pack,demand_kind:'workspace',needs:'source-campaign'},{pack,demand_kind:'workspace',needs:'source-campaign',preferred_sources:release,fields:'campaigns/'});
 add('pack_demand',{pack,demand_kind:'workspace-record',needs:'source-campaign',fields:'source-campaign:'},{pack,demand_kind:'workspace-record',needs:'source-campaign',fields:'source-campaign:'});return rows;
}
if(import.meta.main)console.log(await campaignWorkspaceMaterial());
