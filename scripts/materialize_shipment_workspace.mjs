// The recovered browser workflow becomes an installed private application.
// Source algorithms and native persistence owners are reused without patches.
import {readFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {retainWorkspaceFiles} from './materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from './workspace_continuation.mjs';
import {sampleFiles,SAMPLE_SHIPMENTS} from '../components/BonfyreServiceSite/shipment-material.mjs';
const repo=resolve(import.meta.dir,'..');
export const shipmentPack='experience.shipment-archive.workspace';
export const shipmentSources=Object.freeze(['integrations/browser/shipment-native-model.mjs','integrations/browser/shipment-workspace.mjs','integrations/browser/shipment-workspace.css','integrations/browser/interaction-collection.mjs','components/BonfyreServiceSite/shipment-check.mjs','components/BonfyreServiceSite/instrument-store.mjs']);
export async function shipmentWorkspaceMaterial(options={}){
 const files=new Map(),inputs=[];for(const path of shipmentSources){const b=await readFile(join(repo,path));files.set(path,b);inputs.push([path,createHash('sha256').update(b).digest('hex')]);}
 for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','procurement-order-intent-handoff.mjs'])files.set('components/bridge/'+name,await readFile(join(repo,'integrations/browser',name)));
 files.set('components/BonfyreServiceSite/instrument-local-store.mjs',await readFile(join(repo,'integrations/browser/extension/store.mjs')));
 for(const[name,body]of Object.entries(sampleFiles()))files.set('sample/'+name,body);files.set('sample/shipments.csv',Buffer.from(SAMPLE_SHIPMENTS));
 files.set('shipments/index.html',Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Shipment &amp; archive review · Aurekai</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__CONTEXT__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}</style><link rel="stylesheet" href="../integrations/browser/shipment-workspace.css"></head><body>${workspaceNavigation}<main class="shipment-workspace" data-shipment-workspace></main><script type="module" src="../integrations/browser/shipment-workspace.mjs"></script></body></html>`));
 files.set('shipments/procurement/index.html',Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Procurement shipment handoff · Aurekai</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__CONTEXT__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}</style></head><body>${workspaceNavigation}<main data-procurement-order-intent-handoff></main><script type="module" src="../../components/bridge/procurement-order-intent-handoff.mjs"></script></body></html>`));
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'shipment-batch',label:'Shipment review',entry:'shipments/',prefix:'shipment-batch:',title:'name',query:'record=$key'},{view:'procurement-shipment',label:'Procurement shipment handoff',entry:'shipments/procurement/',prefix:'procurement-order-intent:',title:'id',query:'intent=$key'}],files));
 files.set('SOURCE.tsv',Buffer.from('source_path\tsha256\n'+inputs.map(r=>r.join('\t')).join('\n')+'\n'));
 const material=await retainWorkspaceFiles(files,options);return{...material,files:files.size,pack:shipmentPack,family:'shipment-review',entry:'shipments/',prefix:'shipment-batch:'};
}
export function shipmentWorkspaceSetup(release){
 if(!/^[a-f0-9]{64}$/.test(release))throw Error('Use the reviewed shipment package digest.');const pack=shipmentPack,version='recovered-20261022-'+release.slice(0,16),rows=[];const add=(type,key,values)=>rows.push({type,key,value:{...values,status:'published'}});
 add('pack_artifact',{pack},{pack,canonical_name:'Shipment & archive review',publisher:'aurekai',layer:'official',kind:'base',purpose:'Review original shipment files, duplicates, date corrections and missing proofs; retain exact account batches and export a checked archive.',governed_by:'Original shipment checker and native account workspace records',cannot_infer:'A reviewed document or proposed filename is not independent delivery verification, a carrier update or an accounting post.'});
 add('pack_version',{pack,version},{pack,version,root:release,changed:'Recovered original shipment/archive workflow with private native persistence and exact-file continuations.'});
 add('pack_head',{pack,channel:'stable'},{pack,channel:'stable',version,advanced_by:'aurekai-workflow-recovery'});
 add('pack_demand',{pack,demand_kind:'workspace',needs:'shipment-review'},{pack,demand_kind:'workspace',needs:'shipment-review',preferred_sources:release,fields:'shipments/'});
 add('pack_demand',{pack,demand_kind:'workspace-record',needs:'shipment-review',fields:'shipment-batch:'},{pack,demand_kind:'workspace-record',needs:'shipment-review',fields:'shipment-batch:'});
 add('pack_demand',{pack,demand_kind:'workspace-record',needs:'shipment-review',fields:'procurement-order-intent:'},{pack,demand_kind:'workspace-record',needs:'shipment-review',fields:'procurement-order-intent:',body_regime:'LOCAL',reheat_when:'authorized read-only downstream procurement review; never shipment or fulfillment authority',optional:'false'});return rows;
}
if(import.meta.main)console.log(await shipmentWorkspaceMaterial());
