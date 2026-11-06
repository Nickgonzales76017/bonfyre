import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
import {continuationManifest,workspaceNavigationCSS} from './workspace_continuation.mjs';
import {parseContinuationManifest} from '../integrations/browser/workspace-continuation.mjs';

export const roomProcurementBase='b4aa0852633f6ce39d448f1446346c97d198a6e40bc62e7357c0baee16e581b8';

const source=new URL('../components/BonfyreServiceSite/',import.meta.url);
const bridge=new URL('../integrations/browser/',import.meta.url);
const wasm=new URL('../cmd/BonfyreProcurement/wasm/procurement.wasm',import.meta.url);
const hash=body=>createHash('sha256').update(body).digest('hex');

export function roomProcurementFiles(original){
 const files=new Map(original),members=new Map(),catalogues=[],entries={};

 for(const entry of ['product-room','red3-planner']){
  const html=original.get(entry+'/index.html').toString();
  const config=JSON.parse(html.match(/<script[^>]*data-planner-config[^>]*>([\s\S]*?)<\/script>/)[1]);
  assert(config.catalogue?.digest);
  assert(!entries[config.catalogue.digest]);
  catalogues.push(config.catalogue);
  entries[config.catalogue.digest]=entry;
 }

 for(const name of [
  'room-procurement.mjs',
  'room-procurement-view.mjs',
  'room-procurement.css',
  'planner-model.mjs',
  'catalogue-release.mjs',
  'catalogue-model.mjs',
  'catalogue-preparation.mjs',
  'workspace-lifetime.mjs'
 ])members.set(name,readFileSync(new URL(name,source)));

 const lifetime=members.get('workspace-lifetime.mjs').toString();
 const lifetimeImport="endpoint=>import(endpoint.replace(/_records$/,'bridge/workspace-store.mjs'))";
 assert(lifetime.includes(lifetimeImport));
 members.set('workspace-lifetime.mjs',Buffer.from(lifetime.replace(lifetimeImport,"()=>import('./workspace-store.mjs')")));

 for(const name of [
  'workspace-store.mjs',
  'workspace-continuation.mjs',
  'workspace-msgpack.mjs',
  'workspace-value.mjs',
  'procurement-wasm.mjs',
  'procurement-enactment-client.mjs',
  'procurement-rfq-workbench.mjs'
 ])members.set(name,readFileSync(new URL(name,bridge)));
 members.set('procurement.wasm',readFileSync(wasm));

 const continuation=members.get('procurement-rfq-workbench.mjs').toString();
 const rewritten=continuation
  .replace("../../components/BonfyreServiceSite/workspace-lifetime.mjs","./workspace-lifetime.mjs")
  .replace("../../components/BonfyreServiceSite/catalogue-release.mjs","./catalogue-release.mjs");
 assert.notEqual(rewritten,continuation);
 assert(!rewritten.includes('../../components/BonfyreServiceSite/'));
 members.set('procurement-rfq-workbench.mjs',Buffer.from(rewritten));

 const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([name,body])=>[name,hash(body)]));
 const prefix='data/ui-'+hash(JSON.stringify(identity))+'/procurement/';
 for(const [name,body]of members){
  assert(!files.has(prefix+name));
  files.set(prefix+name,body);
 }

 const link='<a href="/apps/__WORKSPACE__/procurement/">Purchase lists</a>';
 for(const entry of ['product-room/index.html','red3-planner/index.html','catalogue/index.html']){
  const html=original.get(entry).toString();
  const nav=html.match(/<nav data-native-workspace aria-label="Planning tools">[\s\S]*?<\/nav>/)?.[0];
  assert(nav);
  files.set(entry,Buffer.from(html.replace(nav,nav.replace('</nav>',link+'</nav>'))));
 }

 const cfg=JSON.stringify({catalogues,entries}).replaceAll('<','\\u003c');
 const base='/apps/__WORKSPACE__/';
 const html='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex"><title>Room procurement · Aurekai</title><meta name="bonfyre-workspace-store" content="'+base+'_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><link rel="stylesheet" href="'+base+'release.css"><link rel="stylesheet" href="'+base+prefix+'room-procurement.css"><style>'+workspaceNavigationCSS+'</style></head><body>__WORKSPACE_NAV__<main class="rp-main" data-purchase-lists><a href="'+base+'product-room/">← Room planning</a><h1>Room procurement</h1><p>Turn a saved room into a retained purchase list, continue it into supplier-request drafts, retain observed quotes, compare exact revisions, prepare allocation plans, reconcile authority-bound approval requests, and prepare order-intent proposals without implying supplier contact, quote acceptance, commitment or ordering.</p><div class="rp-bar"><label>Saved room<select data-ui="room"><option value="">Loading saved rooms…</option></select></label><button data-ui="prepare" disabled>Preview purchase list</button></div><div class="rp-bar"><label>Saved purchase list<select data-ui="saved"><option value="">Loading saved lists…</option></select></label></div><p role="status" data-ui="status" aria-live="polite"></p><p role="alert" data-ui="error"></p><section class="rp-card" data-ui="review" hidden aria-label="Purchase list review"></section><div class="rp-bar"><button data-ui="save" disabled>Save purchase list</button><button data-ui="refresh" disabled>Refresh from saved room</button><button data-ui="export" disabled>Download purchase list</button></div><p>Saving retains drafts, observations, comparisons, allocation plans, approval requests and order-intent proposals in this workspace. Supplier communication, quote acceptance, purchase approval, CommerceChain admission, reservation, commitments, orders, payment, settlement, fulfillment and shipment remain separate stages.</p></main><script type="application/json" data-purchase-config>'+cfg+'</script><script type="module" src="'+base+prefix+'room-procurement-view.mjs"></script><script type="module" src="'+base+prefix+'procurement-rfq-workbench.mjs"></script></body></html>';
 files.set('procurement/index.html',Buffer.from(html));

 const views=parseContinuationManifest(original.get('CONTINUATIONS.tsv').toString());
 const additions=[
  {view:'purchase-list',label:'Room purchase list',entry:'procurement/',prefix:'procurement-draft:',title:'title',query:'list=$key'},
  {view:'supplier-request',label:'Supplier request',entry:'procurement/',prefix:'procurement-rfq:',title:'id',query:'rfq=$key'},
  {view:'supplier-quote',label:'Supplier quote observation',entry:'procurement/',prefix:'procurement-quote:',title:'id',query:'quote=$key'},
  {view:'quote-comparison',label:'Quote comparison',entry:'procurement/',prefix:'procurement-comparison:',title:'status',query:'comparison=$key'},
  {view:'allocation-plan',label:'Allocation plan',entry:'procurement/',prefix:'procurement-allocation:',title:'id',query:'allocation=$key'},
  {view:'approval-request',label:'Approval request',entry:'procurement/',prefix:'procurement-approval:',title:'id',query:'approval=$key'},
  {view:'order-intent',label:'Order intent proposal',entry:'procurement/',prefix:'procurement-order-intent:',title:'id',query:'orderIntent=$key'}
 ];
 for(const addition of additions)assert(!views.some(view=>view.view===addition.view));
 files.set('CONTINUATIONS.tsv',continuationManifest([...views,...additions],files));

 const changed=['product-room/index.html','red3-planner/index.html','catalogue/index.html','CONTINUATIONS.tsv'];
 for(const [name,body]of original)if(!changed.includes(name))assert(body.equals(files.get(name)),'Historical package member changed: '+name);
 return{files,prefix,changed,modules:members.size};
}

export async function materializeRoomProcurement({root,base=roomProcurementBase}){
 assert.equal(base,roomProcurementBase);
 const {files,...projection}=roomProcurementFiles(await loadWorkspaceFiles(root,base));
 return{...await retainWorkspaceMaterial(files,{root}),base,...projection};
}
