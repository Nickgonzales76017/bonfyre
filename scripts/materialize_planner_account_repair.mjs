import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
export const plannerAccountBase='986ef027731929d618b55a23729b7e9d2be40036ae862ff7e96f7bb77c6dda34';
const source=new URL('../components/BonfyreServiceSite/',import.meta.url),bridge=new URL('../integrations/browser/',import.meta.url),hash=b=>createHash('sha256').update(b).digest('hex');
function once(t,a,b){assert.equal(t.split(a).length,2,'Planner lifetime preimage changed: '+a);return t.replace(a,b);}
function pairs(t,changes){for(const[a,b]of changes)t=once(t,a,b);return t;}
function client(t){
 t=pairs(t,[
 ["import {readLocal,saveLocal} from './store.mjs';","import * as fallbackStore from './store.mjs';\nimport {createWorkspaceLifetime,clearWorkspaceHost,openWorkspaceStore} from './workspace-lifetime.mjs';\nconst lifetime=createWorkspaceLifetime();\nlet storage;\nconst readLocal=(...args)=>storage.readLocal(...args),saveLocal=(...args)=>storage.saveLocal(...args);\nconst listen=(node,name,fn)=>node.addEventListener(name,event=>{if(!lifetime.closed)return fn(event);},{signal:lifetime.signal});"],
 ["const error=message=>{","const error=message=>{if(lifetime.closed)return;"],
 ["const message=text=>{","const message=text=>{if(lifetime.closed)return;"],
 ["const valid=()=>{","const valid=()=>{if(lifetime.closed)return false;"],
 ["if(busy||!valid()","if(lifetime.closed||busy||!valid()"],
 ["}catch{}history.replaceState","}catch{}lifetime.assert();history.replaceState"],
 ["try {await navigator.clipboard.writeText(url.href);","try {await navigator.clipboard.writeText(url.href);if(lifetime.closed)return;"],
 ["}catch{ui('share-url').select();","}catch{if(lifetime.closed)return;ui('share-url').select();"],
 ["async function boot() {","async function boot() {\n  storage=await openWorkspaceStore(lifetime,{fallback:fallbackStore});lifetime.assert();"],
 ["catch(e){storageAvailable=false;","catch(e){if(lifetime.closed)return;storageAvailable=false;"],
 ["bindProjects({root,catalogue,prefix,","bindProjects({root,catalogue,prefix,lifetime,storage:{readLocal},"],
 ["isBusy:()=>busy,","isBusy:()=>lifetime.closed||busy,"],
 ["catch{error('Saved projects could not be opened.","catch{if(lifetime.closed)return;error('Saved projects could not be opened."],
 ["try{composition=await composePlannerWorkspace({root,catalogue,","if(lifetime.closed)return;\n  try{composition=await composePlannerWorkspace({root,catalogue,lifetime,"],
 ["});composition.renderView(root.dataset.workspaceView","});lifetime.assert();composition.renderView(root.dataset.workspaceView"],
 ["  root.dataset.ready='true';","  if(lifetime.closed)return;root.dataset.ready='true';"],
 ["spatial=createSpatialEditor({root,catalogue,","spatial=createSpatialEditor({root,catalogue,lifetime,"],
 ["productScene=createPlannerProductScene({root,catalogue,","productScene=createPlannerProductScene({root,catalogue,lifetime,storage:{readLocal,saveLocal},"],
 ["boot().catch(e=>error('The planner could not open. '+e.message));","lifetime.onClose(()=>spatial?.destroy());\nlifetime.onClose(()=>productScene?.destroy());\nlifetime.onClose(()=>composition?.dispose());\nlifetime.onClose(()=>projectWorkspace?.close());\nlifetime.onClose(()=>{planHistory.clear();lastValid=plan=saved=selected=receipt=null;revision=0;storageAvailable=false;busy=false;for(const d of root.querySelectorAll('dialog'))if(d.open)d.close();root.inert=false;clearWorkspaceHost(root);});\nboot().catch(e=>error('The planner could not open. '+e.message));"]
 ]);
 for(const name of ['controls','changed','fields','readFields','positionEditor','canvas','render','download'])t=t.replace(new RegExp('(function '+name+'\\(\\)\\s*\\{)'),'$1if(lifetime.closed)return;');
 for(const[name,arg]of [['editHistory','direction'],['spatialChange','next,primary=selected'],['replacePlan','next']])t=once(t,'function '+name+'('+arg+'){','function '+name+'('+arg+'){if(lifetime.closed)return;');
 t=once(t,'function select(key) {','function select(key) {if(lifetime.closed)return;');
 t=t.replaceAll('return !busy&&Boolean','return !lifetime.closed&&!busy&&Boolean').replaceAll('return !busy&&!conflict&&Boolean','return !lifetime.closed&&!busy&&!conflict&&Boolean');
 t=t.replace(/(root|ui\('[^']+'\)|ui\(axis\))\.addEventListener\(/g,'listen($1,');
 return t;
}
function projects(t){
 t=pairs(t,[
 ['fail, renderView})','fail, renderView,lifetime,storage={readLocal}})'],
 ['  const ui = name =>','  const {readLocal}=storage,closed=()=>Boolean(lifetime?.closed);\n  const listen=(node,name,fn)=>node.addEventListener(name,event=>{if(!closed())return fn(event);},{signal:lifetime?.signal});\n  const ui = name =>'],
 ['const writeURL = (url, replace = false) => {','const writeURL = (url, replace = false) => {if(closed())return;'],
 ['function renderComparison(preferred) {','function renderComparison(preferred) {if(closed())return;'],
 ['function render() {','function render() {if(closed())return;'],
 ['async function refresh() {','async function refresh() {if(closed())return;'],
 ['const result = projectRows(await readLocal(prefix), prefix, catalogue); projects = result.projects;','const rows=await readLocal(prefix);if(closed())return;const result=projectRows(rows,prefix,catalogue);projects=result.projects;'],
 ['if (transitioning || isBusy())','if(closed())return;\n    if (transitioning || isBusy())'],
 ['      await refresh();','      await refresh();if(closed())return;'],
 ['} catch (error) {fail(error.message);','} catch (error) {if(closed())return;fail(error.message);'],
 ['finally {transitioning = false; root.inert = false;}','finally {transitioning = false;if(!closed())root.inert = false;}'],
 ['function retainComparison() {','function retainComparison() {if(closed())return;'],
 ['async init() {view =','close(){projects=[];query=\'\';lastURL=\'\';},\n    async init() {if(closed())return;view ='],
 ['await refresh(); renderComparison(','await refresh();if(closed())return;renderComparison(']
 ]);
 t=t.replace('root.addEventListener(','listen(root,').replaceAll("  addEventListener(","  listen(globalThis,");
 t=t.replaceAll('event => {','event => {if(closed())return;').replaceAll('() => {','() => {if(closed())return;');
 t=t.replaceAll('() => void navigate','() => !closed() && void navigate');
 return t;
}
function workspace(t){
 return pairs(t,[
 ['select,review,fail})','select,review,fail,lifetime})'],
 ["const docking=await loadWorkspaceDocking(),ui=","const docking=await loadWorkspaceDocking();lifetime?.assert();const ui="],
 ["  const unused=ui('layout');","  lifetime?.onClose(()=>work.dispose());\n  const unused=ui('layout');"],
 ['renderView(view){','renderView(view){if(lifetime?.closed)return false;'],
 ['selected(key){','selected(key){if(lifetime?.closed)return;'],
 ['refresh(){','refresh(){if(lifetime?.closed)return;']
 ]);
}
function scene(t){
 return pairs(t,[
 ['onChange,canEdit,fail})','onChange,canEdit,fail,lifetime,storage={readLocal,saveLocal}})'],
 [" let plan,selection,"," const {readLocal,saveLocal}=storage;\n let plan,selection,"],
 ['function renderQuantities(){','function renderQuantities(){if(destroyed)return;'],
 ['if(!studio||destroyed)return;await studio.ready;','if(!studio||destroyed)return;await studio.ready;if(destroyed)return;'],
 ['if(await studio.restore(id))continue;','const restored=await studio.restore(id);if(destroyed)return;if(restored)continue;'],
 ['studio.selectProduct(id);await studio.importFiles(await publishedModelFiles(descriptor));','const files=await publishedModelFiles(descriptor);if(destroyed)return;studio.selectProduct(id);await studio.importFiles(files);if(destroyed)return;'],
 ['async function show(next){','async function show(next){if(destroyed)return;'],
 ['update(next,selected){','update(next,selected){if(destroyed)return;'],
 ['destroy(){destroyed=true;studio?.destroy();','destroy(){if(destroyed)return;destroyed=true;studio?.destroy();studio=null;plan=selection=null;attempted.clear();']
 ]);
}
function spatial(t){
 return pairs(t,[
 ['onChange,canEdit,fail})','onChange,canEdit,fail,lifetime})'],
 ['  let K,stage,','  let destroyed=false;\n  let K,stage,'],
 ['function controls() {','function controls() {if(destroyed)return;'],
 ['function select(list,notify=true) {','function select(list,notify=true) {if(destroyed)return;'],
 ['function drawSelection() {','function drawSelection() {if(destroyed)return;'],
 ['if(!stage||!plan||dragging)return;','if(destroyed||!stage||!plan||dragging)return;'],
 ['function zoomTo(value) {','function zoomTo(value) {if(destroyed)return;'],
 ['async function load() {','async function load() {if(destroyed)return;'],
 ['K=globalThis.Konva;','if(destroyed)return;K=globalThis.Konva;'],
 ['}catch(error){stage?.destroy();','}catch(error){if(destroyed)return;stage?.destroy();'],
 ['function action(name){if(!canEdit())return;','function action(name){if(destroyed||!canEdit())return;'],
 ['if(!stage)return;const link=','if(destroyed||!stage)return;const link='],
 ["  addEventListener('pageshow',event=>{if(event.persisted)location.reload();});\n  addEventListener('pagehide',()=>{observer.disconnect();stage?.destroy();});","  function destroy(){if(destroyed)return;destroyed=true;observer.disconnect();stage?.destroy();stage=null;layer=null;world=null;outline=null;marquee=null;plan=null;primary=null;keys.clear();nodes.clear();boxStart=panStart=null;delete host.dataset.ready;}\n  if(lifetime)lifetime.onClose(destroy);else{addEventListener('pageshow',event=>{if(event.persisted)location.reload();});addEventListener('pagehide',destroy,{once:true});}"],
 ['    update(next,selected){','    destroy,\n    update(next,selected){if(destroyed)return;']
 ]);
}
export function plannerAccountRepairFiles(original){
 const files=new Map(original),projections=[];
 for(const entry of ['product-room/index.html','red3-planner/index.html']){
  const html=original.get(entry).toString(),prior=html.match(/data\/ui-[a-f0-9]{64}\/planner\//)?.[0];assert(prior);
  const members=new Map([...original].filter(([n])=>n.startsWith(prior)).map(([n,b])=>[n.slice(prior.length),b]));assert.equal(members.size,29);
  for(const[n,change]of [['planner-client.mjs',client],['planner-project-client.mjs',projects],['planner-workspace.mjs',workspace],['planner-product-scene.mjs',scene],['planner-spatial.mjs',spatial]])members.set(n,Buffer.from(change(members.get(n).toString())));
  let life=readFileSync(new URL('workspace-lifetime.mjs',source),'utf8');life=once(life,"endpoint=>import(endpoint.replace(/_records$/,'bridge/workspace-store.mjs'))","()=>import('./workspace-store.mjs')");members.set('workspace-lifetime.mjs',Buffer.from(life));
  for(const n of ['workspace-store.mjs','workspace-continuation.mjs','workspace-msgpack.mjs','workspace-value.mjs'])members.set(n,readFileSync(new URL(n,bridge)));
  const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([n,b])=>[n,hash(b)])),prefix='data/ui-'+hash(JSON.stringify(identity))+'/planner/';
  for(const[n,b]of members){assert(!original.has(prefix+n));assert(!b.includes(Buffer.from(prior)));files.set(prefix+n,b);}
  files.set(entry,Buffer.from(html.replaceAll(prior,prefix)));projections.push({entry,prior,prefix,files:members.size});
 }
 for(const[n,b]of original)if(!projections.some(p=>p.entry===n))assert(b.equals(files.get(n)),'Unrelated package member changed: '+n);
 return {files,projections};
}
export async function materializePlannerAccountRepair({root,base=plannerAccountBase}){
 assert.equal(base,plannerAccountBase);const {files,projections}=plannerAccountRepairFiles(await loadWorkspaceFiles(root,base));
 return {...await retainWorkspaceMaterial(files,{root}),base,projections};
}
