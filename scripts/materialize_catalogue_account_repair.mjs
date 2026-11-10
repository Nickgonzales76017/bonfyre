// Retain the exact editor and its native composition; only the catalogue gets a new lifetime.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
export const catalogueAccountBase='f03b0dd24fcf0ae8b2cc825fb7a80fc543e86acdb87a9ef3fcc88dd9e1e225b9';
const source=new URL('../components/BonfyreServiceSite/',import.meta.url),bridge=new URL('../integrations/browser/',import.meta.url),hash=b=>createHash('sha256').update(b).digest('hex');
function once(text,before,after){assert.equal(text.split(before).length,2,'Catalogue lifetime preimage changed: '+before);return text.replace(before,after);}
const pairs=[["import { readLocal, saveLocal, saveLocalBatch } from './store.mjs';", "import * as fallbackStore from './store.mjs';\\nimport {createWorkspaceLifetime,clearWorkspaceHost,openWorkspaceStore} from './workspace-lifetime.mjs';"], ["const root = document.querySelector", "const lifetime=createWorkspaceLifetime();\\nlet storage;\\nconst readLocal=(...args)=>storage.readLocal(...args),saveLocal=(...args)=>storage.saveLocal(...args),saveLocalBatch=(...args)=>storage.saveLocalBatch(...args);\\nconst root = document.querySelector"], ["const dirty = () => (", "const dirty = () => !lifetime.closed && (("], ["||releases.dirty();", "||releases.dirty());"], ["{config,fetchProduct,records:", "{config,fetchProduct,storage:{saveLocalBatch},records:"], ["{config,fetchProduct,inspectProduct:", "{config,fetchProduct,storage:{saveLocalBatch},inspectProduct:"], ["{config,fetchProduct,storage:{readLocal,saveLocalBatch}", "{config,fetchProduct,lifetime,storage:{readLocal,saveLocalBatch}"], ["function message(text, bad = false) {", "function message(text, bad = false) { if(lifetime.closed)return;"], ["function renderList() {", "function renderList() {\\n if(lifetime.closed)return;"], ["function renderWorkspace(){", "function renderWorkspace(){\\n if(lifetime.closed)return;"], ["function writeLocation(replace=false){", "function writeLocation(replace=false){\\n if(lifetime.closed)return;"], [" if(saving||inventory.busy()", " if(lifetime.closed||saving||inventory.busy()"], ["await getRecords();renderList();renderWorkspace();", "await getRecords();if(lifetime.closed)return;renderList();renderWorkspace();"], ["async function fetchProduct(x){", "async function fetchProduct(x){lifetime.assert();"], ["{cache:'no-cache'}", "{cache:'no-cache',signal:lifetime.signal}"], ["return p;}", "lifetime.assert();return p;}"], ["={}){if(saving)return;", "={}){if(lifetime.closed||saving)return;"], ["function renderEditor(){", "function renderEditor(){\\n if(lifetime.closed)return;"], ["function switchView(view,{route=true}={}){", "function switchView(view,{route=true}={}){\\n if(lifetime.closed)return;"], ["if(saving||blocked||changedElsewhere", "if(lifetime.closed||saving||blocked||changedElsewhere"], ["event.preventDefault();if(saving||$('submit').disabled)", "event.preventDefault();if(lifetime.closed||saving||$('submit').disabled)"], ["const body=reviewRequest(", "lifetime.assert();const body=reviewRequest("], ["signal:AbortSignal.timeout(20000)", "signal:AbortSignal.any([lifetime.signal,AbortSignal.timeout(20000)])"], ["   if(!response.ok||!result.ok)", "   lifetime.assert();if(!response.ok||!result.ok)"], ["}catch(error){status.textContent=", "}catch(error){if(lifetime.closed)return;status.textContent="], ["finally{saving=false;root.querySelectorAll", "finally{saving=false;if(lifetime.closed)return;root.querySelectorAll"], ["await getRecords();await selectProduct(", "await getRecords();if(lifetime.closed)return;await selectProduct("], ["addEventListener('popstate',async()=>{", "addEventListener('popstate',async()=>{\\n if(lifetime.closed)return;"], ["await openWorkspace(params.get('store')", "try{storage=await openWorkspaceStore(lifetime,{fallback:fallbackStore});lifetime.assert();await openWorkspace(params.get('store')"]];
export function catalogueAccountClient(text){
 for(const[a,b]of pairs)if(text.includes(a))text=text.replaceAll(a,b.replaceAll('\\n','\n'));
 assert(text.trimEnd().endsWith("releaseKey:params.get('release')});"));
 text=text.trimEnd()+"}catch(error){if(!lifetime.closed)message(error.message,true);}\n";
 return once(text,'const params=new URL(location.href).searchParams;',"lifetime.onClose(()=>{\n ++loadSerial;inventory.close();product=null;original=null;edit=null;lastSaved='';revision=0;\n records.clear();cache.clear();receiptHistory.clear();versions.clear();unkeptReceipts.clear();\n clearWorkspaceHost(root);\n});\n"+'const params=new URL(location.href).searchParams;');
}
export function catalogueAccountRepairFiles(original){
 const files=new Map(original),entry='catalogue/index.html',prior='data/ui-2de7e9318af8116d9080b20fe6e9665cde2ab52f92278589f4c781d2e21e68d7/catalogue/';
 let html=files.get(entry).toString();assert(html.includes(prior));const members=new Map([...original].filter(([n])=>n.startsWith(prior)).map(([n,b])=>[n.slice(prior.length),b]));
 assert.equal(members.size,39);
 assert.equal(hash(members.get('catalogue-release-view.mjs')),'67e7062b795b01e94147a6e747cabcc649bd3de23af47da85964af9089601dc6');
 let client=catalogueAccountClient(members.get('catalogue-client.mjs').toString());
 client=once(client,'{config,fetchProduct,storage:{saveLocalBatch}', '{config,fetchProduct,lifetime,storage:{saveLocalBatch}');
 client=once(client,'versions.set(key,await reviewVersion(config.review.form,r.value));','const version=await reviewVersion(config.review.form,r.value);lifetime.assert();versions.set(key,version);');
 client=once(client,'if(config.review)versions.set(record.key,await reviewVersion(config.review.form,draft));','if(config.review){const version=await reviewVersion(config.review.form,draft);lifetime.assert();versions.set(record.key,version);}lifetime.assert();');
 client=once(client,'{routes:config.roomRoutes??[],canLeave:', '{routes:config.roomRoutes??[],lifetime,storage:{readLocal,saveLocal},canLeave:');
 client=once(client,'++loadSerial;inventory.close();','++loadSerial;inventory.close();roomDialog.close();');
 members.set('catalogue-client.mjs',Buffer.from(client));
 let grid=once(members.get('catalogue-grid.mjs').toString(),'{config,fetchProduct,records,openProduct,saved,inspectProduct}','{config,fetchProduct,records,openProduct,saved,inspectProduct,storage={saveLocalBatch}}');
 grid=once(grid,'persist:saveLocalBatch','persist:storage.saveLocalBatch');members.set('catalogue-grid.mjs',Buffer.from(grid));
 let scene=once(members.get('catalogue-product-scene.mjs').toString(),'{routes=[],canLeave=()=>true,onError=()=>{}}','{routes=[],canLeave=()=>true,onError=()=>{},lifetime,storage={readLocal,saveLocal}}');
 scene=once(scene,' let dialog=null',' const {readLocal,saveLocal}=storage;\n let dialog=null');
 scene=once(scene,'  close();product=source;','  if(lifetime?.closed)return;close();product=source;');
 scene=once(scene,'const say=(text)=>{status.textContent=text;}','const say=(text)=>{if(!lifetime?.closed)status.textContent=text;}');
 scene=once(scene,'if(busy||!selected||!canLeave())','if(lifetime?.closed||busy||!selected||!canLeave())');
 scene=once(scene,'dialog?.close();dialog?.remove();','if(dialog){for(const n of dialog.querySelectorAll("select"))n.replaceChildren();for(const n of dialog.querySelectorAll("input,textarea"))n.value="";for(const n of dialog.querySelectorAll("button,input,textarea,select"))n.disabled=true;}dialog?.close();dialog?.remove();');
 scene=once(scene,'finally{busy=false;place.disabled=false;}','finally{busy=false;if(!lifetime?.closed)place.disabled=false;}');
 scene=once(scene,'dialog=null;selected=null;}','dialog=null;selected=null;product=null;projectRows=[];}');
 members.set('catalogue-product-scene.mjs',Buffer.from(scene));
 for(const n of ['catalogue-release-view.mjs','workspace-lifetime.mjs'])members.set(n,readFileSync(new URL(n,source)));
 let life=members.get('workspace-lifetime.mjs').toString();
 life=once(life,"endpoint=>import(endpoint.replace(/_records$/,'bridge/workspace-store.mjs'))","()=>import('./workspace-store.mjs')");
 members.set('workspace-lifetime.mjs',Buffer.from(life));
 for(const n of ['workspace-store.mjs','workspace-continuation.mjs','workspace-msgpack.mjs','workspace-value.mjs']){assert(!members.has(n));members.set(n,readFileSync(new URL(n,bridge)));}
 const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([n,b])=>[n,hash(b)])),prefix='data/ui-'+hash(JSON.stringify(identity))+'/catalogue/';
 for(const[n,b]of members){assert(!original.has(prefix+n));assert(!b.includes(Buffer.from(prior)));files.set(prefix+n,b);}
 files.set(entry,Buffer.from(html.replaceAll(prior,prefix)));
 for(const[n,b]of original)if(n!==entry)assert(b.equals(files.get(n)),'Unrelated package member changed: '+n);
 return {files,projection:{prior,prefix,files:members.size}};
}
export async function materializeCatalogueAccountRepair({root,base=catalogueAccountBase}){
 assert.equal(base,catalogueAccountBase);const {files,projection}=catalogueAccountRepairFiles(await loadWorkspaceFiles(root,base));
 return {...await retainWorkspaceMaterial(files,{root}),base,projection};
}
