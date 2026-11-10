// Add the reviewed release view to the retained native catalogue without rebuilding its editor.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
import {parseContinuationManifest} from '../integrations/browser/workspace-continuation.mjs';
import {continuationManifest} from './workspace_continuation.mjs';
export const catalogueReleaseBase='97860a1b32d8921c82ff137551962d33bd69d8d307463af4d2792adb63aff813';
const source=new URL('../components/BonfyreServiceSite/',import.meta.url),hash=body=>createHash('sha256').update(body).digest('hex');
function once(text,before,after){assert.equal(text.split(before).length,2,'Retained catalogue preimage changed.');return text.replace(before,after);}
const expected={
 'catalogue-client.mjs':'2d0b057572f6d3d5cda74e50c0edb0c319d337e4cd8ffb35f67580066f8f3ef0',
 'catalogue-desk.mjs':'f8f6e4dd68c27b5d12475da007b08b23f9423fb0681a1c8e3414c5258d6cd372',
 'catalogue-grid.mjs':'655de3c7234b1881234412d48a4614e3758908164bf88446e7ed2edb6ed9b2ce',
 'catalogue-model.mjs':'5ab19df98539af14da70dc8b12dccc315b17f3067d1289ee4a502fe1f123c48b',
 'planner-model.mjs':'b543a6fab8cbf665920b4291d008c81a345907d49da249ec7d0656da543b8fd0'
};
export function catalogueReleaseRepairFiles(original){
 const files=new Map(original),entry='catalogue/index.html';
 let html=files.get(entry)?.toString();assert.equal(hash(html),'ce88bcb5dad6935954c38af9fa32aa4b26a0062594a234fe0e473999d91ee88a','Choose the reviewed catalogue entry.');
 const prior='data/ui-43bf34f5c3a5f381a7d617e1b5d022f8836a3356a10556cd0ed7176d3d37b414/catalogue/';
 assert(html.includes(prior));const members=new Map([...original].filter(([n])=>n.startsWith(prior)).map(([n,b])=>[n.slice(prior.length),b]));
 for(const[n,h]of Object.entries(expected))assert.equal(hash(members.get(n)),h,'Unexpected retained module: '+n);
 assert.equal(hash(readFileSync(new URL('catalogue-model.mjs',source))),expected['catalogue-model.mjs'],'Catalogue edit semantics changed.');
 let client=members.get('catalogue-client.mjs').toString();
 client="import {catalogueReleases} from './catalogue-release-view.mjs';\n"+client;
 const liveClient=readFileSync(new URL('catalogue-client.mjs',source),'utf8'),init=liveClient.split('\n').find(line=>line.startsWith('const releases=catalogueReleases('));assert(init);
 for(const[a,b]of [
  ['{ readLocal, saveLocal }','{ readLocal, saveLocal, saveLocalBatch }'],
  ['||inventory.dirty();','||inventory.dirty()||releases.dirty();'],
  ['const readableDate =',init+'\nconst readableDate ='],
  [" $('product-layout').hidden=workspaceView!=='products';"," $('releases').hidden=workspaceView!=='releases';\n $('product-layout').hidden=workspaceView!=='products';"],
  [" if(url.href!==location.href)"," if(workspaceView!=='releases')url.searchParams.delete('release');\n if(url.href!==location.href)"],
  ["{replace=false,productId=null,panel='edit'}={}","{replace=false,productId=null,panel='edit',releaseKey=null}={}"],
  ['if(saving||inventory.busy()||dirty()','if(saving||inventory.busy()||releases.busy()||dirty()'],
  [' inventory.close();',' inventory.close();releases.close();'],
  ["if(workspaceView==='inventory')await inventory.activate(store);}","if(workspaceView==='inventory')await inventory.activate(store);if(workspaceView==='releases')await releases.activate(store,releaseKey);}"],
  ["panel:requested.searchParams.get('panel')","panel:requested.searchParams.get('panel'),releaseKey:requested.searchParams.get('release')"],
  ["panel:params.get('panel')","panel:params.get('panel'),releaseKey:params.get('release')"]
 ])client=once(client,a,b);
 members.set('catalogue-client.mjs',Buffer.from(client));
 members.set('catalogue-desk.mjs',Buffer.from(once(members.get('catalogue-desk.mjs').toString(),"{id:'sent',","{id:'releases',label:'Releases',available:()=>true},\n  {id:'sent',")));
 let grid=once(members.get('catalogue-grid.mjs').toString(),'function loadGrid(){','export function loadGrid(){');
 const theme=readFileSync(new URL('catalogue-grid.mjs',source),'utf8').split('\n').find(line=>line.startsWith('export function catalogueGridTheme('));assert(theme);
 members.set('catalogue-grid.mjs',Buffer.from(grid+'\n'+theme+'\n'));
 const planner=readFileSync(new URL('planner-model.mjs',source),'utf8'),start=planner.indexOf('export function validatePlanRecordKey('),end=planner.indexOf('export function validatePlan(',start);assert(start>=0&&end>start);
 members.set('planner-model.mjs',Buffer.from(once(members.get('planner-model.mjs').toString(),'export function validatePlan(',planner.slice(start,end)+'export function validatePlan(')));
 for(const name of ['catalogue-release.mjs','catalogue-release-view.mjs']){assert(!members.has(name));members.set(name,readFileSync(new URL(name,source)));}
 const identity=Object.fromEntries([...members].sort(([a],[b])=>a.localeCompare(b,'en')).map(([n,b])=>[n,hash(b)]));
 const prefix='data/ui-'+hash(JSON.stringify(identity))+'/catalogue/';
 for(const[n,b]of members){assert(!original.has(prefix+n));assert(!b.includes(Buffer.from(prior)),'Unexpected absolute module self reference.');files.set(prefix+n,b);}
 const match=html.match(/(<script[^>]*data-catalogue-config[^>]*>)([\s\S]*?)(<\/script>)/);assert(match);
 const config=JSON.parse(match[2]);assert(Array.isArray(config.roomRoutes)&&config.roomRoutes.length);
 config.fixture_catalogues=[...new Map(config.roomRoutes.map(route=>[route.catalogue.digest,route.catalogue])).values()];
 assert(config.fixture_catalogues.length<=20);
 const encoded=JSON.stringify(config).replaceAll('<','\\u003c').replaceAll('>','\\u003e').replaceAll('&','\\u0026');
 html=once(html,match[0],match[1]+encoded+match[3]);
 const storeSelect=html.match(/(<select[^>]*data-ui="store"[^>]*>)[\s\S]*?(<\/select>)/);assert(storeSelect);
 const escape=text=>String(text).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
 html=once(html,storeSelect[0],storeSelect[1]+config.stores.map(store=>'<option value="'+escape(store.id)+'">'+escape(store.name)+'</option>').join('')+storeSelect[2]);
 html=once(html,'<button type="button" data-workspace-view="sent">','<button type="button" data-workspace-view="releases">Releases</button><button type="button" data-workspace-view="sent">');
 html=once(html,'<section data-ui="desk"','<section data-ui="releases" class="cw-releases" hidden></section><section data-ui="desk"');
 const css=readFileSync(new URL('catalogue.css',source),'utf8').split('\n').filter(line=>line.includes('.cw-releases')).join('\n');assert(css.includes('.cr-grid')&&css.includes('@media'));
 html=once(html,'</head>','<style data-catalogue-releases>'+css+'</style></head>').replaceAll(prior,prefix);files.set(entry,Buffer.from(html));
 const views=parseContinuationManifest(original.get('CONTINUATIONS.tsv').toString());assert(!views.some(v=>v.view==='catalogue-release'));
 files.set('CONTINUATIONS.tsv',continuationManifest([...views,{view:'catalogue-release',label:'Catalogue release',entry:'catalogue/',prefix:'release:catalogue:',title:'title',query:'store=store&release=$key&workspace=@releases'}],files));
 for(const[n,b]of original)if(![entry,'CONTINUATIONS.tsv'].includes(n))assert(b.equals(files.get(n)),'Unrelated retained material changed.');
 return {files,projection:{prior,prefix,files:members.size,fixtureCatalogues:config.fixture_catalogues.map(c=>c.digest)}};
}
export async function materializeCatalogueReleaseRepair({root,base=catalogueReleaseBase}){
 assert.equal(base,catalogueReleaseBase);const {files,projection}=catalogueReleaseRepairFiles(await loadWorkspaceFiles(root,base));
 return {...await retainWorkspaceMaterial(files,{root}),base,projection};
}
