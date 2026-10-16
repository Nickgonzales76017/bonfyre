import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve,join} from 'node:path';
const root=resolve(fileURLToPath(new URL('..',import.meta.url)));
const read=name=>readFile(join(root,name),'utf8');
function replace(text,before,after){if(!text.includes(before))throw Error('Workspace donor changed at: '+before.slice(0,90));return text.replace(before,after);}
export async function assembleWorkspace(files,kind,pack){
  const imports=text=>text.replaceAll('./renderers/',`../../../${pack.path}/`);
  files.set('workspace-dependencies.mjs',Buffer.from(`let pending;
export function loadWorkspaceDocking(){return pending??=(async()=>{
const href='/bonfyre/spaces/assets/workspace-docking/dockview-8.5.1.css';
if(!document.querySelector('link[data-workspace-docking]'))await new Promise((resolve,reject)=>{const link=document.createElement('link');link.dataset.workspaceDocking='true';link.rel='stylesheet';link.href=href;link.onload=resolve;link.onerror=()=>{link.remove();reject(Error('Workspace layout styles could not load.'));};document.head.append(link);});
return import('/bonfyre/spaces/assets/workspace-docking/dockview-8.5.1.mjs');
})().catch(error=>{pending=null;throw error;});}
`));
  if(kind==='planner'){
    files.set('planner-workspace.mjs',Buffer.from(imports(await read('integrations/browser/adapters/planner-workspace.mjs'))));
    let client=files.get('planner-client.mjs').toString();
    client="import {composePlannerWorkspace} from './planner-workspace.mjs';\n"+client;
    client=replace(client,'let projectWorkspace, spatial, productScene, lastValid;','let projectWorkspace, spatial, productScene, lastValid, composition;');
    client=replace(client,"  spatial?.refresh();","  spatial?.refresh();composition?.refresh();");
    client=replace(client,'function select(key) {selected=key;render();positionEditor();}','function select(key) {selected=key;render();positionEditor();composition?.selected(key);}');
    client=replace(client,"isBusy:()=>busy,fail:error});","isBusy:()=>busy,fail:error,renderView:view=>composition?.renderView(view)});");
    client=replace(client,"  root.dataset.ready='true';",`  try{composition=await composePlannerWorkspace({root,catalogue,read:()=>({plan,selected,dirty:!unchanged()}),save,undo:()=>editHistory('undo'),redo:()=>editHistory('redo'),select,review:()=>ui('review').click(),fail:error});composition.renderView(root.dataset.workspaceView||'layout');}catch(e){message('The standard planner remains available. '+e.message);}
  root.dataset.ready='true';`);
    files.set('planner-client.mjs',Buffer.from(client));
    let projects=files.get('planner-project-client.mjs').toString();
    projects=replace(projects,'canLeave, isBusy, fail})','canLeave, isBusy, fail, renderView})');
    projects=replace(projects,"    for (const id of ['layout', 'fixtures', 'projects', 'compare']) ui(id).hidden = id !== view;","    if(!renderView?.(view))for (const id of ['layout', 'fixtures', 'projects', 'compare']) ui(id).hidden = id !== view;");
    files.set('planner-project-client.mjs',Buffer.from(projects));
  }else{
    let adapter=files.get('catalogue-grid.mjs').toString();
    adapter="import {composeCollectionWorkspace} from '../../../"+pack.path+"/collection-composition.mjs';\nimport {loadWorkspaceDocking} from './workspace-dependencies.mjs';\n"+adapter;
    adapter=replace(adapter,'  const presentation={',`  const presentation={
    compose:ports=>composeCollectionWorkspace({...ports,loadDocking:loadWorkspaceDocking,sourceDigests:()=>[...ports.state().values()].map(s=>s.product.source_sha256),changes:()=>inventoryChanges(ports.state()).flatMap(inventoryDelta)}),`);
    files.set('catalogue-grid.mjs',Buffer.from(adapter));
  }
}
