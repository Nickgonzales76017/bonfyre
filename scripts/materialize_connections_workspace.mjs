import {readFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {retainWorkspaceFiles} from './materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from './workspace_continuation.mjs';
export const connectionPack='experience.connection-studio.workspace';
export const connectionFamily='connections';
export const connectionEntry='connections/';
const repo=resolve(import.meta.dir,'..');
export const connectionModules=Object.freeze(['connection-studio.mjs','connection-source.mjs','connection-studio.css','connection-lab.mjs','workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','interaction-collection.mjs','publication-picker.mjs','publication-research.mjs','source-desk.mjs','source-desk.css','source-publication.mjs','source-publication.css','workspace-destinations.mjs','provider-contract.mjs','provider-auth-capability.mjs','authentication-capability-surface.mjs','extension/foreign-sign-in.mjs','extension/foreign-sign-in.css']);
export async function connectionWorkspaceMaterial(options={}){
 const files=new Map();for(const name of connectionModules)files.set('bridge/'+name,await readFile(join(repo,'integrations/browser',name)));
 files.set('connections/index.html',Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Connection Studio · Aurekai</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__CONTEXT__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><link rel="stylesheet" href="../bridge/connection-studio.css"><link rel="stylesheet" href="../bridge/source-desk.css"><link rel="stylesheet" href="../bridge/source-publication.css"><style>${workspaceNavigationCSS}</style></head><body>${workspaceNavigation}<main data-connection-studio class="cs-workspace"></main><script type="module" src="../bridge/connection-studio.mjs"></script></body></html>`));
 files.set('CONTINUATIONS.tsv',continuationManifest([
  {view:'connections',label:'Provider working blocks',entry:connectionEntry,prefix:'connections:blocks',title:'@Your connections',query:'view=@board'},
  {view:'source-set',label:'Retained source selections',entry:connectionEntry,prefix:'connections:source-set:',title:'title',query:'view=@board&record=$key'}
 ],files));
 files.set('SOURCE.tsv',Buffer.from('owner\tBonfyreEnrollment and existing native workspace record owner\nprojection\tAccount-scoped connection preparation and retained source selection; no external signup or consent.\n'));
 for(const[path,body]of files)if(path.endsWith('.mjs')){const text=body.toString();for(const m of text.matchAll(/(?:from\s+|import\s*)['"](\.[^'"]+)['"]/g)){const target=resolve('/material',path,'..',m[1]).slice('/material/'.length);if(!files.has(target))throw Error('Missing package import '+path+' -> '+target);}}
 return{...await retainWorkspaceFiles(files,options),pack:connectionPack,family:connectionFamily,entry:connectionEntry,files:files.size};
}
if(import.meta.main){const result=await connectionWorkspaceMaterial();console.log('CONNECTION_WORKSPACE',result);}

// The retained package is private until its existing Pack lifecycle is invoked.
