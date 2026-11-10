import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {WEB_FILES} from './web-package.mjs';
import {retainWorkspaceFiles} from '../../scripts/materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from '../../scripts/workspace_continuation.mjs';
const root=resolve(import.meta.dir,'../..'),sha=body=>createHash('sha256').update(body).digest('hex');
export async function nativeWorkbenchPackage({packageRoot}={}){
 const files=new Map(),sources=[];
 for(const name of WEB_FILES){const path=join(root,'integrations/browser/extension',name),raw=readFileSync(path);sources.push(name+'\t'+sha(raw));let body=raw;
  if(name.endsWith('.html')){const text=raw.toString('utf8');if(!text.includes('</head>')||!/<body\b/.test(text))throw Error('Workspace document has no binding point.');body=Buffer.from(text.replace('</head>','<meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>'+workspaceNavigationCSS+'</style></head>').replace(/(<body[^>]*>)/,'$1'+workspaceNavigation));}
  if(name==='store.mjs'){const text=raw.toString('utf8'),anchor="import('/apps/workspace-store.mjs')";if(text.split(anchor).length!==2)throw Error('Native store binding changed.');body=Buffer.from(text.replace(anchor,"import('../bridge/workspace-store.mjs')"));}
  files.set('programs/'+name,body);
 }
 files.set('programs/index.html',files.get('programs/workbench.html'));
 for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs']){const body=readFileSync(join(root,'integrations/browser',name));sources.push('bridge/'+name+'\t'+sha(body));files.set('bridge/'+name,body);}
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'program',label:'Recurring program',entry:'programs/',prefix:'draft:',title:'name',query:'draft=$key'}],files));
 files.set('SOURCE.tsv',Buffer.from('source\tsha256\n'+sources.join('\n')+'\n'));
 for(const row of sources){const[name,hash]=row.split('\t'),path=name.startsWith('bridge/')?join(root,'integrations/browser',name.slice(7)):join(root,'integrations/browser/extension',name);if(sha(readFileSync(path))!==hash)throw Error('Workspace source changed during packaging.');}
 return {...await retainWorkspaceFiles(files,{...(packageRoot?{root:packageRoot}:{})}),entry:'programs/',records:['draft:','observation:'],authority:'existing Pack installation, Auth grants and CMS workspace records'};
}
if(import.meta.main){const [packageRoot]=process.argv.slice(2),result=await nativeWorkbenchPackage({packageRoot});for(const [key,value]of Object.entries(result))console.log(key+'\t'+(Array.isArray(value)?value.join(','):value));}
