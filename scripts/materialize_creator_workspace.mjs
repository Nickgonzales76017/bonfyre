import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {creatorFiles} from '../components/BonfyreServiceSite/creator-material.mjs';
import {render} from '../components/BonfyreServiceSite/sections/creator.mjs';
import {escapeHTML} from '../components/BonfyreServiceSite/workspace-navigation.mjs';
import {retainWorkspaceFiles} from './materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation} from './workspace_continuation.mjs';
const repo=resolve(import.meta.dirname,'..'),base='/apps/__WORKSPACE__/',hash=b=>createHash('sha256').update(b).digest('hex');
// Compile the existing arrangement editor, native/Wasm kernel and retained
// generated sample. Only hosting, storage binding and ownership copy change.
export async function materializeCreatorWorkspace({root,directory=join(repo,'packs/aurekai-work-samples/audio')}={}){
 const packed=await creatorFiles(directory),files=new Map(),sources=[];
 const ownership=text=>text.replaceAll('stay in this browser','stay in this workspace').replaceAll('stays in this browser','stays in this workspace').replaceAll('stay on this device','stay in this workspace').replaceAll('saved in this browser','saved in this workspace').replaceAll('saved on this browser','saved in this workspace').replaceAll('Recordings and edits stay in this browser.','Recordings and edits stay in this workspace.').replaceAll('The browser could not save this session.','The native workspace could not save this session.');
 const body=render({id:'creator',title:'Arrange your recordings',data:'/data/creator/',asset_prefix:packed.asset_prefix,sample:packed.sample},{},{esc:escapeHTML});
 let html='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Audio studio</title><meta name="bonfyre-workspace-store" content="'+base+'_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><meta name="bonfyre-workspace-family" content="creator-audio"><script type="module" src="'+base+'bridge/workspace-adoption.mjs"></script><style>html,body{margin:0;background:#101612;color:#edf3e9;font-family:system-ui}[data-native-workspace]{display:flex;flex-wrap:wrap;gap:14px;align-items:center;padding:12px 18px;font-size:14px}[data-native-workspace] a{color:inherit;min-height:44px;display:inline-flex;align-items:center}</style></head><body>'+workspaceNavigation+body+'</body></html>';
 html=ownership(html).replaceAll('href="/#','href="https://aurekai-work-samples.pages.dev/#').replaceAll('/data/creator/',base+'data/creator/');
 files.set('audio/index.html',Buffer.from(html));
 for(const [name,body]of Object.entries(packed.files)){
  const path='data/creator/'+name;sources.push([path,hash(body),body.length]);
  let next=body;
  if(name.endsWith('store.mjs'))next=Buffer.from(readFileSync(join(repo,'integrations/browser/extension/store.mjs'),'utf8').replace("import('/apps/workspace-store.mjs')","import('"+base+"bridge/workspace-store.mjs')"));
  else if(/\.(mjs|js|css)$/.test(name))next=Buffer.from(ownership(body.toString('utf8')));
  files.set(path,next);
 }
 for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-adoption.mjs','workspace-adoption-review.mjs','interaction-collection.mjs','record-comparison.mjs'])files.set('bridge/'+name,readFileSync(join(repo,'integrations/browser',name)));
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'audio',label:'Recording session',entry:'audio/',prefix:'=draft:creator:'+packed.sample.id+':imports',title:'title',query:'workspace=@imports&view=@timeline'}],files));
 files.set('SOURCE.tsv',Buffer.from('owner\tcomponents/BonfyreServiceSite/creator-material.mjs\nsample_id\t'+packed.sample.id+'\nnative_mix_sha256\t'+packed.sample.native_sha256+'\ncompiler_sha256\t'+hash(readFileSync(import.meta.filename))+'\npath\tsha256\tbytes\n'+sources.map(row=>row.join('\t')).join('\n')+'\n'));
 return {...await retainWorkspaceFiles(files,{root}),entry:'audio/',family:'creator-audio',sample:packed.sample.id};
}
if(import.meta.main){const result=await materializeCreatorWorkspace({root:process.argv[2]});for(const [key,value]of Object.entries(result))console.log(key+'\t'+value);}
