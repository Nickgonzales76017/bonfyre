import {readFile,lstat,mkdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {gunzipSync,gzipSync} from 'node:zlib';
import {manifestRows,readWorkspaceFile,readWorkspaceBody,retainWorkspaceMaterial,loadWorkspaceFiles} from './workspace_material.mjs';
export {manifestRows,readWorkspaceFile,verifyWorkspacePackage} from './workspace_material.mjs';
const repo=resolve(import.meta.dirname,'..'),sha=bytes=>createHash('sha256').update(bytes).digest('hex');
// Every local editor uses the same body store as reviewed public source releases.
export async function retainWorkspaceFiles(files,{root=join(homedir(),'.bonfyre/workspace-packages')}={}){
  return retainWorkspaceMaterial(files,{root});
}
// Bound the stream before buffering it; Content-Length alone is not a bound.
export async function workspaceSourceBytes(response,max){
  if(!response.ok){await response.body?.cancel();throw Error(`Source HTTP ${response.status}`);}
  if(Number(response.headers.get('content-length'))>max){await response.body?.cancel();throw Error('Source exceeds manifest bound.');}
  if(!response.body)return Buffer.alloc(0);
  const reader=response.body.getReader(),chunks=[];let total=0;
  try{for(;;){const {done,value}=await reader.read();if(done)break;
    if((total+=value.byteLength)>max)throw Error('Source exceeds manifest bound.');chunks.push(Buffer.from(value));}
    return Buffer.concat(chunks,total);
  }catch(e){try{await reader.cancel(e);}catch{}throw e;}finally{reader.releaseLock();}
}
export async function materializeWorkspacePackage({commit,root=join(homedir(),'.bonfyre/workspace-packages'),fetcher=fetch}={}){
  if(!/^[a-f0-9]{40}$/.test(commit??''))throw Error('Choose an exact reviewed public source commit.');
  root=resolve(root);await mkdir(root,{recursive:true});
  const base=`https://raw.githubusercontent.com/Nickgonzales76017/bonfyre/${commit}/site/spaces/`;
  const stop=new AbortController();
  const load=async(path,max)=>workspaceSourceBytes(await fetcher(base+path,{
    signal:AbortSignal.any([stop.signal,AbortSignal.timeout(30000)]),redirect:'error'}),max);
  const manifest=(await load('RELEASE.tsv',256*1024)).toString('utf8'),sourceId=sha(manifest),rows=manifestRows(manifest);
  let originals,present=false;
  try{await lstat(join(root,sourceId));present=true;}catch(error){if(error.code!=='ENOENT')throw error;}
  if(present)originals=await loadWorkspaceFiles(root,sourceId);
  if(!originals){
    originals=new Map();let at=0;
    const workers=await Promise.allSettled(Array.from({length:4},async()=>{try{for(;;){
      stop.signal.throwIfAborted();const row=rows[at++];if(!row)return;
      const bytes=await readWorkspaceBody(root,row.hash,row.bytes)??await load(row.path,row.bytes);
      if(bytes.length!==row.bytes||sha(bytes)!==row.hash)throw Error('Source digest mismatch: '+row.path);
      originals.set(row.path,bytes);
    }}catch(e){stop.abort(e);throw e;}}));
    const failure=workers.find(r=>r.status==='rejected');if(failure)throw failure.reason;
    await retainWorkspaceMaterial(originals,{root,manifest});
  }
  return projectWorkspaceMaterial({originals,sourceId,commit,root});
}

// Bind a locally built, immutable public release through the same native projection.
export async function materializeLocalWorkspacePackage(files,{root=join(homedir(),'.bonfyre/workspace-packages')}={}){
  const source=await retainWorkspaceMaterial(files,{root});
  const originals=await loadWorkspaceFiles(root,source.id);
  return projectWorkspaceMaterial({originals,sourceId:source.id,commit:null,root});
}
async function projectWorkspaceMaterial({originals,sourceId,commit,root}){
  const {continuationManifest,workspaceNavigation}=await import('./workspace_continuation.mjs');
  const oldRoot='/bonfyre/spaces/',scoped='/apps/__WORKSPACE__/',material='/apps/material/'+sourceId+'/',files=new Map(originals),replaced=new Map();
  for(const [path,body]of originals){if(!path.endsWith('.json.gz'))continue;
    const source=gunzipSync(body,{maxOutputLength:4*1024*1024}).toString('utf8');
    if(!source.includes(oldRoot))continue;
    // Compressed source records change only their public presentation address.
    const next=source.replaceAll(oldRoot+'assets/',material+'assets/');
    if(next.includes(oldRoot))throw Error('An unclassified route entered a source record: '+path);
    const projection=gzipSync(next,{level:9});files.set(path,projection);replaced.set(sha(body),sha(projection));
  }
  const sourceStore=await readFile(join(repo,'integrations/browser/extension/store.mjs'));
  const store=Buffer.from(sourceStore.toString('utf8').replace("import('/apps/workspace-store.mjs')","import('"+scoped+"bridge/workspace-store.mjs')"));
  for(const [path,body]of files){
    if(path.endsWith('/store.mjs')){files.set(path,store);continue;}
    if(!/\.(?:html|mjs|js|css)$/.test(path))continue;
    let text=body.toString('utf8').replaceAll(oldRoot+'assets/',material+'assets/').replaceAll(oldRoot,scoped);
    for(const [before,after]of replaced)text=text.replaceAll(before,after);
    text=text.replaceAll('Saved in this browser','Saved in this workspace').replaceAll('saved in this browser','saved in this workspace').replaceAll('on this browser.','in this workspace.').replaceAll('Your browser could not open draft storage. Enable site storage and reload before saving.','Your native workspace could not open draft storage. Check your session and reload before saving.');
    if(path.endsWith('.html')&&path!=='index.html'){
      if(!text.includes('</head>')||!text.includes('<body'))throw Error('Source document cannot be bound: '+path);
      text=text.replace('</head>','<script type="module" src="'+scoped+'bridge/workspace-adoption.mjs"></script><meta name="bonfyre-workspace-store" content="'+scoped+'_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>[data-native-workspace]{display:flex;gap:16px;flex-wrap:wrap;align-items:center;padding:10px 16px;background:#101b18;color:#f0f5ef;font:14px system-ui;min-height:44px;box-sizing:border-box}[data-native-workspace] a{color:inherit;padding:6px}</style></head>');
      text=text.replace(/(<body[^>]*>)/,'$1'+workspaceNavigation+'<nav data-native-workspace aria-label="Planning tools"><a href="'+scoped+'catalogue/">Catalogue</a><a href="'+scoped+'product-room/">Rooms</a></nav>');
    }
    files.set(path,Buffer.from(text));
  }
  for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-adoption.mjs','workspace-adoption-review.mjs','interaction-collection.mjs','record-comparison.mjs'])files.set('bridge/'+name,await readFile(join(repo,'integrations/browser',name)));
  files.set('CONTINUATIONS.tsv',continuationManifest([
    {view:'room',label:'Room plan',entry:'product-room/',prefix:'fixture-plan:',title:'plan.title',query:'project=plan.id&scene=@room'},
    {view:'catalogue',label:'Product draft',entry:'catalogue/',prefix:'draft:catalogue:',title:'product_id',query:'store=store&product=product_id&workspace=@products&panel=@edit'},
    ...(files.get('catalogue/index.html')?.toString().includes('data-ui="releases"')?[{view:'catalogue-release',label:'Catalogue release',entry:'catalogue/',prefix:'release:catalogue:',title:'title',query:'store=store&release=$key&workspace=@releases'}]:[])
  ],files));
  files.set('SOURCE.tsv',Buffer.from(`source_commit\t${commit??'local-unpublished'}\nsource_manifest\t${sourceId}\nstore_sha256\t${sha(store)}\ncompiler_sha256\t${sha(await readFile(import.meta.filename))}\nprojection\tTransport paths and storage binding; source product, option, fixture and plan identities retained.\n`));
  const result=await retainWorkspaceMaterial(files,{root});
  return {...result,sourceId,commit,files:files.size};
}
// Adopt the already published four-instrument implementation through this same
// release owner. This is a transport/continuation projection, not a second app.
export const connectedInstrumentViews = Object.freeze([
  ['traces','Process traces'],['research','Research editor'],
  ['schema','Schema migration'],['product','Product story map']
].map(([view,label])=>Object.freeze({view,label,entry:'instruments/',prefix:`draft:instruments:${view}:`,title:'title',query:`tool=@${view}&record=$key`})));
export async function materializeConnectedInstruments({commit,root=join(homedir(),'.bonfyre/workspace-packages'),fetcher=fetch}={}) {
  if(!/^[a-f0-9]{40}$/.test(commit??''))throw Error('Choose an exact reviewed public source commit.');
  const api='https://api.github.com/repos/Nickgonzales76017/bonfyre',stop=new AbortController();
  const load=async(url,max)=>workspaceSourceBytes(await fetcher(url,{redirect:'error',signal:AbortSignal.any([stop.signal,AbortSignal.timeout(30000)])}),max);
  const tree=async(id,recursive=false)=>{
    const value=JSON.parse((await load(`${api}/git/trees/${id}${recursive?'?recursive=1':''}`,256*1024)).toString('utf8'));
    if(value.truncated!==false||!Array.isArray(value.tree)||!/^[a-f0-9]{40}$/.test(value.sha??''))throw Error('Public source tree is incomplete.');
    return value;
  };
  const directory=(value,path)=>{
    const found=value.tree.filter(x=>x.path===path);
    if(found.length!==1||found[0].type!=='tree'||found[0].mode!=='040000'||!/^[a-f0-9]{40}$/.test(found[0].sha??''))throw Error('Missing exact source directory: '+path);
    return found[0].sha;
  };
  const top=await tree(commit),site=await tree(directory(top,'site')),subtree=await tree(directory(site,'instruments'),true);
  const rows=subtree.tree.filter(x=>x.type!=='tree');let total=0;
  if(!rows.length||rows.length>96)throw Error('The public instrument release is outside its file bound.');
  const seen=new Set();
  for(const row of rows){
    if(!['100644','100755'].includes(row.mode)||row.type!=='blob'||typeof row.path!=='string'||!row.path||row.path.length>240||!/^[-A-Za-z0-9_./]+$/.test(row.path)||row.path.split('/').some(p=>!p||p==='.'||p==='..')||seen.has(row.path)||!/^[a-f0-9]{40}$/.test(row.sha??'')||!Number.isSafeInteger(row.size)||row.size<0||row.size>4*1024*1024||(total+=row.size)>16*1024*1024)throw Error('Unsafe public instrument member.');
    seen.add(row.path);
  }
  for(const name of ['index.html','app.mjs','working-record.mjs','prepared-opportunity.mjs','runtime/store.mjs','UPSTREAM-LICENSE.txt','vendor/LICENSE-sql.js','vendor/sql-wasm.wasm'])if(!seen.has(name))throw Error('Incomplete connected instrument source: '+name);
  const originals=new Map();let at=0;
  const workers=await Promise.allSettled(Array.from({length:3},async()=>{try{for(;;){
    stop.signal.throwIfAborted();const row=rows[at++];if(!row)return;
    const body=await load(`https://raw.githubusercontent.com/Nickgonzales76017/bonfyre/${commit}/site/instruments/${row.path}`,row.size);
    const gitHash=createHash('sha1').update(`blob ${body.length}\0`).update(body).digest('hex');
    if(body.length!==row.size||gitHash!==row.sha)throw Error('Public instrument bytes changed: '+row.path);
    originals.set(row.path,body);
  }}catch(error){stop.abort(error);throw error;}}));
  const failed=workers.find(r=>r.status==='rejected');if(failed)throw failed.reason;
  return projectConnectedInstrumentMaterial(originals,{commit,sourceTree:subtree.sha,root});
}
export async function projectConnectedInstrumentMaterial(originals,{commit,sourceTree,root=join(homedir(),'.bonfyre/workspace-packages')}={}) {
  if(!/^[a-f0-9]{40}$/.test(commit??'')||!/^[a-f0-9]{40}$/.test(sourceTree??'')||!(originals instanceof Map))throw Error('Use the reviewed public source coordinates.');
  const source=new Map([...originals].map(([name,body])=>[name,Buffer.from(body)]));
  const {continuationManifest,workspaceNavigation,workspaceNavigationCSS}=await import('./workspace_continuation.mjs');
  const html=source.get('index.html')?.toString('utf8');
  if(!html?.includes('</head>')||!html.includes('<body>')||html.includes('bonfyre-workspace-store')||!source.has('runtime/store.mjs'))throw Error('The source is not the public connected-instrument editor.');
  for(const [name,body]of source)if(/\.(mjs|js)$/.test(name)&&name!=='runtime/store.mjs'&&/\b(?:localStorage|indexedDB|sessionStorage)\b/.test(body.toString('utf8')))throw Error('Unclassified browser persistence in '+name);
  const sourceRelease=await retainWorkspaceMaterial(source,{root}),files=new Map();
  const ownership=text=>text.replaceAll('in this browser','in this destination').replaceAll('Saves stay in this browser.','Saves stay in this destination.');
  for(const [name,body]of source)files.set('instruments/'+name,['app.mjs','working-record.mjs','prepared-opportunity.mjs'].includes(name)?Buffer.from(ownership(body.toString('utf8'))):body);
  const base='/apps/__WORKSPACE__/';
  const bound=html.replace('</head>',`<meta name="bonfyre-workspace-store" content="${base}_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}</style></head>`)
    .replace('<body>','<body>'+workspaceNavigation+'<nav class="source-intake-link"><a href="../sources/">Build a review from published sources ↗</a></nav>').replace('Local work · no account or external writes','Private work in this Aurekai destination').replace('<a href="../spaces/catalogue/">Catalogue ↗</a>','<a href="../_work">Saved work ↗</a>');
  files.set('instruments/index.html',Buffer.from(bound));
  const facade=await readFile(join(repo,'integrations/browser/extension/store.mjs'),'utf8'),anchor="import('/apps/workspace-store.mjs')";
  if(facade.split(anchor).length!==2)throw Error('The native store binding changed.');
  files.set('instruments/runtime/store.mjs',Buffer.from(facade.replace(anchor,"import('../../bridge/workspace-store.mjs')")));
  for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs'])files.set('bridge/'+name,await readFile(join(repo,'integrations/browser',name)));
  for(const name of ['interaction-collection.mjs','publication-picker.mjs','publication-research.mjs','publication-research-client.mjs','publication-research.css','source-publication.mjs','source-publication.css'])files.set('bridge/'+name,await readFile(join(repo,'integrations/browser',name)));
  files.set('sources/index.html',Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Source review · Aurekai</title><meta name="bonfyre-workspace-store" content="${base}_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><link rel="stylesheet" href="../bridge/publication-research.css"><style>${workspaceNavigationCSS}</style></head><body>${workspaceNavigation}<div data-source-review></div><script type="module" src="../bridge/publication-research-client.mjs"></script></body></html>`));
  files.set('CONTINUATIONS.tsv',continuationManifest(connectedInstrumentViews,files));
  files.set('SOURCE.tsv',Buffer.from(`source_commit\t${commit}\nsource_tree\t${sourceTree}\nsource_manifest\t${sourceRelease.id}\ncompiler_sha256\t${sha(await readFile(import.meta.filename))}\nprojection\tPublished instrument code with native store binding, hosting text, navigation and exact record continuations; native publication intake prepares records for the unchanged research owner.\n`));
  return {...await retainWorkspaceMaterial(files,{root}),sourceId:sourceRelease.id,commit,sourceTree,entry:'instruments/',family:'connected-instruments',views:connectedInstrumentViews.map(v=>v.view)};
}
if(import.meta.main){
  const args=process.argv.slice(2),connected=args[0]==='--instruments';if(connected)args.shift();
  const [commit,root]=args,result=await (connected?materializeConnectedInstruments:materializeWorkspacePackage)({commit,root});
  for(const [key,value]of Object.entries(result))console.log(key+'\t'+value);
}
