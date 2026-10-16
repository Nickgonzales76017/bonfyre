import {readFile,writeFile,readdir,mkdir,access} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {join,resolve,dirname,relative} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
const repo=resolve(fileURLToPath(new URL('..',import.meta.url)));
const hash=value=>createHash('sha256').update(value).digest('hex');
const names=['edit-history.mjs','review-sheet.mjs','record-grid.mjs','collection-workbench.mjs','scene-workbench.mjs'];
const exists=async path=>{try{await access(path);return true;}catch{return false;}};
async function put(path,bytes){await mkdir(dirname(path),{recursive:true});if(await exists(path)){const before=await readFile(path);if(before.equals(Buffer.from(bytes)))return;}await writeFile(path,bytes);}
async function walk(dir){const files=[];for(const entry of await readdir(dir,{withFileTypes:true})){const path=join(dir,entry.name);if(entry.isSymbolicLink())throw Error('Symlink in release material');if(entry.isDirectory())files.push(...await walk(path));else files.push(path);}return files;}
export async function componentPackage(){
 const modules=await Promise.all(names.map(async name=>[name,await readFile(join(repo,'cmd/BonfyreSurfaceScene/runtime/renderers',name))]));
 const manifest=modules.map(([name,bytes])=>`${name}\t${hash(bytes)}\t${bytes.length}\n`).join('');
 const digest=hash(manifest),path='shared/interaction-'+digest.slice(0,20);
 return {digest,path,manifest,modules};
}
export async function materializeComponents(base,pack){for(const[name,bytes]of pack.modules)await put(join(base,pack.path,name),bytes);await put(join(base,pack.path,'SOURCE.tsv'),'path\tsha256\tbytes\n'+pack.manifest);}
export function bindComponentImports(text,file,base,pack){return text.replace(/(['"])\.\/renderers\//g,(_,q)=>q+relative(dirname(file),join(base,pack.path)).replaceAll('\\','/')+'/');}
const prefix='/bonfyre/spaces/';
const entryPath=entry=>{if(!/^\/bonfyre\/spaces\/data\/ui-[a-f0-9]{64}\/(catalogue|planner)\/[a-z-]+\.mjs$/.test(entry))throw Error('Invalid release entry coordinate');return entry.slice(prefix.length);};
export async function replaceSpacesComponents(directory){
 const spaces=resolve(directory),pack=await componentPackage(),report=[],prior=new Map(),nextPins=[];
 const pinsPath=join(spaces,'COMPONENT-DONORS.tsv');
 if(await exists(pinsPath)){
  const text=await readFile(pinsPath,'utf8');if(text.length>4096||!text.startsWith('page\tsource_entry\trendered_entry\n'))throw Error('Invalid component donor coordinates');
  for(const line of text.trimEnd().split('\n').slice(1)){const fields=line.split('\t');if(fields.length!==3||prior.has(fields[0])||!['catalogue','product-room','red3-planner'].includes(fields[0]))throw Error('Ambiguous component donor');entryPath(fields[1]);entryPath(fields[2]);prior.set(fields[0],fields);}
 }
 await materializeComponents(spaces,pack);
 const catalogueAdapter=await readFile(join(repo,'components/BonfyreServiceSite/catalogue-grid.mjs'),'utf8');
 const sceneAdapter=await readFile(join(repo,'integrations/browser/adapters/product-scene-tools.mjs'),'utf8');
 const rewritten=new Map();
 for(const[page,kind,entryName]of [['catalogue','catalogue','catalogue-client.mjs'],['product-room','planner','planner-client.mjs'],['red3-planner','planner','planner-client.mjs']]){
  const htmlPath=join(spaces,page,'index.html'),html=await readFile(htmlPath,'utf8');
  const shown=html.match(new RegExp('<script[^>]*src="([^"]*'+entryName.replace('.','\\.')+')"'))?.[1];entryPath(shown??'');
  const previous=prior.get(page),entry=previous&&shown===previous[2]?previous[1]:shown;
  if(rewritten.has(entry)){const next=rewritten.get(entry);await put(htmlPath,html.replace(shown,next));nextPins.push([page,entry,next]);continue;}
  const base=dirname(join(spaces,entryPath(entry))),files=new Map();
  for(const file of await walk(base))files.set(relative(base,file),await readFile(file));
  const prospective=join(spaces,'data/ui-component',kind);
  const bound=(text,name)=>bindComponentImports(text,join(prospective,name),spaces,pack);
  if(kind==='catalogue'){
   const before=files.get('catalogue-grid.mjs')?.toString();
   if(!before?.includes('inventoryWorkspace')||!before.includes('inspectProduct'))throw Error('Catalogue donor has no source-bound model continuation');
   const after=bound(catalogueAdapter,'catalogue-grid.mjs');files.set('catalogue-grid.mjs',Buffer.from(after));
   report.push(['catalogue','catalogue-grid.mjs',hash(before),hash(after),Buffer.byteLength(before),Buffer.byteLength(after)]);
  }else{
   const before=files.get('planner-client.mjs');
   if(hash(before)!=='9085817ad65f32b945470fd337d36951ae1d563c00c0177dcb75210767bec125')throw Error('Planner donor changed; reconcile before substituting history');
   // Apply the reviewed patch in a bounded temporary source directory, never to
   // an existing content-addressed package. Domain source references stay intact.
   const {mkdtemp,rm}=await import('node:fs/promises'),{tmpdir}=await import('node:os');
   const temp=await mkdtemp(join(tmpdir(),'component-plan-'));
   let text;try{
    await writeFile(join(temp,'planner-client.mjs'),before);
    const patch=await readFile(join(repo,'integrations/browser/adapters/planner/use-shared-history.patch'));
    execFileSync('git',['apply','--check','-'],{cwd:temp,input:patch});
    execFileSync('git',['apply','-'],{cwd:temp,input:patch});text=await readFile(join(temp,'planner-client.mjs'),'utf8');
   }finally{await rm(temp,{recursive:true,force:true});}
   const after=bound(text,'planner-client.mjs');files.set('planner-client.mjs',Buffer.from(after));
   report.push(['planner','planner-client.mjs',hash(before),hash(after),before.length,Buffer.byteLength(after)]);
  }
  const scene=files.get('product-scene.mjs').toString(),old=scene.match(/import \{createStudioStage,studioCSS\} from '([^']+)'/);
  if(!old||!/^\/bonfyre\/spaces\/assets\/product-scene\/helpers-[a-f0-9]{64}\/product-scene-tools.mjs$/.test(old[1]))throw Error('The reviewed scene helper binding changed');
  const original=await readFile(join(spaces,old[1].slice(prefix.length)),'utf8');
  if(hash(original)!=='ae6c5ac24536afef0a9b054a9ffc782cc647abaa578223f2ed35c1ee37025550')throw Error('Scene donor changed; reconcile before replacing');
  const styleAt=original.indexOf('export const studioCSS=');if(styleAt<0)throw Error('The donor style is missing');
  files.set('product-scene-style.mjs',Buffer.from(original.slice(styleAt)));
  const wrapper=bound(sceneAdapter,'product-scene-tools.mjs');files.set('product-scene-tools.mjs',Buffer.from(wrapper));
  files.set('product-scene.mjs',Buffer.from(scene.replace(old[0],"import {createStudioStage,studioCSS} from './product-scene-tools.mjs'")));
  report.push([kind+'-scene','product-scene-tools.mjs',hash(original),hash(wrapper),Buffer.byteLength(original),Buffer.byteLength(wrapper)]);
  const id=hash([...files].sort(([a],[b])=>a.localeCompare(b)).map(([name,bytes])=>name+':'+hash(bytes)).join('\n'));
  const dest=join(spaces,'data/ui-'+id,kind);for(const[name,bytes]of files)await put(join(dest,name),bytes);
  const next=prefix+'data/ui-'+id+'/'+kind+'/'+entryName;await put(htmlPath,html.replace(shown,next));rewritten.set(entry,next);nextPins.push([page,entry,next]);
 }
 await put(pinsPath,'page\tsource_entry\trendered_entry\n'+nextPins.map(r=>r.join('\t')).join('\n')+'\n');
 const inventory=[];for(const file of await walk(spaces)){if(file.endsWith('/RELEASE.tsv'))continue;const body=await readFile(file);inventory.push(`${relative(spaces,file)}\t${hash(body)}\t${body.length}`);}
 await put(join(spaces,'RELEASE.tsv'),'path\tsha256\tbytes\n'+inventory.sort().join('\n')+'\n');
 return {pack,report};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const result=await replaceSpacesComponents(process.argv[2]??'site/spaces');
 console.log('Component release',result.pack.digest,'bound to catalogue and room; original records and immutable releases retained.');
}
