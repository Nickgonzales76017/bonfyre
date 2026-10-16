// Substitute checked rich renderers in existing built donors. No catalogue,
// application identity, authority or storage owner is changed.
import {readFile,writeFile,readdir,mkdir,cp} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {join,resolve,dirname,relative} from 'node:path';
import {execFileSync} from 'node:child_process';
const root=resolve('.'),runtime=join(root,'cmd/BonfyreSurfaceScene/runtime/renderers');
const digest=b=>createHash('sha256').update(b).digest('hex');
const names=['edit-history.mjs','review-sheet.mjs','record-grid.mjs','collection-workbench.mjs','scene-workbench.mjs'];
const modules=await Promise.all(names.map(async name=>[name,await readFile(join(runtime,name))]));
const packageBody=modules.map(([name,body])=>`${name}\t${digest(body)}\t${body.length}\n`).join(''),version=digest(packageBody);
const shared=`shared/interaction-${version.slice(0,20)}`,report=[];
async function put(file,bytes){await mkdir(dirname(file),{recursive:true});await writeFile(file,bytes);}
async function putPackage(base){for(const[name,bytes]of modules)await put(join(base,shared,name),bytes);await writeFile(join(base,shared,'SOURCE.tsv'),'path\tsha256\tbytes\n'+packageBody);}
function bindImports(text,file,base){return text.replace(/(['"])\.\/renderers\//g,(_,q)=>q+(relative(dirname(file),join(base,shared)).replaceAll('\\','/')+'/'));}
async function walk(dir){const result=[];for(const e of await readdir(dir,{withFileTypes:true})){const path=join(dir,e.name);if(e.isDirectory())result.push(...await walk(path));else result.push(path);}return result;}
await mkdir('component-evidence',{recursive:true});
await cp('site/spaces','component-donors/catalogue-before',{recursive:true});
await cp('component-donors/itad/site','component-donors/itad-before',{recursive:true});
const spaces=resolve('site/spaces'),prefix='/bonfyre/spaces/';await putPackage(spaces);
const catalogueAdapter=await readFile('components/BonfyreServiceSite/catalogue-grid.mjs','utf8');
const sceneAdapter=await readFile('integrations/browser/adapters/product-scene-tools.mjs','utf8');
const plannerEntries=new Map();
for(const[page,kind,entryName]of [['catalogue','catalogue','catalogue-client.mjs'],['product-room','planner','planner-client.mjs'],['red3-planner','planner','planner-client.mjs']]){
 const htmlPath=join(spaces,page,'index.html'),html=await readFile(htmlPath,'utf8');
 const entry=html.match(new RegExp('<script[^>]*src="([^"]*'+entryName.replace('.','\\.')+')"'))?.[1];
 if(!entry?.startsWith(prefix))throw Error('Application entry is outside published material: '+page);
 if(plannerEntries.has(entry)){await writeFile(htmlPath,html.replace(entry,plannerEntries.get(entry)));continue;}
 const base=dirname(join(spaces,entry.slice(prefix.length))),files=new Map();
 for(const file of await walk(base))files.set(relative(base,file),await readFile(file));
 const prospective=join(spaces,'data/ui-component',kind);
 if(kind==='catalogue'){
  const before=files.get('catalogue-grid.mjs')?.toString();
  if(!before?.includes('inventoryWorkspace')||!before.includes('inspectProduct'))throw Error('The current catalogue has no source-bound 3D continuation.');
  const after=bindImports(catalogueAdapter,join(prospective,'catalogue-grid.mjs'),spaces);
  files.set('catalogue-grid.mjs',Buffer.from(after));
  report.push(`catalogue\tcatalogue-grid.mjs\t${digest(before)}\t${digest(after)}\t${Buffer.byteLength(before)}\t${Buffer.byteLength(after)}`);
 }else{
  const before=files.get('planner-client.mjs');
  if(digest(before)!=='9085817ad65f32b945470fd337d36951ae1d563c00c0177dcb75210767bec125')throw Error('Planner donor changed; reconcile before substituting history.');
  const temp='component-donors/planner-candidate';await put(join(temp,'planner-client.mjs'),before);
  const patch=resolve('integrations/browser/adapters/planner/use-shared-history.patch');
  execFileSync('git',['apply','--check','--directory='+temp,patch],{cwd:root});
  execFileSync('git',['apply','--directory='+temp,patch],{cwd:root});
  const after=bindImports(await readFile(join(temp,'planner-client.mjs'),'utf8'),join(prospective,'planner-client.mjs'),spaces);
  files.set('planner-client.mjs',Buffer.from(after));
  report.push(`planner\tplanner-client.mjs\t${digest(before)}\t${digest(after)}\t${before.length}\t${Buffer.byteLength(after)}`);
 }
 // Replace the same helper-backed scene in both real consumers. Retain its
 // published style separately; domain selectors do not enter the shared engine.
 let scene=files.get('product-scene.mjs').toString();
 const old=scene.match(/import \{createStudioStage,studioCSS\} from '([^']+)'/);
 if(!old||!old[1].startsWith(prefix))throw Error('The reviewed scene helper binding changed.');
 const original=await readFile(join(spaces,old[1].slice(prefix.length)),'utf8');
 if(digest(original)!=='ae6c5ac24536afef0a9b054a9ffc782cc647abaa578223f2ed35c1ee37025550')throw Error('The scene donor changed; reconcile before replacing.');
 const styleAt=original.indexOf('export const studioCSS=');if(styleAt<0)throw Error('The donor style is missing.');
 files.set('product-scene-style.mjs',Buffer.from(original.slice(styleAt)));
 const boundScene=bindImports(sceneAdapter,join(prospective,'product-scene-tools.mjs'),spaces);
 files.set('product-scene-tools.mjs',Buffer.from(boundScene));
 files.set('product-scene.mjs',Buffer.from(scene.replace(old[0],"import {createStudioStage,studioCSS} from './product-scene-tools.mjs'")));
 report.push(`${kind}-scene\tproduct-scene-tools.mjs\t${digest(original)}\t${digest(boundScene)}\t${Buffer.byteLength(original)}\t${Buffer.byteLength(boundScene)}`);
 const id=digest([...files].sort(([a],[b])=>a.localeCompare(b)).map(([name,body])=>name+':'+digest(body)).join('\n'));
 const dest=join(spaces,'data/ui-'+id,kind);for(const[name,body]of files)await put(join(dest,name),body);
 const next=prefix+'data/ui-'+id+'/'+kind+'/'+entryName;await writeFile(htmlPath,html.replace(entry,next));plannerEntries.set(entry,next);
}
const itad=resolve('component-donors/itad/site');
const expected={
 'app.mjs':'3918bb1c635c01ef45bdd09f1f585da1fbb3baeb54b216cb03fa2ea9fe067750',
 'index.html':'0d682bee21a5c793fcd0368563a08fa706c17e14b9ac27208158654a2040fb1b',
 'manifest-grid.mjs':'c4be74516d8c9728ecb7f70d78b49a30cbf02c2ec22764a995018c70005ad9c1'
};
for(const[name,sha]of Object.entries(expected)){const bytes=await readFile(join(itad,'itad-exchange',name));if(digest(bytes)!==sha)throw Error(`ITAD ${name} changed; reconcile its donor before replacing.`);}
const patch=resolve('integrations/browser/adapters/itad/use-shared-interactions.patch');
execFileSync('git',['apply','--check','--directory=component-donors/itad/site',patch],{cwd:root});
execFileSync('git',['apply','--directory=component-donors/itad/site',patch],{cwd:root});
const itadHTMLPath=join(itad,'itad-exchange/index.html'),itadHTML=await readFile(itadHTMLPath,'utf8');
if(!itadHTML.includes('<div id="bulkDiff"></div>'))throw Error('ITAD review location changed.');
await writeFile(itadHTMLPath,itadHTML.replace('<div id="bulkDiff"></div>','<div id="bulkDiff"></div><p id="bulkStatus" role="status"></p>'));
await putPackage(itad);
const manifestPath=join(itad,'itad-exchange/manifest-grid.mjs');
await writeFile(manifestPath,bindImports(await readFile('integrations/browser/adapters/itad/manifest-grid.mjs','utf8'),manifestPath,itad));
const appPath=join(itad,'itad-exchange/app.mjs');await writeFile(appPath,bindImports(await readFile(appPath,'utf8'),appPath,itad));
for(const name of Object.keys(expected)){const file=join(itad,'itad-exchange',name),bytes=await readFile(file);report.push(`itad\t${relative(root,file)}\t${expected[name]}\t${digest(bytes)}\t-\t${bytes.length}`);}
const inventory=[];for(const file of await walk(spaces)){if(file.endsWith('/RELEASE.tsv'))continue;const body=await readFile(file);inventory.push(`${relative(spaces,file)}\t${digest(body)}\t${body.length}`);}
await writeFile(join(spaces,'RELEASE.tsv'),'path\tsha256\tbytes\n'+inventory.sort().join('\n')+'\n');
await writeFile('component-evidence/REPLACEMENTS.tsv','consumer\tpath\tbefore_sha256\tafter_sha256\tbefore_bytes\tafter_bytes\n'+report.join('\n')+'\n');
await writeFile('component-evidence/SHARED.tsv','package\t'+version+'\n'+packageBody);
console.log('Replaced',report.length,'donor bindings with one shared five-module renderer package per origin.');
