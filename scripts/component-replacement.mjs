// Substitute checked rich renderers in the existing built donors. No data,
// catalogue generation, application key, authority or storage owner is changed.
import {readFile,writeFile,readdir,mkdir,cp} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {join,resolve,dirname,relative} from 'node:path';
import {execFileSync} from 'node:child_process';
const root=resolve('.'),runtime=join(root,'cmd/BonfyreSurfaceScene/runtime/renderers');
const digest=b=>createHash('sha256').update(b).digest('hex');
const names=['edit-history.mjs','review-sheet.mjs','record-grid.mjs','collection-workbench.mjs'];
const modules=await Promise.all(names.map(async name=>[name,await readFile(join(runtime,name))]));
const packageBody=modules.map(([name,body])=>`${name}\t${digest(body)}\t${body.length}\n`).join(''),version=digest(packageBody);
const shared=`shared/interaction-${version.slice(0,20)}`,report=[];
async function putPackage(base){for(const[name,bytes]of modules){const file=join(base,shared,name);await mkdir(dirname(file),{recursive:true});await writeFile(file,bytes);}await writeFile(join(base,shared,'SOURCE.tsv'),'path\tsha256\tbytes\n'+packageBody);}
function bindImports(text,file,base){return text.replace(/(['"])\.\/renderers\//g,(_,q)=>q+(relative(dirname(file),join(base,shared)).replaceAll('\\','/')+'/'));}
async function walk(dir){const result=[];for(const e of await readdir(dir,{withFileTypes:true})){const path=join(dir,e.name);if(e.isDirectory())result.push(...await walk(path));else result.push(path);}return result;}
await mkdir('component-evidence',{recursive:true});
// Keep byte-identical baseline material for the same browser scenarios.
await cp('site/spaces','component-donors/catalogue-before',{recursive:true});
await cp('component-donors/itad/site','component-donors/itad-before',{recursive:true});
const spaces=resolve('site/spaces');await putPackage(spaces);
const adapter=await readFile('components/BonfyreServiceSite/catalogue-grid.mjs','utf8');
const htmlPath=join(spaces,'catalogue/index.html'),html=await readFile(htmlPath,'utf8');
const entry=html.match(/<script[^>]*src="([^"]*catalogue-client\.mjs)"/)[1];
const prefix='/bonfyre/spaces/';
if(!entry.startsWith(prefix))throw Error('Catalogue entry is outside its published material.');
const base=dirname(join(spaces,entry.slice(prefix.length))),files=new Map();
for(const file of await walk(base))files.set(relative(base,file),await readFile(file));
const before=files.get('catalogue-grid.mjs')?.toString();
if(!before?.includes('inventoryWorkspace')||!before.includes('inspectProduct'))throw Error('The current catalogue has no source-bound 3D continuation.');
const prospective=join(spaces,'data/ui-component/catalogue/catalogue-grid.mjs');
const after=bindImports(adapter,prospective,spaces);files.set('catalogue-grid.mjs',Buffer.from(after));
const id=digest([...files].sort(([a],[b])=>a.localeCompare(b)).map(([name,body])=>name+':'+digest(body)).join('\n'));
const dest=join(spaces,'data/ui-'+id+'/catalogue');
for(const[name,body]of files){await mkdir(dirname(join(dest,name)),{recursive:true});await writeFile(join(dest,name),body);}
const next=prefix+'data/ui-'+id+'/catalogue/catalogue-client.mjs';
await writeFile(htmlPath,html.replace(entry,next));
report.push(`catalogue\t${relative(root,join(dest,'catalogue-grid.mjs'))}\t${digest(before)}\t${digest(after)}\t${Buffer.byteLength(before)}\t${Buffer.byteLength(after)}`);
// Changes to private ITAD controls are reviewed as a preimage-bound source patch.
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
const itadHTMLPath=join(itad,'itad-exchange/index.html');
const itadHTML=await readFile(itadHTMLPath,'utf8');
if(!itadHTML.includes('<div id="bulkDiff"></div>'))throw Error('ITAD review location changed.');
await writeFile(itadHTMLPath,itadHTML.replace('<div id="bulkDiff"></div>','<div id="bulkDiff"></div><p id="bulkStatus" role="status"></p>'));
await putPackage(itad);
const manifestPath=join(itad,'itad-exchange/manifest-grid.mjs');
await writeFile(manifestPath,bindImports(await readFile('integrations/browser/adapters/itad/manifest-grid.mjs','utf8'),manifestPath,itad));
const appPath=join(itad,'itad-exchange/app.mjs');await writeFile(appPath,bindImports(await readFile(appPath,'utf8'),appPath,itad));
for(const name of Object.keys(expected)){const file=join(itad,'itad-exchange',name),bytes=await readFile(file);report.push(`itad\t${relative(root,file)}\t${expected[name]}\t${digest(bytes)}\t-\t${bytes.length}`);}
// Existing release inventory owns emitted bytes. Do not touch the source data digests.
const inventory=[];for(const file of await walk(spaces)){if(file.endsWith('/RELEASE.tsv'))continue;const body=await readFile(file);inventory.push(`${relative(spaces,file)}\t${digest(body)}\t${body.length}`);}
await writeFile(join(spaces,'RELEASE.tsv'),'path\tsha256\tbytes\n'+inventory.sort().join('\n')+'\n');
await writeFile('component-evidence/REPLACEMENTS.tsv','consumer\tpath\tbefore_sha256\tafter_sha256\tbefore_bytes\tafter_bytes\n'+report.join('\n')+'\n');
await writeFile('component-evidence/SHARED.tsv','package\t'+version+'\n'+packageBody);
console.log('Replaced',report.length,'donor files with one shared four-module renderer package per origin.');
