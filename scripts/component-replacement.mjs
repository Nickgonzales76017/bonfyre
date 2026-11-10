// Before/after verification uses the same component release builder as Pages.
import {readFile,writeFile,mkdir,cp} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {join,resolve,relative} from 'node:path';
import {execFileSync} from 'node:child_process';
import assert from 'node:assert/strict';
import {replaceSpacesComponents,materializeComponents,bindComponentImports} from './component-release.mjs';
const root=resolve('.'),digest=b=>createHash('sha256').update(b).digest('hex');
await mkdir('component-evidence',{recursive:true});
await cp('site/spaces','component-donors/catalogue-before',{recursive:true});
await cp('component-donors/itad/site','component-donors/itad-before',{recursive:true});
const {pack,report}=await replaceSpacesComponents('site/spaces');
const first=await readFile('site/spaces/RELEASE.tsv');
await replaceSpacesComponents('site/spaces');
assert.deepEqual(await readFile('site/spaces/RELEASE.tsv'),first,'A repeat component build must leave every emitted byte and address unchanged');
const itad=resolve('component-donors/itad/site');
const expected={
 'app.mjs':'3918bb1c635c01ef45bdd09f1f585da1fbb3baeb54b216cb03fa2ea9fe067750',
 'index.html':'0d682bee21a5c793fcd0368563a08fa706c17e14b9ac27208158654a2040fb1b',
 'manifest-grid.mjs':'c4be74516d8c9728ecb7f70d78b49a30cbf02c2ec22764a995018c70005ad9c1'
};
const beforeSizes=new Map();
for(const[name,sha]of Object.entries(expected)){const bytes=await readFile(join(itad,'itad-exchange',name));beforeSizes.set(name,bytes.length);if(digest(bytes)!==sha)throw Error(`ITAD ${name} changed; reconcile its donor before replacing.`);}
const patch=resolve('integrations/browser/adapters/itad/use-shared-interactions.patch');
execFileSync('git',['apply','--check','--directory=component-donors/itad/site',patch],{cwd:root});
execFileSync('git',['apply','--directory=component-donors/itad/site',patch],{cwd:root});
const htmlPath=join(itad,'itad-exchange/index.html'),html=await readFile(htmlPath,'utf8');
if(!html.includes('<div id="bulkDiff"></div>'))throw Error('ITAD review location changed');
await writeFile(htmlPath,html.replace('<div id="bulkDiff"></div>','<div id="bulkDiff"></div><p id="bulkStatus" role="status"></p>'));
await materializeComponents(itad,pack);
const manifest=join(itad,'itad-exchange/manifest-grid.mjs');
await writeFile(manifest,bindComponentImports(await readFile('integrations/browser/adapters/itad/manifest-grid.mjs','utf8'),manifest,itad,pack));
const app=join(itad,'itad-exchange/app.mjs');await writeFile(app,bindComponentImports(await readFile(app,'utf8'),app,itad,pack));
for(const name of Object.keys(expected)){const file=join(itad,'itad-exchange',name),bytes=await readFile(file);report.push(['itad',relative(root,file),expected[name],digest(bytes),beforeSizes.get(name),bytes.length]);}
await writeFile('component-evidence/REPLACEMENTS.tsv','consumer\tpath\tbefore_sha256\tafter_sha256\tbefore_bytes\tafter_bytes\n'+report.map(r=>r.join('\t')).join('\n')+'\n');
await writeFile('component-evidence/SHARED.tsv','package\t'+pack.digest+'\n'+pack.manifest);
await writeFile('component-evidence/BUILD.txt','PASS Repeat component release produces byte-identical inventory and unchanged package addresses.\n');
console.log('Replaced',report.length,'donor bindings through the shared release builder; second build is byte-identical.');
