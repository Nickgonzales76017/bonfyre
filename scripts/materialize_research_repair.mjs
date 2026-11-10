// Replace only the Research Desk editor inside an exact retained planning bundle.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
import {render} from '../components/BonfyreServiceSite/sections/synthesis.mjs';
const repo=resolve(import.meta.dir,'..'),hash=bytes=>createHash('sha256').update(bytes).digest('hex');
export const researchRepairModules=['synthesis.mjs','synthesis-client.mjs','campaign-composer.mjs','campaign-composer-client.mjs'];
export const researchRepairSources=['scripts/materialize_research_repair.mjs','scripts/workspace_material.mjs','components/BonfyreServiceSite/sections/synthesis.mjs','components/BonfyreServiceSite/synthesis.css',...researchRepairModules.map(name=>'components/BonfyreServiceSite/'+name)];
export async function materializeResearchRepair({sourceRelease,root=join(homedir(),'.bonfyre/workspace-packages')}={}){
 assert(/^[a-f0-9]{64}$/.test(sourceRelease??''),'Choose the exact retained planning package.');
 const files=await loadWorkspaceFiles(root,sourceRelease),before=new Map(files);
 for(const name of ['research/index.html','research/sample-project.json','research/instrument-store.mjs','bridge/workspace-store.mjs','instruments/index.html','CONTINUATIONS.tsv'])assert(files.has(name),'Incomplete planning package: '+name);
 const html=files.get('research/index.html').toString(),start='<section data-kind="synthesis" id="research" class="research-desk" data-synthesis>',end='<script type="module" src="./synthesis-client.mjs"></script></section>';
 assert.equal(html.split(start).length,2,'The installed research section changed.');assert.equal(html.split(end).length,2,'The installed research section boundary changed.');
 const from=html.indexOf(start),to=html.indexOf(end)+end.length;assert(to>from);
 const sample=JSON.parse(files.get('research/sample-project.json').toString());assert(Array.isArray(sample.retrieved)&&sample.retrieved[0]?.fetched_at,'The retained research sample has no acquisition date.');
 const esc=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const section=render({id:'research',title:'Research desk',data:'/data/research/',asset_prefix:'v-'+sourceRelease+'/',sample},{},{esc})
  .replace('<section id="research"', '<section data-kind="synthesis" id="research"')
  .replaceAll('/data/research/v-'+sourceRelease+'/', './').replaceAll('href="/#','href="../#');
 files.set('research/index.html',Buffer.from(html.slice(0,from)+section+html.slice(to)));
 for(const name of researchRepairModules){assert(files.has('research/'+name),'Missing original research module.');files.set('research/'+name,await readFile(join(repo,'components/BonfyreServiceSite',name)));}
 const allowed=new Set(['research/index.html',...researchRepairModules.map(name=>'research/'+name)]);
 const changedFiles=[...before].filter(([name,body])=>!body.equals(files.get(name))).map(([name])=>name).sort();
 assert(changedFiles.every(name=>allowed.has(name))&&changedFiles.length>0,'The repair must change only its selected editor files.');
 const inputs=await Promise.all(researchRepairSources.map(async path=>({path,sha256:hash(await readFile(join(repo,path)))})));
 const result=await retainWorkspaceMaterial(files,{root});
 return {...result,base:sourceRelease,changedFiles,inputs,preservedFiles:files.size-changedFiles.length};
}
