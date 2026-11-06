// Narrow renderer projection. No source record, task or permission is changed.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from './workspace_material.mjs';
const repo=resolve(import.meta.dir,'..');
function replaceOnce(input,before,after){assert.equal(input.split(before).length,2,'The current renderer changed: '+before.slice(0,70));return input.replace(before,after);}
export function referenceAwareResearch(input){
  assert(typeof input==='string'&&!input.includes('createMaterialReader'),'This renderer already has a reference reader.');
  input="import {createMaterialReader} from '../bridge/retained-material-view.mjs';\n"+input;
  input=replaceOnce(input,' let pending=null,searchAbort=null;',' const material=createMaterialReader({recordKey:()=>ctx.sourceRecordKey()});\n let pending=null,searchAbort=null;');
  input=replaceOnce(input,'const prefix=range.cloneRange();prefix.selectNodeContents(box);prefix.setEnd(range.startContainer,range.startOffset);const start=prefix.toString().length;pending=excerpt(current(),start,start+range.toString().length);','const exact=material.selection(box,current(),selection);pending=excerpt(current(),exact.start,exact.end);');
  const highlights=input.split('\n').filter(line=>line.startsWith(' function highlight(ref){'));assert.equal(highlights.length,1);
  input=replaceOnce(input,highlights[0]," function highlight(ref){const box=ctx.main.querySelector('#original-source');if(box&&validateExcerpt(state.sources,ref))material.highlight(box,current(),ref);}");
  input=replaceOnce(input,"el('div',{id:'original-source',class:'source-text',tabIndex:0,text:s.body})","material.mount(el('div',{id:'original-source',class:'source-text',tabIndex:0}),s)");
  input=replaceOnce(input,'cleanup:()=>searchAbort?.abort()','cleanup:()=>{searchAbort?.abort();material.dispose();}');return input;
}
export async function materializeReferenceRepair({sourceRelease,root=join(homedir(),'.bonfyre/workspace-packages')}={}){
  assert(/^[a-f0-9]{64}$/.test(sourceRelease),'Choose the exact current editor release.');
  const files=await loadWorkspaceFiles(root,sourceRelease),before=new Map(files),inputs=[];
  assert(files.has('instruments/research-view.mjs')&&files.has('bridge/workspace-store.mjs'));
  files.set('instruments/research-view.mjs',Buffer.from(referenceAwareResearch(files.get('instruments/research-view.mjs').toString())));
  const app=files.get('instruments/app.mjs').toString();
  files.set('instruments/app.mjs',Buffer.from(replaceOnce(app,'const ctx={main:elements.main,','const ctx={sourceRecordKey:()=>record?.key,main:elements.main,')));
  for(const name of ['retained-material.mjs','retained-material-view.mjs','retained-material.css','interaction-collection.mjs']){
    const path='integrations/browser/'+name,body=await readFile(join(repo,path));files.set('bridge/'+name,body);inputs.push([path,createHash('sha256').update(body).digest('hex')]);
  }
  const changed=[...before].filter(([name,body])=>!body.equals(files.get(name))).map(([name])=>name).sort();assert.deepEqual(changed.filter(name=>name!=='bridge/interaction-collection.mjs'),['instruments/app.mjs','instruments/research-view.mjs']);
  files.set('SOURCE.tsv',Buffer.from((files.get('SOURCE.tsv')?.toString()||'')+'\nreference_reader_base\t'+sourceRelease+'\nreference_reader\tCurrent-account source reads; exact historical text and evidence offsets preserved.\n'+inputs.map(r=>r.join('\t')).join('\n')+'\n'));
  const result=await retainWorkspaceMaterial(files,{root});return{...result,base:sourceRelease,pack:'experience.connected-instruments.workspace',changedFiles:[...changed,'SOURCE.tsv'],inputs};
}
if(import.meta.main){const[sourceRelease]=process.argv.slice(2);console.log(await materializeReferenceRepair({sourceRelease}));}
