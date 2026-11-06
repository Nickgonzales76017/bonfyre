import {test,expect,afterAll} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {materializePickerRepair,pickerDependencies} from '../release/publication_picker_upgrade.mjs';
import {loadWorkspaceFiles,retainWorkspaceMaterial,verifyWorkspacePackage} from '../workspace_material.mjs';
const repo=resolve(import.meta.dir,'../..'),homes=[];
afterAll(()=>homes.forEach(home=>rmSync(home,{recursive:true,force:true})));
async function fixture(broken=false){
 const root=mkdtempSync(join(tmpdir(),'picker-release-'));homes.push(root);
 const files=new Map([['instruments/author.mjs',Buffer.from('mountPublicationPicker // unchanged controller')],['instruments/production/index.html',Buffer.from('Original editor shell')],['bridge/publication-picker.mjs',Buffer.from('// previous picker')],['CONTINUATIONS.tsv',Buffer.from('original continuations')],['instruments/RECOVERY.tsv',Buffer.from('original recovery profile')]]);
 for(const name of pickerDependencies)files.set('bridge/'+name,readFileSync(join(repo,'integrations/browser',name)));
 if(broken)files.set('bridge/publication-research.mjs',Buffer.from('// incompatible dependency'));
 const base=await retainWorkspaceMaterial(files,{root});return {root,files,base};
}
test('only the picker changes in a retained connected-editor bundle',async()=>{
 const f=await fixture(),result=await materializePickerRepair({root:f.root,sourceRelease:f.base.id}),files=await loadWorkspaceFiles(f.root,result.id);
 expect((await verifyWorkspacePackage(f.root,result.id)).id).toBe(result.id);expect((await materializePickerRepair({root:f.root,sourceRelease:f.base.id})).id).toBe(result.id);
 expect(result.changedFiles).toEqual(['bridge/publication-picker.mjs']);expect(files.size).toBe(f.files.size);
 for(const [name,body]of f.files)expect(files.get(name)).toEqual(name==='bridge/publication-picker.mjs'?readFileSync(join(repo,'integrations/browser/publication-picker.mjs')):body);
 expect(await loadWorkspaceFiles(f.root,f.base.id)).toEqual(f.files);
});
test('an incompatible retained dependency refuses the narrow release',async()=>{
 const f=await fixture(true);await expect(materializePickerRepair({root:f.root,sourceRelease:f.base.id})).rejects.toThrow('dependency changed');expect(await loadWorkspaceFiles(f.root,f.base.id)).toEqual(f.files);
});
