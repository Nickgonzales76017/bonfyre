import {test,expect} from 'bun:test';
import {mkdtempSync,readFileSync,writeFileSync,readdirSync,rmSync,chmodSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {nativeWorkbenchPackage} from '../native-workbench-package.mjs';
import {WEB_FILES} from '../web-package.mjs';
import {retainWorkspaceFiles,manifestRows,readWorkspaceFile} from '../../../scripts/materialize_workspace_package.mjs';
import {projectProgramWorkspaceSetup} from '../../../scripts/publish_experience_setups.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex');
test('native package retains exact compiler sources, binary storage bridge and record-bound documents',async()=>{
 const root=mkdtempSync(join(tmpdir(),'program-package-'));try{const result=await nativeWorkbenchPackage({packageRoot:root}),manifest=readFileSync(join(result.dir,'MANIFEST.tsv'),'utf8');expect(sha(manifest)).toBe(result.id);
 const expected=[...WEB_FILES.map(name=>'programs/'+name),'programs/index.html','CONTINUATIONS.tsv','SOURCE.tsv',...['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs'].map(name=>'bridge/'+name)];
 expect(result.files).toBe(expected.length);expect(manifestRows(manifest).map(row=>row.path).sort()).toEqual(expected.sort());
 for(const row of manifestRows(manifest)){const body=await readWorkspaceFile(root,result.id,row.path);expect(body.length).toBe(row.bytes);expect(sha(body)).toBe(row.hash);}
 const html=(await readWorkspaceFile(root,result.id,'programs/index.html')).toString('utf8');expect(html).toContain('bonfyre-workspace-store');expect(html).toContain('/apps/__WORKSPACE__/_records');expect(html).toContain('__DESTINATION__');expect(html).toContain('__WORKSPACE_NAV__');expect((await readWorkspaceFile(root,result.id,'CONTINUATIONS.tsv')).toString('utf8')).toBe('view\tlabel\tentry\tprefix\ttitle\tquery\nprogram\tRecurring program\tprograms/\tdraft:\tname\tdraft=$key\n');
 expect(await readWorkspaceFile(root,result.id,'programs/recurring-commerce.mjs')).toEqual(readFileSync(new URL('../../../scripts/recurring-commerce/draft.mjs',import.meta.url)));expect((await readWorkspaceFile(root,result.id,'programs/store.mjs')).toString('utf8')).toContain("import('../bridge/workspace-store.mjs')");expect((await nativeWorkbenchPackage({packageRoot:root})).id).toBe(result.id);expect(readdirSync(root).sort()).toEqual([result.id,'_bodies'].sort());
 const definition=projectProgramWorkspaceSetup(result.id);expect(definition.filter(r=>r.type==='pack_artifact')).toHaveLength(1);expect(definition.filter(r=>r.type==='pack_demand'&&r.value.demand_kind==='workspace-record').map(r=>r.value.fields)).toEqual(['draft:','observation:']);expect(definition.at(-1).type).toBe('pack_head');
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('reviewed local material uses the existing path and size checks without partial retention',async()=>{
 const root=mkdtempSync(join(tmpdir(),'program-package-reject-'));try{for(const name of ['../outside','/absolute','bad//path','MANIFEST.tsv'])await expect(retainWorkspaceFiles(new Map([[name,Buffer.from('x')]]),{root})).rejects.toThrow();expect(readdirSync(root)).toEqual([]);await expect(retainWorkspaceFiles(new Map([['body','not bytes']]),{root})).rejects.toThrow();
 const result=await retainWorkspaceFiles(new Map([['programs/index.html',Buffer.from('document')]]),{root});const hash=sha('document'),body=join(root,'_bodies/sha256',hash.slice(0,2),hash);chmodSync(body,0o600);writeFileSync(body,'corrupt');await expect(retainWorkspaceFiles(new Map([['programs/index.html',Buffer.from('document')]]),{root})).rejects.toThrow('Source changed');
 }finally{rmSync(root,{recursive:true,force:true});}
});
