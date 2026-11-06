import {test,expect,afterAll} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {materializeResearchRepair,researchRepairModules} from '../materialize_research_repair.mjs';
import {retainWorkspaceMaterial,loadWorkspaceFiles,verifyWorkspacePackage} from '../workspace_material.mjs';
const homes=[],repo=resolve(import.meta.dir,'../..');
afterAll(()=>homes.forEach(home=>rmSync(home,{recursive:true,force:true})));
async function fixture({broken=false}={}){
 const root=mkdtempSync(join(tmpdir(),'research-projection-'));homes.push(root);
 const files=new Map([
  ['research/index.html',Buffer.from('<html><head><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"></head><body><nav>Retained navigation</nav><main><section data-kind="synthesis" id="research" class="research-desk" data-synthesis><p>Previous editor</p><script type="module" src="./synthesis-client.mjs"></script></section></main></body></html>')],
  ['research/sample-project.json',Buffer.from(JSON.stringify({retrieved:[{fetched_at:'2026-10-19T00:00:00Z'}],records:[],excerpts:[],claims:[],decisions:{}}))],
  ['research/instrument-store.mjs',Buffer.from('// preserved shared instrument store')],
  ['bridge/workspace-store.mjs',Buffer.from('// preserved native store')],
  ['instruments/index.html',Buffer.from('<a href="../research/">Research desk</a>')],
  ['CONTINUATIONS.tsv',Buffer.from('view\tlabel\tentry\tprefix\ttitle\tquery\nreview\tResearch desk\tresearch/\treview-project:\tname\trecord=$key\n')],
  ['pricing/index.html',Buffer.from('Other editor exact bytes')]
 ]);
 for(const name of researchRepairModules)files.set('research/'+name,Buffer.from('// earlier '+name));
 if(broken)files.set('research/index.html',Buffer.from('<html>Unreviewed editor layout</html>'));
 const base=await retainWorkspaceMaterial(files,{root});return{root,files,base};
}
test('projection changes only the four reviewed modules and research section, preserving other bundle bytes',async()=>{
 const f=await fixture(),result=await materializeResearchRepair({root:f.root,sourceRelease:f.base.id});
 expect((await verifyWorkspacePackage(f.root,result.id)).id).toBe(result.id);
 expect((await materializeResearchRepair({root:f.root,sourceRelease:f.base.id})).id).toBe(result.id);
 const files=await loadWorkspaceFiles(f.root,result.id);expect(files.size).toBe(f.files.size);
 expect(result.changedFiles).toEqual(['research/index.html',...researchRepairModules.map(name=>'research/'+name)].sort());
 for(const [name,body]of f.files)if(!result.changedFiles.includes(name))expect(files.get(name)).toEqual(body);
 for(const name of researchRepairModules)expect(files.get('research/'+name)).toEqual(readFileSync(join(repo,'components/BonfyreServiceSite',name)));
 const html=files.get('research/index.html').toString();expect(html).toContain('<nav>Retained navigation</nav>');expect(html).toContain('data-ui="campaigns"');expect(html).toContain('src="./synthesis-client.mjs"');expect(html).toEndWith('</main></body></html>');expect(html).not.toContain('/data/research/');
 expect(await loadWorkspaceFiles(f.root,f.base.id)).toEqual(f.files);
});
test('an unknown section boundary refuses projection without altering its retained source',async()=>{
 const f=await fixture({broken:true});await expect(materializeResearchRepair({root:f.root,sourceRelease:f.base.id})).rejects.toThrow('research section changed');expect(await loadWorkspaceFiles(f.root,f.base.id)).toEqual(f.files);
});
