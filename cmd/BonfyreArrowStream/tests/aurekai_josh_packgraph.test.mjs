import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {mkdtempSync,mkdirSync,cpSync,writeFileSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {readRepositoryCutReceipt} from '../../../integrations/browser/repository-cuts.mjs';
import {repositoryCutPackDemand} from '../../../integrations/browser/repository-pack-intake.mjs';

const root=resolve(import.meta.dir,'../../..');
const cms=join(root,'cmd/BonfyreCMS/bonfyre-cms');
const arrow=join(root,'cmd/BonfyreArrowStream/bonfyre-arrow-stream');
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const cutBody=()=>{
 const provider='forgejo',org='aurekai',repository='source-modules',
  ref='HEAD',filter=':/datascience',commit='a'.repeat(40),tree='b'.repeat(40);
 const digest=sha(['bonfyre.repository-cut.v1',provider,org,repository,ref,
  filter,commit,tree,''].join('\n'));
 const fields={
   schema:'bonfyre.repository-cut.v1',source_provider:provider,source_org:org,
   source_repository:repository,requested_ref:ref,josh_filter:filter,
   virtual_commit:commit,virtual_tree:tree,cut_digest:digest,
   canonical_mapping:'unverified',pack_root:'uncompiled',
   install_authority:'none',publication_authority:'none'
 };
 return Object.entries(fields).map(([key,value])=>key+': '+value+'\n').join('');
};
test('Aurekai JOSH demand goes through canonical CMS BFP1 and ArrowStream Parquet joined proof without installation',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'aurekai-native-git-join-'));
 try{
   const schemaDir=join(dir,'schemas');mkdirSync(schemaDir);
   for(const name of ['pack_artifact','pack_version','pack_head','pack_dependency',
     'pack_overlay','pack_demand','pack_installation','pack_activation'])
     cpSync(join(root,'cmd/BonfyreCMS/content-types',name+'.json'),
       join(schemaDir,name+'.json'));
   const db=join(dir,'cms.db'),pack='aurekai-source-studio';
   const args=['--db',db,'--schemas',schemaDir];
   const cli=(binary,...argv)=>{
     const proc=Bun.spawnSync([binary,...argv],{cwd:dir,stdout:'pipe',stderr:'pipe'});
     return {code:proc.exitCode,stdout:proc.stdout.toString(),stderr:proc.stderr.toString()};
   };
   const demand=async()=>{
     const body=cutBody(),cut=await readRepositoryCutReceipt(body);
     const proposal=repositoryCutPackDemand(cut,{pack});
     expect(proposal).toMatchObject({readiness:'review_required',
       source_demand_published:false,install_authorized:false});
     return {body,cut,proposal};
   };
   expect(cli(cms,'schema','migrate',...args).code).toBe(0);
   const put=(type,value)=>{
     const answer=cli(cms,'entry','create',type,JSON.stringify(value),
       ...args,'--ns','root');
     expect(answer.code).toBe(0);
   };
   const {body,cut,proposal}=await demand(),rootHash=sha(pack+':1');
   put('pack_artifact',{pack,canonical_name:'Aurekai Source Studio',
     publisher:'aurekai',layer:'official',kind:'base',
     purpose:'JOSH source cut fixture',governed_by:'native-approval',
     cannot_infer:'No installation, source reuse, or publication',status:'published'});
   put('pack_version',{pack,version:'1',root:rootHash,changed:'Initial',
     status:'published'});
   put('pack_head',{pack,channel:'stable',version:'1',
     advanced_by:'test-reviewer',status:'published'});
   const denied=cli(cms,'pack-image',...args,'--pack',pack,'--cut-digest',cut.cutDigest);
   expect(denied.code).not.toBe(0);
   // Only the isolated test reviewer makes this source demand published.
   put('pack_demand',{...proposal.attributes,status:'published'});
   const admitted=cli(cms,'pack-image',...args,'--pack',pack,'--cut-digest',cut.cutDigest);
   expect(admitted.code).toBe(0);
   expect(admitted.stdout).toContain('repository_cut\t'+cut.cutDigest);
   const compiled=cli(cms,'pack-image',...args,'--pack',pack,'--out-dir',dir);
   expect(compiled.code).toBe(0);
   const image=/^image_path\t(.+)$/m.exec(compiled.stdout)?.[1];
   expect(image).toBeTruthy();
   const source=join(dir,'cut.yaff');writeFileSync(source,body);
   const parquet=join(dir,'evidence.parquet');
   const index=cli(arrow,'index','--cut',source,'--pack-image',image,'--out',parquet);
   expect(index.code).toBe(0);
   expect(index.stdout).toContain('matched_edges\t1');
   expect(index.stdout).toContain('rows\t3');
   expect(index.stdout).toContain('authority\tderived-only');
   const query="SELECT COUNT(*) FROM read_parquet('"+parquet+"') edge JOIN "
     +"read_parquet('"+parquet+"') cut ON edge.source_cut_digest=cut.identity_digest "
     +"WHERE edge.kind='pack_source_cut' AND cut.kind='repository_cut' "
     +"AND edge.pack='"+pack+"' AND edge.link_state='receipt-matched' "
     +"AND cut.josh_filter=':/datascience' AND cut.git_commit='"+cut.virtualCommit+"'";
   const joined=cli('duckdb','-noheader','-csv','-c',query);
   expect(joined.code).toBe(0);
   expect(joined.stdout.trim()).toBe('1');
   const verify=new Database(db,{readonly:true});
   try{
     expect(verify.query('SELECT COUNT(*) n FROM pack_installation').get().n).toBe(0);
     expect(verify.query('SELECT COUNT(*) n FROM pack_activation').get().n).toBe(0);
     expect(verify.query("SELECT preferred_sources FROM pack_demand WHERE pack=?")
       .get(pack).preferred_sources).toBe('repository-cut:'+cut.cutDigest);
   }finally{verify.close();}
 }finally{rmSync(dir,{recursive:true,force:true});}
},80000);
