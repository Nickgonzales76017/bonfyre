import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {readRepositoryCutReceipt} from '../repository-cuts.mjs';
import {prepareRepositoryCutSource,inspectRepositoryCutSource,saveReviewedRepositoryCut}
 from '../repository-cut-intake.mjs';
import {prepareFile,readSourceText} from '../../../cmd/BonfyreSourceFrontier/tools/source_material.mjs';

function receipt(filter=':/src'){
 const commit='a'.repeat(40),tree='b'.repeat(40);
 const fields={
  schema:'bonfyre.repository-cut.v1',source_provider:'forgejo',source_org:'aurekai',
  source_repository:'source-set',requested_ref:'HEAD',josh_filter:filter,
  virtual_commit:commit,virtual_tree:tree,
  cut_digest:createHash('sha256').update(['bonfyre.repository-cut.v1','forgejo','aurekai',
   'source-set','HEAD',filter==='none'?'':filter,commit,tree,''].join('\n')).digest('hex'),
  canonical_mapping:filter==='none'?'direct-git':'unverified',
  pack_root:'uncompiled',install_authority:'none',publication_authority:'none'
 };
 return Object.entries(fields).map(([key,value])=>key+': '+value+String.fromCharCode(10)).join('');
}
async function fixture(body=receipt()){
 const file=new File([body],'source-cut.yaff',{type:'text/plain'});
 const cut=await readRepositoryCutReceipt(body);
 return {file,cut,prepared:await prepareRepositoryCutSource(file,cut,prepareFile)};
}
function store(){
 const values=new Map();let writes=0;
 return {
  values,get writes(){return writes;},
  async readVersion(key){return values.get(key)||null;},
  async saveLocal(key,value,revision){
   expect(revision).toBe(0);writes++;
   values.set(key,{key,value,revision:1});
   return {key,revision:1};
  }
 };
}
test('native YAFF stays exact reviewable Source Library text and carries pinned JOSH digest',async()=>{
 const {prepared,cut,file}=await fixture();
 const change=prepared.change;
 expect(change.key).toMatch(/^source-material:[a-f0-9]{64}$/);
 expect(change.expectedRevision).toBe(0);
 expect(change.value).toMatchObject({schema:'bonfyre.source-material.v1',
   kind:'file',collection:'Repository cuts',textLike:true,bytes:file.size,
   filename:file.name});
 expect(change.value.tags).toEqual(['git','repository-cut','josh']);
 expect(change.value.original).toBe(file);
 expect((await readSourceText({value:change.value}))).toBe(await file.text());
 expect(cut.canonicalMapping).toBe('unverified');
});
test('explicit reviewed save creates one source record; repeated action does not rewrite it',async()=>{
 const {prepared}=await fixture(),db=store();
 expect(await inspectRepositoryCutSource(db,prepared)).toEqual({state:'absent'});
 const first=await saveReviewedRepositoryCut(db,prepared);
 expect(first).toMatchObject({outcome:'saved',revision:1,key:prepared.change.key});
 const again=await saveReviewedRepositoryCut(db,prepared);
 expect(again).toMatchObject({outcome:'already_retained',revision:1});
 expect(db.writes).toBe(1);
});
test('post-write lost acknowledgement recovers by exact saved readback without replay',async()=>{
 const {prepared}=await fixture(),db=store();
 const save=db.saveLocal.bind(db);
 db.saveLocal=async(...args)=>{await save(...args);throw Error('transport lost');};
 expect(await saveReviewedRepositoryCut(db,prepared)).toMatchObject({outcome:'recovered',revision:1});
 expect(db.writes).toBe(1);
});
test('ambiguous uncommitted write fails closed and never retries',async()=>{
 const {prepared}=await fixture(),db=store();
 db.saveLocal=async()=>{throw Error('offline');};
 await expect(saveReviewedRepositoryCut(db,prepared)).rejects.toMatchObject({code:'source_save_uncertain'});
});
test('changed source-cut bytes and conflicting saved source refuse intake or overwrite',async()=>{
 const {file,cut,prepared}=await fixture(),db=store();
 const changed=new File([receipt(':/other')],file.name,{type:'text/plain'});
 await expect(prepareRepositoryCutSource(changed,cut,prepareFile)).rejects.toThrow();
 db.values.set(prepared.change.key,{key:prepared.change.key,revision:1,
  value:{...prepared.change.value,contentSHA256:'0'.repeat(64)}});
 expect(await inspectRepositoryCutSource(db,prepared)).toEqual({state:'conflict'});
 await expect(saveReviewedRepositoryCut(db,prepared)).rejects.toThrow();
 expect(db.writes).toBe(0);
});
