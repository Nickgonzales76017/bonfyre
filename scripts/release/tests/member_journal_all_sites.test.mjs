import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {selectMemberJournalCanaries,verifyMemberJournalReadback} from '../member_journal_canary.mjs';

const fixture=()=>{
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-all-member-journals-'));
 const path=join(dir,'cms.db'),db=new Database(path,{create:true});
 db.exec("CREATE TABLE site_presentation(namespace TEXT,status TEXT,profile TEXT,site_id TEXT,host TEXT);"+
  "CREATE TABLE site_page(namespace TEXT,status TEXT,route TEXT);"+
  "CREATE TABLE site_fragment(namespace TEXT,page TEXT,slot TEXT,audience TEXT,status TEXT,kind TEXT,projection_contract TEXT,semantic_tags TEXT,body TEXT);");
 return {dir,path,db,close:()=>{db.close();rmSync(dir,{recursive:true,force:true});}};
};
const journal=(db,site,host,count)=>{
 db.query("INSERT INTO site_presentation VALUES('root','published','member-profile-v1',?,?)").run(site,host);
 const page=db.query("INSERT INTO site_page VALUES(?,'published',?)");
 const fragment=db.query("INSERT INTO site_fragment VALUES(?,?,'showcase','public','published','profile-story','{\"html\":1}','{\"authoring_v1\":{\"version\":1}}',?)");
 for(let i=0;i<count;i++){const route='/blog/story-'+String(i).padStart(3,'0')+'/';page.run(site,route);fragment.run(site,route,'Substantive published editorial passage '+i+'.');}
};
const read=async(base,route,host)=>{
 const hostname=base?new URL(base).host:host;
 const text=route==='/blog/'?'<div class="fd ak-journal-list"></div>':route==='/?tab=posts'
  ?'<main data-profile-journal></main>':'<link rel="canonical" href="https://'+hostname+route+'"><div class="ak-zen-body ak-zen-text"><p>Reviewed original article body available through the native editor.</p></div>';
 return {status:200,text,sha256:'a'.repeat(64)};
};

test('every published member journal is included, not only the largest account',async()=>{
 const f=fixture();
 try{
  journal(f.db,'person-writer-work','writer.aurekai.work',91);
  journal(f.db,'person-editor-life','editor.aurekai.life',2);
  f.db.close();
  const canaries=selectMemberJournalCanaries(f.path);
  expect(canaries.map(x=>[x.host,x.total])).toEqual([['writer.aurekai.work',91],['editor.aurekai.life',2]]);
  expect(canaries[0].routes).toHaveLength(91);
  expect(canaries[1].routes).toHaveLength(2);
  for(const canary of canaries){
   const local=await verifyMemberJournalReadback(canary,read);
   const publicReceipt=await verifyMemberJournalReadback(canary,read,{publicRead:true});
   expect(local.routes_verified).toBe(canary.total);
   expect(publicReceipt.routes_verified).toBe(canary.total);
  }
 }finally{try{f.db.close()}catch{}rmSync(f.dir,{recursive:true,force:true});}
});
test('a secondary member site with a 404 rejects the release despite a healthy largest journal',async()=>{
 const f=fixture();
 try{
  journal(f.db,'person-writer-work','writer.aurekai.work',91);
  journal(f.db,'person-editor-life','editor.aurekai.life',2);
  f.db.close();
  const canaries=selectMemberJournalCanaries(f.path);
  expect((await verifyMemberJournalReadback(canaries[0],read)).routes_verified).toBe(91);
  const broken=(base,route,host)=>route==='/blog/story-001/'?Promise.resolve({status:404,text:'',sha256:'0'.repeat(64)}):read(base,route,host);
  await expect(verifyMemberJournalReadback(canaries[1],broken,{publicRead:true})).rejects.toThrow('published article');
 }finally{try{f.db.close()}catch{}rmSync(f.dir,{recursive:true,force:true});}
});
test('a secondary member journal without released typed content blocks selection',()=>{
 const f=fixture();
 try{
  journal(f.db,'person-writer-work','writer.aurekai.work',91);
  journal(f.db,'person-editor-life','editor.aurekai.life',2);
  f.db.query("UPDATE site_fragment SET audience='self' WHERE namespace='person-editor-life' AND page='/blog/story-001/'").run();
  f.db.close();
  expect(()=>selectMemberJournalCanaries(f.path)).toThrow('published pages without released authored bodies');
 }finally{try{f.db.close()}catch{}rmSync(f.dir,{recursive:true,force:true});}
});
test('member journal site count cannot be silently truncated by the bounded readback',()=>{
 const f=fixture();
 try{
  for(let i=0;i<65;i++)journal(f.db,'person-'+i+'-work','member-'+i+'.aurekai.work',1);
  f.db.close();
  expect(()=>selectMemberJournalCanaries(f.path)).toThrow('more than 64 sites');
 }finally{try{f.db.close()}catch{}rmSync(f.dir,{recursive:true,force:true});}
});
