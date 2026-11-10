import {test} from "node:test";
import assert from "node:assert/strict";
import {newAuthoringTree,applyAuthoringEdit} from "../runtime/authoring-tree.mjs";
import {treeFromNativeSnapshot,toNativePageWrite,openNativeAuthoring,saveNativeAuthoring}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
const digest="a".repeat(64);
const base=(ns="person-test-work",route="/stories",id=0)=>({
 schema:"bonfyre.page-snapshot.v1",id,digest,
 snapshot:{namespace:ns,route,page:id?{id:12,route,title:"Stories",heading:"Stories",status:"draft"}:null,fragments:[]}
});
const node=(id,kind="profile-card",extra={})=>({id,kind,
 props:{heading:id,body:"A visible story",variant:"forest",...extra},
 responsive:{desktop:{layout:"grid",columns:2},phone:{layout:"stack"}},children:[]});
const story=(ns="person-test-work")=>{
 let doc=newAuthoringTree(ns,"/stories");
 doc=applyAuthoringEdit(doc,{type:"insert",block:node("story-section","profile-section"),index:0});
 doc=applyAuthoringEdit(doc,{type:"insert",parent:"story-section",block:node("first-story","profile-story",{
  editorial:[{id:"p1",type:"paragraph",data:{text:"Original paragraph"}}]}),index:0});
 return doc;
};
const flat=doc=>doc.blocks.map(x=>x.id+"("+x.children.map(c=>c.id).join(",")+")").join("|");
test("native CMS aggregate keeps nested coordinates, reading order and editorial blocks",()=>{
 const b=base(),doc=story();
 const write=toNativePageWrite(b,doc,{pageDraft:{title:"Writing",heading:"Original writing"}});
 assert.equal(write.operation,"create");
 assert.equal(write.expected_id,0);
 assert.equal(write.page.status,"draft");
 assert.equal(write.fragments.length,2);
 assert.equal(write.fragments[0].semantic_tags.authoring_v1.parent,null);
 assert.equal(write.fragments[1].semantic_tags.authoring_v1.parent,"story-section");
 assert.equal(write.fragments[1].semantic_tags.authoring_v1.editorial[0].data.text,"Original paragraph");
 assert.equal(write.fragments[1].semantic_tags.authoring_v1.responsive.phone.layout,"stack");
 assert.ok(write.fragments.every(x=>x.status==="draft"));
 const after={...b,id:12,snapshot:{...b.snapshot,page:{...write.page,id:12},fragments:write.fragments}};
 const restored=treeFromNativeSnapshot(after);
 assert.equal(flat(restored),flat(doc));
 assert.equal(restored.blocks[0].children[0].props.body,"A visible story");
 assert.deepEqual(restored.blocks[0].children[0].props.editorial,doc.blocks[0].children[0].props.editorial);
});
test("article metadata stays bound to the native draft",()=>{
 const b=base(),doc=story(),pageDraft={title:"Journal",heading:"Journal",editorial_kind:"article",media_subject_ref:"article:journal-one",
  author_credit:"Aurekai Editorial",author_subject_ref:"person:nick",editorial_published_on:"2026-11-07",editorial_updated_on:"2026-11-09"};
 const page=toNativePageWrite(b,doc,{pageDraft}).page;assert.equal(page.editorial_kind,"article");assert.equal(page.media_subject_ref,"article:journal-one");
 assert.equal(page.author_credit,"Aurekai Editorial");
 assert.equal(page.author_subject_ref,"person:nick");
 assert.equal(page.editorial_published_on,"2026-11-07");
 assert.equal(page.editorial_updated_on,"2026-11-09");
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,media_subject_ref:"article:../other"}}),/identity/);
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,media_subject_ref:""}}),/identity/);
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,author_subject_ref:"person:../admin"}}),/author identity/);
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,author_credit:""}}),/author profile/);
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,editorial_published_on:"2026-02-29"}}),/publication dates/);
 assert.throws(()=>toNativePageWrite(b,doc,{pageDraft:{...pageDraft,editorial_updated_on:"2026-11-06"}}),/publication dates/);
});
test("other native fragments remain byte-for-byte untouched in composite write",()=>{
 const b=base(),row={id:300,coordinate:"about-intro",page:"/stories",slot:"body",
  audience:"anonymous",kind:"copy",status:"published",heading:"Existing page",
  body:"Do not modify",source_ref:"source:first"};
 b.snapshot.fragments.push(row);
 const write=toNativePageWrite(b,story());
 const unrelated=write.fragments.find(x=>x.coordinate==="about-intro");
 assert.deepEqual(unrelated,Object.fromEntries(Object.entries(row).filter(([k])=>k!=="id")));
});
test("cross-account, stale shapes, and published page edits are rejected",()=>{
 assert.throws(()=>toNativePageWrite(base("person-other-work"),story()),/different site/);
 const b=base();b.digest="invalid";
 assert.throws(()=>toNativePageWrite(b,story()),/snapshot/);
 const p=base("person-test-work","/stories",12);p.snapshot.page.status="published";
 assert.throws(()=>toNativePageWrite(p,story()),/immutable/);
});
test("already published blocks cannot be edited, removed, reordered or republished by the draft writer",()=>{
 const doc=story(),b=base("person-test-work","/stories",12);
 const saved=toNativePageWrite(base(),doc);
 b.snapshot.fragments=saved.fragments.map((x,i)=>({...x,id:100+i,status:i===0?"published":"draft"}));
 const opened=treeFromNativeSnapshot(b);
 assert.equal(opened.blocks[0].props.readOnly,true);
 assert.equal(toNativePageWrite(b,opened).fragments[0].status,"published");
 assert.throws(()=>toNativePageWrite(b,applyAuthoringEdit(opened,{type:"remove",id:"story-section"})),/published block/);
 const tampered=structuredClone(opened);tampered.blocks[0].props.heading="Changed heading";
 assert.throws(()=>toNativePageWrite(b,tampered),/reviewed fork/);
});
test("native commit is reconciled by fresh readback, not asserted from a 200 response",async()=>{
 const b=base(),doc=story();let stored=b;let count=0;
 async function fetcher(path,opts){
  count++;assert.ok(path.includes("site=person-test-work"));
  if(opts?.method==="POST"){
   const req=JSON.parse(opts.body);
   assert.equal(req.expected_digest,digest);
   stored={schema:b.schema,id:1,digest:"b".repeat(64),
    snapshot:{namespace:b.snapshot.namespace,route:b.snapshot.route,
     page:{...req.page,id:1},fragments:req.fragments}};
   return {ok:true,json:async()=>({committed:true})};
  }
  assert.ok(path.startsWith("/cms/compose/page?route="));
  return {ok:true,json:async()=>structuredClone(stored)};
 }
 const result=await saveNativeAuthoring({fetcher,base:b,tree:doc,site:"person-test-work"});
 assert.equal(result.committed,true);
 assert.equal(result.revision,"b".repeat(64));
 assert.equal(flat(result.tree),flat(doc));
 assert.equal(count,2);
 const reopened=await openNativeAuthoring({fetcher,route:"/stories",site:"person-test-work"});
 assert.equal(reopened.tree.blocks.length,1);
});
test("CMS refuses false successful save when post-commit readback lost an authored section",async()=>{
 const b=base(),doc=story();
 const fetcher=async(path,opts)=>opts?.method==="POST"?
  {ok:true,json:async()=>({committed:true})}:
  {ok:true,json:async()=>structuredClone(b)};
 await assert.rejects(saveNativeAuthoring({fetcher,base:b,tree:doc}),/readback/);
});
