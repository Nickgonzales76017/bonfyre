import {test} from "node:test";
import assert from "node:assert/strict";
import {newAuthoringTree,validateAuthoringTree,applyAuthoringEdit,createAuthoringSession,
 toPuckData,fromPuckData,toGrapesProject,fromGrapesProject,toEditorBlocks,
 applyEditorBlocks,TREE_SCHEMA,assertAuthoringContinuity,authoringBasisDigest,applyAuthoringProposal} from "../runtime/authoring-tree.mjs";
const card=(id,heading)=>({id,kind:"profile-card",props:{heading,variant:"forest"},
 responsive:{desktop:{layout:"grid",columns:2},phone:{layout:"stack"}},children:[]});
const nested=()=>{
 const doc=newAuthoringTree("person-first-work");
 doc.blocks=[{id:"casework",kind:"profile-section",props:{heading:"Product stories"},
 responsive:{desktop:{layout:"grid",columns:3}},children:[
 card("first-project","Retained source"),card("second-project","Bound application")]}];
 return validateAuthoringTree(doc);
};
test("native-ready tree survives Puck slots and GrapesJS typed project round trips",()=>{
 const first=nested();
 assert.deepEqual(fromPuckData(toPuckData(first),first.site_id),first);
 assert.deepEqual(fromGrapesProject(toGrapesProject(first),first.site_id),first);
 assert.equal(first.schema,TREE_SCHEMA);
});
test("drag/drop and reorder preserve identity, copies and complete subtree",()=>{
 const source=nested();
 const added=applyAuthoringEdit(source,{type:"insert",parent:"casework",index:1,block:card("third-project","New")});
 assert.deepEqual(added.blocks[0].children.map(x=>x.id),["first-project","third-project","second-project"]);
 const moved=applyAuthoringEdit(added,{type:"move",id:"third-project",parent:null,index:1});
 assert.deepEqual(moved.blocks.map(x=>x.id),["casework","third-project"]);
 const dup=applyAuthoringEdit(moved,{type:"duplicate",id:"casework",newId:"casework-copy"});
 assert.deepEqual(dup.blocks.map(x=>x.id),["casework","casework-copy","third-project"]);
 assert.equal(dup.blocks[1].children[0].id,"casework-copy-1");
 assert.equal(dup.blocks[1].children[0].props.heading,"Retained source");
 assert.equal(applyAuthoringEdit(dup,{type:"remove",id:"casework"}).blocks.length,2);
 assert.equal(source.blocks.length,1,"no mutation of source");
});
test("responsive values, read-only fields and escaping boundary are enforced",()=>{
 const source=nested();
 assert.throws(()=>applyAuthoringEdit(source,{type:"style",id:"casework",viewport:"phone",value:{background:"url(javascript:evil)"}}),/invalid|forbidden/);
 assert.throws(()=>applyAuthoringEdit(source,{type:"style",id:"casework",viewport:"phone",value:{position:"absolute"}}),/arbitrary/);
 assert.throws(()=>applyAuthoringEdit(source,{type:"style",id:"casework",viewport:"phone",value:{columns:14}}),/invalid/);
 assert.throws(()=>applyAuthoringEdit(source,{type:"insert",parent:"first-project",block:card("casework","Duplicate")}),/duplicate/);
 assert.throws(()=>applyAuthoringEdit(source,{type:"move",id:"casework",parent:"first-project"}),/subtree/);
 assert.throws(()=>applyAuthoringEdit(source,{type:"update",id:"first-project",field:"href",value:"javascript:alert(1)"}),/unsafe URL/);
 const readOnly=applyAuthoringEdit(source,{type:"update",id:"first-project",field:"readOnly",value:true});
 assert.throws(()=>applyAuthoringEdit(readOnly,{type:"update",id:"first-project",field:"body",value:"override"}),/read-only/);
});
test("session supports durable independent undo redo and isolation",()=>{
 const start=nested();
 const a=createAuthoringSession(start),b=createAuthoringSession(start);
 let events=0;
 const close=a.subscribe(()=>events++);
 a.edit({type:"update",id:"first-project",field:"heading",value:"Changed"});
 assert.equal(b.snapshot().blocks[0].children[0].props.heading,"Retained source");
 assert.equal(a.snapshot().blocks[0].children[0].props.heading,"Changed");
 assert.equal(a.undo(),true);
 assert.equal(a.snapshot().blocks[0].children[0].props.heading,"Retained source");
 assert.equal(a.redo(),true);
 assert.equal(a.snapshot().blocks[0].children[0].props.heading,"Changed");
 close();
 assert.equal(events,3);
});
test("Editor.js article editing never destroys sibling blocks or layout",()=>{
 const doc=nested();
 const original=doc.blocks[0].children[0];
 const editor=toEditorBlocks({...original,props:{body:"First passage\n\nSecond passage"}});
 assert.equal(editor.blocks.length,2);
 const updated=applyEditorBlocks(doc,"first-project",{blocks:[
  {type:"paragraph",data:{text:"Edited introduction"}},
  {type:"header",data:{text:"Additional section"}}]});
 assert.equal(updated.blocks[0].children[0].props.body,"Edited introduction\n\nAdditional section");
 assert.deepEqual(updated.blocks[0].children[0].responsive,original.responsive);
 assert.equal(updated.blocks[0].children[1].props.heading,"Bound application");
 assert.throws(()=>applyEditorBlocks(doc,"first-project",{blocks:[{type:"raw",data:{html:"<script>"}}]}),/unregistered editorial/);
 assert.throws(()=>applyEditorBlocks(doc,"first-project",{blocks:[{type:"paragraph",data:{text:"<img src=x onerror=y>"}}]}),/sanitation/);
});
test("cross-account imports, arbitrary CSS or HTML bypasses are rejected",()=>{
 const doc=nested();
 assert.throws(()=>fromGrapesProject({pages:[{frames:[{component:{components:[{type:"div",content:"unsafe"}]}}]}]},"person-other-work"),/arbitrary HTML/);
 assert.throws(()=>validateAuthoringTree({...doc,site_id:"other/../../../"}),/site ID/);
 assert.throws(()=>validateAuthoringTree({...doc,blocks:[{id:"script",kind:"javascript",props:{body:"alert(1)"},children:[]}]}),/unregistered block/);
 assert.throws(()=>validateAuthoringTree({...doc,blocks:[{id:"bad",kind:"profile-card",props:{style:"<style>"},children:[]}]}),/unregistered prop/);
});

test("semantic bindings survive all editor modes and cannot be silently retargeted",()=>{
 const doc=nested();
 const block=doc.blocks[0].children[0];
 block.props.source_ref="frappe:crm:Lead";
 block.props.source_digest="a".repeat(64);
 block.props.bindings={owner:"frappe",program_id:"frappe:crm:Lead",field:"lead_name"};
 const session=createAuthoringSession(doc);
 assert.deepEqual(fromPuckData(toPuckData(doc),doc.site_id),doc);
 assert.deepEqual(fromGrapesProject(toGrapesProject(doc),doc.site_id),doc);
 const styled=applyAuthoringEdit(doc,{type:"style",id:block.id,viewport:"phone",value:{layout:"stack"}});
 session.replace(styled);
 for(const change of [
  {source_ref:"frappe:crm:Other"},{source_digest:"b".repeat(64)},
  {bindings:{owner:"frappe",program_id:"frappe:crm:Other",field:"lead_name"}},
  {readOnly:false}]){
  const forged=structuredClone(styled);
  Object.assign(forged.blocks[0].children[0].props,change);
  assert.throws(()=>session.replace(forged),/binding cannot be edited/);
 }
 const forged=structuredClone(styled);
 forged.blocks[0].children[0].kind="ui:card";
 assert.throws(()=>assertAuthoringContinuity(styled,forged),/changed kind/);
 assert.throws(()=>applyAuthoringEdit(styled,{type:"update",id:block.id,field:"source_ref",value:"other"}),/binding cannot be edited/);
});

test("computed or human proposals enter one revision-checked typed edit path, never a new authority",()=>{
 const base=nested();
 base.blocks[0].children[0].props.source_ref="frappe:crm:CRM Lead";
 base.blocks[0].children[0].props.source_digest="a".repeat(64);
 base.blocks[0].children[0].props.bindings={owner:"BonfyreFrappeCompiler",field:"lead_name"};
 base.blocks[0].children[0].props.readOnly=true;
 validateAuthoringTree(base);
 const original=structuredClone(base);
 const candidate={site_id:base.site_id,page:base.page,
  expected_digest:authoringBasisDigest(base),operations:[
  {type:"style",id:"first-project",viewport:"phone",value:{layout:"stack",width:"wide"}},
  {type:"insert",block:card("generated-summary","Candidate presentation")}
 ]};
 const next=applyAuthoringProposal(base,candidate);
 assert.deepEqual(base,original,"candidate cannot mutate its basis");
 assert.equal(next.blocks[0].children[0].responsive.phone.width,"wide");
 assert.equal(next.blocks.at(-1).props.heading,"Candidate presentation");
 const session=createAuthoringSession(base);
 session.replace(next);
 assert.equal(session.undo(),true);
 assert.deepEqual(session.snapshot(),base,"one proposal is one undoable revision");
 assert.equal(session.redo(),true);
 assert.deepEqual(session.snapshot(),next);
 assert.throws(()=>applyAuthoringProposal(next,candidate),/stale proposal/);
 for(const operation of [
  {type:"remove",id:"first-project"},
  {type:"update",id:"first-project",field:"source_ref",value:"foreign:changed"},
  {type:"insert",block:{...card("forged","Forged"),props:{
   heading:"Forged",bindings:{owner:"unadmitted"}}}},
  {type:"duplicate",id:"first-project",newId:"fabricated-binding"}
 ]){
  assert.throws(()=>applyAuthoringProposal(base,{...candidate,operations:[operation]}),
   /source-bound|binding cannot be edited|fabricate source identity/);
 }
 assert.throws(()=>applyAuthoringProposal(base,{...candidate,site_id:"other"}),/proposal does not address/);
 assert.throws(()=>applyAuthoringProposal(base,{...candidate,operations:[]}),/out of bounds/);
 assert.throws(()=>applyAuthoringProposal(base,{...candidate,operations:[{type:"execute",cmd:"publish"}]}),/unknown edit/);
});
