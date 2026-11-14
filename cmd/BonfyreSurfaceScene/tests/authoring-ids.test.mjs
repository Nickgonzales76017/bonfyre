import {test} from "node:test";
import assert from "node:assert/strict";
import {authoringCoordinate,fromGrapesProject,fromPuckData,newAuthoringTree,
 toGrapesProject,toPuckData} from "../runtime/authoring-tree.mjs";
test("new Puck blocks normalize upstream UUIDs into stable native CMS coordinates",()=>{
 const doc=newAuthoringTree("person-artist-live");
 const puck=toPuckData(doc);
 puck.content=[{type:"service:cases",props:{id:"CaseBlock-9A0B_UG!",
  heading:"Releases",payload:{title:"Releases",items:[]},children:[]}}];
 const first=fromPuckData(puck,doc.site_id),second=fromPuckData(puck,doc.site_id);
 assert.match(first.blocks[0].id,/^node-[a-f0-9]{16}$/);
 assert.deepEqual(first,second);
 assert.equal(first.blocks[0].props.payload.title,"Releases");
 assert.equal(authoringCoordinate("existing-owner-id"),"existing-owner-id");
});
test("GrapesJS new nested drop has a valid retained identity even before a save",()=>{
 const project=toGrapesProject(newAuthoringTree("person-builder-work"));
 project.pages[0].frames[0].component.components=[
  {type:"aurekai:profile-section",attributes:{"data-coordinate":"section-root"},aurekai:{heading:"Projects"},components:[
   {type:"aurekai:service:hero",attributes:{id:"gjs-t4xx"},aurekai:{
    heading:"Hero",payload:{headline:"A real hero"}},components:[]},
   {type:"aurekai:service:cases",attributes:{},aurekai:{
    heading:"Cases",payload:{title:"Projects",items:[]}},components:[]}
  ]}];
 const recovered=fromGrapesProject(project,"person-builder-work");
 assert.equal(recovered.blocks[0].children[0].id,"gjs-t4xx");
 assert.match(recovered.blocks[0].children[1].id,/^node-[a-f0-9]{16}$/);
 assert.deepEqual(fromGrapesProject(project,"person-builder-work"),recovered);
});
test("foreign Puck and Grapes projects cannot override the selected namespace or route",()=>{
 const origin=newAuthoringTree("person-a-work");
 const puck=toPuckData(origin),grapes=toGrapesProject(origin);
 assert.throws(()=>fromPuckData(puck,"person-b-shop"),/foreign/);
 assert.throws(()=>fromGrapesProject(grapes,"person-b-shop"),/foreign/);
 assert.throws(()=>fromGrapesProject(grapes,origin.site_id,"/outside"),/foreign/);
 assert.throws(()=>fromPuckData(puck,origin.site_id,"/outside"),/foreign/);
});
