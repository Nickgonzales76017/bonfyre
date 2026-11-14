import {test} from "node:test";
import assert from "node:assert/strict";
import {editStructuredPayload,insertShape,startingSectionPayload}
 from "../../../components/BonfyreServiceSite/authoring/structured-fields.mjs";
import {makePuckConfig}
 from "../../../components/BonfyreServiceSite/authoring/unified-editor.mjs";
import {SITE_SECTION_CATALOGUE}
 from "../../../components/BonfyreServiceSite/authoring/site-section-catalogue.generated.mjs";
import {newAuthoringTree,validateAuthoringTree,toPuckData,fromPuckData}
 from "../runtime/authoring-tree.mjs";
const original=()=>({title:"Selected projects",items:[
 {title:"Editorial",text:"First"},
 {title:"Commerce",text:"Second"}],image:{src:"/assets/approved-cover.webp",alt:"Cover"}});
test("all 27 first-party Puck components expose a real mutable structured field",()=>{
 const fakeReact={createElement:(type,props,...children)=>({type,props,children})};
 const config=makePuckConfig(fakeReact);
 for(const spec of SITE_SECTION_CATALOGUE){
  const block=config.components["service:"+spec.kind];
  assert.ok(block,"missing Puck component "+spec.kind);
  assert.equal(block.fields.payload.type,"custom","Puck must not flatten "+spec.kind);
  assert.ok(block.defaultProps.payload);
  assert.deepEqual(Object.keys(block.defaultProps.payload).sort(),[...spec.required].sort());
  const component=block.fields.payload.render({value:original(),onChange:()=>{}});
  assert.equal(typeof component.type,"function","real structured field mounted "+spec.kind);
 }
 assert.equal(config.components["profile-card"].fields.payload,undefined);
});
test("Puck can edit every nested field without converting cards to text",()=>{
 let result=original();
 result=editStructuredPayload(result,{type:"set",path:["items",0,"text"],value:"Changed in Puck"});
 result=editStructuredPayload(result,{type:"insert",path:["items"],index:1,
  value:{title:"Systems",text:"Third source"}});
 result=editStructuredPayload(result,{type:"move",path:["items"],from:2,to:0});
 result=editStructuredPayload(result,{type:"set",path:["image","alt"],value:"Revised description"});
 assert.deepEqual(result.items.map(x=>x.title),["Commerce","Editorial","Systems"]);
 assert.equal(result.items[1].text,"Changed in Puck");
 assert.equal(result.image.alt,"Revised description");
 assert.equal(original().items[0].text,"First");
 result=editStructuredPayload(result,{type:"remove",path:["items",2]});
 assert.equal(result.items.length,2);
 result=editStructuredPayload(result,{type:"set",path:["image","focal"],value:{x:20,y:30}});
 assert.equal(result.image.focal.y,30);
});
test("nested edits refuse cross-level addressing and unsafe properties",()=>{
 assert.throws(()=>editStructuredPayload(original(),{type:"set",path:["items",9,"text"],value:"x"}),/array path missing/);
 assert.throws(()=>editStructuredPayload(original(),{type:"set",path:["__proto__"],value:"x"}),/invalid field address/);
 assert.throws(()=>editStructuredPayload(original(),{type:"set",path:["image","src"],value:"javascript:alert(1)"}),/executable|unsafe URL/);
 assert.throws(()=>editStructuredPayload(original(),{type:"insert",path:["image"],value:{}}),/array insert/);
 assert.throws(()=>editStructuredPayload(original(),{type:"move",path:["items"],from:30,to:0}),/reorder/);
 assert.throws(()=>editStructuredPayload(original(),{type:"set",path:[],value:{}}),/cannot replace/);
});
test("native authoring tree preserves structured edits through Puck",()=>{
 const doc=newAuthoringTree("person-designer-life");
 const cases=SITE_SECTION_CATALOGUE.find(x=>x.kind==="cases");
 assert.deepEqual(startingSectionPayload(cases),{title:"",items:[]});
 assert.deepEqual(insertShape("items"),{title:"",text:""});
 doc.blocks=[{id:"case-studies",kind:"service:cases",props:{
  heading:"Cases",payload:editStructuredPayload(original(),{type:"set",path:["items",1,"text"],value:"Saved"})},
  responsive:{},children:[]}];
 validateAuthoringTree(doc);
 const opened=fromPuckData(toPuckData(doc),doc.site_id);
 assert.deepEqual(opened,doc);
 assert.equal(opened.blocks[0].props.payload.items[1].text,"Saved");
});
