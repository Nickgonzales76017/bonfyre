import {test} from "node:test";
import assert from "node:assert/strict";
import {SITE_SECTION_CATALOGUE} from "../../../components/BonfyreServiceSite/authoring/site-section-catalogue.generated.mjs";
import {collectSectionCatalogue,compileSectionCatalogue} from "../tools/compile_authoring_sections.mjs";
import {readFileSync} from "node:fs";
import {newAuthoringTree,validateAuthoringTree,validateComponentData,
 toPuckData,fromPuckData,toGrapesProject,fromGrapesProject}
 from "../runtime/authoring-tree.mjs";
import {treeFromNativeSnapshot,toNativePageWrite}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
const sha="a".repeat(64);
function base(namespace){
 return {schema:"bonfyre.page-snapshot.v1",id:0,digest:sha,
  snapshot:{namespace,route:"/",page:null,fragments:[]}};
}
function sectionPayload(entry){
 const payload={source:{label:"Source-owned component",href:"https://example.org/source"},
  responsiveContent:{desktop:{lede:"Desktop description"},phone:{lede:"Phone description"}}};
 for(const key of entry.required){
  payload[key]=key==="entries"?[{id:"entry-one",title:"Sample",price:499}]:
   key==="stores"?[{id:"source-one",name:"Source",origin:"https://example.org"}]:
   key==="items"?[{title:"Retained result",text:"It remains editable"}]:
   key==="rows"?[{label:"Cost",value:499}]:
   key==="form"?{id:"contact-request",fields:[{label:"Question",type:"text"}]}:
   key==="catalogue"?[{id:"catalogue-one",name:"Reusable sample"}]:
   key==="sample"?{name:"Bound domain sample",owner:"account-work"}:
   "Authored "+key;
 }
 return payload;
}
function document(site){
 const doc=newAuthoringTree(site);
 doc.blocks.push({id:"all-components",kind:"profile-section",props:{heading:"Rich compositions"},responsive:{desktop:{layout:"grid",columns:3},phone:{layout:"stack"}},children:[]});
 for(let i=0;i<SITE_SECTION_CATALOGUE.length;i++){
  const spec=SITE_SECTION_CATALOGUE[i];
  doc.blocks[0].children.push({
   id:"component-"+spec.kind,kind:"service:"+spec.kind,
   props:{heading:spec.kind,variant:"editorial",payload:sectionPayload(spec)},
   responsive:{tablet:{layout:"split",columns:2}},children:[]});
 }
 return validateAuthoringTree(doc);
}
test("the deployed section registry compiles from 27 canonical section owners, no ghost kinds",()=>{
 const actual=collectSectionCatalogue();
 assert.equal(actual.length,27);
 assert.deepEqual(actual,SITE_SECTION_CATALOGUE);
 assert.deepEqual(JSON.parse(JSON.stringify(actual)),JSON.parse(JSON.stringify(SITE_SECTION_CATALOGUE)));
 const native=readFileSync(new URL("../../BonfyreWebAddress/generated/native_site_section_catalogue.h",import.meta.url),"utf8");
 const compiled=compileSectionCatalogue(actual);
 assert.equal(native,compiled.cText);
 assert.equal(readFileSync(new URL("../../../components/BonfyreServiceSite/authoring/site-section-catalogue.generated.mjs",import.meta.url),"utf8"),compiled.jsText);
 for(const type of ["catalogue","hero","planner","library","prose","workbook"])
  assert.ok(actual.find(x=>x.kind===type)?.required.length>0);
 for(const invalid of ["offer","training","source","media","documents"])
  assert.equal(actual.some(x=>x.kind===invalid),false,"undeclared component "+invalid);
});
test("all 27 real section kinds retain nested domain payloads through both visual editor projections",()=>{
 const original=document("person-publishing-work");
 const puck=fromPuckData(toPuckData(original),original.site_id);
 const grapes=fromGrapesProject(toGrapesProject(original),original.site_id);
 assert.deepEqual(puck,original);
 assert.deepEqual(grapes,original);
 const data=grapes.blocks[0].children.find(x=>x.kind==="service:catalogue").props.payload;
 assert.equal(data.entries[0].price,499);
 assert.equal(data.stores[0].origin,"https://example.org");
 assert.equal(data.responsiveContent.phone.lede,"Phone description");
});
test("native CMS persistence retains all 27 sections, their required fields, sources and responsive data",()=>{
 for(const namespace of ["person-publishing-work","person-unrelated-shop"]){
  const original=document(namespace),before=base(namespace);
  const request=toNativePageWrite(before,original,{pageDraft:{title:"Portfolio",heading:"Native portfolio"}});
  assert.equal(request.fragments.length,28);
  assert.ok(request.fragments.every(f=>f.status==="draft"));
  const snapshot={schema:before.schema,id:42,digest:"b".repeat(64),
   snapshot:{namespace,route:"/",page:{...request.page,id:42},fragments:request.fragments}};
  const recovered=treeFromNativeSnapshot(snapshot);
  assert.equal(recovered.site_id,namespace);
  assert.equal(recovered.blocks[0].children.length,27);
  for(const spec of SITE_SECTION_CATALOGUE){
   const actual=recovered.blocks[0].children.find(x=>x.kind==="service:"+spec.kind);
   const originalNode=original.blocks[0].children.find(x=>x.kind==="service:"+spec.kind);
   assert.deepEqual(actual.props.payload,originalNode.props.payload);
   assert.deepEqual(actual.responsive,originalNode.responsive);
   for(const key of spec.required)assert.ok(Object.hasOwn(actual.props.payload,key),
    spec.kind+" lost required field "+key);
  }
 }
});
test("complex section data refuses executable markup, invalid URLs and class injection",()=>{
 assert.throws(()=>validateComponentData({catalogue:{items:[{html:"<script>alert(1)</script>"}]}}),/unsafe section property|executable/);
 assert.throws(()=>validateComponentData({image:{src:"javascript:alert(1)"}}),/executable/);
 assert.throws(()=>validateComponentData({image:{src:"//untrusted.example/a"}}),/unsafe local/);
 assert.throws(()=>validateComponentData({body:{onClick:"alert(1)"}}),/unsafe section property/);
 assert.throws(()=>validateComponentData({list:[1,2,Number.POSITIVE_INFINITY]}),/nonfinite/);
 assert.doesNotThrow(()=>validateComponentData(sectionPayload(SITE_SECTION_CATALOGUE[0])));
});
test("changing a published rich component is not an unreviewed CMS draft write",()=>{
 const doc=document("person-publishing-work"),before=base(doc.site_id);
 const req=toNativePageWrite(before,doc);
 const snap={schema:before.schema,id:10,digest:"c".repeat(64),
  snapshot:{namespace:doc.site_id,route:"/",page:{...req.page,id:10},fragments:
   req.fragments.map(f=>({...f,status:"published"}))}};
 const original=treeFromNativeSnapshot(snap);
 assert.equal(original.blocks[0].props.readOnly,true);
 const mutation=structuredClone(original);
 mutation.blocks[0].children[0].props.payload.source.label="Changed without review";
 assert.throws(()=>toNativePageWrite(snap,mutation),/reviewed fork|published composition/);
});

test("compiled capability bindings and metadata survive the visual editors and native CMS",()=>{
 const site="person-compiled-application",original=document(site),before=base(site);
 const component=original.blocks[0].children[0];
 component.props.source_ref="frappe:crm:Lead";
 component.props.source_digest=sha;
 component.props.bindings={owner:"BonfyreFrappeCompiler",program_id:"frappe:crm:Lead",field:"lead_name"};
 component.props.metadata={doctype:"Lead",representation:"compiled-surface"};
 component.props.readOnly=true;
 const puck=fromPuckData(toPuckData(original),site);
 const grapes=fromGrapesProject(toGrapesProject(original),site);
 assert.deepEqual(puck,original);assert.deepEqual(grapes,original);
 const request=toNativePageWrite(before,original);
 const row=request.fragments.find(x=>x.coordinate===component.id);
 assert.equal(row.semantic_tags.authoring_v1.bindings.program_id,"frappe:crm:Lead");
 assert.deepEqual(row.semantic_tags.authoring_v1.metadata,component.props.metadata);
 const snap={schema:before.schema,id:44,digest:"b".repeat(64),snapshot:{
  namespace:site,route:"/",page:{...request.page,id:44},fragments:request.fragments}};
 const recovered=treeFromNativeSnapshot(snap);
 assert.deepEqual(recovered.blocks[0].children[0].props.bindings,component.props.bindings);
 assert.deepEqual(recovered.blocks[0].children[0].props.metadata,component.props.metadata);
 assert.equal(recovered.blocks[0].children[0].props.readOnly,true);
 const published={...snap,snapshot:{...snap.snapshot,fragments:snap.snapshot.fragments.map(x=>({
  ...x,status:x.coordinate===component.id?"published":x.status}))}};
 const reopened=treeFromNativeSnapshot(published);
 const forged=structuredClone(reopened);
 forged.blocks[0].children[0].props.bindings.program_id="frappe:crm:Other";
 assert.throws(()=>toNativePageWrite(published,forged),/published composition/);
});
