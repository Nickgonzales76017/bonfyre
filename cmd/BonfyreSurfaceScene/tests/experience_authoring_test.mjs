import {test} from "node:test";
import assert from "node:assert/strict";
import {createHash} from "node:crypto";
import {authorFrappeExperience,authorSceneExperience,composeAuthoredExperience}
 from "../runtime/experience-programs.mjs";
import {newAuthoringTree,applyAuthoringEdit,createAuthoringSession,toPuckData,
 fromPuckData,toGrapesProject,fromGrapesProject}
 from "../runtime/authoring-tree.mjs";
import {renderAuthoredPattern} from "../../../components/BonfyreServiceSite/authored-pattern-renderer.mjs";
import {toNativePageWrite,treeFromNativeSnapshot}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";

const digest="a".repeat(64);
const field=(name,presentation,overrides={})=>({field:name,label:name,
 presentation,organ:"",required:false,read_only:false,in_list_view:false,
 reference:"",depends_on:"",withheld:"",...overrides});
const compiled={
 schema:"bonfyre.frappe-surface-programs.v1",app:"crm",programs:[{
 program_id:"frappe:crm:CRM Lead",doctype:"CRM Lead",module:"CRM",
 title_field:"lead_name",description:"CRM lead fields from the native compiler",
 record_posture:{submittable:false,child:false,single:false,tree:false,
  read_only:false,guest_readable:false},
 layout:[
  {tab:"Details",empty_in_metadata:false,sections:[
   {section:"Identity",collapsible:false,organ:"",columns:[{fields:[
    field("lead_name","text",{required:true}),
    field("annual_revenue","money"),
    field("contact","reference",{reference:"Contact",depends_on:"eval:doc.lead_name"})]}]},
   {section:"Owner",collapsible:true,organ:"aperture",columns:[{fields:[
    field("secret","text",{withheld:"a secret is never rendered, echoed or stored"}),
    field("send_message","stated-action",{withheld:"a donor controller cannot be invoked"})]}]}
  ]},
  {tab:"Person",empty_in_metadata:true,sections:[]}
 ],
 organs:["aperture"],counts:{fields:9,rendered:3,withheld:2,layout:4},
 cannot_infer:"a schema is not a record or an executable controller"
 }]};
const chosen=compiled.programs[0],page="/profiles/leads/";
const flattened=nodes=>nodes.flatMap(n=>[n,...flattened(n.children)]);

test("native Frappe DocType compiles as the SAME retained authoring tree, with all field declarations",()=>{
 const before=structuredClone(compiled);
 const tree=authorFrappeExperience({surface:compiled,program_id:chosen.program_id,
  site_id:"person-native-work",page});
 assert.deepEqual(compiled,before,"source owner bytes remain untouched");
 assert.equal(tree.schema,"bonfyre.authoring-tree.v1");
 assert.equal(tree.page,page);
 const rows=flattened(tree.blocks);
 assert.equal(rows.filter(x=>x.props.metadata?.sourceKind==="frappe-field").length,5);
 assert.equal(rows.filter(x=>x.props.metadata?.sourceKind==="frappe-tab").length,1,
  "the explicitly empty donor tab is retained as data, not rendered as dead UI");
 assert.deepEqual(tree.blocks[0].props.payload.empty_tabs,[{index:1,label:"Person"}]);
 assert.equal(rows.some(x=>x.kind==="ui:action"||x.kind==="ui:form"),false,
  "a donor button never becomes an executable browser form");
 const source=rows.find(x=>x.props.bindings.field==="annual_revenue");
 assert.equal(source.props.metadata.field.presentation,"money");
 assert.equal(source.kind,"ui:field");
 assert.equal(source.props.payload.type,"number");
 assert.equal(source.props.payload.disabled,true);
 const preview=renderAuthoredPattern(source);
 assert.match(preview,/type="number"/);
 assert.match(preview,/ disabled/);
 assert.doesNotMatch(preview,/<form|type="submit"/);
 assert.equal(source.props.source_ref,"frappe:crm:CRM Lead");
 const expected=createHash("sha256").update(JSON.stringify({app:"crm",program:chosen})).digest("hex");
 assert.equal(source.props.source_digest,expected);
 const hidden=rows.find(x=>x.props.bindings.field==="secret");
 assert.equal(hidden.kind,"ui:alert");
 assert.match(hidden.props.payload.body,/secret is never rendered/);
 assert(!JSON.stringify(tree).includes("<input type=\"password\""));
 for(const converted of [fromPuckData(toPuckData(tree),tree.site_id,page),
   fromGrapesProject(toGrapesProject(tree),tree.site_id,page)])
  assert.deepEqual(converted,tree,"all editor modes share the exact compiled field layout");
});

test("complete typed Frappe composition survives native CMS revision without losing field meaning",()=>{
 const tree=authorFrappeExperience({surface:compiled,program_id:chosen.program_id,
  site_id:"person-native-work",page});
 const before={schema:"bonfyre.page-snapshot.v1",id:0,digest,
  snapshot:{namespace:tree.site_id,route:page,page:null,fragments:[]}};
 const write=toNativePageWrite(before,tree,{pageDraft:{title:"CRM Lead",heading:"CRM Lead"}});
 assert.equal(write.page.status,"draft");
 assert(write.fragments.every(f=>f.status==="draft"));
 const saved={...before,id:4,snapshot:{...before.snapshot,page:{...write.page,id:4},
  fragments:write.fragments}};
 const reloaded=treeFromNativeSnapshot(saved);
 assert.deepEqual(reloaded,tree,"source bindings, declared fields, regions and layout round-trip");
 const fieldRow=reloaded.blocks[0].children[0].children[0].children[0].children[0];
 assert.equal(fieldRow.props.readOnly,true);
 const moved=applyAuthoringEdit(reloaded,{type:"style",id:fieldRow.id,viewport:"phone",
  value:{layout:"stack",background:"muted"}});
 assert.equal(moved.blocks[0].children[0].children[0].children[0].children[0]
  .responsive.phone.background,"muted");
 const forged=structuredClone(reloaded);
 forged.blocks[0].children[0].children[0].children[0].children[0]
  .props.bindings.field="foreign_controller";
 assert.throws(()=>createAuthoringSession(reloaded).replace(forged),/binding cannot be edited/);
});

test("a native Stitch Scene composes only materialized regions while preserving owner addresses",()=>{
 const scene={schema:"bonfyre-surface-scene.v1",source_digest:"sha256:"+digest,
  focus:{subject_address:"person:maker",actor_scope:"self"},
  slots:[
   {slot_id:"timeline",role:"feed",extent:"page",continuity_key:"stories"},
   {slot_id:"uninstalled",role:"places",extent:"page"}
  ],fragments:[{fragment_id:"publication",role:"lead",extent:"section",
   renderer_family:"feed",source_projection:"/native/publication"}],
  bindings:[{binding_id:"native:feed",slot_id:"timeline",application_kind:"PublicationFeed"}],
  operations:{apertures:["edit"]},dependencies:["source:journal"]};
 const copy=structuredClone(scene);
 const tree=authorSceneExperience({scene,site_id:"person-native-work",page,
  pattern:"profile",look:"paper"});
 assert.deepEqual(scene,copy,"native effect and source owners are not rewritten");
 assert.equal(tree.blocks[0].props.source_digest,digest);
 assert.equal(tree.blocks[0].props.metadata.pattern,"profile");
 const all=flattened(tree.blocks);
 assert.equal(all.some(n=>n.props.bindings.region_id==="uninstalled"),false);
 assert.equal(all.filter(n=>n.props.metadata?.sourceKind==="native-region").length,2);
 assert.equal(all.find(n=>n.props.bindings.region_id==="timeline")
  .props.bindings.binding_ids[0],"native:feed");
 assert.deepEqual(tree.blocks[0].props.payload.declared_regions,
  [{id:"uninstalled",role:"places",zone:"rail"}]);
 assert.equal(JSON.stringify(tree).includes("\"apertures\":["),false,
  "no owner operation gets copied into a template");
 for(const converted of [fromPuckData(toPuckData(tree),tree.site_id,page),
  fromGrapesProject(toGrapesProject(tree),tree.site_id,page)])
  assert.deepEqual(converted,tree);
 const edited=newAuthoringTree(tree.site_id,page);
 edited.blocks.push({id:"my-writing",kind:"profile-story",
  props:{heading:"Own work",body:"Personal section"},responsive:{},children:[]});
 const merged=composeAuthoredExperience(edited,tree);
 assert.equal(merged.blocks.length,2);
 assert.equal(merged.blocks[0].props.body,"Personal section");
 assert.throws(()=>composeAuthoredExperience(merged,tree),/duplicate or invalid block ID/);
});

test("uncertain or forged source closures are refused instead of dropping fields or granting effects",()=>{
 for(const forged of [
  {...compiled,programs:[]},
  {...compiled,programs:[chosen,chosen]},
  {...compiled,programs:[{...chosen,counts:{...chosen.counts,rendered:4}}]},
  {...compiled,programs:[{...chosen,layout:[{tab:"X",sections:[],empty_in_metadata:false}]}]}
 ]){
  assert.throws(()=>authorFrappeExperience({surface:forged,program_id:chosen.program_id,
   site_id:"person-native-work",page}));
 }
 const invalid={schema:"bonfyre-surface-scene.v1",source_digest:"sha256:short",
  focus:{subject_address:"person:maker"},slots:[],fragments:[],bindings:[]};
 assert.throws(()=>authorSceneExperience({scene:invalid,site_id:"person-native-work",
  page,pattern:"profile"}),/source digest/);
});
