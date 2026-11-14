// Real donor -> native Frappe compiler -> one retained authoring tree.
// No controller execution, browser import or invented instance records.
import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,rmSync,readFileSync,mkdirSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {execFileSync} from "node:child_process";
import {authorFrappeExperience,authorSceneExperience}
 from "../runtime/experience-programs.mjs";
import {toPuckData,fromPuckData,toGrapesProject,fromGrapesProject,
 authoringBasisDigest,applyAuthoringProposal,createAuthoringSession}
 from "../runtime/authoring-tree.mjs";
import {toNativePageWrite,treeFromNativeSnapshot}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";

const root=resolve(fileURLToPath(new URL("../../..",import.meta.url)));
const frappe=join(root,"cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler");
const scene=join(root,"cmd/BonfyreSurfaceScene/bonfyre-surface-scene");
const cms=join(root,"cmd/BonfyreCMS/bonfyre-cms");
const schemas=join(root,"cmd/BonfyreCMS/content-types");
const fields=nodes=>nodes.flatMap(n=>[n,...fields(n.children)]);
const run=(bin,args,env={})=>execFileSync(bin,args,{cwd:root,
 env:{...process.env,...env},encoding:"utf8",timeout:120000,
 maxBuffer:16*1024*1024,stdio:["pipe","pipe","pipe"]});

test("installed CRM Lead source becomes one authored document through both editors and CMS",()=>{
 const tmp=mkdtempSync(join(tmpdir(),"bonfyre-experience-crm-"));
 try{
  const filename=join(tmp,"surface-programs.json");
  run(frappe,["--root",root,"--emit-surface-programs",filename,
   "--emit-surface-db",join(tmp,"surfaces.db"),"-o",join(tmp,"pack"),
   join(root,"integrations/frappe-bench/apps/crm")]);
  const surface=JSON.parse(readFileSync(filename,"utf8"));
  const lead=surface.programs.find(p=>p.doctype==="CRM Lead");
  assert(lead,"real CRM Lead DocType was not compiled");
  assert(lead.layout.length>=8,"real CRM Lead tab hierarchy was retained");
  const declared=lead.layout.flatMap(t=>t.sections.flatMap(s=>
   s.columns.flatMap(c=>c.fields)));
  assert(declared.length>25,"actual CRM Lead fields are used, not a toy fixture");
  const ns="person-compiled-crm",route="/crm/leads/";
  const doc=authorFrappeExperience({surface,program_id:lead.program_id,
   site_id:ns,page:route});
  const compiledFields=fields(doc.blocks).filter(n=>n.props.metadata?.sourceKind==="frappe-field");
  assert.equal(compiledFields.length,declared.length);
  assert(compiledFields.every(n=>n.props.source_ref===lead.program_id));
  const p=fromPuckData(toPuckData(doc),ns,route);
  const g=fromGrapesProject(toGrapesProject(doc),ns,route);
  assert.deepEqual(p,doc);assert.deepEqual(g,doc);
  const original={schema:"bonfyre.page-snapshot.v1",id:0,digest:"a".repeat(64),
   snapshot:{namespace:ns,route,page:null,fragments:[]}};
  const write=toNativePageWrite(original,doc,{pageDraft:{title:"CRM Lead",heading:"CRM Lead"}});
  assert.equal(write.fragments.length,fields(doc.blocks).length);
  const retained={...original,id:29,snapshot:{...original.snapshot,
   page:{...write.page,id:29},fragments:write.fragments}};
  assert.deepEqual(treeFromNativeSnapshot(retained),doc);
  assert(write.fragments.every(r=>r.status==="draft"));
  const bound=compiledFields.find(n=>n.props.metadata.field.field==="annual_revenue")||compiledFields[0];
  const candidate={site_id:ns,page:route,expected_digest:authoringBasisDigest(doc),
   operations:[{type:"style",id:bound.id,viewport:"phone",
    value:{layout:"stack",background:"muted"}}]};
  const updated=applyAuthoringProposal(doc,candidate);
  const session=createAuthoringSession(doc);
  session.replace(updated);
  assert.equal(session.undo(),true);
  assert.deepEqual(session.snapshot(),doc);
  assert.equal(session.redo(),true);
  const revised=toNativePageWrite(retained,session.snapshot());
  const stored={...retained,digest:"b".repeat(64),snapshot:{
   ...retained.snapshot,fragments:revised.fragments}};
  assert.deepEqual(treeFromNativeSnapshot(stored),updated);
  assert.throws(()=>applyAuthoringProposal(updated,candidate),/stale proposal/);
  assert.throws(()=>applyAuthoringProposal(doc,{...candidate,
   operations:[{type:"remove",id:bound.id}]}),/source-bound/);
  console.log("REAL CRM Lead: "+lead.layout.length+" tabs, "+
   declared.length+" fields, "+write.fragments.length+
   " native editable composition coordinates; editor/CMS continuity PASS");
 }finally{rmSync(tmp,{recursive:true,force:true});}
},120000);

test("real CMS -> native C Scene -> Stitch-aware authoring uses the same retained tree",()=>{
 const scratch=mkdtempSync(join(tmpdir(),"bonfyre-experience-scene-"));
 const db=join(scratch,"cms.db");
 try{
  mkdirSync(join(scratch,"deltas"));
  const env={BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_SCHEMAS:schemas,
   BONFYRE_SURFACE_SCENE_SCHEMA:join(schemas,"surface_scene.json"),
   BONFYRE_FACT_DELTAS:join(scratch,"deltas")};
  const c=(...args)=>run(cms,[...args,"--ns","root","--db",db,"--schemas",schemas],env);
  run(cms,["schema","migrate","--db",db,"--schemas",schemas],env);
  c("entry","create","world",JSON.stringify({world:"native-editor-proof",
   title:"A native authored space",character:"work",template:"ghost:night",
   lede:"First party material",subject:"world:native-editor-proof",
   sources:"source:proof",grammars:"records",
   cannot_infer:"An owner contract is not permission",status:"published"}));
  c("entry","create","site_presentation",JSON.stringify({site_id:"native-editor-proof",
   host:"native-editor-proof.example.test",subject_address:"world:native-editor-proof",
   profile:"ghost:night",territory:"work",navigation:["Records"],status:"published"}));
  c("entry","create","site_program",JSON.stringify({program_id:"native-editor-proof:v1",
   site_id:"native-editor-proof",experience_programs:[],
   application_bindings:["native:records"],renderer_preferences:{},
   source_collections:["source:proof"],capability_programs:[],
   actor_presentations:{public:"public"},status:"published"}));
  c("entry","create","semantic_slot",JSON.stringify({slot_id:"native:primary",
   site_id:"native-editor-proof",slot_name:"primary",ordinal:1,extent:"page",
   accepts:["PublicationFeed"],min_material_quality:"observed",max_items:6,status:"published"}));
  c("entry","create","application_binding",JSON.stringify({binding_id:"native:records",
   site_id:"native-editor-proof",slot_id:"native:primary",
   application_kind:"PublicationFeed",material_query:{kind:"PublicationOccurrence"},
   source_collection:"source:proof",action_aperture:"read",
   continuity_key:"source:proof",status:"published"}));
  const live=JSON.parse(run(scene,["compile","--db",db,
   "--site","native-editor-proof","--actor-scope","public"],env));
  assert.equal(live.schema,"bonfyre-surface-scene.v1");
  const doc=authorSceneExperience({scene:live,site_id:"native-editor-proof",
   page:"/",pattern:"profile",look:"paper"});
  const nodes=fields(doc.blocks);
  assert(nodes.some(n=>n.props.bindings.binding_ids?.includes("native:records")));
  const original={schema:"bonfyre.page-snapshot.v1",id:0,digest:"b".repeat(64),
   snapshot:{namespace:doc.site_id,route:doc.page,page:null,fragments:[]}};
  const write=toNativePageWrite(original,doc,{pageDraft:{title:"Native Space",heading:"Native Space"}});
  const stored={...original,id:70,snapshot:{...original.snapshot,
   page:{...write.page,id:70},fragments:write.fragments}};
  assert.deepEqual(treeFromNativeSnapshot(stored),doc);
  console.log("REAL C Scene: "+live.source_digest+
   " bound to native visual authoring/CMS without copying effects PASS");
 }finally{rmSync(scratch,{recursive:true,force:true});}
},120000);
