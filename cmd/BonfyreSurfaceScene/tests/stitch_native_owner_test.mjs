// End-to-end: isolated canonical CMS → real C SurfaceScene → Stitch presentation.
// This is native material, not a screenshot-derived identity or mocked scene.
import assert from "node:assert/strict";
import {mkdtempSync,rmSync,mkdirSync} from "node:fs";
import {join,resolve} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import {execFileSync} from "node:child_process";
import {composeStitchScene,composeStitchRuntime} from "../runtime/stitch-composition.mjs";

const root=resolve(fileURLToPath(new URL("../../..",import.meta.url)));
const scratch=mkdtempSync(join(tmpdir(),"bonfyre-stitch-native-")),db=join(scratch,"cms.db");
const schemas=join(root,"cmd/BonfyreCMS/content-types");
const cms=join(root,"cmd/BonfyreCMS/bonfyre-cms");
const sceneOwner=join(root,"cmd/BonfyreSurfaceScene/bonfyre-surface-scene");
const env={...process.env,BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_SCHEMAS:schemas,
  BONFYRE_SURFACE_SCENE_SCHEMA:join(schemas,"surface_scene.json"),
  BONFYRE_FACT_DELTAS:join(scratch,"deltas")};
mkdirSync(env.BONFYRE_FACT_DELTAS);
function run(bin,args) {
  try {
    return execFileSync(bin,args,{cwd:root,env,encoding:"utf8",timeout:20000,
      maxBuffer:4*1024*1024,stdio:["pipe","pipe","pipe"]});
  } catch(error) { throw new Error(bin+" "+args.slice(0,3).join(" ")+": "+String(error.stderr||error.message).slice(-2000)); }
}
const create=(type,record)=>run(cms,["entry","create",type,JSON.stringify(record),"--ns","root","--db",db,"--schemas",schemas]);
const compile=()=>JSON.parse(run(sceneOwner,["compile","--db",db,"--site","stitch-proof","--actor-scope","public"]));
try {
  run(cms,["schema","migrate","--db",db,"--schemas",schemas]);
  create("world",{world:"stitch-proof",title:"Proof World",character:"work",template:"ghost:night",
    lede:"Retained owner material",subject:"world:stitch-proof",sources:"source:proof",
    grammars:"records",cannot_infer:"No member, offer, or effect is inferred",status:"published"});
  create("site_presentation",{site_id:"stitch-proof",host:"stitch-proof.example.test",
    subject_address:"world:stitch-proof",profile:"ghost:night",territory:"work",
    navigation:["Records"],status:"published"});
  create("site_program",{program_id:"stitch-proof:v1",site_id:"stitch-proof",
    experience_programs:[],application_bindings:["stitch:records"],renderer_preferences:{},
    source_collections:["source:proof"],capability_programs:[],
    actor_presentations:{public:"public"},status:"published"});
  create("semantic_slot",{slot_id:"stitch:primary",site_id:"stitch-proof",slot_name:"primary",
    ordinal:1,extent:"page",accepts:["PublicationFeed"],min_material_quality:"observed",
    max_items:6,status:"published"});
  create("application_binding",{binding_id:"stitch:records",site_id:"stitch-proof",
    slot_id:"stitch:primary",application_kind:"PublicationFeed",
    material_query:{kind:"PublicationOccurrence"},source_collection:"source:proof",
    action_aperture:"read",continuity_key:"source:proof",status:"published"});
  const native=compile();
  assert.equal(native.schema,"bonfyre-surface-scene.v1");
  assert.equal(native.focus.subject_address,"world:stitch-proof");
  assert(native.slots.some(x=>x.slot_id==="stitch:primary"));
  assert(native.bindings.some(x=>x.binding_id==="stitch:records"));
  const preserved=structuredClone(native);
  for(const pattern of ["profile","organization","directory","detail","reader","connections","workbench","feed","market","spatial"]) {
    const composed=composeStitchScene(native,{pattern});
    for(const field of ["source_digest","focus","slots","fragments","bindings","stages",
      "patterns","operations","interaction","runtime_queries","dependencies"])
      assert.deepEqual(composed[field],native[field],"native "+field+" must survive "+pattern);
    assert.equal(composeStitchRuntime(native,{pattern}).sourceDigest,native.source_digest);
    assert.equal(composed.presentation.at(-1).pattern,pattern);
  }
  assert.deepEqual(native,preserved,"composition does not mutate native owner output");
  const worldId=run("sqlite3",[db,"SELECT id FROM world WHERE world='stitch-proof' AND namespace='root' LIMIT 1;"]).trim();
  assert.match(worldId,/^[0-9]+$/);
  run(cms,["entry","update","world",worldId,JSON.stringify({title:"Revised owner content"}),
    "--ns","root","--db",db,"--schemas",schemas]);
  const successor=compile();
  assert.notEqual(successor.source_digest,native.source_digest,"source changes must invalidate exact scene digest");
  assert.equal(composeStitchRuntime(successor,{pattern:"profile"}).sourceDigest,successor.source_digest);
  console.log("stitch_native_owner_test: PASS CMS → C SurfaceScene → 10 Aurekai compositions, owner truth, successor revision");
} finally {rmSync(scratch,{recursive:true,force:true});}
