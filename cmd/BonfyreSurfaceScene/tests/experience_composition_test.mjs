import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {EXPERIENCE_PROGRAMS,EXPERIENCE_APEX_ROUTES,experienceZone,compileSceneExperience} from "../runtime/experience-programs.mjs";
import {STITCH_PATTERNS,STITCH_REFERENCES} from "../runtime/stitch-grammar.mjs";
import {composeStitchScene} from "../runtime/stitch-composition.mjs";

const page=readFileSync(new URL("../../BonfyreWebAddress/generated/native_page_composition.h",import.meta.url),"utf8");
const native=readFileSync(new URL("../../BonfyreWebAddress/generated/native_experience_composition.h",import.meta.url),"utf8");
const roles=[...page.matchAll(/^\s*\{"([a-z][a-z0-9_-]*)","[^"]*",BF_PAGE_MATERIAL_/gm)].map(x=>x[1]);
const compiled=[...native.matchAll(/^\s*\{"([a-z]+)","([^"]+)",([0-3]), \{([^}]+)\}\},/gm)];
assert.equal(roles.length,22);
assert.equal(compiled.length,14);
assert.equal(Object.keys(EXPERIENCE_PROGRAMS).length,14);
for(const row of compiled){
  const [,pattern,shell,bits,lane]=row;
  assert.equal(shell,STITCH_PATTERNS[pattern].shell);
  const expected=roles.map(role=>({hero:"BF_EXP_HERO",rail:"BF_EXP_RAIL",wide:"BF_EXP_WIDE",primary:"BF_EXP_PRIMARY"})[experienceZone(pattern,role)]);
  assert.deepEqual(lane.split(",").map(x=>x.trim()),expected,"One recipe must compile identically into native C: "+pattern);
  const views=EXPERIENCE_PROGRAMS[pattern].views;
  assert.equal(Number(bits),(views.includes("focus")?1:0)|(views.includes("compare")?2:0));
}
const nativeRoutes=readFileSync(new URL("../../BonfyreWebAddress/generated/native_experience_routes.h",import.meta.url),"utf8");
let totalRoutes=0;
for(const [apex,declared] of Object.entries(EXPERIENCE_APEX_ROUTES)){
  const block=nativeRoutes.match(new RegExp("static const BfStitchWorkRoute bf_stitch_"+apex+"_routes\\[\\] = \\{([\\s\\S]*?)\\};"));
  assert(block,"Native route table missing "+apex);
  const resolved=[...block[1].matchAll(/^\s*\{"([^"]+)","([^"]+)"\},?$/gm)].map(x=>[x[1],x[2]]);
  assert.deepEqual(resolved,declared,"Apex routes must share the executable experience registry: "+apex);
  totalRoutes+=resolved.length;
}
assert.equal(totalRoutes,86);
const scene={
  schema:"bonfyre-surface-scene.v1",source_digest:"sha256:retained-snapshot",
  focus:{subject_address:"community:example",actor_scope:"institution"},
  slots:[{slot_id:"member-app",role:"channels",extent:"workspace",accepts:["CRM"],max_items:3}],
  fragments:[{fragment_id:"screening",role:"watch",surface_grammar:"media",
    renderer_family:"watch",extent:"section",source_projection:"/native/exact-screening",
    continuity_group:"published-video",action_aperture:"inspect"}],
  bindings:[{binding_id:"installed-crm",slot_id:"member-app",application_kind:"CRM",action_aperture:"inspect"}],
  operations:{apertures:["inspect"],object_contracts:[]},
  presentation:[]
};
const baseline=structuredClone(scene);
const plan=compileSceneExperience(scene,{pattern:"media"});
assert.deepEqual(scene,baseline);
assert.equal(plan.schema,"aurekai-experience-program.v1");
assert.equal(plan.sourceDigest,scene.source_digest);
assert.equal(plan.subject,scene.focus.subject_address);
assert.deepEqual(plan.zones.hero,["screening"]);
assert.deepEqual(plan.zones.rail,["member-app"]);
assert.deepEqual(plan.zones.primary,[]);
assert.deepEqual(plan.bindingIds,["installed-crm"]);
assert.equal(plan.regions[1].sourceProjection,"/native/exact-screening");
assert.equal(plan.regions.some(x=>x.apertures?.includes("inspect")),false,"The layout cannot authorize an effect");
const decorated=composeStitchScene(scene,{pattern:"media"});
assert.deepEqual(decorated.presentation.at(-1).experience_zones,plan.zones);
assert.deepEqual(decorated.presentation.at(-1).experience_modes,plan.modes);
assert.equal(decorated.source_digest,scene.source_digest);
assert.deepEqual(decorated.operations,scene.operations);
for(const reference of Object.keys(STITCH_REFERENCES)){
  const experience=compileSceneExperience(scene,{reference});
  assert.equal(experience.pattern,STITCH_REFERENCES[reference]);
  assert.equal(experience.regions.length,2,"A reference cannot make additional material");
}
assert.throws(()=>compileSceneExperience({...scene,source_digest:""},{pattern:"media"}),TypeError);
assert.throws(()=>compileSceneExperience({...scene,fragments:[{fragment_id:"member-app"}]},{pattern:"media"}),/repeated/);
assert.throws(()=>compileSceneExperience(scene,{pattern:"not-a-pattern"}),RangeError);
assert.throws(()=>compileSceneExperience(scene,{reference:"unverified-reference"}),RangeError);
console.log("Native experience IR: 14 reusable programs, 91 reference resolutions, exact C parity, unchanged material, actors and effects PASS");
