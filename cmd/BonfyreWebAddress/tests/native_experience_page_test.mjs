// Executable native PageComposition -> inert SurfaceScene -> shared experience
// contract. Input is the server-rendered page from an isolated retained source.
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {compileSceneExperience} from "../../BonfyreSurfaceScene/runtime/experience-programs.mjs";

const file=process.argv[2],pattern=process.argv[3];
if(!file||!pattern)throw Error("Usage: native_experience_page_test.mjs <page.html> <experience-pattern>");
const html=readFileSync(file,"utf8");
const template=html.match(/<template id="bonfyre-surface-scene"[^>]*>([\s\S]*?)<\/template>/);
assert(template,"The owner must emit its exact inert native SurfaceScene");
const native=JSON.parse(template[1]);
const experience=compileSceneExperience(native,{pattern});
assert.equal(native.schema,"bonfyre-surface-scene.v1");
assert.equal(experience.sourceDigest,native.source_digest);
assert.equal(experience.subject,native.focus.subject_address);
assert.equal(experience.effectAuthority,"native-owner-only");
assert.deepEqual(native.bindings,[],"Public page compatibility does not invent installed apps");
assert.deepEqual(native.operations.apertures,[],"Layout does not mint command authority");
const placeholders=experience.regions.filter(x=>x.kind==="semantic_slot"&&x.materialization==="declared");
assert(placeholders.length>=1,"The unbound page slot stays declared, not installed");
assert(!Object.values(experience.zones).flat().some(id=>placeholders.some(x=>x.id===id)),
  "A declared-only slot cannot appear as a materialized pane");
const sections=[...html.matchAll(/<section\b[^>]*data-bonfyre-region="([^"]+)"[^>]*>/g)];
const fragments=experience.regions.filter(x=>x.kind==="world_fragment");
assert(fragments.length>=1,"Expected published native material in the PageComposition witness");
assert.equal(sections.length,fragments.length,"Page sections and native scene projections must agree");
assert.equal(new Set(sections.map(x=>x[1])).size,sections.length,"Each region needs a distinct coordinate");
for(const match of sections){
  const fragment=fragments.find(x=>x.id===match[1]);
  assert(fragment,"DOM region omitted from the native scene: "+match[1]);
  assert.equal(fragment.materialization,"server-rendered",
    "A generic page scene must not infer an unproved source-projection contract");
  assert(match[0].includes('data-exp-lane="'+fragment.zone+'"'),
    "Shared C layout and JS scene compiler disagree for "+fragment.role);
  assert(match[0].includes('data-exp-region-index="'),"Native region index must be retained");
}
assert(html.includes('data-ak-experience="'+pattern+'"'),"The named native experience must render");
assert(/href="\/assets\/native-experience\.css(?:\?[^\"]*)?"/.test(html),"The experience stylesheet must be linked");
assert(!html.includes('id="aurekai-native-experience"'),"Experience styles must not be emitted inline");
assert(!html.includes("Google Stitch placeholder"),"No design donor data was injected");
console.log("Native page experience: PASS "+pattern+" / "+fragments.length+" owner regions, exact C/Scene placement, zero fake installed panes or actions");
