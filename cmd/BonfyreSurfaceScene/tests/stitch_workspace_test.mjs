import assert from "node:assert/strict";
import {composeStitchWorkspace} from "../runtime/stitch-workspace.mjs";
import {STITCH_PATTERNS,STITCH_REFERENCES} from "../runtime/stitch-grammar.mjs";
const makeScene=() => ({
  schema:"bonfyre-surface-scene.v1",source_digest:"sha256:workspace",
  focus:{subject_address:"institution:example",actor_scope:"institution"},
  slots:[{slot_id:"app",extent:"workspace",accepts:["Editor"],max_items:2}],
  fragments:[],patterns:[],stages:[],dependencies:[],
  bindings:[
    {binding_id:"editor-a",slot_id:"app",application_kind:"Editor",action_aperture:"open"},
    {binding_id:"editor-b",slot_id:"app",application_kind:"Editor",action_aperture:""}
  ],
  interaction:{desktop_posture:{}},presentation:[]
});
const adapter=(label,commands=[])=>({kind:"Editor",label,element:()=>null,commands});
const real=new Map([
  ["editor-a",adapter("Editor A",[{id:"open",label:"Open",run:()=>{},enabled:()=>true}])],
  ["editor-b",adapter("Editor B")]
]);
const original=makeScene(),copy=structuredClone(original);
const compiled=composeStitchWorkspace(original,real,{pattern:"organization"});
assert.deepEqual(original,copy,"do not mutate native Scene or its installed apps");
assert.equal(compiled.panels.length,2);
assert.equal(compiled.layouts.length,3);
assert.equal(compiled.initial,"aurekai-desk");
assert.equal(compiled.commands.size,1);
assert.equal(compiled.commands.has("open"),true);
assert.throws(()=>composeStitchWorkspace(original,new Map(),{pattern:"organization"}),/executable binding/);
assert.throws(()=>composeStitchWorkspace({...original,bindings:[]},real,{pattern:"organization"}),/No installed/);
const spatial=composeStitchWorkspace(original,real,{pattern:"spatial"});
assert.equal(spatial.initial,"aurekai-comparison");
assert.equal(spatial.panels.length,2);
assert.equal(spatial.commands.size,1);
for(const [reference,pattern] of Object.entries(STITCH_REFERENCES)){
  const plan=composeStitchWorkspace(original,real,{reference});
  assert.equal(plan.panels.length,2,"retained app binds: "+reference);
  assert.equal(plan.commands.size,1,"no invented commands: "+reference);
  assert.equal(plan.revision,original.source_digest,"owner revision: "+reference);
  assert.equal(plan.initial,
    ["reader","feed","device","detail"].includes(pattern)?"aurekai-focus":
    ["directory","discover","market","media","spatial"].includes(pattern)?"aurekai-comparison":"aurekai-desk");
}
assert.equal(Object.keys(STITCH_REFERENCES).length,91);
assert.equal(Object.keys(STITCH_PATTERNS).length,14);
assert.throws(()=>composeStitchWorkspace(original,real,{reference:"unknown"}),RangeError);
const withOriginalLayouts=makeScene();
withOriginalLayouts.interaction.desktop_posture={layouts:[{id:"owner",panels:[{id:"editor-a"},{id:"editor-b",direction:"below",relative:"editor-a"}]}],initial_layout:"owner"};
assert.equal(composeStitchWorkspace(withOriginalLayouts,real,{pattern:"workbench"}).initial,"owner","never override an owner-declared workspace layout");
console.log("Stitch installed-workspace composition: native bindings, commands and owner layouts PASS");
