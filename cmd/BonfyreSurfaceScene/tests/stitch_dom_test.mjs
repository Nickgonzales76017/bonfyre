import assert from "node:assert/strict";
import {mountStitchPresentation} from "../runtime/stitch-dom-bridge.mjs";

function fixture(id){
  return {schema:"bonfyre-surface-scene.v1",source_digest:"sha256:"+id,
    focus:{subject_address:"subject:"+id,actor_scope:"public"},
    slots:[{slot_id:"main",extent:"page",accepts:["PublicationFeed"],max_items:4}],
    fragments:[],bindings:[],stages:[],patterns:[],dependencies:["cms:"+id],
    operations:{apertures:[],object_contracts:[],proposal_contracts:[]},
    interaction:{default_lens:"public",lens_order:["public"]},presentation:[]};
}
const native={dataset:{},style:{values:{},setProperty(k,v){this.values[k]=v;}}};
const existingRegion={dataset:{},getAttribute(k){return k==="data-bonfyre-region"?"main":null;}};
const inventedRegion={dataset:{},getAttribute(k){return k==="data-bonfyre-region"?"invented":null;}};
inventedRegion.dataset.aurekaiNativeRegion="old";
const root={
  documentElement:native,querySelectorAll(){return [existingRegion,inventedRegion];},
  addEventListener(){},defaultView:{addEventListener(){}}
};
const created=mountStitchPresentation(root,fixture("one"),{pattern:"organization"});
assert.equal(native.dataset.aurekaiPattern,"organization");
assert.equal(native.dataset.aurekaiShell,"identity");
assert.equal(native.style.values["--aurekai-accent"],"#d0fc4f");
assert.equal(existingRegion.dataset.aurekaiNativeRegion,"main");
assert.equal(inventedRegion.dataset.aurekaiNativeRegion,undefined);
assert.equal(existingRegion.dataset.aurekaiZone,"primary");
assert.equal(existingRegion.dataset.aurekaiMaterialization,"declared","uninstalled native slot must not claim an installed app");
assert.equal(inventedRegion.dataset.aurekaiZone,undefined);
assert.equal(inventedRegion.dataset.aurekaiMaterialization,undefined);
assert.equal(created.model.sourceDigest,"sha256:one");
assert.equal(created.model.regions.length,1);
assert.equal(created.model.operations.apertures.length,0,"no synthetic authorizations");
created.update(fixture("two"));
assert.equal(created.model.sourceDigest,"sha256:two");
assert.equal(created.scene.focus.subject_address,"subject:two");
assert.equal(created.runtime.applyDelta({schema:"bonfyre-surface-delta.v1",
  before_digest:"sha256:two",after_digest:"sha256:three",scene:fixture("three")}),true);
assert.equal(created.model.sourceDigest,"sha256:three");
assert.equal(created.model.presentation.at(-1).pattern,"organization","Stitch survives native SurfaceDelta");
assert.equal(native.dataset.aurekaiPattern,"organization");
console.log("Stitch DOM bridge preserves owner records, no placeholder regions or commands: PASS");
