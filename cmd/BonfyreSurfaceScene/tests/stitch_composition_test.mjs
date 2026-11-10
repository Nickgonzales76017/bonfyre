import {readFileSync} from "node:fs";
import assert from "node:assert/strict";
import {STITCH_PATTERNS,STITCH_REFERENCES,STITCH_REFERENCE_COUNT} from "../runtime/stitch-grammar.mjs";
import {composeStitchScene,composeStitchRuntime} from "../runtime/stitch-composition.mjs";

assert.equal(STITCH_REFERENCE_COUNT,91);
assert.equal(Object.keys(STITCH_PATTERNS).length,14);
assert.equal(new Set(Object.keys(STITCH_REFERENCES)).size,91);
const inventory=readFileSync(new URL("../../../docs/engineering/stitch-aurekai-20261104/COVERAGE.tsv",import.meta.url),"utf8")
  .trimEnd().split("\n").slice(1).map(line=>line.split("\t"));
assert.equal(inventory.length,91,"all unique archived Stitch design names retained in delivery ledger");
assert.deepEqual(inventory.map(row=>row[0]).sort(),Object.keys(STITCH_REFERENCES).sort(),
  "the ledger must reconcile to the actual SurfaceScene 91-reference grammar");
for(const row of inventory){
  assert.equal(row.length,12,"complete ledger contract for "+row[0]);
  assert.notEqual(row[3],"unmapped","no design is permitted to lack a candidate native owner: "+row[0]);
  assert.notEqual(row[4],"unassigned","candidate owner required: "+row[0]);
}
const fixture = (id, material) => ({
  schema:"bonfyre-surface-scene.v1",source_digest:"sha256:" + id,
  focus:{subject_address:"subject:" + id,actor_scope:"public"},
  slots:[{slot_id:"main",extent:"page",accepts:["PublicationFeed"],max_items:5}],
  fragments:material ? [{fragment_id:"items",surface_grammar:"discover",renderer_family:"grid",
    renderer_variant:"cards",extent:"section",source_projection:"/native/" + id,
    action_aperture:"inspect"}] : [],
  bindings:[],stages:[],patterns:[],dependencies:["owner:" + id],
  operations:{apertures:["inspect"],object_contracts:[],proposal_contracts:[]},
  interaction:{default_lens:"discover",lens_order:["discover"]},
  presentation:[{kind:"native-owner",label:"retained"}]
});
for(const [reference,pattern] of Object.entries(STITCH_REFERENCES)){
  const before=fixture("one",true);
  const frozen=structuredClone(before);
  const after=composeStitchScene(before,{reference});
  assert.deepEqual(before,frozen,"input must remain unchanged");
  assert.equal(after.presentation.at(-1).pattern,pattern);
  assert.equal(after.source_digest,before.source_digest);
  for(const key of ["focus","slots","fragments","patterns","bindings","stages","dependencies","operations"])
    assert.deepEqual(after[key],before[key],"no ownership or authority change: " + key);
  assert.equal(after.presentation[0].kind,"native-owner");
}
const profile=composeStitchRuntime(fixture("alpha",true),{reference:"aurekai_people_credited_makers_sovereign_addresses"});
const other=composeStitchRuntime(fixture("beta",false),{pattern:"profile",look:"paper"});
assert.equal(profile.sourceDigest,"sha256:alpha");
assert.equal(other.sourceDigest,"sha256:beta");
assert.equal(profile.regions.length,2);
assert.equal(other.regions.length,1,"empty material may not create screenshot items");
assert.equal(profile.presentation.at(-1).shell,"identity");
assert.equal(other.presentation.at(-1).look,"paper");
assert.throws(()=>composeStitchScene(fixture("x",true),{reference:"unknown"}),RangeError);
assert.throws(()=>composeStitchScene(fixture("x",true),{pattern:"review",reference:"aurekai_discover"}),RangeError);
assert.throws(()=>composeStitchScene({...fixture("x",true),source_digest:""},{pattern:"profile"}),TypeError);
assert.throws(()=>composeStitchScene(fixture("x",true),{pattern:"new-authority"}),RangeError);
console.log("91 Stitch references -> 14 reusable Aurekai patterns; ownership-preserving native Scene projection PASS");
