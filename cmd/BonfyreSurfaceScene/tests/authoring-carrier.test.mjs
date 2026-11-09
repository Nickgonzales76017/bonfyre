import {test} from "node:test";
import assert from "node:assert/strict";
import {encodeNativeAuthoring,decodeNativeAuthoring,AUTHORING_CARRIER_MAGIC} from "../runtime/authoring-carrier.mjs";
import {toPuckData,fromPuckData,toGrapesProject,fromGrapesProject} from "../runtime/authoring-tree.mjs";
import {journalScene,profileScene} from "./fixtures/authoring-publication.mjs";

test("complete Journal source, editorial, bindings, media and responsive data survive the native wire",()=>{
 const doc=journalScene(),bytes=encodeNativeAuthoring(doc);
 assert.ok(bytes instanceof Uint8Array);
 assert.ok(bytes.length>400);
 assert.ok(!new TextDecoder().decode(bytes).includes('"blocks"'),"native wire is not JSON output");
 assert.deepEqual(decodeNativeAuthoring(bytes),doc);
 assert.deepEqual(encodeNativeAuthoring(decodeNativeAuthoring(bytes)),bytes);
 const story=decodeNativeAuthoring(bytes).blocks[1].children[0];
 assert.deepEqual(story.props.editorial[4].data.content[2],["Hearing","Testimony"]);
 assert.deepEqual(story.props.bindings,{source:{type:"public-council",keys:["file","date"]}});
 assert.equal(story.props.source_digest,"a".repeat(64));
});
test("second unrelated profile uses same native contract, independent route and responsive layout",()=>{
 const d=profileScene(),bytes=encodeNativeAuthoring(d),decoded=decodeNativeAuthoring(bytes);
 assert.deepEqual(decoded,d);
 assert.equal(decoded.site_id,"person-example");
 assert.equal(decoded.blocks[1].children[1].props.metadata.years[1],2026);
 assert.equal(decoded.blocks[1].responsive.tablet.columns,2);
});
test("both upstream visual editors and native carrier reproduce identical full documents",()=>{
 for(const doc of [journalScene(),profileScene()]){
  assert.deepEqual(decodeNativeAuthoring(encodeNativeAuthoring(fromPuckData(toPuckData(doc),doc.site_id,doc.page))),doc);
  assert.deepEqual(decodeNativeAuthoring(encodeNativeAuthoring(fromGrapesProject(toGrapesProject(doc),doc.site_id,doc.page))),doc);
 }
});
test("wire format rejects truncation, tampered version, and noncanonical trailing data",()=>{
 const bytes=encodeNativeAuthoring(journalScene());
 assert.ok(new TextDecoder().decode(bytes.slice(0,AUTHORING_CARRIER_MAGIC.length)).startsWith("BFAU2"));
 assert.throws(()=>decodeNativeAuthoring(bytes.slice(0,bytes.length-3)),/truncated/);
 const magic=bytes.slice();magic[0]=1;
 assert.throws(()=>decodeNativeAuthoring(magic),/magic/);
 const trailing=new Uint8Array(bytes.length+1);trailing.set(bytes);
 assert.throws(()=>decodeNativeAuthoring(trailing),/trailing/);
});
test("wire does not accept cross-account source substitution",()=>{
 const doc=journalScene(),binary=encodeNativeAuthoring(doc);
 assert.notEqual(decodeNativeAuthoring(binary).site_id,profileScene().site_id);
});
