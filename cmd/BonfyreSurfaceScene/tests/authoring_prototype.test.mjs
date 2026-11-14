import {test} from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {compileAuthoringDraft,toCmsDrafts,AUTHORING_BLOCKS,nativeAuthoringHeader}
 from "../runtime/authoring-prototype.mjs";
const site_id="person-test-work";
const fields={heading:"Published <work> & media",body:"Actual story",label:"Read",href:"https://example.org/a",variant:"forest"};
const compile=(engine,data)=>compileAuthoringDraft({engine,data,site_id});
const puck={content:[{type:"Hero",props:{id:"work-hero",...fields}}]};
const grapes={pages:[{frames:[{component:{components:[{type:"aurekai:profile-hero",
 attributes:{"data-coordinate":"work-hero"},aurekai:fields}]}}]}]};
test("three editor surfaces converge on the same typed draft content",()=>{
 const p=compile("puck",puck),g=compile("grapesjs",grapes);
 assert.deepEqual(g,p);
 assert.equal(p.schema,"bonfyre.authoring.v1");
 assert.equal(p.fragments[0].status,"draft");
 assert.equal(p.fragments[0].kind,"profile-hero");
 const e=compile("editorjs",{blocks:[{id:"work-hero",type:"header",data:{text:"Published <work> & media"}},
  {id:"story-2",type:"paragraph",data:{text:"Actual story"}}]});
 assert.equal(e.fragments[0].heading,p.fragments[0].heading);
 assert.equal(e.fragments[1].kind,"profile-story");
 assert.equal(e.fragments[1].body,"Actual story");
});
test("CMS adapter retains explicit site, coordinate and draft status",()=>{
 const entries=toCmsDrafts(compile("puck",puck));
 assert.equal(entries.length,1);
 assert.equal(entries[0].namespace,site_id);
 assert.equal(entries[0].status,"draft");
 assert.equal(entries[0].position,0);
 assert.equal(entries[0].href,"https://example.org/a");
});
test("unknown HTML, scripts, and unpublished media cannot enter the authoring grammar",()=>{
 assert.throws(()=>compile("grapesjs",{pages:[{frames:[{component:{components:[{type:"text",content:"<script>alert(1)</script>"}]}}]}]}),/generic HTML/);
 assert.throws(()=>compile("puck",{content:[{type:"Image",props:{id:"unreviewed",src:"https://third.party/stock.jpg"}}]}),/unknown component/);
 assert.throws(()=>compile("puck",{content:[{type:"Hero",props:{id:"thing",...fields,href:"javascript:alert(1)"}}]}),/unsafe link/);
 assert.throws(()=>compile("puck",{content:[{type:"Hero",props:{id:"thing",...fields,href:"https://user:password@example.org/a"}}]}),/unsafe link/);
});
test("bounded input, coordinate uniqueness, isolated namespaces",()=>{
 assert.throws(()=>compile("puck",{content:[{type:"Hero",props:{id:"a",body:"a".repeat(5001)}}]}),/boundary/);
 assert.throws(()=>compile("puck",{content:[{type:"Hero",props:{id:"a"}},{type:"Card",props:{id:"a"}}]}),/duplicate/);
 assert.throws(()=>compileAuthoringDraft({engine:"puck",data:puck,site_id:"../../../other"}),/site identity/);
 assert.equal(compileAuthoringDraft({engine:"puck",data:puck,site_id:"person-other-work"}).site_id,"person-other-work");
});
test("generated native C contract derives exactly from the JS authoring grammar",()=>{
 assert.equal(AUTHORING_BLOCKS.length,7);
 const content=readFileSync(new URL("../../BonfyreWebAddress/generated/native_authoring_contract.h",import.meta.url),"utf8");
 assert.equal(content,nativeAuthoringHeader());
 assert.match(content,/bf_authoring_block_accepts/);
});
