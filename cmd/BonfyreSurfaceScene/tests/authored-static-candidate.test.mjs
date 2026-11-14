import {test} from "bun:test";
import assert from "node:assert/strict";
import {mkdtempSync,readFileSync,rmSync,existsSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {newAuthoringTree} from "../runtime/authoring-tree.mjs";
import {toNativePageWrite} from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {compileAuthoredCandidate,writeAuthoredCandidate}
 from "../../../components/BonfyreServiceSite/authored-static-candidate.mjs";
const rev="a".repeat(64),next="b".repeat(64);
function snapshot(namespace,route="/"){
 const doc=newAuthoringTree(namespace,route);
 doc.blocks=[
  {id:"first",kind:"profile-hero",props:{heading:"An actual account site",body:"CMS, source and publication evidence"},
   children:[]},
  {id:"work",kind:"service:cases",props:{heading:"Work",
   payload:{title:"Selected work",items:[{title:"Research platform",text:"A published tool"}]}},
   children:[]}
 ];
 const empty={schema:"bonfyre.page-snapshot.v1",id:0,digest:rev,
  snapshot:{namespace,route,page:null,fragments:[]}};
 const write=toNativePageWrite(empty,doc,{pageDraft:{title:"Portfolio",heading:"Portfolio"}});
 return {schema:empty.schema,id:19,digest:next,
  snapshot:{namespace,route,page:{...write.page,id:19},fragments:write.fragments}};
}
test("static candidate compiles the SAME CMS authoring tree and keeps release disabled",()=>{
 const a=snapshot("person-account-one-work");
 const out=compileAuthoredCandidate(a,{name:"Account One"});
 assert.equal(out.file,"index.html");
 assert.match(out.result.html,/Research platform/);
 assert.match(out.result.html,/data-authored-profile="person-account-one-work"/);
 assert.match(out.result.html,/noindex,nofollow/);
 assert.equal(out.source_revision,next);
});
test("two unrelated profiles get complete non-public HTML and CSS without source-code templates",()=>{
 const temp=mkdtempSync(join(tmpdir(),"bonfyre-candidate-witness-"));
 try{
  const first=writeAuthoredCandidate(snapshot("person-account-one-work"),
   {output:join(temp,"first"),name:"Account One"});
  const second=writeAuthoredCandidate(snapshot("person-account-two-life","/journal/"),
   {output:join(temp,"second"),name:"Account Two"});
  assert.equal(first.preview_only,true);
  assert.equal(first.site_id,"person-account-one-work");
  assert.equal(second.page,"/journal/");
  const html1=readFileSync(join(temp,"first","index.html"),"utf8");
  const html2=readFileSync(join(temp,"second","journal","index.html"),"utf8");
  const css1=readFileSync(join(temp,"first","assets","site.css"),"utf8");
  const css2=readFileSync(join(temp,"second","assets","site.css"),"utf8");
  const manifest=readFileSync(join(temp,"first","CANDIDATE.tsv"),"utf8");
  assert.match(html1,/href="assets\/site\.css"/);
  assert.match(html2,/href="\.\.\/assets\/site\.css"/);
  assert.ok(css1.length>250&&css2.length>250,"first-party Tailwind styles included");
  assert.match(manifest,/publication\tnoindex-preview-only/);
  assert.match(manifest,/static_assets\tstylesheet-only/);
  assert.match(manifest,/html_sha256\t[a-f0-9]{64}/);
  assert.ok(!html1.includes("Account Two"));
  assert.ok(!html2.includes("Account One"));
  assert.throws(()=>writeAuthoredCandidate(snapshot("person-account-one-work"),
   {output:join(temp,"first"),name:"Account One"}),/refusing to replace/);
 }finally{rmSync(temp,{force:true,recursive:true});}
},30000);
test("invalid data fails before writing an authorized-looking static release",()=>{
 const doc=snapshot("person-account-one-work");
 doc.snapshot.fragments[1].semantic_tags.authoring_v1.payload.items=[{title:"Unsafe",text:"<script>bad</script>"}];
 assert.throws(()=>compileAuthoredCandidate(doc,{name:"Account One"}),/executable/);
 const check=join(import.meta.dir,"../../../site");
 assert.equal(existsSync(check),true);
 assert.throws(()=>writeAuthoredCandidate(snapshot("person-account-one-work"),{output:check,name:"Account One"}),/canonical public site/);
});
