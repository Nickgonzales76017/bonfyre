import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,readFileSync,writeFileSync,rmSync,existsSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {spawnSync} from "node:child_process";
import {journalScene,profileScene} from "./fixtures/authoring-publication.mjs";
import {encodeNativeAuthoring} from "../runtime/authoring-carrier.mjs";
const root=resolve(import.meta.dirname,"..");
const temp=mkdtempSync(join(tmpdir(),"aurekai-native-authoring-"));
function execute(bin,args){
 const p=spawnSync(bin,args,{encoding:"utf8",timeout:25000});
 assert.notEqual(p.error?.code,"ETIMEDOUT");
 return p;
}
function build(){
 const exe=join(temp,"authoring-preview");
 const cc=execute(process.env.CC||"cc",["-std=c11","-Wall","-Wextra","-Werror",
  join(root,"tests/native_authoring_carrier_main.c"),"-o",exe]);
 assert.equal(cc.status,0,"native C compile failed "+cc.stderr);
 return exe;
}
try{
 const exe=build();
 for(const [name,doc] of [["journal",journalScene()],["profile",profileScene()]]){
  const wire=join(temp,name+".bfau"),output=join(temp,name+".html");
  writeFileSync(wire,encodeNativeAuthoring(doc));
  const result=execute(exe,[wire,output]);
  assert.equal(result.status,0,name+" native render failed "+result.stderr);
  const html=readFileSync(output,"utf8");
  assert.ok(html.startsWith("<!doctype html>"),"native HTML document");
  assert.ok(html.includes('data-release="draft-preview-only"'),"no false publication");
  assert.ok(html.includes('content="noindex,nofollow"'),"review-only");
  assert.ok(html.includes('data-site="'+doc.site_id+'"'),"account identity");
  assert.ok(html.includes('data-route="'+doc.page+'"'),"route identity");
  assert.ok(html.includes("data-layout-phone"),"mobile composition emitted");
  assert.ok(html.includes("@media(max-width:680px)"),"responsive CSS");
  assert.ok(!/<script\b/i.test(html),"cannot inject scripts");
  assert.ok(result.stderr.includes("AUTHORING_NATIVE_PREVIEW_PASS"),"native receipt");
  if(name==="journal"){
   for(const text of ["AUREKAI / JOURNAL","The public record is a process, not a file",
    "Source retained","The first distinction","A public record is not the whole event.",
    "Research primary evidence","Timestamp","Testimony","Media pending rights-approved"])
    assert.ok(html.includes(text),"missing native editorial presentation: "+text);
   assert.ok(html.includes("An investigation into testimony, dates, redaction &amp;amp; provenance."),
    "browser escaped untrusted source HTML, not run");
   assert.ok(html.indexOf('data-id="chapter"')<html.indexOf('data-id="footnote"'),"nested owner order");
   assert.ok(html.includes('data-asset-ref="media:council-001"'),"retained media identity");
  }else {
   assert.ok(html.includes("Design systems &amp; public software"),"escaped profile heading");
   assert.ok(html.includes('data-id="project-a"'),"profile components");
   assert.ok(html.includes('data-id="project-b"'),"different project preserved");
  }
 }
 const clean=encodeNativeAuthoring(journalScene());
 for(const [suffix,payload] of [["truncated",clean.subarray(0,clean.length-18)],
    ["spoofed",Uint8Array.from(clean, (x,i)=>i===0?0:x)]]){
  const wire=join(temp,suffix+".bfau"),html=join(temp,suffix+".html");
  writeFileSync(wire,payload);const p=execute(exe,[wire,html]);
  assert.equal(p.status,3,"malformed bytes should be rejected");
  assert.equal(existsSync(html),false,"no output on invalid input");
 }
 console.log("AUTHORING_NATIVE_RENDER_PASS two accounts, editorial/list/table, native C, responsive, escaping, malformed rejection");
}finally{rmSync(temp,{recursive:true,force:true});}
test("native publication program verified",()=>assert.ok(true));
