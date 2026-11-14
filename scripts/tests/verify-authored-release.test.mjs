import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,readFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {gzipSync} from "node:zlib";
import {createHash} from "node:crypto";
import {writeFileSync} from "node:fs";
import {verifyAndStage} from "../verify-authored-release.mjs";
const hash=x=>createHash("sha256").update(x).digest("hex");
const release="b".repeat(64),revision="c".repeat(64),pageRevision="d".repeat(64);
const root=mkdtempSync(join(tmpdir(),"aurekai-release-test-"));
const makeHeader=(name,bytes,type="0")=>{
 const h=Buffer.alloc(512);
 const field=(text,start,size)=>h.write(text,start,Math.min(Buffer.byteLength(text),size-1),"utf8");
 const oct=(num,start,size)=>field(num.toString(8).padStart(size-1,"0"),start,size);
 field(name,0,100);oct(0o644,100,8);oct(0,108,8);oct(0,116,8);
 oct(bytes.length,124,12);oct(0,136,12);h.fill(32,148,156);
 field(type,156,2);field("ustar",257,6);field("00",263,3);
 let total=0;for(const b of h)total+=b;
 oct(total,148,8);return h;
};
function tar(entries){
 const buffers=[];
 for(const [path,value,type]of entries){
  const body=Buffer.from(value),header=makeHeader(path,body,type||"0");
  buffers.push(header,body,Buffer.alloc((512-body.length%512)%512));
 }
 buffers.push(Buffer.alloc(1024));return gzipSync(Buffer.concat(buffers));
}
function fixture({noindex=false,legacy=false,wrongDigest=false,missing=false,duplicate=false}={}){
 const html=(route)=>'<!doctype html><html data-authored-publication="'+release+'"'+
   ' data-authored-profile="person-test-work"><head><title>'+route+'</title>'+
   (noindex?'<meta name="robots" content="noindex">':"")+
   '</head><body><main><h1>Authored through the account editor</h1></main></body></html>';
 const items=[
  ["site/index.html",Buffer.from(html("/"))],
  ["site/blog/index.html",Buffer.from(html("/blog/"))],
  ["site/assets/site.css",Buffer.from("main{max-width:80ch}")]];
 const metadata=[
  "schema\tbonfyre.authored-site-release.v1",
  "publication\t"+(legacy?"NOINDEX-CANDIDATE-ONLY":"published"),
  "account_namespace\tperson-test-work",
  "release_id\t"+release,
  "cms_revision\t"+revision,
  "pages\t2"].join("\n")+"\n";
 const files="path\tsha256\n"+items.map(([name,bytes])=>
   name+"\t"+(wrongDigest&&name==="site/index.html"?revision:hash(bytes))).join("\n")+"\n";
 const pages="route\ttitle\tnative_revision\thtml_sha256\n"+
  "/\tHome\t"+pageRevision+"\t"+hash(items[0][1])+"\n"+
  "/blog/\tJournal\t"+pageRevision+"\t"+hash(items[1][1])+"\n";
 const entries=[
  ["RELEASE.tsv",metadata],
  ["FILES.tsv",files],
  ["SITE-PAGES.tsv",pages],
  ...items.filter(([name])=>!missing||name!=="site/assets/site.css")];
 if(duplicate)entries.push(items[0]);
 return tar(entries);
}
function admit(bytes,tag){
 const archive=join(root,tag+".tar.gz"),output=join(root,"out-"+tag);
 writeFileSync(archive,bytes);return verifyAndStage(archive,hash(bytes),{output});
}
test("published, pinned, complete account-authored pages replace checkout site",()=>{
 const result=admit(fixture(),"valid");
 assert.equal(result.namespace,"person-test-work");
 assert.equal(result.release,release);
 assert.equal(result.pages,2);
 assert.match(readFileSync(join(root,"out-valid/index.html"),"utf8"),/Authored through the account editor/);
 assert.ok(readFileSync(join(root,"out-valid/blog/index.html")).length>30);
});
test("bad external archive digest never writes any site",()=>{
 const archive=join(root,"bad-digest.tar.gz");writeFileSync(archive,fixture());
 assert.throws(()=>verifyAndStage(archive,"e".repeat(64),
  {output:join(root,"out-bad-digest")}),/archive bytes differ/);
});
test("candidate, noindex, partial and silently altered files all fail closed",()=>{
 for(const [tag,params]of Object.entries({
  candidate:{legacy:true},noindex:{noindex:true},
  partial:{missing:true},tampered:{wrongDigest:true},duplicate:{duplicate:true}})){
  assert.throws(()=>admit(fixture(params),tag),/publication refused/i,tag);
 }
});
test("tar traversal and symlinks are never extracted",()=>{
 const base=fixture(),extra=tar([["site/../escape","unsafe"]]);
 assert.throws(()=>admit(extra,"traversal"),/unsafe archive path/);
 assert.throws(()=>admit(tar([["site/linked","", "2"]]),"symlink"),
  /tar links/);
 assert.ok(base.length>100);
});
process.on("exit",()=>rmSync(root,{recursive:true,force:true}));
