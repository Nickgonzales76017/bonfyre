import {test} from "node:test";
import assert from "node:assert/strict";
import {readFileSync,mkdtempSync,rmSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {createHash} from "node:crypto";
import {journalArticles,authoringFromJournal,extractJournalSource,exportAllJournal}
 from "../tools/import_journal.mjs";
import {encodeNativeAuthoring,decodeNativeAuthoring} from "../runtime/authoring-carrier.mjs";
import {toGrapesProject,fromGrapesProject,toPuckData,fromPuckData} from "../runtime/authoring-tree.mjs";
test("all 90 real editorial posts are byte-identical through both visual editors and native authoring carrier",()=>{
 const files=journalArticles();assert.equal(files.length,90);
 const sourceRoot=new URL("../../../editorial/posts/",import.meta.url);
 let total=0;
 for(const f of files){
  const bytes=readFileSync(new URL(f,sourceRoot)),doc=authoringFromJournal(f,bytes);
  const carrier=encodeNativeAuthoring(doc);
  for(const candidate of [
   decodeNativeAuthoring(carrier),
   fromGrapesProject(toGrapesProject(doc),doc.site_id,doc.page),
   fromPuckData(toPuckData(doc),doc.site_id,doc.page)
  ]){
   assert.deepEqual(extractJournalSource(candidate),bytes,"lost Markdown source: "+f);
   assert.equal(candidate.blocks[1].props.source_digest,createHash("sha256").update(bytes).digest("hex"));
  }
  total+=bytes.length;
 }
 assert.ok(total>90000,"90 articles should represent substantial actual editorial work");
});
test("all 90 original essays export as distinct native C-ready articles with source manifest",()=>{
 const dir=mkdtempSync(join(tmpdir(),"bf-journal-90-"));
 try{
  const result=exportAllJournal({out:dir});
  assert.equal(result.articles,90);
  assert.equal(readdirSync(dir).filter(x=>x.endsWith(".bfau")).length,90);
  assert.equal(result.manifest.trimEnd().split("\n").length,91);
  assert.ok(!result.manifest.includes("undefined"));
 }finally{rmSync(dir,{recursive:true,force:true});}
});
test("source importer refuses wrong frontmatter identity and unreviewed composition shortcuts",()=>{
 assert.throws(()=>authoringFromJournal("wrong.md",
  "---\nslug: rightful\ntitle: Original\ndescription: Source\n---\nContent\n"),/identity/);
 assert.throws(()=>authoringFromJournal("valid.md","Content without frontmatter"),/frontmatter/);
});
