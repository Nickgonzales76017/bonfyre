/* This is NOT a static HTML importer: real authoring operations build every
 * component from a blank account page, persist the native CMS row shape, then
 * the same C projection WebAddress links emits actual browser-interactive HTML.
 * The database is an isolated test fixture; no publication is performed. */
import {test,expect} from "bun:test";
import assert from "node:assert/strict";
import {Database} from "bun:sqlite";
import {mkdtempSync,readFileSync,rmSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {createRequire} from "node:module";
import {AUTHORING_PATTERNS,defaultPatternPayload}
 from "../../../components/BonfyreServiceSite/authoring/presentation-patterns.mjs";
import {newAuthoringTree,applyAuthoringEdit,validateAuthoringTree,
 toPuckData,fromPuckData,toGrapesProject,fromGrapesProject}
 from "../runtime/authoring-tree.mjs";
import {toNativePageWrite}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {renderAuthoredEditorial} from "../../../components/BonfyreServiceSite/authored-pattern-renderer.mjs";
import {renderAuthoredProfilePreview} from "../../../components/BonfyreServiceSite/authored-page-renderer.mjs";
import {renderBrowserReview} from "../../../components/BonfyreServiceSite/authoring/review-preview.mjs";
const require=createRequire(join(import.meta.dir,"../../../tools/creator-ingest/package.json"));
const puppeteer=require("puppeteer");
const root=new URL("../../../",import.meta.url);
const cwd=new URL("../",import.meta.url).pathname;
const read=x=>readFileSync(new URL(x,root));
function blankPage(site){
 let doc=newAuthoringTree(site,"/");
 doc=applyAuthoringEdit(doc,{type:"insert",block:{
  id:"original-documentation",kind:"ui:documentation",
  props:{heading:"This is a genuine authored original",payload:{
   title:"Aurekai authored reference",lede:"From blank page to native C",
   toc:[{label:"Overview",href:"#overview"}]}},
  responsive:{desktop:{layout:"grid",columns:3},phone:{layout:"stack"}},children:[]}});
 for(const spec of AUTHORING_PATTERNS){
  const block={id:"new-"+spec.name,kind:spec.kind,props:{
   heading:spec.name+" created in the builder",payload:defaultPatternPayload(spec.kind)},
   responsive:{},children:[]};
  doc=applyAuthoringEdit(doc,{type:"insert",
   parent:"original-documentation",block});
 }
 doc.blocks[0].children.find(n=>n.kind==="ui:hero").props.payload.title=
  "Native C authoring is shared across accounts";
 doc.blocks[0].children.find(n=>n.kind==="ui:nav-tabs").props.payload.tabs=[
  {label:"One",body:"Visible first panel"},
  {label:"Two",body:"Visible second panel"}];
 doc.blocks[0].children.find(n=>n.kind==="ui:carousel").props.payload.slides=[
  {title:"First scene",body:"Slide one",src:"",alt:""},
  {title:"Second scene",body:"Slide two",src:"",alt:""}];
 return validateAuthoringTree(doc);
}
function compileNative(output){
 const src=[
  "tests/native_authored_document_cli.c",
  "../BonfyreWebAddress/src/site_authored_document.c",
  "src/authoring_native_components.c",
  "src/authoring_editorial.c",
  "src/authoring_markdown.c",
  "vendor/md4c/md4c.c",
  "vendor/md4c/md4c-html.c",
  "vendor/md4c/entity.c",
  "src/authoring_template.c",
  "vendor/mustach/mini-mustach.c",
  "../../lib/libbonfyre/libbonfyre.a"];
 const args=["cc","-std=c11","-O2","-Wall","-Wextra","-Werror",
  "-I../../lib/libbonfyre/include","-Ivendor/md4c",...src,"-lsqlite3","-lm","-o",output];
 const result=Bun.spawnSync({cmd:args,cwd,stdout:"pipe",stderr:"pipe"});
 if(result.exitCode!==0)
  throw Error("native authored page C build failed: "+result.stderr.toString());
}
function setupCMS(path){
 const db=new Database(path,{create:true});
 db.exec("CREATE TABLE site_page(id INTEGER PRIMARY KEY, namespace TEXT, route TEXT,status TEXT);");
 db.exec("CREATE TABLE site_fragment(id INTEGER PRIMARY KEY, namespace TEXT,page TEXT,slot TEXT,audience TEXT,status TEXT,position INTEGER,coordinate TEXT,kind TEXT,heading TEXT,body TEXT,semantic_tags TEXT,projection_contract TEXT,source_digest TEXT,evidence_ref TEXT);");
 const insertPage=db.prepare("INSERT INTO site_page(namespace,route,status) VALUES(?,?,'published')");
 const insertBlock=db.prepare("INSERT INTO site_fragment(namespace,page,slot,audience,status,position,coordinate,kind,heading,body,semantic_tags,projection_contract,source_digest,evidence_ref) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
 function add(doc){
  const untouched={schema:"bonfyre.page-snapshot.v1",id:0,
   digest:"0".repeat(64),snapshot:{
    namespace:doc.site_id,route:doc.page,page:null,fragments:[]}};
  const request=toNativePageWrite(untouched,doc,{pageDraft:{
   title:"Native reference",heading:"Native reference"}});
  const expected=doc.blocks.reduce(function count(total,node){
   return total+1+node.children.reduce(count,0);
  },0);
  assert.equal(request.fragments.length,expected);
  const transaction=db.transaction(()=>{
   insertPage.run(doc.site_id,doc.page);
   for(const row of request.fragments)
    insertBlock.run(doc.site_id,row.page,row.slot,row.audience,
     "published",row.position,row.coordinate,row.kind,row.heading,row.body,
     JSON.stringify(row.semantic_tags),JSON.stringify(row.projection_contract),
     row.source_digest||"",row.evidence_ref||"");
  });
  transaction();
 }
 return {db,add};
}
function nativePage(exe,path,site){
 const process=Bun.spawnSync({cmd:[exe,path,site,"/"],cwd,stdout:"pipe",stderr:"pipe"});
 if(process.exitCode!==0)
  throw Error("native public CMS projection failed: "+process.stderr.toString());
 return process.stdout.toString();
}
test("every registered component passes Puck/GrapesJS, native CMS, C renderer and Chromium in two profiles",async()=>{
 const tmp=mkdtempSync(join(tmpdir(),"bonfyre-native-browser-parity-"));
 let browser=null,server=null,db=null;
 try{
  const binary=join(tmp,"native-page"),dbpath=join(tmp,"cms.sqlite");
  compileNative(binary);
  const storage=setupCMS(dbpath);db=storage.db;
  for(const site of ["person-original-work","person-unrelated-life"]){
   const doc=blankPage(site);
   assert.deepEqual(fromPuckData(toPuckData(doc),site,"/"),doc);
   assert.deepEqual(fromGrapesProject(toGrapesProject(doc),site,"/"),doc);
   storage.add(doc);
  }
  const first=nativePage(binary,dbpath,"person-original-work");
  const other=nativePage(binary,dbpath,"person-unrelated-life");
  expect((first.match(/data-ak-id="/g)||[]).length).toBe(AUTHORING_PATTERNS.length+1);
  expect((other.match(/data-ak-id="/g)||[]).length).toBe(AUTHORING_PATTERNS.length+1);
  expect(first).toContain('data-authored-native="c"');
  expect(first).toContain("Native C authoring is shared across accounts");
  expect(first).toContain('data-desktop-layout="grid" data-desktop-columns="3"');
  expect(first).toContain('data-phone-layout="stack"');
  expect(first).toContain("role=\"tablist\"");
  expect(first).toContain("data-ak-slide=\"1\"");
  expect(first).toContain("data-ak-effect=\"review-required\"");
  expect(first).not.toContain("<script>alert");
  const index='<!doctype html><html lang="en"><head><meta charset="utf-8">'+
   '<meta name="robots" content="noindex,nofollow">'+
   '<link rel="stylesheet" href="/assets/authoring-reader.css">'+
   '<script defer src="/assets/authoring-reader.js"></script></head><body>'+
   first+'</body></html>';
  const assets={
   "/assets/authoring-reader.css":read("site/matrix/authoring-reader.css"),
   "/assets/authoring-reader.js":read("site/matrix/authoring-reader.js")};
  server=Bun.serve({port:0,fetch(request){
   const path=new URL(request.url).pathname;
   if(path==="/")return new Response(index,{headers:{"content-type":"text/html; charset=utf-8"}});
   if(assets[path])return new Response(assets[path],{
    headers:{"content-type":path.endsWith(".js")?"application/javascript":"text/css"}});
   return new Response("Not found",{status:404});
  }});
  browser=await puppeteer.launch({
   executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
   headless:true,args:["--no-sandbox"]});
  const tab=await browser.newPage(),errors=[];
  tab.on("pageerror",error=>errors.push(error.message));
  const page=await tab.goto("http://127.0.0.1:"+server.port+"/",{waitUntil:"networkidle0"});
  expect(page.status()).toBe(200);
  await tab.setViewport({width:1280,height:900});
   expect(await tab.$eval('[data-ak-id="original-documentation"]',
    el=>getComputedStyle(el).display)).toBe("grid");
   await tab.setViewport({width:390,height:844});
   expect(await tab.$eval('[data-ak-id="original-documentation"]',
    el=>getComputedStyle(el).display)).toBe("flex");
   await tab.click('[data-ak-tab="1"]');
  expect(await tab.$eval('[data-ak-panel="1"]',x=>x.hidden)).toBe(false);
  await tab.click("[data-ak-next]");
  expect(await tab.$eval('[data-ak-slide="1"]',x=>x.hidden)).toBe(false);
  await tab.click("[data-ak-dialog]");
  expect(await tab.$eval(".ak-modal dialog",x=>x.open)).toBe(true);
  await tab.click(".ak-modal [data-ak-close]");
  expect(await tab.$eval(".ak-modal dialog",x=>x.open)).toBe(false);
  expect(await tab.$eval('[data-ak-effect="review-required"] button[type="submit"]',
   x=>x.disabled)).toBe(true);
  expect(errors).toEqual([]);
 }finally{
  if(browser)await browser.close();
  if(server)server.stop(true);
  if(db)db.close();
  rmSync(tmp,{recursive:true,force:true});
 }
},120000);

test("eight Editor.js tools produce byte-identical markup in native C, visual review and static profile",()=>{
 const tmp=mkdtempSync(join(tmpdir(),"bonfyre-editorial-native-parity-"));
 let database=null;
 try{
  const binary=join(tmp,"native-editorial"),dbpath=join(tmp,"cms.sqlite");
  compileNative(binary);
  const store=setupCMS(dbpath);database=store.db;
  const blocks=[
   {type:"paragraph",data:{text:"Original <b>bold</b> & documented text"}},
   {type:"header",data:{level:4,text:"A grounded chapter"}},
   {type:"quote",data:{text:"Attributed source",caption:"Recorded author"}},
   {type:"code",data:{code:"if (n < 10) return;"}},
   {type:"delimiter",data:{}},
   {type:"list",data:{style:"ordered",items:["Discover","Verify"]}},
   {type:"checklist",data:{items:[{text:"Review evidence",checked:true},{text:"Publish later",checked:false}]}},
   {type:"table",data:{content:[["Field","Status"],["CMS","Retained"]]}}
  ];
  const site="person-editorial-original",doc=applyAuthoringEdit(newAuthoringTree(site,"/"),{
   type:"insert",block:{id:"editor-written-story",kind:"profile-story",
    props:{heading:"Written in the user editor",editorial:blocks},
    responsive:{desktop:{layout:"grid",columns:2},phone:{layout:"stack"}},
    children:[]}
  });
  validateAuthoringTree(doc);store.add(doc);
  const expected=renderAuthoredEditorial(blocks);
  assert.ok(expected.includes("Original &lt;b&gt;bold&lt;/b&gt; &amp; documented"));
  assert.ok(expected.includes('class="ak-editorial-checklist"'));
  const native=nativePage(binary,dbpath,site);
  const reviewed=renderBrowserReview(doc,{name:"Original written work"}).html;
  const staticPage=renderAuthoredProfilePreview(doc,{name:"Original written work"}).html;
  for(const [surface,html]of Object.entries({native,reviewed,staticPage})){
   assert.ok(html.includes(expected),surface+" changed the canonical eight-tool document HTML");
   assert.ok(html.includes("Written in the user editor"),surface+" lost the article title");
   assert.ok(html.includes('data-phone-layout="stack"'),surface+" lost responsive layout");
  }
 }finally{
  if(database)database.close();
  rmSync(tmp,{recursive:true,force:true});
 }
},120000);
