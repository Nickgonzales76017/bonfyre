import {test,expect,beforeAll,afterAll} from "bun:test";
import {copyFileSync} from "node:fs";
import {join} from "node:path";
import {createRequire} from "node:module";
import {newAuthoringTree,validateAuthoringTree} from "../../BonfyreSurfaceScene/runtime/authoring-tree.mjs";
import {encodeNativeAuthoring} from "../../BonfyreSurfaceScene/runtime/authoring-carrier.mjs";
import {journalArticles,authoringFromJournal,stageJournalDraftForNamespace,extractJournalSource} from "../../BonfyreSurfaceScene/tools/import_journal.mjs";
import {treeFromNativeSnapshot,toNativePageWrite} from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {spawnSync} from "node:child_process";

import {packSetupFixture,root,password} from "./pack_setup_fixture.mjs";
let f,writer,other,browser;
const route="/unified-authoring-proof",json=x=>JSON.parse(x.body);
beforeAll(async()=>{
 f=await packSetupFixture();
 copyFileSync(join(root,"cmd/BonfyreCMS/content-types/site_fragment.json"),
  join(f.home,"schemas/site_fragment.json"));
 f.run(f.cms,["schema","migrate","--db",f.db,"--schemas",join(f.home,"schemas")]);
 for(const name of ["editor-a","editor-b"])
  f.run(f.auth,["--db",f.authdb,"signup","--email",name+"@example.test",
   "--name",name,"--password",password,"--tier","pro"]);
 writer=await f.login("editor-a@example.test",{signup:false});
 other=await f.login("editor-b@example.test",{signup:false});
 const puppeteer=createRequire(join(root,"tools/creator-ingest/package.json"))("puppeteer");
 browser=await puppeteer.launch({
  executablePath:process.env.BONFYRE_TEST_CHROME||
   (process.platform==="darwin"?"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome":puppeteer.executablePath()),
  headless:true,args:["--no-sandbox","--host-resolver-rules=MAP aurekai.life 127.0.0.1",
   // Only the isolated browser test uses HTTP on a loopback-mapped hostname.
   // Production editors require an ordinary HTTPS secure context for SHA-256.
   "--unsafely-treat-insecure-origin-as-secure=http://aurekai.life:"+f.port]});
},45000);
afterAll(async()=>{await browser?.close();await f?.close();},45000);
test("the advanced editor inherits existing native session and serves its real bundle",async()=>{
 expect((await f.request("/cms/compose/advanced")).status).toBe(403);
 const page=await f.request("/cms/compose/advanced",{cookie:writer.cookie});
 expect(page.status).toBe(200);
 expect(page.body).toContain('data-aurekai-authoring');
 expect(page.body).toContain('/assets/authoring-editor.js');
 for(const a of ["/assets/authoring-editor.js","/assets/authoring-editor.css"])
  expect((await f.request(a)).status).toBe(200);
});
test("real GrapesJS/Puck/Editor.js modes preserve a native CMS draft and account scope",async()=>{
 const opened=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
  {cookie:writer.cookie}));
 expect(opened.id).toBe(0);
 const page={route,title:"Authoring witness",heading:"Writing and projects",status:"draft"};
 const fragments=[{coordinate:"first-story",page:route,slot:"showcase",audience:"public",
  kind:"profile-story",position:0,heading:"Original essay",body:"Opening paragraph",
  status:"draft",variant_key:"paper",
  semantic_tags:{authoring_v1:{version:1,parent:null,responsive:{desktop:{layout:"stack"}}}}}];
 const save=await f.request("/cms/compose/page",{cookie:writer.cookie,
  form:JSON.stringify({operation:"create",route,expected_id:0,
   expected_digest:opened.digest,page,fragments}),
  headers:{"Content-Type":"application/json"}});
 expect(save.status).toBe(200);
 const ctx=await browser.createBrowserContext(),tab=await ctx.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...bits]=writer.cookie.split("=");
 const localHost="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:bits.join("="),url:localHost,httpOnly:true});
 const switchTo=async label=>tab.evaluate(value=>{
  const b=[...document.querySelectorAll(".authoring-modes button")].find(x=>x.textContent===value);
  if(!b)throw Error("Missing editing mode "+value);
  b.click();
 },label);
 const ready=async key=>tab.waitForFunction(k=>{
  const s=document.querySelector(".authoring-status");
  return s?.textContent?.includes("Editing "+k)&&s.dataset.error!=="true";
 },{timeout:25000},key);
 try{
  const response=await tab.goto(localHost+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000});
  expect(response.status()).toBe(200);
  await tab.waitForSelector(".authoring-modes button",{timeout:9000}).catch(async error=>{
   const state=await tab.evaluate(()=>({title:document.title,body:document.body?.innerText?.slice(0,700),url:location.href}));
   throw Error("Authoring mount failed "+JSON.stringify({state,errors})+" "+error.message);
  });
  await ready("grapesjs");
  expect(await tab.$(".gjs-editor")).not.toBeNull();
  // Shared native inspector: change actual responsive tokens and a media
  // caption while GrapesJS is mounted, then cross into Puck/Editor.js.
  await tab.waitForSelector('.authoring-inspector-control[aria-label="Viewport"]',{timeout:15000});
  const inspect=async(label,value)=>tab.$eval(
   '.authoring-inspector-control[aria-label="'+label+'"]',
   (input,next)=>{input.value=next;input.dispatchEvent(new Event("change",{bubbles:true}));},value);
  await inspect("Viewport","phone");
  await inspect("Layout","grid");
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")
   ?.textContent?.includes("phone layout updated"),{timeout:15000});
  await inspect("Columns","2");
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")
   ?.textContent?.includes("phone columns updated"),{timeout:15000});
  await inspect("Credit / caption","Reviewed caption retained across specialist editors");
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")
   ?.textContent?.includes("Credit / caption updated"),{timeout:15000});
  await switchTo("Structure");
  await ready("puck");
  await tab.waitForFunction(()=>document.querySelector(".authoring-puck")?.children.length>0,
   {timeout:15000});
  await switchTo("Article");
  await ready("editorjs");
  await tab.waitForSelector(".ce-block",{timeout:20000});
  await tab.$eval(".ce-paragraph",node=>{
   node.focus();node.textContent="Edited in the real Editor.js canvas";
   node.dispatchEvent(new InputEvent("input",{bubbles:true,inputType:"insertText"}));
  });
  // Unsaved Editor.js document must be previewable through the actual C
  // owner, not through a browser-only imitation or a published mutation.
  await tab.evaluate(()=>{
   [...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent==="Native preview")?.click();
  });
  await tab.waitForSelector(".authoring-native-preview[open]",{timeout:20000});
  const nativeFrame=await (await tab.$(".authoring-preview-frame")).contentFrame();
  await nativeFrame.waitForSelector('[data-release="draft-preview-only"]',{timeout:15000});
  const nativeBody=await nativeFrame.$eval("#content",x=>x.textContent);
  expect(nativeBody).toContain("Edited in the real Editor.js canvas");
  await tab.click(".authoring-preview-close");
  await tab.evaluate(()=>{
   [...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent==="Review")?.click();
  });
  await tab.waitForSelector(".authoring-review[open]",{timeout:10000});
  await tab.click(".authoring-review-close");
  await tab.evaluate(()=>{
   [...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent==="Save draft")?.click();
  });
  await tab.waitForFunction(()=>{
   const s=document.querySelector(".authoring-status");
   return s?.textContent?.includes("Draft saved")||s?.dataset.error==="true";
  },{timeout:20000});
  const status=await tab.$eval(".authoring-status",x=>x.textContent);
  expect(status).toContain("Draft saved");
  const persisted=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie}));
  expect(persisted.id).toBeGreaterThan(0);
  expect(persisted.snapshot.fragments[0].status).toBe("draft");
  const retainedTags=persisted.snapshot.fragments[0].semantic_tags;
  const savedTags=typeof retainedTags==="string"?JSON.parse(retainedTags):retainedTags;
  expect(savedTags.authoring_v1.editorial?.length).toBeGreaterThan(0);
  expect(savedTags.authoring_v1.editorial[0].data.text).toContain("Edited in the real Editor.js canvas");
  expect(savedTags.authoring_v1.responsive.phone.layout).toBe("grid");
  expect(savedTags.authoring_v1.responsive.phone.columns).toBe(2);
  expect(savedTags.authoring_v1.props.caption).toBe("Reviewed caption retained across specialist editors");
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},60000);

test("native C authoring preview requires current namespace, exact writer session and same-origin",async()=>{
 const site=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
  {cookie:writer.cookie})).snapshot.namespace;
 const doc=newAuthoringTree(site,route);
 doc.blocks=[{id:"preview-section",kind:"profile-section",
   props:{heading:"Visible C title",body:"<script>alert(1)</script> & provenance"},
   responsive:{desktop:{layout:"grid",columns:2},phone:{layout:"stack",columns:1}},
   children:[{id:"source-quote",kind:"profile-quote",props:{body:"Attached public record"},
    responsive:{},children:[]}]}];
 validateAuthoringTree(doc);
 const bytes=encodeNativeAuthoring(doc),payload="BFAU2:"+Buffer.from(bytes).toString("hex");
 const send=async(cookie,form=payload,headers={})=>f.request("/cms/compose/preview",{
  ...(cookie?{cookie}:{}),form,headers:{"Content-Type":"text/plain",...headers}
 });
 expect((await send("")).status).toBe(403);
 expect((await send(other.cookie)).status).toBe(403);
 expect((await send(writer.cookie,"BFAU2:1234")).status).toBe(400);
 const response=await send(writer.cookie);
 expect(response.status).toBe(200);
 expect(response.body).toContain('data-release="draft-preview-only"');
 expect(response.body).toContain('data-id="preview-section"');
 expect(response.body).toContain("Visible C title");
 expect(response.body).toContain("&lt;script&gt;alert(1)&lt;/script&gt;");
 expect(response.body).toContain("Attached public record");
 expect(response.body).not.toContain("<script>");
});

test("90 real Journal articles become isolated native CMS drafts and C previews with exact source readback",async()=>{
 const files=journalArticles(join(root,"editorial/posts"));
 expect(files.length).toBe(90);
 const coordinates=new Set();
 const directory=mkdtempSync(join(tmpdir(),"bf-90-native-parity-"));
 const nativeExe=join(root,"cmd/BonfyreSurfaceScene/build/authoring-previews/bf-authoring-preview-native");
 let byteEqual=0;
 let kept=0;
 for(const file of files){
  const raw=readFileSync(join(root,"editorial/posts",file));
  const source=authoringFromJournal(file,raw);
  const base=json(await f.request("/cms/compose/page?route="+encodeURIComponent(source.page),
   {cookie:writer.cookie}));
  expect(base.id).toBe(0);
  const draft=stageJournalDraftForNamespace(source,base.snapshot.namespace);
  const prepared=toNativePageWrite(base,draft,{
   pageDraft:{title:draft.blocks[0].props.heading,
    heading:draft.blocks[0].props.heading,lede:draft.blocks[0].props.body}
  });
  expect(prepared.page.status).toBe("draft");
  expect(prepared.fragments.length).toBe(2);
  for(const part of prepared.fragments){
   expect(coordinates.has(part.coordinate)).toBe(false);
   coordinates.add(part.coordinate);
  }
  const saved=await f.request("/cms/compose/page",{cookie:writer.cookie,
   form:JSON.stringify(prepared),headers:{"Content-Type":"application/json"}});
  if(saved.status!==200)throw Error("Native CMS refused "+file+": "+saved.status+" "+saved.body.slice(0,350));
  const reopened=json(await f.request("/cms/compose/page?route="+encodeURIComponent(source.page),
    {cookie:writer.cookie}));
  expect(reopened.id).toBeGreaterThan(0);
  const roundtrip=treeFromNativeSnapshot(reopened);
  expect(Buffer.from(extractJournalSource(roundtrip)).equals(raw)).toBe(true);
  expect(roundtrip.blocks[1].props.source_digest).toBe(source.blocks[1].props.source_digest);
  expect(roundtrip.blocks[0].props.metadata.publication).toBe("not-granted");
  expect(reopened.snapshot.fragments.every(x=>x.status==="draft")).toBe(true);
  // Same account-owned carrier must render byte-identically through C
  // as both a static draft preview and an authenticated HTTP preview.
  const wire=encodeNativeAuthoring(roundtrip),name=file.slice(0,-3);
  const input=join(directory,name+".bfau"),html=join(directory,name+".html");
  writeFileSync(input,wire);
  const rendered=spawnSync(nativeExe,[input,html],{encoding:"utf8",timeout:20000});
  expect(rendered.status).toBe(0);
  const preview=await f.request("/cms/compose/preview",{cookie:writer.cookie,
   form:"BFAU2:"+Buffer.from(wire).toString("hex"),
   headers:{"Content-Type":"text/plain"}});
  expect(preview.status).toBe(200);
  expect(preview.body).toBe(readFileSync(html,"utf8"));
  expect(preview.body).toContain('data-release="draft-preview-only"');
  expect(preview.body).toContain('<div class="bf-markdown">');
  byteEqual++;
  if([0,44,89].includes(kept)){
   const otherScope=json(await f.request("/cms/compose/page?route="+encodeURIComponent(source.page),
    {cookie:other.cookie}));
   expect(otherScope.id).toBe(0);
  }
  kept++;
 }
 expect(kept).toBe(90);
 expect(coordinates.size).toBe(180);
 expect(byteEqual).toBe(90);
 rmSync(directory,{recursive:true,force:true});
 console.log("AUREKAI_90_NATIVE_CMS_DRAFTS_PASS 90 complete reads / 180 unique fragments / 0 publications");
 console.log("AUREKAI_90_NATIVE_HTTP_STATIC_PARITY_PASS 90 byte-identical C representations of the same account-owned drafts");
},240000);

test("edit a real imported Journal Markdown source in the unified desk; preview with C and persist both digests",async()=>{
 const file=journalArticles(join(root,"editorial/posts"))[0];
 const original=authoringFromJournal(file,readFileSync(join(root,"editorial/posts",file)));
 const browserContext=await browser.createBrowserContext(),page=await browserContext.newPage();
 const [name,...bits]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 try{
  await page.setCookie({name,value:bits.join("="),url:host,httpOnly:true});
  const response=await page.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(original.page),
   {waitUntil:"networkidle0",timeout:30000});
  expect(response.status()).toBe(200);
  await page.waitForFunction(()=>[...document.querySelectorAll(".authoring-modes button")]
    .some(x=>x.textContent==="Markdown source"),{timeout:15000});
  await page.evaluate(()=>[...document.querySelectorAll(".authoring-modes button")]
   .find(x=>x.textContent==="Markdown source").click());
  await page.waitForSelector(".authoring-markdown-source",{timeout:15000});
  const actual=await page.$eval(".authoring-markdown-source",e=>e.value);
  expect(actual).toBe(readFileSync(join(root,"editorial/posts",file),"utf8"));
  const addition="\n\n## A reviewed amendment\n\nNative source authoring preserves provenance.\n";
  await page.$eval(".authoring-markdown-source",(e,postscript)=>{
   e.value+=postscript;e.dispatchEvent(new Event("input",{bubbles:true}));
  },addition);
  await page.evaluate(()=>[...document.querySelectorAll(".authoring-actions button")]
   .find(x=>x.textContent==="Native preview").click());
  await page.waitForSelector(".authoring-native-preview[open]",{timeout:20000});
  const frame=await (await page.$(".authoring-preview-frame")).contentFrame();
  await frame.waitForSelector(".bf-markdown",{timeout:15000});
  const html=await frame.$eval(".bf-markdown",el=>el.textContent);
  expect(html).toContain("A reviewed amendment");
  expect(html).toContain("Native source authoring preserves provenance.");
  await page.click(".authoring-preview-close");
  await page.evaluate(()=>[...document.querySelectorAll(".authoring-actions button")]
   .find(x=>x.textContent==="Save draft").click());
  await page.waitForFunction(()=>document.querySelector(".authoring-status")
    ?.textContent?.includes("Draft saved"),{timeout:20000});
  const reopened=json(await f.request("/cms/compose/page?route="+encodeURIComponent(original.page),
   {cookie:writer.cookie}));
  const current=treeFromNativeSnapshot(reopened).blocks[1].props;
  expect(current.body).toContain("A reviewed amendment");
  expect(current.source_digest).toBe(original.blocks[1].props.source_digest);
  expect(current.metadata.changed_from_original).toBe(true);
  const expected=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(current.body));
  expect(current.metadata.draft_sha256).toBe(Buffer.from(expected).toString("hex"));
  expect(reopened.snapshot.fragments.every(x=>x.status==="draft")).toBe(true);
  console.log("JOURNAL_NATIVE_SOURCE_EDITOR_PASS original provenance unchanged / derived SHA-256 verified / C preview and CMS draft readback");
 }finally{await browserContext.close();}
},90000);
