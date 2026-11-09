import {test,expect,beforeAll,afterAll} from "bun:test";
import {copyFileSync} from "node:fs";
import {join} from "node:path";
import {createRequire} from "node:module";
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
  headless:true,args:["--no-sandbox","--host-resolver-rules=MAP aurekai.life 127.0.0.1"]});
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
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},60000);
