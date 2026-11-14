import {test,expect,beforeAll,afterAll} from "bun:test";
import {copyFileSync,readFileSync} from "node:fs";
import {AUTHORING_PATTERNS} from "../../../components/BonfyreServiceSite/authoring/presentation-patterns.mjs";
import {treeFromNativeSnapshot} from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {renderAuthoredProfilePreview} from "../../../components/BonfyreServiceSite/authored-page-renderer.mjs";
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
  executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
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
  semantic_tags:{authoring_v1:{version:1,parent:null,responsive:{desktop:{layout:"stack"}}}}},
   {coordinate:"case-content",page:route,slot:"showcase",audience:"public",
    kind:"service:cases",position:1,heading:"Selected projects",body:"Native cases",
    status:"draft",variant_key:"paper",projection_contract:{html:true},
    semantic_tags:{authoring_v1:{version:1,parent:null,responsive:{},
     payload:{title:"Portfolio sections",items:[
      {title:"First project",text:"Original nested description"}]}}}}];
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
   // A real Puck selection and nested form control, not editor-data injection.
   // The Puck canvas is iframe-hosted. Its Outline uses native selection.
   await tab.evaluate(()=>{
    const outline=[...document.querySelectorAll(".authoring-puck li")]
     .find(x=>x.textContent?.trim()==="Outline");
    if(!outline)throw Error("Real Puck Outline navigation unavailable");
    (outline.querySelector("div")||outline).click();
   });
   await tab.waitForSelector('[data-puck-layer-tree-id="case-content"]',{timeout:8000});
   await tab.$eval('[data-puck-layer-tree-id="case-content"]',node=>{
    const buttons=[...node.querySelectorAll("button")];
    const button=buttons.find(x=>x.textContent?.trim()==="cases")||
     buttons.find(x=>x.textContent?.includes("cases"))||buttons.at(-1);
    if(!button)throw Error("Puck component selection unavailable");
    button.click();
   });
   await tab.waitForSelector('[data-aurekai-structured-kind="cases"]',{timeout:3500}).catch(async()=>{
    const state=await tab.evaluate(()=>{
     const host=document.querySelector(".authoring-puck");
     return {text:host?.innerText?.slice(0,1900),
      kinds:[...document.querySelectorAll("[data-authoring-kind]")].map(x=>x.getAttribute("data-authoring-kind")),
      custom:[...document.querySelectorAll("[data-aurekai-structured-kind]")].map(x=>x.outerHTML.slice(0,180)),
      frames:[...document.querySelectorAll("iframe")].map(x=>({
       html:x.contentDocument?.body?.innerHTML?.slice(0,900),outer:x.outerHTML.slice(0,220)}))};
    });
    throw Error("Puck structured field not selected "+JSON.stringify(state));
   });
   const field='[data-field-path="items.0.text"] textarea';
   await tab.waitForSelector(field,{timeout:12000});
   await tab.$eval(field,el=>{
    const setter=Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,"value").set;
    setter.call(el,"Edited through the Puck nested field");
    el.dispatchEvent(new Event("input",{bubbles:true}));
   });
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
   const preview=await tab.$eval(".authoring-review-preview",x=>({html:x.srcdoc,
    sandbox:x.getAttribute("sandbox"),referrer:x.getAttribute("referrerpolicy")}));
   expect(preview.html).toContain("Edited in the real Editor.js canvas");
   expect(preview.html).toContain("Edited through the Puck nested field");
   expect(preview.sandbox).toBe("");expect(preview.referrer).toBe("no-referrer");
   await tab.click(".authoring-review-devices button:last-child");
   expect(await tab.$eval(".authoring-review-preview",x=>x.dataset.viewport)).toBe("phone");
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
   const savedCase=persisted.snapshot.fragments.find(x=>x.coordinate==="case-content");
   expect(savedCase).toBeDefined();
   const caseTags=typeof savedCase.semantic_tags==="string"?
    JSON.parse(savedCase.semantic_tags):savedCase.semantic_tags;
   expect(caseTags.authoring_v1.payload.items[0].text)
    .toBe("Edited through the Puck nested field");
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},60000);
test("entire native component library can be created via visible palette, saved to CMS and exercised in browser",async()=>{
 const route="/blank-canvas-live-proof";
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...bits]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:bits.join("="),url:host,httpOnly:true});
 const original=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
  {cookie:writer.cookie}));
 expect(original.id).toBe(0);
 const waitReady=async()=>tab.waitForFunction(()=>{
  const status=document.querySelector(".authoring-status");
  return status?.textContent.includes("Editing grapesjs")&&status.dataset.error!=="true";
 },{timeout:20000});
 const add=async(kind,parent)=>{
  await tab.select(".authoring-component-picker",kind);
  await tab.select(".authoring-parent-picker",parent||"");
  await tab.click(".authoring-insert");
  await tab.waitForFunction(k=>{
   const s=document.querySelector(".authoring-status");
   return s?.dataset.error==="true"||s?.textContent?.startsWith("Added "+k+" to ");
  },{timeout:20000},kind);
  const message=await tab.$eval(".authoring-status",x=>({message:x.textContent,error:x.dataset.error}));
  expect(message.error).toBe("false");
  expect(message.message).toContain("Added "+kind);
 };
 try{
  const response=await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000});
  expect(response.status()).toBe(200);
  await waitReady();
  const choices=await tab.$$eval(".authoring-component-picker option",nodes=>nodes.map(x=>x.value));
  expect(choices).toContain("ui:documentation");
  expect(choices).toContain("ui:modal");
  expect(choices).toContain("service:cases");
  await add("ui:documentation",null);
  const visualGrapes=await Promise.all(tab.frames().map(async frame=>
   !!(await frame.$(".authoring-trusted-preview .ak-documentation"))));
  expect(visualGrapes.some(Boolean)).toBe(true);
  const parent=await tab.$eval(".authoring-parent-picker",x=>[...x.options].find(o=>o.value)?.value);
  expect(parent).toMatch(/^node-[0-9a-f]{24}$/);
  for(const pattern of AUTHORING_PATTERNS)
   await add(pattern.kind,parent);
  await add("profile-story",parent);
  // Edit real component material in Puck after creating it from a blank page.
  await tab.evaluate(()=>{
   const button=[...document.querySelectorAll(".authoring-modes button")]
    .find(x=>x.textContent==="Structure");
   if(!button)throw Error("Puck mode missing");
   button.click();
  });
  await tab.waitForFunction(()=>{
   const s=document.querySelector(".authoring-status");
   return s?.textContent.includes("Editing puck")&&s.dataset.error!=="true";
  },{timeout:20000});
  await tab.evaluate(()=>{
   const outline=[...document.querySelectorAll(".authoring-puck li")]
    .find(x=>x.textContent?.trim()==="Outline");
   if(!outline)throw Error("Puck outline unavailable");
   (outline.querySelector("div")||outline).click();
  });
  const target='[data-puck-layer-tree-id="'+parent+'"]';
  await tab.waitForSelector(target,{timeout:10000});
  await tab.$eval(target,node=>{
   const buttons=[...node.querySelectorAll("button")];
   const button=buttons.find(x=>x.textContent?.trim()==="documentation")||
    buttons.find(x=>x.textContent?.includes("documentation"))||buttons.at(-1);
   if(!button)throw Error("Created documentation block not selectable");
   button.click();
  });
  await tab.waitForSelector('[data-aurekai-structured-kind="ui:documentation"]',
   {timeout:10000});
  const visualPuck=await Promise.all(tab.frames().map(async frame=>
   !!(await frame.$(".authoring-trusted-preview .ak-modal"))));
  expect(visualPuck.some(Boolean)).toBe(true);
  await tab.$eval('[data-field-path="title"] textarea',el=>{
   const setter=Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,"value").set;
   setter.call(el,"Native component manual");
   el.dispatchEvent(new Event("input",{bubbles:true}));
  });
  // Save the named user action, never a positional button. Puck can
  // asynchronously recompose the action rail after an outline selection.
  await tab.waitForFunction(()=>{
   const button=[...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent?.trim()==="Save draft");
   return button&&!button.disabled;
  },{timeout:30000});
  await tab.evaluate(()=>{
   const button=[...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent?.trim()==="Save draft");
   if(!button||button.disabled)throw Error("Save draft action unavailable");
   button.click();
  });
  await tab.waitForFunction(()=>{
   const s=document.querySelector(".authoring-status");
   return s?.textContent.includes("Draft saved")||s?.dataset.error==="true";
  },{timeout:60000}).catch(async error=>{
   const debug=await tab.evaluate(()=>({
    status:document.querySelector(".authoring-status")?.textContent,
    failed:document.querySelector(".authoring-status")?.dataset.error,
    actionLabels:[...document.querySelectorAll(".authoring-actions button")]
     .map(x=>({text:x.textContent,disabled:x.disabled})),
    selectedMode:[...document.querySelectorAll(".authoring-modes button")]
     .find(x=>x.getAttribute("aria-pressed")==="true")?.textContent
   }));
   throw Error("CMS Save draft acceptance failed "+JSON.stringify(debug)+": "+error.message);
  });
  const status=await tab.$eval(".authoring-status",x=>x.textContent);
  expect(status).toContain("Draft saved");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie}));
  expect(saved.snapshot.fragments.length).toBe(AUTHORING_PATTERNS.length+2);
  const rows=saved.snapshot.fragments;
  const doc=rows.find(x=>x.kind==="ui:documentation");
  expect(doc).toBeDefined();
  const children=rows.filter(x=>x.coordinate!==doc.coordinate);
  expect(children.every(x=>{const meta=typeof x.semantic_tags==="string"?JSON.parse(x.semantic_tags):x.semantic_tags;
   return meta?.authoring_v1?.parent===doc.coordinate;})).toBe(true);
  expect(children.map(x=>x.kind)).toContain("ui:modal");
  expect(children.map(x=>x.kind)).toContain("profile-story");
  for(const pattern of AUTHORING_PATTERNS)
   expect(children.map(x=>x.kind)).toContain(pattern.kind);
  const reopened=treeFromNativeSnapshot(saved);
  const preview=renderAuthoredProfilePreview(reopened,{name:"Composed by an Aurekai account"});
  expect(preview.html).toContain("Native component manual");
  expect(preview.html).toContain('name="robots" content="noindex,nofollow"');
  await tab.setContent(preview.html,{waitUntil:"domcontentloaded"});
  await tab.addScriptTag({content:readFileSync(
   join(root,"components/BonfyreServiceSite/authoring/pattern-runtime.js"),"utf8")});
  await tab.click('[data-ak-tab="1"]');
  expect(await tab.$eval('[data-ak-panel="1"]',node=>node.hidden)).toBe(false);
  await tab.click("[data-ak-next]");
  expect(await tab.$eval('[data-ak-slide="1"]',node=>node.hidden)).toBe(false);
  await tab.click("[data-ak-dialog]");
  expect(await tab.$eval(".ak-modal dialog",node=>node.open)).toBe(true);
  await tab.click(".ak-modal [data-ak-close]");
  expect(await tab.$eval(".ak-modal dialog",node=>node.open)).toBe(false);
  expect(children.every(x=>x.status==="draft")).toBe(true);
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},90000);
test("another account creates the full visual library through its own blank editor, without importing",async()=>{
 const route="/another-original-author/",host="http://aurekai.life:"+f.port;
 expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
  {cookie:other.cookie})).id).toBe(0);
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];
 tab.on("pageerror",error=>errors.push(error.message));
 const [name,...bits]=other.cookie.split("=");
 await tab.setCookie({name,value:bits.join("="),url:host,httpOnly:true});
 const add=async(kind,parent)=>{
  await tab.select(".authoring-component-picker",kind);
  await tab.select(".authoring-parent-picker",parent||"");
  await tab.click(".authoring-insert");
  await tab.waitForFunction(k=>{
   const status=document.querySelector(".authoring-status");
   return status?.dataset.error==="true"||status?.textContent?.startsWith("Added "+k+" to ");
  },{timeout:20000},kind);
  expect(await tab.$eval(".authoring-status",x=>x.dataset.error)).toBe("false");
 };
 try{
  expect((await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000})).status()).toBe(200);
  await tab.waitForFunction(()=>{
   const status=document.querySelector(".authoring-status");
   return status?.textContent?.includes("Editing grapesjs")&&status.dataset.error!=="true";
  },{timeout:20000});
  expect(await tab.$$eval(".authoring-composition-row",items=>items.length)).toBe(0);
  await add("ui:documentation",null);
  const parent=await tab.$eval(".authoring-parent-picker",select=>
   [...select.options].find(x=>x.value)?.value);
  expect(parent).toMatch(/^node-[a-f0-9]{24}$/);
  for(const pattern of AUTHORING_PATTERNS)await add(pattern.kind,parent);
  await add("profile-story",parent);
  await tab.evaluate(()=>{
   const button=[...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent==="Save draft");
   if(!button||button.disabled)throw Error("Save draft unavailable");
   button.click();
  });
  await tab.waitForFunction(()=>{
   const status=document.querySelector(".authoring-status");
   return status?.textContent?.includes("Draft saved")||status?.dataset.error==="true";
  },{timeout:30000});
  expect(await tab.$eval(".authoring-status",x=>x.textContent)).toContain("Draft saved");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie}));
  expect(saved.id).toBeGreaterThan(0);
  expect(saved.snapshot.fragments.length).toBe(AUTHORING_PATTERNS.length+2);
  const typed=treeFromNativeSnapshot(saved);
  const kinds=typed.blocks[0].children.map(x=>x.kind);
  expect(kinds).toContain("ui:carousel");
  expect(kinds).toContain("ui:modal");
  expect(kinds).toContain("profile-story");
  expect(renderAuthoredProfilePreview(typed,{name:"An independent author's original work"}).html)
   .toContain('data-authored-profile=');
  // Knowing an exact route must never grant a foreign creator's draft.
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},90000);
test("an Aurekai account creates and reopens a multi-page documentation site with editable page titles",async()=>{
 const first="/reference-handbook/",second="/reference-handbook/components/";
 const c=await browser.createBrowserContext(),tab=await c.newPage(),errors=[];
 tab.on("pageerror",error=>errors.push(error.message));
 const [name,...parts]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:parts.join("="),url:host,httpOnly:true});
 const ready=()=>tab.waitForFunction(()=>{
  const n=document.querySelector(".authoring-status");
  return n?.textContent?.includes("Editing grapesjs")&&n.dataset.error!=="true";
 },{timeout:20000});
 const setMeta=async(meta)=>{
  for(const [key,value]of Object.entries(meta)){
   const selector='.authoring-page-settings [name="'+key+'"]';
   await tab.$eval(selector,(node,value)=>{
    const prototype=Object.getPrototypeOf(node);
    const setter=Object.getOwnPropertyDescriptor(prototype,"value")?.set;
    if(setter)setter.call(node,value);else node.value=value;
    node.dispatchEvent(new Event("input",{bubbles:true}));
   },value);
  }
 };
 const add=async kind=>{
  await tab.select(".authoring-component-picker",kind);
  await tab.click(".authoring-insert");
  await tab.waitForFunction(value=>{
   const n=document.querySelector(".authoring-status");
   return n?.textContent?.startsWith("Added "+value+" to ")||n?.dataset.error==="true";
  },{timeout:20000},kind);
  const status=await tab.$eval(".authoring-status",x=>({text:x.textContent,error:x.dataset.error}));
  expect(status.error).toBe("false");
 };
 const save=async()=>{
  await tab.click(".authoring-actions button:last-child");
  await tab.waitForFunction(()=>{
   const n=document.querySelector(".authoring-status");
   return n?.textContent?.includes("Draft saved")||n?.dataset.error==="true";
  },{timeout:20000});
  expect(await tab.$eval(".authoring-status",x=>x.textContent)).toContain("Draft saved");
 };
 try{
  await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(first),
   {waitUntil:"networkidle0",timeout:30000});
  await ready();
  await setMeta({title:"Aurekai Pattern Reference",heading:"Build your own component reference",
   lede:"Original documentation maintained entirely through the authenticated site editor.",editorial_kind:"article",media_subject_ref:"article:reference-handbook",
    author_credit:"Aurekai Editorial",author_subject_ref:"person:nick",editorial_published_on:"2026-11-07",editorial_updated_on:"2026-11-09"});
  await add("ui:documentation");
  await add("ui:nav-tabs");
  await save();
  const firstCMS=json(await f.request("/cms/compose/page?route="+encodeURIComponent(first),
   {cookie:writer.cookie}));
  expect(firstCMS.snapshot.page.title).toBe("Aurekai Pattern Reference");
  expect(await tab.$eval(".authoring-page-settings [name=media_subject_ref]",x=>x.value)).toBe("article:reference-handbook");
  expect(firstCMS.snapshot.page.heading).toBe("Build your own component reference");
  expect(firstCMS.snapshot.page.lede).toContain("authenticated site editor");
  expect(firstCMS.snapshot.page.editorial_kind).toBe("article");expect(firstCMS.snapshot.page.media_subject_ref).toBe("article:reference-handbook");
   expect(firstCMS.snapshot.page.author_credit).toBe("Aurekai Editorial");
   expect(firstCMS.snapshot.page.author_subject_ref).toBe("person:nick");
   expect(firstCMS.snapshot.page.editorial_published_on).toBe("2026-11-07");
   expect(firstCMS.snapshot.page.editorial_updated_on).toBe("2026-11-09");
  expect(firstCMS.snapshot.fragments.length).toBe(2);
  await tab.$eval(".authoring-page-route-form input",(node,value)=>{node.value=value;},second);
  await Promise.all([
   tab.waitForNavigation({waitUntil:"domcontentloaded",timeout:25000}),
   tab.click(".authoring-page-route-form button")
  ]);
  await ready();
  await setMeta({title:"Aurekai Pattern Reference · Components",
   heading:"Interactive components",lede:"Editable account-owned documentation pages."});
  await add("ui:accordion");
  await add("ui:modal");
  await save();
  const secondCMS=json(await f.request("/cms/compose/page?route="+encodeURIComponent(second),
   {cookie:writer.cookie}));
  expect(secondCMS.snapshot.page.title).toContain("Components");
  expect(secondCMS.snapshot.fragments.map(x=>x.kind)).toEqual(["ui:accordion","ui:modal"]);
  await tab.waitForSelector('.authoring-page-list button[data-route="'+first+'"]',
   {timeout:10000});
  await tab.waitForSelector('.authoring-page-list button[data-route="'+second+'"]',
   {timeout:10000});
  // A dirty page must warn before leaving. Refusing the prompt preserves it.
  await setMeta({title:"Unsaved changes"});
  let warned=false;
  tab.once("dialog",async dialog=>{warned=true;await dialog.dismiss();});
  await tab.click('.authoring-page-list button[data-route="'+first+'"]');
  expect(warned).toBe(true);
  expect(tab.url()).toContain(encodeURIComponent(second));
  // Restore the committed title, then reopen the first authored page.
  await setMeta({title:"Aurekai Pattern Reference · Components"});
  await Promise.all([
   tab.waitForNavigation({waitUntil:"domcontentloaded",timeout:25000}),
   tab.click('.authoring-page-list button[data-route="'+first+'"]')
  ]);
  await ready();
  expect(await tab.$eval(".authoring-page-settings [name=title]",x=>x.value))
   .toBe("Aurekai Pattern Reference");
  const ownerPages=json(await f.request("/cms/compose/pages",{cookie:writer.cookie}));
  expect(ownerPages.pages.map(x=>x.route)).toContain(first);
  expect(ownerPages.pages.map(x=>x.route)).toContain(second);
  const otherPages=json(await f.request("/cms/compose/pages",{cookie:other.cookie}));
  expect(otherPages.pages.map(x=>x.route)).not.toContain(first);
  expect(otherPages.pages.map(x=>x.route)).not.toContain(second);
  expect(errors).toEqual([]);
 }finally{await c.close();}
},100000);
test("the actual journal file is imported through the profile UI and saved as an audio-addressable native draft",async()=>{
 const slug="a-council-agenda-can-be-useful",route="/journal/"+slug+"/";
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...bits]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:bits.join("="),url:host,httpOnly:true});
 try{
  const response=await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000});
  expect(response.status()).toBe(200);
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Editing grapesjs"),{timeout:25000});
  await tab.click(".authoring-journal-intake summary");
  const file=await tab.$(".authoring-journal-intake input[type=file]");
  expect(file).not.toBeNull();
  await file.uploadFile(join(root,"editorial/posts/"+slug+".md"));
  await tab.waitForFunction(()=>!document.querySelector(".authoring-journal-intake button")?.disabled,{timeout:12000});
  expect(await tab.$eval(".authoring-import-preview",x=>x.textContent))
   .toContain("A city council agenda should lead somewhere");
  await tab.click(".authoring-journal-intake button");
  await tab.waitForFunction(()=>document.querySelector(".authoring-import-status")?.textContent?.includes("Article added"),{timeout:25000});
  expect(await tab.$eval(".authoring-page-settings [name=media_subject_ref]",x=>x.value))
   .toBe("article:"+slug);
  expect(await tab.$eval(".authoring-page-settings [name=editorial_kind]",x=>x.value))
   .toBe("article");
  expect(await tab.$eval(".authoring-page-settings [name=editorial_published_on]",x=>x.value))
   .toBe("2026-11-07");
  await tab.click(".authoring-actions button:last-child");
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Draft saved"),{timeout:25000});
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie}));
  expect(saved.id).toBeGreaterThan(0);
  expect(saved.snapshot.page.status).toBe("draft");
  expect(saved.snapshot.page.media_subject_ref).toBe("article:"+slug);
  expect(saved.snapshot.fragments.length).toBeGreaterThan(2);
  expect(saved.snapshot.fragments.every(x=>x.kind==="profile-story"&&/^[a-f0-9]{64}$/.test(x.source_digest))).toBe(true);
  expect(saved.snapshot.fragments.map(x=>x.body).join(" "))
   .toContain("A city council agenda tells you what is scheduled");
  const foreign=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie}));
  expect(foreign.id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},80000);
test("an account imports a journal collection in the real browser as distinct native drafts",async()=>{
 const route="/journal-collection-work";
 const slugs=["a-publisher-deserves-a-home","why-ai-needs-an-operating-company"];
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...parts]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:parts.join("="),url:host,httpOnly:true});
 try{
  const response=await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000});
  expect(response.status()).toBe(200);
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Editing grapesjs"),
   {timeout:25000});
  expect(await tab.$eval(".authoring-journal-collection h2",x=>x.textContent)).toContain("Bring existing articles");
   expect(await tab.$eval(".authoring-journal-collection",x=>x.id)).toBe("journal-batch");
  const input=await tab.$(".authoring-journal-collection input[type=file]");
  expect(input).not.toBeNull();
  await input.uploadFile(...slugs.map(slug=>join(root,"editorial/posts/"+slug+".md")));
  await tab.waitForFunction(()=>{
   const box=document.querySelector(".authoring-journal-collection");
   return box?.querySelector(".authoring-import-preview")?.textContent?.includes("2 articles ready")||
    !!box?.querySelector(".authoring-import-status")?.textContent;
  },{timeout:15000});
  expect(await tab.$eval(".authoring-journal-collection .authoring-import-preview",x=>x.textContent))
   .toContain("2 articles ready");
  await tab.click(".authoring-journal-collection button");
  await tab.waitForFunction(()=>{
   const value=document.querySelector(".authoring-journal-collection .authoring-import-status")?.textContent;
   return value?.includes("2 drafts saved and verified")||
    value?.includes("batch stopped")||value?.includes("No drafts saved");
  },{timeout:30000});
  expect(await tab.$eval(".authoring-journal-collection .authoring-import-status",x=>x.textContent))
   .toContain("2 drafts saved and verified");
  for(const slug of slugs){
   const pageRoute="/journal/"+slug+"/";
   const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(pageRoute),
    {cookie:writer.cookie}));
   expect(saved.id).toBeGreaterThan(0);
   expect(saved.snapshot.page.status).toBe("draft");
   expect(saved.snapshot.page.media_subject_ref).toBe("article:"+slug);
   expect(saved.snapshot.fragments.length).toBeGreaterThan(1);
   expect(saved.snapshot.fragments.every(x=>/^[a-f0-9]{64}$/.test(x.source_digest))).toBe(true);
   const denied=json(await f.request("/cms/compose/page?route="+encodeURIComponent(pageRoute),
    {cookie:other.cookie}));
   expect(denied.id).toBe(0);
  }
  const listing=json(await f.request("/cms/compose/pages",{cookie:writer.cookie}));
  for(const slug of slugs)expect(listing.pages.map(x=>x.route)).toContain("/journal/"+slug+"/");
  expect(errors).toEqual([]);
 }finally{await context.close();}
},90000);
test("blank profile composition creates, edits, lays out, rearranges and reopens user-written work",async()=>{
 const route="/composed-from-empty/";
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...parts]=writer.cookie.split("="),host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:parts.join("="),url:host,httpOnly:true});
 const modeReady=mode=>tab.waitForFunction(m=>{
  const status=document.querySelector(".authoring-status");
  return status?.textContent?.includes("Editing "+m)&&status.dataset.error!=="true";
 },{timeout:24000},mode);
 const action=label=>tab.evaluate(text=>{
  const button=[...document.querySelectorAll(".authoring-actions button")]
   .find(n=>n.textContent===text);
  if(!button||button.disabled)throw Error("Action unavailable: "+text);
  button.click();
 },label);
 const compositionAction=label=>tab.evaluate(text=>{
  const button=[...document.querySelectorAll(".authoring-composition-actions button, .authoring-composition-fields button, .authoring-composition-layout button")]
   .find(n=>n.textContent===text);
  if(!button||button.disabled)throw Error("Composition action unavailable: "+text);
  button.click();
 },label);
 try{
  expect((await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000})).status()).toBe(200);
  await modeReady("grapesjs");
  expect(await tab.$$eval(".authoring-composition-row",nodes=>nodes.length)).toBe(0);
  // No import, bundle, preset article or programmatic tree creation.
  await action("New article");
  await tab.waitForSelector(".authoring-article .ce-paragraph",{timeout:10000}).catch(async error=>{
   const state=await tab.evaluate(()=>({
    status:document.querySelector(".authoring-status")?.textContent,
    error:document.querySelector(".authoring-status")?.dataset.error,
    mode:[...document.querySelectorAll(".authoring-modes button")].map(x=>[x.textContent,x.getAttribute("aria-current")]),
    article:document.querySelector(".authoring-article")?.innerHTML?.slice(0,700),
    outline:document.querySelectorAll(".authoring-composition-row").length
   }));
   throw Error("New article did not initialize "+JSON.stringify({state,errors})+" "+error.message);
  });
  await tab.$eval(".authoring-article .ce-paragraph",node=>{
   node.focus();node.textContent="This entire article was written using the account editor.";
   node.dispatchEvent(new InputEvent("input",{bubbles:true,inputType:"insertText"}));
  });
  await tab.evaluate(()=>{
   const button=[...document.querySelectorAll(".authoring-modes button")]
    .find(x=>x.textContent==="Design");
   button.click();
  });
  await modeReady("grapesjs");
  await tab.select(".authoring-component-picker","ui:section");
  await tab.select(".authoring-parent-picker","");
  await tab.click(".authoring-insert");
  await tab.waitForFunction(()=>document.querySelectorAll(".authoring-composition-row").length===2,
   {timeout:20000});
  const section=await tab.$eval(".authoring-composition-row button[aria-current=true]",x=>x.dataset.coordinate);
  await tab.select(".authoring-component-picker","ui:card");
  await tab.select(".authoring-parent-picker",section);
  await tab.click(".authoring-insert");
  await tab.waitForFunction(()=>document.querySelectorAll(".authoring-composition-row").length===3,
   {timeout:20000});
  await tab.click('.authoring-composition-row button[data-coordinate="'+section+'"]');
  await tab.$eval(".authoring-composition-fields input[name=composition-heading]",node=>{
   node.value="A human-composed feature";node.dispatchEvent(new Event("input",{bubbles:true}));
  });
  await tab.$eval(".authoring-composition-fields textarea[name=composition-body]",node=>{
   node.value="Written, arranged and revised without importing a prepared page.";
   node.dispatchEvent(new Event("input",{bubbles:true}));
  });
  await compositionAction("Apply text");
  await tab.waitForFunction(()=>document.querySelector(".authoring-composition-row button[aria-current=true]")?.textContent?.includes("A human-composed feature"),
   {timeout:12000}).catch(async error=>{
    const diagnostic=await tab.evaluate(()=>({
     status:document.querySelector(".authoring-composition-status")?.textContent,
     editorStatus:document.querySelector(".authoring-status")?.textContent,
     selected:document.querySelector(".authoring-composition-row button[aria-current=true]")?.textContent,
     rows:[...document.querySelectorAll(".authoring-composition-row button")].map(x=>x.textContent).slice(0,8),
     disabled:[...document.querySelectorAll(".authoring-composition-fields button")].map(x=>x.disabled)
    }));
    throw Error("Authoring update did not bind to selection "+JSON.stringify(diagnostic)+" "+error.message);
   });
  await tab.select(".authoring-composition-layout label:nth-child(1) select","phone");
  await tab.select(".authoring-composition-layout label:nth-child(2) select","grid");
  await tab.$eval(".authoring-composition-layout label:nth-child(3) input",node=>{node.value="3";});
  await compositionAction("Apply layout");
  await tab.waitForFunction(()=>document.querySelector(".authoring-composition-status")?.textContent?.includes("Composition updated"),
   {timeout:20000});
  await compositionAction("Duplicate");
  await tab.waitForFunction(()=>document.querySelectorAll(".authoring-composition-row").length===5,
   {timeout:20000});
  const copied=await tab.$eval(".authoring-composition-row button[aria-current=true]",x=>x.dataset.coordinate);
  expect(copied).not.toBe(section);
  await compositionAction("Move up");
  await tab.waitForFunction(()=>document.querySelector(".authoring-composition-status")?.textContent?.includes("Composition updated"),
   {timeout:20000});
  await compositionAction("Remove");
  await tab.waitForFunction(()=>document.querySelectorAll(".authoring-composition-row").length===3,
   {timeout:20000});
  // Metadata is also typed directly by the account, not copied from an import.
  const meta={title:"The Blank-Page Journal",heading:"An original article",
   lede:"Created entirely in the account composer",editorial_kind:"article",
   media_subject_ref:"article:blank-page-journal"};
  for(const [key,value]of Object.entries(meta))
   await tab.$eval('.authoring-page-settings [name="'+key+'"]',(input,text)=>{
    input.value=text;input.dispatchEvent(new Event("input",{bubbles:true}));
   },value);
  await action("Review");
  await tab.waitForSelector(".authoring-review[open]",{timeout:20000});
  const preview=await tab.$eval(".authoring-review-preview",node=>node.srcdoc);
  expect(preview).toContain("This entire article was written using the account editor.");
  expect(preview).toContain("A human-composed feature");
  expect(preview).toContain('data-phone-layout="grid" data-phone-columns="3"');
  await tab.click(".authoring-review-devices button:last-child");
  expect(await tab.$eval(".authoring-review-preview",x=>x.dataset.viewport)).toBe("phone");
  await tab.click(".authoring-review-close");
  await action("Save draft");
  await tab.waitForFunction(()=>{
   const s=document.querySelector(".authoring-status");
   return s?.textContent?.includes("Draft saved")||s?.dataset.error==="true";
  },{timeout:30000});
  expect(await tab.$eval(".authoring-status",x=>x.textContent)).toContain("Draft saved");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie}));
  expect(saved.snapshot.page.status).toBe("draft");
  expect(saved.snapshot.page.media_subject_ref).toBe("article:blank-page-journal");
  expect(saved.snapshot.fragments.map(x=>x.kind)).toEqual(["profile-story","ui:section","ui:card"]);
  const frag=saved.snapshot.fragments.find(x=>x.coordinate===section);
  const tags=typeof frag.semantic_tags==="string"?JSON.parse(frag.semantic_tags):frag.semantic_tags;
  expect(tags.authoring_v1.responsive.phone).toEqual({layout:"grid",columns:3});
  expect(frag.heading).toBe("A human-composed feature");
  const story=saved.snapshot.fragments.find(x=>x.kind==="profile-story");
  const storyMeta=typeof story.semantic_tags==="string"?JSON.parse(story.semantic_tags):story.semantic_tags;
  expect(storyMeta.authoring_v1.editorial[0].data.text).toContain("entire article was written");
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000});
  await modeReady("grapesjs");
  expect(await tab.$$eval(".authoring-composition-row",x=>x.length)).toBe(3);
  expect(await tab.$eval('.authoring-page-settings [name=title]',x=>x.value)).toBe("The Blank-Page Journal");
  expect(errors).toEqual([]);
 }finally{await context.close();}
},120000);

test("profile article entry creates an original unsaved journal and keeps its draft address",async()=>{
 const ctx=await browser.createBrowserContext(),tab=await ctx.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...value]=writer.cookie.split("=");
 const host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:value.join("="),url:host,httpOnly:true});
 try{
  expect((await tab.goto(host+"/cms/compose/advanced?new=article",
   {waitUntil:"networkidle0",timeout:30000})).status()).toBe(200);
  await tab.waitForSelector(".authoring-article .ce-paragraph",{timeout:25000});
  const current=new URL(tab.url()),route=current.searchParams.get("route");
  expect(route).toMatch(/^\/journal\/draft-[a-f0-9]{20}\/$/);
  expect(current.searchParams.has("new")).toBe(false);
  expect(await tab.$eval('.authoring-page-settings [name=editorial_kind]',x=>x.value)).toBe("article");
  const identity="article:"+route.split("/").filter(Boolean).at(-1);
  expect(await tab.$eval('.authoring-page-settings [name=media_subject_ref]',x=>x.value)).toBe(identity);
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie})).id).toBe(0);
  await tab.$eval(".authoring-article .ce-paragraph",node=>{
   node.focus();node.textContent="Original content written through the visible account action.";
   node.dispatchEvent(new InputEvent("input",{bubbles:true,inputType:"insertText"}));
  });
  for(const key of ["title","heading"])
   await tab.$eval('.authoring-page-settings [name="'+key+'"]',(node,value)=>{
    node.value=value;node.dispatchEvent(new Event("input",{bubbles:true}));
   },"An original account article");
  await tab.evaluate(()=>{
   const save=[...document.querySelectorAll(".authoring-actions button")]
    .find(n=>n.textContent==="Save draft");
   if(!save)throw Error("Draft action unavailable");
   save.click();
  });
  await tab.waitForFunction(()=>{
   const s=document.querySelector(".authoring-status");
   return s?.textContent?.includes("Draft saved")||s?.dataset.error==="true";
  },{timeout:30000});
  expect(await tab.$eval(".authoring-status",x=>x.textContent)).toContain("Draft saved");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:writer.cookie}));
  expect(saved.snapshot.page.status).toBe("draft");
  expect(saved.snapshot.page.title).toBe("An original account article");
  expect(saved.snapshot.page.media_subject_ref).toBe(identity);
  expect(saved.snapshot.fragments.map(x=>x.kind)).toEqual(["profile-story"]);
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),
   {cookie:other.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},90000);
test("user drags visible components and reparents a card without import or data injection",async()=>{
 const route="/gesture-composition/";
 const ctx=await browser.createBrowserContext(),tab=await ctx.newPage(),errors=[];
 tab.on("pageerror",e=>errors.push(e.message));
 const [name,...value]=writer.cookie.split("="),host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:value.join("="),url:host,httpOnly:true});
 const count=async n=>tab.waitForFunction(n=>
  document.querySelectorAll(".authoring-composition-row").length===n,{timeout:20000},n);
 try{
  expect((await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000})).status()).toBe(200);
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Editing grapesjs"),
   {timeout:25000});
  for(const [kind,n]of [["ui:section",1],["ui:section",2],["ui:card",3]]){
   await tab.select(".authoring-component-picker",kind);
   await tab.click(".authoring-insert");
   await count(n);
  }
  const [first,second,card]=await tab.$$eval(".authoring-composition-row button[data-coordinate]",
   nodes=>nodes.map(node=>node.dataset.coordinate));
  await tab.select(".authoring-composition-move select[name=composition-parent]",first);
  await tab.click(".authoring-composition-move button");
  await tab.waitForFunction(({first,card,second})=>{
   const rows=[...document.querySelectorAll(".authoring-composition-row button[data-coordinate]")]
    .map(x=>x.dataset.coordinate);
   return rows.join("|")===[first,card,second].join("|");
  },{timeout:20000},{first,card,second});
  // Physical browser gesture only: an active Puck rerender may detach a
  // draggable row mid-gesture. Retry fresh coordinates; no synthetic drop,
  // tree mutation, injected editor data, or import bypasses this acceptance.
   await tab.$eval('.authoring-composition-row button[data-coordinate="'+first+'"]',
    node=>node.closest("li").scrollIntoView({block:"center",inline:"nearest"}));
  let completed=false;
  for(let attempt=0;attempt<3&&!completed;attempt++){
   const source=await tab.$eval('.authoring-composition-row button[data-coordinate="'+second+'"]',
    x=>{const b=x.closest("li").getBoundingClientRect();
     return{x:b.x+Math.min(110,b.width/2),y:b.y+b.height/2};});
   const target=await tab.$eval('.authoring-composition-row button[data-coordinate="'+first+'"]',
    x=>{const b=x.closest("li").getBoundingClientRect();
     return{x:b.x+Math.min(110,b.width/2),y:b.y+Math.min(5,b.height/4)};});
   await tab.mouse.move(source.x,source.y);
   await tab.mouse.down();
   await tab.mouse.move(source.x+12,source.y+12,{steps:10});
   await tab.mouse.move(target.x,target.y,{steps:22});
   await tab.mouse.up();
   completed=await tab.waitForFunction(({second,first,card})=>{
    const rows=[...document.querySelectorAll(".authoring-composition-row button[data-coordinate]")]
     .map(x=>x.dataset.coordinate);
    return rows.join("|")===[second,first,card].join("|");
   },{timeout:5500},{second,first,card}).then(()=>true).catch(()=>false);
   if(!completed){
    const status=await tab.$eval(".authoring-status",
     n=>({error:n.dataset.error,message:n.textContent}));
    if(status.error==="true")throw Error("Native drag refused: "+status.message);
   }
  }
  expect(completed).toBe(true);
  for(const [name,value]of [["title","Original composed gesture"],["heading","Original composed gesture"]])
   await tab.$eval('.authoring-page-settings [name="'+name+'"]',(x,v)=>{
    x.value=v;x.dispatchEvent(new Event("input",{bubbles:true}));
   },value);
  await tab.evaluate(()=>{
   [...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent==="Save draft").click();
  });
  await tab.waitForFunction(()=>{
   const n=document.querySelector(".authoring-status");
   return n?.textContent?.includes("Draft saved")||n?.dataset.error==="true";
  },{timeout:30000});
  expect(await tab.$eval(".authoring-status",x=>x.textContent)).toContain("Draft saved");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),{cookie:writer.cookie}));
  expect(saved.snapshot.fragments.map(x=>x.coordinate)).toEqual([second,first,card]);
  const nested=saved.snapshot.fragments.find(x=>x.coordinate===card);
  const metadata=typeof nested.semantic_tags==="string"?JSON.parse(nested.semantic_tags):nested.semantic_tags;
  expect(metadata.authoring_v1.parent).toBe(first);
  expect(saved.snapshot.page.status).toBe("draft");
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},90000);
test("account workspace composes, reviews, saves and starts a new page without import",async()=>{
 const ctx=await browser.createBrowserContext(),tab=await ctx.newPage(),errors=[];
 tab.on("pageerror",error=>errors.push(error.message));
 const [name,...bits]=writer.cookie.split("="),host="http://aurekai.life:"+f.port;
 await tab.setCookie({name,value:bits.join("="),url:host,httpOnly:true});
 const route="/studio-workflow-proof/";
 const clickAction=label=>tab.evaluate(text=>{
  const button=[...document.querySelectorAll(".authoring-actions button")]
   .find(x=>x.textContent?.trim()===text);
  if(!button||button.disabled)throw Error("Unavailable editor action: "+text);
  button.click();
 },label);
 try{
  expect((await tab.goto(host+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"networkidle0",timeout:30000})).status()).toBe(200);
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Editing grapesjs"),
   {timeout:25000});
  const shell=await tab.evaluate(()=>({
   areas:getComputedStyle(document.querySelector(".aurekai-authoring")).gridTemplateAreas,
   inspector:!!document.querySelector(".authoring-inspector-rail .authoring-composition"),
   palette:!!document.querySelector(".authoring-inspector-rail .authoring-component-picker"),
   empty:!document.querySelector(".authoring-empty-state").hidden,
   state:document.querySelector(".authoring-save-state").textContent
  }));
  expect(shell.areas).toContain("canvas inspector");
  expect(shell.inspector).toBe(true);
  expect(shell.palette).toBe(true);
  expect(shell.empty).toBe(true);
  expect(shell.state).toBe("New unsaved page");
  expect(await tab.$eval('.authoring-workflow-step[data-step=compose]',x=>x.dataset.state)).toBe("active");
  await tab.click(".authoring-empty-actions button:last-child");
  await tab.waitForFunction(()=>document.querySelectorAll(".authoring-composition-row").length===1,
   {timeout:20000});
  expect(await tab.$eval(".authoring-empty-state",x=>x.hidden)).toBe(true);
  expect(await tab.$eval('.authoring-workflow-step[data-step=compose]',x=>x.dataset.state)).toBe("complete");
  await tab.click(".authoring-device-controls button:last-child");
  expect(await tab.$eval(".authoring-device-controls button:last-child",x=>x.getAttribute("aria-pressed"))).toBe("true");
  await tab.click(".authoring-device-controls button:first-child");
  expect(await tab.$eval(".authoring-device-controls button:first-child",x=>x.getAttribute("aria-pressed"))).toBe("true");
  await tab.$eval('.authoring-page-settings [name=title]',node=>{
   node.value="Composed through Aurekai";node.dispatchEvent(new Event("input",{bubbles:true}));
  });
  expect(await tab.$eval(".authoring-save-state",x=>x.dataset.state)).toBe("unsaved");
  await clickAction("Review");
  await tab.waitForSelector(".authoring-review[open]",{timeout:15000});
  expect(await tab.$eval('.authoring-workflow-step[data-step=review]',x=>x.dataset.state)).toBe("complete");
  await tab.click(".authoring-review-close");
  await tab.$eval('.authoring-page-settings [name=title]',node=>{
   node.value="Composed and revised";node.dispatchEvent(new Event("input",{bubbles:true}));
  });
  expect(await tab.$eval('.authoring-workflow-step[data-step=review]',x=>x.dataset.state)).not.toBe("complete");
  await clickAction("Save draft");
  await tab.waitForFunction(()=>document.querySelector(".authoring-status")?.textContent?.includes("Draft saved"),
   {timeout:30000});
  expect(await tab.$eval(".authoring-save-state",x=>x.textContent)).toBe("Saved private draft");
  expect(await tab.$eval('.authoring-workflow-step[data-step=save]',x=>x.dataset.state)).toBe("complete");
  const saved=json(await f.request("/cms/compose/page?route="+encodeURIComponent(route),{cookie:writer.cookie}));
  expect(saved.snapshot.page.status).toBe("draft");
  expect(saved.snapshot.page.title).toBe("Composed and revised");
  expect(saved.snapshot.fragments.map(x=>x.kind)).toEqual(["ui:section"]);
  await tab.waitForFunction(r=>[...document.querySelectorAll(".authoring-page-list button")]
   .some(x=>x.dataset.route===r&&x.dataset.status==="draft"),{timeout:14000},route);
  await tab.click(".authoring-page-create-actions button:first-child");
  await tab.waitForFunction(()=>/\/pages\/draft-[a-f0-9]{20}\/$/.test(new URL(location.href).searchParams.get("route")||""),
   {timeout:25000});
  await tab.waitForFunction(()=>document.querySelector(".authoring-save-state")?.textContent==="New unsaved page",
   {timeout:25000});
  const draftRoute=new URL(tab.url()).searchParams.get("route");
  expect(draftRoute).toMatch(/^\/pages\/draft-[a-f0-9]{20}\/$/);
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(draftRoute),
   {cookie:writer.cookie})).id).toBe(0);
  await tab.click(".authoring-page-create-actions button:last-child");
  await tab.waitForSelector(".authoring-article .ce-paragraph",{timeout:25000});
  const articleRoute=new URL(tab.url()).searchParams.get("route");
  expect(articleRoute).toMatch(/^\/journal\/draft-[a-f0-9]{20}\/$/);
  expect(await tab.$eval('.authoring-page-settings [name=editorial_kind]',x=>x.value)).toBe("article");
  expect(json(await f.request("/cms/compose/page?route="+encodeURIComponent(articleRoute),
   {cookie:writer.cookie})).id).toBe(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},120000);
