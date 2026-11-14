// Native advanced editor acceptance: one account, one retained CMS page, real browser.
import {test,expect} from "bun:test";
import {copyFileSync} from "node:fs";
import {join} from "node:path";
import {createRequire} from "node:module";
import {packSetupFixture,root,password} from "./pack_setup_fixture.mjs";

async function setup(){
 const f=await packSetupFixture(),schemas=join(f.home,"schemas");
 for(const type of ["site_fragment","site_presentation"])
  copyFileSync(join(root,"cmd/BonfyreCMS/content-types",type+".json"),
   join(schemas,type+".json"));
 f.run(f.cms,["schema","migrate","--db",f.db,"--schemas",schemas]);
 f.run(f.auth,["--db",f.authdb,"signup","--email","advanced-author@example.test",
  "--name","Advanced author","--password",password,"--tier","pro"]);
 const author=await f.login("advanced-author@example.test",{signup:false});
 return {...f,author};
}
const snapshot=async(f,route)=>JSON.parse((await f.request(
 "/cms/compose/page?route="+encodeURIComponent(route),{cookie:f.author.cookie})).body);

test("native advanced workspace serves a complete account-scoped editor",async()=>{
 const f=await setup();
 try{
  const route="/advanced-authoring-acceptance/";
  expect((await f.request("/cms/compose/advanced?route="+encodeURIComponent(route))).status).not.toBe(200);
  const opened=await f.request("/cms/compose/advanced?route="+encodeURIComponent(route),
   {cookie:f.author.cookie});
  expect(opened.status).toBe(200);
  expect(opened.body).toContain("data-aurekai-authoring");
  expect(opened.body).toContain("authoring-editor.js");
  for(const asset of ["authoring-editor.js","authoring-shell.css","authoring-grapes.css"])
   expect((await f.request("/assets/"+asset,{cookie:f.author.cookie})).status).toBe(200);
  const base=await snapshot(f,route);
  expect(base.schema).toBe("bonfyre.page-snapshot.v1");
  expect(base.snapshot.namespace).toBe("actor-"+f.author.id);
  expect(base.snapshot.fragments).toEqual([]);
 }finally{await f.close();}
},60000);

test("Chrome composes and saves an actual article through the native three-editor workspace",async()=>{
 const f=await setup();let browser=null;
 try{
  const puppeteer=createRequire(join(root,"tools/creator-ingest/package.json"))("puppeteer");
  browser=await puppeteer.launch({headless:true,
   executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
   args:["--no-sandbox","--disable-dev-shm-usage","--no-proxy-server",
    "--host-resolver-rules=MAP aurekai.life 127.0.0.1"]});
  const context=await browser.createBrowserContext(),page=await context.newPage();
  const errors=[];page.on("pageerror",e=>errors.push(e.message));
  page.on("response",response=>{if(response.status()>=400)errors.push("HTTP "+response.status()+" "+response.url());});
  page.on("console",msg=>{if(msg.type()==="error")errors.push(msg.text());});
  page.setDefaultTimeout(20000);
  // Use the canonical Matrix host for browser asset routing, mapped solely
  // inside this isolated Chrome process to the test server on loopback.
  const origin=f.base.replace("127.0.0.1","aurekai.life");
  const [name,...bits]=f.author.cookie.split("=");
  await page.setCookie({name,value:bits.join("="),url:origin,httpOnly:true,sameSite:"Lax"});
  const route="/advanced-editor-browser/";
  const response=await page.goto(origin+"/cms/compose/advanced?route="+encodeURIComponent(route),
   {waitUntil:"domcontentloaded"});
  expect(response.status()).toBe(200);
  // The browser module loads asynchronously: require a real mount or visible error.
  await page.waitForFunction(()=>document.querySelector('.authoring-error')||
   document.querySelectorAll('.authoring-modes button').length===3,{timeout:30000});
  const probe=await page.evaluate(()=>({
   body:document.body?.textContent.slice(0,400),
   error:document.querySelector(".authoring-error")?.textContent||"",
   modes:[...document.querySelectorAll(".authoring-modes button")].map(n=>n.textContent.trim()),migrationTools:document.querySelectorAll(".authoring-journal-intake").length
  }));
  if(probe.modes.length!==3)throw Error("Editor failed to mount: "+JSON.stringify(probe)+
   " console: "+JSON.stringify(errors));
  expect(probe.error).toBe("");
  expect(probe.modes).toEqual(["Design","Structure","Article"]);
 expect(probe.migrationTools).toBe(0); // Native authoring, not importing, is the default.
  await page.waitForSelector(".authoring-empty-primary");
  await page.click(".authoring-empty-primary");
  await page.waitForSelector(".ce-paragraph",{visible:true});
  const words="An article authored and saved with the shared Bonfyre composer.";
  await page.$eval(".ce-paragraph",(n,value)=>{
   n.textContent=value;n.dispatchEvent(new Event("input",{bubbles:true}));
  },words);
  await page.evaluate(()=>{
   const save=[...document.querySelectorAll(".authoring-actions button")]
    .find(x=>x.textContent.trim()==="Save draft");
   if(!save)throw Error("Native draft save is missing");save.click();
  });
  await page.waitForFunction(()=>document.querySelector(".authoring-status")?.
   textContent.includes("Draft saved at revision"));
  const saved=await snapshot(f,route);
  expect(saved.id).toBeGreaterThan(0);
  expect(saved.snapshot.namespace).toBe("actor-"+f.author.id);
  expect(saved.snapshot.page.status).toBe("draft");
  expect(saved.snapshot.fragments.some(x=>x.kind==="profile-story"&&x.body.includes(words))).toBe(true);
  await page.reload({waitUntil:"domcontentloaded"});
  await page.waitForSelector(".authoring-modes button");
  expect(await page.$eval(".authoring-save-state",x=>x.textContent)).toContain("Saved");
  expect(errors).toEqual([]);
  await context.close();
 }finally{await browser?.close();await f.close();}
},120000);
