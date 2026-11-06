import assert from "node:assert/strict";
import {readFileSync,mkdtempSync,rmSync} from "node:fs";
import {execFileSync} from "node:child_process";
import {createRequire} from "node:module";
import {join,resolve} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";

// Real C-emitted composition document, interpreted in Chromium. No fake backend.
const root=resolve(fileURLToPath(new URL("../../../",import.meta.url)));
const require=createRequire(join(root,"tools/creator-ingest/package.json"));
const puppeteer=require("puppeteer");
const dir=mkdtempSync(join(tmpdir(),"bonfyre-native-experience-"));
let browser;
try{
 const binary=join(dir,"compiled-composition");
 execFileSync("cc",["-std=c11","-Wall","-Wextra","-Werror",
   join(root,"cmd/BonfyreWebAddress/tests/native_experience_composition_test.c"),"-o",binary],{cwd:root});
 const html=execFileSync(binary,["--html"],{encoding:"utf8"});
 assert(html.includes("data-ak-experience=\"media\""));
 browser=await puppeteer.launch({executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
   headless:true,args:["--no-sandbox","--disable-dev-shm-usage","--disable-background-networking"]});
 const page=await browser.newPage(),errors=[],writes=[];
 page.on("pageerror",error=>errors.push(error.message));
 page.on("request",request=>{if(request.method()!=="GET")writes.push(request.url());});
 await page.setViewport({width:1280,height:800});
 await page.setContent(html,{waitUntil:"domcontentloaded"});
 assert.equal(await page.$$eval("[data-ak-region]",nodes=>nodes.length),3);
 assert.deepEqual(await page.$$eval("[data-ak-region]",nodes=>nodes.map(n=>n.dataset.expLane)),
   ["hero","rail","wide"]);
 assert.equal(await page.$eval("[data-exp-controls]",n=>n.hidden),false);
 assert.equal(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.length),3);
 await page.click("[data-exp-mode=focus]");
 assert.equal(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.length),1);
 assert.equal(await page.$eval("[data-ak-experience]",n=>n.dataset.expView),"focus");
 await page.click("[data-exp-mode=compare]");
 assert.equal(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.length),2);
 assert.equal(await page.$eval("[data-ak-experience]",n=>n.dataset.expView),"compare");
 await page.keyboard.press("Escape");
 assert.equal(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.length),3);
 assert.equal(await page.$eval("[data-ak-experience]",n=>n.dataset.expView),"overview");
 await page.click("[data-ak-region=\"2\"] [data-exp-focus]");
 assert.deepEqual(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.map(n=>n.dataset.akRegion)),["2"]);
 await page.click("[data-exp-mode=overview]");
 assert.equal(await page.$eval("[data-exp-search]",node=>node.getAttribute("aria-label")),"Find in published sections");
 await page.type("[data-exp-search]","Screening catalogue");
 assert.deepEqual(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.map(n=>n.dataset.akRegion)),["2"],
   "Search must narrow only the C-emitted published regions");
 assert.equal(await page.$eval("[data-exp-status]",node=>node.textContent),"1 of 3 published sections match");
 await page.keyboard.press("Escape");
 assert.equal(await page.$eval("[data-exp-search]",node=>node.value),"","Escape clears the local search without requesting data");
 await page.click("[data-exp-mode=overview]");
 await page.evaluate(()=>document.querySelector("[data-exp-search]").blur());
 await page.keyboard.press("/");
 assert.equal(await page.evaluate(()=>document.activeElement?.hasAttribute("data-exp-search")),true,
   "Slash reaches the local search from a non-editable focus");
 await page.type("[data-exp-search]","not an owner record");
 assert.equal(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.length),0,
   "An unmatched search must not materialize a fake record");
 await page.keyboard.press("Escape");
 assert.equal(await page.$eval("[data-exp-lanes]",n=>n.hidden),false,"Published lane controls are usable");
 assert.equal(await page.$eval("[data-exp-lane-filter=primary]",n=>n.hidden),true,
   "Unpublished lanes must never be offered as real facets");
 await page.click("[data-exp-lane-filter=rail]");
 assert.deepEqual(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.map(n=>n.dataset.akRegion)),["1"],
   "Related lens reveals only real rail material");
 assert.equal(await page.$eval("[data-exp-lane-filter=rail]",n=>n.getAttribute("aria-pressed")),"true");
 await page.keyboard.press("Escape");
 assert.deepEqual(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.map(n=>n.dataset.akRegion)),["0","1","2"],
   "Escape restores the published owner sequence");
 await page.click("[data-exp-lane-filter=wide]");
 assert.deepEqual(await page.$$eval("[data-ak-region]:not([hidden])",nodes=>nodes.map(n=>n.dataset.akRegion)),["2"]);
 await page.click("[data-exp-lane-filter=all]");
 for(const width of [390,768,1365]){
   await page.setViewport({width,height:820});
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,
     "Native experience must fit viewport "+width);
 }
 assert.deepEqual(errors,[],"No runtime script errors");
 assert.deepEqual(writes,[],"Read-only presentation cannot submit owner effects");
 await page.close();
 console.log("Native C experience Chromium: PASS hero/rail/wide, focus/compare, Escape, 390/768/1365 px, no writes");
}finally{if(browser)await browser.close();rmSync(dir,{recursive:true,force:true});}
