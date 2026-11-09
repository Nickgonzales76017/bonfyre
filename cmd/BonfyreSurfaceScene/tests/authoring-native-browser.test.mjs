import {test,expect} from "bun:test";
import {createRequire} from "node:module";
import {join,resolve} from "node:path";
import {readFileSync,mkdirSync} from "node:fs";
const repo=resolve(import.meta.dir,"../../..");
const out=join(repo,"cmd/BonfyreSurfaceScene/build/authoring-previews");
const puppeteer=createRequire(join(repo,"tools/creator-ingest/package.json"))("puppeteer");

test("real native C editorial and profile render at desktop tablet and phone without overflow",async()=>{
 const browser=await puppeteer.launch({headless:true,args:["--no-sandbox","--disable-dev-shm-usage"]});
 try{
  const page=await browser.newPage();
  for(const width of [1440,980,390]){
   await page.setViewport({width,height:900,deviceScaleFactor:1});
   for(const name of ["journal","profile"]){
    const file=join(out,name+".html");
    const response=await page.goto("file://"+file,{waitUntil:"load",timeout:15000});
    expect(response?.status()).toBe(200);
    const geometry=await page.evaluate(()=>({
     body:document.body.scrollWidth,viewport:document.documentElement.clientWidth,
     head:document.title,site:document.body.dataset.site,
     escaped:!document.querySelector("script"),
     number:document.querySelectorAll(".bf-block").length,
     media:document.querySelectorAll("[data-asset-ref]").length,
     anchors:[...document.querySelectorAll("a")].every(a=>a.protocol==="https:")
    }));
    expect(geometry.body).toBeLessThanOrEqual(geometry.viewport+1);
    expect(geometry.escaped).toBe(true);
    expect(geometry.number).toBeGreaterThanOrEqual(4);
    expect(geometry.anchors).toBe(true);
    expect(geometry.site).toBe(name==="journal"?"aurekai-journal":"person-example");
    if(name==="journal"){
     expect(geometry.media).toBe(1);
     const content=await page.$eval("#content",x=>x.textContent);
     expect(content).toContain("The first distinction");
     expect(content).toContain("Testimony");
    }
    await page.screenshot({path:join(out,name+"-"+width+".png"),fullPage:true});
   }
  }
  expect(readFileSync(join(out,"SOURCE.tsv"),"utf8").split("\n").length).toBeGreaterThan(2);
 }finally{await browser.close();}
},65000);
