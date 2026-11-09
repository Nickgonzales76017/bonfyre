import {test,expect} from "bun:test";
import {createRequire} from "node:module";
import {join,resolve} from "node:path";
import {readFileSync,mkdirSync,readdirSync} from "node:fs";
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

test("all 90 true Journal editions have usable native C layouts on desktop, tablet and phone",async()=>{
 const directory=join(repo,"cmd/BonfyreSurfaceScene/build/journal-native");
 const html=readdirSync(directory).filter(x=>x.endsWith(".html")&&x!=="index.html").sort();
 expect(html.length).toBe(90);
 const entries=readFileSync(join(directory,"SOURCE.tsv"),"utf8").trim().split("\n");
 expect(entries.length).toBe(91);
 const browser=await puppeteer.launch({headless:true,args:["--no-sandbox","--disable-dev-shm-usage"]});
 const page=await browser.newPage();
 try{
  for(const width of [1440,980,390]){
   await page.setViewport({width,height:900,deviceScaleFactor:1});
   let inspected=0;
   for(const file of html){
    const response=await page.goto("file://"+join(directory,file),{waitUntil:"load",timeout:12000});
    expect(response?.status()).toBe(200);
    const metric=await page.evaluate(()=>({
     scroll:document.documentElement.scrollWidth,
     viewport:document.documentElement.clientWidth,
     category:document.querySelector(".bf-eyebrow")?.textContent,
     chapters:document.querySelectorAll(".bf-markdown h2").length,
     source:document.querySelector("[data-source-ref]")?.getAttribute("data-source-ref"),
     release:document.body.dataset.release,
     site:document.body.dataset.site,
     forbidden:!!document.querySelector("script,iframe,object"),
     sourceImage:document.querySelectorAll("figure.bf-edition-image img").length,
     credit:document.querySelectorAll("figure.bf-edition-image figcaption").length,
     chapterCover:document.querySelectorAll(".bf-chapter-cover").length
    }));
    expect(metric.scroll).toBeLessThanOrEqual(metric.viewport+1);
    expect(metric.release).toBe("draft-preview-only");
    expect(metric.site).toBe("aurekai-journal");
    expect(metric.forbidden).toBe(false);
    expect(metric.source).toBe("editorial/posts/"+file.slice(0,-5)+".md");
    expect(metric.chapters).toBeGreaterThan(0);
    expect(metric.sourceImage+metric.chapterCover).toBe(1);
    expect(metric.credit).toBe(metric.sourceImage);
    if([0,44,89].includes(inspected)&&width!==980)
     await page.screenshot({path:join(directory,file.slice(0,-5)+"-"+width+".png"),fullPage:true});
    inspected++;
   }
  }
  await page.setViewport({width:390,height:900});
  await page.goto("file://"+join(directory,"index.html"));
  expect(await page.$$eval(".edition",nodes=>nodes.length)).toBe(90);
  const grid=await page.$eval(".grid",e=>getComputedStyle(e).gridTemplateColumns);
  expect(grid.split(" ").length).toBe(1);
  await page.screenshot({path:join(directory,"review-index-phone.png"),fullPage:true});
  console.log("JOURNAL_90_NATIVE_BROWSER_ACCEPTANCE_PASS 90 articles × 3 viewports / 0 overflows");
 }finally{await browser.close();}
},120000);
