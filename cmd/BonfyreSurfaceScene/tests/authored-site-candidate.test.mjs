import {test,expect} from "bun:test";
import assert from "node:assert/strict";
import {mkdtempSync,readFileSync,existsSync,rmSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {newAuthoringTree,validateAuthoringTree}
 from "../runtime/authoring-tree.mjs";
import {newAuthoredComponent}
 from "../../../components/BonfyreServiceSite/authoring/component-palette.mjs";
import {toNativePageWrite}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {compileAuthoredSiteCandidate,writeAuthoredSiteCandidate}
 from "../../../components/BonfyreServiceSite/authored-site-candidate.mjs";
const draft=(site_id,route,title,index=0)=>{
 const doc=newAuthoringTree(site_id,route);
 const block=newAuthoredComponent("ui:documentation","page-"+(index+1));
 block.props.payload.title=title;
 block.children=[newAuthoredComponent(index%2?"ui:modal":"ui:nav-tabs",
  "interaction-"+(index+1))];
 doc.blocks=[block];validateAuthoringTree(doc);
 const previous={schema:"bonfyre.page-snapshot.v1",id:0,digest:"a".repeat(64),
  snapshot:{namespace:site_id,route,page:null,fragments:[]}};
 const write=toNativePageWrite(previous,doc,{pageDraft:{title,heading:title,
  lede:"Authored with the actual Aurekai component builder"}});
 return {schema:previous.schema,id:19+index,digest:(index%2?"c":"b").repeat(64),
  snapshot:{namespace:site_id,route,page:{...write.page,id:19+index},
   fragments:write.fragments}};
};
test("same account composes a multi-page static candidate with a GitHub Pages subpath",()=>{
 const a=draft("person-original-work","/","Original homepage");
 const b=draft("person-original-work","/docs/components/","Component reference",1);
 const preview=compileAuthoredSiteCandidate([b,a],{basePath:"/bonfyre"});
 assert.equal(preview.pages.length,2);
 assert.equal(preview.unresolved.length,0);
 assert.equal(preview.directory.route,"/site-pages/");
 const home=preview.pages.find(x=>x.route==="/");
 const doc=preview.pages.find(x=>x.route==="/docs/components/");
 assert.match(home.html,/href="\/bonfyre\/site-pages\/"/);
 assert.match(home.html,/src="assets\/pattern-runtime\.js"/);
 assert.match(doc.html,/href="\.\.\/\.\.\/assets\/site\.css"/);
 assert.match(doc.html,/src="\.\.\/\.\.\/assets\/pattern-runtime\.js"/);
 assert.match(preview.directory.html,/href="\/bonfyre\/docs\/components\/"/);
 assert.match(doc.html,/Component reference/);
 assert.match(home.html,/NOINDEX-CANDIDATE-ONLY|noindex,nofollow/);
});
test("multi-page builder writes one shared CSS and script, page digests, safe catalog",()=>{
 const parent=mkdtempSync(join(tmpdir(),"bonfyre-site-candidate-suite-"));
 try{
  const result=writeAuthoredSiteCandidate([
   draft("person-original-work","/docs/components/","Reference",1),
   draft("person-original-work","/","Home")
  ],{output:join(parent,"site"),basePath:"/bonfyre",requireLinkClosure:true});
  assert.equal(result.candidate,true);
  assert.equal(result.unresolved_links,0);
  assert.equal(result.page_count,2);
  for(const file of ["index.html","docs/components/index.html",
    "site-pages/index.html","assets/site.css","assets/pattern-runtime.js",
    "SITE-PAGES.tsv","FILES.tsv","UNRESOLVED-LINKS.tsv","CANDIDATE.tsv"])
   assert.equal(existsSync(join(parent,"site",file)),true,file);
  const manifest=readFileSync(join(parent,"site","CANDIDATE.tsv"),"utf8");
  assert.match(manifest,/publication\tNOINDEX-CANDIDATE-ONLY/);
  assert.match(manifest,/base_path\t\/bonfyre/);
  assert.match(manifest,/unresolved_links\t0/);
  const pages=readFileSync(join(parent,"site","SITE-PAGES.tsv"),"utf8");
  assert.match(pages,/docs\/components\/\tReference\tc{64}\t[a-f0-9]{64}/);
  const css=readFileSync(join(parent,"site","assets","site.css"));
  assert.ok(css.length>250);
  assert.throws(()=>writeAuthoredSiteCandidate([draft("person-original-work","/","Home")],
   {output:join(parent,"site")}),/already exists/);
 }finally{rmSync(parent,{recursive:true,force:true});}
});
test("site release candidates refuse foreign namespaces, collisions, stale and published rows",()=>{
 const first=draft("person-one-work","/","Home"),second=draft("person-two-life","/docs/","Foreign",1);
 assert.throws(()=>compileAuthoredSiteCandidate([first,second]),/exact committed CMS identity/);
 assert.throws(()=>compileAuthoredSiteCandidate([first,first]),/duplicate CMS page route/);
 const bad=draft("person-one-work","/docs/","Bad",2);
 bad.digest="invalid";
 assert.throws(()=>compileAuthoredSiteCandidate([first,bad]),/exact committed CMS identity/);
 const published=draft("person-one-work","/docs/","Published",3);
 published.snapshot.page.status="published";
 assert.throws(()=>compileAuthoredSiteCandidate([first,published]),/unpublished page draft/);
 assert.throws(()=>compileAuthoredSiteCandidate([first],{basePath:"/bad//prefix"}),/deployment base/);
});
test("unresolved internal links explicitly block closed candidates instead of being claimed working",()=>{
 const data=draft("person-one-work","/","Home");
 const navigation=newAuthoredComponent("ui:navbar","navigation");
 data.snapshot.fragments.push({
  coordinate:"navigation",page:"/",slot:"showcase",audience:"public",
  kind:"ui:navbar",position:2,status:"draft",variant_key:"paper",
  semantic_tags:{authoring_v1:{version:1,parent:null,responsive:navigation.responsive,
   payload:navigation.props.payload}},projection_contract:{html:true}
 });
 const result=compileAuthoredSiteCandidate([data]);
 assert.ok(result.unresolved.some(x=>x.href==="/work/"));
 const tmp=mkdtempSync(join(tmpdir(),"bonfyre-site-link-gate-"));
 try{
  assert.throws(()=>writeAuthoredSiteCandidate([data],{
   output:join(tmp,"candidate"),requireLinkClosure:true}),/unresolved local links/);
  assert.equal(existsSync(join(tmp,"candidate")),false);
 }finally{rmSync(tmp,{recursive:true,force:true});}
});
test("actual exported GitHub Pages-style site navigates and runs its authored components in Chromium",async()=>{
 const root=mkdtempSync(join(tmpdir(),"bonfyre-browser-site-"));
 let server=null,browser=null;
 try{
  const output=join(root,"candidate");
  writeAuthoredSiteCandidate([
   draft("person-original-work","/","Original homepage"),
   draft("person-original-work","/docs/components/","Component manual",1)
  ],{output,basePath:"/bonfyre",requireLinkClosure:true});
  server=Bun.serve({port:0,fetch(request){
   const path=new URL(request.url).pathname;
   if(!path.startsWith("/bonfyre/"))return new Response("Not found",{status:404});
   let relative=path.slice("/bonfyre/".length);
   if(!relative||relative.endsWith("/"))relative+="index.html";
   const filename=join(output,relative);
   if(!filename.startsWith(output+"/")||!existsSync(filename))
    return new Response("Not found",{status:404});
   return new Response(Bun.file(filename));
  }});
  const {createRequire}=await import("node:module");
  const puppeteer=createRequire(join(import.meta.dir,"../../../tools/creator-ingest/package.json"))("puppeteer");
  browser=await puppeteer.launch({
   executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
   headless:true,args:["--no-sandbox"]});
  const tab=await browser.newPage(),errors=[];
  tab.on("pageerror",e=>errors.push(e.message));
  const base="http://127.0.0.1:"+server.port+"/bonfyre/";
  const home=await tab.goto(base,{waitUntil:"networkidle0"});
  expect(home.status()).toBe(200);
  expect(await tab.$eval('meta[name="robots"]',x=>x.content)).toBe("noindex,nofollow");
  await tab.click('[data-ak-tab="1"]');
  expect(await tab.$eval('[data-ak-panel="1"]',x=>x.hidden)).toBe(false);
  const directory=await tab.goto(base+"site-pages/",{waitUntil:"networkidle0"});
  expect(directory.status()).toBe(200);
  const links=await tab.$$eval("main a",nodes=>nodes.map(x=>x.getAttribute("href")));
  expect(links).toContain("/bonfyre/docs/components/");
  const docs=await tab.goto(base+"docs/components/",{waitUntil:"networkidle0"});
  expect(docs.status()).toBe(200);
  expect(await tab.$eval("title",x=>x.textContent)).toContain("Component manual");
  await tab.click("[data-ak-dialog]");
  expect(await tab.$eval(".ak-modal dialog",x=>x.open)).toBe(true);
  await tab.click(".ak-modal [data-ak-close]");
  expect(await tab.$eval(".ak-modal dialog",x=>x.open)).toBe(false);
  expect(errors).toEqual([]);
 }finally{
  if(browser)await browser.close();
  if(server)server.stop(true);
  rmSync(root,{recursive:true,force:true});
 }
},70000);
