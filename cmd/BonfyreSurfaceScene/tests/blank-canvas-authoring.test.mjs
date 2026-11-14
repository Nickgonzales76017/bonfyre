import {test,expect} from "bun:test";
import assert from "node:assert/strict";
import {readFileSync,mkdtempSync,rmSync,existsSync} from "node:fs";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {createRequire} from "node:module";
import {AUTHORING_PATTERNS,defaultPatternPayload}
 from "../../../components/BonfyreServiceSite/authoring/presentation-patterns.mjs";
import {makePuckConfig,registerGrapesComponents}
 from "../../../components/BonfyreServiceSite/authoring/unified-editor.mjs";
import {newAuthoringTree,applyAuthoringEdit,validateAuthoringTree,
  toPuckData,fromPuckData,toGrapesProject,fromGrapesProject}
 from "../runtime/authoring-tree.mjs";
import {toNativePageWrite,treeFromNativeSnapshot}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {renderAuthoredProfilePreview}
 from "../../../components/BonfyreServiceSite/authored-page-renderer.mjs";
import {writeAuthoredCandidate}
 from "../../../components/BonfyreServiceSite/authored-static-candidate.mjs";
import {generatedNativeTree}
 from "../tools/compile_authoring_native_tree.mjs";
const revision="a".repeat(64);
function blankWitness(site_id="person-creator-work",page="/reference/"){
 let doc=newAuthoringTree(site_id,page);
 doc=applyAuthoringEdit(doc,{type:"insert",block:{
  id:"documentation-root",kind:"ui:documentation",
  props:{heading:"Built entirely in Aurekai",payload:{title:"Native component manual",
   lede:"Composed without importing Bootstrap source",toc:[{label:"Overview",href:"#content"}]}},
  responsive:{desktop:{layout:"stack"}},children:[]}});
 for(const spec of AUTHORING_PATTERNS){
  const node={id:"pattern-"+spec.name,kind:spec.kind,
   props:{heading:spec.name+" written here",payload:defaultPatternPayload(spec.kind)},
   responsive:spec.children?{desktop:{layout:"grid",columns:2},phone:{layout:"stack"}}:{},
   children:[]};
  doc=applyAuthoringEdit(doc,{type:"insert",parent:"documentation-root",block:node});
 }
 const hero=doc.blocks[0].children.find(x=>x.kind==="ui:hero");
 hero.props.payload.title="Create the complete component library yourself";
 const tabs=doc.blocks[0].children.find(x=>x.kind==="ui:nav-tabs");
 tabs.props.payload.tabs=[{label:"First tab",body:"A real first panel"},{label:"Second tab",body:"Changed with one click"}];
 const table=doc.blocks[0].children.find(x=>x.kind==="ui:table");
 table.props.payload.rows=[["Components",AUTHORING_PATTERNS.length],["Source","User-authored"]];
 return validateAuthoringTree(doc);
}
const sourceRoot=new URL("../../../",import.meta.url);
test("empty-canvas creation exposes every registered component in Puck, GrapesJS and native C",()=>{
 assert.ok(AUTHORING_PATTERNS.length>=36,"the original component vocabulary must remain");
 const React={createElement:(type,props,...children)=>({type,props,children})};
 const config=makePuckConfig(React);
 const components=new Map(),blocks=new Map();
 registerGrapesComponents({
  Components:{addType:(kind,value)=>components.set(kind,value)},
  Blocks:{add:(kind,value)=>blocks.set(kind,value)}
 });
 for(const pattern of AUTHORING_PATTERNS){
  assert.equal(config.components[pattern.kind].fields.payload.type,"custom",pattern.kind);
  assert.equal(components.get("aurekai:"+pattern.kind).model.defaults.droppable,pattern.children);
  assert.equal(blocks.get("aurekai:"+pattern.kind).category,pattern.group);
  assert.ok(Object.hasOwn(config.components[pattern.kind].defaultProps,"payload"));
 }
 const header=readFileSync(new URL("../../BonfyreWebAddress/generated/native_authoring_tree.h",import.meta.url),"utf8");
 assert.equal(header,generatedNativeTree(),"C grammar must be generated from the same authoring catalogue");
 for(const pattern of AUTHORING_PATTERNS)
  assert.ok(header.includes('{"'+pattern.kind+'",'+Number(pattern.children)+'}'),pattern.kind+" missing from native C");
});
test("user-created nested documentation survives every editor projection and native CMS, without an importer",()=>{
 for(const site_id of ["person-creator-work","person-unrelated-life"]){
  const original=blankWitness(site_id);
  const puck=fromPuckData(toPuckData(original),site_id,original.page);
  const grapes=fromGrapesProject(toGrapesProject(original),site_id,original.page);
  assert.deepEqual(puck,original);
  assert.deepEqual(grapes,original);
  const empty={schema:"bonfyre.page-snapshot.v1",id:0,digest:revision,
   snapshot:{namespace:site_id,route:original.page,page:null,fragments:[]}};
  const request=toNativePageWrite(empty,original,{pageDraft:{title:"Manual",heading:"Manual"}});
  assert.equal(request.fragments.length,AUTHORING_PATTERNS.length+1);
  assert.ok(request.fragments.every(f=>f.status==="draft"));
  assert.ok(request.fragments.every(f=>f.projection_contract?.html===true));
  const snapshot={schema:empty.schema,id:10,digest:"b".repeat(64),snapshot:{
   namespace:site_id,route:original.page,page:{...request.page,id:10},fragments:request.fragments}};
  const reopened=treeFromNativeSnapshot(snapshot);
  assert.deepEqual(reopened.blocks[0].children.map(x=>x.kind),original.blocks[0].children.map(x=>x.kind));
  assert.equal(reopened.blocks[0].children.find(x=>x.kind==="ui:hero").props.payload.title,
   "Create the complete component library yourself");
  assert.equal(reopened.blocks[0].children.find(x=>x.kind==="ui:nav-tabs").props.payload.tabs[1].body,
   "Changed with one click");
 }
});
test("native preview renders all authored patterns with accessible interactive semantics, no imported HTML",()=>{
 const rendered=renderAuthoredProfilePreview(blankWitness(),{name:"Author's site"});
 assert.equal(rendered.sections,1);
 assert.match(rendered.html,/data-ak-id=/);
 assert.match(rendered.html,/role="tablist"/);
 assert.match(rendered.html,/role="tabpanel"/);
 assert.match(rendered.html,/<dialog /);
 assert.match(rendered.html,/data-ak-slide/);
 assert.match(rendered.html,/data-ak-dialog/);
 assert.match(rendered.html,/data-ak-effect="review-required"/);
 assert.match(rendered.html,/data-ak-menu/);
 assert.match(rendered.html,/pattern-runtime\.js/);
 assert.match(rendered.html,/noindex,nofollow/);
 assert.doesNotMatch(rendered.html,/<script>.*user/i);
});
test("offline candidate is built from a blank, CMS-compatible document with one reusable controller",()=>{
 const site=blankWitness();
 const empty={schema:"bonfyre.page-snapshot.v1",id:0,digest:revision,
  snapshot:{namespace:site.site_id,route:site.page,page:null,fragments:[]}};
 const write=toNativePageWrite(empty,site,{pageDraft:{title:"Manual",heading:"Manual"}});
 const snapshot={schema:empty.schema,id:10,digest:"b".repeat(64),snapshot:{
  namespace:site.site_id,route:site.page,page:{...write.page,id:10},fragments:write.fragments}};
 const folder=mkdtempSync(join(tmpdir(),"authoring-parity-"));
 try{
  const target=join(folder,"candidate");
  const receipt=writeAuthoredCandidate(snapshot,{name:"Author's site",output:target});
  assert.equal(receipt.preview_only,true);
  assert.ok(existsSync(join(target,"assets","pattern-runtime.js")));
  const page=readFileSync(join(target,"reference","index.html"),"utf8");
  const manifest=readFileSync(join(target,"CANDIDATE.tsv"),"utf8");
  assert.match(page,/Native component manual/);
  assert.match(page,/pattern-runtime\.js/);
  assert.match(manifest,/pattern_runtime_sha256\t[a-f0-9]{64}/);
  assert.match(manifest,/noindex-preview-only/);
 }finally{rmSync(folder,{force:true,recursive:true});}
},45000);
test("real Chromium can operate tabs, carousel, dialog, dismiss, and range on user-authored content",async()=>{
 const Puppeteer=createRequire(join(sourceRoot.pathname,"tools/creator-ingest/package.json"))("puppeteer");
 const browser=await Puppeteer.launch({executablePath:"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  headless:true,args:["--no-sandbox"]});
 try{
  const page=await browser.newPage();
  await page.setContent(renderAuthoredProfilePreview(blankWitness(),{name:"Builder"}).html,{
   waitUntil:"domcontentloaded"});
  await page.addScriptTag({content:readFileSync(new URL("../../../components/BonfyreServiceSite/authoring/pattern-runtime.js",import.meta.url),"utf8")});
  await page.click("[data-ak-tab='1']");
  assert.equal(await page.$eval("[data-ak-panel='1']",x=>x.hidden),false);
  assert.equal(await page.$eval("[data-ak-panel='0']",x=>x.hidden),true);
  await page.click("[data-ak-next]");
  assert.equal(await page.$eval("[data-ak-slide='1']",x=>x.hidden),false);
  await page.click("[data-ak-dialog]");
  assert.equal(await page.$eval(".ak-modal dialog",x=>x.open),true);
  await page.click(".ak-modal [data-ak-close]");
  assert.equal(await page.$eval(".ak-modal dialog",x=>x.open),false);
  await page.click(".ak-alert [data-ak-dismiss]");
  assert.equal(await page.$eval(".ak-alert .ak-alert",x=>x.hidden),true);
  await page.$eval("[data-ak-range]",x=>{
   x.value="75";x.dispatchEvent(new Event("input",{bubbles:true}));
  });
  assert.equal(await page.$eval(".ak-range output",x=>x.value),"75");
  assert.equal(await page.$eval(".ak-form button[type=submit]",x=>x.disabled),true);
  await page.close();
 }finally{await browser.close();}
},50000);
