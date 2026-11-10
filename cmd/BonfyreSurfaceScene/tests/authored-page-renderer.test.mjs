import {test} from "node:test";
import assert from "node:assert/strict";
import {newAuthoringTree,validateAuthoringTree}
 from "../runtime/authoring-tree.mjs";
import {renderAuthoredProfilePreview}
 from "../../../components/BonfyreServiceSite/authored-page-renderer.mjs";
import {sectionHelpers}
 from "../../../components/BonfyreServiceSite/section-helpers.mjs";
import {kinds}
 from "../../../components/BonfyreServiceSite/section-registry.mjs";
import {renderBrowserReview} from "../../../components/BonfyreServiceSite/authoring/review-preview.mjs";
import {NATIVE_EDITORIAL_TEMPLATES} from "../../../components/BonfyreServiceSite/authoring/native-component-templates.mjs";
const make=(site,name)=>{
 const doc=newAuthoringTree(site);
 doc.blocks.push({
  kind:"profile-section",id:"portfolio",props:{heading:name+" — selected work"},
  responsive:{desktop:{layout:"grid",columns:2}},children:[
   {id:"opening",kind:"service:hero",props:{heading:"Native source-backed sites",
    payload:{headline:"Real *publications* and websites",
     lede:"Provenance and native publishing, not copied HTML"}},children:[]},
   {id:"studies",kind:"service:cases",props:{heading:"Case studies",
    payload:{title:"Selected programmes",items:[
     {title:"Portable authoring",text:"One source to public and static.",
      icon:"article",href:"/blog/"},
     {title:"Bound commerce",text:"Content stays attached to its source.",
      icon:"shopping_bag"}]}},children:[]},
   {id:"notes",kind:"service:prose",props:{heading:"The work",
    payload:{title:"Research notes",items:[
     {title:"Architecture",text:"A source-managed site renderer."},
     {title:"Publication",text:"Revision checked and traceable."}]}},children:[]},
   {id:"editorial",kind:"profile-story",props:{heading:"Original editorial",
    editorial:[{type:"header",data:{text:"Research journal",level:2}},
     {type:"paragraph",data:{text:"Reviewed source & record"}},
     {type:"table",data:{content:[["Source","Published"],["CMS","true"]]}}]},
    children:[]}
  ]});
 return validateAuthoringTree(doc);
};
test("real first-party Hero, Cases and Prose render inside a nested authored profile",()=>{
 const doc=make("person-editor-work","Editor One");
 const view=renderAuthoredProfilePreview(doc,{name:"Editor One"});
 assert.equal(view.site_id,doc.site_id);
 assert.match(view.html,/data-authored-profile="person-editor-work"/);
 assert.match(view.html,/Real <span class="text-secondary">publications<\/span> and websites/);
 assert.match(view.html,/Selected programmes/);
 assert.match(view.html,/Portable authoring/);
 assert.match(view.html,/Research notes/);
 assert.match(view.html,/Research journal/);
 assert.match(view.html,/Reviewed source &amp; record/);
 assert.match(view.html,/<td>CMS<\/td><td>true<\/td>/);
 assert.match(view.html,/name="robots" content="noindex,nofollow"/);
 assert.equal((view.html.match(/class="authored-component"/g)||[]).length,3);
 // This must actually call the already-shipped ServiceSite owner, not a card proxy.
 const sample={id:"notes",kind:"prose",title:"Research notes",items:[
  {title:"Architecture",text:"A source-managed site renderer."},
  {title:"Publication",text:"Revision checked and traceable."}]};
 const exact=kinds.prose.render(sample,{},sectionHelpers);
 assert.ok(view.html.includes(exact),"first-party prose renderer bytes must be reused");
});
test("unrelated account can render equally rich content without touching original",()=>{
 const a=renderAuthoredProfilePreview(make("person-editor-work","Editor One"),{name:"Editor One"});
 const b=renderAuthoredProfilePreview(make("person-maker-life","Maker Two"),{name:"Maker Two"});
 assert.match(b.html,/Maker Two — selected work/);
 assert.equal(b.html.includes("Editor One"),false);
 assert.equal(a.html.includes("Maker Two"),false);
 assert.match(b.html,/data-authored-profile="person-maker-life"/);
});
test("profile renderer rejects incomplete owner payloads and HTML or action bypass",()=>{
 const doc=make("person-editor-work","Editor One");
 delete doc.blocks[0].children[1].props.payload.items;
 assert.throws(()=>renderAuthoredProfilePreview(doc,{name:"Editor One"}),/cases missing required field items/);
 const b=make("person-editor-work","Editor One");
 b.blocks[0].children[0].props.payload.form={id:"effectful-form"};
 assert.throws(()=>renderAuthoredProfilePreview(b,{name:"Editor One"}),/request\/review actions/);
 const c=make("person-editor-work","Editor One");
 c.blocks[0].children[3].props.href="javascript:alert(1)";
 c.blocks[0].children[3].props.label="Read";
 assert.throws(()=>renderAuthoredProfilePreview(c,{name:"Editor One"}),/unsafe URL/);
});
test("ordinary business sites still use exactly the shared presentation helper",async()=>{
 const {renderSite}=await import("../../../components/BonfyreServiceSite/site-renderer.mjs");
 assert.equal(typeof renderSite,"function");
 assert.equal(sectionHelpers.esc("<source> & more"),"&lt;source&gt; &amp; more");
 assert.equal(sectionHelpers.marked("A *clear* title"),'A <span class="text-secondary">clear</span> title');
});

test("all eight Editor.js tools retain identical semantic HTML in review and exported profile",()=>{
 const types=["paragraph","header","quote","code","delimiter","list","checklist","table"];
 assert.deepEqual(NATIVE_EDITORIAL_TEMPLATES.map(row=>row.kind),types);
 const blocks=[
  {type:"paragraph",data:{text:"Reviewed article & evidence"}},
  {type:"header",data:{text:"A chapter",level:4}},
  {type:"quote",data:{text:"Words & context",caption:"Verified author"}},
  {type:"code",data:{code:"if (x < 5) return;"}},
  {type:"delimiter",data:{}},
  {type:"list",data:{style:"ordered",items:["First","Second"]}},
  {type:"checklist",data:{items:[{text:"Confirmed",checked:true},{text:"Next",checked:false}]}},
  {type:"table",data:{content:[["Name","Value"],["CMS","42"]]}}
 ];
 const doc=newAuthoringTree("person-editor-work");
 doc.blocks.push({id:"original-article",kind:"profile-story",props:{
  heading:"Written in the account editor",editorial:blocks},responsive:{},children:[]});
 validateAuthoringTree(doc);
 const preview=renderBrowserReview(doc,{name:"Editor journal"}).html;
 const exported=renderAuthoredProfilePreview(doc,{name:"Editor journal"}).html;
 const fragments=[
  '<p class="ak-editorial-paragraph">Reviewed article &amp; evidence</p>',
  "<h4>A chapter</h4>",
  '<blockquote class="ak-editorial-quote"><p>Words &amp; context</p><cite>Verified author</cite></blockquote>',
  '<pre class="ak-editorial-code"><code>if (x &lt; 5) return;</code></pre>',
  '<hr class="ak-editorial-break">',
  '<ol class="ak-editorial-list"><li>First</li><li>Second</li></ol>',
  '<ul class="ak-editorial-checklist"><li><input type="checkbox" disabled checked> Confirmed</li><li><input type="checkbox" disabled> Next</li></ul>',
  '<div class="ak-editorial-table"><table><tbody><tr><td>Name</td><td>Value</td></tr><tr><td>CMS</td><td>42</td></tr></tbody></table></div>'
 ];
 for(const fragment of fragments){
  assert.ok(preview.includes(fragment),"review missing "+fragment.slice(0,65));
  assert.ok(exported.includes(fragment),"static output missing "+fragment.slice(0,65));
 }
 const unsafe=structuredClone(doc);unsafe.blocks[0].props.editorial.push({
  type:"unknown-custom-html",data:{text:"Must not vanish"}});
 assert.throws(()=>renderBrowserReview(unsafe,{name:"Editor journal"}),/unregistered editorial tool|unknown editorial tool/);
 assert.throws(()=>renderAuthoredProfilePreview(unsafe,{name:"Editor journal"}),/unregistered editorial tool|unknown editorial tool/);
});

test("public composition is continuous and disclosures are deliberate in both compilers",async()=>{
 const {authoredPatternStyles}=await import("../../../components/BonfyreServiceSite/authored-pattern-styles.mjs");
 const {renderAuthoredPattern}=await import("../../../components/BonfyreServiceSite/authored-pattern-renderer.mjs");
 const {NATIVE_AUTHORING_TEMPLATES}=await import("../../../components/BonfyreServiceSite/authoring/native-component-templates.mjs");
 assert.ok(authoredPatternStyles.includes(".authored-site .ak-pattern{border:0;border-radius:0"));
 assert.ok(authoredPatternStyles.includes(".authored-site .ak-card{padding:"));
 assert.ok(authoredPatternStyles.includes(".authored-site .ak-editorial-paragraph"));
 const html=renderAuthoredPattern({id:"answers",kind:"ui:accordion",props:{payload:{items:[{title:"Question",body:"Answer"}]}},children:[]});
 assert.match(html,/<details class="ak-disclosure">/);
 assert.equal(html.includes('<details class="ak-disclosure" open'),false);
 const native=NATIVE_AUTHORING_TEMPLATES.find(x=>x.kind==="ui:accordion");
 assert.ok(native&&!native.start.includes(" open"),"native must not open a disclosure by default");
});
