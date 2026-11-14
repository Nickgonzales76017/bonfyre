import {test} from "bun:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {kinds} from "../../../components/BonfyreServiceSite/section-registry.mjs";
import {sectionHelpers as h} from "../../../components/BonfyreServiceSite/section-helpers.mjs";
import {resolveTheme} from "../../../components/BonfyreServiceSite/theme.mjs";
import {catalogueFiles} from "../../../components/BonfyreServiceSite/catalogue-material.mjs";
import {collectSectionCatalogue}
 from "../../BonfyreSurfaceScene/tools/compile_authoring_sections.mjs";

// Test the ACTUAL 27 owners. A name in a registry or editable payload alone
// is not a working section. Sixteen come from the real Aurekai Work Samples
// source/package system; the remaining eleven receive independent authored
// content to exercise their executable HTML, rather than stub implementations.
const pack=join(import.meta.dir,"../../../packs/aurekai-work-samples/site");
const configured=JSON.parse(readFileSync(join(pack,"content.json"),"utf8"));
const ownerSpecs=collectSectionCatalogue();
function independent(kind){
 const common={id:"original-"+kind,title:"Authored "+kind,
  lede:"An independent person can author and publish this composition",
  text:"Original user-authored content with retained source",
  sample:{name:"A tested source-backed example"},data:"/data/independent/",
  asset_prefix:"v-"+"a".repeat(64)+"/",
  items:[{title:"Original item",text:"The editor wrote this paragraph",name:"A reviewer",
   quote:"Verified original source",icon:"check_circle",place:"Dallas",price:199}],
  rows:[{what:"Result",ours:"Verified",other:"Unknown",label:"Scope",value:42}],
  other:"A different workflow",
  choices:[{label:"Small",rows:[["Part","2 units"]]}],
  count:1,noun:"entries",source:{name:"Record owner",url:"https://example.org/source"},
  action:"https://example.org/next/",href:"/work/",
  form:{id:"request",fields:[{label:"Question",type:"text",required:true}]},
  contact:{name:"Independent author"},stores:[{id:"one",name:"Source store",
   origin:"https://example.org/"}],entries:[{id:"one",title:"Original",price:19}]};
 if(kind==="library")common.data="/data/library/";
 return common;
}
test("all 27 source-owned ServiceSite sections execute their real renderers",async()=>{
 // Read each canonical material owner independently. One retained proposed
 // product-copy witness currently loops through a .bonfyre symlink; the real
 // product rows are separately retained, and may be compiled WITHOUT falsely
 // claiming that missing optional proposed-copy enrichment was reviewed.
 const prepared=structuredClone(configured);
 const provider=await import(join(pack,"material.mjs"));
 let catalogueEvidenceGap=false;
 for(const section of [...(prepared.sections||[]),
   ...(prepared.pages||[]).flatMap(page=>page.sections||[])]){
  if(!section.material)continue;
  let payload;
  if(section.material==="products"){
   const input=JSON.parse(Buffer.from(
    Bun.gunzipSync(readFileSync(join(pack,"catalogue-input.json.gz")))).toString());
   assert.ok(input.rows.length>0&&input.stores.length>0);
   payload=catalogueFiles(input.rows,input.stores,{
    samples:{},fixtureCatalogues:[provider.fixtures().catalogue]});
   catalogueEvidenceGap=true; // not a publishable review of the missing copy
  }else{
   assert.equal(typeof provider[section.material],"function",
    "a real source material owner must exist: "+section.material);
   payload=await provider[section.material]();
  }
  Object.assign(section,{...payload,files:undefined});
 }
 assert.equal(catalogueEvidenceGap,true,
  "historical copy enrichment cannot be silently represented as reviewed");
 const byKind=new Map();
 for(const s of [...(prepared.sections||[]),
   ...(prepared.pages||[]).flatMap(page=>page.sections||[])]){
  if(!byKind.has(s.kind))byKind.set(s.kind,s);
 }
 const site={...prepared,
  theme:resolveTheme(prepared.theme),page_path:"/",ask:"#ask",
  contact:{name:"Aurekai",email:"contact@example.org",...prepared.contact},
  business:{name:"Aurekai",...prepared.business},
  initials:"AK",phone_tel:"",phone_display:""};
 const kindsSource=Object.keys(kinds).sort();
 assert.deepEqual(kindsSource,ownerSpecs.map(x=>x.kind));
 assert.equal(kindsSource.length,27);
 let actualPackages=0,interactive=0,renderedBytes=0,authored=0;
 for(const spec of ownerSpecs){
  const real=byKind.get(spec.kind);
  if(real)actualPackages++;else authored++;
  const section={...independent(spec.kind),...(real||{}),
   kind:spec.kind,id:real?.id||"original-"+spec.kind};
  if(!section.title&&spec.required.includes("title"))
   section.title="Authored "+spec.kind;
  for(const needed of spec.required)
   assert.ok(section[needed]!==undefined,spec.kind+" missing "+needed);
  let html;
  try{html=kinds[spec.kind].render(section,site,h);}
  catch(e){throw Error("Real owner "+spec.kind+" rejected retained section: "+String(e.message));}
  assert.ok(typeof html==="string"&&html.startsWith("<section"),
   spec.kind+" did not render a real section");
  assert.ok(html.length>150,spec.kind+" is only a stub");
  assert.ok(!/<script\b[^>]*\bsrc=["']javascript:/i.test(html),
   spec.kind+" contains untrusted active script source");
  renderedBytes+=html.length;
  if(/<script\b/.test(html)||/data-(?:catalogue|planner|shipments|library|amounts)\b/.test(html))
   interactive++;
  if(real?.material)
   assert.ok(typeof section.data==="string"&&section.data.startsWith("/data/"),
    spec.kind+" dropped its owner-supplied data package");
 }
 assert.ok(actualPackages>=16,"real deployed samples must cover at least 16 apps");
 assert.ok(authored>=10,"exercise real first-party sections beyond the work-samples package");
 assert.ok(interactive>=10,"complex workspaces must retain operational client markup");
 assert.ok(renderedBytes>60000,"the 27 real rendered modules cannot be miniature placeholders");
});
