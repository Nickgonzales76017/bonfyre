// One pure preview renderer for every existing ServiceSite section plus profile
// nesting and editorial content. Site publication is a separate native effect.
import {validateAuthoringTree} from "../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
import {SITE_SECTION_CATALOGUE} from "./authoring/site-section-catalogue.generated.mjs";
import {sectionHelpers as h,esc} from "./section-helpers.mjs";
import {resolveTheme} from "./theme.mjs";
import {kinds} from "./section-registry.mjs";
import {renderAuthoredPattern,authoredResponsiveAttributes,renderAuthoredEditorial} from "./authored-pattern-renderer.mjs";
import {authoredPatternStyles} from "./authored-pattern-styles.mjs";
const registry=new Map(SITE_SECTION_CATALOGUE.map(x=>[x.kind,x]));
const baseline={ground:"#f9f8f2",ink:"#1a322b",display:"#19382f",accent:"#205945",muted:"#4c6358"};
const fail=s=>{throw Error("authored profile: "+s);};
const object=x=>x!==null&&typeof x==="object"&&!Array.isArray(x);
function safeHref(s){
 if(typeof s!=="string")fail("invalid link");
 if(s[0]==="/"){
  if(s.startsWith("//")||s.includes("..")||/[<>"'\s\\]/.test(s))fail("unsafe relative link");
  return s;
 }
 let url;
 try{url=new URL(s);}catch{fail("invalid link URL");}
 if(url.protocol!=="https:"||url.username||url.password||/[<>"'\s\\]/.test(s))fail("unsafe link URL");
 return s;
}
function blockHTML(node,site){
 const fields=node.props,children=node.children.map(n=>blockHTML(n,site)).join("\n");
 const attributes=" data-node=\""+esc(node.id)+"\""+(node.kind.startsWith("ui:")?"":authoredResponsiveAttributes(node));
 if(node.kind.startsWith("ui:"))return "<div class=\"authored-component\""+attributes+">"+
  renderAuthoredPattern(node,children)+"</div>";
 if(node.kind.startsWith("service:")){
  const kind=node.kind.slice(8),spec=registry.get(kind),owner=kinds[kind];
  if(!spec||!owner)fail("missing first-party section owner "+kind);
  const payload=object(fields.payload)?structuredClone(fields.payload):{};
  const section={...payload,kind,id:node.id,
   title:payload.title??fields.heading??"",
   headline:payload.headline??fields.heading??"",
   text:payload.text??fields.body??"",
   lede:payload.lede??fields.body??"",
   overline:payload.overline??fields.eyebrow??""};
  for(const required of spec.required)
   if(section[required]==null||(Array.isArray(section[required])&&!section[required].length))
    fail(kind+" missing required field "+required);
  if(section.form||section.review)fail("request/review actions need a bound destination");
  const rendered=owner.render(section,site,h);
  if(typeof rendered!=="string"||!rendered.startsWith("<section"))fail("section owner did not render "+kind);
  return "<div class=\"authored-component\""+attributes+">"+rendered+children+"</div>";
 }
 const intro=fields.eyebrow?"<small>"+esc(fields.eyebrow)+"</small>":"";
 const heading=fields.heading?"<h2>"+esc(fields.heading)+"</h2>":"";
 const body=fields.body?"<p>"+esc(fields.body)+"</p>":"";
 const link=fields.label&&fields.href?"<a rel=\"noopener noreferrer\" href=\""+
  esc(safeHref(fields.href))+"\">"+esc(fields.label)+"</a>":"";
 const content=intro+heading+body+renderAuthoredEditorial(fields.editorial)+children+link;
 const kind=node.kind;
 if(!["profile-hero","profile-section","profile-card","profile-story",
       "profile-quote","profile-metric","profile-action"].includes(kind))
  fail("unregistered profile type "+kind);
 const cls="authored-"+kind.slice(8);
 return "<section class=\""+cls+"\""+attributes+">"+content+"</section>";
}
const styles=[
 ".authored-site{color:var(--ink);background:var(--paper);font:16px/1.65 system-ui}",
 ".authored-site *{box-sizing:border-box}",
 ".authored-site .authored-content{max-width:1280px;margin:auto}",
 ".authored-site .authored-head{padding:25px clamp(20px,4vw,50px);display:flex;justify-content:space-between;gap:20px;border-bottom:1px solid #ccd7cb}",
 ".authored-site .authored-head strong{font:700 24px Georgia,serif}",
 ".authored-site .authored-head nav{display:flex;flex-wrap:wrap;gap:18px}",
 ".authored-site .authored-head a{color:inherit}",
 ".authored-site [class^='authored-profile-']{padding:clamp(24px,4vw,72px) clamp(18px,4vw,60px);margin:0;border:0;border-radius:0;background:transparent}",
 ".authored-site .authored-profile-hero{background:#204733;color:#fffef4}",
 ".authored-site .authored-profile-hero h2{font:700 clamp(40px,6vw,72px)/1.08 Georgia,serif}",
 ".authored-site .authored-profile-card{background:#f4f4ee;border-top:1px solid #cad9cb}",
 ".authored-site .authored-profile-quote{border-left:6px solid #ba7150}",
 ".authored-site .authored-profile-section{display:grid;gap:clamp(18px,3vw,40px);border-bottom:1px solid #d4ded3}",
 ".authored-site .authored-table{overflow-x:auto}",
 ".authored-site .authored-table td{padding:9px;border:1px solid #b7c7b8}",
 ".authored-site [data-desktop-layout=grid]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:15px}",
 "@media(max-width:800px){.authored-site .authored-head{flex-direction:column}.authored-site [data-desktop-layout=grid]{display:block}}"
].join("\n");
export function renderAuthoredProfilePreview(doc,{name,contact={},theme={},navigation=[]}={}){
 validateAuthoringTree(doc);
 if(typeof name!=="string"||!name.trim()||name.length>200)fail("site name required");
 if(!object(contact)||!Array.isArray(navigation)||navigation.length>25)fail("invalid profile settings");
 const chosen=resolveTheme({...baseline,...theme});
 const site={site_id:doc.site_id,business:{name},contact:{name,...contact},
  page_path:doc.page,theme:chosen,ask:"#content",phone_tel:"",
  phone_display:"",action:"Explore",initials:name.split(/\s+/).map(x=>x[0]).join("").slice(0,2)};
 const nav=navigation.map(link=>{
  if(!link||typeof link.label!=="string"||link.label.length>90)fail("invalid navigation");
  return "<a href=\""+esc(safeHref(link.href))+"\">"+esc(link.label)+"</a>";
 }).join("");
 const nodes=doc.blocks.map(node=>blockHTML(node,site)).join("\n");
 const hasPatterns=JSON.stringify(doc.blocks).includes('"kind":"ui:');
 const pathDepth=doc.page.split("/").filter(Boolean).length;
 const stylesheet="../".repeat(pathDepth)+"assets/site.css";
 const palette=Object.entries(chosen.vars).map(([key,value])=>key+":"+value).join(";");
 const html="<!DOCTYPE html><html lang=\"en\" data-authored-profile=\""+esc(doc.site_id)+"\">"+
  "<head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"+
  "<meta name=\"robots\" content=\"noindex,nofollow\"><title>"+esc(name)+"</title>"+
  "<link rel=\"stylesheet\" href=\""+esc(stylesheet)+"\"><style>"+styles+"\n"+authoredPatternStyles+"</style>"+
  (hasPatterns?"<script defer src=\""+esc("../".repeat(pathDepth)+"assets/pattern-runtime.js")+"\"></script>":"")+"</head>"+
  "<body class=\"authored-site bg-surface\" style=\""+esc(palette)+"\">"+
  "<a href=\"#content\" class=\"sr-only focus:not-sr-only\">Skip to content</a>"+
  "<header class=\"authored-head\"><strong>"+esc(name)+"</strong><nav aria-label=\"Site\">"+
  nav+"</nav></header><main id=\"content\" class=\"authored-content\">"+
  nodes+"</main></body></html>";
 return {schema:"bonfyre.authored-preview.v1",site_id:doc.site_id,
  page:doc.page,sections:doc.blocks.length,theme:chosen,html};
}
