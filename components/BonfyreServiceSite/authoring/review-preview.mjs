// Browser-only, CSP-contained review for typed composition. Server profile
// compilation remains the release authority for product-specific sections.
import {validateAuthoringTree} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
import {renderAuthoredPattern,authoredResponsiveAttributes,renderAuthoredEditorial} from "../authored-pattern-renderer.mjs";
import {authoredPatternStyles} from "../authored-pattern-styles.mjs";
import {SITE_SECTION_CATALOGUE} from "./site-section-catalogue.generated.mjs";
import {sectionHelpers as sectionH} from "../section-helpers.mjs";
import * as cases from "../sections/cases.mjs";
import * as prose from "../sections/prose.mjs";
import * as hero from "../sections/hero.mjs";
import * as reviews from "../sections/reviews.mjs";
import * as steps from "../sections/steps.mjs";
import * as proof from "../sections/proof.mjs";
import * as call from "../sections/call.mjs";
import * as person from "../sections/person.mjs";
import * as figures from "../sections/figures.mjs";
import * as compare from "../sections/compare.mjs";
import * as listings from "../sections/listings.mjs";
const serviceOwners={cases,prose,hero,reviews,steps,proof,call,person,figures,compare,listings};
const contracts=new Map(SITE_SECTION_CATALOGUE.map(x=>[x.kind,x]));

const q=text=>String(text??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
function block(node,context){
 const children=node.children.map(child=>block(child,context)).join(""),p=node.props||{};
 if(node.kind.startsWith("ui:"))return renderAuthoredPattern(node,children);
 const heading=p.heading?"<h2>"+q(p.heading)+"</h2>":"";
 const viewport=authoredResponsiveAttributes(node);
 const intro=p.eyebrow?"<small>"+q(p.eyebrow)+"</small>":"";
 if(node.kind.startsWith("service:")){
  const kind=node.kind.slice(8),owner=serviceOwners[kind],contract=contracts.get(kind);
  if(!contract)throw Error("Unknown first-party section "+kind);
  const payload=p.payload||{},section={...payload,kind,id:node.id,
   title:payload.title??p.heading??"",headline:payload.headline??p.heading??"",
   text:payload.text??p.body??"",lede:payload.lede??p.body??"",
   overline:payload.overline??p.eyebrow??""};
  if(section.form||section.review)
   throw Error(kind+" has an unreviewed action; bind a destination before preview");
  if(owner){
   for(const field of contract.required)
    if(section[field]==null||(Array.isArray(section[field])&&!section[field].length))
     throw Error(kind+" needs "+field+" before rendering");
   if(owner.present&&owner.present(section)===false)
    throw Error(kind+" is missing its verified presentation sources");
   const site={site_id:context.site,page_path:context.page,
    business:{name:context.name},contact:{name:context.name},
    theme:{vars:{}},ask:"#content",phone_tel:"",phone_display:""};
   const html=owner.render(section,site,sectionH);
   if(typeof html!=="string"||!html.startsWith("<section")||
      /<script\b|<iframe\b|\bon[a-z]+\s*=/i.test(html))
    throw Error(kind+" must be reviewed through its source-backed server renderer");
   return '<div class="reviewed-service" data-review-node="'+q(node.id)+'"'+viewport+'>'+
    html+children+"</div>";
  }
   throw Error(kind+" requires its source-backed renderer for review; browser placeholders are not publishable.");
 }
 return '<section class="profile-block" data-review-node="'+q(node.id)+'"'+viewport+'>'+intro+heading+
  (p.body?"<p>"+q(p.body)+"</p>":"")+renderAuthoredEditorial(p.editorial)+children+"</section>";
}
export function renderBrowserReview(tree,{name="Unpublished page"}={}){
 validateAuthoringTree(tree);
 if(typeof name!=="string"||name.length>220)throw Error("Invalid preview title");
  const content=tree.blocks.map(node=>block(node,{site:tree.site_id,page:tree.page,name})).join("");
 const styles="*{box-sizing:border-box}body{margin:0;background:#f3f0e8;color:#21352b;font:15px/1.7 system-ui}"+
  "header{padding:20px clamp(20px,5vw,50px);background:#203e33;color:white}header h1{font:500 clamp(28px,5vw,48px)/1.1 Georgia,serif;margin:0}"+
  "main{max-width:1120px;margin:auto;padding:20px}.profile-block,.owned-section{background:white;border:1px solid #cad4c8;padding:clamp(20px,3vw,40px);margin:14px 0;border-radius:9px}"+
  "h2,h3{font-family:Georgia,serif}p{white-space:pre-wrap;overflow-wrap:anywhere;max-width:85ch}blockquote{border-left:4px solid #a87352;padding-left:18px}";
 const html='<!doctype html><html lang="en"><head><meta charset="utf-8">'+
  '<meta name="viewport" content="width=device-width,initial-scale=1">'+
  '<meta http-equiv="Content-Security-Policy" content="default-src &#39;none&#39;; style-src &#39;unsafe-inline&#39;">'+
  "<title>"+q(name)+"</title><style>"+styles+"\n"+authoredPatternStyles+"</style></head><body class=\"authored-site\">"+
  "<header><h1>"+q(name)+"</h1></header><main>"+(content||"<p>This page has no authored blocks yet.</p>")+"</main></body></html>";
 return {html,sections:tree.blocks.length};
}
