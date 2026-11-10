// Canonical Aurekai authoring <-> native BonfyreCMS page aggregate.
// This adapter never publishes: it performs exact-namespace, revision-checked
// writes through the existing /cms/compose/page owner and reopens the snapshot.
import {newAuthoringTree,validateAuthoringTree}
 from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
const clean=x=>Object.fromEntries(Object.entries(x||{}).filter(([k])=>!new Set(["id","namespace","created_at","updated_at","published_at"]).has(k)));
const fail=s=>{throw new Error("native authoring: "+s);};
const clone=x=>structuredClone(x);
const digestOK=s=>typeof s==="string"&&/^[a-f0-9]{64}$/.test(s);
const editorialDate=s=>{
 if(typeof s!=="string"||!/^(18|19|20|21)\d\d-\d\d-\d\d$/.test(s))return false;
 const [year,month,day]=s.split("-").map(Number);
 const leap=year%4===0&&(year%100!==0||year%400===0);
 return month>=1&&month<=12&&day>=1&&day<=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31][month-1];
};
const authoringKind=s=>typeof s==="string"&&(/^profile-(?:hero|section|card|story|quote|metric|action)$/.test(s)||/^service:[a-z][a-z-]*$/.test(s)||/^ui:[a-z][a-z-]*$/.test(s));
function snapshot(base){
 if(!base||base.schema!=="bonfyre.page-snapshot.v1"||!digestOK(base.digest)||
  !base.snapshot||typeof base.snapshot.namespace!=="string"||
  !base.snapshot.namespace||typeof base.snapshot.route!=="string"||
  !Array.isArray(base.snapshot.fragments))fail("complete native snapshot is required");
 if(base.id&&(!base.snapshot.page||base.snapshot.page.route!==base.snapshot.route))
  fail("page is not bound to the requested route");
 if(!base.id&&base.snapshot.page!==null)fail("missing page must be null");
 return base.snapshot;
}
function tags(row){
 const value=row?.semantic_tags;
 if(value==null||value==="")return {};
 let obj=value;
 if(typeof obj==="string"){try{obj=JSON.parse(obj);}catch{fail("invalid stored semantic tags");}}
 if(!obj||typeof obj!=="object"||Array.isArray(obj))fail("invalid stored semantic tags");
 return clone(obj);
}
const rowsOf=doc=>{
 const rows=[];
 const walk=(blocks,parent)=>{
  for(const node of blocks){rows.push({node,parent,position:rows.length});walk(node.children,node.id);}
 };
 walk(doc.blocks,null);return rows;
};
function managed(f,route){
 return f&&f.page===route&&f.slot==="showcase"&&authoringKind(f.kind);
}
export function treeFromNativeSnapshot(base){
 const snap=snapshot(base),doc=newAuthoringTree(snap.namespace,snap.route);
 const rows=snap.fragments.filter(f=>managed(f,snap.route));
 const dict=new Map();
 for(const row of rows){
  if(dict.has(row.coordinate))fail("duplicate retained block coordinate");
  const t=tags(row).authoring_v1||{};
  const props={heading:row.heading||"",body:row.body||"",eyebrow:row.eyebrow||"",
   label:row.label||"",href:row.href||"",variant:row.variant_key||"paper",
   source_ref:row.source_ref||"",source_digest:row.source_digest||"",
   ...(row.status==="published"?{readOnly:true}:{}),
   ...(own(t,"editorial")?{editorial:t.editorial}:{}),
   ...(own(t,"payload")?{payload:t.payload}:{})};
  const node={id:row.coordinate,kind:row.kind,props,responsive:t.responsive||{},children:[]};
  dict.set(row.coordinate,{node,parent:t.parent??null,position:row.position});
 }
 for(const [id,row]of dict){
  if(row.parent!==null){
   const target=dict.get(row.parent);
   if(!target)fail("orphaned retained block "+id);
   target.node.children.push(row.node);
  }else doc.blocks.push(row.node);
 }
 const ordered=nodes=>{
  nodes.sort((a,b)=>dict.get(a.id).position-dict.get(b.id).position||a.id.localeCompare(b.id));
  nodes.forEach(b=>ordered(b.children));
 };
 ordered(doc.blocks);
 return validateAuthoringTree(doc);
}
function authoredRow(value,stored,route){
 const {node,parent,position}=value;
 const old=stored?clean(stored):{};
 const baseTags=stored?tags(stored):{};
 const meta={...baseTags,authoring_v1:{version:1,parent,
  responsive:clone(node.responsive),
  ...(own(node.props,"editorial")?{editorial:clone(node.props.editorial)}:{}),
  ...(own(node.props,"payload")?{payload:clone(node.props.payload)}:{})}};
 return {...old,coordinate:node.id,page:route,slot:"showcase",
  audience:stored?.audience||"public",kind:node.kind,position,
  eyebrow:node.props.eyebrow||"",heading:node.props.heading||"",
  body:node.props.body||"",label:node.props.label||"",
  href:node.props.href||"",variant_key:node.props.variant||"paper",
  source_ref:node.props.source_ref||"",source_digest:node.props.source_digest||"",
  semantic_tags:meta,projection_contract:{html:true},status:"draft"};
}
export function toNativePageWrite(base,doc,{pageDraft}={}){
 const snap=snapshot(base);validateAuthoringTree(doc);
 if(doc.site_id!==snap.namespace||doc.page!==snap.route)
  fail("different site or route cannot be edited through this workspace");
 if(snap.page?.status==="published")
  fail("published page is immutable; create a reviewed draft revision");
 const oldRows=snap.fragments.filter(f=>managed(f,snap.route));
 const byId=new Map(oldRows.map(row=>[row.coordinate,row]));
 const elements=rowsOf(doc),incoming=new Map(elements.map(x=>[x.node.id,x]));
 // Preserve previously published fragments exactly; no in-place replacement,
 // reordering, deletion or rights change through an authoring draft save.
 for(const original of oldRows.filter(f=>f.status==="published")){
  const target=incoming.get(original.coordinate);
  if(!target)fail("published block cannot be removed");
  const next=authoredRow(target,original,snap.route);
  for(const key of ["kind","eyebrow","heading","body","label","href","source_ref",
   "source_digest","variant_key","position"]){
    if((next[key]??"")!==(original[key]??""))fail("published block requires a reviewed fork");
  }
  const before=tags(original).authoring_v1;
  if(before&&JSON.stringify(before.responsive??{})!==JSON.stringify(target.node.responsive)||
     before&&JSON.stringify(before.editorial??null)!==JSON.stringify(target.node.props.editorial??null)||
     before&&JSON.stringify(before.payload??null)!==JSON.stringify(target.node.props.payload??null)||
     before&&(before.parent??null)!==(target.parent??null))
    fail("published composition must be forked");
 }
 const authored=elements.map(item=>{
  const original=byId.get(item.node.id);
  return original?.status==="published"?clean(original):authoredRow(item,original,snap.route);
 });
 // Unmanaged records are never touched, even during a whole-page replacement.
 const unrelated=snap.fragments.filter(f=>!managed(f,snap.route)).map(clean);
 if(unrelated.length+authored.length>512)fail("native aggregate exceeds 512 fragments");
 const page=snap.page?clean(snap.page):{
  route:snap.route,title:pageDraft?.title||"Untitled page",
  heading:pageDraft?.heading||"Untitled page",
  lede:pageDraft?.lede||"",machine_view:"authored",status:"draft"
 };
 if(pageDraft)for(const [field,max]of [["title",220],["heading",220],["lede",5000],["editorial_kind",20],["media_subject_ref",104],["author_credit",159],["author_subject_ref",159],["editorial_published_on",10],["editorial_updated_on",10]]){
  if(!own(pageDraft,field))continue;
  const text=pageDraft[field];
  if(typeof text!=="string"||text.length>max||/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text)||
     ((field==="title"||field==="heading")&&!text.trim()))fail("invalid page "+field);
  page[field]=text;
 }
 if(page.editorial_kind&&!["article","essay","report","feature","longform"].includes(page.editorial_kind)||page.media_subject_ref&&!/^article:[a-z0-9][a-z0-9._-]{0,95}$/.test(page.media_subject_ref)||!!page.editorial_kind!==!!page.media_subject_ref)fail("article kind and identity must be supplied together");
 if(page.author_subject_ref&&!/^person:[a-z0-9-]{1,70}$/.test(page.author_subject_ref))fail("invalid published author identity");
 if(page.author_subject_ref&&!page.author_credit?.trim())fail("author profile requires a visible credit");
 if((page.editorial_published_on&&!editorialDate(page.editorial_published_on))||
    (page.editorial_updated_on&&!editorialDate(page.editorial_updated_on))||
    (page.editorial_updated_on&&(!page.editorial_published_on||page.editorial_updated_on<page.editorial_published_on)))
    fail("invalid editorial publication dates");
 if(!page.editorial_kind&&(page.author_credit||page.author_subject_ref||page.editorial_published_on||page.editorial_updated_on))
    fail("editorial credits require an article kind");
 if(page.status!=="draft")fail("only page drafts can be updated");
 const request={operation:base.id?"replace":"create",route:snap.route,
  expected_id:base.id,expected_digest:base.digest,page,
  fragments:[...unrelated,...authored]};
 if(new TextEncoder().encode(JSON.stringify(request)).length>220000)fail("native request exceeds 220KB");
 return request;
}
async function request(fetcher,url,options){
 const response=await fetcher(url,{credentials:"same-origin",cache:"no-store",...options});
 if(!response?.ok)fail("native CMS refused "+url+" ("+(response?.status||"network")+")");
 const value=await response.json();
 if(!value||typeof value!=="object")fail("native CMS returned an invalid response");
 return value;
}
export async function openNativeAuthoring({fetcher=fetch,route="/",site=""}={}){
 const base=await request(fetcher,"/cms/compose/page?route="+encodeURIComponent(route)+(site?"&site="+encodeURIComponent(site):""));
 return {base,tree:treeFromNativeSnapshot(base)};
}
export async function saveNativeAuthoring({fetcher=fetch,base,tree,pageDraft,site=""}){
 const payload=toNativePageWrite(base,tree,{pageDraft});
 const result=await request(fetcher,"/cms/compose/page"+(site?"?site="+encodeURIComponent(site):""),{method:"POST",
  headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
 if(result.committed!==true)fail("no native commit receipt");
 const reopened=await openNativeAuthoring({fetcher,route:tree.page,site});
 if(pageDraft&&["title","heading","lede","editorial_kind","media_subject_ref","author_credit","author_subject_ref","editorial_published_on","editorial_updated_on"].some(field=>own(pageDraft,field)&&
     (reopened.base.snapshot.page?.[field]??"")!==pageDraft[field]))
  fail("native CMS page metadata did not match committed readback");
 const input=rowsOf(tree),output=rowsOf(reopened.tree);
 if(input.length!==output.length||input.some((a,i)=>
  a.node.id!==output[i].node.id||a.node.kind!==output[i].node.kind||
  a.parent!==output[i].parent||
  JSON.stringify(a.node.responsive)!==JSON.stringify(output[i].node.responsive)||
  JSON.stringify(a.node.props.payload??null)!==JSON.stringify(output[i].node.props.payload??null)||
  (a.node.props.body||"")!==(output[i].node.props.body||"")||
  JSON.stringify(a.node.props.editorial||null)!==JSON.stringify(output[i].node.props.editorial||null)))
  fail("committed readback is not the edited tree");
 return {committed:true,revision:reopened.base.digest,base:reopened.base,tree:reopened.tree};
}
