// Canonical Aurekai authoring <-> native BonfyreCMS page aggregate.
// This adapter never publishes: it performs exact-namespace, revision-checked
// writes through the existing /cms/compose/page owner and reopens the snapshot.
// Nested composition is persisted in typed semantic_tags.authoring_v1 metadata.
import {newAuthoringTree,validateAuthoringTree}
 from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
const clean=x=>Object.fromEntries(Object.entries(x||{}).filter(([k])=>!new Set(["id","namespace","created_at","updated_at","published_at"]).has(k)));
const fail=s=>{throw new Error("native authoring: "+s);};
const clone=x=>structuredClone(x);
const digestOK=s=>typeof s==="string"&&/^[a-f0-9]{64}$/.test(s);
const authoringKind=s=>typeof s==="string"&&(/^profile-(?:hero|section|card|story|quote|metric|action)$/.test(s)||/^service:[a-z][a-z-]*$/.test(s));
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
   ...(own(t,"editorial")?{editorial:t.editorial}:{})};
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
  ...(own(node.props,"editorial")?{editorial:clone(node.props.editorial)}:{})}};
 return {...old,coordinate:node.id,page:route,slot:"showcase",
  audience:stored?.audience||"public",kind:node.kind,position,
  eyebrow:node.props.eyebrow||"",heading:node.props.heading||"",
  body:node.props.body||"",label:node.props.label||"",
  href:node.props.href||"",variant_key:node.props.variant||"paper",
  source_ref:node.props.source_ref||"",source_digest:node.props.source_digest||"",
  semantic_tags:meta,status:"draft"};
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
export async function openNativeAuthoring({fetcher=fetch,route="/"}={}){
 const base=await request(fetcher,"/cms/compose/page?route="+encodeURIComponent(route));
 return {base,tree:treeFromNativeSnapshot(base)};
}
export async function saveNativeAuthoring({fetcher=fetch,base,tree,pageDraft}){
 const payload=toNativePageWrite(base,tree,{pageDraft});
 const result=await request(fetcher,"/cms/compose/page",{method:"POST",
  headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
 if(result.committed!==true)fail("no native commit receipt");
 const reopened=await openNativeAuthoring({fetcher,route:tree.page});
 const input=rowsOf(tree),output=rowsOf(reopened.tree);
 if(input.length!==output.length||input.some((a,i)=>
  a.node.id!==output[i].node.id||a.node.kind!==output[i].node.kind||
  a.parent!==output[i].parent||
  JSON.stringify(a.node.responsive)!==JSON.stringify(output[i].node.responsive)||
  (a.node.props.body||"")!==(output[i].node.props.body||"")||
  JSON.stringify(a.node.props.editorial||null)!==JSON.stringify(output[i].node.props.editorial||null)))
  fail("committed readback is not the edited tree");
 return {committed:true,revision:reopened.base.digest,base:reopened.base,tree:reopened.tree};
}
