// Native-ready authoring model shared by all editor surfaces.
// Browser canvas is an adapter. This tree is retained authoring state, never
// raw HTML/CSS, and publication is a separate exact-destination review.
import {AUTHORING_BLOCKS} from "./authoring-prototype.mjs";
export const TREE_SCHEMA="bonfyre.authoring-tree.v1";
export const SERVICE_COMPONENTS=Object.freeze([
 "hero","cases","person","reviews","figures","library","prose","pricing","catalogue",
 "plan","planner","creator","steps","synthesis","opportunity","listings","offer",
 "compare","schema","workbook","shipments","simulation","ask","call","door",
 "calculator","proof","amounts","training","source","documents","media"
]);
const validTypes=new Set([...AUTHORING_BLOCKS.map(x=>x.kind),...SERVICE_COMPONENTS.map(x=>"service:"+x)]);
// One container grammar for native C, Puck slots and GrapesJS drop targets.
export const CONTAINER_KINDS=Object.freeze(["profile-section","profile-story","service:cases","service:prose","service:hero"]);
const containers=new Set(CONTAINER_KINDS);
const themes=new Set(["paper","forest","night","rust","wide","slate","editorial"]);
const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
const clone=x=>structuredClone(x);
const fail=s=>{throw new TypeError("authoring tree: "+s);};
const idOK=s=>typeof s==="string"&&/^[a-z0-9][a-z0-9-]{0,117}$/.test(s);
const text=(s,max,label)=>{if(s==null)return "";if(typeof s!=="string"||s.length>max||/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(s))fail(label+" invalid");return s;};
const https=s=>{
 if(!s)return "";
 const out=text(s,1800,"url");let url;
 try{url=new URL(out);}catch{fail("unsafe URL");}
 if(url.protocol!=="https:"||!url.hostname.includes(".")||url.username||url.password||url.href!==out||/[\s<>"'\\]/.test(out))fail("unsafe URL");
 return out;
};
const layouts=new Set(["stack","grid","columns","carousel","masonry","split","full-bleed","feature"]);
const deviceSet=new Set(["desktop","tablet","phone"]);
const styleTokens={
 background:new Set(["default","surface","panel","muted","accent","reversed","dark","light"]),
 typography:new Set(["body","display","serif","sans","mono","editorial"]),
 width:new Set(["auto","narrow","medium","wide","full"])
};
const ints=(x,name,min,max)=>{if(!Number.isInteger(x)||x<min||x>max)fail(name+" invalid");return x;};
function validateStyle(s){
 if(s===undefined)return {};
 if(!s||typeof s!=="object"||Array.isArray(s))fail("style is not typed");
 const allowed=["layout","columns","span","gap","align","justify","padding","radius","background","typography","width"];
 for(const k of Object.keys(s))if(!allowed.includes(k))fail("arbitrary styles forbidden: "+k);
 const result={};
 for(const [k,v] of Object.entries(s)){
  if(k==="layout"){if(!layouts.has(v))fail("layout invalid");result[k]=v;}
  else if(["columns","span"].includes(k))result[k]=ints(v,k,1,12);
  else if(["gap","padding","radius"].includes(k))result[k]=ints(v,k,0,64);
  else if(["align","justify"].includes(k)){
   if(!["start","center","end","stretch","between"].includes(v))fail("alignment invalid");
   result[k]=v;
  }else {if(!styleTokens[k]?.has(v))fail(k+" token invalid");result[k]=v;}
 }
 return result;
}

const editorialTypes=new Set(["paragraph","header","quote","list","code","delimiter","checklist","table"]);
function validateEditorial(blocks){
 if(!Array.isArray(blocks)||blocks.length>256)fail("invalid editorial blocks");
 let bytes=0;
 function walk(value,depth=0){
  if(depth>12)fail("editorial nesting exceeded");
  if(value===null||typeof value==="boolean"||typeof value==="number")return;
  if(typeof value==="string"){
   bytes+=value.length;if(bytes>100000)fail("editorial document oversized");
   // Formatting markup remains stored as opaque data; it is never a renderable
   // HTML claim. Only known inline tags can enter the retained draft.
   for(const tag of value.match(/<[^>]*>/g)||[])
    if(!/^<\/?(?:b|i|strong|em|code|a)>$/.test(tag)&&
       !/^<a href="https:\/\/[^"<>]+">$/.test(tag))fail("editorial HTML needs native sanitation");
   return;
  }
  if(Array.isArray(value)){if(value.length>1024)fail("editorial data too broad");for(const item of value)walk(item,depth+1);return;}
  if(!value||typeof value!=="object")fail("editorial data unsupported");
  const keys=Object.keys(value);
  if(keys.length>80||keys.some(k=>["__proto__","constructor","prototype"].includes(k)))fail("editorial object unsafe");
  for(const item of Object.values(value))walk(item,depth+1);
 }
 for(const block of blocks){
  if(!block||typeof block!=="object"||!editorialTypes.has(block.type)||!block.data)fail("unregistered editorial tool");
  walk(block.data);
 }
 return blocks;
}

export function validateAuthoringTree(source){
 if(!source||source.schema!==TREE_SCHEMA)fail("unknown tree version");
 if(!idOK(source.site_id))fail("bad site ID");
 if(typeof source.page!=="string"||source.page.length>180||!/^\/(?:[a-z0-9-]+\/?)*$/.test(source.page))fail("bad page");
 if(!Array.isArray(source.blocks))fail("missing page blocks");
 const ids=new Set();let total=0;
 function visit(nodes,depth){
  if(depth>16)fail("nested depth exceeded");
  for(const b of nodes){
   if(++total>512)fail("more than 512 blocks");
   if(!b||typeof b!=="object"||Array.isArray(b))fail("invalid block");
   if(!idOK(b.id)||ids.has(b.id))fail("duplicate or invalid block ID");
   ids.add(b.id);
   if(!validTypes.has(b.kind))fail("unregistered block "+b.kind);
   if(!b.props||typeof b.props!=="object"||Array.isArray(b.props))fail("block props invalid");
   const allowedProps=new Set(["heading","body","eyebrow","label","href","variant","alt","caption","source_ref","source_digest","asset_ref","editorial","metadata","readOnly","bindings"]);
   for(const k of Object.keys(b.props))if(!allowedProps.has(k))fail("unregistered prop "+k);
   for(const [k,v] of Object.entries(b.props)){
    if(k==="editorial"){validateEditorial(v);continue;}
    if(["metadata","bindings"].includes(k)){
     if(v==null||typeof v!=="object"||Array.isArray(v)||Object.keys(v).length>32)fail("invalid "+k);
     const payload=JSON.stringify(v);
     if(payload.length>4000||/<script|javascript:|data:text\/html/i.test(payload))fail("unsafe "+k);
     continue;
    }
    if(k==="readOnly"){if(typeof v!=="boolean")fail("readOnly invalid");continue;}
    if(k==="href"){https(v);continue;}
    if(k==="variant"){if(!themes.has(v))fail("variant invalid");continue;}
    if(k==="source_digest"){if(v&&!/^[0-9a-f]{64}$/.test(v))fail("source digest invalid");continue;}
    const max=k==="body"?50000:k==="heading"?220:k==="asset_ref"?240:1000;
    text(v,max,k);
   }
   const styles=b.responsive||{};
   if(!styles||typeof styles!=="object"||Array.isArray(styles))fail("responsive invalid");
   for(const [device,style] of Object.entries(styles)){if(!deviceSet.has(device))fail("unknown viewport");validateStyle(style);}
   if(!Array.isArray(b.children))fail("children must be explicit");
   if(b.children.length&&!containers.has(b.kind))fail("non-container cannot own children: "+b.kind);
   visit(b.children,depth+1);
  }
 }
 visit(source.blocks,0);
 return source;
}
export function newAuthoringTree(site_id,page="/"){
 const doc={schema:TREE_SCHEMA,site_id,page,blocks:[]};
 return validateAuthoringTree(doc);
}
function find(doc,id){
 const recur=(nodes,parent)=>{
  for(let i=0;i<nodes.length;i++){
   const node=nodes[i];
   if(node.id===id)return {node,nodes,index:i,parent};
   const child=recur(node.children,node);
   if(child)return child;
  }
 };
 return recur(doc.blocks,null);
}
function destination(doc,parent){
 if(parent==null)return doc.blocks;
 const target=find(doc,parent);if(!target)fail("parent not found");
 if(!containers.has(target.node.kind))fail("parent is not a container");
 return target.node.children;
}
function stripIncoming(block){
 if(!block||typeof block!=="object"||Array.isArray(block))fail("insert requires block");
 return {id:block.id,kind:block.kind,props:clone(block.props||{}),
  responsive:clone(block.responsive||{}),children:clone(block.children||[])};
}
export function applyAuthoringEdit(source,operation){
 const doc=clone(validateAuthoringTree(source));
 if(!operation||typeof operation!=="object")fail("operation not structured");
 const op=operation.type,id=operation.id;
 if(op==="insert"){
  const nodes=destination(doc,operation.parent??null),index=operation.index??nodes.length;
  if(!Number.isInteger(index)||index<0||index>nodes.length)fail("insert index invalid");
  nodes.splice(index,0,stripIncoming(operation.block));
 }else if(op==="remove"){
  const found=find(doc,id);if(!found)fail("block not found");
  found.nodes.splice(found.index,1);
 }else if(op==="move"){
  const found=find(doc,id);if(!found)fail("block not found");
  const subtree=new Set();
  const walk=n=>{subtree.add(n.id);for(const c of n.children)walk(c);};walk(found.node);
  if(operation.parent!=null&&subtree.has(operation.parent))fail("cannot move into own subtree");
  const moved=found.nodes.splice(found.index,1)[0];
  const dest=destination(doc,operation.parent??null),index=operation.index??dest.length;
  if(!Number.isInteger(index)||index<0||index>dest.length)fail("move index invalid");
  dest.splice(index,0,moved);
 }else if(op==="update"){
  const found=find(doc,id);if(!found)fail("block not found");
  const key=operation.field;
  if(typeof key!=="string"||!["heading","body","eyebrow","label","href","variant","alt","caption","source_ref","source_digest","asset_ref","editorial","readOnly"].includes(key))fail("field cannot be edited");
  if(found.node.props.readOnly&&key!=="readOnly")fail("source-bound field is read-only");
  found.node.props[key]=operation.value;
 }else if(op==="style"){
  const found=find(doc,id);if(!found)fail("block not found");
  if(!deviceSet.has(operation.viewport))fail("viewport invalid");
  found.node.responsive[operation.viewport]=validateStyle(operation.value);
 }else if(op==="duplicate"){
  const found=find(doc,id);if(!found)fail("block not found");
  const copied=clone(found.node);const prefix=operation.newId;
  if(!idOK(prefix))fail("copy identity invalid");
  const replace=(node,path)=>{
   node.id=prefix+(path?"-"+path:"");
   node.children.forEach((c,i)=>replace(c,path?path+"-"+(i+1):String(i+1)));
  };
  replace(copied,"");
  found.nodes.splice(found.index+1,0,copied);
 }else fail("unknown edit "+op);
 return validateAuthoringTree(doc);
}
export function createAuthoringSession(initial,{historyLimit=100}={}){
 let value=clone(validateAuthoringTree(initial)),past=[],future=[],listeners=new Set();
 const notify=()=>{for(const cb of listeners)cb(clone(value));};
 return Object.freeze({
  snapshot:()=>clone(value),
  edit(op){const next=applyAuthoringEdit(value,op);past.push(value);if(past.length>historyLimit)past.shift();value=next;future=[];notify();return clone(value);},
  replace(next){validateAuthoringTree(next);past.push(value);if(past.length>historyLimit)past.shift();value=clone(next);future=[];notify();return clone(value);},
  undo(){if(!past.length)return false;future.push(value);value=past.pop();notify();return true;},
  redo(){if(!future.length)return false;past.push(value);value=future.pop();notify();return true;},
  subscribe(fn){listeners.add(fn);return ()=>listeners.delete(fn);},
  canUndo:()=>past.length>0,canRedo:()=>future.length>0
 });
}
export function toPuckData(doc){
 validateAuthoringTree(doc);
 const convert=b=>({type:b.kind,props:{...clone(b.props),id:b.id,
  responsive:clone(b.responsive),children:b.children.map(convert)}});
 return {content:doc.blocks.map(convert),root:{props:{site_id:doc.site_id,page:doc.page}},zones:{}};
}
export function fromPuckData(data,site_id,page="/"){
 const convert=b=>{
  if(!b||typeof b!=="object"||typeof b.type!=="string")fail("invalid Puck component");
  const {id,responsive={},children=[],...props}=b.props||{};
  return {id,kind:b.type,props,responsive,children:children.map(convert)};
 };
 return validateAuthoringTree({schema:TREE_SCHEMA,site_id,page,blocks:(data.content||[]).map(convert)});
}
export function toGrapesProject(doc){
 validateAuthoringTree(doc);
 const convert=b=>({type:"aurekai:"+b.kind,attributes:{"data-coordinate":b.id},
  aurekai:clone(b.props),responsive:clone(b.responsive),components:b.children.map(convert)});
 return {pages:[{frames:[{component:{components:doc.blocks.map(convert)}}]}]};
}
export function fromGrapesProject(data,site_id,page="/"){
 const top=data?.pages?.[0]?.frames?.[0]?.component?.components;
 if(!Array.isArray(top)||data.pages.length!==1)fail("unknown GrapesJS document");
 const convert=b=>{
  if(typeof b.type!=="string"||!b.type.startsWith("aurekai:"))fail("arbitrary HTML components forbidden");
  return {id:b.attributes?.["data-coordinate"],kind:b.type.slice(8),
   props:clone(b.aurekai||{}),responsive:clone(b.responsive||{}),
   children:(b.components||[]).map(convert)};
 };
 return validateAuthoringTree({schema:TREE_SCHEMA,site_id,page,blocks:top.map(convert)});
}
// Editor.js edits article paragraphs only, not the page canvas. This prevents
// text changes from erasing layouts, responsive variants, sources or media.
export function toEditorBlocks(node){
 if(!node||!validTypes.has(node.kind))fail("unknown article target");
 if(Array.isArray(node.props.editorial))return {time:0,blocks:clone(node.props.editorial),version:"2"};
 const body=node.props.body||"";
 return {time:0,blocks:body.split(/\n\n+/).filter(Boolean).map((text,i)=>({
  id:node.id+"-p"+i,type:"paragraph",data:{text}
 })),version:"2"};
}
function editorialPreview(value){
 if(typeof value==="string")return value.replace(/<\/?(?:b|i|strong|em|code|a)(?:\s[^>]*)?>/gi,"");
 if(Array.isArray(value))return value.map(editorialPreview).filter(Boolean).join("\n");
 if(value&&typeof value==="object")
  return editorialPreview(value.text??value.code??value.caption??value.content??value.items??value.values??[]);
 return "";
}
export function applyEditorBlocks(doc,nodeId,data){
 if(!Array.isArray(data?.blocks))fail("invalid editorial blocks");
 const blocks=validateEditorial(data.blocks).map(x=>clone(x));
 const preview=blocks.map(b=>editorialPreview(b.data)).filter(Boolean).join("\n\n");
 const result=applyAuthoringEdit(doc,{type:"update",id:nodeId,field:"body",value:preview});
 return applyAuthoringEdit(result,{type:"update",id:nodeId,field:"editorial",value:blocks});
}
