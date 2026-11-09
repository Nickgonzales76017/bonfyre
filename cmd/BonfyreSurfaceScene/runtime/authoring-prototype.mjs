// Single authoring prototype behind GrapesJS, Puck and Editor.js adapters.
// Native CMS/Matrix remains responsible for identity, rights and publication.
export const AUTHORING_SCHEMA="bonfyre.authoring.v1";
export const AUTHORING_BLOCKS=Object.freeze([
 ["hero","profile-hero","lead"],["section","profile-section","shelf"],
 ["card","profile-card","grid"],["story","profile-story","thread"],
 ["quote","profile-quote","thread"],["metric","profile-metric","grid"],
 ["action","profile-action","field"]
].map(([type,kind,role])=>Object.freeze({type,kind,role,maxHeading:220,maxBody:5000})));
const byType=new Map(AUTHORING_BLOCKS.map(x=>[x.type,x]));
const own=(x,k)=>Object.prototype.hasOwnProperty.call(x,k);
const aliases={header:"section",paragraph:"story",linkTool:"card"};
function fail(why){throw new TypeError("authoring: "+why);}
function object(x,what){if(!x||typeof x!=="object"||Array.isArray(x))fail(what+" must be an object");return x;}
function text(x,name,max){
 if(x===undefined||x===null)return "";
 if(typeof x!=="string"||x.length>max||/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(x))fail(name+" exceeds text boundary");
 return x;
}
function siteId(x){if(typeof x!=="string"||!/^[a-z0-9][a-z0-9-]{1,94}$/.test(x))fail("invalid site identity");return x;}
function route(x){if(typeof x!=="string"||!/^\/(?:[a-z0-9-]+\/?)*$/.test(x)||x.length>180)fail("invalid page route");return x;}
function href(x){
 x=text(x,"href",1800);if(!x)return "";
 let u;try{u=new URL(x);}catch{fail("link must use absolute HTTPS");}
 if(u.protocol!=="https:"||!u.hostname.includes(".")||u.username||u.password||u.href!==x||/[\s<>"'\\]/.test(x))fail("unsafe link");
 return x;
}
function kindOf(type){
 if(typeof type!=="string")fail("missing component type");
 const plain=type.replace(/^aurekai[:/-]/,"").replace(/^profile-/,"").replace(/([a-z])([A-Z])/g,"$1-$2").toLowerCase();
 const spec=byType.get(aliases[type]||plain);
 if(!spec)fail("unknown component "+type);
 return spec;
}
function puckNodes(data){
 const rows=object(data,"Puck").content;if(!Array.isArray(rows))fail("Puck content missing");
 return rows.map(x=>{const p=object(x.props,"Puck props");return {type:x.type,id:p.id,fields:p};});
}
function editorNodes(data){
 const rows=object(data,"Editor.js").blocks;if(!Array.isArray(rows))fail("Editor.js blocks missing");
 return rows.map(x=>{
  const p=object(x.data,"Editor.js block");
  if(x.type==="linkTool")return {type:x.type,id:x.id,fields:{heading:p.meta?.title||p.link||"",body:p.meta?.description||"",label:"Open source",href:p.link||""}};
  return {type:x.type,id:x.id,fields:{heading:x.type==="header"?p.text:x.type==="quote"?p.caption:"",
   body:x.type==="header"?"":p.text??""}};
 });
}
function grapesNodes(data){
 const pages=object(data,"GrapesJS").pages;
 if(!Array.isArray(pages)||pages.length!==1)fail("GrapesJS requires one page");
 const frames=pages[0]?.frames;
 if(!Array.isArray(frames)||frames.length!==1)fail("GrapesJS requires one frame");
 const items=frames[0]?.component?.components;
 if(!Array.isArray(items))fail("GrapesJS requires typed block components");
 const result=[];
 function walk(rows,depth){
  if(depth>8)fail("nested composition too deep");
  for(const x of rows){
   object(x,"GrapesJS node");
   if(typeof x.type!=="string"||!x.type.startsWith("aurekai:"))fail("generic HTML/CSS cannot become a published component");
   const attrs=object(x.attributes||{},"GrapesJS attributes");
   result.push({type:x.type,id:attrs["data-coordinate"]||attrs.id,fields:object(x.aurekai||{},"Aurekai fields")});
   if(x.components!==undefined){if(!Array.isArray(x.components))fail("bad nested block list");walk(x.components,depth+1);}
  }
 }
 walk(items,0);return result;
}
const engines={grapesjs:grapesNodes,puck:puckNodes,editorjs:editorNodes};
export function compileAuthoringDraft({engine,data,site_id,page="/"}){
 if(!own(engines,engine))fail("unknown editor adapter");
 const site=siteId(site_id),path=route(page),nodes=engines[engine](data);
 if(nodes.length>80)fail("more than 80 blocks");
 const seen=new Set();
 const fragments=nodes.map((node,i)=>{
  const spec=kindOf(node.type),p=object(node.fields,"component fields");
  const coordinate=text(node.id??(spec.type+"-"+String(i+1).padStart(4,"0")),"coordinate",118).toLowerCase();
  if(!/^[a-z0-9][a-z0-9-]{0,117}$/.test(coordinate)||seen.has(coordinate))fail("invalid or duplicate component coordinate");
  seen.add(coordinate);
  const heading=text(p.heading??p.headline??p.title??"",coordinate+" heading",220);
  const body=text(p.body??p.lede??p.text??"",coordinate+" body",5000);
  const eyebrow=text(p.eyebrow??p.overline??"",coordinate+" eyebrow",100);
  const label=text(p.label??p.button??"",coordinate+" label",100);
  const variant_key=text(p.variant_key??p.variant??"paper",coordinate+" variant",40);
  if(!/^(paper|forest|night|rust|wide|slate|editorial)$/.test(variant_key))fail("unknown theme");
  const source_ref=text(p.source_ref??"",coordinate+" source",500);
  const source_digest=text(p.source_digest??"",coordinate+" digest",128);
  if(source_digest&&!/^[a-f0-9]{64}$/.test(source_digest))fail("invalid source digest");
  return {coordinate,page:path,slot:"showcase",audience:"public",kind:spec.kind,position:i,
   status:"draft",eyebrow,heading,body,label,href:href(p.href??""),variant_key,source_ref,source_digest};
 });
 return {schema:AUTHORING_SCHEMA,site_id:site,page:path,fragments};
}
export function toCmsDrafts(doc){
 object(doc,"compiled document");
 if(doc.schema!==AUTHORING_SCHEMA||!Array.isArray(doc.fragments))fail("not a compiled authoring document");
 siteId(doc.site_id);route(doc.page);
 return doc.fragments.map(f=>({...f,namespace:doc.site_id,status:"draft"}));
}
// Generated C is a compact pure block kernel. It does NOT authorize publishing,
// execute editor code, emit raw editor HTML or resolve source material.
export function nativeAuthoringHeader(){
 const rows=AUTHORING_BLOCKS.map(s=>'    {"'+s.kind+'","'+s.role+'",'+s.maxHeading+','+s.maxBody+'},').join("\n");
 return '/* Generated from authoring-prototype.mjs; do not hand-edit. */\n'
  +'#ifndef BONFYRE_NATIVE_AUTHORING_CONTRACT_H\n#define BONFYRE_NATIVE_AUTHORING_CONTRACT_H\n'
  +'#include <stddef.h>\n#include <string.h>\n'
  +'typedef struct {const char *kind;const char *role;size_t max_heading;size_t max_body;} BfAuthoringBlock;\n'
  +'static const BfAuthoringBlock bf_authoring_blocks[]={\n'+rows+'\n};\n'
  +'#define BF_AUTHORING_BLOCK_COUNT (sizeof(bf_authoring_blocks)/sizeof(bf_authoring_blocks[0]))\n'
  +'static inline const BfAuthoringBlock *bf_authoring_block(const char *kind){\n'
  +' if(!kind)return NULL;\n'
  +' for(size_t i=0;i<BF_AUTHORING_BLOCK_COUNT;i++)if(strcmp(kind,bf_authoring_blocks[i].kind)==0)return &bf_authoring_blocks[i];\n'
  +' return NULL;\n}\n'
  +'static inline int bf_authoring_block_accepts(const char *kind,size_t heading,size_t body){\n'
  +' const BfAuthoringBlock *s=bf_authoring_block(kind);return s&&heading<=s->max_heading&&body<=s->max_body;\n}\n'
  +'/* Shape validity never grants source, audience, account or publication authority. */\n'
  +'#endif\n';
}
