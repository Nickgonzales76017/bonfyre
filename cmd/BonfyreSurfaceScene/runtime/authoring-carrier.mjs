// Bonfyre-native authoring carrier. Versioned, deterministic, length-bounded,
// typed wire format; no JSON, DOM serialization, template code or HTML.
// Supports the ENTIRE validated authoring tree including nested editorial,
// bindings, source metadata and every responsive token.
import {validateAuthoringTree,TREE_SCHEMA} from "./authoring-tree.mjs";

export const AUTHORING_CARRIER_MAGIC="BFAU2\r\n";
export const AUTHORING_CARRIER_MAX=4*1024*1024;
const enc=new TextEncoder(),dec=new TextDecoder("utf-8",{fatal:true});
const fail=s=>{throw new TypeError("native authoring carrier: "+s);};
const special=new Set(["__proto__","constructor","prototype"]);
const segment=s=>s.replaceAll("~","~0").replaceAll("/","~1");
const unsegment=s=>s.replaceAll("~1","/").replaceAll("~0","~");
const safePart=s=>{if(special.has(s)||s.length>255)fail("unsafe path segment");return s;};
const eq=(a,b)=>a.length===b.length&&a.every((v,i)=>v===b[i]);
const pack32=n=>{const b=new Uint8Array(4);new DataView(b.buffer).setUint32(0,n,true);return b;};
const byteString=s=>{
 if(typeof s!=="string")fail("non-string field");
 const bytes=enc.encode(s);
 if(bytes.length>2*1024*1024)fail("field exceeds byte bound");
 return [pack32(bytes.length),bytes];
};
function flatten(value,path,fields,depth=0){
 if(depth>32)fail("nested value exceeds 32");
 const add=(type,body="")=>{fields.push([path,type,body]);if(fields.length>30000)fail("field count exceeded");};
 if(value===null){add("n");return;}
 if(typeof value==="boolean"){add("b",value?"1":"0");return;}
 if(typeof value==="number"){if(!Number.isFinite(value))fail("invalid number");add("d",String(value));return;}
 if(typeof value==="string"){add("s",value);return;}
 if(Array.isArray(value)){
  add("a",String(value.length));
  for(let i=0;i<value.length;i++)flatten(value[i],path+"/"+i,fields,depth+1);
  return;
 }
 if(value&&typeof value==="object"){
  const keys=Object.keys(value).sort();
  add("o",String(keys.length));
  for(const key of keys)flatten(value[key],path+"/"+segment(safePart(key)),fields,depth+1);
  return;
 }
 fail("unsupported value");
}
function nodesOf(doc){
 const list=[];
 const walk=(items,parent)=>{
  for(let i=0;i<items.length;i++){
   const b=items[i],fields=[];
   flatten(b.props,"/props",fields);
   flatten(b.responsive||{},"/responsive",fields);
   list.push({id:b.id,kind:b.kind,parent:parent||"",position:i,fields});
   walk(b.children,b.id);
  }
 };
 walk(doc.blocks,null);
 return list;
}
export function encodeNativeAuthoring(source){
 const doc=validateAuthoringTree(source);
 const nodes=nodesOf(doc),parts=[enc.encode(AUTHORING_CARRIER_MAGIC),
  ...byteString(doc.site_id),...byteString(doc.page),pack32(nodes.length)];
 let size=parts.reduce((n,b)=>n+b.length,0);
 const push=b=>{size+=b.length;if(size>AUTHORING_CARRIER_MAX)fail("document exceeds 4 MiB");parts.push(b);};
 const pushText=s=>{for(const x of byteString(s))push(x);};
 for(const n of nodes){
  pushText(n.id);pushText(n.kind);pushText(n.parent);push(pack32(n.position));
  push(pack32(n.fields.length));
  for(const [path,type,value] of n.fields){pushText(path);pushText(type);pushText(value);}
 }
 const out=new Uint8Array(size);let at=0;
 for(const part of parts){out.set(part,at);at+=part.length;}
 return out;
}
function reader(payload){
 if(!(payload instanceof Uint8Array)||payload.length>AUTHORING_CARRIER_MAX)fail("invalid byte boundary");
 let at=0;
 const take=n=>{if(n<0||at+n>payload.length)fail("truncated carrier");const s=payload.subarray(at,at+n);at+=n;return s;};
 const number=()=>new DataView(take(4).buffer,payload.byteOffset+at-4,4).getUint32(0,true);
 // DataView is constructed from the taken view, not from a caller-supplied offset.
 const u32=()=>{const b=take(4);return new DataView(b.buffer,b.byteOffset,4).getUint32(0,true);};
 const string=()=>{const n=u32();if(n>2*1024*1024)fail("field too long");return dec.decode(take(n));};
 const header=dec.decode(take(enc.encode(AUTHORING_CARRIER_MAGIC).length));
 if(header!==AUTHORING_CARRIER_MAGIC)fail("bad magic or version");
 return {u32,string,done:()=>at===payload.length};
}
function place(root,path,type,value){
 if(!path.startsWith("/"))fail("path is not rooted");
 const keys=path.slice(1).split("/").map(x=>safePart(unsegment(x)));
 if(keys.length<1||keys.length>34||!["props","responsive"].includes(keys[0]))fail("wrong document field");
 const valueOf=()=>{
  if(type==="o"||type==="a"){if(!/^\d+$/.test(value)||Number(value)>30000)fail("invalid container bound");return type==="o"?{}:[];}
  if(type==="n"){if(value!=="")fail("invalid null");return null;}
  if(type==="b"){if(!["0","1"].includes(value))fail("invalid boolean");return value==="1";}
  if(type==="d"){const n=Number(value);if(!value||!Number.isFinite(n)||String(n)!==value)fail("invalid number");return n;}
  if(type==="s")return value;
  fail("unsupported scalar type");
 };
 let o=root;
 for(let i=0;i<keys.length-1;i++){
  const k=keys[i];
  if(!Object.hasOwn(o,k)||typeof o[k]!=="object"||o[k]===null)fail("missing container parent");
  o=o[k];
 }
 const key=keys.at(-1);
 if(Object.hasOwn(o,key))fail("duplicate wire field");
 if(Array.isArray(o)&&(!/^(0|[1-9]\d*)$/.test(key)||Number(key)!==o.length))fail("out-of-order array element");
 o[key]=valueOf();
}
export function decodeNativeAuthoring(payload){
 const r=reader(payload),site_id=r.string(),page=r.string(),count=r.u32();
 if(count>512)fail("too many nodes");
 const byId=new Map(),roots=[];
 for(let n=0;n<count;n++){
  const id=r.string(),kind=r.string(),parent=r.string(),position=r.u32(),fieldCount=r.u32();
  if(byId.has(id)||fieldCount>30000)fail("duplicate node or oversized node");
  const node={id,kind,children:[]};
  for(let i=0;i<fieldCount;i++)place(node,r.string(),r.string(),r.string());
  const siblings=parent?byId.get(parent)?.children:roots;
  if(!siblings||siblings.length!==position)fail("invalid node ownership or order");
  siblings.push(node);byId.set(id,node);
 }
 if(!r.done())fail("carrier has trailing data");
 const doc={schema:TREE_SCHEMA,site_id,page,blocks:roots};
 validateAuthoringTree(doc);
 if(!eq(encodeNativeAuthoring(doc),payload))fail("noncanonical or lossy wire form");
 return doc;
}
