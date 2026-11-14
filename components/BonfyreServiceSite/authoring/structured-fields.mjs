// Typed structural editing shared by Puck's nested field inspector and future
// GrapesJS traits. No innerHTML, arbitrary CSS or effect-bearing values.
import {validateComponentData} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
const fail=s=>{throw new TypeError("Aurekai structured field: "+s);};
const clone=x=>structuredClone(x);
const pathOK=p=>Array.isArray(p)&&p.length<=20&&p.every(x=>
 typeof x==="number"?(Number.isInteger(x)&&x>=0&&x<=10000):
 (typeof x==="string"&&/^[a-zA-Z][a-zA-Z0-9_-]{0,79}$/.test(x)&&
  !["__proto__","constructor","prototype"].includes(x)));
const object=x=>x&&typeof x==="object"&&!Array.isArray(x);
function walk(root,path){
 let node=root;
 for(const key of path){
  if(Array.isArray(node)){if(typeof key!=="number"||key>=node.length)fail("array path missing");}
  else if(object(node)){if(typeof key!=="string"||!Object.hasOwn(node,key))fail("object path missing");}
  else fail("cannot traverse a scalar field");
  node=node[key];
 }
 return node;
}
export function editStructuredPayload(payload,action){
 validateComponentData(payload);
 const root=clone(payload);
 if(!action||!pathOK(action.path))fail("invalid field address");
 const {type,path}=action;
 if(type==="set"){
  if(!path.length)fail("cannot replace the component payload itself");
  const parent=walk(root,path.slice(0,-1)),key=path.at(-1);
  if(Array.isArray(parent)){
   if(typeof key!=="number"||key>=parent.length)fail("array field missing");
  }else if(!object(parent)||typeof key!=="string")fail("invalid object field");
  parent[key]=clone(action.value);
 }else if(type==="remove"){
  if(!path.length)fail("cannot remove the payload root");
  const parent=walk(root,path.slice(0,-1)),key=path.at(-1);
  if(Array.isArray(parent)){
   if(typeof key!=="number"||key>=parent.length)fail("array item missing");
   parent.splice(key,1);
  }else if(object(parent)&&typeof key==="string"&&Object.hasOwn(parent,key))delete parent[key];
  else fail("object field missing");
 }else if(type==="insert"){
  const target=walk(root,path),at=action.index??target.length;
  if(!Array.isArray(target)||!Number.isInteger(at)||at<0||at>target.length)
   fail("array insert position invalid");
  target.splice(at,0,clone(action.value));
 }else if(type==="move"){
  const target=walk(root,path),a=action.from,b=action.to;
  if(!Array.isArray(target)||![a,b].every(i=>Number.isInteger(i)&&i>=0&&i<target.length))
   fail("array reorder invalid");
  const [item]=target.splice(a,1);target.splice(b,0,item);
 }else fail("unknown structural operation "+type);
 return validateComponentData(root);
}
const listShapes={
 entries:()=>({id:"",title:"",description:""}),
 stores:()=>({id:"",name:"",origin:""}),
 items:()=>({title:"",text:""}),
 rows:()=>({label:"",value:""}),
 links:()=>({label:"",href:""}),
 reel:()=>({src:"",alt:"",href:""}),
 fields:()=>({label:"",type:"text"})
};
export function insertShape(field){
 return (listShapes[field]||(()=>({})) )();
}
export function startingSectionPayload(spec){
 if(!spec||typeof spec!=="object"||!Array.isArray(spec.required))fail("invalid section contract");
 const payload={};
 for(const field of spec.required){
  if(["items","rows","stores","entries"].includes(field))payload[field]=[];
  else if(["form","sample","catalogue"].includes(field))payload[field]={};
  else payload[field]="";
 }
 return validateComponentData(payload);
}
