// Real nested data inspector for Puck. Every edit is a typed tree operation
// with validation shared by the C-backed authoring document.
import {editStructuredPayload,insertShape,startingSectionPayload} from "./structured-fields.mjs";
const object=x=>x!==null&&typeof x==="object"&&!Array.isArray(x);
const label=x=>String(x).replace(/[-_]/g," ").replace(/([a-z])([A-Z])/g,"$1 $2");
const defaults={text:"",object:{},array:[],number:0,boolean:false};
export function makeStructuredPayloadField(React,spec){
 const h=(tag,props,...children)=>React.createElement(tag,props,...children);
 function Field({root,value,path,name,apply,readOnly}){
  const id=path.map(x=>String(x)).join(".");
  const title=h("strong",{className:"authoring-field-label"},label(name));
  if(Array.isArray(value)){
   const items=value.map((item,index)=>h("li",{key:index,className:"authoring-structured-item"},
    h("div",{className:"authoring-structured-item-actions"},
     h("small",null,"Item "+(index+1)),
     h("button",{type:"button",disabled:readOnly||index===0,onClick:()=>apply({type:"move",path,from:index,to:index-1})},"Up"),
     h("button",{type:"button",disabled:readOnly||index===value.length-1,onClick:()=>apply({type:"move",path,from:index,to:index+1})},"Down"),
     h("button",{type:"button",disabled:readOnly,onClick:()=>apply({type:"remove",path:[...path,index]})},"Remove")),
    h(Field,{root,value:item,path:[...path,index],name:"item",apply,readOnly})));
   return h("fieldset",{className:"authoring-structured-array","data-field-path":id},
    h("legend",null,title),
    h("ol",null,...items),
    h("button",{type:"button",disabled:readOnly,onClick:()=>apply({
     type:"insert",path,index:value.length,value:insertShape(name)})},"Add "+label(name)));
  }
  if(object(value)){
   const rows=Object.entries(value).map(([key,child])=>h("div",{
    className:"authoring-structured-property",key},
    h(Field,{root,value:child,path:[...path,key],name:key,apply,readOnly}),
    h("button",{type:"button",disabled:readOnly,className:"authoring-field-remove",
     "aria-label":"Remove "+key,onClick:()=>apply({type:"remove",path:[...path,key]})},"Remove field")));
   return h("fieldset",{className:"authoring-structured-object","data-field-path":id},
    h("legend",null,title),...rows,
    h(KeyAdder,{path,root,apply,readOnly}));
  }
  if(typeof value==="boolean")
   return h("label",{className:"authoring-structured-value","data-field-path":id},
    title,h("input",{type:"checkbox",checked:value,disabled:readOnly,
     onChange:e=>apply({type:"set",path,value:e.target.checked})}));
  if(typeof value==="number")
   return h("label",{className:"authoring-structured-value","data-field-path":id},
    title,h("input",{type:"number",value:String(value),disabled:readOnly,
     onChange:e=>{const n=Number(e.target.value);if(Number.isFinite(n))apply({type:"set",path,value:n});}}));
  return h("label",{className:"authoring-structured-value","data-field-path":id},
   title,h("textarea",{rows:name==="body"||name==="text"||name==="description"?4:2,
    value:value==null?"":String(value),disabled:readOnly,
    onChange:e=>apply({type:"set",path,value:e.target.value})}));
 }
 function KeyAdder({path,apply,readOnly}){
  const [key,setKey]=React.useState(""),[type,setType]=React.useState("text");
  const [error,setError]=React.useState("");
  const insert=()=>{
   try{apply({type:"set",path:[...path,key],value:structuredClone(defaults[type])});
    setKey("");setError("");}
   catch(err){setError(err.message);}
  };
  return h("div",{className:"authoring-structured-add"},
   h("label",null,"New field",h("input",{value:key,disabled:readOnly,
    onChange:e=>setKey(e.target.value),placeholder:"image, reel, summary…"})),
   h("label",null,"Value kind",h("select",{value:type,disabled:readOnly,
    onChange:e=>setType(e.target.value)},
     ...Object.keys(defaults).map(v=>h("option",{key:v,value:v},label(v))))),
   h("button",{type:"button",disabled:readOnly||!key,onClick:insert},"Add field"),
   error?h("small",{role:"alert"},error):null);
 }
 function Inspector({value,onChange,readOnly}){
  const [error,setError]=React.useState("");
  const current=object(value)?value:startingSectionPayload(spec);
  const apply=operation=>{
   try{const next=editStructuredPayload(current,operation);onChange(next);setError("");}
   catch(e){setError(e.message);throw e;}
  };
  const missing=spec.required.filter(key=>!Object.hasOwn(current,key));
  const entries=Object.entries(current).map(([key,v])=>h(Field,{
   key,root:current,value:v,path:[key],name:key,apply,readOnly}));
  return h("div",{className:"authoring-structured-inspector",
   "data-aurekai-structured-kind":spec.kind},
   missing.length?h("small",{role:"alert"}, "Required: "+missing.join(", ")):null,
   ...entries,h(KeyAdder,{path:[],apply,readOnly}),
   error?h("small",{role:"alert"},error):null);
 }
 return {type:"custom",label:"Section content",render:props=>
  h(Inspector,{value:props.value,onChange:props.onChange,readOnly:props.readOnly})};
}
