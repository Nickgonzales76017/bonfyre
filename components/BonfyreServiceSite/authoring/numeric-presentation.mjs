// Browser reader of the layout vocabulary in schemas/control/numeric-presentation.cue
// (generated table). Factoring, selection and CSS lowering: bonfyre-surface-scene numeric.
import {portableSha256} from "./sha256-portable.mjs";
import vocabulary from "./numeric-presentation.vocabulary.json" with {type:"json"};
export const NUMERIC_SCHEMA=vocabulary.schema;
const {devices,props,enums,ranges}=vocabulary,fail=s=>{throw new TypeError("numeric presentation: "+s);};
const object=o=>o!==null&&typeof o==="object"&&!Array.isArray(o)&&Object.getPrototypeOf(o)===Object.prototype;
export function normalizeNumericPresentation(responsive={}){
 if(!object(responsive))fail("responsive must be plain");
 for(const key of Object.keys(responsive))if(!devices.includes(key))fail("unknown viewport");
 const out={};
 for(const device of devices.filter(d=>Object.hasOwn(responsive,d))){
  const style=responsive[device],sorted={};
  if(!object(style))fail("invalid viewport");
  for(const [key,value] of Object.entries(style)){
   if(!props.includes(key))fail("unknown property");
   const range=ranges[key];
   if(range?(!Number.isInteger(value)||value<range[0]||value>range[1]):!enums[key]?.includes(value))fail(range?"numeric range":"unknown enum");
  }
  for(const key of props)if(Object.hasOwn(style,key))sorted[key]=style[key];
  if(Object.keys(sorted).length)out[device]=sorted;
 }
 return out;
}
export const numericPresentationAddress=responsive=>portableSha256(new TextEncoder().encode(
 JSON.stringify([NUMERIC_SCHEMA,normalizeNumericPresentation(responsive)])));
