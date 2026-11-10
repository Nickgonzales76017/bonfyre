import {FORMATS,THEMES} from '../../components/BonfyreServiceSite/campaign-composer.mjs';
import {publicationInput} from './publication-research.mjs';
const check=(ok,message)=>{if(!ok)throw Error(message);};
const plain=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
const text=(x,n)=>typeof x==='string'&&x.length<=n&&!/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(x);
export const digest=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
export function blankCampaign(){return{version:1,content:{quote:'',cite:'',claim:'',mark:'Aurekai'},source:null,overrides:{},outputs:[]};}
export function checkedCampaign(value){
 check(plain(value)&&value.version===1&&plain(value.content)&&plain(value.overrides)&&Array.isArray(value.outputs)&&value.outputs.length<=6,'This campaign draft has an unsupported structure.');
 const limits={quote:3000,cite:160,claim:160,mark:80},content={};for(const[k,n]of Object.entries(limits)){check(text(value.content[k],n),'The '+k+' field exceeds its reviewed bound.');content[k]=value.content[k];}
 check(Object.keys(value.content).every(k=>k in limits),'Unexpected content field.');const overrides={};
 for(const[format,o]of Object.entries(value.overrides)){check(Object.hasOwn(FORMATS,format)&&plain(o)&&Object.keys(o).every(k=>['scale','theme','align'].includes(k)),'Unknown artboard adjustment.');const next={};if(o.scale!==undefined){check(Number.isFinite(o.scale)&&o.scale>=0.5&&o.scale<=1.8,'Type scale must be between 0.5 and 1.8.');next.scale=o.scale;}if(o.theme!==undefined){check(Object.hasOwn(THEMES,o.theme),'Unknown artboard theme.');next.theme=o.theme;}if(o.align!==undefined){check(['left','center'].includes(o.align),'Unknown artboard alignment.');next.align=o.align;}overrides[format]=next;}
 let source=null;if(value.source!==null){const s=value.source;check(plain(s),'The selected source is unavailable.');source=publicationInput([s.id,s.title,s.body,s.url,s.sourceRef,s.receipt,s.rightsBasis,s.retention,s.publishedAt]);if(s.sourceSubject!==undefined){check(text(s.sourceSubject,256),'The retained source subject is invalid.');source.sourceSubject=s.sourceSubject;}}
 const outputs=[],seen=new Set();let total=0;for(const o of value.outputs){check(plain(o)&&Object.hasOwn(FORMATS,o.format)&&['png','webm'].includes(o.kind)&&/^[a-f0-9]{64}$/.test(o.sha256)&&/^[a-f0-9]{64}$/.test(o.inputDigest)&&o.body instanceof Blob,'A generated output reference is incomplete.');check(o.body.type===(o.kind==='png'?'image/png':'video/webm')&&o.body.size>0&&o.body.size<=6*1024*1024,'An output exceeds its format or size bound.');check(o.width===FORMATS[o.format].w&&o.height===FORMATS[o.format].h,'Output dimensions disagree with the artboard.');check(text(o.created,64)&&!Number.isNaN(Date.parse(o.created)),'An output timestamp is invalid.');const key=o.format+':'+o.kind;check(!seen.has(key),'Repeated output identity.');seen.add(key);total+=o.body.size;check(total<=24*1024*1024,'Retain at most 24 MiB of outputs in one campaign.');outputs.push({...o});}
 return{version:1,content,source,overrides,outputs};
}
export function quotedExactly(state){return !!state.source&&state.content.quote.trim()===state.source.body.trim();}
export async function inputDigest(value){const state=checkedCampaign(value),ordered={version:1,content:state.content,source:state.source,overrides:Object.fromEntries(Object.keys(FORMATS).map(k=>[k,state.overrides[k]||{}]))};return digest(new TextEncoder().encode(JSON.stringify(ordered)));}
export async function restoreCampaign(value,{alive=()=>true}={}){const state=checkedCampaign(value);for(const o of state.outputs){check(alive(),'Account access changed.');check(await digest(await o.body.arrayBuffer())===o.sha256,'A retained campaign output failed its digest check.');}check(alive(),'Account access changed.');return state;}
export async function withOutput(value,format,kind,body){
 const state=checkedCampaign(value);check(Object.hasOwn(FORMATS,format)&&['png','webm'].includes(kind)&&body instanceof Blob,'Choose a supported artboard output.');const canonical=new Blob([body],{type:kind==='png'?'image/png':'video/webm'}),out={format,kind,body:canonical,sha256:await digest(await canonical.arrayBuffer()),inputDigest:await inputDigest(state),width:FORMATS[format].w,height:FORMATS[format].h,created:new Date().toISOString()};
 return checkedCampaign({...state,outputs:[...state.outputs.filter(o=>o.format!==format||o.kind!==kind),out]});
}
