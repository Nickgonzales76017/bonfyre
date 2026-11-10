// Recovery admission uses the existing instrument semantics without a DOM.
// text/csvRead below are copied unchanged from pinned instruments/data.mjs at
// commit 396bcaf02f24bd96c1789aee5410d111edbf0818 (tree e27b1a7640d51bbace199793471a271674513f43).
// Keeping the pure carrier here makes its exact bytes part of validator_sha256.
import {validateDataChecksRecord} from './authored-data-checks.mjs';
export function text(value, label='Value', limit=10000) {
  if(typeof value!=='string'||value.length>limit||value.includes('\0'))throw Error(`${label} must be text of at most ${limit} characters without NULs.`);
  return value;
}
export function csvRead(input,{maxRows=100000,maxBytes=16*1024*1024}={}){
  text(input,'Import',maxBytes);if(new TextEncoder().encode(input).length>maxBytes)throw Error('Import exceeds its byte limit.');
  input=input.replace(/^\uFEFF/,'');const lines=[];let row=[],field='',quoted=false,after=false;
  const cell=()=>{row.push(field);field='';after=false;if(row.length>256)throw Error('Too many columns.');};
  const line=()=>{cell();if(row.some(v=>v!==''))lines.push(row);row=[];if(lines.length>maxRows+1)throw Error('Too many rows.');};
  for(let i=0;i<input.length;i++){
    const c=input[i];
    if(quoted){if(c==='"'){if(input[i+1]==='"'){field+='"';i++;}else{quoted=false;after=true;}}else field+=c;continue;}
    if(c===','){cell();continue;}if(c==='\n'||c==='\r'){if(c==='\r'&&input[i+1]==='\n')i++;line();continue;}
    if(after)throw Error('Unexpected text after a quoted CSV field.');
    if(c==='"'){if(field)throw Error('A quote must start a CSV field.');quoted=true;}else field+=c;
  }
  if(quoted)throw Error('Unclosed quoted CSV field.');if(field||row.length||after)line();
  if(!lines.length)throw Error('The import has no header.');
  const headers=lines.shift().map(h=>h.trim());
  if(headers.some(h=>!h)||new Set(headers).size!==headers.length)throw Error('Header names must be nonempty and unique.');
  return {headers,rows:lines.map((values,i)=>({row:i+2,values,error:values.length===headers.length?null:`Expected ${headers.length} fields; received ${values.length}.`})),records:lines.filter(v=>v.length===headers.length).map(v=>Object.fromEntries(headers.map((h,i)=>[h,v[i]])))};
}
const fail=(ok,message)=>{if(!ok)throw Error(message);};
const plain=v=>v&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
const closed=(v,keys,optional=[])=>plain(v)&&keys.every(k=>Object.hasOwn(v,k))&&Object.keys(v).every(k=>keys.includes(k)||optional.includes(k));
const hashPattern=/^[a-f0-9]{64}$/;
const safeURL=value=>{try{const u=new URL(value);return ['http:','https:'].includes(u.protocol)&&!u.username&&!u.password;}catch{return false;}};
const digest=async value=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))].map(b=>b.toString(16).padStart(2,'0')).join('');
function list(value,max,label){fail(Array.isArray(value)&&value.length<=max,label+' exceeds its bound.');return value;}
function identity(value,label){text(value,label,200);fail(value.trim(),label+' is required.');return value;}
async function research(value){
 fail(closed(value,['kind','title','sources','findings','selectedSource','selectedFinding','tab'])&&value.kind==='research','Unsupported research record fields.');
 text(value.title,'Review title',500);fail(value.title.trim(),'Name the research review.');
 list(value.sources,50,'Sources');list(value.findings,1000,'Findings');fail(['sources','matrix','synthesis'].includes(value.tab),'Unsupported research panel.');
 const sources=new Map();let sourceBytes=0;
 for(const source of value.sources){
  fail(closed(source,['id','title','body','url','sha256','coordinate'],['publication']),'Unsupported retained source fields.');identity(source.id,'Source ID');text(source.title,'Source title',500);text(source.body,'Source text',2*1024*1024);text(source.url,'Source URL',2048);
  fail(source.title.trim()&&source.body.trim()&&!sources.has(source.id)&&(!source.url||safeURL(source.url)),'Each source needs unique identity, title, exact text and a safe original address.');
  fail(hashPattern.test(source.sha256)&&await digest(source.body)===source.sha256&&source.coordinate==='UTF-16 offsets into exact original text','A retained source differs from its exact digest or coordinate convention.');
  sourceBytes+=new TextEncoder().encode(source.body).length;fail(sourceBytes<=4*1024*1024,'The retained sources exceed four MiB.');sources.set(source.id,source);
  if(source.publication){const p=source.publication;fail(closed(p,['occurrence','sourceRef','receipt','rightsBasis','retention','publishedAt']),'Unsupported publication provenance.');for(const [key,n]of [['occurrence',256],['sourceRef',256],['receipt',512],['rightsBasis',1024],['retention',256],['publishedAt',128]])text(p[key],key,n);}
 }
 const findings=new Set();for(const finding of value.findings){
  fail(closed(finding,['id','statement','theme','decision','evidence']),'Unsupported finding fields.');identity(finding.id,'Finding ID');fail(!findings.has(finding.id),'Finding identities must be unique.');findings.add(finding.id);text(finding.statement,'Finding',12000);text(finding.theme,'Theme',200);fail(finding.statement.trim()&&['defer','include','exclude'].includes(finding.decision),'Write a finding and retain its explicit review decision.');
  list(finding.evidence,100,'Evidence');fail(finding.evidence.length,'A finding needs at least one exact source passage.');const ids=new Set();for(const ref of finding.evidence){
   fail(closed(ref,['id','sourceId','sourceSha256','start','end','quote','relation']),'Unsupported evidence fields.');identity(ref.id,'Evidence ID');fail(!ids.has(ref.id),'Evidence identities repeat.');ids.add(ref.id);const source=sources.get(ref.sourceId);
   fail(source&&source.sha256===ref.sourceSha256&&Number.isSafeInteger(ref.start)&&Number.isSafeInteger(ref.end)&&ref.start>=0&&ref.end>ref.start&&ref.end<=source.body.length&&source.body.slice(ref.start,ref.end)===ref.quote&&['supports','challenges','context'].includes(ref.relation),'A cited passage no longer matches its exact retained source.');
  }
 }
 fail(value.selectedSource===null||sources.has(value.selectedSource),'The selected source is missing.');fail(value.selectedFinding===null||findings.has(value.selectedFinding),'The selected finding is missing.');return true;
}
export async function validateRecoveryInstrument(kind,value){
 const snapshot=structuredClone(value);
 if(kind==='research')return research(snapshot);
 if(kind==='checks'){await validateDataChecksRecord(snapshot,{parseCSV:csvRead});return true;}
 throw Error('Recovery import does not yet admit '+String(kind)+' material. Open its native editor to import and validate that material directly.');
}
