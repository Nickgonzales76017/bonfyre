// Account adapter around the original shipment checker. The checker remains
// canonical; this module binds exact file bytes and review inputs to a draft.
import {readShipments, checkBatch, normalDate, DOCUMENT_TYPES, pdfText, jpegText, jpegPhotoDate, storedZip} from '../../components/BonfyreServiceSite/shipment-check.mjs';
export const bounds=Object.freeze({files:48,fileBytes:12*1024*1024,totalBytes:48*1024*1024,listBytes:96*1024,shipments:500,decodedBytes:2*1024*1024});
const assert=(yes,message)=>{if(!yes)throw Error(message);};
const plain=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const text=(s,n)=>typeof s==='string'&&s.length<=n&&!/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(s);
export const sha=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join('');
export function shipmentList(value){
 assert(typeof value==='string'&&new TextEncoder().encode(value).length<=bounds.listBytes,'Use a shipment list of at most 96 KiB.');
 if(!value.trim())return[];
 const rows=readShipments(value);assert(rows.length<=bounds.shipments,'Review at most 500 shipments in one batch.');
 for(const row of rows){assert(/^[A-Za-z0-9][A-Za-z0-9 _.:-]{0,95}$/.test(row.shipment_id)&&!row.shipment_id.includes('..'),'A shipment reference contains characters that cannot enter a filename.');for(const k of ['origin','destination','ship_date'])assert(text(row[k],300),'A shipment field exceeds its review bound.');}
 return rows;
}
export function fileName(value){assert(text(value,200)&&value.trim()&&!/[\x00-\x1f\x7f/\\]/.test(value)&&value!=='.'&&value!=='..','Use a plain file name without path components.');return value;}
async function inflate(bytes){
 const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('deflate')).getReader(),parts=[];let count=0;
 try{for(;;){const p=await reader.read();if(p.done)break;count+=p.value.length;assert(count<=bounds.decodedBytes,'Decoded PDF text exceeds 2 MiB.');parts.push(p.value);}}finally{try{await reader.cancel();}catch{}reader.releaseLock();}
 const out=new Uint8Array(count);let at=0;for(const p of parts){out.set(p,at);at+=p.length;}return out;
}
export async function inspectFile(file){
 assert(file instanceof Blob&&Number.isSafeInteger(file.size)&&file.size<=bounds.fileBytes,'Choose each original file within the 12 MiB limit.');
 fileName(file.name);const bytes=new Uint8Array(await file.arrayBuffer()),hash=await sha(bytes),isPdf=bytes[0]===37&&bytes[1]===80&&bytes[2]===68&&bytes[3]===70;
 let extracted='',extraction='No embedded text was read. Review the original and enter its details.';
 try{extracted=isPdf?await pdfText(bytes,inflate):jpegText(bytes);if(extracted)extraction=isPdf?'Embedded PDF text; no OCR or document authenticity check.':'Embedded photo metadata; not a reading of the visible image.';}catch{extraction='Embedded text could not be decoded within the read limit. Review the original.';}
 return{key:hash+':'+file.name,name:file.name,size:file.size,sha256:hash,type:isPdf?'application/pdf':file.type||'',text:extracted.slice(0,20000),photo_date:isPdf?'':jpegPhotoDate(bytes),extraction,body:file};
}
export async function appendFiles(current,incoming,{alive=()=>true}={}){
 assert(Array.isArray(current)&&current.length<=bounds.files,'The current batch is unavailable.');const files=Array.from(incoming);
 assert(files.length+current.length<=bounds.files,'Use at most 48 original files in one batch.');
 assert(current.reduce((n,f)=>n+f.size,0)+files.reduce((n,f)=>n+f.size,0)<=bounds.totalBytes,'Keep one batch within 48 MiB.');
 for(const f of files){fileName(f.name);assert(f.size<=bounds.fileBytes,'No files were added: one file exceeds 12 MiB.');}
 const result=[...current],keys=new Set(current.map(f=>f.key));
 for(const f of files){assert(alive(),'This account or batch changed during file reading.');const fact=await inspectFile(f);assert(alive(),'This account or batch changed during file reading.');if(!keys.has(fact.key)){keys.add(fact.key);result.push(fact);}}
 return result;
}
export function reviewChoices(chosen,files,shipments){
 assert(plain(chosen)&&Object.keys(chosen).length<=bounds.files,'The saved review choices are incomplete.');const keys=new Set(files.map(f=>f.key)),ids=new Set(shipments.map(s=>s.shipment_id)),result={};
 for(const[key,pick]of Object.entries(chosen)){
  assert(keys.has(key)&&plain(pick)&&Object.keys(pick).every(k=>['shipment_id','document_type','date'].includes(k)),'A correction names an unknown file or field.');const row={};
  if(pick.shipment_id!==undefined){assert(ids.has(pick.shipment_id),'A correction names a shipment outside this batch.');row.shipment_id=pick.shipment_id;}
  if(pick.document_type!==undefined){assert(DOCUMENT_TYPES.includes(pick.document_type),'A correction has an unknown document type.');row.document_type=pick.document_type;}
  if(pick.date!==undefined){assert(/^\d{4}-\d{2}-\d{2}$/.test(pick.date)&&normalDate(pick.date)===pick.date,'Use a real calendar date for a correction.');row.date=pick.date;}
  result[key]=row;
 }return result;
}
export function snapshotOf(state){
 const shipments=shipmentList(state.shipmentsText),chosen=reviewChoices(state.chosen,state.files,shipments);
 assert(state.files.length<=bounds.files&&state.files.reduce((n,f)=>n+f.size,0)<=bounds.totalBytes,'The batch exceeds its file bound.');
 return{version:1,shipmentsText:state.shipmentsText,chosen,files:state.files.map(f=>{fileName(f.name);assert(/^[a-f0-9]{64}$/.test(f.sha256)&&f.body instanceof Blob&&f.body.size===f.size&&f.key===f.sha256+':'+f.name,'An original file changed identity.');return{name:f.name,sha256:f.sha256,size:f.size,type:f.type,body:f.body};})};
}
export async function restoreSnapshot(value,{alive=()=>true}={}){
 assert(plain(value)&&value.version===1&&Object.keys(value).every(k=>['version','shipmentsText','chosen','files'].includes(k))&&Array.isArray(value.files)&&value.files.length<=bounds.files,'This is not a supported shipment batch.');
 const shipments=shipmentList(value.shipmentsText);let total=0;const files=[],seen=new Set();
 for(const f of value.files){assert(plain(f)&&f.body instanceof Blob&&text(f.type,150)&&/^[a-f0-9]{64}$/.test(f.sha256)&&Number.isSafeInteger(f.size)&&f.size===f.body.size,'An original file reference is invalid.');fileName(f.name);total+=f.size;assert(total<=bounds.totalBytes,'The retained batch exceeds its byte bound.');assert(alive(),'Current account access changed.');const fact=await inspectFile(new File([f.body],f.name,{type:f.type,lastModified:0}));assert(fact.sha256===f.sha256&&!seen.has(fact.key),'A retained file failed its digest or identity check.');seen.add(fact.key);files.push(fact);}
 assert(alive(),'Current account access changed.');return{shipmentsText:value.shipmentsText,shipments,files,chosen:reviewChoices(value.chosen,files,shipments)};
}
export function evaluateBatch(state){return checkBatch(shipmentList(state.shipmentsText),state.files,reviewChoices(state.chosen,state.files,shipmentList(state.shipmentsText)));}
const cell=v=>{let s=String(v??'');if(/^[=+@\-\t\r]/.test(s))s="'"+s;return /[",\r\n]/.test(s)?'"'+s.replaceAll('"','""')+'"':s;};
const csv=(rows)=>rows.map(row=>row.map(cell).join(',')).join('\r\n')+'\r\n';
export function batchReports(state){
 const result=evaluateBatch(state);return{result,checked:csv([['file','shipment','kind','date','date_basis','disposition','reason','proposed_name','sha256','bytes'],...result.rows.map(r=>[r.file_name,r.shipment_id,r.document_type,r.date,r.date_from,r.disposition,r.reasons.join('; '),r.proposed_name,r.sha256,r.bytes])]),missing:csv([['shipment','ship_date','origin','destination','issue'],...result.missing.map(r=>[r.shipment_id,r.ship_date,r.origin,r.destination,r.issue])])};
}
export async function reviewedArchive(state){
 const {result,checked,missing}=batchReports(state),ready=result.rows.filter(r=>r.disposition==='ready');assert(ready.length,'There are no ready files in this review.');const entries=[],names=new Set();
 for(const row of ready){fileName(row.proposed_name);assert(!names.has(row.proposed_name),'Two reviewed files have the same proposed name.');names.add(row.proposed_name);const file=state.files.find(f=>f.key===row.key),bytes=new Uint8Array(await file.body.arrayBuffer());assert(await sha(bytes)===row.sha256,'An original file failed verification. No archive was produced.');entries.push({name:row.proposed_name,bytes});}
 const enc=new TextEncoder();entries.push({name:'checked-files.csv',bytes:enc.encode(checked)},{name:'missing-delivery-proof.csv',bytes:enc.encode(missing)},{name:'READ-ME.txt',bytes:enc.encode('Review export. Original source bytes are unchanged; names are proposed. Presence of a document is not independent verification of delivery. Corrections reflect human review. No originals were renamed and no carrier, customer or accounting system was updated.\n')});return storedZip(entries);
}
