// SourceFrontier's private working material. CMS owns records, blobs and revisions.
// A saved address is a bookmark, not an observed source or connected account.
const fail=(ok,message)=>{if(!ok)throw Error(message);};
const utf8=new TextEncoder();
export const sourcePrefix='source-material:';
export const sourceFileLimit=24*1024*1024;
export const sourceHash=async data=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',data))].map(x=>x.toString(16).padStart(2,'0')).join('');
const string=(value,name,max)=>{fail(typeof value==='string'&&value.length<=max&&!/[\u0000]/.test(value),'Use a bounded '+name+'.');return value;};
export function sourceAddress(value){
 string(value,'public address',2048);let u;try{u=new URL(value.trim());}catch{throw Error('Enter a complete public HTTP or HTTPS address.');}
 fail(['https:','http:'].includes(u.protocol)&&!u.username&&!u.password,'Use a public address without an embedded login.');
 fail(!/^(localhost$|127\.|0\.|10\.|169\.254\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|\[)/i.test(u.hostname)&&!u.hostname.endsWith('.local'),'Private device addresses do not belong in the public source library.');
 fail(![...u.searchParams.keys()].some(k=>/^(access_token|api_?key|password|secret|token)$/i.test(k)),'Remove credentials from this address before saving it.');
 return u.href;
}
// A feed handoff is an addressable retained item, not an independently fetched body.
// Display this continuation only when its stored coordinates agree exactly.
export function feedSourceContinuation(value){
 const p=value?.provenance;
 if(p?.schema!=='bonfyre.feed-handoff.v1'||typeof p.itemRef!=='string')return null;
 const m=/^([a-z0-9._-]{1,127})\/([a-f0-9]{24})$/.exec(p.itemRef);
 if(!m||p.source!==m[1]||p.publicPath!=='/o/'+m[1]+'/'+m[2]||
    typeof p.family!=='string'||!(/^[a-z0-9._-]{1,64}$/.test(p.family))||
    typeof p.kind!=='string'||!(/^[a-z0-9._-]{1,64}$/.test(p.kind)))return null;
 return {path:p.publicPath,source:p.source,family:p.family,kind:p.kind,itemRef:p.itemRef};
}
export function feedSourceContinuations(value){
 const candidates=[value?.provenance,...(Array.isArray(value?.feedOrigins)&&value.feedOrigins.length<=23?value.feedOrigins:[])];
 const seen=new Set(),items=[];
 for(const candidate of candidates){
  const current=feedSourceContinuation({provenance:candidate});
  if(current&&!seen.has(current.itemRef)){items.push(current);seen.add(current.itemRef);}
 }
 return items;
}
// ForeignTwin catalogue members are review candidates, not saved drafts.
// A source remains foreign until a user reviews a bookmark in this workspace.
export function knownSourcePage(packet){
 fail(packet?.schema==='bonfyre.foreign.twins.v2'&&packet.state==='current','The retained-source snapshot changed. Start a new read.');
 fail(/^[A-Za-z0-9_-]{1,63}$/.test(packet.catalogue_generation||'')&&Number.isSafeInteger(packet.total)&&packet.total>=0&&Array.isArray(packet.twins)&&packet.twins.length<=200&&
   (packet.next_offset===null||Number.isSafeInteger(packet.next_offset)&&packet.next_offset>=0),'The retained-source page is incomplete.');
 const refs=new Set(),rows=[];
 for(const twin of packet.twins){
  const contract=twin?.release?.source_contract,ref=contract?.source;
  fail(typeof ref==='string'&&ref.length<=256&&/^[a-z0-9._-]+$/.test(ref)&&!refs.has(ref),'Ambiguous retained source identity.');
  refs.add(ref);
  const url=sourceAddress(twin.url);
  const title=typeof contract.label==='string'&&contract.label.trim()?contract.label.trim().slice(0,500):new URL(url).hostname;
  const family=typeof contract.family==='string'&&/^[a-zA-Z0-9 _.-]{1,64}$/.test(contract.family)?contract.family:'Other';
  const archived=Number(twin.release?.stages?.archive?.items)||0;
  rows.push({sourceRef:ref,url,title,family,archived:Math.max(0,archived)});
 }
 return {rows,total:packet.total,next:packet.next_offset,generation:packet.catalogue_generation};
}
// Native A2A recipe search is a retained-corpus projection, not a new recipe database.
export function recipeCorpusResults(reply,query){
 const data=reply?.result?.data;
 fail(reply?.jsonrpc==='2.0'&&reply.result?.skill==='recipe-search'&&reply.result?.execution==='free'&&data?.query===query&&
   Number.isSafeInteger(data.count)&&data.count>=0&&Array.isArray(data.hits)&&data.hits.length<=20&&data.count===data.hits.length,
   'The native recipe search result changed.');
 const seen=new Set();
 return data.hits.map(row=>{
  fail(typeof row?.key==='string'&&/^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$/.test(row.key)&&
   typeof row.title==='string'&&row.title.trim()&&row.title.length<=500&&!seen.has(row.key),
   'Invalid or repeated native recipe identity.');
  seen.add(row.key);return {key:row.key,title:row.title};
 });
}
export function sourceTags(value){
 const tags=typeof value==='string'?value.split(',').map(s=>s.trim()).filter(Boolean):value;
 fail(Array.isArray(tags)&&tags.length<=24&&tags.every(t=>typeof t==='string'&&t.length>0&&t.length<=64&&!/[\r\n\t\u0000]/.test(t)),'Use up to 24 tags of at most 64 characters.');
 return [...new Set(tags)];
}
export function checkSource(value){
 fail(value?.schema==='bonfyre.source-material.v1'&&['file','link'].includes(value.kind)&&['active','archived'].includes(value.state),'This record is not a supported source material.');
 fail(/^[a-f0-9]{64}$/.test(value.id)&&/^[a-f0-9]{64}$/.test(value.contentSHA256),'The source identity is incomplete.');
 fail(string(value.title,'source title',500).trim(),'Name this source.');string(value.collection,'collection',120);string(value.notes,'notes',8000);sourceTags(value.tags);
 fail(Array.isArray(value.history)&&value.history.length<=50,'The source history exceeds its bound.');
 if(value.kind==='file')fail(value.original instanceof Blob&&value.original.size<=sourceFileLimit,'The original file is missing or exceeds 24 MiB.');
 else fail(sourceAddress(value.url)===value.url,'The saved address changed shape.');
 return value;
}
export async function prepareFile(file,{title=file?.name??'',collection='',tags=[],notes=''}={}){
 fail(file instanceof Blob&&typeof file.name==='string'&&file.name.length>0&&file.size>0&&file.size<=sourceFileLimit,'Choose a nonempty original file of at most 24 MiB.');
 const bytes=new Uint8Array(await file.arrayBuffer()),contentSHA256=await sourceHash(bytes),ext=file.name.split('.').pop().toLowerCase();
 const textLike=['txt','md','markdown','csv','tsv','vtt','srt','ris','log','xml'].includes(ext)&&!bytes.subarray(0,8192).includes(0);
 let preview='';if(textLike)preview=new TextDecoder().decode(bytes.subarray(0,8000));
 const media=bytes[0]===0x89&&bytes[1]===0x50?'image/png':bytes[0]===0xff&&bytes[1]===0xd8?'image/jpeg':String.fromCharCode(...bytes.subarray(0,4))==='%PDF'?'application/pdf':textLike?'text/plain':/^(audio|video)\//.test(file.type)?file.type:'application/octet-stream';
 const value={schema:'bonfyre.source-material.v1',id:await sourceHash(utf8.encode(file.name+'\0'+contentSHA256)),kind:'file',state:'active',title,collection,tags:sourceTags(tags),notes,original:file,filename:file.name,media,bytes:file.size,contentSHA256,textLike,preview,createdAt:new Date().toISOString(),history:[]};
 checkSource(value);return {key:sourcePrefix+value.id,expectedRevision:0,value};
}
export async function prepareLink(url,{title='',collection='',tags=[],notes=''}={}){
 const address=sourceAddress(url),contentSHA256=await sourceHash(utf8.encode(address));
 const value={schema:'bonfyre.source-material.v1',id:contentSHA256,kind:'link',state:'active',title:title.trim()||new URL(address).hostname,collection,tags:sourceTags(tags),notes,url:address,contentSHA256,createdAt:new Date().toISOString(),history:[]};
 checkSource(value);return {key:sourcePrefix+value.id,expectedRevision:0,value};
}
export function reviseSource(row,patch,action='edit'){
 fail(row&&Number.isSafeInteger(row.revision)&&row.revision>0&&!row.deleted&&row.key===sourcePrefix+checkSource(row.value).id,'Reload this exact source before changing it.');
 fail(['edit','archive','restore'].includes(action),'Choose a supported source operation.');
 fail(patch&&typeof patch==='object'&&!Array.isArray(patch)&&Object.keys(patch).every(k=>['title','collection','tags','notes'].includes(k)),'Only descriptive fields can be edited here. Original content is immutable.');
 if(action==='archive')fail(row.value.state==='active','This source is already archived.');
 if(action==='restore')fail(row.value.state==='archived','This source is already active.');
 const value={...row.value,...patch,tags:sourceTags(patch.tags??row.value.tags),state:action==='archive'?'archived':action==='restore'?'active':row.value.state,history:[...row.value.history,{action,fromRevision:row.revision,at:new Date().toISOString()}].slice(-50)};
 checkSource(value);return {key:row.key,expectedRevision:row.revision,value};
}
export async function readSourceText(row,{readOnly=false}={}){
 const source=checkSource(row.value);fail((readOnly||source.state==='active')&&source.kind==='file'&&source.textLike,'Choose an active text, Markdown, CSV, subtitle or RIS file. Other originals are retained without invented extraction.');
 fail(source.original.size<=2*1024*1024,'Read text files up to 2 MiB here. Download this original to read it in another app.');
 const bytes=await source.original.arrayBuffer();fail(await sourceHash(bytes)===source.contentSHA256,'The original file no longer matches its retained digest.');
 let body;try{body=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}catch{throw Error('This file is not valid UTF-8. Convert a copy; keep the original intact.');}
 fail(readOnly||body.trim(),'The original contains no usable text.');return body;
}
export async function prepareMaterialResearch({rows,title,existing=null,sourceRecord,digest,binding}){
 fail(Array.isArray(rows)&&rows.length>0&&rows.length<=50&&typeof sourceRecord==='function'&&typeof digest==='function'&&/^ws-[a-f0-9]{40}$/.test(binding),'Choose sources and an installed receiving research editor.');
 fail(typeof title==='string'&&title.trim()&&title.length<=500,'Name the research review.');
 if(existing)fail(!existing.deleted&&existing.key.startsWith('draft:instruments:research:')&&existing.value?.kind==='research'&&Array.isArray(existing.value.sources)&&Array.isArray(existing.value.findings),'Choose an existing research review.');
 const next=existing?structuredClone(existing.value):{kind:'research',title:title.trim(),sources:[],findings:[],selectedSource:null,selectedFinding:null,tab:'sources'};
 const added=[],unchanged=[];for(const old of next.sources)fail(await digest(old.body)===old.sha256,'An existing research source has changed. Reconcile it in its editor.');
 for(const row of rows){const body=await readSourceText(row),value=row.value,id='material-'+value.id,record=await sourceRecord({id,title:value.title,body,url:''});
  fail(record?.body===body&&record.sha256===await digest(body),'The receiving editor did not retain the exact original text.');
  record.material={binding,key:row.key,revision:row.revision,sha256:value.contentSHA256,filename:value.filename};
  const old=next.sources.find(s=>s.id===id);if(old){fail(old.sha256===record.sha256,'An existing cited source differs. Review correction in research.');unchanged.push(id);}else{next.sources.push(record);added.push(id);}
 }
 fail(next.sources.length<=50&&next.sources.reduce((sum,s)=>sum+utf8.encode(s.body).length,0)<=4*1024*1024,'The receiving review exceeds its source or text limit.');
 if(!existing)next.selectedSource=added[0]??null;
 const change={key:existing?.key??'draft:instruments:research:'+crypto.randomUUID(),expectedRevision:existing?.revision??0,value:next};
 return {change,added,unchanged,preimage:existing?await digest(existing.value):null,digest:await digest(change)};
}
export function sourceSummary(rows){return rows.map(row=>({key:row.key,revision:row.revision,title:row.value.title,kind:row.value.kind,state:row.value.state,collection:row.value.collection,tags:row.value.tags,bytes:row.value.bytes??0,sha256:row.value.contentSHA256}));}
