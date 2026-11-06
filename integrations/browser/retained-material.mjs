// Retained paths are evidence coordinates, not URLs or permission to read a file.
// Only records returned by the current workspace owner can resolve a reference.
const HEX=/^[a-f0-9]{64}$/;
export const CONTRACT_PREFIX='draft:instruments:research:history-contract-';
const check=(ok,message)=>{if(!ok)throw Error(message);};
const byteDigest=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
export const textDigest=text=>byteDigest(new TextEncoder().encode(text));
export function localReferences(body){
  check(typeof body==='string'&&body.length<=4*1024*1024,'Retained text exceeds the reader limit.');
  const pattern=/(?:file:\/\/)?\/(?:Users|home|mnt|private\/tmp|tmp)\/[^\s<>`"'\]\}]+|~\/(?:\.bonfyre|Downloads|Documents|Developer)\/[^\s<>`"'\]\}]+|sandbox:\/[^\s<>`"'\]\}]+|[A-Za-z]:\\(?:Users|Temp)\\[^\s<>`"'\]\}]+/g;
  return [...body.matchAll(pattern)].flatMap(match=>{
    const raw=match[0].replace(/[.,;:!?)]*$/,'');
    return raw?[{start:match.index,end:match.index+raw.length,raw,name:raw.replaceAll('\\','/').split('/').at(-1)||'Original file'}]:[];
  });
}
export function referenceDescriptor(source,token){
  const release=/^~\/\.bonfyre\/releases\/([A-Za-z0-9_-]+)\/$/.exec(token.raw);
  if(release)return {kind:'provenance',label:'Historical release: '+release[1]};
  const h=source?.history,digest=/\/([a-f0-9]{64})\.md$/.exec(token.raw)?.[1];
  if(digest&&h?.contractKey===CONTRACT_PREFIX+digest.slice(0,32))
    return {kind:'contract',digest,key:h.contractKey,label:'Shared production contract'};
  if(digest&&h?.detailSha256===digest)
    return {kind:'contract',digest,key:CONTRACT_PREFIX+digest.slice(0,32),label:'Complete production contract'};
  return {kind:'attachment',locator:token.raw,label:token.name};
}
export function requirementRange(source){
  const body=source.body;
  if(!source.history?.goalKey)return {start:0,end:body.length};
  const marker=/(?:Requirement SHA-256: [a-f0-9]{64}|Source kind: [^\n]+)\n\n/.exec(body);
  if(!marker)return {start:0,end:body.length};
  const start=marker.index+marker[0].length;let end=body.length;
  for(const boundary of ['\n\nReceiving context:\n','\n\nReceiving allocation (','\n\nOriginal surrounding context:\n','\n\nRelated original obligations (']){
    const at=body.indexOf(boundary,start);if(at>=0)end=Math.min(end,at);
  }
  while(end>start&&/\s/.test(body[end-1]))end--;
  return {start,end};
}
export function materialSegments(source,range={start:0,end:source.body.length}){
  check(Number.isSafeInteger(range.start)&&Number.isSafeInteger(range.end)&&range.start>=0&&range.end>=range.start&&range.end<=source.body.length,'Invalid retained text range.');
  const parts=[];let at=range.start;
  for(const token of localReferences(source.body)){
    if(token.end<=range.start||token.start>=range.end)continue;
    check(token.start>=range.start&&token.end<=range.end,'This excerpt cuts through a source reference.');
    if(token.start>at)parts.push({kind:'text',start:at,end:token.start,text:source.body.slice(at,token.start)});
    parts.push({kind:'reference',start:token.start,end:token.end,reference:referenceDescriptor(source,token)});at=token.end;
  }
  if(at<range.end)parts.push({kind:'text',start:at,end:range.end,text:source.body.slice(at,range.end)});
  return parts;
}
export async function readContract(ref,{store,signal}={}){
  check(ref?.kind==='contract'&&HEX.test(ref.digest)&&ref.key===CONTRACT_PREFIX+ref.digest.slice(0,32),'Invalid retained contract identity.');
  signal?.throwIfAborted();const rows=await store.readLocal(ref.key);signal?.throwIfAborted();
  check(Array.isArray(rows)&&rows.length>0&&rows.length<=16,'The complete contract is not attached to this account.');
  const pieces=[];let total=null,bytes=0;
  for(const row of rows){
    check(!row.deleted&&Number.isSafeInteger(row.revision)&&row.revision>0&&row.value?.kind==='research'&&row.value.sources?.length===1,'A contract part changed shape.');
    const source=row.value.sources[0],h=source.history;
    check(h&&h.detailSha256===ref.digest&&Number.isSafeInteger(h.part)&&Number.isSafeInteger(h.parts)&&h.parts>0&&h.parts<=16&&h.part>=0&&h.part<h.parts,'A contract part has a different identity.');
    check(row.key===ref.key+(h.part?'-'+h.part:'')&&(total===null||total===h.parts),'The contract record coordinates changed.');total=h.parts;
    check(typeof source.body==='string'&&HEX.test(source.sha256)&&await textDigest(source.body)===source.sha256,'A contract part failed its content check.');
    check(Number.isSafeInteger(h.detailStart)&&Number.isSafeInteger(h.detailEnd)&&h.detailStart>=0&&h.detailEnd-h.detailStart===source.body.length,'Contract offsets changed.');
    bytes+=new TextEncoder().encode(source.body).length;check(bytes<=1024*1024,'This contract exceeds the reader limit.');
    pieces.push({part:h.part,start:h.detailStart,end:h.detailEnd,body:source.body,key:row.key,revision:row.revision});
  }
  pieces.sort((a,b)=>a.part-b.part);
  check(pieces.length===total&&pieces.every((p,i)=>p.part===i&&p.start===(i?pieces[i-1].end:0)),'A contract part is missing or repeated.');
  const body=pieces.map(p=>p.body).join('');check(await textDigest(body)===ref.digest,'The complete contract does not match the retained original.');signal?.throwIfAborted();
  return {id:'contract-'+ref.digest,title:ref.label,body,sha256:ref.digest,parts:pieces,history:{detailSha256:ref.digest},readOnly:true};
}
async function originatingSource(ref,{store,signal}={}){
  check(typeof ref.recordKey==='string'&&ref.recordKey.startsWith('draft:instruments:research:')&&ref.recordKey.length<512&&typeof ref.sourceId==='string'&&HEX.test(ref.sourceSha256),'Open this reference from its saved research record.');
  signal?.throwIfAborted();const row=await store.readVersion(ref.recordKey);signal?.throwIfAborted();
  check(row&&!row.deleted&&row.value?.kind==='research'&&Array.isArray(row.value.sources),'The originating record is unavailable.');
  const matches=row.value.sources.filter(s=>s.id===ref.sourceId);check(matches.length===1,'The originating source identity changed.');const source=matches[0];
  check(source.sha256===ref.sourceSha256&&await textDigest(source.body)===ref.sourceSha256,'The originating source changed. Reopen its current version.');return {row,source};
}
export async function readLibraryOriginal(ref,options={}){
  const m=ref?.material;
  check(ref?.kind==='material'&&m&&/^ws-[a-f0-9]{40}$/.test(m.binding)&&/^source-material:[a-f0-9]{64}$/.test(m.key)&&Number.isSafeInteger(m.revision)&&m.revision>0&&HEX.test(m.sha256)&&typeof m.filename==='string'&&m.filename.length<=500&&typeof options.libraryStore==='function','The original source-library reference is incomplete.');
  const {source}=await originatingSource(ref,options);
  check(source.material&&Object.keys(m).length===5&&Object.keys(source.material).length===5&&['binding','key','revision','sha256','filename'].every(key=>source.material[key]===m[key])&&source.sha256===m.sha256,'The saved research source reference changed. Reopen the research review.');
  options.signal?.throwIfAborted();
  const row=await options.libraryStore(m.binding).readVersion(m.key);options.signal?.throwIfAborted();
  const v=row?.value;check(row&&!row.deleted&&row.key===m.key&&Number.isSafeInteger(row.revision)&&row.revision>=m.revision&&v?.schema==='bonfyre.source-material.v1'&&v.kind==='file'&&['active','archived'].includes(v.state)&&m.key==='source-material:'+v.id&&v.contentSHA256===m.sha256&&v.original instanceof Blob&&v.original.size<=2*1024*1024,'The referenced original is unavailable or changed. Your research copy remains intact.');
  const bytes=await v.original.arrayBuffer();
  check(await byteDigest(bytes)===m.sha256,'The original file does not match its retained digest.');
  const body=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);check(body===source.body,'The original text differs from the research copy.');options.signal?.throwIfAborted();
  return {id:source.id,title:ref.label,body,sha256:m.sha256,parts:[{key:m.key,revision:row.revision,href:'/apps/'+m.binding+'/sources/?'+new URLSearchParams({record:m.key})}],material:{revision:m.revision,currentRevision:row.revision,state:v.state},readOnly:true};
}
export async function readAttachment(ref,options={}){
  check(ref?.kind==='attachment'&&typeof ref.locator==='string'&&ref.locator.length<=4096,'Invalid original source reference.');const {row,source}=await originatingSource(ref,options),{signal}=options;
  const path=ref.locator.replace(/^file:\/\//,''),anchor=path.indexOf('/.bonfyre/run8/canonical/');
  check(anchor>=0&&/\/PROFILES\/[A-Za-z0-9_.-]+\.yaml$/.test(path),'This reference has no attached readable source copy.');
  const coordinate=path.slice(anchor+1),marker='\n'+coordinate+'\n',at=source.body.indexOf(marker),tail='\nHistorical reconstruction observed_at:';
  check(at>=0&&source.body.indexOf(marker,at+marker.length)<0,'An exact source copy is not attached at this coordinate.');const start=at+marker.length,end=source.body.indexOf(tail,start);
  const context=source.body.indexOf('Original surrounding context:\n');
  check(end>start&&context>=0&&context<at,'The embedded source copy is incomplete.');const body=source.body.slice(start,end);
  signal?.throwIfAborted();return {id:'embedded-'+await textDigest(body),title:ref.label,body,sha256:await textDigest(body),parentSha256:source.sha256,parts:[{key:row.key,revision:row.revision}],readOnly:true};
}
export async function readMaterial(ref,options){check(['contract','attachment','material'].includes(ref?.kind),'A historical deployment label is not a readable source.');return ref.kind==='contract'?readContract(ref,options):ref.kind==='material'?readLibraryOriginal(ref,options):readAttachment(ref,options);}
export function appendMaterial(node,source,{range,onReference}={}){
  node.replaceChildren();for(const part of materialSegments(source,range)){
    if(part.kind==='text'){
      const span=node.ownerDocument.createElement('span');span.dataset.originalStart=String(part.start);span.dataset.originalEnd=String(part.end);span.textContent=part.text;node.append(span);
    }else{
      const button=node.ownerDocument.createElement(part.reference.kind==='provenance'?'span':'button');button.className='rm-reference';button.dataset.sourceReference=part.reference.kind;button.textContent=part.reference.label;
      if(part.reference.kind!=='provenance'){button.type='button';button.setAttribute('aria-label','Read retained source: '+part.reference.label);button.onclick=()=>onReference?.(part.reference);}
      else button.title='Original deployment identifier; no source file is implied.';node.append(button);
    }
  }return node;
}
export function selectedMaterialRange(box,source,selection=globalThis.getSelection()){
  check(selection?.rangeCount===1,'Select one passage from this source.');const range=selection.getRangeAt(0);
  check(box.contains(range.startContainer)&&box.contains(range.endContainer),'Select words inside the retained source.');
  for(const control of box.querySelectorAll('[data-source-reference]'))check(!range.intersectsNode(control),'Select passage text without crossing a source-reference control.');
  function offset(node,n){
    const element=node.nodeType===3?node.parentElement:node,span=element.closest?.('[data-original-start]');check(span&&box.contains(span),'Select text, not a source-reference control.');
    const before=range.cloneRange();before.selectNodeContents(span);before.setEnd(node,n);return Number(span.dataset.originalStart)+before.toString().length;
  }
  const start=offset(range.startContainer,range.startOffset),end=offset(range.endContainer,range.endOffset);
  check(end>start&&source.body.slice(start,end)===range.toString(),'The selected passage no longer matches its original text.');return {start,end};
}
export function highlightMaterial(box,source,ref){
  if(source.body.slice(ref.start,ref.end)!==ref.quote)return false;
  const spans=[...box.querySelectorAll('[data-original-start]')],first=spans.find(n=>+n.dataset.originalStart<=ref.start&&+n.dataset.originalEnd>ref.start),last=spans.find(n=>+n.dataset.originalStart<ref.end&&+n.dataset.originalEnd>=ref.end);
  if(!first?.firstChild||!last?.firstChild)return false;
  const range=box.ownerDocument.createRange();range.setStart(first.firstChild,ref.start-Number(first.dataset.originalStart));range.setEnd(last.firstChild,ref.end-Number(last.dataset.originalStart));
  for(const n of box.querySelectorAll('[data-source-reference]'))if(range.intersectsNode(n))return false;
  const selected=box.ownerDocument.getSelection();selected.removeAllRanges();selected.addRange(range);box.scrollIntoView({block:'center'});return true;
}
