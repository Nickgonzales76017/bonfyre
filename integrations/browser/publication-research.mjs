// Prepare the existing research owner's record; native workspace storage owns it.
const fail=(ok,message)=>{if(!ok)throw Error(message);};
const utf8=new TextEncoder();
const plain=v=>v&&typeof v==='object'&&!Array.isArray(v);
const canonical=v=>Array.isArray(v)?v.map(canonical):plain(v)?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
export async function researchDigest(value){const body=typeof value==='string'?value:JSON.stringify(canonical(value));return [...new Uint8Array(await crypto.subtle.digest('SHA-256',utf8.encode(body)))].map(n=>n.toString(16).padStart(2,'0')).join('');}
export function publicationInput(row){
 fail(Array.isArray(row)&&row.length===9&&row.every(v=>typeof v==='string'),'The publication input is incomplete.');
 const [id,title,body,url,sourceRef,receipt,rightsBasis,retention,publishedAt]=row;
 fail(id&&id.length<=256&&title.trim()&&title.length<=300&&body.length<=12000&&sourceRef&&sourceRef.length<=256&&receipt&&receipt.length<=512,'The publication input exceeds its retained bounds.');
 let href;try{href=new URL(url);}catch{}
 fail(href&&['https:','http:'].includes(href.protocol)&&!href.username&&!href.password&&url.length<=2048,'The original source address is unavailable.');
 fail(rightsBasis.length<=1024&&retention.length<=256&&publishedAt.length<=128,'The source receipt exceeds its bound.');
 return {id,title,body,url,sourceRef,receipt,rightsBasis,retention,publishedAt};
}
export function publicationPacket(packet){
 const retained=Array.isArray(packet)&&packet[0]===3;
 fail(!retained||(packet.length===4&&typeof packet[3]==='string'&&/^[a-f0-9]{64}$/.test(packet[3])),'The retained source snapshot is incomplete.');
 fail(Array.isArray(packet)&&packet.length===(retained?4:3)&&[1,2,3].includes(packet[0])&&Array.isArray(packet[1])&&packet[1].length<=24&&Number.isSafeInteger(packet[2])&&packet[2]>=0,'The publication page is incomplete.');
 const inputs=packet[1].map(row=>{
  if(packet[0]===1)return publicationInput(row);
  fail(Array.isArray(row)&&row.length===10&&typeof row[9]==='string'&&row[9].length<=256&&!/[\u0000-\u001f\u007f]/.test(row[9]),'The native source subject is incomplete.');
  return {...publicationInput(row.slice(0,9)),sourceSubject:row[9]};
 });fail(new Set(inputs.map(s=>s.sourceRef+'\0'+s.id)).size===inputs.length,'The publication page repeats an identity.');
 return retained?{inputs,cursor:packet[2],snapshot:packet[3]}:{inputs,cursor:packet[2]};
}
export async function prepareResearchInputs({inputs,title,existing=null,sourceRecord}){
 fail(typeof sourceRecord==='function','Connect the installed research source owner.');
 fail(Array.isArray(inputs)&&inputs.length&&inputs.length<=50,'Choose one to 50 retained excerpts.');
 fail(typeof title==='string'&&title.trim()&&title.length<=500,'Name the receiving review.');
 const checked=inputs.map(s=>{const input=publicationInput([s.id,s.title,s.body,s.url,s.sourceRef,s.receipt,s.rightsBasis,s.retention,s.publishedAt]);if(s.inputSnapshot!==undefined){fail(typeof s.inputSnapshot==='string'&&/^[a-f0-9]{64}$/.test(s.inputSnapshot)&&/^[a-f0-9]{24}$/.test(input.id),'The selected public input fingerprint is incomplete.');input.inputSnapshot=s.inputSnapshot;}return input;});
 fail(new Set(checked.map(s=>s.sourceRef+'\0'+s.id)).size===checked.length,'Choose each publication only once.');
 let next;
 if(existing){
  fail(existing.value?.kind==='research'&&Array.isArray(existing.value.sources)&&Array.isArray(existing.value.findings)&&Number.isSafeInteger(existing.revision)&&existing.revision>=0,'Choose an exact research draft and its current revision.');
  fail(existing.key.startsWith('draft:instruments:research:')&&/^[A-Za-z0-9:._-]{1,240}$/.test(existing.key),'The receiving record belongs to another editor.');
  next=structuredClone(existing.value);
  for(const source of next.sources)fail(typeof source.body==='string'&&await researchDigest(source.body)===source.sha256,'A receiving source no longer matches its retained digest.');
 }else next={kind:'research',title,sources:[],findings:[],selectedSource:null,selectedFinding:null,tab:'sources'};
 const added=[],unchanged=[];
 for(const input of checked){
  fail(input.body.trim(),'This publication has no retained excerpt. Open its original source to obtain usable text.');
  const id='publication-'+await researchDigest([input.sourceRef,input.id]),record=await sourceRecord({id,title:input.title,body:input.body,url:input.url});
  fail(record?.id===id&&record.body===input.body&&record.title===input.title&&record.url===input.url&&record.sha256===await researchDigest(input.body),'The research owner did not retain the exact selected text.');
  record.publication={occurrence:input.id,sourceRef:input.sourceRef,receipt:input.receipt,rightsBasis:input.rightsBasis,retention:input.retention,publishedAt:input.publishedAt};
  if(input.inputSnapshot)record.publication.inputSnapshot=input.inputSnapshot;
  const old=next.sources.find(s=>s.id===id);
  if(old){
   fail(old.sha256===record.sha256&&old.title===record.title&&old.url===record.url&&await researchDigest(old.publication)===await researchDigest(record.publication),'This source has a changed excerpt or receipt. Review the correction before replacing cited material.');
   unchanged.push(id);continue;
  }
  next.sources.push(record);added.push(id);
 }
 fail(next.sources.length<=50&&new Set(next.sources.map(s=>s.id)).size===next.sources.length&&next.sources.reduce((n,s)=>n+utf8.encode(s.body).length,0)<=4*1024*1024,'The receiving review exceeds 50 distinct sources or 4 MiB of text.');
 if(!existing){next.selectedSource=added[0];next.title=title.trim();}
 const change={key:existing?.key??'draft:instruments:research:'+crypto.randomUUID(),expectedRevision:existing?.revision??0,value:next};
 return {change,added,unchanged,preimage:existing?await researchDigest(existing.value):null,digest:await researchDigest(change)};
}
export async function savePreparedResearch(store,preview){
 fail(preview?.change&&await researchDigest(preview.change)===preview.digest,'The prepared review changed. Review its inputs again.');
 const {change}=preview;
 // An uncertain reply may follow a committed write; exact matching readback settles it.
 const previous=await store.readVersion(change.key);
 if(previous&&!previous.deleted&&previous.revision===change.expectedRevision+1&&await researchDigest(previous.value)===await researchDigest(change.value))return previous;
 fail((previous?.revision??0)===change.expectedRevision&&!previous?.deleted&&(!previous||await researchDigest(previous.value)===preview.preimage),'The receiving review changed. Prepare the addition against its current revision.');
 if(!preview.added.length&&previous)return previous;
 try{return await store.saveLocal(change.key,change.value,change.expectedRevision);}
 catch(error){
  // An acknowledgement can disappear after commit. Read once; never replay a
  // write automatically, and never accept a changed or deleted receiving row.
  let current;try{current=await store.readVersion(change.key);}catch{throw error;}
  if(current&&!current.deleted&&current.revision===change.expectedRevision+1&&await researchDigest(current.value)===await researchDigest(change.value))return current;
  throw error;
 }
}
