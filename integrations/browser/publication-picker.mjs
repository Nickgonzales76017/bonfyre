import {decode} from './workspace-msgpack.mjs';
import {publicationPacket,researchDigest} from './publication-research.mjs';
async function readSourcePacket(response,limit){
 if(!response.ok||response.headers.get('content-type')?.split(';')[0]!=='application/msgpack'){await response.body?.cancel();throw Object.assign(Error(response.status===409?'This source changed between pages. Reload it before selecting more material.':'The retained sources could not be read for this destination.'),{status:response.status});}
 const reader=response.body?.getReader();if(!reader)throw Error('The native reader returned no material.');const parts=[];let size=0;
 try{for(;;){const part=await reader.read();if(part.done)break;size+=part.value.length;if(size>limit)throw Error('The source page exceeds its bound.');parts.push(part.value);}}finally{await reader.cancel();reader.releaseLock();}
 const bytes=new Uint8Array(size);let at=0;for(const part of parts){bytes.set(part,at);at+=part.length;}return decode(bytes);
}
// One bounded read of the existing admitted publication owner. No source is fetched here.
export async function readPublicationPage(endpoint,{query='',cursor=0,source='',snapshot='',item='',details=false,signal,request=globalThis.fetch.bind(globalThis)}={}){
  if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint)||typeof query!=='string'||new TextEncoder().encode(query).length>256||!Number.isSafeInteger(cursor)||cursor<0)throw Error('Choose a current workspace and a bounded source search.');
  if(source&&!/^[a-z0-9._-]{1,95}$/.test(source)||snapshot&&!/^[a-f0-9]{64}$/.test(snapshot))throw Error('Choose a current source reader and snapshot.');
  if(item&&(!source||!/^[a-f0-9]{24}$/.test(item)||!snapshot||cursor))throw Error('Choose the exact public excerpt and its revision.');
  const address=source?endpoint.slice(0,-8)+'_retained_inputs?'+new URLSearchParams({source,cursor:String(cursor),...(snapshot?{snapshot}:{}),...(item?{item}:{})}):endpoint.slice(0,-8)+'_publication_inputs?'+new URLSearchParams({q:query,cursor:String(cursor),...(details?{details:'1'}:{})});
  const response=await request(address,{credentials:'same-origin',cache:'no-store',redirect:'error',signal});
  return publicationPacket(await readSourcePacket(response,512*1024));
}

// Re-read each selected page once at adoption. A saved selection is evidence of
// what was reviewed, not proof that the owner still offers those exact bytes.
export async function verifyPublicationSelection(endpoint,selection,{signal,readPage=readPublicationPage}={}){
 if(!Array.isArray(selection)||!selection.length||selection.length>50)throw Error('Choose one to 50 retained excerpts.');
 const reviewed=structuredClone(selection),pages=new Map(),seen=new Set();
 for(const {input,page}of reviewed){
  const identity=input?.sourceRef+'\0'+input?.id;
  if(!input||!page||seen.has(identity))throw Error('Review each selected excerpt once.');
  seen.add(identity);signal?.throwIfAborted();
  const coordinate={query:page.query??'',cursor:page.cursor??0,source:page.source??'',snapshot:page.snapshot??'',item:page.item??''},key=JSON.stringify(coordinate);
  if(!pages.has(key))pages.set(key,await readPage(endpoint,{...coordinate,signal}));
  signal?.throwIfAborted();
  const current=pages.get(key),matches=current.inputs.filter(item=>item.sourceRef===input.sourceRef&&item.id===input.id);
  if(coordinate.snapshot&&current.snapshot!==coordinate.snapshot||matches.length!==1||await researchDigest(matches[0])!==await researchDigest(input))
   throw Error('A selected excerpt or receipt changed or is no longer available. Reload its source and select the current passage.');
 }
 signal?.throwIfAborted();return reviewed.map(item=>item.input);
}

// Saved inputs have stable source/occurrence identities. Search only the same
// admitted reader, with bounded pagination; absence beyond the bound is unknown.
export async function inspectPublicationInput(endpoint,input,{signal,readPage=readPublicationPage}={}){
 if(!input?.sourceRef||!input?.id)throw Error('Choose a retained source before checking it.');
 const reviewed=structuredClone(input),retained=reviewed.receipt?.startsWith('public-material:'+reviewed.sourceRef+'/'+reviewed.id+';body-sha256:');
 let cursor=0,snapshot='';const cursors=new Set();
 for(let pageNumber=0;pageNumber<16;pageNumber++){
  signal?.throwIfAborted();if(cursors.has(cursor))throw Error('The source reader repeated a page. Its current state could not be verified.');cursors.add(cursor);
  const page=await readPage(endpoint,{source:retained?reviewed.sourceRef:'',query:'',cursor,snapshot,signal});signal?.throwIfAborted();
  if(retained&&(!page.snapshot||snapshot&&page.snapshot!==snapshot))throw Error('The source changed during this check. Check it again.');
  snapshot=page.snapshot??'';
  const matches=page.inputs.filter(item=>item.sourceRef===reviewed.sourceRef&&item.id===reviewed.id);
  if(matches.length>1)throw Error('The source reader returned an ambiguous identity.');
  if(matches.length){const same=await researchDigest(matches[0])===await researchDigest(reviewed);signal?.throwIfAborted();return same
   ?{state:'current',message:'The passage and receipt match the retained source at this check.'}
   :{state:'changed',message:'The retained source changed. Review its current passage before rendering again.'};}
  if(!page.cursor)return {state:'unavailable',message:'This passage is no longer available from its retained source. Review the source before rendering again.'};
  cursor=page.cursor;
 }
 throw Error('The source was not found within the bounded read. Its current state could not be verified.');
}

export async function readSourceReaders(endpoint,{query='',cursor=0,signal,request=globalThis.fetch.bind(globalThis)}={}){
 if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint)||typeof query!=='string'||new TextEncoder().encode(query).length>256||!Number.isSafeInteger(cursor)||cursor<0)throw Error('Choose an existing destination and a bounded feed search.');
 const r=await request(endpoint.slice(0,-8)+'_retained_inputs?'+new URLSearchParams({readers:'1',q:query,cursor:String(cursor)}),{credentials:'same-origin',cache:'no-store',redirect:'error',signal});
 const p=await readSourcePacket(r,64000);
 if(!Array.isArray(p)||p.length!==3||p[0]!==1||!Array.isArray(p[1])||p[1].length>24||!Number.isSafeInteger(p[2])||p[2]<0)throw Error('The public feed directory is incomplete.');
 for(const row of p[1])if(!Array.isArray(row)||row.length!==2||typeof row[0]!=='string'||!/^[a-z0-9._-]{1,95}$/.test(row[0])||typeof row[1]!=='string'||!row[1].trim()||row[1].length>300)throw Error('An invalid public feed was returned.');
 return {readers:p[1],cursor:p[2]};
}

// Selection is transient. The receiving instrument validates and owns the actual change.
export function mountPublicationPicker(container,{endpoint,onApply}){
  if(!container||typeof onApply!=='function')throw Error('Choose a receiving source editor.');
  const make=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
  const form=make('form'),label=make('label','Find retained material'),query=make('input'),search=make('button','Search'),status=make('p'),results=make('div'),basket=make('ol'),count=make('p'),more=make('button','Load more'),apply=make('button','Add selected excerpts');
  query.type='search';query.maxLength=256;query.setAttribute('aria-label','Find retained material');search.type='submit';label.append(query);form.append(label,search);status.setAttribute('role','status');status.setAttribute('aria-live','polite');results.dataset.publicationPickerResults='';basket.dataset.publicationPickerSelection='';more.hidden=true;more.type=apply.type='button';apply.className='primary';
  container.append(form,status,results,make('h3','Selected excerpts'),count,basket,more,make('p','Retained excerpts keep their source receipts. Selecting one does not grant reuse rights or create a research finding.'),apply);
  let disposed=false,busy=false,cursor=0,phrase='',controller=null,source='',generation='',readerCursor=0;const selected=new Map(),selectedPages=new Map();
  const feedForm=make('form'),feedQuery=make('input'),feedFind=make('button','Find public feeds'),feedLabel=make('label','Find a publisher'),feedChoice=make('select'),feedMore=make('button','More feeds');feedForm.className='publication-feed-choice';feedQuery.type='search';feedQuery.setAttribute('aria-label','Find a public feed');feedQuery.placeholder='Publisher or feed name';feedChoice.setAttribute('aria-label','Read from public feed');feedMore.type='button';feedMore.hidden=true;const original=make('option','Published occurrence records');original.value='';feedChoice.append(original);feedLabel.append(feedQuery);feedForm.append(feedLabel,feedFind,feedChoice,feedMore);container.prepend(feedForm);
  const identity=s=>s.sourceRef+'\0'+s.id;
  function controls(){for(const n of container.querySelectorAll('button,input,select'))n.disabled=busy||n.dataset.unavailable==='true';apply.disabled=busy||!selected.size;count.textContent=selected.size+' excerpts selected';}
  function selection(){for(const key of selectedPages.keys())if(!selected.has(key))selectedPages.delete(key);basket.replaceChildren();for(const [key,item]of selected){const li=make('li',item.title+' '),remove=make('button','Remove');remove.type='button';remove.onclick=()=>{selected.delete(key);for(const box of results.querySelectorAll('input[type=checkbox]'))if(box.dataset.identity===key)box.checked=false;selection();};li.append(remove);basket.append(li);}controls();}
  const run=fn=>async e=>{e?.preventDefault();if(disposed||busy)return;busy=true;controls();try{await fn();}catch(error){if(!disposed)status.textContent=error.message;}finally{busy=false;if(!disposed)controls();}};
  async function load(next=false){controller?.abort();controller=new AbortController();const timeout=setTimeout(()=>controller?.abort(),15000);
    try{if(!next){phrase=query.value;cursor=0;generation='';}const coordinate={query:phrase,cursor,source,snapshot:generation},page=await readPublicationPage(endpoint,{...coordinate,signal:controller.signal});if(disposed)return;generation=page.snapshot||'';coordinate.snapshot=generation;
      if(!next)results.replaceChildren();cursor=page.cursor;more.hidden=!cursor;let corrected=false;
      for(const input of page.inputs){if(source&&phrase&&!(input.title+' '+input.body).toLowerCase().includes(phrase.toLowerCase()))continue;const prior=selected.get(identity(input));if(prior&&JSON.stringify(prior)!==JSON.stringify(input)){selected.delete(identity(input));corrected=true;}const row=make('article'),heading=make('label'),box=make('input');box.type='checkbox';box.dataset.identity=identity(input);box.setAttribute('aria-label','Select '+input.title);box.checked=selected.has(identity(input));box.dataset.unavailable=String(!input.body.trim());
        box.onchange=()=>{if(box.checked){if(selected.size>=50){box.checked=false;status.textContent='Choose at most 50 excerpts.';return;}selected.set(identity(input),input);selectedPages.set(identity(input),coordinate);}else selected.delete(identity(input));selection();};heading.append(box,make('strong',input.title));
        const details=make('details'),summary=make('summary','Read retained excerpt and source terms'),link=make('a','Original source ↗');link.href=input.url;link.target='_blank';link.rel='noopener noreferrer';details.append(summary,make('blockquote',input.body||'No excerpt retained.'),make('p',input.rightsBasis),make('small',input.retention+' · '+(input.publishedAt||'Date not supplied')),link);row.append(heading,make('p',input.body.slice(0,240)),details);results.append(row);
      }
      selection();status.textContent=corrected?'A selected excerpt or receipt changed. Review and select its current version again.':page.inputs.length?'Choose the excerpts to add to this working review.':'No retained excerpts match this search.';
    }finally{clearTimeout(timeout);}
  }
  form.onsubmit=run(()=>load());more.onclick=run(()=>load(true));apply.onclick=run(async()=>{
    controller?.abort();const active=new AbortController();controller=active;const timeout=setTimeout(()=>active.abort(),15000);
    try{const inputs=await verifyPublicationSelection(endpoint,[...selected].map(([key,input])=>({input,page:selectedPages.get(key)})),{signal:active.signal});
      if(disposed)return;await onApply(inputs);
    }finally{clearTimeout(timeout);}
  });
  async function feeds(morePages=false){
   const current=feedChoice.value,p=await readSourceReaders(endpoint,{query:feedQuery.value.trim(),cursor:morePages?readerCursor:0,signal:AbortSignal.timeout(15000)});if(disposed)return;
   if(!morePages){const selectedOption=[...feedChoice.options].find(o=>o.value===current);feedChoice.replaceChildren(original);if(current&&selectedOption)feedChoice.append(selectedOption);}
   for(const[id,title]of p.readers)if(![...feedChoice.options].some(o=>o.value===id)){const o=make('option',title);o.value=id;feedChoice.append(o);}feedChoice.value=current;readerCursor=p.cursor;feedMore.hidden=!readerCursor;status.textContent=p.readers.length+' public feeds found. Choose a feed to read its passages.';
  }
  feedForm.onsubmit=run(()=>feeds());feedMore.onclick=run(()=>feeds(true));feedChoice.onchange=run(async()=>{source=feedChoice.value;generation='';await load();});
  selection();void run(async()=>{await load();if(![...results.querySelectorAll('input[type=checkbox]')].some(n=>n.dataset.unavailable==='false'))await feeds();})();
  return {dispose(){disposed=true;controller?.abort();container.replaceChildren();}};
}
