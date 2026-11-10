import {openDestinationStore} from '../bridge/workspace-store.mjs';
import {workspaceElement as el,workspaceButton as button,workspaceLink as link,mountCollection,mountTextSearch} from '../bridge/interaction-collection.mjs';
import {readProductionBindings} from '../bridge/workspace-destinations.mjs';
import {researchDigest,savePreparedResearch} from '../bridge/publication-research.mjs';
import {prepareFile,prepareLink,reviseSource,checkSource,sourcePrefix,prepareMaterialResearch,readSourceText,feedSourceContinuations} from './source-material.mjs';
const field=(text,node)=>{const n=el('label',text);n.append(node);return n;};
const input=(name,value='',type='text')=>{const n=el('input');n.type=type;n.name=name;n.value=value;n.setAttribute('aria-label',name);return n;};
const assert=(ok,text)=>{if(!ok)throw Error(text);};
const bytes=n=>n<1024?n+' B':n<1048576?(n/1024).toFixed(1)+' KiB':(n/1048576).toFixed(1)+' MiB';
const endpoint=document.querySelector('meta[name=bonfyre-workspace-store]')?.content;
const context=document.querySelector('meta[name=bonfyre-workspace-context-selector]')?.content||'self';
const binding=endpoint?.split('/')[2],root=document.querySelector('[data-source-library]'),lifetime=new AbortController(),dialogs=new Set();
const live=()=>lifetime.signal.throwIfAborted(),openStore=address=>openDestinationStore(address,{signal:lifetime.signal,request:async(...args)=>{const response=await fetch(...args);if(response.status===403){void response.body?.cancel().catch(()=>{});suspend();}return response;}}),store=openStore(endpoint);
let busy=false,rows=[],selected=[],cursor='',epoch=-1,hasMore=false,view='active',collection,activeSource='',readingMode='catalogue';
const status=el('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
function say(text){if(!lifetime.signal.aborted)status.textContent=text;}
function lock(value){if(lifetime.signal.aborted)return;busy=value;root.dataset.busy=String(value);for(const n of root.querySelectorAll('[data-library-action]'))n.disabled=value;collection?.controller.lock(value);}
const guarded=fn=>async()=>{if(busy||lifetime.signal.aborted)return;lock(true);try{await fn();}catch(e){if(e.name!=='AbortError')say(e.message);}finally{lock(false);}};
function suspend(){if(lifetime.signal.aborted)return;lifetime.abort();collection?.destroy();rows=[];selected=[];for(const d of dialogs){for(const n of d.querySelectorAll('input,textarea'))n.value='';d.replaceChildren();d.remove();}dialogs.clear();root.dataset.ready='false';root.dataset.closed='true';root.replaceChildren(el('h1','Reopen your source library'),el('p','The account or page context changed. Retained text, previews and pending reviews were cleared.'),link('Read current access and reopen',location.pathname+location.search));}
addEventListener('bonfyre:account-changed',suspend,{signal:lifetime.signal});addEventListener('pagehide',suspend,{signal:lifetime.signal});addEventListener('pageshow',event=>{if(event.persisted&&lifetime.signal.aborted)location.reload();});
function modal(title){live();const d=el('dialog'),h=el('h2',title),message=el('p'),body=el('section'),footer=el('footer');d.className='source-dialog';d.dataset.review='';message.setAttribute('role','status');d.append(h,message,body,footer);document.body.append(d);dialogs.add(d);d.showModal();let held=false;const close=button('Close',()=>{if(!held)d.close();});footer.append(close);d.addEventListener('cancel',e=>{if(held)e.preventDefault();});d.addEventListener('close',()=>{dialogs.delete(d);d.remove();},{once:true});return {d,body,footer,message,uncertain:false,conflicted:false,hold(value){held=value;body.inert=value;close.disabled=value;d.dataset.busy=String(value);},close};}
async function metadataEqual(a,b){return await researchDigest({...a,original:null})===await researchDigest({...b,original:null});}
async function write(changes,dialog,target=store,committed=refresh,attempt=()=>target.saveLocalBatch(changes)){
 live();
 assert(!dialog.uncertain&&!dialog.conflicted,'Check the previous save before sending another write.');
 dialog.hold(true);dialog.message.textContent='Saving the reviewed changes\u2026';
 async function inspectOutcome(){
  const kept=await Promise.all(changes.map(c=>target.readVersion(c.key).catch(()=>undefined)));live();
  if(kept.some(r=>r===undefined))return 'unavailable';
  if(kept.every((r,i)=>r&&!r.deleted&&r.revision===changes[i].expectedRevision+1)&&await Promise.all(kept.map((r,i)=>metadataEqual(r.value,changes[i].value))).then(matches=>matches.every(Boolean)))return 'committed';
  if(kept.every((r,i)=>(r?.revision??0)===changes[i].expectedRevision&&!r?.deleted))return 'unchanged';
  return 'conflict';
 }
 try{return await attempt();}
 catch(error){
  const outcome=await inspectOutcome();
  if(outcome==='committed')return [];
  if(outcome==='unchanged'){dialog.hold(false);throw error;}
  if(outcome==='conflict'){dialog.conflicted=true;dialog.hold(false);throw Error('A reviewed record changed in another editor. Close this review and reload before preparing the change again.');}
  dialog.uncertain=true;dialog.d.dataset.state='uncertain';
  const check=button(target===store?'Check saved sources':'Check saved research',async()=>{check.disabled=true;try{
   const state=await inspectOutcome();
   if(state==='committed'){dialog.uncertain=false;dialog.hold(false);dialog.d.dataset.state='saved';dialog.message.textContent='The reviewed changes are saved. No additional write was sent.';await committed();check.remove();}
   else if(state==='unchanged'){dialog.uncertain=false;dialog.hold(false);dialog.conflicted=true;dialog.message.textContent='No reviewed record changed. Close and prepare the addition again before saving.';check.remove();}
   else if(state==='conflict'){dialog.uncertain=false;dialog.conflicted=true;dialog.hold(false);dialog.message.textContent='Current revisions differ from this review. Close and reload; nothing was overwritten.';check.remove();}
   else dialog.message.textContent='Readback is still unavailable. No additional write was sent.';
  }catch(error){if(!lifetime.signal.aborted)dialog.message.textContent=error.message;}finally{check.disabled=false;}});dialog.footer.append(check);
  throw Error('The save reply is uncertain. Check the retained sources before continuing. No write will be repeated automatically.');
 }
}

function shown(){return rows.filter(r=>view==='all'||r.value.state===view).map(r=>({id:r.key,version:String(r.revision),title:r.value.title,subtitle:r.value.kind==='file'?(r.value.filename+' \u00B7 '+bytes(r.value.bytes)):r.value.url,category:r.value.collection||'Unfiled',status:r.value.state,search:[...(r.value.tags||[]),r.value.notes||''].join(' '),record:r}));}
function paint(){collection.controller.lock(false);collection.refresh(shown());collection.controller.lock(busy);more.hidden=!hasMore;if(activeSource&&!rows.some(row=>row.key===activeSource)){activeSource='';readerFocus.disabled=true;setReadingMode('catalogue');}}
async function refresh(append=false){
 const p=await store.readMetadata(sourcePrefix,{cursor:append?cursor:'',epoch:append?epoch:-1});live();
 const merged=append?new Map(rows.map(r=>[r.key,r])):new Map();for(const r of p.rows){assert(r.value?.schema==='bonfyre.source-material.v1','The source library contains an unsupported record.');merged.set(r.key,r);}
 assert(merged.size<=1000,'Use another collection before loading more than 1,000 materials.');rows=[...merged.values()];cursor=p.cursor;epoch=p.epoch;hasMore=p.more;paint();say(rows.length+' source records loaded. Original files are fetched only when opened.');root.dataset.ready='true';
}
async function exact(row){const r=await store.readVersion(row.key);live();assert(r&&!r.deleted&&r.revision===row.revision,'This source changed. Reload it before applying an edit.');checkSource(r.value);return r;}
function mountSourceCitation(text,version,preview,{signal}){
 const panel=el('section'),status=el('p'),body=el('textarea'),prepare=button('Prepare source citation',async()=>{
  try{
   const quote=selectedQuote();
   assert(quote.length>0,'Select a passage inside this original source first.');
   assert(quote.length<=2000,'Select no more than 2,000 characters for one citation.');
   const current=await exact(version);signal.throwIfAborted();
   assert(text.isConnected,'The original source is no longer open.');
   assert(/^[a-f0-9]{64}$/.test(current.value.contentSHA256),'Original source digest is unavailable.');
   assert(current.value.kind==='file'&&current.value.textLike,'Only retained original text can be quoted.');
   body.value=['Source: '+current.value.title,'Account-local source: '+current.key,
      'Original SHA-256: '+current.value.contentSHA256,'Recorded revision: '+current.revision,
      'Exact selected passage:',quote,
      'A quotation from retained original text; not independently verified or published.'].join('\n');
   panel.hidden=false;status.textContent='Review the exact quotation and retained source identity before copying.';
  }catch(error){body.value='';panel.hidden=true;status.textContent=error.message;}
 });
 let cached='';
 const inSource=range=>text.contains(range.startContainer)&&text.contains(range.endContainer);
 const selectedQuote=()=>{
  const selection=window.getSelection();
  if(selection?.rangeCount&&inSource(selection.getRangeAt(0)))return selection.toString().trim();
  return cached;
 };
 document.addEventListener('selectionchange',()=>{
  const selection=window.getSelection();
  if(selection?.rangeCount&&inSource(selection.getRangeAt(0)))cached=selection.toString().trim();
 },{signal});
 body.className='source-citation-text';body.readOnly=true;body.rows=10;body.setAttribute('aria-label','Prepared source citation');
 status.setAttribute('role','status');status.setAttribute('aria-live','polite');status.textContent='Select an exact passage in the opened original.';
 const copy=button('Copy reviewed citation',async()=>{
  try{assert(body.value,'Prepare a citation before copying.');await navigator.clipboard.writeText(body.value);status.textContent='Reviewed citation copied. Nothing was saved or published.';}
  catch{body.focus();body.select();status.textContent='Clipboard not available. The reviewed citation is selected for manual copy.';}
 });
 panel.className='source-citation-review';panel.hidden=true;
 panel.append(el('h4','Exact source citation'),el('p','This uses a selection from retained original bytes, not an AI summary or a publisher assertion.'),body,copy);
 preview.append(prepare,status,panel);
}
async function inspect(record,host,{signal}){
 const row=record.record,v=row.value,preview=el('section'),actions=el('div');actions.className='source-actions';activeSource=record.id;readerFocus.disabled=false;readerHint.textContent='Reading '+v.title+'. Focus mode keeps your current source and native inspector controls.';
 host.append(el('p',(v.tags||[]).join(' \u00B7 ')),el('p',v.notes||'No notes yet.'),preview,actions);
 const fromFeed=feedSourceContinuations(v);
 if(fromFeed.length){const provenance=el('aside');provenance.className='source-feed-provenance';provenance.append(el('h4',fromFeed.length+' retained feed '+(fromFeed.length===1?'item':'items')));for(const entry of fromFeed){const item=el('p'),label=el('span',entry.family+' · '+entry.kind+' · '+entry.source);item.dataset.item=entry.itemRef;item.append(label,link('Open retained item',entry.path));provenance.append(item);}host.append(provenance);}
 else if(v.provenance?.schema==='bonfyre.feed-handoff.v1')host.append(el('p','The stored feed coordinate could not be verified. Review the original source address.'));
 const details=el('details');details.append(el('summary','Original and revision'),el('p','Revision '+row.revision),el('code',v.contentSHA256));host.append(details);
 for(const h of [...(v.history||[])].reverse())details.append(el('p',h.action+' \u00B7 '+h.at));
 if(v.kind==='link'){const a=link('Open original address \u2197',v.url);a.target='_blank';a.rel='noopener noreferrer';preview.append(a,link('Review and read this address in Aurekai','/studio/?'+new URLSearchParams({foreign_url:v.url})));}
 else{if(v.preview){const p=el('pre',v.preview);p.className='source-preview-text';preview.append(p,el('small','Preview of the beginning. Original bytes remain unchanged.'));}
  const load=button('Open original file',guarded(async()=>{const full=await exact(row);signal.throwIfAborted();const original=full.value.original,url=URL.createObjectURL(new Blob([original],{type:full.value.media}));const download=link('Download '+v.filename,url);download.download=v.filename;preview.append(download);let media;
   if(['image/png','image/jpeg'].includes(v.media)){media=el('img');media.alt=v.title;}
   else if(v.media.startsWith('audio/')){media=el('audio');media.controls=true;media.preload='none';}
   else if(v.media.startsWith('video/')){media=el('video');media.controls=true;media.preload='none';}
   if(media){media.src=url;preview.append(media);}load.disabled=true;releases.push(()=>URL.revokeObjectURL(url));
   if(v.textLike){try{const body=await readSourceText(full,{readOnly:true});signal.throwIfAborted();const text=el('pre',body);text.className='source-original-text';preview.replaceChildren(el('p','Complete original text · '+bytes(original.size)),text,download);mountTextSearch(text,{signal});mountSourceCitation(text,full,preview,{signal});}catch(error){if(signal.aborted)return;preview.append(el('p',error.message));}}
  }));preview.append(load);
 }
 const releases=[];actions.append(button('Edit details',()=>edit(row)),button(v.state==='active'?'Archive source':'Restore source',()=>changeState([row],v.state==='active'?'archive':'restore')));
 return ()=>releases.forEach(fn=>fn());
}
async function edit(row){const d=modal('Edit source details'),v=row.value,title=input('Source title',v.title),folder=input('Collection',v.collection),tags=input('Tags',v.tags.join(', ')),notes=el('textarea');notes.value=v.notes;notes.maxLength=8000;notes.setAttribute('aria-label','Source notes');title.maxLength=500;folder.maxLength=120;
 d.body.append(field('Title',title),field('Collection',folder),field('Tags, separated by commas',tags),field('Notes',notes),el('p','This changes the description, not the original content or any research already made from it.'));
 const save=button('Save source details',async()=>{save.disabled=true;try{const current=await exact(row),change=reviseSource(current,{title:title.value,collection:folder.value,tags:tags.value,notes:notes.value});await write([change],d);d.hold(false);d.d.close();await refresh();}catch(e){d.message.textContent=e.message;save.disabled=d.uncertain||d.conflicted;}});d.footer.prepend(save);
}
async function changeState(chosen,action){if(!chosen.length)return say('Select at least one source.');const d=modal(action==='archive'?'Archive selected sources':'Restore selected sources');d.body.append(el('p',chosen.length+' source records will change state. Their original files and downstream research are retained.'));
 const save=button(action==='archive'?'Archive sources':'Restore sources',async()=>{save.disabled=true;try{const current=[];for(const row of chosen)current.push(await exact(row));await write(current.map(row=>reviseSource(row,{},action)),d);d.hold(false);d.d.close();await refresh();}catch(e){d.message.textContent=e.message;save.disabled=d.uncertain||d.conflicted;}});d.footer.prepend(save);
}
async function intake(filesOrURLs){
 if(lifetime.signal.aborted)return;const d=modal('Review source intake'),folder=input('Collection for new sources',''),tags=input('Tags for new sources',''),list=el('div');list.className='source-intake-list';d.body.append(field('Collection',folder),field('Tags',tags),list);
 d.message.textContent='Preparing original identities and previews\u2026';d.hold(true);const candidates=[],chosen=new Set();
 try{assert(filesOrURLs.length>0&&filesOrURLs.length<=24,'Choose between one and 24 sources per reviewed intake.');assert(filesOrURLs.reduce((s,f)=>s+(typeof f==='string'?0:f.size),0)<=64*1024*1024,'Keep this intake within 64 MiB total.');
  for(const value of filesOrURLs){const c=typeof value==='string'?await prepareLink(value):await prepareFile(value);const existing=await store.readMetadata(sourcePrefix+c.value.id);const duplicate=existing.rows.some(r=>r.key===c.key)||candidates.some(x=>x.key===c.key);const line=el('label'),pick=input('Keep '+c.value.title,'','checkbox');pick.checked=!duplicate;pick.disabled=duplicate;
   const name=input('Title for '+c.value.title,c.value.title);name.maxLength=500;name.disabled=duplicate;name.oninput=()=>{c.value.title=name.value;};line.append(pick,name,el('small',duplicate?'Already retained':c.value.kind==='file'?bytes(c.value.bytes)+' \u00B7 '+c.value.media:'Public address, not fetched'));list.append(line);if(!duplicate){chosen.add(c.key);candidates.push(c);}pick.onchange=()=>{pick.checked?chosen.add(c.key):chosen.delete(c.key);};
  }
  d.message.textContent=candidates.length+' new sources are ready for review.';
 }catch(e){d.message.textContent=e.message;d.hold(false);return;}d.hold(false);
 const save=button('Save selected sources',async()=>{save.disabled=true;try{const changes=candidates.filter(c=>chosen.has(c.key));assert(changes.length,'Choose at least one new source.');for(const c of changes){c.value.collection=folder.value;c.value.tags=tags.value.split(',').map(x=>x.trim()).filter(Boolean);checkSource(c.value);}await write(changes,d);d.hold(false);d.d.close();await refresh();}catch(e){d.message.textContent=e.message;save.disabled=d.uncertain||d.conflicted;}});d.footer.prepend(save);
}
function addresses(){const d=modal('Bring public addresses'),text=el('textarea');text.rows=9;text.setAttribute('aria-label','Public addresses, one per line');d.body.append(field('One complete address per line',text));d.footer.prepend(button('Review addresses',async()=>{try{const urls=text.value.split(/\r?\n/).map(x=>x.trim()).filter(Boolean);assert(urls.length,'Enter at least one address.');d.d.close();await intake(urls);}catch(e){d.message.textContent=e.message;}}));}
async function research(){
 const chosen=[...selected];assert(chosen.length&&chosen.every(r=>r.value.kind==='file'&&r.value.textLike&&r.value.state==='active'),'Select active text sources to prepare research.');
 const d=modal('Use original text in research'),workspaces=el('select'),receiving=el('select'),title=input('New research title','Research from source library');workspaces.setAttribute('aria-label','Research destination');receiving.setAttribute('aria-label','Receiving research review');d.body.append(field('Installed research workspace',workspaces),field('Add to',receiving),field('New review title',title));
 let destination=null,module=null,prepared=null,currentRows=[];d.hold(true);
 async function choose(){destination=null;prepared=null;receiving.replaceChildren();const id=workspaces.value;assert(/^ws-[a-f0-9]{40}$/.test(id),'Choose an installed research workspace.');destination=openStore('/apps/'+id+'/_records');module=await import('/apps/'+id+'/instruments/research.mjs');live();const first=el('option','Create a new review');first.value='';receiving.append(first);const saved=await destination.readMetadata('draft:instruments:research:');assert(!saved.more,'Choose the receiving review from its editor; this destination has more than one page of reviews.');for(const row of saved.rows){const o=el('option',row.value.title+' \u00B7 revision '+row.revision);o.value=row.key;receiving.append(o);}}
 try{const bindings=await readProductionBindings(context,{signal:lifetime.signal});for(const row of bindings)if(row.ready){const o=el('option',row.label);o.value=row.id;workspaces.append(o);}assert(workspaces.options.length,'Enable a research workspace for this destination first.');await choose();d.message.textContent='Choose an existing review or name a new one. No findings will be generated.';}catch(e){if(e.status===403)suspend();d.message.textContent=e.message;d.body.append(link('Review research setup','/packs/setup?'+new URLSearchParams({pack:'experience.connected-instruments.workspace',context})));d.hold(false);return;}d.hold(false);
 workspaces.onchange=async()=>{d.hold(true);try{await choose();}catch(e){d.message.textContent=e.message;}finally{d.hold(false);}};
 const summary=el('section');
 function researchSaved(result){d.uncertain=false;d.hold(false);d.d.dataset.state='saved';d.message.textContent='Research saved at revision '+result.revision+'.';summary.replaceChildren(link('Open the saved research review','/apps/'+workspaces.value+'/instruments/?'+new URLSearchParams({tool:'research',record:result.key})));review.disabled=save.disabled=true;}
 const researchCommitted=()=>researchSaved({key:prepared.change.key,revision:prepared.change.expectedRevision+1});
 const save=button('Save reviewed research',async()=>{if(d.uncertain||d.conflicted||lifetime.signal.aborted||!prepared)return;save.disabled=review.disabled=true;d.hold(true);let attempted=false;
  try{for(const row of currentRows)await exact(row);attempted=true;const result=await write([prepared.change],d,destination,researchCommitted,()=>savePreparedResearch(destination,prepared));if(result?.key)researchSaved(result);else researchCommitted();}
  catch(error){if(!attempted)d.conflicted=true;d.message.textContent=error.message;d.hold(d.uncertain);save.disabled=review.disabled=d.uncertain||d.conflicted;}
 });save.disabled=true;
 const review=button('Review research addition',async()=>{review.disabled=true;d.hold(true);try{currentRows=[];for(const row of chosen)currentRows.push(await exact(row));const existing=receiving.value?await destination.readVersion(receiving.value):null;prepared=await prepareMaterialResearch({rows:currentRows,title:existing?.value.title||title.value,existing,sourceRecord:module.sourceRecord,digest:researchDigest,binding});summary.textContent=prepared.added.length+' exact originals will be added; '+prepared.unchanged.length+' already match. Existing findings stay unchanged.';save.disabled=false;for(const c of [workspaces,receiving,title])c.disabled=true;}catch(e){d.message.textContent=e.message;review.disabled=false;}finally{d.hold(false);}});d.body.append(summary);d.footer.prepend(review,save);
}
const header=el('header'),heading=el('div');heading.append(el('p','YOUR SOURCES'),el('h1','Keep the original. Continue the work.'),el('p','Files and web addresses, organized for the next job.'));header.append(heading,link('Browse the known internet sources','/link'),link('Accounts and connections','/account'));
const toolbar=el('nav'),importFiles=button('Import files',()=>picker.click()),add=button('Add web addresses',addresses),handoff=button('Prepare research',()=>research().catch(e=>say(e.message))),archive=button('Archive selected',()=>changeState(selected,'archive')),restore=button('Restore selected',()=>changeState(selected,'restore')),reload=button('Reload',guarded(()=>refresh()));
for(const n of [importFiles,add,handoff,archive,restore,reload])n.dataset.libraryAction='';toolbar.setAttribute('aria-label','Source actions');toolbar.append(importFiles,add,handoff,archive,restore,reload);
const picker=el('input');picker.type='file';picker.multiple=true;picker.hidden=true;picker.onchange=()=>{const files=[...picker.files];picker.value='';void intake(files);};
const drop=el('section','Drop original files here, or choose Import files. Review before saving.');drop.className='source-drop';drop.tabIndex=0;drop.setAttribute('role','button');drop.setAttribute('aria-label','Import original files');drop.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();picker.click();}};drop.onclick=()=>picker.click();drop.ondragover=e=>{e.preventDefault();drop.dataset.dragging='true';};drop.ondragleave=()=>delete drop.dataset.dragging;drop.ondrop=e=>{e.preventDefault();delete drop.dataset.dragging;if(!busy)void intake([...e.dataTransfer.files]);};
const filter=el('select');filter.setAttribute('aria-label','Source state');for(const value of ['active','archived','all']){const o=el('option',value[0].toUpperCase()+value.slice(1));o.value=value;filter.append(o);}filter.onchange=()=>{view=filter.value;paint();};
const readerControls=el('nav');readerControls.className='source-reader-toolbar';readerControls.setAttribute('aria-label','Reading room layout and typography');
const readerCatalogue=button('Catalogue and inspector',()=>setReadingMode('catalogue'));
const readerFocus=button('Focus selected source',()=>setReadingMode('focus'));readerFocus.disabled=true;
const readerSize=el('select');readerSize.setAttribute('aria-label','Reading text size');
for(const [value,label] of [['standard','Standard text'],['comfortable','Comfortable text'],['large','Large text']]){const option=el('option',label);option.value=value;readerSize.append(option);}
readerSize.onchange=()=>{root.dataset.readerSize=readerSize.value;};
const readerFont=el('select');readerFont.setAttribute('aria-label','Reading font');
for(const [value,label] of [['literata','Literata / serif'],['sans','Sans / system']]){const option=el('option',label);option.value=value;readerFont.append(option);}
readerFont.onchange=()=>{root.dataset.readerFont=readerFont.value;};
const readerGround=el('select');readerGround.setAttribute('aria-label','Reading ground');
for(const [value,label] of [['paper','Paper'],['obsidian','Pure obsidian'],['twilight','Warm twilight'],['sepia','Deep sepia']]){const option=el('option',label);option.value=value;readerGround.append(option);}
readerGround.onchange=()=>{root.dataset.readerGround=readerGround.value;};
const readerDim=button('Dim controls',()=>{const dim=root.dataset.readerDim!=='true';root.dataset.readerDim=String(dim);readerDim.setAttribute('aria-pressed',String(dim));readerDim.textContent=dim?'Restore controls':'Dim controls';});
readerDim.setAttribute('aria-pressed','false');readerDim.disabled=true;readerDim.dataset.readerDimControl='';
const readerProgress=el('progress');readerProgress.max=100;readerProgress.value=0;readerProgress.setAttribute('aria-label','Reading progress on this page');readerProgress.hidden=true;
function updateReadingProgress(){if(lifetime.signal.aborted||readingMode!=='focus')return;const max=Math.max(0,document.documentElement.scrollHeight-innerHeight);readerProgress.value=max?Math.min(100,Math.max(0,Math.round(scrollY/max*100))):100;}
addEventListener('scroll',updateReadingProgress,{signal:lifetime.signal,passive:true});
addEventListener('resize',updateReadingProgress,{signal:lifetime.signal});
document.addEventListener('keydown',event=>{if(event.key!=='Escape'||readingMode!=='focus'||document.querySelector('dialog[open]'))return;const active=document.activeElement;if(active?.matches?.('input,select,textarea,[contenteditable=true]'))return;setReadingMode('catalogue');readerCatalogue.focus();},{signal:lifetime.signal});
const readerHint=el('p','Choose a source to enable focus reading. All original records and edits stay in the current account.');readerHint.className='source-reader-hint';
readerControls.append(el('strong','Reading room'),readerCatalogue,readerFocus,field('Text',readerSize),readerHint);
readerControls.append(field('Font',readerFont),field('Ground',readerGround),readerDim,readerProgress);
function setReadingMode(next){
 if(next!=='catalogue'&&next!=='focus')throw Error('Unknown source reading mode.');
 if(next==='focus'&&(!activeSource||!shown().some(row=>row.id===activeSource)))return;
 readingMode=next;root.dataset.readingMode=next;readerDim.disabled=next!=='focus';readerProgress.hidden=next!=='focus';if(next!=='focus'){root.dataset.readerDim='false';readerDim.setAttribute('aria-pressed','false');readerDim.textContent='Dim controls';}else requestAnimationFrame(updateReadingProgress);
 readerCatalogue.setAttribute('aria-pressed',String(next==='catalogue'));
 readerFocus.setAttribute('aria-pressed',String(next==='focus'));
 readerHint.textContent=next==='focus'?'Showing the selected source and its native reading actions. Return to catalogue for other sources.':'Choose a source to read, inspect, and continue work.';
}
setReadingMode('catalogue');root.dataset.readerSize='standard';root.dataset.readerFont='literata';root.dataset.readerGround='paper';root.dataset.readerDim='false';
const work=el('div'),more=button('Load more source records',guarded(()=>refresh(true)));more.hidden=true;more.dataset.libraryAction='';
work.addEventListener('click',event=>{if(event.target.closest?.('[data-inspect-back]'))setReadingMode('catalogue');});
root.append(header,toolbar,picker,drop,status,field('Show',filter),readerControls,work,more);
collection=mountCollection(work,{rows:[],label:'Source library',maxSelected:24,pageSize:30,onSelection:values=>{selected=values.map(v=>v.record);},renderInspector:inspect});
void guarded(async()=>{
 await refresh();const key=new URL(location.href).searchParams.get('record');if(!key)return;
 assert(/^source-material:[a-f0-9]{64}$/.test(key),'This is not a source-library continuation.');
 if(!rows.some(r=>r.key===key)){const p=await store.readMetadata(key);const found=p.rows.find(r=>r.key===key);if(found)rows.push(found);}
 const found=rows.find(r=>r.key===key);assert(found,'This saved source is no longer available.');view='all';filter.value='all';paint();collection.inspect(key);
})();
