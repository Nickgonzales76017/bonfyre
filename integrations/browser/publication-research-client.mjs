import {openDestinationStore} from './workspace-store.mjs';
import {workspaceElement as el,mountCollection,mountTabs,mountCommandPalette} from './interaction-collection.mjs';
import {encode} from './workspace-msgpack.mjs';
import {readPublicationPage,readSourceReaders} from './publication-picker.mjs';
import {prepareResearchInputs,savePreparedResearch} from './publication-research.mjs';
import {selectedPublicationSource,openSourcePublication,publicationRecoveryId} from './source-publication.mjs';
import {sourceRecord} from '../instruments/research.mjs';
const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]').content,base=endpoint.slice(0,-8),store=openDestinationStore(endpoint);
const root=document.querySelector('[data-source-review]'),selected=new Map();let busy=false,preview=null,uncertain=false,cursor=0,query='',reviews=[],retained='',generation='',readerCursor=0,readersLoaded=false;
let collection=null,updatingCollection=false,loadedRows=new Map(),chosenRows=new Map();
root.innerHTML=`<header class="intake-heading"><p class="eyebrow">Sources → authored research</p><h1>Build a review from retained material.</h1><p>Choose the passages you want to work with. Their retained text and source references arrive in the research editor, ready for your findings.</p></header>
<div class="intake-layout"><main><form id="publication-search"><label>Find published material<input name="q" type="search" placeholder="A subject, place or title"></label><button>Search</button></form><p id="intake-status" role="status" aria-live="polite"></p><div id="publication-results"></div><button id="publication-more" hidden>Load more</button></main>
<aside><h2>Your selection</h2><p id="selection-count">No excerpts selected</p><ol id="selected-inputs"></ol><label>Receiving review<select id="receiving-review"><option value="">Create a new review</option></select></label><label>New review title<input id="review-title" maxlength="500" placeholder="What do you want to understand?"></label><p class="fine">Only retained excerpts are available here. Source selection does not grant reuse rights or turn a publication into an independently verified finding.</p><button id="prepare-research">Review preparation</button><section id="preparation" hidden><h3>Ready for the research editor</h3><div id="preparation-summary"></div><button id="save-research">Save and open research</button><button class="secondary" id="cancel-preparation">Revise selection</button></section></aside></div>`;
const $=id=>document.getElementById(id),identity=s=>s.sourceRef+'\0'+s.id;
const publishing=el('section');publishing.className='source-publication-start';publishing.append(el('h3','Publish a source World'),el('p','Choose excerpts from one observed source to review a public source collection and its World. Your research remains private.'));
const publishSource=el('button','Review source publication');publishSource.id='publish-source-world';publishSource.className='secondary';publishSource.dataset.unavailable='true';publishing.append(publishSource);root.querySelector('aside').append(publishing);
publishSource.onclick=()=>{if(busy||uncertain)return;const source=selectedPublicationSource([...selected.values()]);if(source)void openSourcePublication(source).catch(e=>message(e.message));};
function message(text){$('intake-status').textContent=text;}
function controls(){publishSource.dataset.unavailable=String(!selectedPublicationSource([...selected.values()]));root.dataset.busy=String(busy);root.setAttribute('aria-busy',String(busy));root.querySelectorAll('button,input,select').forEach(n=>{if(!n.closest('.ac-collection'))n.disabled=busy||(uncertain&&n.id!=='save-research')||n.dataset.unavailable==='true';});$('review-title').disabled=busy||uncertain||Boolean($('receiving-review').value);$('prepare-research').disabled=busy||uncertain||!selected.size;if(!updatingCollection)collection?.controller.lock(busy||uncertain);}
function invalidate(){preview=null;$('preparation').hidden=true;}
const guard=work=>async event=>{event?.preventDefault();if(busy)return;busy=true;controls();try{await work(event);}catch(e){message(e.message);}finally{busy=false;controls();}};
function basket(){
 $('selection-count').textContent=selected.size?`${selected.size} excerpts selected`:'No excerpts selected';$('selected-inputs').replaceChildren();
 for(const [key,item]of selected){const row=el('li'),remove=el('button','Remove');remove.className='secondary';remove.onclick=()=>{if(busy||uncertain)return;collection.controller.toggle(key,{checked:false});};row.append(el('span',item.title),remove);$('selected-inputs').append(row);}
 controls();
}
async function loadReviews(){
 reviews=await store.readLocal('draft:instruments:research:');
 for(const row of reviews){if(row.value?.kind!=='research')continue;const option=el('option',row.value.title);option.value=row.key;$('receiving-review').append(option);}
}
function syncSelection(rows){
 if(updatingCollection)return;const unchanged=rows.length===selected.size&&rows.every(r=>selected.get(r.id)===r.input);if(unchanged)return;
 const replacing=[...chosenRows].some(([id,row])=>!rows.some(r=>r.id===id)&&loadedRows.get(id)?.version!==row.version);
 selected.clear();chosenRows=new Map(rows.map(row=>[row.id,row]));for(const row of rows)selected.set(row.id,row.input);invalidate();basket();if(replacing)queueMicrotask(()=>refreshCollection());
}
function refreshCollection({sourceFilter=false}={}){
 if(!collection)return;const rows=new Map(loadedRows);for(const[id,row]of chosenRows){const changed=loadedRows.has(id)&&loadedRows.get(id).version!==row.version;rows.set(id,{...row,changed,status:changed?'Selected earlier excerpt':row.status});}
 updatingCollection=true;try{collection.controller.lock(false);collection.refresh([...rows.values()]);if(sourceFilter){collection.controller.showSelected(false);collection.controller.filter('',retained);}collection.controller.lock(busy||uncertain);}finally{updatingCollection=false;}
}
collection=mountCollection($('publication-results'),{label:'Retained publications',maxSelected:50,pageSize:50,onSelection:syncSelection,renderInspector(row,host,{element,signal}){
 if(signal.aborted)return;const input=row.input,nav=element('div'),excerpt=element('section'),source=element('section'),first=element('button','Excerpt'),second=element('button','Source & use');nav.setAttribute('aria-label','Publication details');nav.append(first,second);excerpt.append(element('blockquote',input.body||'No excerpt retained.'));
 if(row.changed)excerpt.prepend(element('p','This source changed after selection. The earlier selected excerpt is preserved. Remove it from your selection before choosing the updated text.'));
 const details=element('dl');for(const[label,value]of [['Publisher',input.sourceRef],['Published',input.publishedAt||'Date not supplied'],['Retention',input.retention],['Source terms',input.rightsBasis],['Reference',input.receipt]])details.append(element('dt',label),element('dd',value));source.append(details);
 const link=element('a','Open original source \u2197');link.href=input.url;link.target='_blank';link.rel='noopener noreferrer';source.append(link);host.append(nav,excerpt,source);const tabs=mountTabs(nav,[{id:'excerpt',button:first,panel:excerpt},{id:'source',button:second,panel:source}]);return ()=>tabs.destroy();
}});
const commandButton=el('button','Commands \u00B7 \u2318K');commandButton.type='button';commandButton.className='secondary';commandButton.dataset.sourceCommands='';root.querySelector('.intake-heading').append(commandButton);
const palette=mountCommandPalette(commandButton,{scope:root,label:'Source Review commands',onError:e=>message(e.message),commands:()=>[
 {id:'find',label:'Find in loaded publications',group:'Navigate',disabled:busy||uncertain,run:()=>collection.root.querySelector('input[type=search]').focus()},
 {id:'selected',label:'Inspect selected excerpts',group:'Selection',disabled:busy||uncertain||!selected.size,run:()=>collection.controller.showSelected(true)},
 {id:'clear',label:'Clear selection',group:'Selection',disabled:busy||uncertain||!selected.size,run:()=>collection.controller.clear()},
 {id:'prepare',label:'Prepare selected research',group:'Research',disabled:$('prepare-research').disabled,run:()=>$('prepare-research').click()},
 {id:'publish',label:'Review source World publication',group:'Publication',disabled:publishSource.disabled,run:()=>publishSource.click()}
]});
async function search(more=false){
 const requestedQuery=$('publication-search').elements.q.value,nextCursor=more?cursor:0,nextGeneration=more?generation:'';message('Loading retained material\u2026');
 let page;try{page=await readPublicationPage(endpoint,{query:requestedQuery,cursor:nextCursor,source:retained,snapshot:nextGeneration,details:true,signal:AbortSignal.timeout(15000)});}catch(error){cursor=0;generation='';$('publication-more').hidden=true;throw error;}const next=more?new Map(loadedRows):new Map();
 for(const input of page.inputs){if(retained&&requestedQuery&&!((input.title+' '+input.body).toLowerCase().includes(requestedQuery.toLowerCase())))continue;
  const version=[...new Uint8Array(await crypto.subtle.digest('SHA-256',encode([input.id,input.title,input.url,input.sourceRef,input.sourceSubject||'',input.body,input.receipt,input.rightsBasis,input.retention,input.publishedAt])))].map(n=>n.toString(16).padStart(2,'0')).join('');
  next.set(identity(input),{id:identity(input),version,title:input.title,subtitle:input.sourceRef+(input.publishedAt?' \u00B7 '+input.publishedAt:''),at:Date.parse(input.publishedAt)||0,category:input.sourceRef,search:input.body,status:input.body.trim()?'Retained excerpt':'No retained text',selectable:!!input.body.trim(),input});
 }
 if(new Set([...next.keys(),...chosenRows.keys()]).size>1000)throw Error('The loaded working set has reached 1,000 publications. Choose another feed or narrow the search; your selection is preserved.');
 loadedRows=next;query=requestedQuery;cursor=page.cursor;generation=page.snapshot||'';$('publication-more').hidden=!cursor;refreshCollection({sourceFilter:!more});
 message(page.inputs.length+' publications loaded.'+(cursor?' More are available.':''));
 if(!more&&!page.inputs.some(s=>s.body.trim())&&!retained){if(!readersLoaded)await loadReaders();message('These occurrence records contain no retained text. Choose a public feed above to begin a research review.');}
}

const readersForm=el('form');readersForm.id='retained-reader-search';
const readerLabel=el('label','Read a public feed'),readerQuery=el('input');readerQuery.type='search';readerQuery.maxLength=256;readerQuery.placeholder='Find a publisher or feed';readerQuery.setAttribute('aria-label','Find a retained feed');readerLabel.append(readerQuery);
const readerFind=el('button','Find feeds'),readerMore=el('button','More feeds');readerMore.type='button';readerMore.className='secondary';readerMore.hidden=true;
const sourceLabel=el('label','Reading source'),sourceSelect=el('select');sourceSelect.id='retained-source';sourceSelect.setAttribute('aria-label','Reading source');
const original=el('option','Published occurrence records');original.value='';sourceSelect.append(original);sourceLabel.append(sourceSelect);
const sourceHint=el('p','Read the excerpts already retained by Aurekai. Nothing is fetched from the publisher when you choose a feed.');sourceHint.className='fine';readersForm.append(readerLabel,readerFind,sourceLabel,readerMore,sourceHint);$('publication-search').before(readersForm);
async function loadReaders(more=false){
 const p=await readSourceReaders(endpoint,{query:readerQuery.value.trim(),cursor:more?readerCursor:0,signal:AbortSignal.timeout(15000)}),options=p.readers;
 if(!more){const selectedOption=[...sourceSelect.options].find(o=>retained&&o.value===retained);sourceSelect.replaceChildren(original);if(selectedOption)sourceSelect.append(selectedOption);}
 for(const row of options){if(![...sourceSelect.options].some(o=>o.value===row[0])){const option=el('option',row[1]+' \u00B7 '+row[0]);option.value=row[0];sourceSelect.append(option);}}
 sourceSelect.value=retained;readerCursor=p.cursor;readerMore.hidden=!readerCursor;readersLoaded=true;
 sourceHint.textContent=options.length+' public feeds found. Choose a source to read its retained excerpts.'+(readerCursor?' More feeds are available.':'');
}
readersForm.onsubmit=guard(()=>{if(uncertain)throw Error('Resolve the pending save first.');return loadReaders();});readerMore.onclick=guard(()=>loadReaders(true));
sourceSelect.onchange=guard(async()=>{if(uncertain)throw Error('Resolve the pending save first.');retained=sourceSelect.value;generation='';await search();});

$('publication-search').onsubmit=guard(()=>{if(uncertain)throw Error('Resolve the pending save first.');return search();});
$('publication-more').onclick=guard(()=>search(true));
$('receiving-review').onchange=()=>{invalidate();controls();};$('review-title').oninput=invalidate;
$('prepare-research').onclick=guard(async()=>{
 const key=$('receiving-review').value,existing=key?await store.readVersion(key):null;
 if(key&&(!existing||existing.deleted))throw Error('The receiving review is no longer available. Choose another destination.');
 preview=await prepareResearchInputs({inputs:[...selected.values()],title:existing?.value.title??$('review-title').value,existing,sourceRecord});
 $('preparation-summary').replaceChildren(el('p',`${preview.added.length} excerpts will be added to “${preview.change.value.title}”. ${preview.unchanged.length} already match the saved review.`),el('p',existing?`Existing findings and source selections remain attached. This addition is based on saved revision ${existing.revision}.`:'A new review will open with the selected text and no prewritten findings.'));
 $('preparation').hidden=false;message('Review the receiving title and source count, then save.');
});
$('cancel-preparation').onclick=()=>{if(!busy&&!uncertain)invalidate();};
$('save-research').onclick=guard(async()=>{
 if(!preview)throw Error('Prepare the selected excerpts first.');uncertain=true;controls();
 try{
  const row=await savePreparedResearch(store,preview);uncertain=false;
  location.assign(base+'instruments/?'+new URLSearchParams({tool:'research',record:row.key}));
 }catch(error){if(error.message.startsWith('The receiving review changed.')||(error.status>=400&&error.status<500)){uncertain=false;invalidate();}throw error;}
});
addEventListener('beforeunload',event=>{if(uncertain){event.preventDefault();event.returnValue='';}});
try{busy=true;controls();await loadReviews();await search();root.dataset.ready='true';}catch(e){message(e.message);root.dataset.ready='false';}finally{busy=false;controls();}
const recovering=publicationRecoveryId();if(recovering)void openSourcePublication(null,{requestId:recovering}).catch(e=>message(e.message));

addEventListener('pagehide',event=>{if(!event.persisted){palette.destroy();collection.destroy();}});
