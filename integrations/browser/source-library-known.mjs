// ForeignTwin catalogue -> reviewed private source bookmark.
// Catalogue entries are observations, not user records, connected providers or permission.
import {readWorkspacePacket} from '../bridge/workspace-destinations.mjs';
import {openDestinationStore} from '../bridge/workspace-store.mjs';
import {prepareLink,checkSource,knownSourcePage,sourceTags,recipeCorpusResults} from './source-material.mjs';
const mk=(tag,text='')=>{const x=document.createElement(tag);x.textContent=text;return x;};
const button=(text,fn)=>{const x=mk('button',text);x.type='button';x.addEventListener('click',fn);return x;};
const field=(label,input)=>{const x=mk('label',label);x.append(input);return x;};
// One explicitly reviewed native bookmark writer for public source coordinates.
// The two source families share revision, conflict, and uncertain-reply policy.
function reviewBookmark({signal,store,dialogs,url,title,collection,tags,notes,provenance,label,description,identity,kind}){
 if(signal.aborted)return;
 const dialog=mk('dialog'),name=mk('input'),folder=mk('input'),tagField=mk('input'),message=mk('p'),footer=mk('footer');
 name.value=title;name.maxLength=500;folder.value=collection;folder.maxLength=120;tagField.value=tags.join(', ');tagField.maxLength=128;
 dialog.className='source-dialog'+(kind==='known'?' known-source-review':'');
 if(kind==='recipe')dialog.dataset.recipeReview='';
 message.setAttribute('role','status');
 let pending=false,uncertain=false;
 const save=button(label,()=>void commit()),close=button('Close',()=>{if(!pending)dialog.close();});
 footer.append(close,save);
 dialog.append(mk('h2','Review source bookmark'),mk('p',description),mk('p',identity),mk('code',url),field('Title',name),field('Collection',folder),field('Tags',tagField),message,footer);
 document.body.append(dialog);dialogs.add(dialog);dialog.showModal();
 dialog.addEventListener('cancel',e=>{if(pending)e.preventDefault();});
 dialog.addEventListener('close',()=>{dialogs.delete(dialog);dialog.remove();},{once:true});
 function saved(key){message.textContent='Reviewed source reference saved exactly once.';save.remove();const link=mk('a','Open saved source');link.href='?'+new URLSearchParams({record:key});footer.prepend(link);}
 async function commit(){
  if(pending||uncertain||signal.aborted)return;
  pending=true;save.disabled=close.disabled=true;message.textContent='Checking current native source identity…';
  let change,attempted=false;
  try{
   change=await prepareLink(url,{title:name.value,collection:folder.value,tags:sourceTags(tagField.value),notes});
   change.value.provenance=provenance;checkSource(change.value);signal.throwIfAborted();
   const retained=await store.readVersion(change.key);signal.throwIfAborted();
    if(retained&&!retained.deleted){if(retained.value?.schema!=='bonfyre.source-material.v1'||retained.value.url!==change.value.url)throw Error('The retained source identity does not match the reviewed address. Reopen your library.');saved(change.key);message.textContent='This source address is already saved. Reopen the existing record; no duplicate write was sent.';return;}
   if(retained?.deleted)throw Error('Restore archived material in the saved Source Library first.');
   attempted=true;await store.saveLocal(change.key,change.value,0);signal.throwIfAborted();saved(change.key);
  }catch(error){
   if(signal.aborted)return;
   if(attempted&&change){
    let current;try{current=await store.readVersion(change.key);}catch{}
    if(current&&!current.deleted&&current.value?.id===change.value.id&&current.value?.url===change.value.url){saved(change.key);return;}
    uncertain=true;message.textContent='Save acknowledgement uncertain. Read your saved library before retrying; no write was replayed.';
   }else message.textContent=error.message;
  }finally{pending=false;close.disabled=false;if(!uncertain&&save.isConnected&&!signal.aborted)save.disabled=false;}
 }
}
function mountRecipeCorpus(root,{signal,store,dialogs}){
 const shell=mk('section'),form=mk('form'),query=mk('input'),go=mk('button','Search recipes'),status=mk('p','Search the retained preparation corpus. Nothing has been saved.'),results=mk('div');
 shell.dataset.recipeCorpus='';query.type='search';query.maxLength=140;query.required=true;query.placeholder='Recipe or ingredient';query.setAttribute('aria-label','Search retained recipes');go.type='submit';status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 form.append(query,go);shell.append(mk('h2','Recipes from the retained corpus'),mk('p','Open exact ingredients and scaled plans, then explicitly save a reference to your Source Library. A bookmark does not copy the recipe or purchase ingredients.'),form,status,results);root.append(shell);
 let loading=false;
 form.addEventListener('submit',async event=>{
  event.preventDefault();if(loading||signal.aborted)return;const q=query.value.trim();if(q.length<2||q.length>140){status.textContent='Use 2–140 characters.';return;}
  loading=true;go.disabled=true;results.replaceChildren();status.textContent='Searching retained recipes…';
  try{
   const response=await fetch('/a2a',{method:'POST',headers:{'Content-Type':'application/json'},credentials:'same-origin',signal,body:JSON.stringify({jsonrpc:'2.0',id:'source-library-recipe',method:'message/send',params:{skill:'recipe-search',query:q}})});
   if(!response.ok||!/^application\/a2a\+json\b/.test(response.headers.get('Content-Type')||''))throw Error('Native recipe search is unavailable.');
   const body=await response.text();if(body.length>32768)throw Error('Recipe search exceeded the response bound.');
   const hits=recipeCorpusResults(JSON.parse(body),q);signal.throwIfAborted();
   for(const hit of hits){
    const card=mk('article'),link=mk('a','Open ingredients and plan');link.href='/recipes?'+new URLSearchParams({id:hit.key});
    card.append(mk('h3',hit.title),link,button('Review source bookmark',()=>review(hit)));results.append(card);
   }
   status.textContent=hits.length+' exact recipe matches. No private records created.';
  }catch(error){if(!signal.aborted)status.textContent='Recipe search unavailable: '+error.message+' Nothing was saved.';}
  finally{loading=false;if(!signal.aborted)go.disabled=false;}
 });
 function review(hit){
  const url=new URL('/recipes?'+new URLSearchParams({id:hit.key}),'https://aurekai.life').href;
  reviewBookmark({signal,store,dialogs,url,title:hit.title,collection:'Recipes',tags:['recipe'],
   notes:'Native recipe material key: '+hit.key,identity:hit.key,kind:'recipe',
   provenance:{schema:'bonfyre.recipe-material-ref.v1',recordKey:hit.key,source:'native-recipe-material'},
   label:'Save reviewed recipe reference',
   description:'This saves an exact public recipe address, not the recipe body, grocery order, license or financial approval.'});
 }
}
function mount(root){
 const endpoint=document.querySelector('meta[name=bonfyre-workspace-store]')?.content;
 if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint||''))return;
 const abort=new AbortController(),signal=abort.signal,store=openDestinationStore(endpoint,{signal}),dialogs=new Set();
 const shelf=mk('section'),head=mk('header'),title=mk('h2','Browse retained sources'),description=mk('p','Discover verified catalogue coordinates, then explicitly review a publisher bookmark for your private library. This view is not a saved draft or permission to reuse its contents.');
 const status=mk('p','Browse the current ForeignTwin catalogue. Nothing is saved until you approve.'),results=mk('div'),query=mk('input'),form=mk('form');
 const browse=button('Browse known sources',()=>void load(false)),more=button('Show more sources',()=>void advance());
 shelf.className='known-source-lane';shelf.dataset.knownSourceLane='';results.className='known-source-results';
 status.setAttribute('role','status');status.setAttribute('aria-live','polite');query.type='search';query.maxLength=140;query.setAttribute('aria-label','Search known publishers');query.placeholder='Filter sources by publisher or topic';
 form.className='known-source-search';const go=mk('button','Search');go.type='submit';form.append(query,go);
 form.addEventListener('submit',e=>{e.preventDefault();void load(false);});
 head.append(title,description);const actions=mk('nav');actions.append(browse,more);more.hidden=true;shelf.append(head,form,actions,status,results);
 const drop=root.querySelector('.source-drop');if(drop)drop.after(shelf);else root.append(shelf);mountRecipeCorpus(root,{signal,store,dialogs});
 let rows=[],shown=0,next=null,generation='',search='',reading=false;
 const stop=()=>{if(signal.aborted)return;abort.abort();for(const d of dialogs){d.close();d.replaceChildren();d.remove();}dialogs.clear();results.replaceChildren();status.textContent='Account context changed. Reopen this library.';};
 window.addEventListener('pagehide',stop,{signal,once:true});window.addEventListener('bonfyre:account-changed',stop,{signal,once:true});
 function render(){
  for(const row of rows.slice(shown,shown+24)){
   const card=mk('article'),info=mk('p',row.family+' · '+row.archived+' archived items · '+row.sourceRef),
     original=mk('a','Visit publisher');original.href=row.url;original.target='_blank';original.rel='noopener noreferrer';
   card.append(mk('h3',row.title),info,original,button('Review bookmark',()=>review(row,generation)));results.append(card);
  }
  shown=Math.min(rows.length,shown+24);more.hidden=shown===rows.length&&next===null;
  more.textContent=shown<rows.length?'Show more matches':'Next catalogue page';
 }
 async function advance(){if(reading||signal.aborted)return;if(shown<rows.length)render();else if(next!==null)await load(true);}
 async function load(append){
  if(reading||signal.aborted)return;reading=true;browse.disabled=more.disabled=go.disabled=true;
  status.textContent='Reading the current source catalogue…';
  try{
   const requested=append?search:query.value.trim();
   if(requested.length>140)throw Error('Narrow the publisher query.');
   const url='/foreign/twins'+(append&&next!==null?'/page/'+next:'')+'?'+new URLSearchParams({q:requested,...(append?{snapshot:generation}:{})});
   const page=knownSourcePage(await readWorkspacePacket(url,{type:'application/json',signal,limit:4*1024*1024,timeoutMs:15000}));
   signal.throwIfAborted();if(append&&page.generation!==generation)throw Error('The catalogue changed. Search again.');
   if(append&&page.next===next)throw Error('The catalogue cursor did not advance.');
   if(!append){rows=[];shown=0;results.replaceChildren();generation=page.generation;search=requested;}
   const seen=new Set(rows.map(r=>r.sourceRef));for(const row of page.rows){if(seen.has(row.sourceRef))throw Error('Repeated source across catalogue pages.');seen.add(row.sourceRef);rows.push(row);}
   if(rows.length>1000)throw Error('Too many matching sources. Narrow the query.');
   next=page.next;render();status.textContent=rows.length+' known sources of '+page.total+' matches. These are not your saved records.';
  }catch(e){if(!signal.aborted)status.textContent='Known-source read unavailable: '+e.message+'. Your saved work was not changed.';}
  finally{reading=false;if(!signal.aborted)browse.disabled=more.disabled=go.disabled=false;}
 }
 function review(row,snapshot){
  reviewBookmark({signal,store,dialogs,url:row.url,title:row.title,collection:row.family,
   tags:row.family==='Other'?[]:[row.family],notes:'Reviewed known source: '+row.sourceRef,
   identity:'Retained source: '+row.sourceRef,kind:'known',
   provenance:{schema:'bonfyre.known-source-review.v1',sourceRef:row.sourceRef,catalogueGeneration:snapshot},
   label:'Save reviewed bookmark',
   description:'Save only this public publisher address; the catalogue does not verify claims, transfer original bodies or grant account access.'});
 }
}
if(typeof document!=='undefined'){
 const start=()=>{const root=document.querySelector('[data-source-library]');if(root)mount(root);};
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
}
