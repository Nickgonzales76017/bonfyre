/* Audio Studio presents narration productions already saved through its native
 * destination-owned workspace. Scripts, takes and settings use the existing
 * production model; publishing remains a separate reviewed native operation. */
import {openDestinationStore} from '../../bridge/workspace-store.mjs';
import {validateProduction,productionStatus,previewProductionEdit,applyProductionEdit,reviewScripts,renderRequests,acceptTake} from '../../bridge/narration-production.mjs';
import {decodeWav} from './creator-import.mjs';
import {encode} from '../../bridge/workspace-msgpack.mjs';
import {mountEditorialSoundDesign} from './editorial-sound-design-ui.mjs';
const root=document.querySelector('[data-narration-workspace]');
const path=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
if(root&&path){
 const store=openDestinationStore(path),activeUrls=new Set();
 const ui=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
 const status=ui('p'),index=ui('div'),more=ui('button','Load more productions'),list=ui('div');
 status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 list.className='narration-worklist';
 index.className='narration-production-index';more.type='button';more.hidden=true;
 root.append(ui('h2','Narration productions'),
   ui('p','Write the script, record or render a take, and set its music. Saving a take never publishes it.'),
   status,index,more,list);
 const style=ui('style');
 style.textContent='[data-narration-workspace]{margin:20px auto;max-width:1100px;padding:20px;border:1px solid #496451;border-radius:12px;background:#18291f;color:#edf5ee}'+
 '.narration-worklist{display:grid;gap:16px}.narration-worklist article{border:1px solid #5a765f;border-radius:10px;padding:16px;min-width:0}'+
 '.narration-worklist .passage{padding:12px 0;border-top:1px solid #526b58}.narration-worklist label{display:block;margin:12px 0}'+
 '.narration-worklist textarea,.narration-worklist input{display:block;box-sizing:border-box;width:100%;margin-top:5px;padding:10px;background:#102018;color:#fff;border:1px solid #89a893;border-radius:7px;font:inherit}'+
 '.narration-worklist textarea{min-height:90px;resize:vertical}.narration-worklist button{min-height:44px;padding:10px;background:#3b704d;color:white;border:1px solid #9fc4a5;border-radius:7px;cursor:pointer}'+
 '.narration-worklist button:disabled{opacity:.5}.narration-worklist .actions{display:flex;flex-wrap:wrap;gap:9px}'+
 '.narration-worklist audio{display:block;max-width:100%;margin:12px 0}.narration-worklist a{color:#c0efcf;overflow-wrap:anywhere}.narration-production-index{display:grid;gap:8px;margin:16px 0}.narration-production-index button{display:block;text-align:left;min-height:48px;padding:12px;color:#eaf3ec;background:#2b4934;border:1px solid #799883;border-radius:8px;cursor:pointer}.editorial-sound-design{margin:20px 0;padding:20px;border:1px solid #6f9076;border-radius:12px;background:#203729}.editorial-sound-fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:13px;align-items:end}.editorial-sound-design select,.editorial-sound-design input{width:100%;min-height:44px;margin-top:5px;padding:9px;background:#102018;color:#fff;border:1px solid #89a893;border-radius:7px;box-sizing:border-box}.editorial-sound-design details{margin-top:18px;padding-top:12px;border-top:1px solid #799883}.editorial-sound-design summary{cursor:pointer;font-weight:600;padding:10px 0}.editorial-sound-actions{display:flex;flex-wrap:wrap;gap:9px;margin:14px 0}.editorial-sound-design p{color:#c4d9c7;font-size:13px;line-height:1.6}';
 root.append(style);
 const say=text=>{status.textContent=text;};
 const action=(text,fn)=>{const b=ui('button',text);b.type='button';b.addEventListener('click',async()=>{b.disabled=true;try{await fn();}catch(e){say(e.message);}finally{b.disabled=false;}});return b;};
 const save=async(row,next)=>{validateProduction(next);await store.readVersion(row.key);await store.saveLocal(row.key,next,row.revision);await refresh();await openProduction(row.key);};
 const goodKey=key=>/^draft:creator:narration:[a-f0-9]{64}$/.test(key);
 async function view(row){
  if(!goodKey(row.key))return;
  const p=validateProduction(row.value),state=await productionStatus(p),card=ui('article');
  card.append(ui('h3',p.title),ui('p',p.languages.map(l=>l.label||l.id).join(' · ')));
  for(const item of p.items){
   const link=ui('a','Original source: '+item.title);link.href=item.source.href;link.target='_blank';link.rel='noopener noreferrer';card.append(link);
  }
   await mountEditorialSoundDesign({host:card,production:p,store,save:next=>save(row,next)});
  for(const cell of p.cells){
   const current=state.find(s=>s.id===cell.id),section=ui('div');section.className='passage';
   section.append(ui('h4',cell.segment+' · '+cell.language+' · '+current.state));
   const script=ui('textarea');script.value=cell.script;script.maxLength=12000;
   const voice=ui('input');voice.value=current.settings.voice;voice.maxLength=80;
   const rate=ui('input');rate.type='number';rate.min='80';rate.max='300';rate.step='1';rate.value=String(current.settings.rate);
   const gap=ui('input');gap.type='number';gap.min='0';gap.max='10000';gap.step='50';gap.value=String(current.settings.gapMs);
   const gain=ui('input');gain.type='number';gain.min='0';gain.max='1';gain.step='0.05';gain.value=String(current.settings.gain);
   for(const [label,control]of [['Script',script],['Voice',voice],['Words per minute',rate],['Gap after passage (ms)',gap],['Voice level (0–1)',gain]]){const field=ui('label',label);field.append(control);section.append(field);}
   const changed=()=>script.value!==cell.script||voice.value!==current.settings.voice||Number(rate.value)!==current.settings.rate||Number(gap.value)!==current.settings.gapMs||Number(gain.value)!==current.settings.gain;
   const actions=ui('div');actions.className='actions';
   actions.append(action('Save passage settings',async()=>{
    if(!changed()){say('No script changes to save.');return;}
    const preview=await previewProductionEdit(p,{scope:'cells',ids:[cell.id],set:{script:script.value,voice:voice.value,rate:Number(rate.value),gapMs:Number(gap.value),gain:Number(gain.value)}});
    await save(row,await applyProductionEdit(p,preview));
   }));
   actions.append(action('Review script',async()=>{if(changed())throw Error('Save passage settings before review.');await save(row,await reviewScripts(p,[cell.id]));}));
   actions.append(action('Render reviewed passage',async()=>{
    if(changed())throw Error('Save and review script changes before rendering.');
    const [request]=await renderRequests(p,[cell.id]);say('Rendering the reviewed passage…');
    const response=await fetch(path.replace(/_records$/,'_narrate'),{method:'POST',credentials:'same-origin',redirect:'error',cache:'no-store',headers:{'Content-Type':'application/msgpack'},body:encode([1,request.script,request.voice,request.rate])});
    if(!response.ok)throw Error((await response.text()).slice(0,250)||'Native narration was refused.');
    if(response.headers.get('content-type')?.split(';')[0]!=='audio/wav')throw Error('Unexpected narration format.');
    const bytes=await response.arrayBuffer(),sha=new Uint8Array(await crypto.subtle.digest('SHA-256',bytes));
    const id='take-'+Array.from(sha.slice(0,12),n=>n.toString(16).padStart(2,'0')).join('');
    await save(row,await acceptTake(p,request,{id,bytes,decode:bytes=>decodeWav(bytes,undefined,{metadataOnly:true})}));
   }));
   section.append(actions);
   const take=cell.takes.find(t=>t.id===cell.activeTake);
   if(take){const audio=ui('audio');audio.controls=true;audio.preload='none';audio.setAttribute('aria-label','Recorded passage '+cell.id);
    const url=URL.createObjectURL(new Blob([take.bytes],{type:'audio/wav'}));activeUrls.add(url);audio.src=url;section.append(audio);}
   card.append(section);
  }
  list.append(card);
 }
  let indexCursor='',indexEpoch=-1,totalListed=0,openedKey=null;
  async function openProduction(key){
   if(!goodKey(key))throw Error('Choose the exact narration production.');
   const row=await store.readVersion(key);
   if(!row||row.deleted)throw Error('This production is no longer available.');
   openedKey=key;for(const url of activeUrls)URL.revokeObjectURL(url);activeUrls.clear();list.replaceChildren();
   await view(row);say('Opened '+row.value.title+' at revision '+row.revision+'.');
  }
 async function refresh(reset=true){
  if(reset){indexCursor='';indexEpoch=-1;totalListed=0;index.replaceChildren();}
  const page=await store.readMetadata('draft:creator:narration:',{cursor:indexCursor,epoch:indexEpoch});const rows=page.rows.filter(row=>goodKey(row.key));
  if(!rows.length&&!totalListed)say('No narration productions in this destination yet.');
  for(const row of rows){if(row.value?.schema!=='aurekai.narration-production.v1')continue;const name=typeof row.value.title==='string'?row.value.title:'Untitled production';const detail=(row.value.languages?.length||0)+' languages · '+(row.value.cells?.length||0)+' passages · revision '+row.revision;index.append(action(name+' · '+detail,()=>openProduction(row.key)));totalListed++;}indexCursor=page.cursor;indexEpoch=page.epoch;more.hidden=!page.more;if(totalListed&&!openedKey)say(totalListed+' productions listed; open one to load recordings.');
 }
  more.addEventListener('click',async()=>{more.disabled=true;try{await refresh(false);}catch(e){say('The native production index changed: '+e.message+' Reload the workspace to refresh.');}finally{more.disabled=false;}});
 addEventListener('pagehide',()=>{for(const url of activeUrls)URL.revokeObjectURL(url);activeUrls.clear();},{once:true});
 (async()=>{
  await refresh();
  const requested=new URLSearchParams(location.search).get('production');
  if(requested!==null){
   if(!/^[a-f0-9]{64}$/.test(requested))throw Error('Select an exact saved narration edition.');
   await openProduction('draft:creator:narration:'+requested);
   list.scrollIntoView?.({block:'start'});
  }
 })().catch(e=>say('Narration workspace unavailable: '+e.message));
}
