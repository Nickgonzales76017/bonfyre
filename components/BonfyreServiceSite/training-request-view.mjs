import {readLocal,saveLocal} from './store.mjs';
const esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const states={pending:'Waiting to send',ready:'Received',running:'In progress',attention:'Needs creator attention',complete:'Delivered',cancelled:'Closed',dead_letter:'Needs creator attention',retry_wait:'Waiting for creator'};
export async function openRequestReceipts(member){
  const key=`training:requests:${member}`,stored=(await readLocal(key)).find(r=>r.key===key);let entries=stored?.value??[],revision=stored?.revision??0;
  const views=new Map();
  async function save(next){const saved=await saveLocal(key,next,revision);entries=next;revision=saved.revision;}
  async function send(id){
    const entry=entries.find(e=>e.id===id);if(!entry)throw Error('Request draft not found.');
    const response=await fetch('/api/training/requests',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(entry.brief)}),value=await response.json();
    if(!response.ok)throw Error(value.error??'The request could not be sent.');
    await save(entries.map(e=>e.id===id?{...e,accessToken:value.accessToken}:e));views.set(id,value);return value;
  }
  return {
    rows:creatorId=>entries.filter(e=>e.brief.creatorId===creatorId).map(e=>({...e,view:views.get(e.id)})),
    async submit(brief){if(entries.length>=500)throw Error('This device holds 500 request receipts.');const id=crypto.randomUUID();await save([...entries,{id,brief:{...brief,id},accessToken:null}]);return send(id);},
    retry:send,
    async refresh(creatorId){for(const entry of entries.filter(e=>e.brief.creatorId===creatorId&&e.accessToken)){
      const response=await fetch(`/api/training/requests/${entry.id}`,{headers:{'x-request-receipt':entry.accessToken}}),value=await response.json();
      if(!response.ok)throw Error(value.error??'Could not refresh requests.');views.set(entry.id,value);
    }},
    delivered:id=>views.get(id)?.delivery?.routine
  };
}
export function requestsView(creator,rows,state){return `<div class="heading"><p class="eyebrow">${esc(creator.settings.brand.name)}</p><h1>Program requests</h1><p class="lede">Ask your creator for a change to equipment, timing or programming.</p></div><form id="creator-request-form" class="panel"><h2>Request a program change</h2><label>Program<select name="programId">${creator.routines.map(r=>`<option value="${esc(r.id)}">${esc(r.name)}</option>`).join('')}</select></label><label>Title<input name="title" maxlength="160" required placeholder="A dumbbell version of Foundations"></label><label>What do you need?<textarea name="details" rows="4" maxlength="4000" required placeholder="Tell your creator what equipment and time you have."></textarea></label><p class="muted">Sends this brief and the program reference to ${esc(creator.settings.brand.name)}.</p><button>Send request</button></form><div class="section-title"><h2>Your requests</h2><button class="secondary" data-action="request.refresh">Refresh</button></div>${rows.length?rows.slice().reverse().map(row=>{
  const view=row.view,label=view?states[view.state]??view.state:row.accessToken?'Refresh for status':'Waiting to send';
  return `<article class="panel request-card"><p class="eyebrow">${esc(label)}</p><h2>${esc(row.brief.title)}</h2><p>${esc(row.brief.details)}</p>${!row.accessToken?`<button class="secondary" data-action="request.retry" data-id="${row.id}">Retry submission</button>`:''}${view?.delivery?`<div class="delivery"><h3>${esc(view.delivery.routine.name)}</h3><p>${view.delivery.routine.exercises.length} exercises · version ${esc(view.delivery.routine.source.version)}</p><details><summary>View delivered program</summary>${view.delivery.routine.exercises.map(e=>`<p>${esc(e.name)} · ${e.sets.length} sets</p>`).join('')}</details><button data-action="request.adopt" data-id="${row.id}" ${state.routines.some(r=>r.id===view.delivery.routine.id)?'disabled':''}>${state.routines.some(r=>r.id===view.delivery.routine.id)?'Added to your routines':'Add to my routines'}</button></div>`:''}</article>`;
}).join(''):'<p class="muted">Requests you send to this studio will appear here.</p>'}`;}
