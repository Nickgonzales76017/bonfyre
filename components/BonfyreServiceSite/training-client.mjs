import {heartRateConnection,currentHeartRate} from './training-devices.mjs';
import {readTCX,planCalendar} from './training-device-files.mjs';
import {devicesView,heartRateTile,heartRateHistory,deviceActivityCard} from './training-device-view.mjs';
import {publicPlanCards,planAdoption,memberPlans} from "./training-plan-view.mjs";
import {loadStudioAppearance} from "./training-appearance.mjs";
import {openRequestReceipts,requestsView} from './training-request-view.mjs';
import {loadCatalogue} from './training-catalogue.mjs';
import {importBrandAsset,brandAssetControls,assetPreview,studioMarkIcon} from './training-brand-assets.mjs';
import {buildAssistRequest,applyAssistProposal} from './training-assist.mjs';
import {assistView} from './training-assist-view.mjs';
import {readLocal,saveLocal} from './store.mjs';
import {nextTargets} from './training-progression.mjs';
import {weightPanel} from './training-weight.mjs';
import {enableOffline} from './training-offline.mjs';
import {newTraining,makeSet,trainingCommand,exportTraining,importTraining,checkTraining,workoutSummary,elapsedSeconds,restSeconds,toKg,fromKg,nextTrainingSet} from './training-model.mjs';
import {importOpenGym} from './training-import.mjs';
import {routineDraft,editorHTML,readEditor,changeDraft,finishDraft} from './training-editor.mjs';
import {checkSettings,exportPrograms,brandColors} from './training-settings.mjs';
const $=id=>document.getElementById(id),esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const key='training:member:primary';
let pendingPlan=null,pendingDeviceWorkouts=null,recordHeartRate=false,lastHeartSave=0,heartRateSince=0;
const sensor=heartRateConnection({onChange:()=>updateSensor()});
function updateSensor(){const connection=sensor.state,r=currentHeartRate(connection);document.querySelectorAll("[data-heart-bpm]").forEach(el=>el.textContent=r?r.bpm:"—");document.querySelectorAll("[data-heart-name]").forEach(el=>el.textContent=connection.name);document.querySelectorAll("[data-device-state]").forEach(el=>el.textContent=connection.status);document.querySelectorAll("[data-heart-status]").forEach(el=>el.textContent=connection.message||(r?"Live reading":connection.status==="connected"?"Waiting for a fresh reading":connection.status));}
let state,revision=0,material,creator,settings,settingsRevision=0,pendingImport=null,importNotes=[],busy=false,editing=null,assist=null,modelAbort=null,pendingBrandAssets={},requestReceipts;
let catalogue={programs:[],available:false,fresh:false};
const uid=()=>crypto.randomUUID(),format=(n,d=0)=>Number(n).toLocaleString(undefined,{maximumFractionDigits:d});
const duration=s=>`${Math.floor(s/60)}:${String(s%60).padStart(2,'0')}`;
const settingsKey=()=>`training:studio:${creator.id}:settings`;
function notice(message){$('notice').textContent=message;$('notice').hidden=!message;}
function download(name,body,type='application/json') {
  const url=URL.createObjectURL(new Blob([body],{type})),a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
async function action(fn) {
  if(busy)return;busy=true;notice('');
  try{await fn();}catch(error){notice(error.message);}finally{busy=false;}
}
async function commit(command,{refresh=true}={}) {
  const next=trainingCommand(state,command),saved=await saveLocal(key,next,revision);
  if(['start','resume'].includes(command.type))heartRateSince=Date.now();
  state=next;revision=saved.revision;$('save-state').textContent='Saved on this device';if(refresh)render();
}
function button(label,actionName,extra=''){return `<button data-action="${actionName}" ${extra}>${label}</button>`;}
function heading(eyebrow,title,body=''){return `<div class="heading"><p class="eyebrow">${esc(eyebrow)}</p><h1>${esc(title)}</h1>${body?`<p class="lede">${esc(body)}</p>`:''}</div>`;}
function targetPreview(r){
  const exercises=r.exercises.filter(e=>e.progression);if(!exercises.length)return '';
  return '<details class="target-preview"><summary>Next targets</summary>'+exercises.map(e=>{const target=nextTargets(e,r.id,state.history);return `<div><strong>${esc(e.name)}</strong><p>${target.sets.filter(s=>s.type==='work').map(s=>e.kind==='timed'?s.seconds+' seconds':format(fromKg(s.loadKg,state.unit),2)+' '+state.unit+' × '+s.reps).join(' · ')}</p><p>${esc(target.reason)}</p></div>`;}).join('')+'</details>';
}
function routineCard(r,adopt=false){return `<article class="program-card"><div class="program-art" aria-hidden="true"><span>${esc(r.name.split(' ').map(x=>x[0]).slice(0,2).join(''))}</span><div class="art-ring"></div></div><div class="card-body"><p class="eyebrow">${esc(r.source?.creator??'Your routine')}${r.source?.version?' · Version '+esc(r.source.version):''} · ${r.exercises.length} ${r.exercises.length===1?"exercise":"exercises"}</p><h2>${esc(r.name)}</h2><p>${esc(r.publicDescription??r.exercises.map(e=>e.name).join(' / '))}</p>${adopt?'':targetPreview(r)}${button(adopt?(state.routines.some(x=>x.id===r.id)?'Added to your routines':'Add to my routines'):'Start workout',adopt?'adopt':'start',`data-id="${esc(r.id)}" ${adopt&&state.routines.some(x=>x.id===r.id)?'disabled':''}`)}${adopt?'':button('Edit routine','routine.edit',`class="secondary" data-id="${esc(r.id)}"`)}${adopt?'':`<details class="adapt-options"><summary>Adapt with a model</summary>${r.exercises.map(e=>button(esc(e.name),'assist.open',`class="text-button" data-routine="${esc(r.id)}" data-exercise="${esc(e.id)}"`)).join('')}</details>`}</div></article>`;}
function today(){
  const total=state.history.reduce((n,w)=>n+workoutSummary(w).sets,0),latest=state.history.at(-1);
  return heading('YOUR TRAINING','Your next session.',creator.description)+`<div class="stats"><div><strong>${state.history.length}</strong><span>Workouts logged</span></div><div><strong>${total}</strong><span>Sets completed</span></div><div><strong>${state.routines.length}</strong><span>Your routines</span></div></div>`+
    memberPlans(state,{compact:true})+(state.active?`<div class="resume"><div><p class="eyebrow">${state.active.pausedAt===null?'IN PROGRESS':'PAUSED'}</p><h2>${esc(state.active.name)}</h2></div><a class="button" href="#train">Continue workout</a></div>`:'')+
    `<div class="section-title"><h2>${state.routines.length?'Ready when you are':'Start with a program'}</h2><a href="#programs">All programs ↗</a></div><div class="cards">${(state.routines.length?state.routines.slice(0,2):creator.routines.slice(0,2)).map(r=>routineCard(r,!state.routines.length)).join('')}</div>`+
    (latest?`<div class="recent"><p class="eyebrow">LAST SESSION</p><h3>${esc(latest.name)}</h3><p>${new Date(latest.endedAt).toLocaleDateString()} · ${workoutSummary(latest).sets} sets · ${duration(elapsedSeconds(latest))}</p></div>`:'');
}
function train(){
  const a=state.active;
  if(assist)return heading('YOUR ROUTINE','Adapt your training')+assistView(assist,state,settings.model);
  if(editing)return heading('YOUR ROUTINE','Plan your session')+editorHTML(editing,state.unit);
  if(!a) return heading('TRAIN','Your next session')+memberPlans(state)+`<div class="cards">${state.routines.map(r=>routineCard(r)).join('')}</div><div class="toolbar">${button('Build a routine','routine.new')}</div>`;

  const next=nextTrainingSet(a),done=workoutSummary(a).sets,total=a.exercises.reduce((n,e)=>n+e.sets.length,0);
  return heading(a.pausedAt===null?'WORKOUT IN PROGRESS':'WORKOUT PAUSED',a.name)+(sensor.state.status==='connected'?heartRateTile(sensor.state,recordHeartRate):'<p><a href="#devices">Connect a heart-rate sensor ↗</a></p>')+`<div class="session-bar"><span><strong id="elapsed">${duration(elapsedSeconds(a))}</strong> elapsed</span><span>${done} / ${total} sets</span>${button(a.pausedAt===null?'Pause':'Resume',a.pausedAt===null?'pause':'resume','class="secondary"')}</div>${next?`<p class="next-set">Next: ${esc(next.name)} · set ${next.round+1}</p>`:''}<div class="rest-bar"><span id="rest">${restSeconds(a)?`Rest · ${duration(restSeconds(a))}`:'Ready for your next set'}</span>${button('Skip rest','rest.skip','class="text-button"')}</div>`+
  a.exercises.map(e=>`<article class="exercise"><div class="exercise-head"><div><p class="eyebrow">${esc(e.kind)}${e.superset?' · Superset '+esc(e.superset):''}</p><h2>${esc(e.name)}</h2></div><span>${e.restSeconds}s rest</span></div>${e.progressionReason?`<p class="muted">${esc(e.progressionReason)}</p>`:''}<div class="set-labels"><span>Set</span><span>${e.kind==='timed'||e.kind==='cardio'?'Seconds':state.unit}</span><span>${e.kind==='cardio'?'Meters':e.kind==='timed'?'Added '+state.unit:'Reps'}</span><span>Done</span></div>${e.sets.map((s,i)=>`<form class="set-row ${s.done?'complete':''}" data-exercise="${esc(e.id)}" data-set="${esc(s.id)}"><span>${i+1}${s.side?`<small>${s.side==='L'?'Left':'Right'}</small>`:''}${s.type==='warmup'?'<small>Warmup</small>':''}</span><input aria-label="${esc(e.name)} set ${i+1} ${e.kind==='timed'||e.kind==='cardio'?'seconds':state.unit}" name="first" type="number" min="0" max="${e.kind==='timed'||e.kind==='cardio'?86400:4409}" step="any" value="${e.kind==='timed'||e.kind==='cardio'?s.seconds:Math.round(fromKg(s.loadKg,state.unit)*100)/100}" required ${a.pausedAt!==null?'disabled':''}><input aria-label="${esc(e.name)} set ${i+1} ${e.kind==='cardio'?'meters':e.kind==='timed'?'load':'reps'}" name="second" type="number" min="0" max="${e.kind==='cardio'?1e6:10000}" step="${e.kind==='cardio'||e.kind==='timed'?'any':'1'}" value="${e.kind==='cardio'?s.distanceM:e.kind==='timed'?Math.round(fromKg(s.loadKg,state.unit)*100)/100:s.reps}" required ${a.pausedAt!==null?'disabled':''}><button class="set-done" aria-label="${s.done?'Undo':'Complete'} ${esc(e.name)} set ${i+1}" ${a.pausedAt!==null?'disabled':''}>${s.done?'✓':'○'}</button>${(()=>{const prescribed=e.prescribedSets?.find(p=>p.id===s.id),scale=prescribed?.rir!=null?'rir':prescribed?.rpe!=null?'rpe':null;return scale?`<label class="set-effort">${scale.toUpperCase()} recorded (target ${prescribed[scale]})<input name="effort" type="number" min="${scale==='rpe'?1:0}" max="10" step="any" value="${s[scale]??''}" ${a.pausedAt!==null?'disabled':''}></label>`:'';})()}</form>`).join('')}</article>`).join('')+`<div class="session-actions">${button('Finish workout','finish')}${button('Discard workout','discard','class="text-button"')}</div>`;
}
function activity(){return heading('ACTIVITY','Training history')+weightPanel(state)+(state.deviceActivities?.length?'<section><h2>Watch workouts</h2>'+[...state.deviceActivities].sort((a,b)=>b.startedAt-a.startedAt).map(deviceActivityCard).join('')+'</section>':'')+(!state.history.length?'<div class="panel"><h2>Your history starts here</h2><p>Finish a workout to see your sessions and training volume.</p><a href="#train">Choose a workout ↗</a></div>':`<div class="history">${[...state.history].reverse().map(w=>{const s=workoutSummary(w);return `<article class="history-row"><div class="date-tile"><strong>${new Date(w.endedAt).getDate()}</strong><span>${new Date(w.endedAt).toLocaleDateString(undefined,{month:'short'})}</span></div><div><h2>${esc(w.name)}</h2><p>${s.sets} sets · ${format(fromKg(s.volumeKg,state.unit))} ${state.unit} lifted · ${duration(elapsedSeconds(w))}</p></div><details><summary>Sets & heart rate</summary>${heartRateHistory(w)}${w.exercises.map(e=>`<p><strong>${esc(e.name)}</strong><br>${e.sets.filter(s=>s.done).map(s=>e.kind==='timed'||e.kind==='cardio'?`${s.seconds}s${s.distanceM?' / '+s.distanceM+'m':''}`:`${format(fromKg(s.loadKg,state.unit),2)} ${state.unit} × ${s.reps}`).join(' · ')}</p>`).join('')}</details></article>`;}).join('')}</div>`)+`<div class="toolbar">${button('Export my training','export','class="secondary"')}<label class="button secondary">Import training<input type="file" id="import-file" accept=".json,application/json" hidden></label></div>${pendingImport?`<div class="panel"><h2>Review import</h2><p>${pendingImport.history.length} workouts and ${pendingImport.routines.length} routines${pendingImport.plans?.length?' and '+pendingImport.plans.length+' plans':''}. This replaces the training saved on this device.</p>${importNotes.length?`<ul>${importNotes.map(n=>`<li>${esc(n)}</li>`).join('')}</ul>`:''}${button('Replace with this file','import.apply')}${button('Cancel','import.cancel','class="text-button"')}</div>`:''}`;}
function settingsView(){const b=settings.brand,m=settings.model;const options=(values,current)=>values.map(v=>`<option value="${v}" ${v===current?'selected':''}>${v}</option>`).join('');return heading('STUDIO SETTINGS','Studio settings','Your appearance and connection preferences on this device.')+`<form id="settings-form" class="panel"><h2>Brand & style</h2><p>${settings.followStudio?'Following the studio appearance.':'Using your appearance on this device.'}</p><button type="button" class="secondary" data-action="appearance.follow">Use studio appearance</button>${brandAssetControls({...b,...pendingBrandAssets})}<label>Display name<input name="brand" value="${esc(b.name)}" required maxlength="80"></label><div class="fields"><label>Accent<input type="color" name="accent" value="${b.accent}"></label><label>Background<input type="color" name="ground" value="${b.ground}"></label><label>Text<input type="color" name="ink" value="${b.ink}"></label></div><div class="fields"><label>Typography<select name="font">${options(['sans','serif','mono'],b.font)}</select></label><label>Density<select name="density">${options(['comfortable','compact'],b.density)}</select></label><label>Training units<select name="unit">${options(['kg','lb'],state.unit)}</select></label></div><h2>Model preference</h2><label>Provider<select name="provider">${options(['none','local','openai-compatible','provider-router'],m.provider)}</select></label><div class="fields"><label>Model ID<input name="model" value="${esc(m.model)}" placeholder="Model or native route ID"></label><label>Endpoint<input name="endpoint" value="${esc(m.endpoint)}" placeholder="http://127.0.0.1:11434"></label></div><label>Data allowed in a reviewed request<select name="data">${options(['program-only','selected-session'],m.data)}</select></label><p class="muted">Local execution uses the route connected to this server. Review the data before sending and the proposal before applying.</p><button>Save settings</button><button type="button" class="secondary" data-action="model.connected">Use connected model</button></form><section class="panel"><h2>Program connections</h2><p>Download a draft for your publishing system.</p>${settings.bridges.map(b=>`<div class="connection"><div><strong>${esc(b.name)}</strong><p>${esc(b.kind)} · ${esc(b.endpoint)}</p></div>${button('Export programs','bridge.export',`class="secondary" data-id="${esc(b.id)}"`)}</div>`).join('')}<form id="bridge-form"><div class="fields"><label>Name<input name="name" required maxlength="80" placeholder="My website"></label><label>System<select name="kind"><option value="wordpress">WordPress</option><option value="frappe">Frappe</option><option value="generic">Program JSON</option></select></label></div><label>Website address<input name="endpoint" type="url" required placeholder="https://studio.example"></label><button class="secondary">Add connection</button></form></section>`;}
function applyBrand(){const b=settings.brand,colors=brandColors(b);document.documentElement.style.setProperty("--accent-text",colors.accentText);document.documentElement.style.setProperty("--on-accent",colors.onAccent);document.documentElement.style.setProperty('--accent',b.accent);document.documentElement.style.setProperty('--ground',b.ground);document.documentElement.style.setProperty('--ink',b.ink);document.documentElement.style.colorScheme=colors.scheme;document.documentElement.dataset.font=b.font;document.documentElement.dataset.density=b.density;const wordmark=document.querySelector('.wordmark');wordmark.classList.toggle('logo-wordmark',!!b.logo&&b.logo.width>b.logo.height*2);wordmark.setAttribute('aria-label',b.name);const mark=document.querySelector('.mark');mark.classList.toggle('custom-mark',!!b.logo);if(b.logo){if(mark.firstElementChild?.getAttribute('src')!==b.logo.dataUrl){const img=document.createElement('img');img.src=b.logo.dataUrl;img.alt='';mark.replaceChildren(img);}}else mark.textContent='↗';const icon=document.querySelector('link[rel=icon]');icon.href=b.icon?.dataUrl??studioMarkIcon(b);icon.type=b.icon?'image/png':'image/svg+xml';$('brand-name').textContent=b.name;document.title=`${b.name} · Your training`;document.querySelector('meta[name="theme-color"]').content=b.ground;}
function render(){
  applyBrand();const tab=location.hash.slice(1)||'today';document.querySelectorAll('nav a').forEach(a=>a.setAttribute('aria-current',a.hash===`#${tab}`?'page':'false'));
  $('view').innerHTML=tab==='devices'?devicesView(state,sensor.state,{recording:recordHeartRate,pending:pendingDeviceWorkouts,bluetooth:!!navigator.bluetooth}):tab==='requests'?requestsView(creator,requestReceipts.rows(creator.id),state):tab==='train'?train():tab==='activity'?activity():tab==='programs'?heading(creator.settings.brand.name,'Programs',creator.description)+(pendingPlan?planAdoption(pendingPlan):publicPlanCards(catalogue.plans??[],state))+`${catalogue.available?`<div class="section-title"><span>${catalogue.fresh?'Studio programs are up to date':'Saved studio programs'}</span>${button('Refresh programs','catalogue.refresh','class="secondary"')}</div>`:''}<div class="cards">${creator.routines.map(r=>routineCard(r,true)).join('')}</div>`:tab==='settings'?settingsView():today();
}
async function refreshCatalogue(strict=false){catalogue=await loadCatalogue(creator.id,{strict});const base=material.creators.find(c=>c.id===creator.id);creator.routines=[...base.routines,...catalogue.programs.map(p=>({...p.routine,publicDescription:p.description}))];render();}
async function selectCreator(id){pendingPlan=null;modelAbort?.abort();assist=null;pendingBrandAssets={};creator=structuredClone(material.creators.find(c=>c.id===id)??material.creators[0]);const stored=(await readLocal(settingsKey())).find(r=>r.key===settingsKey());const shared=await loadStudioAppearance(creator.id,creator.settings.brand);creator.settings.brand=shared.brand;settings=checkSettings(stored?.value??{...structuredClone(creator.settings),followStudio:true});if(settings.followStudio)settings.brand=shared.brand;$('creator').querySelector(`option[value="${creator.id}"]`).textContent=shared.brand.name;settingsRevision=stored?.revision??0;await refreshCatalogue();}
document.addEventListener('click',e=>{const b=e.target.closest('[data-action]');if(!b)return;if(b.dataset.action==='assist.cancel'){modelAbort?.abort();assist=null;render();return;}action(async()=>{
  const type=b.dataset.action;
  if(type==='device.connect'){await sensor.connect();render();return;}
  if(type==='device.disconnect'){sensor.disconnect();render();return;}
  if(type==='device.cancel'){pendingDeviceWorkouts=null;render();return;}
  if(type==='device.import'){await commit({type:'device.import',activities:pendingDeviceWorkouts});pendingDeviceWorkouts=null;render();notice('Watch workouts added to your history.');return;}
  if(type==='device.remove'){if(confirm('Remove this watch workout from this device?'))await commit({type,id:b.dataset.id});return;}
  if(type==='device.calendar'||type==='device.calendar.download'){
    const plan=state.plans.find(p=>p.id===b.dataset.id),body=planCalendar(plan,state.history),file=new File([body],'training-plan.ics',{type:'text/calendar'});
    if(type==='device.calendar'){
      try{
        if(navigator.canShare?.({files:[file]})&&navigator.share){
          await navigator.share({files:[file],title:plan.plan.name});
          notice('Calendar shared. Follow the calendar app’s instructions to import it.');return;
        }
      }catch(error){
        if(error.name==='AbortError'){notice('Calendar sharing cancelled.');return;}
        download('training-plan.ics',body,'text/calendar');
        notice('Sharing is unavailable. Open the downloaded training-plan.ics file in your calendar app.');return;
      }
    }
    download('training-plan.ics',body,'text/calendar');
    notice('Open the downloaded training-plan.ics file in your calendar app.');return;
  }
  if(type==='catalogue.refresh'){await refreshCatalogue(true);return;}
  if(type==='request.refresh'){await requestReceipts.refresh(creator.id);render();}
  else if(type==='request.retry'){try{await requestReceipts.retry(b.dataset.id);}finally{render();}}
  else if(type==='request.adopt'){const routine=requestReceipts.delivered(b.dataset.id);if(!routine)throw Error('Refresh this request before adopting its program.');await commit({type:'routine.save',routine});}
  else if(type==='brand.remove'){pendingBrandAssets[b.dataset.kind]=null;$('brand-'+b.dataset.kind+'-preview').innerHTML=assetPreview(null,b.dataset.kind);}
  else if(type==='assist.open'){const routine=state.routines.find(r=>r.id===b.dataset.routine);assist={base:{routine:structuredClone(routine),exerciseId:b.dataset.exercise}};location.hash='train';render();}
  else if(type==='assist.back'){assist.request=null;render();}
  else if(type==='assist.send'){
    const current=assist;current.running=true;render();modelAbort=new AbortController();
    try{const response=await fetch('/api/training/propose',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(current.request),signal:modelAbort.signal});const result=await response.json();if(!response.ok)throw Error(result.error??'The model request failed.');if(assist===current){current.result=result;current.running=false;render();}}
    catch(error){if(assist===current){current.running=false;render();throw error;}}finally{modelAbort=null;}
  }
  else if(type==='assist.apply'){const routine=state.routines.find(r=>r.id===assist.base.routine.id);const next=applyAssistProposal(routine,assist.base,assist.result.proposal,assist.result.receipt);await commit({type:'routine.save',routine:next});assist=null;render();}
  else if(type==='appearance.follow'){const shared=await loadStudioAppearance(creator.id,creator.settings.brand);creator.settings.brand=shared.brand;const next=checkSettings({...settings,brand:shared.brand,followStudio:true}),saved=await saveLocal(settingsKey(),next,settingsRevision);settings=next;settingsRevision=saved.revision;pendingBrandAssets={};render();}
  else if(type==='model.connected'){const response=await fetch('/api/training/model-route');if(!response.ok)throw Error('Could not read the connected model.');const route=await response.json();if(!route)throw Error('No model endpoint is connected to this server.');const form=$('settings-form');for(const name of ['provider','model','endpoint'])form.elements[name].value=route[name];notice('Connected model selected. Save settings to use it.');}
  else if(type==='routine.new'||type==='routine.edit'){editing=routineDraft(type==='routine.edit'?state.routines.find(r=>r.id===b.dataset.id):null);location.hash='train';render();}
  else if(type.startsWith('editor.')){const edit=type.slice(7);if(edit==='cancel')editing=null;else editing=changeDraft(readEditor($('routine-editor'),editing,state.unit),edit,b.dataset.exercise,b.dataset.set);render();}
  else if(type==='plan.review'){pendingPlan=(catalogue.plans??[]).find(p=>p.plan.id===b.dataset.id)?.plan;if(!pendingPlan)throw Error('Plan not found.');render();}
  else if(type==='plan.cancel'){pendingPlan=null;render();}
  else if(type==='plan.start'){await commit({type:'start',id:uid(),enrollmentId:b.closest('[data-enrollment]').dataset.enrollment,slotId:b.dataset.slot});location.hash='train';}
  else if(type==='plan.skip'||type==='plan.restore')await commit({type,id:b.closest('[data-enrollment]').dataset.enrollment,slotId:b.dataset.slot});
  else if(type==='plan.archive'){if(confirm('Archive this plan? Completed workouts stay in your history.'))await commit({type,id:b.closest('[data-enrollment]').dataset.enrollment});}
  else if(type==='adopt'){if(state.routines.some(r=>r.id===b.dataset.id))throw Error('This program version is already in your routines.');await commit({type:'routine.save',routine:creator.routines.find(r=>r.id===b.dataset.id)});location.hash='train';}
  else if(type==='start'){await commit({type:'start',id:uid(),routineId:b.dataset.id});location.hash='train';}
  else if(type==='weight.edit'){const row=state.bodyweight.find(w=>w.day===b.dataset.day);$('weight-form').elements.day.value=row.day;$('weight-form').elements.weight.value=Math.round(fromKg(row.weightKg,state.unit)*1000)/1000;$('weight-form').scrollIntoView({block:'center'});}
  else if(type==='weight.remove')await commit({type,day:b.dataset.day});
  else if(type==='export')download('my-training.json',exportTraining(state));
  else if(type==='bridge.export'){const bridge=settings.bridges.find(x=>x.id===b.dataset.id);download(`${creator.id}-programs.${bridge.kind==='wordpress'?'xml':'json'}`,exportPrograms(creator.routines,bridge),bridge.kind==='wordpress'?'application/xml':'application/json');}
  else if(type==='import.cancel'){pendingImport=null;render();}
  else if(type==='import.apply'){const saved=await saveLocal(key,pendingImport,revision);state=pendingImport;revision=saved.revision;pendingImport=null;render();}
  else if(type==='discard'){if(confirm('Discard this unfinished workout?'))await commit({type});}
  else {await commit({type});if(type==='finish')location.hash='activity';}
});});
document.addEventListener('submit',e=>{e.preventDefault();const form=e.target;action(async()=>{
  const values=Object.fromEntries(new FormData(form));
  if(form.id==='plan-adoption'){await commit({type:'plan.adopt',id:uid(),plan:pendingPlan,startDay:values.startDay});pendingPlan=null;location.hash='train';render();}
  else if(form.id==='creator-request-form'){try{await requestReceipts.submit({creatorId:creator.id,programId:values.programId,title:values.title,details:values.details});}finally{render();}}
  else if(form.id==='assist-form'){assist.instruction=values.instruction;assist.request=buildAssistRequest(state,assist.base.routine.id,assist.base.exerciseId,values.instruction,settings.model,values.session??'');render();}
  else if(form.matches('.set-row')){const exercise=state.active.exercises.find(x=>x.id===form.dataset.exercise),set=exercise.sets.find(s=>s.id===form.dataset.set),a=Number(values.first),b=Number(values.second);const timed=exercise.kind==='timed'||exercise.kind==='cardio';await commit({type:'set',exerciseId:exercise.id,setId:set.id,values:{...(()=>{const p=exercise.prescribedSets?.find(s=>s.id===set.id);return p?.rir!=null?{rir:values.effort===''?null:Number(values.effort)}:p?.rpe!=null?{rpe:values.effort===''?null:Number(values.effort)}:{};})(),...(timed?{seconds:a,...(exercise.kind==='cardio'?{distanceM:b}:{loadKg:toKg(b,state.unit)})}:{loadKg:toKg(a,state.unit),reps:b}),done:!set.done}});}
  else if(form.id==='weight-form'){const old=(state.bodyweight??[]).find(w=>w.day===values.day);const kg=old&&values.weight===String(Math.round(fromKg(old.weightKg,state.unit)*1000)/1000)?old.weightKg:toKg(Number(values.weight),state.unit);await commit({type:'weight.save',day:values.day,weightKg:kg});}
  else if(form.id==='routine-editor'){const draft=finishDraft(readEditor(form,editing,state.unit));const next=trainingCommand(state,{type:'routine.save',routine:draft}),saved=await saveLocal(key,next,revision);state=next;revision=saved.revision;editing=null;render();}
  else if(form.id==='settings-form'){const next=checkSettings({...settings,brand:{...settings.brand,...pendingBrandAssets,name:values.brand,accent:values.accent,ground:values.ground,ink:values.ink,font:values.font,density:values.density},model:{provider:values.provider,model:values.model,endpoint:values.endpoint,data:values.data}});next.followStudio=!!settings.followStudio&&JSON.stringify(next.brand)===JSON.stringify(settings.brand);const saved=await saveLocal(settingsKey(),next,settingsRevision);settings=next;settingsRevision=saved.revision;pendingBrandAssets={};await commit({type:'unit',unit:values.unit});}
  else if(form.id==='bridge-form'){const next=checkSettings({...settings,bridges:[...settings.bridges,{id:uid(),name:values.name,kind:values.kind,endpoint:values.endpoint,scope:'programs',direction:'export'}]});const saved=await saveLocal(settingsKey(),next,settingsRevision);settings=next;settingsRevision=saved.revision;render();}
});});
document.addEventListener('change',e=>{if(e.target.id==='record-heart-rate'){recordHeartRate=e.target.checked;heartRateSince=Date.now();render();return;}if(e.target.id==='device-workout-file'){action(async()=>{const file=e.target.files[0];if(!file)return;if(file.size>8*1024*1024)throw Error('Choose a workout file smaller than 8 MB.');pendingDeviceWorkouts=null;render();pendingDeviceWorkouts=await readTCX(await file.text());render();});return;}if(e.target.dataset.brandAsset)action(async()=>{const file=e.target.files[0];if(!file)return;const kind=e.target.dataset.brandAsset;pendingBrandAssets[kind]=await importBrandAsset(file,kind);$('brand-'+kind+'-preview').innerHTML=assetPreview(pendingBrandAssets[kind],kind);});if(e.target.matches('#routine-editor select[name=kind],#routine-editor select[name=progression]'))action(async()=>{editing=readEditor($('routine-editor'),editing,state.unit);render();});if(e.target.id==='import-file')action(async()=>{const file=e.target.files[0];if(!file)return;if(file.size>20*1024*1024)throw Error('Training file exceeds 20 MB.');const raw=await file.text();if(JSON.parse(raw).schema==='bonfyre.training.v1'){pendingImport=importTraining(raw,state.member);importNotes=[];}else{const imported=importOpenGym(raw,state.member);pendingImport=imported.state;importNotes=imported.notes;}render();});});
$('creator').onchange=e=>action(()=>selectCreator(e.target.value));
window.onhashchange=()=>{if(state){render();if(location.hash==='#programs')action(()=>refreshCatalogue());if(location.hash==='#requests')action(async()=>{await requestReceipts.refresh(creator.id);render();});}};
setInterval(()=>{
  updateSensor();const reading=currentHeartRate(sensor.state);
  if(recordHeartRate&&state?.active?.pausedAt===null&&reading&&reading.at>=heartRateSince&&!busy&&Date.now()-lastHeartSave>=5000&&reading.at>(state.active.heartRate?.at(-1)?.at??0)){
    lastHeartSave=Date.now();void action(()=>commit({type:'heart-rate',workoutId:state.active.id,sample:{...reading,device:sensor.state.name}},{refresh:false}));
  }
  if(state?.active){if($('elapsed'))$('elapsed').textContent=duration(elapsedSeconds(state.active));if($('rest'))$('rest').textContent=restSeconds(state.active)?`Rest · ${duration(restSeconds(state.active))}`:'Ready for your next set';}},1000);
await action(async()=>{
  const response=await fetch('/training.json');if(!response.ok)throw Error('Could not open the training collection.');material=await response.json();
  const stored=(await readLocal(key)).find(r=>r.key===key);state=checkTraining(stored?.value??newTraining(`member:${uid()}`));revision=stored?.revision??0;
  if(!stored){const saved=await saveLocal(key,state,0);revision=saved.revision;}
  requestReceipts=await openRequestReceipts(state.member);
  $('date').textContent=new Date().toLocaleDateString(undefined,{weekday:'long',month:'long',day:'numeric'});
  $('creator').innerHTML=material.creators.map(c=>`<option value="${esc(c.id)}">${esc(c.settings.brand.name)}</option>`).join('');
  $('save-state').textContent='Saved on this device';await selectCreator(material.creators[0].id);
  void enableOffline();
});
