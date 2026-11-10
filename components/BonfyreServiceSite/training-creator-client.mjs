import {workspaceRoute,workspacePath,workspaceLinks} from './workspace-navigation.mjs';
import {queueState,queueSearch,queueRows} from './workspace-queue.mjs';
import {creatorViews,accountView,teamView,requestListView} from './training-workspace-views.mjs';
import {newPlanDraft,readPlanEditor,planEditorView,planLibraryView} from "./training-plan-editor.mjs";
import {appearanceView} from "./training-appearance.mjs";
import {checkSettings,defaultSettings} from "./training-settings.mjs";
import {routineDraft,editorHTML,readEditor,changeDraft,finishDraft} from './training-editor.mjs';
import {brandColors} from './training-settings.mjs';
import {importBrandAsset,studioMarkIcon} from './training-brand-assets.mjs';
import {publicationView,studioProgramsView} from './training-publication-view.mjs';
const $=selector=>document.querySelector(selector),esc=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const root=$('#creator-view');let session=null,studio='',rows=[],draft=null,editing=null,review=false,busy=false;
let revision=0,savedAt=null,dirty=false,changeNumber=0,conflict=false,saveTimer=null,sourceProgram=null;
let appearance=null,appearanceDirty=false,planEditor=null,planChoices=[],planReleases=[],planDrafts=[],planWithdrawal=null,planChanges=0;
let mode='requests',programs=[],libraryDrafts=[],draftKind='requests',publication=null,withdrawal=null;
let team=null,teamScope='',teamPending=null,teamQuery='',teamRole='all',requestView=queueState('');
let currentPath=location.pathname+location.search+location.hash;
async function api(path,body){const response=await fetch('/api/training/creator'+path,{method:body===undefined?'GET':'POST',credentials:'same-origin',headers:body===undefined?{}:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body,(_key,value)=>{if(typeof value==='number'&&!Number.isFinite(value))throw Error('Enter a valid numeric target before saving.');return value;}),cache:'no-store'});const result=await response.json();if(!response.ok){if(response.status===401||response.status===403){session=null;clearWorkspace();render();}throw Object.assign(Error(result.error),{status:response.status});}return result;}
const endpoint=id=>`/studios/${studio}/requests${id?'/'+id:''}`;
const draftEndpoint=id=>draftKind==='library'?`/studios/${studio}/program-drafts/${id}`:endpoint(id)+'/draft';
function notice(message){$('#notice').hidden=!message;$('#notice').textContent=message;}
function clearDraft(){clearTimeout(saveTimer);draft=null;editing=null;review=false;revision=0;savedAt=null;dirty=false;conflict=false;sourceProgram=null;draftKind='requests';}
function draftStatus(){
  if($('#draft-state'))$('#draft-state').textContent=conflict?'Another tab saved changes.':dirty?'Changes not yet saved':savedAt?'Saved '+new Date(savedAt).toLocaleTimeString(undefined,{hour:'numeric',minute:'2-digit'}):'Draft not yet saved';
  if($('#draft-conflict'))$('#draft-conflict').hidden=!conflict;
  if($('[data-action="draft.source"]'))$('[data-action="draft.source"]').hidden=dirty||revision>0||!sourceProgram;
}
function scheduleSave(){clearTimeout(saveTimer);saveTimer=setTimeout(()=>{if(!editing||!dirty||conflict)return;if(busy){scheduleSave();return;}act(saveDraft);},700);}
function changed(){dirty=true;changeNumber++;draftStatus();scheduleSave();}
async function saveDraft(){
  clearTimeout(saveTimer);if(!editing||(!dirty&&revision>0))return;
  if(conflict)throw Error('Load the saved draft before making further changes. You can download your current edits first.');
  const id=editing,changedAtStart=changeNumber;
  if($('#routine-editor'))draft=readEditor($('#routine-editor'),draft,'kg');
  const result=await api(draftEndpoint(id),{routine:draft,expectedRevision:revision});
  if(editing!==id)return;
  revision=result.revision;savedAt=result.savedAt;dirty=changeNumber!==changedAtStart;
  if(!dirty)draft=result.routine;
  draftStatus();if(dirty)scheduleSave();
}
async function openDraft(id){const saved=await api(endpoint(id)+'/draft');clearDraft();editing=id;draft=saved.routine??routineDraft();revision=saved.revision;savedAt=saved.savedAt;sourceProgram=saved.sourceProgram;render();}
async function openLibraryDraft(id){const saved=await api(`/studios/${studio}/program-drafts/${id}`);clearDraft();draftKind='library';editing=id;draft=saved.routine;revision=saved.revision;savedAt=saved.savedAt;render();}
async function newLibraryDraft(routine){await saveBeforeLeaving();clearDraft();draftKind='library';editing=crypto.randomUUID();draft=routineDraft(routine);dirty=true;await saveDraft();render();}
async function saveBeforeLeaving(){if(planEditor?.dirty){await savePlanDraft();if(planEditor.dirty)throw Error('Your plan changed while saving. Save again before leaving.');}if(appearanceDirty)throw Error('Apply the appearance or reload the studio style before leaving.');if(dirty)await saveDraft();if(dirty)throw Error('Your edits changed while saving. Wait for them to save before leaving.');}
function render(){
  $('#sign-out').hidden=!session;$('#studio').hidden=!session?.studios.length;$('#actor-label').textContent=session?session.profile.name||session.profile.email:'';
  $('#workspace-nav').innerHTML='';$('#view-title').textContent='Studio workspace';
  if(!session){root.innerHTML='<div class="heading"><p class="eyebrow">Creator access</p><h1>Sign in</h1><p class="lede">Open your studio workspace.</p></div><form id="creator-login" class="creator-login"><label>Email<input type="email" name="email" autocomplete="username" required></label><label>Password<input type="password" name="password" autocomplete="current-password" required></label><button>Sign in</button></form>';return;}
  const selected=session.studios.find(c=>c.id===studio);$('#brand-name').textContent=selected?.name??'Creator studio';
  $('#workspace-nav').innerHTML=workspaceLinks('/creator',selected,creatorViews,mode);
  $('#view-title').textContent=creatorViews.find(v=>v.id===mode)?.label??'Studio workspace';
  if(selected){const b=selected.brand,colors=brandColors(b);for(const [key,value] of Object.entries({accent:b.accent,ground:b.ground,ink:b.ink,'accent-text':colors.accentText,'on-accent':colors.onAccent}))document.documentElement.style.setProperty('--'+key,value);document.documentElement.style.colorScheme=colors.scheme;document.documentElement.dataset.font=b.font;document.documentElement.dataset.density=b.density;document.title=b.name+' · Creator studio';const icon=$('link[rel="icon"]');icon.href=b.icon?.dataUrl??studioMarkIcon(b);const mark=$('.mark');mark.classList.toggle('custom-mark',!!b.logo);$('.wordmark').classList.toggle('logo-wordmark',!!b.logo&&b.logo.width>b.logo.height*2);if(b.logo){const img=document.createElement('img');img.src=b.logo.dataUrl;img.alt='';mark.replaceChildren(img);}else mark.textContent='↗';}
  if(!selected){root.innerHTML='<div class="heading"><h1>Your studio access</h1><p class="lede">Your account has no studio permissions. Ask a studio administrator to add you.</p></div>';return;}
  if(mode==='account'){root.innerHTML=accountView(session,selected);return;}
  if(mode==='team'){root.innerHTML=teamView(team,selected,teamScope,session,teamPending,{query:teamQuery,role:teamRole});return;}
  if(mode==='appearance'&&appearance){root.innerHTML=appearanceView(appearance,selected.canEditAppearance);return;}
  if(planEditor){root.innerHTML=planEditorView(planEditor,planChoices,planReleases);return;}
  if(publication){root.innerHTML=publicationView(publication,selected);return;}
  if(mode==='programs'&&!editing){root.innerHTML=studioProgramsView(programs,selected,withdrawal,libraryDrafts)+(session.plansAvailable?planLibraryView(planReleases,planDrafts,selected.canPublishPrograms,planWithdrawal):'');return;}
  if(editing){
    const row=draftKind==='library'?{brief:{title:'Build your next program.',details:'Save it to the studio library, then review its public release.'}}:rows.find(r=>r.id===editing);
    root.innerHTML=`<div class="heading"><p class="eyebrow">Program delivery</p><h1>${esc(row.brief.title)}</h1><p class="lede">${esc(row.brief.details)}</p></div><div class="draft-toolbar"><span id="draft-state" role="status"></span>${!review?'<button class="secondary" data-action="draft.save">Save draft</button>':''}</div><div id="draft-conflict" class="draft-conflict" hidden><p>Your current edits are still here. Download them before loading the version saved in another tab.</p><button class="secondary" data-action="draft.download">Download my edits</button><button class="secondary" data-action="draft.reload">Load saved draft</button></div>`;
    if(review){root.innerHTML+=`<section class="creator-review"><p class="eyebrow">Ready for the member</p><h2>${esc(draft.name)}</h2><ul>${draft.exercises.map(e=>`<li><strong>${esc(e.name)}</strong><small>${e.sets.length} sets · ${e.restSeconds}s rest</small>${e.sets.map(s=>`<small>${esc(s.type)}: ${e.kind==='timed'?s.seconds+' seconds':e.kind==='cardio'?s.distanceM+' m / '+s.seconds+' seconds':s.reps+' reps · '+s.loadKg+' kg'}</small>`).join('')}</li>`).join('')}</ul><p>The member will receive this routine and can add it to their training.</p><footer><button data-action="publish">Deliver routine</button><button class="secondary" data-action="back">Keep editing</button></footer></section>`;}
    else{root.innerHTML+=`${sourceProgram?`<button class="secondary" data-action="draft.source">Start from ${esc(sourceProgram.name)}</button>`:''}`+editorHTML(draft,'kg',{showSourceNote:false});$('#routine-editor button[type="submit"]').textContent=draftKind==='library'?'Prepare publication':'Review delivery';$('[data-action="editor.cancel"]').textContent=draftKind==='library'?'Back to library':'Back to requests';}
    draftStatus();
    return;
  }
  root.innerHTML=requestListView(rows,session,selected,requestView);
}
function writeRoute(replace=false){
  const suffix=mode==='requests'?queueSearch(requestView):mode==='team'&&teamScope?'?scope='+encodeURIComponent(teamScope):'';
  const path=studio?workspacePath('/creator',studio,mode)+suffix:'/creator';
  if(location.pathname+location.search+location.hash!==path)history[replace?'replaceState':'pushState'](null,'',path);
  currentPath=path;
}
function clearWorkspace(){
  clearDraft();planEditor=null;planChoices=[];planReleases=[];planDrafts=[];planWithdrawal=null;
  appearance=null;appearanceDirty=false;programs=[];libraryDrafts=[];rows=[];publication=null;withdrawal=null;
  team=null;teamScope='';teamPending=null;teamQuery='';teamRole='all';requestView=queueState('');
}
async function refresh(){
  const selected=session?.studios.find(c=>c.id===studio);if(!selected)return render();
  if(!creatorViews.find(v=>v.id===mode)?.available(selected))mode=creatorViews.find(v=>v.available(selected)).id;
  if(mode==='team'){
    if(!selected.adminScopes.includes(teamScope))teamScope=selected.adminScopes[0];
    team=await api(`/studios/${studio}/team/${teamScope}`);
  }else if(mode==='appearance'){appearance={...await api(`/studios/${studio}/appearance`),review:false};appearanceDirty=false;}
  else if(mode==='programs'){programs=await api(`/studios/${studio}/programs`);libraryDrafts=await api(`/studios/${studio}/program-drafts`);if(session.plansAvailable){planReleases=await api(`/studios/${studio}/plans`);planDrafts=await api(`/studios/${studio}/plan-drafts`);}}
  else if(mode==='requests'){rows=await api(endpoint());if(!requestView.selected)requestView.selected=queueRows(rows,requestView,session.worker)[0]?.id??'';}
  writeRoute(true);render();$('#connection').textContent='Up to date';
}
async function signedIn(){
  session=await api('/session');
  const route=workspaceRoute(new URL(location.href),'/creator',session.studios,creatorViews);
  studio=route.workspace?.id??'';mode=route.view?.id??'account';readViewState(new URL(location.href));
  $('#studio').innerHTML=session.studios.map(c=>`<option value="${esc(c.id)}">${esc(c.name)}</option>`).join('');$('#studio').value=studio;
  if(studio)await refresh();else render();
}
function readViewState(url){requestView=queueState(url.search);teamScope=url.searchParams.get('scope')??'';}
function updateQueue(patch,{replace=false}={}){requestView={...requestView,...patch};if(!Object.hasOwn(patch,'selected'))requestView.selected=queueRows(rows,requestView,session.worker)[0]?.id??'';writeRoute(replace);render();}
async function navigate(nextStudio,nextMode,{replace=false,url=null}={}){
  await saveBeforeLeaving();
  session=await api('/session');
  $('#studio').innerHTML=session.studios.map(c=>`<option value="${esc(c.id)}">${esc(c.name)}</option>`).join('');
  if(!session.studios.some(w=>w.id===nextStudio)){clearWorkspace();studio=session.studios[0]?.id??'';mode='account';$('#studio').value=studio;writeRoute(true);render();throw Error('This account no longer has access to that studio.');}
  clearWorkspace();studio=nextStudio;mode=nextMode;if(url)readViewState(url);$('#studio').value=studio;
  writeRoute(replace);await refresh();root.focus();
}

async function act(fn){if(busy)return;busy=true;notice('');$('#connection').textContent='Working…';try{await fn();$('#connection').textContent=session?'Up to date':'Signed out';}catch(error){if(error.status===409&&planEditor){if($('#plan-editor'))planEditor.plan=readPlanEditor($('#plan-editor'),planEditor.plan);planEditor.conflict=true;planEditor.payload=null;render();}if(error.status===409&&editing){conflict=true;clearTimeout(saveTimer);draftStatus();}notice(error.message);$('#connection').textContent='Needs attention';}finally{busy=false;}}
document.addEventListener('submit',event=>{event.preventDefault();act(async()=>{if(event.target.id==='request-search'){updateQueue({query:new FormData(event.target).get('query').trim()});}else if(event.target.id==='team-search'){const data=new FormData(event.target);teamQuery=data.get('query').trim();teamRole=data.get('role');render();}else if(event.target.id==='team-add'||event.target.classList.contains('team-edit')){const data=new FormData(event.target);teamPending={email:event.target.closest('[data-member]')?.dataset.member??data.get('email').trim(),role:data.get('role')};teamPending.before=team.members.find(m=>m.email.toLowerCase()===teamPending.email.toLowerCase())??null;render();$('.team-review')?.focus();}else if(event.target.id==='creator-login'){const data=new FormData(event.target);await api('/login',{email:data.get('email'),password:data.get('password')});event.target.reset();await signedIn();}else if(event.target.id==='plan-editor'){const choice=event.target.elements.namedItem('release').value;await savePlanDraft();if(planEditor.dirty)throw Error('Save the latest changes before reviewing.');const old=planReleases.find(p=>p.key===choice);planEditor.payload={publicationId:crypto.randomUUID(),planId:old?.key??crypto.randomUUID(),expectedVersion:old?.version??0,draftId:planEditor.id,draftRevision:planEditor.revision};render();}else if(event.target.id==='appearance-form'){readAppearance();checkSettings({...defaultSettings(),brand:appearance.brand});appearance.review=true;render();}else if(event.target.id==='routine-editor'){draft=finishDraft(readEditor(event.target,draft,'kg'));await saveDraft();if(dirty)throw Error('Your edits changed while saving. Review them again.');if(draftKind==="library"){publication={draftId:editing,draftRevision:revision,routine:structuredClone(draft),title:draft.name,description:"",programId:"new",library:await api(`/studios/${studio}/programs`),payload:null};}else review=true;render();}else if(event.target.id==='publication-form'){const data=new FormData(event.target),key=data.get('program'),previous=publication.library.find(p=>p.key===key);publication.title=data.get('title').trim();publication.description=data.get('description').trim();publication.programId=previous?.key??'new';publication.payload={publicationId:crypto.randomUUID(),programId:previous?.key??crypto.randomUUID(),expectedVersion:previous?.version??0,...(publication.draftId?{draftId:publication.draftId,draftRevision:publication.draftRevision}:{requestId:publication.requestId}),title:publication.title,description:publication.description};render();}});});
document.addEventListener('click',event=>{const button=event.target.closest('[data-action],[data-workspace],#sign-out');if(!button||(!button.dataset.action&&!button.dataset.workspace&&button.id!=='sign-out'))return;if(button.tagName==='A')event.preventDefault();act(async()=>{
  const action=button.dataset.action,id=button.closest('[data-request]')?.dataset.request;
  if(button.dataset.workspace){await navigate(button.dataset.workspace,button.dataset.view??'account');return;}
  if(action==='requests.filter'){updateQueue({filter:button.dataset.filter});return;}
  if(action==='requests.layout'){updateQueue({layout:button.dataset.layout,selected:requestView.selected});return;}
  if(action==='requests.select'){updateQueue({selected:id});$('.request-inspector')?.scrollIntoView({block:'nearest'});return;}
  if(action==='requests.close'){const old=requestView.selected;updateQueue({selected:''});document.querySelector(`.queue-row[data-request="${old}"]`)?.focus();return;}
  if(action==='requests.reset'){updateQueue({query:'',filter:'all'});return;}
  if(action==='team.more'){if(team?.next_user_id){const next=await api(`/studios/${studio}/team/${teamScope}?after=${team.next_user_id}`);team={...next,members:[...team.members,...next.members]};render();}return;}
  if(action==='team.refresh'){teamPending=null;await refresh();return;}
  if(action==='team.cancel'){teamPending=null;render();return;}
  if(action==='team.confirm'){if(!teamPending)throw Error('Review the change first.');await api(`/studios/${studio}/team/${teamScope}`,{email:teamPending.email,role:teamPending.role,expected:teamPending.before?{role:teamPending.before.role,expires_at:teamPending.before.expires_at}:null});teamPending=null;await refresh();notice('Team access updated.');return;}
  if(button.id==='sign-out'){await saveBeforeLeaving();await api('/logout',{});session=null;clearWorkspace();render();return;}
  if(action?.startsWith('plan.')){await planAction(action,button);return;}
  if(action==='refresh'||action==='requests.refresh')await refresh();
  if(action==='appearance.reload')await refresh();
  if(action==='appearance.edit'){appearance.review=false;render();}
  if(action==='appearance.apply'){if(!appearance.review)throw Error('Preview the appearance before applying it.');await api(`/studios/${studio}/appearance`,{brand:appearance.brand,expectedRevision:appearance.revision});appearanceDirty=false;await signedIn();notice('Studio appearance applied.');}
  if(action==='brand.remove'&&mode==='appearance'){readAppearance();appearance.brand[button.dataset.kind]=null;appearance.review=false;appearanceDirty=true;render();}
  if(action?.startsWith('mode.')){await navigate(studio,action.slice(5));return;}
  if(action==='publication.open'){const row=rows.find(r=>r.id===id);const library=await api(`/studios/${studio}/programs`);publication={requestId:id,routine:row.delivery.routine,title:row.delivery.routine.name,description:'',programId:'new',library,payload:null};render();}
  if(action==='publication.cancel'){publication=null;render();}
  if(action==='publication.back'){publication.payload=null;publication.library=await api(`/studios/${studio}/programs`);render();}
  if(action==='publication.confirm'){await api(`/studios/${studio}/programs`,publication.payload);clearDraft();publication=null;mode='programs';writeRoute();await refresh();}
  if(action==='publication.withdraw'){withdrawal=Number(button.closest('[data-release]').dataset.release);render();}
  if(action==='publication.withdraw-cancel'){withdrawal=null;render();}
  if(action==='publication.withdraw-confirm'){await api(`/studios/${studio}/programs/${withdrawal}/withdraw`,{});withdrawal=null;await refresh();}
  if(action==='accept'||action==='renew'){await api(endpoint(id)+'/'+action,{});await refresh();}
  if(action==='build')await openDraft(id);
  if(action==='library.new')await newLibraryDraft();
  if(action==='library.open')await openLibraryDraft(button.closest('[data-draft]').dataset.draft);
  if(action==='library.copy')await newLibraryDraft(programs.find(p=>p.releaseId===Number(button.closest('[data-release]').dataset.release)).routine);
  if(action==='back'){review=false;render();}
  if(action==='publish'){await api(endpoint(editing)+'/deliver',{revision});clearDraft();await refresh();}
  if(action==='draft.save')await saveDraft();
  if(action==='draft.reload'){publication=null;if(draftKind==='library')await openLibraryDraft(editing);else await openDraft(editing);}
  if(action==='draft.source'){draft=routineDraft(sourceProgram);changed();render();}
  if(action==='draft.download'){const value=$('#routine-editor')?readEditor($('#routine-editor'),draft,'kg'):draft,url=URL.createObjectURL(new Blob([JSON.stringify(value,null,2)+'\n'],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='routine-draft.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
  if(action?.startsWith('editor.')){if(action==='editor.cancel'){await saveBeforeLeaving();clearDraft();await refresh();}else{draft=changeDraft(readEditor($('#routine-editor'),draft,'kg'),action.slice(7),button.dataset.exercise,button.dataset.set);changed();render();}}
});});
async function savePlanDraft(){
  if(!planEditor)return;if(planEditor.conflict)throw Error("Load the saved plan before saving. Download your edits first if needed.");
  if($("#plan-editor"))planEditor.plan=readPlanEditor($("#plan-editor"),planEditor.plan);
  const changed=planChanges,result=await api(`/studios/${studio}/plan-drafts/${planEditor.id}`,{plan:planEditor.plan,expectedRevision:planEditor.revision});
  planEditor.revision=result.revision;planEditor.dirty=changed!==planChanges;if(!planEditor.dirty)planEditor.plan=result.plan;
  if($("#plan-save-state"))$("#plan-save-state").textContent=planEditor.dirty?"Changes not yet saved":"Saved draft · revision "+result.revision;
}
async function openPlan(id){const saved=await api(`/studios/${studio}/plan-drafts/${id}`);planChoices=await api(`/studios/${studio}/plan-routines`);planEditor={...saved,dirty:false,conflict:false,payload:null};render();}
async function planAction(action,button){
  if(action==="plan.new"){await saveBeforeLeaving();const plan=newPlanDraft();planEditor={id:plan.id,revision:0,plan,dirty:true,conflict:false,payload:null};planChoices=await api(`/studios/${studio}/plan-routines`);await savePlanDraft();render();return;}
  if(action==="plan.open"){await openPlan(button.closest("[data-plan-draft]").dataset.planDraft);return;}
  if(action==="plan.withdraw"){planWithdrawal=Number(button.closest("[data-plan-release]").dataset.planRelease);render();return;}
  if(action==="plan.withdraw-cancel"){planWithdrawal=null;render();return;}
  if(action==="plan.withdraw-confirm"){await api(`/studios/${studio}/plans/${planWithdrawal}/withdraw`,{});planWithdrawal=null;await refresh();return;}
  if(!planEditor)throw Error("Open a plan first.");
  if(action==="plan.reload"){await openPlan(planEditor.id);return;}
  if(action==="plan.publish"){await api(`/studios/${studio}/plans`,planEditor.payload);planEditor=null;await refresh();return;}
  if(action==="plan.edit"){planEditor.payload=null;render();return;}
  if(action==="plan.close"){await saveBeforeLeaving();planEditor=null;await refresh();return;}
  if(action==="plan.save"){await savePlanDraft();return;}
  if($("#plan-editor"))planEditor.plan=readPlanEditor($("#plan-editor"),planEditor.plan);
  const p=planEditor.plan,index=Number(button.dataset.weekIndex),week=p.weeks[index];
  if(action==="plan.download"){const url=URL.createObjectURL(new Blob([JSON.stringify(p,null,2)],{type:"application/json"})),a=document.createElement("a");a.href=url;a.download="plan-draft.json";a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;}
  if(action==="plan.week-add"||action==="plan.week-copy"){if(p.weeks.length>=52)throw Error("A plan supports up to 52 weeks.");p.weeks.push(action==="plan.week-copy"?{name:week.name,sessions:week.sessions.map(s=>({...s,id:crypto.randomUUID()}))}:{name:"Next phase",sessions:[]});}
  else if(action==="plan.week-remove"){if(p.weeks.length<2)throw Error("Keep at least one week.");p.weeks.splice(index,1);}
  else if(action==="plan.session-add"){if(week.sessions.length>=14)throw Error("A week supports up to 14 sessions.");const routine=planChoices[0]??p.routines[0];if(!routine)throw Error("Create a routine before adding sessions.");week.sessions.push({id:crypto.randomUUID(),day:1,routineId:routine.id});}
  else if(action==="plan.session-remove")week.sessions=week.sessions.filter(s=>s.id!==button.dataset.slot);
  else throw Error("Unknown plan action.");planEditor.dirty=true;planChanges++;render();
}
function readAppearance(){const form=$('#appearance-form');if(!form)return;const v=Object.fromEntries(new FormData(form));appearance.brand={...appearance.brand,name:v.brand,accent:v.accent,ground:v.ground,ink:v.ink,font:v.font,density:v.density};}
function appearanceChanged(){appearanceDirty=true;if(appearance){appearance.review=false;if($('[data-action="appearance.apply"]'))$('[data-action="appearance.apply"]').disabled=true;}}
document.addEventListener('input',event=>{if(event.target.closest('#plan-editor')){planEditor.dirty=true;planChanges++;if($('#plan-save-state'))$('#plan-save-state').textContent='Changes not yet saved';}if(event.target.closest('#appearance-form'))appearanceChanged();if(event.target.closest('#routine-editor'))changed();});
document.addEventListener('change',event=>{if(event.target.dataset.brandAsset&&mode==='appearance'){act(async()=>{const file=event.target.files[0];if(!file)return;readAppearance();appearance.brand[event.target.dataset.brandAsset]=await importBrandAsset(file,event.target.dataset.brandAsset);appearanceChanged();render();});return;}if(event.target.closest('#appearance-form'))appearanceChanged();if(event.target.id==='request-sort'){act(()=>updateQueue({sort:event.target.value}));}else if(event.target.id==='team-scope'){const next=event.target.value;act(async()=>{teamScope=next;teamPending=null;teamQuery='';teamRole='all';await refresh();});}else if(event.target.id==='studio'){const next=event.target.value;$('#studio').value=studio;act(()=>navigate(next,mode));}else if(event.target.closest('#routine-editor')&&['kind','progression','effort'].includes(event.target.name)){draft=readEditor($('#routine-editor'),draft,'kg');changed();render();}});
window.addEventListener('popstate',()=>{const requested=new URL(location.href);history.replaceState(null,'',currentPath);if(busy){notice('Wait for the current action to finish before navigating.');return;}act(async()=>{if(!session)return;const route=workspaceRoute(requested,'/creator',session.studios,creatorViews);await navigate(route.workspace?.id??'',route.view?.id??'account',{replace:true,url:requested});});});
document.addEventListener('keydown',event=>{const row=event.target.closest?.('.queue-row');if(!row||!['ArrowDown','ArrowUp','Home','End'].includes(event.key))return;event.preventDefault();if(busy)return;const buttons=[...document.querySelectorAll('.queue-row')],index=buttons.indexOf(row),next=event.key==='Home'?0:event.key==='End'?buttons.length-1:Math.max(0,Math.min(buttons.length-1,index+(event.key==='ArrowDown'?1:-1)));buttons[next]?.focus();});
window.addEventListener('beforeunload',event=>{if(dirty||appearanceDirty||planEditor?.dirty){event.preventDefault();event.returnValue='';}});
window.addEventListener('pagehide',()=>{root.replaceChildren();session=null;clearWorkspace();});
window.addEventListener('pageshow',event=>{if(event.persisted)act(signedIn);});
render();act(signedIn);
