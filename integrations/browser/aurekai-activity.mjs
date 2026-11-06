import {readDestinations,readActivity,readTasks,readAgents,readCompute,readAccountActivity,updateActivityNotification,sameActivityBinding,workTheme,workThemeIcons} from './aurekai-activity-source.mjs';
import * as inboxSource from './aurekai-inbox-source.mjs';
import {readPushStatus,currentPushDevice,enablePush,disablePush,resumePushFromLocation} from './aurekai-push.mjs';

const element=(tag,text)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=String(text);return node;};
const button=(text,action,iconName)=>{const node=element('button',text);if(iconName)node.dataset.akIcon=iconName;node.type='button';node.addEventListener('click',action);return node;};
const contextPattern=/^(self|(institution|household):[a-z0-9-]{1,63})$/;
const phases={planned:'Ready for review',submitting:'Running',completed:'Completed',refused:'Stopped',uncertain:'Outcome unconfirmed',pending:'Queued',running:'Running',blocked:'Needs attention',waiting:'Waiting',failed:'Failed',cancelled:'Cancelled',done:'Completed'};
const noticeTitles={planned:'Plan ready for review',completed:'Research finished',refused:'Research needs attention',uncertain:'Check the saved outcome'};
const activeStates=new Set(['submitting','running','active']);
const attentionStates=new Set(['planned','blocked','waiting','refused','failed','uncertain']);
const completedStates=new Set(['completed','done','succeeded']);
const validTime=value=>Number.isSafeInteger(value)&&value>=0&&value<=8640000000000000;
const date=value=>validTime(value)?new Date(value).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}):'Time not supplied';
const workKey=row=>row.workKey||(row.kind==='task'?'task':'research')+':'+row.id;
const phaseNames={pending:'Queued',active:'Running',attention:'Needs attention',completed:'Completed',cancelled:'Cancelled'};
const stateLabel=row=>row.kind==='task'||row.operatorId||row.workerId?phaseNames[row.phase]||phases[row.state]||row.state:phases[row.state]||row.state;
const stepNames={architecture_atlas:'Check architecture',project_search:'Inspect project',project_read:'Read source',project_tree:'Inspect project structure',project_patch_preview:'Review change',project_apply_low_risk_patch:'Apply safe change',project_apply_patch:'Apply approved change',project_test_profile:'Run verification',project_diff:'Review project changes',native_tool_invoke:'Run native owner',operator_goal_add:'Add work item',operator_status:'Check work queue',task_status:'Check task',matrix_address_resolve:'Resolve address',cms_record:'Update content'};
const stepLabel=value=>{const raw=String(value||'Work step'),key=raw.includes('__')?raw.split('__').at(-1):raw;if(stepNames[key])return stepNames[key];if(/^[a-z][a-z0-9_.:-]{1,127}$/.test(key))return key.replace(/[_.:-]+/g,' ').replace(/\b[a-z]/g,c=>c.toUpperCase());return raw;};
const isActive=row=>row.kind==='task'?row.phase==='active'||row.counts?.active>0||row.counts?.activeWorkers>0||row.workers?.some(worker=>worker.phase==='active'):activeStates.has(row.state);
const plural=(n,one,many=one+'s')=>n+' '+(n===1?one:many);
function localLink(text,value){
 const url=new URL(value,location.origin);
 if(url.origin!==location.origin||!['http:','https:'].includes(url.protocol)||url.username||url.password||!/^\/(?:sources\/web$|work-board$|inbox$|apps(?:\/|$)|programs$|s\/models\/all$)/.test(url.pathname))throw Error('The activity address is not an Aurekai work view.');
 const node=element('a',text);node.href=url.pathname+url.search+url.hash;return node;
}
function signInLink(){const a=element('a','Sign in');a.href='/session/continue?'+new URLSearchParams({next:location.pathname+location.search+location.hash});return a;}

function time(value){const node=element('time',date(value));if(validTime(value))node.dateTime=new Date(value).toISOString();return node;}
function fixedError(error,fallback){return error?.status===401||error?.status===403?'Current access to this activity is unavailable.':fallback;}

// Each native source resolves current access on every request. The rail does
// not acquire work or store account state; recipient actions return to their owner.
export function mountAurekaiActivity({destinations=readDestinations,activity=readActivity,tasks=readTasks,agents=readAgents,compute=readCompute,inbox=inboxSource.readInbox,needs=inboxSource.readAccountNeeds,setNotice=inboxSource.setInboxState,accountActivity=readAccountActivity,notificationAction=updateActivityNotification,providerView=()=>import('./extension/foreign-sign-in.mjs'),modelView=()=>import('./aurekai-model.mjs'),pushStatus=readPushStatus,pushDevice=currentPushDevice,pushEnable=enablePush,pushDisable=disablePush,pushResume=resumePushFromLocation,intervalMs=30000}={}){
 if(document.querySelector('[data-aurekai-activity]'))return null;
 const workspace=document.querySelector('[data-native-workspace][data-activity-context]');
 const rail=element('aside');rail.className='ak-work-rail';rail.dataset.aurekaiActivity='';rail.setAttribute('aria-label','Aurekai activity and inbox');
 const launch=button('',()=>{automaticLayout=false;pinned=!open||!pinned;setOpen(pinned,{focus:false});});launch.className='ak-rail-launch';launch.setAttribute('aria-expanded','false');launch.setAttribute('aria-controls','ak-rail-cards');
 const mark=element('span','A');mark.className='ak-rail-mark';mark.setAttribute('aria-hidden','true');const launchCopy=element('span'),brand=element('strong','Aurekai'),launchState=element('small','Your activity');launchCopy.append(brand,launchState);
 const badge=element('span');badge.className='ak-rail-badge';badge.hidden=true;launch.append(mark,launchCopy,badge);
 const cards=element('div');cards.id='ak-rail-cards';cards.className='ak-rail-cards';cards.hidden=true;
 const sections=element('nav');sections.className='ak-rail-sections';sections.setAttribute('aria-label','Activity sections');
 const activitySection=button('Activity',()=>jumpTo(title),'activity'),outputsSection=button('Outputs',()=>jumpTo(outputHeading),'outputs'),inboxSection=button('Inbox',()=>jumpTo(inboxHeading),'inbox'),agentsSection=button('Agents',()=>{jumpTo(title);activityBody.scrollTop+=agentHeading.getBoundingClientRect().top-activityBody.getBoundingClientRect().top;agentHeading.tabIndex=-1;agentHeading.focus({preventScroll:true});},'agents');const computeSection=button('Compute',()=>{jumpTo(computeHeading);},'compute');sections.append(activitySection,agentsSection,computeSection,outputsSection,inboxSection);
 const workCard=element('section');workCard.className='ak-rail-card ak-rail-work';workCard.setAttribute('aria-labelledby','ak-activity-heading');
 const heading=element('header'),title=element('h2','Activity');title.id='ak-activity-heading';const actions=element('div');actions.className='ak-rail-actions';
 const pin=button('Keep open',()=>{automaticLayout=false;pinned=!pinned;setOpen(true);},'pin'),close=button('Close',()=>{automaticLayout=false;pinned=false;setOpen(false,{focus:true});},'close');pin.setAttribute('aria-pressed','false');close.setAttribute('aria-label','Close Aurekai activity');actions.append(pin,close);heading.append(title,actions);
 const scopeLabel=element('label');scopeLabel.className='ak-rail-scope';const scopeCopy=element('span','Account or organization');scopeCopy.className='ak-rail-visually-hidden';scopeLabel.append(scopeCopy);
 const scope=element('select');scope.setAttribute('aria-label','Activity account or organization');scope.disabled=true;scopeLabel.append(scope);
 const status=element('p','Checking your account…');status.className='ak-rail-status';status.setAttribute('role','status');
 const coverage=element('p','Research runs');coverage.className='ak-rail-caption';coverage.hidden=true;
 const filters=element('div');filters.className='ak-rail-filters';filters.setAttribute('role','group');filters.setAttribute('aria-label','Filter activity');filters.hidden=true;
 const filterButtons=new Map();for(const [key,label] of [['all','All'],['running','Running'],['attention','Needs you'],['completed','Finished']]){const node=button(label,()=>{workFilter=key;workLimit=3;renderWork();});node.dataset.filter=key;node.setAttribute('aria-pressed',String(key==='all'));filterButtons.set(key,node);filters.append(node);}
 const runList=element('ul');runList.className='ak-rail-runs';runList.setAttribute('aria-label','Recent work');
 const moreRuns=button('Show more',()=>{workLimit=workLimit===3?50:3;renderWork();});moreRuns.className='ak-rail-more';moreRuns.hidden=true;

 const taskStatus=element('p','Checking native work…');taskStatus.className='ak-rail-native-status';taskStatus.setAttribute('role','status');const taskList=element('ul');taskList.className='ak-rail-tasks';taskList.setAttribute('aria-label','Native tasks and subagents');
 const researchHeading=element('h3','Research and background work'),updatesHeading=element('h3','Research updates'),researchUpdates=element('ul');researchUpdates.className='ak-rail-research-updates';researchUpdates.setAttribute('aria-label','Research state updates');
 const detail=element('section');detail.className='ak-rail-detail';detail.hidden=true;detail.setAttribute('aria-label','Exact work details');
 const outputHeading=element('h3','Outputs'),outputList=element('ul');outputHeading.className='ak-rail-output-heading';outputList.className='ak-rail-outputs';outputList.setAttribute('aria-label','Retained outputs and receipt evidence');outputList.tabIndex=0;
 const footer=element('footer'),researchLink=localLink('Open research','/sources/web'),modelLink=localLink('Model Commons','/s/models/all'),programLink=localLink('Programs','/programs'),refreshButton=button('Refresh',()=>{if(!canRead())window.dispatchEvent(new Event('bonfyre:account-changed'));else void refresh();},'refresh');footer.append(programLink,researchLink,modelLink);actions.prepend(refreshButton);
 const activityBody=element('div');activityBody.className='ak-rail-activity-body';activityBody.setAttribute('aria-label','Activity list');activityBody.tabIndex=0;activityBody.append(taskStatus,taskList,researchHeading,status,coverage,filters,runList,moreRuns,updatesHeading,researchUpdates,footer);
 const agentHeading=element('h3','Agents'),agentNote=element('p'),agentList=element('ul');agentHeading.className='ak-rail-agent-heading';agentNote.className='ak-rail-agent-note';agentList.className='ak-rail-agents';agentList.setAttribute('aria-label','Agent runs in this account');
 const computeHeading=element('h3','Compute & limits'),computeNote=element('p'),computeList=element('ul');computeHeading.className='ak-rail-compute-heading';computeNote.className='ak-rail-compute-note';computeList.className='ak-rail-compute';computeList.setAttribute('aria-label','Measured token usage and provider accounting');
 activityBody.append(agentHeading,agentNote,agentList,computeHeading,computeNote,computeList);workCard.append(heading,scopeLabel,activityBody,outputHeading,outputList);
 const inboxCard=element('section');inboxCard.className='ak-rail-card ak-rail-inbox';inboxCard.setAttribute('aria-labelledby','ak-inbox-heading');
 const inboxHeader=element('header'),inboxHeading=element('h2','Inbox'),inboxCount=element('span');inboxHeading.id='ak-inbox-heading';inboxCount.className='ak-rail-caption';const inboxPage=element('a','Open full Inbox');inboxPage.href='/inbox?context=self';inboxPage.className='ak-rail-caption';inboxPage.dataset.inboxPage='';inboxHeader.append(inboxHeading,inboxCount,inboxPage);
 const inboxScope=element('p');inboxScope.className='ak-rail-caption';
 const inboxFilters=element('div');inboxFilters.className='ak-rail-filters ak-inbox-filters';inboxFilters.setAttribute('role','group');inboxFilters.setAttribute('aria-label','Filter inbox');inboxFilters.hidden=true;
 const inboxFilterButtons=new Map();for(const [key,label] of [['unread','Unread'],['all','All'],['dismissed','Dismissed']]){const node=button(label,()=>{inboxFilter=key;inboxLimit=3;renderInbox();});node.dataset.inboxFilter=key;node.setAttribute('aria-pressed',String(key==='all'));inboxFilterButtons.set(key,node);inboxFilters.append(node);}
 const inboxWork=element('details'),inboxWorkHeading=element('summary','Needs you'),inboxWorkList=element('ul');inboxWork.className='ak-rail-attention';inboxWork.setAttribute('aria-label','Work needing attention');inboxWork.hidden=true;inboxWorkList.className='ak-rail-attention-list';inboxWork.append(inboxWorkHeading,inboxWorkList);
 const notices=element('ul');notices.className='ak-rail-notices';notices.setAttribute('aria-label','Notifications for this recipient');notices.tabIndex=0;
 const moreNotices=button('Show more updates',()=>{inboxLimit=inboxLimit===3?50:3;renderInbox();});moreNotices.className='ak-rail-more';moreNotices.hidden=true;
 const inboxPages=element('div');inboxPages.className='ak-inbox-pages';inboxPages.hidden=true;
 const recentNotices=button('Recent updates',()=>{if(pendingNotice)return;inboxCursor='';inboxLimit=3;void refresh();}),earlierNotices=button('Earlier updates',()=>{if(pendingNotice||!inboxNext)return;inboxCursor=inboxNext;inboxLimit=3;void refresh();});inboxPages.append(recentNotices,earlierNotices);
 const inboxNote=element('p','Checking notifications for this account and destination…');inboxNote.className='ak-rail-caption ak-rail-inbox-note';inboxNote.setAttribute('role','status');const receipt=element('div');receipt.className='ak-inbox-receipt';receipt.setAttribute('role','status');receipt.tabIndex=-1;receipt.dataset.focusKey='notice-receipt';receipt.hidden=true;const inboxBody=element('div');inboxBody.className='ak-rail-inbox-body';inboxBody.append(inboxWork,inboxFilters,notices,moreNotices,inboxPages,receipt,inboxNote);inboxCard.append(inboxHeader,inboxScope,inboxBody);
 const pushSettings=element('details');pushSettings.className='ak-rail-push';const pushSummary=element('summary','Device notifications'),pushNote=element('p','Choose an account to manage device notifications.'),pushControl=button('Enable on this device',event=>void changePush(event));pushNote.setAttribute('role','status');pushControl.disabled=true;pushSettings.append(pushSummary,pushNote,pushControl);inboxCard.append(pushSettings);
 cards.append(sections,workCard,inboxCard);rail.append(launch,cards,detail);document.body.append(rail);

 const desktop=window.matchMedia('(min-width:1100px)'),initialPadding=document.body.style.paddingInlineEnd,basePadding=getComputedStyle(document.body).paddingInlineEnd;
 let open=false,pinned=false,wasBound=false,automaticLayout=true,destroyed=false,suspended=document.hidden,epoch=0,request=null,timer=null,hoverTimer=null,closeTimer=null,rows=[],chosen='',signature='',navigationNode=null,navigationContext=workspace?.dataset.activityContext,firstChoice=true;
 let workRows=[],outputRows=[],noticeRows=[],needsRows=[],hasNeeds=false,hasInbox=false,workFilter='all',inboxFilter='all',workLimit=3,inboxLimit=3,pendingNotice=null,lastDismissed=null,noticeMessage='',inboxMore=false,inboxCursor='',inboxNext='',queueSnapshot=null,outputSnapshot=null;
 const expanded=new Set();
 let noticeSignature='',receiptSignature='';
 let research=null,native=null,binding=null,scopeEpoch=0,nativeSignature='',outputSignature='',selected=null,detailEpoch=0,detailRequest=null,providerMount=null,detailReturn=null,detailIntent=null,bindingRetries=0,handledHash='';
 let pushSnapshot=null,pushSubscription=null,pushRequest=null,pushEpoch=0,pushPending=false,pushIntentHandled='',pushIntentRequest=null;
 const actionsPending=new Set(),actionRequests=new Set(),unconfirmedActions=new Map();
 const cleanup=[];
 const on=(target,type,handler,options)=>{target.addEventListener(type,handler,options);cleanup.push(()=>target.removeEventListener(type,handler,options));};
 function navigationState(){const slot=document.querySelector('[data-account-slot]');return slot?(slot.querySelector('.ak-session')?.dataset.sessionState||'checking'):workspace?'workspace':'checking';}
 // Installed editor navigation supplies a destination, never an access grant.
 // Every native owner must authorize the current session and destination.
 function canRead(){return ['signed-in','workspace'].includes(navigationState());}
 function workspaceContext(){try{const value=decodeURIComponent(workspace?.dataset.activityContext||'');return contextPattern.test(value)?value:'';}catch{return '';}}
 function setOpen(value,{focus=false}={}){
  if(destroyed)return;const changed=open!==value;open=value;cards.hidden=!value;detail.hidden=!value||!selected;rail.dataset.open=String(value);rail.dataset.connectionOpen=String(value&&selected?.provider===true);launch.setAttribute('aria-expanded',String(value));pin.setAttribute('aria-pressed',String(pinned));pin.textContent=pinned?'Pinned':'Keep open';clearTimeout(closeTimer);
  const docked=open&&pinned&&desktop.matches;rail.dataset.docked=String(docked);document.body.style.paddingInlineEnd=docked?'calc('+basePadding+' + 390px)':initialPadding;document.documentElement.style.setProperty('--ak-dock',docked?'390px':'0px');
  if(!value){clearTimeout(hoverTimer);if(focus)launch.focus();}
  else if(changed&&!request)void refresh();
 }
 function empty(list,text){const li=element('li',text);li.className='ak-rail-empty';list.replaceChildren(li);}
 function jumpTo(node){cards.scrollTop=Math.max(0,cards.scrollTop+node.getBoundingClientRect().top-cards.getBoundingClientRect().top-sections.getBoundingClientRect().height-12);node.tabIndex=-1;node.focus({preventScroll:true});}
 function preserveFocus(container,render){
  const active=document.activeElement,focus=container.contains(active)?active?.dataset.focusKey:null,top=cards.scrollTop;render();
  if(focus){const replacement=[...container.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey===focus);replacement?.focus({preventScroll:true});}
  cards.scrollTop=top;
 }
 function clearPrivate(message='Checking current access…',{reset=true,preserveDetailIntent=false}={}){
  noticeSignature=receiptSignature='';
  workRows=[];outputRows=[];noticeRows=[];needsRows=[];hasNeeds=false;queueSnapshot=outputSnapshot=null;hasInbox=false;inboxMore=false;expanded.clear();filters.hidden=true;moreRuns.hidden=true;inboxFilters.hidden=true;moreNotices.hidden=true;receipt.hidden=true;receipt.replaceChildren();inboxPage.href='/inbox?context=self';inboxSection.textContent='Inbox';inboxSection.removeAttribute('aria-label');launchState.removeAttribute('title');
  if(reset){pendingNotice?.controller?.abort();pendingNotice=null;lastDismissed=null;noticeMessage='';workFilter='all';inboxFilter='all';workLimit=3;inboxLimit=3;inboxCursor='';}inboxNext='';renderInboxPages();
  signature='';rows=[];refreshButton.disabled=false;rail.removeAttribute('aria-busy');scope.disabled=true;scope.replaceChildren();coverage.hidden=true;status.replaceChildren(document.createTextNode(message));launchState.textContent='Your activity';badge.hidden=true;badge.textContent='';inboxScope.textContent='';inboxCount.textContent='';
  research=native=binding=null;nativeSignature=outputSignature='';agentNote.textContent='';empty(agentList,'');computeNote.textContent='';computeList.replaceChildren();computeHeading.hidden=computeNote.hidden=computeList.hidden=computeSection.hidden=true;actionsPending.clear();unconfirmedActions.clear();for(const action of actionRequests)action.abort();actionRequests.clear();closeDetail(false,{preserveIntent:preserveDetailIntent});taskStatus.textContent='';inboxNote.textContent='';empty(taskList,'');empty(researchUpdates,'');empty(runList,'');empty(outputList,'');empty(notices,'');inboxWork.hidden=true;inboxWorkList.replaceChildren();researchLink.href='/sources/web';modelLink.href='/s/models/all';programLink.href='/programs';
  inboxWork.open=false;inboxWork.removeAttribute('data-actions-context');
  pushEpoch++;pushRequest?.abort();pushRequest=null;pushIntentRequest?.abort();pushIntentRequest=null;pushSnapshot=pushSubscription=null;pushPending=false;pushControl.disabled=true;pushControl.textContent='Enable on this device';pushSummary.textContent='Device notifications';pushNote.textContent='Choose an account to manage device notifications.';
 }
 function invalidate(message,{reset=true,preserveDetailIntent=false}={}){scopeEpoch++;epoch++;request?.abort();request=null;clearTimeout(timer);timer=null;clearPrivate(message,{reset,preserveDetailIntent});window.dispatchEvent(new CustomEvent('bonfyre:activity-context-invalidated',{detail:{context:chosen}}));}
 function suspendPrivate(){
  const operation=pendingNotice;
  const retained=operation&&canRead()&&operation.context===chosen?{context:operation.context,binding:operation.binding,id:operation.id,version:operation.version,revision:operation.revision,action:operation.action,requestId:operation.requestId,controller:null,busy:false}:null;
  // Keep only an intent to reopen this work. Account presentation and provider
  // attachment are cleared; both current access and the exact work are read again.
  if(open&&selected?.provider===true&&binding&&chosen)detailIntent={kind:selected.kind,id:selected.id,context:chosen,binding:{actor:binding.actor,subject:binding.subject,scope:binding.scope},expanded:selected.expanded===true};
  invalidate(undefined,{preserveDetailIntent:true});pendingNotice=retained;
 }
 function authorityRefused(error){return error?.status===401||error?.status===403||error?.code==='access_required';}
 function refuseAccess(){firstChoice=true;chosen='';invalidate('Current access to this activity is unavailable.');status.append(' ',signInLink());empty(notices,'No current update could be confirmed.');}
 function anonymous(state){
  firstChoice=true;chosen='';
  if(automaticLayout&&!wasBound&&pinned&&!selected){pinned=false;setOpen(false);} // a visitor not signed in on this page has no activity to keep beside it
  invalidate(state==='anonymous'?'Sign in to see your activity.':state==='unavailable'?'Your account could not be checked.':'Checking your account…');
  if(state==='anonymous')rail.dataset.visitor='anonymous';else delete rail.dataset.visitor;if(state==='anonymous'){status.append(' ',signInLink());empty(outputList,'Your saved outputs appear after sign-in.');empty(notices,'Sign in to see account updates.');}
  else if(state==='unavailable')empty(notices,'Use Refresh to check your account again.');
 }
 function renderChoices(all){
  rows=all;scope.replaceChildren();
  if(!chosen){const option=element('option','Choose an account or organization');option.value='';scope.append(option);}
  for(const row of rows){const option=element('option',row.label);option.value=row.context;scope.append(option);}scope.value=chosen;scope.disabled=false;
 }
 function renderResearch(snapshot){
  research=snapshot;researchUpdates.replaceChildren();
  const updates=(snapshot?.activities||[]).filter(row=>!row.archived&&Object.hasOwn(noticeTitles,row.state)).sort((a,b)=>b.updated-a.updated);
  for(const row of updates.slice(0,3)){const li=element('li'),a=localLink('',row.href),top=element('span');top.className='ak-notice-top';top.append(element('strong',noticeTitles[row.state]),time(row.updated));a.append(top,element('span',row.title));if(row.state==='uncertain')a.append(element('small','Open the saved run to check its outcome.'));li.append(a);researchUpdates.append(li);}
  if(!researchUpdates.childElementCount)empty(researchUpdates,snapshot?'No recent research updates.':'Research updates are unavailable.');
 }
 function matches(row,filter){return filter==='all'||filter==='running'&&isActive(row)||filter==='attention'&&(row.kind==='task'?row.phase==='attention':attentionStates.has(row.state))||filter==='completed'&&(row.kind==='task'?row.phase==='completed':completedStates.has(row.state));}
 function outputLink(row,{attached=false}={}){
  const a=row.href?localLink('',row.href):element('div'),body=element('span');a.classList.add('ak-output-entry');body.append(element('strong',row.title),element('small',Number.isInteger(row.items)?plural(row.items,'retained source'):'Recorded output reference'+(row.href?' · Open workspace':'')));
  const icon=element('span',row.href?'↗':'');icon.setAttribute('aria-hidden','true');a.append(body,icon);a.dataset.focusKey=(attached?'attached:':'output:')+(row.key||row.id);return a;
 }
 function renderWork(){
  preserveFocus(runList,()=>{
   const visible=workRows.filter(row=>matches(row,workFilter));for(const [key,node] of filterButtons){node.setAttribute('aria-pressed',String(key===workFilter));const count=workRows.filter(row=>matches(row,key)).length;node.setAttribute('aria-label',node.textContent+', '+plural(count,'recent item'));}
   runList.replaceChildren();
   for(const row of visible.slice(0,workLimit)){
    const key=workKey(row),li=element('li'),detail=element('details'),summary=element('summary'),indicator=element('span'),body=element('span');detail.dataset.workKey=key;detail.open=expanded.has(key);summary.dataset.focusKey='work:'+key;
    indicator.className='ak-rail-dot';indicator.dataset.state=row.state;indicator.setAttribute('aria-hidden','true');const family=workTheme(row),familyLabel=element('span'),familyIcon=element('span');familyLabel.className='ak-work-family-line';familyIcon.dataset.akIcon=workThemeIcons[family];familyIcon.setAttribute('aria-hidden','true');familyLabel.append(familyIcon,element('small',[family,row.provider||'Background work',stateLabel(row)].join(' · ')));body.append(element('strong',row.title),familyLabel);if(row.kind==='task'&&row.usage?.pricedRuns){const estimate=element('small',`Estimated $${(row.usage.estimatedMicroUsd/1000000).toFixed(2)} · ${row.usage.pricedRuns} of ${row.usage.runs} runs`);estimate.className='ak-work-estimate';body.append(estimate);}summary.append(indicator,body);
    const workerCount=row.counts?.workers||row.workers?.length||0,tail=element('span',workerCount?plural(workerCount,'worker'):'');tail.className='ak-work-expand';tail.setAttribute('aria-hidden','true');summary.append(tail);detail.append(summary);
    const content=element('div');content.className='ak-work-detail';
    const meta=element('p');meta.className='ak-work-time';meta.append('Updated ',time(row.updated));content.append(meta);
    if(row.message)content.append(element('p',row.message));
    if(row.counts){const counts=element('p');counts.className='ak-work-counts';counts.textContent=[plural(row.counts.nodes,'step'),row.counts.active?plural(row.counts.active,'running step'):null,row.counts.attention?plural(row.counts.attention,'step needing attention'):null].filter(Boolean).join(' · ');content.append(counts);}
    if(row.kind==="task"&&row.usage){const u=row.usage,measure=element("p");measure.className="ak-work-counts";
     measure.textContent=(u.reportedRuns?`Token reports: ${u.reportedRuns} of ${u.runs} runs · ${u.inputTokens.toLocaleString()} input · ${u.outputTokens.toLocaleString()} output`:`Token reports: none of ${u.runs} runs`)+(u.pricedRuns?` · Estimated cost: $${(u.estimatedMicroUsd/1000000).toFixed(2)} for ${u.pricedRuns} of ${u.runs} runs (not an invoice)`:' · Cost unknown');
     content.append(measure);const finance=localLink("Open financial tools","/apps?"+new URLSearchParams({context:chosen}));finance.className="ak-work-continue";content.append(finance);
    }
    const nodes=Array.isArray(row.nodes)?row.nodes:[];
    if(nodes.length){content.append(element('h4','Steps'));const list=element('ul');list.className='ak-work-children';const rendered=new Set();
     const draw=(node,depth)=>{if(rendered.has(node.id))return;rendered.add(node.id);const item=element('li'),dot=element('span'),copy=element('span');item.style.setProperty('--ak-step-depth',String(Math.min(depth,3)));dot.className='ak-rail-dot';dot.dataset.state=node.phase==='active'?'running':node.state;dot.setAttribute('aria-hidden','true');copy.append(element('strong',stepLabel(node.operatorId||node.family||'Work step')),element('small',[node.family,stateLabel(node),node.attempt?'Attempt '+node.attempt:null].filter(Boolean).join(' · ')));item.append(dot,copy);list.append(item);for(const child of nodes.filter(candidate=>candidate.parentId===node.id))draw(child,depth+1);};
     for(const node of nodes.filter(candidate=>candidate.parentId===null||!nodes.some(parent=>parent.id===candidate.parentId)))draw(node,0);for(const node of nodes)draw(node,0);content.append(list);if(row.nodesMore)content.append(element('p','Showing '+nodes.length+' of '+row.counts.nodes+' steps.'));
    }
    const workers=Array.isArray(row.workers)?row.workers:[];
    if(workers.length){content.append(element('h4','Workers'));const list=element('ul');list.className='ak-work-children ak-work-workers';for(const worker of workers){const item=element('li'),dot=element('span'),copy=element('span');dot.className='ak-rail-dot';dot.dataset.state=worker.phase==='active'?'running':worker.state;dot.setAttribute('aria-hidden','true');copy.append(element('strong',worker.workerId),element('small',stateLabel(worker)));item.append(dot,copy);list.append(item);}content.append(list);if(row.workersMore)content.append(element('p','Showing '+workers.length+' of '+row.counts.workers+' workers.'));}
    const outputs=outputRows.filter(output=>(output.workKey||workKey(output))===key);
    if(outputs.length){content.append(element('h4',row.kind==='task'?'Output references':'Outputs'));const list=element('ul');list.className='ak-work-attached';for(const output of outputs){const item=element('li');item.append(outputLink(output,{attached:true}));list.append(item);}content.append(list);if(row.kind==='task')content.append(element('p',row.outputsMore?'Showing '+outputs.length+' of '+row.counts.outputs+' recorded output references.':'These references were recorded by the work steps.'));}
    if(row.state==='uncertain')content.append(element('p','Open this run to check its saved outcome before trying again.'));
    const continuation=typeof row.continuation==='string'?{href:row.continuation,label:'Open workspace'}:row.continuation||{href:row.href,label:row.kind==='task'?'Open workspace':'Open research run'};
    if(continuation.href){const a=localLink(continuation.label||'Continue',continuation.href);a.className='ak-work-continue';a.dataset.focusKey='continue:'+key;content.append(a);}if(row.kind==='task'){const board=localLink('Open work board','/work-board?'+new URLSearchParams({context:chosen,task:row.id}));board.className='ak-work-board-link';board.dataset.focusKey='board:'+key;content.append(board);}
    detail.append(content);detail.addEventListener('toggle',()=>{if(!detail.isConnected)return;if(detail.open)expanded.add(key);else expanded.delete(key);});li.append(detail);if(row.kind==='research'){const inspect=button('Details',()=>{if(inspect.isConnected&&research?.activities.some(current=>current.id===row.id&&current.revision===row.revision))void openDetail('research',row.id,inspect);});inspect.setAttribute('aria-label','Details for '+row.title);inspect.dataset.focusKey='inspect:'+key;li.append(inspect);}runList.append(li);
   }
   if(!runList.childElementCount)empty(runList,workRows.length?'No recent work matches this filter.':'No recent work in this destination.');
   moreRuns.hidden=visible.length<=3;moreRuns.textContent=workLimit===3?'Show '+plural(visible.length-3,'more item'):'Show fewer';
  });
  renderOutputs();
 }
 function renderWorkProjection({researchError=false,taskError=false}={}){
  const entries=(research?.activities||[]).filter(row=>!row.archived).map(row=>({...row,kind:'research'}));
  const taskRows=(queueSnapshot?.tasks||[]).filter(row=>!row.archived).map(row=>({...row,kind:'task',href:row.continuation||undefined}));
  workRows=[...entries,...taskRows].sort((a,b)=>b.updated-a.updated||workKey(a).localeCompare(workKey(b)));
  const taskOutputs=(outputSnapshot?.tasks||taskRows).flatMap(task=>(task.outputs||[]).map(row=>({...row,title:row.label,key:'task:'+task.id+':'+row.id,workKey:'task:'+task.id,href:'/work-board?'+new URLSearchParams({context:chosen,task:task.id}),kind:'declared-output'})));
  const researchOutputs=(research?.outputs||[]).filter(row=>entries.some(entry=>workKey(entry)===(row.workKey||workKey(row)))).map(row=>({...row,kind:'research'}));
  outputRows=[...new Map([...researchOutputs,...taskOutputs].map(row=>[row.key||row.id,row])).values()].sort((a,b)=>b.updated-a.updated);
  const more=!!research?.more||!!queueSnapshot?.more;
  status.textContent=researchError?'Research could not be refreshed.':taskError?'Background work could not be checked.':more?'Showing a recent page of work.':queueSnapshot?'Current account activity':'Current research activity';
  coverage.hidden=false;coverage.textContent=queueSnapshot?'Research and background work':'Research runs';filters.hidden=!workRows.length;
  researchLink.href='/sources/web?'+new URLSearchParams({context:chosen});
  programLink.href='/programs?'+new URLSearchParams({context:chosen});
  const next=JSON.stringify([chosen,workRows,outputRows]);if(next!==signature){signature=next;const valid=new Set(workRows.map(workKey));for(const key of expanded)if(!valid.has(key))expanded.delete(key);renderWork();}
 }
 function workLink(row,label=row.title){const link=localLink(label,row.href);link.addEventListener('click',event=>{if(event.button||event.metaKey||event.ctrlKey||event.altKey||event.shiftKey)return;event.preventDefault();if(link.isConnected)void openDetail('fabric',row.taskId,link);});return link;}
 function renderNative(snapshot){
  native=snapshot;const roots=snapshot.tasks.filter(row=>row.kind==='task'),subagents=snapshot.tasks.filter(row=>row.kind==='subagent');taskStatus.textContent=(snapshot.more?'Recent native work':'Native work')+' · '+roots.length+' '+(roots.length===1?'task':'tasks')+' · '+subagents.length+' '+(subagents.length===1?'subagent':'subagents');
  const key=[scopeEpoch,snapshot.revision,...snapshot.tasks.map(row=>row.id+':'+row.revision+':'+row.phase),...snapshot.notifications.map(row=>row.id+':'+row.revision+':'+row.state),...actionsPending,...unconfirmedActions.keys()].join('|');
  if(key===nativeSignature){renderInbox();return;}nativeSignature=key;const expanded=new Set([...taskList.querySelectorAll('details[open]')].map(node=>node.dataset.taskId));taskList.replaceChildren();
  const taskRow=row=>{const li=element('li');li.dataset.kind=row.kind;li.append(workLink(row),element('small',(row.kind==='subagent'?'Subagent':row.workKind==='model'?'Model run':'Native task')+' · '+row.phase));if(row.reconciliationRequired)li.append(element('small','Outcome needs reconciliation.'));return li;};
  for(const row of roots){
   const li=taskRow(row),children=subagents.filter(child=>child.parentTaskId===row.id);if(children.length){const disclosure=element('details'),summary=element('summary',children.length+' '+(children.length===1?'subagent':'subagents')),list=element('ul');disclosure.className='ak-rail-subagents';disclosure.dataset.taskId=row.id;disclosure.open=expanded.has(row.id);for(const child of children)list.append(taskRow(child));disclosure.append(summary,list);li.append(disclosure);}taskList.append(li);
  }
  if(!taskList.childElementCount)empty(taskList,'No native work in this destination.');
  renderInbox();
 }
 // One line per recorded run; usage only where the run's own record reported it.
 function renderAgents(snapshot){const span=ms=>ms<6e4?Math.max(1,Math.round(ms/1e3))+' s':ms<36e5?Math.round(ms/6e4)+' min':ms<1728e5?(ms/36e5).toFixed(1).replace(/\.0$/,'')+' h':Math.round(ms/864e5)+' days',names={codex:'Codex',claude:'Claude'},now=Date.now(),c=snapshot?.counts;agentNote.textContent=!snapshot?'Agent runs could not be checked.':[c.active+' running',c.attention?c.attention+(c.attention===1?' needs you':' need you'):null,c.completed?c.completed+' finished this week':null].filter(Boolean).join(' · ');agentList.replaceChildren();
  for(const row of snapshot?.agents||[]){const li=element('li'),a=localLink('','/work-board?'+new URLSearchParams({context:chosen,task:row.taskId})),dot=element('span'),copy=element('span'),u=row.usage,elapsed=row.phase==='active'&&!row.unconfirmed?now-row.started:row.wallMs;li.dataset.phase=row.phase;a.dataset.focusKey='agent:'+row.id;dot.className='ak-rail-dot';dot.dataset.state=row.phase==='active'?'running':row.state;dot.setAttribute('aria-hidden','true');copy.append(element('strong',row.title),element('small',[names[row.agent]||row.agent,row.role&&stepLabel(row.role),row.session&&stepLabel(row.session),(row.unconfirmed?'Outcome unconfirmed':phaseNames[row.phase])+(Number.isSafeInteger(elapsed)&&elapsed>=0?' '+span(elapsed):'')].filter(Boolean).join(' · ')));
   if(u)copy.append(element('small',[u.inputTokens!==null&&u.inputTokens.toLocaleString()+' input',u.outputTokens!==null&&u.outputTokens.toLocaleString()+' output',u.cacheReadTokens!==null&&u.cacheReadTokens.toLocaleString()+' cached',u.estimatedMicroUsd!==null&&'about $'+(u.estimatedMicroUsd/1e6).toFixed(2)].filter(Boolean).join(' · ')||'Usage not reported'));if(row.phase==='active'&&now-row.updated>216e5)copy.append(element('small','No update since '+date(row.updated)));if(row.limit){const limit=element('small','Provider limit reached');limit.dataset.limit='';copy.append(limit);}a.append(dot,copy);li.append(a);agentList.append(li);}
  if(snapshot&&!agentList.childElementCount)empty(agentList,'No agent runs in this destination.');if(snapshot?.more)agentList.append(element('li','Showing the '+snapshot.agents.length+' most recent runs.'));}
 function renderCompute(snapshot,agentSnapshot) {
  const visible=chosen==='self';computeHeading.hidden=computeNote.hidden=computeList.hidden=computeSection.hidden=!visible;
  if(!visible)return;
  computeList.replaceChildren();
  if(!snapshot){computeNote.textContent='Token evidence could not be checked.';empty(computeList,'Check the Meter owner and account activity connection.');return;}
  if(snapshot.status==='no_evidence'){computeNote.textContent='No imported token evidence for this account.';empty(computeList,'That does not establish zero usage or cost.');return;}
  computeNote.textContent=snapshot.additiveTokens.toLocaleString()+' additive tokens · '+snapshot.records.toLocaleString()+' records · '+snapshot.providerSessions.toLocaleString()+' provider sessions · '+snapshot.cachedInputTokens.toLocaleString()+' cached input';
  for(const row of snapshot.providers){
   const li=element('li'),label=element('strong',row.provider==='codex'?'Codex':'Claude'),amount=element('small',row.additiveTokens.toLocaleString()+' additive tokens · '+row.sessions.toLocaleString()+(row.sessions===1?' session':' sessions')),breakdown=element('small',row.cachedInputTokens.toLocaleString()+' cached input · '+row.uncachedInputTokens.toLocaleString()+' fresh input · '+row.cacheWriteTokens.toLocaleString()+' cache writes · '+row.outputTokens.toLocaleString()+' output');
   const fraction=element('progress');fraction.max=snapshot.additiveTokens||1;fraction.value=row.additiveTokens;fraction.setAttribute('aria-label',label.textContent+' share of additive tokens');li.append(label,amount,fraction,breakdown);computeList.append(li);
  }
  const limited=agentSnapshot?.agents?.filter(row=>row.limit)??[];
  if(limited.length){
   const item=element('li');item.className='ak-rail-compute-limits';
   item.append(element('strong',limited.length+' provider-limited '+(limited.length===1?'run':'runs')+' to review'),element('small','Open the recorded run for its recovery state. A limit flag does not establish a remaining balance.'));
   for(const row of limited.slice(0,5))item.append(localLink(row.title+' · '+(row.agent==='codex'?'Codex':'Claude'),'/work-board?'+new URLSearchParams({context:chosen,task:row.taskId})));
   computeList.append(item);
  }
  const note=element('li');note.className='ak-rail-compute-disclosure';note.append(element('strong','Not a bill'),element('small',snapshot.inheritedSnapshotTokens.toLocaleString()+' inherited snapshot tokens excluded. Dollar charges, subscription credits and remaining provider balance are not established by token telemetry.'));computeList.append(note);
 }
 function renderOutputs(){
  const receipts=native?.outputs||[],key=JSON.stringify([scopeEpoch,outputRows,receipts]);
  if(key!==outputSignature){outputSignature=key;preserveFocus(outputList,()=>{outputList.replaceChildren();
   for(const row of outputRows.slice(0,8)){const li=element('li');li.append(outputLink(row));outputList.append(li);}
   for(const row of receipts.slice(0,5)){const li=element('li'),a=workLink(row);a.append(element('small',row.kind==='model-output'?'Model output · Open result':row.kind==='model-usage'?'Model usage · Review run':'Native receipt evidence · Review work'));li.append(a);outputList.append(li);}
   if(!outputList.childElementCount)empty(outputList,research||native||outputSnapshot||queueSnapshot?'No retained outputs in this account view.':'Outputs could not be checked.');
  });}
  const active=workRows.filter(isActive).length+(native?.tasks||[]).filter(row=>row.phase==='active').length;
  badge.hidden=!active;badge.textContent=active?String(active):'';badge.setAttribute('aria-label',plural(active,'running item')+(research?.more||queueSnapshot?.more||native?.more?' in this recent page':''));
 }
 async function refreshPush(){
  if(destroyed||suspended||!binding||!chosen||pushPending)return;
  const context=chosen,expected=binding,ownerEpoch=scopeEpoch,ticket=++pushEpoch,controller=new AbortController();pushRequest?.abort();pushRequest=controller;
  const current=()=>!destroyed&&!suspended&&!controller.signal.aborted&&ownerEpoch===scopeEpoch&&ticket===pushEpoch&&context===chosen&&sameActivityBinding(expected,binding);
  try{
   const snapshot=await pushStatus(context,{signal:controller.signal});if(!current())return;
   if(!sameActivityBinding(snapshot.binding,expected)){invalidate('The account changed while checking notifications.');void refresh();return;}
   let device=null,unsupported=null;
   try{device=await pushDevice(snapshot,{signal:controller.signal});}catch(error){unsupported=error;}
   if(!current())return;pushSnapshot=snapshot;pushSubscription=device;
   pushControl.textContent=device?'Turn off for this account':'Enable on this device';
   pushControl.disabled=Boolean(unsupported)||!device&&(!snapshot.configured||snapshot.paused);
   pushSummary.textContent=device?'Device notifications · On':'Device notifications';
   pushNote.textContent=unsupported?.message||(snapshot.paused?'Background delivery is paused.':!snapshot.configured?'Device notifications are not connected yet.':device?'Enabled for '+(rows.find(row=>row.context===context)?.label||'this account')+'.':'Get an alert when new activity needs your attention.');
  }catch(error){if(current()){if(authorityRefused(error)){refuseAccess();return;}pushSnapshot=pushSubscription=null;pushControl.disabled=true;pushNote.textContent='Device notifications could not be checked. Use Refresh to try again.';}}
  finally{if(current())pushRequest=null;}
 }
 async function changePush(event){
  if(destroyed||suspended||pushPending||!pushSnapshot||!binding||!sameActivityBinding(pushSnapshot.binding,binding)||pushControl.disabled)return;
  const context=chosen,expected=binding,ownerEpoch=scopeEpoch,snapshot=pushSnapshot,device=pushSubscription,controller=new AbortController();pushRequest?.abort();pushRequest=controller;pushPending=true;pushControl.disabled=true;pushNote.textContent=device?'Turning off notifications for this account…':'Waiting for browser notification permission…';
  const current=()=>!destroyed&&!suspended&&!controller.signal.aborted&&ownerEpoch===scopeEpoch&&context===chosen&&sameActivityBinding(expected,binding);
  try{
   if(device)await pushDisable(context,snapshot,device.id,{signal:controller.signal});
   else await pushEnable(context,snapshot,{event,signal:controller.signal});
   if(current()){pushPending=false;pushRequest=null;await refreshPush();}
  }catch(error){if(current()){if(authorityRefused(error)){refuseAccess();return;}pushNote.textContent=error?.code==='permission_required'?'Notifications are blocked in this browser. Change its site permission, then use Refresh.':error?.code==='conflict'?'The account changed. Refresh before enabling notifications.':(error?.message||'This notification change could not be confirmed.')+' Use Refresh before trying again.';}}
  finally{if(current()&&pushRequest===controller){pushPending=false;pushRequest=null;pushControl.disabled=true;}}
 }
 async function resumePushIntent(){
  const url=new URL(location.href),id=url.searchParams.get('aurekai_push');if(!id||id===pushIntentHandled||pushIntentRequest||!binding)return;
  const context=chosen,expected=binding,ownerEpoch=scopeEpoch,controller=new AbortController();pushIntentRequest=controller;
  const result=await pushResume(location,{signal:controller.signal}).catch(()=>null);
  if(pushIntentRequest===controller)pushIntentRequest=null;
  if(destroyed||suspended||controller.signal.aborted||ownerEpoch!==scopeEpoch||context!==chosen||!sameActivityBinding(expected,binding)||!result)return;
  if(result.state==='ready'&&sameActivityBinding(result.binding,expected)){
   pushIntentHandled=id;const target=new URL(result.href,location.origin);history.replaceState(history.state,'',target.pathname+target.search+target.hash);handledHash=target.hash;void openDetail('fabric',result.taskId);
  }else{
   pushSettings.open=true;pushNote.textContent=result.state==='access_required'?'Switch to the account for this notification to open its work.':result.state==='expired'?'This device notification expired. Your retained work is still available in Activity.':'This notification could not be opened. Refresh to try again.';
  }
 }
 function renderNoticeReceipt(){
  if(lastDismissed&&!noticeRows.some(row=>row.id===lastDismissed.id&&row.version===lastDismissed.version&&row.dismissed))lastDismissed=null;
  const next=JSON.stringify(['queue',scopeEpoch,chosen,binding,pendingNotice?[pendingNotice.context,pendingNotice.binding,pendingNotice.id,pendingNotice.version,pendingNotice.revision,pendingNotice.action,pendingNotice.requestId,pendingNotice.busy]:null,lastDismissed,noticeMessage]);
  if(next===receiptSignature)return;
  receiptSignature=next;
  const active=document.activeElement,focused=receipt.contains(active),focus=active?.dataset.focusKey;
  receipt.replaceChildren();receipt.hidden=!pendingNotice&&!lastDismissed&&!noticeMessage;
  if(pendingNotice){receipt.append(element('span',pendingNotice.busy?'Saving your Inbox change…':'The Inbox change is unconfirmed. Check the saved result before making another change.'));if(!pendingNotice.busy){const check=button('Check status',()=>void recoverNotice());check.dataset.focusKey='notice-check';receipt.append(check);}}
  else if(lastDismissed){receipt.append(element('span','Notification dismissed.'));const undo=button('Undo',()=>{const row=noticeRows.find(item=>item.id===lastDismissed.id&&item.version===lastDismissed.version);if(row)void changeNotice(row,'restore');});undo.dataset.focusKey='notice-undo';receipt.append(undo);}else if(noticeMessage)receipt.append(element('span',noticeMessage));
  if(focused&&!receipt.hidden)([...receipt.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey===focus)||receipt.querySelector('button')||receipt).focus({preventScroll:true});
 }
 function renderInboxPages(){inboxPages.hidden=!inboxCursor&&(!hasInbox||!inboxNext);recentNotices.hidden=!inboxCursor;earlierNotices.hidden=!hasInbox||!inboxNext;recentNotices.disabled=earlierNotices.disabled=!!request||!!pendingNotice;}
 function renderInbox(){
  preserveFocus(notices,()=>{
   for(const [key,node] of inboxFilterButtons)node.setAttribute('aria-pressed',String(key===inboxFilter));
   const accountHref='/inbox?'+new URLSearchParams({context:chosen}),accountActions=hasNeeds?needsRows.map(row=>({id:row.id,title:row.title,kind:row.kind,updated:row.updated,href:accountHref+'#'+(row.kind==='invitation'?'sub-accounts':encodeURIComponent(row.id)),owner:'account',read:false,dismissed:false})):[];
   const combined=[...accountActions.sort((a,b)=>b.updated-a.updated),...[...noticeRows.map(row=>({...row,owner:'queue'})),...(native?.notifications||[]).map(row=>({...row,owner:'fabric',read:row.state==='read',dismissed:false,body:'',updated:row.updated}))].sort((a,b)=>b.updated-a.updated)];
   const visible=combined.filter(row=>row.owner==='account'?inboxFilter!=='dismissed':inboxFilter==='dismissed'?row.dismissed:!row.dismissed&&(inboxFilter!=='unread'||!row.read));
   const available=hasInbox||!!native,unread=noticeRows.filter(row=>!row.dismissed&&!row.read).length+(native?.unread||0);
   // The rail includes actionable Inbox work as well as recipient notifications.
   const attention=[...(queueSnapshot?.tasks||[]).filter(row=>row.phase==='attention').map(row=>({id:row.id,title:row.title,updated:row.updated,href:'/work-board?'+new URLSearchParams({context:chosen,task:row.id})})),
    ...(native?.tasks||[]).filter(row=>row.kind==='task'&&row.phase==='attention').map(row=>({id:row.taskId,title:row.title,updated:row.updated,href:row.href}))].filter((row,i,all)=>all.findIndex(item=>item.id===row.id)===i);
   inboxFilters.hidden=!available;inboxCount.textContent=(hasNeeds&&needsRows.length?plural(needsRows.length,'account action')+' · ':'')+(available?plural(unread,'unread update')+(inboxMore||inboxCursor||native?.more?' in these recent pages':''):hasNeeds?'':'Notifications unavailable');
   inboxSection.textContent='Inbox'+(needsRows.length||available&&unread?' · '+(needsRows.length+unread):'');inboxSection.setAttribute('aria-label',hasNeeds||available?'Inbox, '+inboxCount.textContent:'Inbox');const label=rows.find(row=>row.context===chosen)?.label;if(label){launchState.textContent=label+(available&&unread?' · '+unread+' unread':'');launchState.title=launchState.textContent;}
   // Keep a pressed control attached only while its owner, binding and state match.
   const next=JSON.stringify([scopeEpoch,chosen,binding,hasInbox,!!native,inboxFilter,inboxLimit,!!pendingNotice,hasNeeds,needsRows,combined,attention,[...actionsPending],[...unconfirmedActions.keys()]]);
   if(next===noticeSignature)return;
   noticeSignature=next;
   inboxWork.hidden=!attention.length&&hasNeeds;inboxWorkHeading.textContent='Work needing attention'+(attention.length?' · '+attention.length:'');
   inboxWorkList.replaceChildren();for(const row of attention.slice(0,5)){const li=element('li'),a=localLink(row.title,row.href);a.dataset.focusKey='attention:'+row.id;li.append(a);inboxWorkList.append(li);}
   if(attention.length>5){const li=element('li');li.append(localLink('See all work needing attention','/work-board?'+new URLSearchParams({context:chosen})));inboxWorkList.append(li);}if(!hasNeeds){const li=element('li');li.append(localLink('Account actions could not be checked. Open full Inbox',accountHref));inboxWorkList.append(li);inboxWork.open=true;}
   notices.replaceChildren();const initialLimit=inboxFilter==='dismissed'?3:Math.max(3,Math.min(accountActions.length,5));
   const displayLimit=inboxLimit===3?Math.max(3,Math.min(5,accountActions.length)):inboxLimit;
   for(const row of visible.slice(0,displayLimit)){
    const li=element('li');li.dataset.noticeOwner=row.owner;li.dataset.noticeId=row.id;li.dataset.read=String(row.read);li.dataset.dismissed=String(row.dismissed);if(row.owner==='fabric')li.dataset.state=row.state;if(row.owner==='account')li.dataset.inboxKind=row.kind;
    if(row.owner==='account'){const a=localLink(row.title,row.href);a.className='ak-notice-body';a.dataset.focusKey='account:'+row.id;a.append(element('small','Account action · Open full Inbox'));li.append(a);notices.append(li);continue;}
    const a=row.owner==='fabric'?workLink(row):row.href?localLink('',row.href):element('div'),top=element('span');a.className='ak-notice-body';top.className='ak-notice-top';top.append(element('strong',row.title),time(row.updated));a.replaceChildren(top,element('span',row.body||''));if(row.href)a.dataset.focusKey='notice:'+row.owner+':'+row.id;li.append(a);
    const controls=element('div');controls.className='ak-notice-actions';
    if(row.owner==='queue'){
     const read=button(row.read?'Mark unread':'Mark read',()=>void changeNotice(row,row.read?'unread':'read'));read.dataset.focusKey='notice-read:'+row.id;read.disabled=!!pendingNotice;
     const dismiss=button(row.dismissed?'Restore':'Dismiss',()=>void changeNotice(row,row.dismissed?'restore':'dismiss'));dismiss.dataset.focusKey='notice-dismiss:'+row.id;dismiss.disabled=!!pendingNotice;controls.append(read,dismiss);
    }else for(const action of row.allowedActions){const control=button(action==='read'?'Mark read':'Dismiss',()=>void actOnNotification(row,action,control));control.dataset.focus='notification:'+row.id+':'+action;control.dataset.focusKey=control.dataset.focus;control.disabled=actionsPending.has(row.id)||unconfirmedActions.has(row.id);controls.append(control);}
    li.append(controls);notices.append(li);
   }
   if(!notices.childElementCount)empty(notices,available?(inboxFilter==='unread'?'No unread updates.':inboxFilter==='dismissed'?'No dismissed updates.':'No recent notifications. Open the full Inbox for approvals, invitations and account steps.'):'Recent updates could not be checked. Open the full Inbox for account steps.');
   moreNotices.hidden=visible.length<=displayLimit;moreNotices.textContent=inboxLimit===3?'Show '+plural(visible.length-displayLimit,accountActions.length?'more Inbox item':'more update'):'Show fewer updates';
  });
  inboxNote.textContent=unconfirmedActions.size?'An action could not be confirmed. Refresh the inbox before trying again.':hasInbox?(inboxCursor?'Earlier updates. ':'')+'Inbox changes apply only to you. Dismissing an update keeps its work available.':native?'Notifications for this recipient and destination.':'Recipient notifications are unavailable. Research state updates appear under Activity.';
  renderNoticeReceipt();renderInboxPages();
 }
 function applyInbox(snapshot){if(!snapshot||snapshot.context!==chosen||!Array.isArray(snapshot.notices)||!sameActivityBinding(snapshot.binding,binding))throw Error('Inbox belongs to a different destination.');noticeRows=snapshot.notices;hasInbox=true;inboxMore=!!snapshot.more;inboxNext=snapshot.cursor||'';inboxNote.textContent=(inboxCursor?'Earlier updates. ':'')+'Inbox changes apply only to you. Dismissing an update keeps its work available.';renderInbox();}
 async function actOnNotification(row,action,control){
  const current=native?.notifications.find(item=>item.id===row.id);if(destroyed||suspended||!control.isConnected||!current||current.revision!==row.revision||!current.allowedActions.includes(action)||actionsPending.has(row.id)||unconfirmedActions.has(row.id))return;
  const context=chosen,ticket=scopeEpoch,expected=binding,controller=new AbortController();let failed=false;actionsPending.add(row.id);actionRequests.add(controller);for(const node of control.closest('li').querySelectorAll('button'))node.disabled=true;inboxNote.textContent='Confirming the recipient action…';
  try{await notificationAction(context,{id:row.id,revision:row.revision,action,requestId:crypto.randomUUID()},{signal:controller.signal});if(destroyed||ticket!==scopeEpoch||!sameActivityBinding(expected,binding))return;await refresh();}
  catch(error){if(!destroyed&&ticket===scopeEpoch){if(authorityRefused(error)){refuseAccess();return;}failed=true;unconfirmedActions.set(row.id,{});}}
  finally{actionRequests.delete(controller);if(!destroyed&&ticket===scopeEpoch){actionsPending.delete(row.id);nativeSignature='';if(native)renderNative(native);if(failed)inboxNote.textContent='The action could not be confirmed. Refresh the inbox before trying again.';}}
 }
 function closeDetail(focus=false,{preserveIntent=false}={}){
  if(!preserveIntent)detailIntent=null;
  detailEpoch++;detailRequest?.abort();detailRequest=null;const old=providerMount;providerMount=null;selected=null;detail.replaceChildren();detail.hidden=true;detail.classList.remove('ak-rail-connection');delete detail.dataset.expanded;detail.setAttribute('aria-label','Exact work details');rail.dataset.connectionOpen='false';
  if(focus){const key=detailReturn?.dataset.focusKey,target=detailReturn?.isConnected?detailReturn:key?[...rail.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey===key):null;(target||launch).focus();}detailReturn=null;try{old?.dispose();}catch{}
 }
 async function openDetail(kind,id,control,{focus=true,expanded=false}={}){
  if(destroyed||suspended||!binding||!chosen)return;const context=chosen,expected=binding,ownerEpoch=scopeEpoch;closeDetail();detailReturn=control||launch;selected={kind,id};const ticket=detailEpoch,controller=new AbortController();detailRequest=controller;
  const current=()=>!destroyed&&!suspended&&!controller.signal.aborted&&ticket===detailEpoch&&ownerEpoch===scopeEpoch&&context===chosen&&sameActivityBinding(expected,binding);
  detail.hidden=false;const heading=element('h3','Checking this work…'),close=button('Close details',()=>closeDetail(true));detail.append(heading,close);pinned=true;setOpen(true);detail.tabIndex=-1;if(focus)detail.focus();
  try{
   const snapshot=kind==='research'?await activity(context,{jobId:id,signal:controller.signal}):await accountActivity(context,{taskId:id,signal:controller.signal});if(!current())return;
   if(!sameActivityBinding(expected,snapshot.binding)){invalidate('The account changed while opening this work.');void refresh();return;}
   if(kind==='research'){
    const row=snapshot.activities[0];if(!row||row.id!==id)throw Error('Missing exact research run.');selected.revision=row.revision;heading.textContent=row.title;const retained=localLink('Open retained research run',row.href);detail.append(element('p','Research · '+(phases[row.state]||row.state)),retained);
    if(snapshot.providerContinuation?.canContinue===true){
     const target=element('div');target.className='ak-rail-provider';const module=await providerView();if(!current())return;
     const mounted=module.mountForeignSignIn(target,{context,jobId:id,revision:row.revision,capability:snapshot.providerContinuation,binding:expected,destinationLabel:rows.find(destination=>destination.context===context)?.label||context,workTitle:row.title,returnHref:retained.getAttribute('href'),expanded,
      onDismiss:()=>{if(current())closeDetail(true);},
      onExpandedChange:value=>{if(current()){selected.expanded=value===true;detail.dataset.expanded=String(selected.expanded);}},
      onChange:()=>{if(current())void refresh();},onAccessRefused:error=>{if(current()&&(authorityRefused(error)||error?.code==='binding_changed'))refuseAccess();}});
     if(current()){providerMount=mounted;selected.provider=true;selected.expanded=expanded===true;detail.dataset.expanded=String(selected.expanded);detail.classList.add('ak-rail-connection');detail.setAttribute('aria-label','Sign-in for '+row.title);detail.replaceChildren(target);rail.dataset.connectionOpen=String(open);}else try{mounted?.dispose();}catch{}
    }
    else if(snapshot.providerContinuation?.reason)detail.append(element('p',snapshot.providerContinuation.reason));
   }else{
    const root=snapshot.tasks.find(row=>row.kind==='task'&&row.taskId===id);if(!root)throw Error('Missing exact native task.');heading.textContent=root.title;detail.append(element('p',(root.workKind==='model'?'Model run':'Native task')+' · '+root.phase));
    if(root.workKind==='model'){
     const module=await modelView();if(!current())return;const target=element('div');target.className='ak-rail-model';detail.append(target);
     const mounted=module.mountModelRun(target,{context,binding:expected,taskId:id,onChange:()=>{if(current())void refresh();},onAccessRefused:error=>{if(current()&&authorityRefused(error))refuseAccess();}});
     if(current())providerMount=mounted;else mounted?.dispose();return;
    }
    const children=element('ul');for(const row of snapshot.tasks.filter(row=>row.kind==='subagent'))children.append(element('li','Subagent · '+row.title+' · '+row.phase));detail.append(children);
    if(root.reconciliationRequired)detail.append(element('p','The retained outcome needs reconciliation.'));
    for(const row of snapshot.outputs){const evidence=element('section');evidence.append(element('h4',row.title),element('p','Receipt evidence · '+row.kind),element('code',row.digest));detail.append(evidence);}
    if(!snapshot.outputs.length)detail.append(element('p','No retained receipt evidence for this task.'));
   }
  }catch(error){if(current()){if(authorityRefused(error)){refuseAccess();return;}heading.textContent='This work could not be checked.';detail.append(element('p','Your retained work remains with its native owner. Close this view and try again.'));}}
 }
 async function changeNotice(row,action){
  if(pendingNotice||!hasInbox||!canRead()||destroyed||suspended||typeof setNotice!=='function'||!binding)return;
  const active=document.activeElement,focused=notices.contains(active)||receipt.contains(active),operation={context:chosen,binding,id:row.id,version:row.version,revision:row.revision,action,requestId:crypto.randomUUID(),controller:new AbortController(),busy:true,focusKey:active?.dataset.focusKey};pendingNotice=operation;lastDismissed=null;noticeMessage='';renderInbox();if(focused)receipt.focus({preventScroll:true});await submitNotice(operation);
 }
 async function submitNotice(operation){
  const current=()=>!destroyed&&!suspended&&pendingNotice===operation&&chosen===operation.context&&(!binding||sameActivityBinding(operation.binding,binding))&&!operation.controller.signal.aborted;
  try{
   const result=await setNotice(operation.context,{expectedBinding:operation.binding,id:operation.id,version:operation.version,revision:operation.revision,action:operation.action,requestId:operation.requestId},{signal:operation.controller.signal});if(!current())return;
   if(result?.context!==operation.context||!sameActivityBinding(result.binding,operation.binding)||result.requestId!==operation.requestId||result.appliedVersion!==operation.version||result.appliedRevision!==operation.revision+1||!result.notice||result.notice.id!==operation.id)throw Error('The saved Inbox result is incomplete.');
   const at=noticeRows.findIndex(row=>row.id===result.notice.id);if(at<0)noticeRows.push(result.notice);else noticeRows[at]=result.notice;
   epoch++;request?.abort();request=null;clearTimeout(timer);timer=null;refreshButton.disabled=false;rail.removeAttribute('aria-busy');if(rows.length)scope.disabled=false;const focused=receipt.contains(document.activeElement);pendingNotice=null;if(operation.action==='dismiss'&&result.notice.version===operation.version&&result.notice.revision===result.appliedRevision)lastDismissed={id:result.notice.id,version:operation.version};renderInbox();if(focused&&receipt.hidden){const target=[...notices.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey===operation.focusKey)||[...notices.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey==='notice:queue:'+operation.id)||inboxFilterButtons.get(inboxFilter);target?.focus({preventScroll:true});}void refresh();
  }catch(error){if(!current())return;if(authorityRefused(error)){refuseAccess();return;}if(['invalid_request','not_found','version_conflict','request_conflict'].includes(error.code)){pendingNotice=null;lastDismissed=null;noticeMessage=error.status===409?'This update changed. Its current version is shown below.':'The Inbox change was refused. Check the current update before trying again.';renderInbox();void refresh();return;}operation.busy=false;renderInbox();}
 }
 async function recoverNotice(){
  const previous=pendingNotice;if(!previous||previous.busy||destroyed||suspended||!canRead()||chosen!==previous.context)return;
  previous.controller?.abort();const operation={...previous,controller:new AbortController(),busy:true};pendingNotice=operation;renderInbox();
  // The native owner resolves an exact idempotency identity. A new mutation is
  // never invented to recover a response that may have been lost.
  await submitNotice(operation);
 }
 async function refresh(){
  if(destroyed||suspended||document.hidden)return;
  const state=navigationState();if(!canRead()){anonymous(state);return;}
  const ticket=++epoch,reconciling=new Map(unconfirmedActions);request?.abort();const controller=new AbortController();request=controller;clearTimeout(timer);refreshButton.disabled=true;scope.disabled=true;rail.setAttribute('aria-busy','true');renderInboxPages();
  const current=()=>!destroyed&&!suspended&&!controller.signal.aborted&&ticket===epoch;
  const readOwner=read=>Promise.resolve().then(read).catch(error=>{if(current()&&authorityRefused(error))refuseAccess();throw error;});
  try{
   const all=await destinations({signal:controller.signal});if(!current())return;
   if(!Array.isArray(all)||!all.length)throw Object.assign(Error('No destination is available to this account.'),{status:403});
   if(firstChoice){const wanted=workspaceContext()||new URL(location.href).searchParams.get('context');chosen=wanted&&contextPattern.test(wanted)?wanted:'self';firstChoice=false;}
   if(chosen&&!all.some(row=>row.context===chosen)){chosen='';clearPrivate('Choose a destination currently available to your account.');renderChoices(all);return;}
   renderChoices(all);if(!chosen){status.textContent='Choose an account or organization to see its activity.';return;}
   const reading=chosen;
   const workRead=typeof tasks==='function'?readOwner(()=>tasks(reading,{signal:controller.signal})):null;
   // These Queue reads can each rebuild the same stale account index.
   const outputsPending=typeof tasks==='function'?Promise.resolve(workRead).catch(()=>null).then(()=>current()&&reading===chosen?readOwner(()=>tasks(reading,{signal:controller.signal,outputsOnly:true})):null):null;
   const agentsPending=typeof agents==='function'?Promise.resolve(outputsPending).catch(()=>null).then(()=>current()&&reading===chosen?readOwner(()=>agents(reading,{signal:controller.signal})):null):null;
   const computePending=reading==='self'&&typeof compute==='function'?Promise.resolve(agentsPending).catch(()=>null).then(()=>current()&&reading===chosen?readOwner(()=>compute(reading,{signal:controller.signal})):null):null;
   const [researchRead,nativeRead,taskRead,inboxRead,outputsRead,needsRead,agentsRead,computeRead]=await Promise.allSettled([
    typeof activity==='function'?readOwner(()=>activity(reading,{signal:controller.signal})):null,
    typeof accountActivity==='function'?readOwner(()=>accountActivity(reading,{signal:controller.signal})):null,
    workRead,
    typeof inbox==='function'?readOwner(()=>inbox(reading,{signal:controller.signal,...(inboxCursor?{cursor:inboxCursor}:{})})):null,
    outputsPending,
    typeof needs==='function'?readOwner(()=>needs(reading,{signal:controller.signal})):null,
    agentsPending,
    computePending
   ]);if(!current()||reading!==chosen)return;
   const confirmed=[researchRead,nativeRead,taskRead,inboxRead,outputsRead,needsRead,agentsRead,computeRead].filter(result=>result.status==='fulfilled'&&result.value).map(result=>result.value);
   if(!confirmed.length)throw Error('No activity owner could be reached.');
   if(confirmed.some(value=>value.context!==chosen||!value.binding)||confirmed.some(value=>!sameActivityBinding(confirmed[0].binding,value.binding))){
    const retry=bindingRetries++===0;invalidate('The account changed between owner reads. Checking it again…');if(retry)queueMicrotask(()=>void refresh());else status.textContent='The current account could not be confirmed. Use Refresh to try again.';return;
   }
   const nextBinding=confirmed[0].binding;
   if(binding&&!sameActivityBinding(binding,nextBinding)){scopeEpoch++;clearPrivate('Opening current account activity…');renderChoices(all);}
   if(pendingNotice&&!sameActivityBinding(pendingNotice.binding,nextBinding)){pendingNotice.controller?.abort();pendingNotice=null;lastDismissed=null;noticeMessage='';}
   if(automaticLayout&&desktop.matches&&!wasBound&&!open){pinned=true;setOpen(true);}wasBound=true;delete rail.dataset.visitor;
   binding=nextBinding;bindingRetries=0;inboxScope.textContent=rows.find(row=>row.context===chosen)?.label||'';launchState.textContent=inboxScope.textContent||'Your activity';modelLink.href='/s/models/all?'+new URLSearchParams({context:chosen});inboxPage.href='/inbox?'+new URLSearchParams({context:chosen});
   window.dispatchEvent(new CustomEvent('bonfyre:activity-context-changed',{detail:{context:chosen,binding,label:inboxScope.textContent}}));
   renderResearch(researchRead.status==='fulfilled'?researchRead.value:null);if(!research&&selected?.kind==='research')closeDetail();
   queueSnapshot=taskRead.status==='fulfilled'?taskRead.value:null;
   outputSnapshot=outputsRead.status==='fulfilled'?outputsRead.value:null;
   if(needsRead.status==='fulfilled'&&needsRead.value){needsRows=needsRead.value.items;hasNeeds=true;}else{needsRows=[];hasNeeds=false;}if(inboxRead.status==='fulfilled'&&inboxRead.value)applyInbox(inboxRead.value);else{noticeRows=[];hasInbox=false;inboxMore=false;inboxNext='';}
   if(nativeRead.status==='fulfilled'&&nativeRead.value){for(const [id,attempt]of reconciling)if(unconfirmedActions.get(id)===attempt)unconfirmedActions.delete(id);renderNative(nativeRead.value);}
   else{native=null;nativeSignature='';taskStatus.textContent='Native task activity is unavailable.';empty(taskList,'No current native task state could be confirmed.');if(selected?.kind==='fabric')closeDetail();}
   renderAgents(agentsRead.status==='fulfilled'?agentsRead.value:null);
   renderCompute(computeRead.status==='fulfilled'?computeRead.value:null,agentsRead.status==='fulfilled'?agentsRead.value:null);
   renderWorkProjection({researchError:researchRead.status==='rejected',taskError:taskRead.status==='rejected'});renderInbox();renderOutputs();
   if(detailIntent&&!selected){const intent=detailIntent;detailIntent=null;if(intent.context===chosen&&sameActivityBinding(intent.binding,binding)&&research){const control=[...runList.querySelectorAll('[data-focus-key]')].find(node=>node.dataset.focusKey==='inspect:research:'+intent.id);void openDetail(intent.kind,intent.id,control,{focus:false,expanded:intent.expanded});}}
   void refreshPush().then(()=>resumePushIntent()).catch(()=>{});
   const match=location.hash.match(/^#fabric-task-([A-Za-z0-9][A-Za-z0-9_.:-]{0,159})$/);if(native&&match&&!selected&&handledHash!==location.hash){handledHash=location.hash;void openDetail('fabric',match[1]);}
  }catch(error){
   if(!current())return;if(authorityRefused(error)){refuseAccess();return;}clearPrivate('Activity could not be refreshed.',{reset:false});renderNoticeReceipt();
   if(chosen)status.append(' Your saved work can be opened from the research page.');
   empty(notices,'No current update could be confirmed.');
  }finally{
   if(current()){request=null;refreshButton.disabled=false;rail.removeAttribute('aria-busy');if(rows.length)scope.disabled=false;renderInboxPages();timer=setTimeout(()=>void refresh(),open||!badge.hidden?Math.min(intervalMs,5000):intervalMs);}
  }
 }
 on(scope,'change',()=>{const next=scope.value;if(!rows.some(row=>row.context===next))return;chosen=next;invalidate('Opening this destination…');void refresh();});
 on(rail,'pointerenter',event=>{if(!desktop.matches||event.pointerType==='touch'||pinned)return;clearTimeout(closeTimer);hoverTimer=setTimeout(()=>setOpen(true),150);});
 on(rail,'pointerleave',event=>{if(event.pointerType==='touch')return;clearTimeout(hoverTimer);if(!pinned&&!rail.contains(document.activeElement))closeTimer=setTimeout(()=>setOpen(false),280);});
 on(rail,'focusout',()=>queueMicrotask(()=>{if(!destroyed&&!pinned&&!rail.matches(':hover')&&!rail.contains(document.activeElement))setOpen(false);}));
 on(document,'keydown',event=>{if(event.key!=='Escape'||!open||event.defaultPrevented)return;const within=rail.contains(event.target);if(!within&&document.querySelector('dialog[open],[role="dialog"][aria-modal="true"]'))return;if(selected&&detail.contains(event.target)){closeDetail(true);event.preventDefault();return;}automaticLayout=false;pinned=false;setOpen(false,{focus:within});if(within)event.preventDefault();});
 on(document,'pointerdown',event=>{if(open&&!pinned&&!rail.contains(event.target))setOpen(false);});
 on(window,'bonfyre:account-changed',()=>{firstChoice=true;chosen='';invalidate();void refresh();});
 on(window,'bonfyre:workspace-changed',()=>{firstChoice=true;chosen='';invalidate();void refresh();});
 on(window,'bonfyre:work-changed',event=>{if(event.detail?.context===chosen)void refresh();});
 on(window,'bonfyre:activity-context-request',event=>{const next=event.detail?.context;if(next===chosen||!rows.some(row=>row.context===next))return;chosen=next;invalidate('Opening this destination…');void refresh();});
 if(navigator.serviceWorker)on(navigator.serviceWorker,'message',event=>{if(event.data?.type==='aurekai-push-subscription-change')void refreshPush();});
 on(window,'pagehide',event=>{suspended=true;suspendPrivate();pinned=false;setOpen(false);if(!event.persisted)destroy();});
 on(window,'pageshow',()=>{suspended=document.hidden;invalidate(undefined,{reset:false,preserveDetailIntent:true});if(!suspended)void refresh();});
 on(document,'visibilitychange',()=>{suspended=document.hidden;if(suspended)suspendPrivate();else{invalidate(undefined,{reset:false,preserveDetailIntent:true});void refresh();}});
 on(window,'focus',()=>{if(!suspended)void refresh();});
 on(window,'hashchange',()=>{handledHash=location.hash;const match=location.hash.match(/^#fabric-task-([A-Za-z0-9][A-Za-z0-9_.:-]{0,159})$/);if(match&&native)void openDetail('fabric',match[1]);});
 const slot=document.querySelector('[data-account-slot]');
 const observer=new MutationObserver(()=>{const next=slot?.querySelector('.ak-session'),context=workspace?.dataset.activityContext;if(next===navigationNode&&context===navigationContext)return;navigationNode=next;navigationContext=context;firstChoice=true;chosen='';invalidate();void refresh();});
 if(slot){observer.observe(slot,{childList:true,subtree:true});navigationNode=slot.querySelector('.ak-session');}
 if(workspace)observer.observe(workspace,{attributes:true,attributeFilter:['data-activity-context']});
 on(desktop,'change',()=>{if(selected?.provider===true){pinned=true;setOpen(true);return;}if(automaticLayout)pinned=desktop.matches&&canRead();setOpen(automaticLayout?pinned:open);});
 function destroy(){if(destroyed)return;destroyed=true;epoch++;scopeEpoch++;request?.abort();clearPrivate();clearTimeout(timer);clearTimeout(hoverTimer);clearTimeout(closeTimer);observer.disconnect();for(const off of cleanup)off();document.body.style.paddingInlineEnd=initialPadding;rail.remove();}
 if(desktop.matches&&workspace){pinned=true;setOpen(true);}else void refresh();return {destroy,refresh,element:rail};
}
// Navigation is already transported by this existing app module. Keep one
// executable copy instead of replicating the entire script inside C pages.
function mountNativeAccountNavigation(){
 for(const header of document.querySelectorAll('.ak-bar')){
  if(header.dataset.bfNavMounted==='1')continue;
  header.dataset.bfNavMounted='1';
  (function(){"use strict";const m=header,s=m&&m.querySelector("[data-account-slot]");if(!s)return;s.dataset.accountRefresh="2";let b=0,d=null,y=null,x=null,k=0,u=!1;function q(){let t=location.pathname+location.search+location.hash;if(location.pathname==="/login"||location.pathname==="/signup"){const e=document.querySelector('form input[name="next"]');t=e?e.value:"/account"}return location.pathname==="/account/navigation"||!t||new TextEncoder().encode(t).length>=4096||Array.from(t).some(e=>e.charCodeAt(0)<32||e.charCodeAt(0)===127)?"/account":t}function g(t){t.querySelectorAll("a[href]").forEach(e=>{const n=new URL(e.getAttribute("href"),location.origin);n.origin===location.origin&&["/login","/signup","/session/continue"].includes(n.pathname)&&(n.searchParams.set("next",q()),e.setAttribute("href",n.pathname+n.search))})}function A(t){var n;const e=(n=location.hostname.match(/(?:^|\.)aurekai\.(work|life|live|shop)$/))==null?void 0:n[1];e&&m.querySelectorAll("a[href]").forEach(r=>{var p;const i=r.dataset.territoryHref||r.getAttribute("href"),a=new URL(i,location.origin),l=r.hasAttribute("data-public-profile")||r.hasAttribute("data-public-profile-menu"),o=(p=a.hostname.match(l?/(?:^|\.)aurekai\.(work|life|live|shop)$/:/^aurekai\.(work|life|live|shop)$/))==null?void 0:p[1];if(!o||o===e||a.protocol!=="https:"||a.port||a.username||a.password)return;if(r.dataset.territoryHref=i,!t){r.setAttribute("href",i);return}const c=new URL("https://aurekai."+e+"/session/switch");c.searchParams.set("to",o),c.searchParams.set("next",l?a.href:a.pathname+a.search+a.hash),r.setAttribute("href",c.href)})}function v(){var o;if(location.pathname!=="/"||!/^aurekai\.(work|life|live|shop)$/.test(location.hostname))return;const t=s.querySelector('.ak-session[data-session-state="signed-in"]');let e=document.querySelector("[data-account-continuation]");if(!t){e==null||e.remove();return}const n=document.querySelector("main");if(!n)return;if(!e){e=document.createElement("section"),e.dataset.accountContinuation="",e.className="ak-returning",e.setAttribute("aria-label","Continue your work");const c=document.createElement("style");c.textContent=".ak-returning{margin:24px auto;width:min(1360px,calc(100\x25 - 40px));padding:24px 28px;border:1px solid #63826d;border-radius:20px;background:linear-gradient(120deg,#18352b,#31503a);color:#f5f8e9;display:flex;align-items:center;justify-content:space-between;gap:24px;box-sizing:border-box}.ak-returning h2{font:400 clamp(27px,3vw,40px)/1.08 Georgia,serif;margin:0 0 8px;color:inherit}.ak-returning p{margin:0;color:#d4e5ce;font:14px/1.5 system-ui}.ak-returning nav{display:flex;flex-wrap:wrap;gap:9px}.ak-returning a{display:inline-flex;align-items:center;min-height:44px;padding:0 17px;border-radius:24px;border:1px solid #b8d6a780;background:#e8f1dc;color:#183322;font:600 13px system-ui;text-decoration:none;transition:background .2s,transform .2s}.ak-returning a:hover{background:#fff;transform:translateY(-2px)}.ak-returning a:focus-visible{outline:3px solid #c7e295;outline-offset:3px}@media(max-width:760px){.ak-returning{flex-direction:column;align-items:stretch;padding:21px;width:calc(100\x25 - 28px)}.ak-returning nav{gap:8px}}@media(prefers-reduced-motion:reduce){.ak-returning a{transition:none;transform:none}}";const p=document.createElement("div"),h=document.createElement("h2"),C=document.createElement("p");h.dataset.returningName="",C.textContent="Your profile, saved work and publishing tools stay connected across Aurekai.",p.append(h,C);const S=document.createElement("nav");S.setAttribute("aria-label","Your Aurekai"),e.append(c,p,S),n.prepend(e)}e.querySelector("[data-returning-name]").textContent="Welcome back"+((o=t.querySelector(".ak-user-name"))!=null&&o.textContent?", "+t.querySelector(".ak-user-name").textContent:"")+".";const r=e.querySelector("nav"),i=t.querySelector("[data-public-profile-menu]"),a=[[i==null?void 0:i.getAttribute("href"),"Public profile"],["/apps?context=self","Continue your work"],["/cms/","Create and publish"]].filter(c=>c[0]),l=a.map(c=>c.join("|")).join(";");if(r.dataset.links!==l){r.replaceChildren();for(const[c,p]of a){const h=document.createElement("a");h.href=c,h.textContent=p,r.append(h)}r.dataset.links=l}}function E(t,e){const n=document.createElement("div");n.className="ak-session",n.dataset.sessionState=t;const r=document.createElement("a");r.className="ak-account",r.href="/account";const i=document.createElement("span");if(i.textContent="Account",r.append(i),n.append(r),e){const a=document.createElement("button");a.type="button",a.className="ak-account",a.dataset.sessionRetry="",a.textContent="Retry",a.setAttribute("aria-label","Account menu unavailable. Check again"),n.append(a)}s.replaceChildren(n),x=null,A(!1),v()}function w(){b++,clearTimeout(y),y=null;const t=d;d=null,t==null||t.controller.abort(),s.removeAttribute("aria-busy"),E("checking",!1)}async function L(t){if(!t.ok||!/^text\/html(?:;|$)/i.test(t.headers.get("content-type")||""))throw Error("navigation response");const e=t.headers.get("content-length");if(e&&(!/^\d+$/.test(e)||Number(e)>32768))throw Error("navigation size");const n=t.body.getReader(),r=[];let i=0;try{for(;;){const o=await n.read();if(o.done)break;if(i+=o.value.byteLength,i>32768)throw await n.cancel(),Error("navigation size");r.push(o.value)}}finally{n.releaseLock()}const a=new Uint8Array(i);let l=0;for(const o of r)a.set(o,l),l+=o.byteLength;return new TextDecoder("utf-8",{fatal:!0}).decode(a)}function f(t){if(u||document.hidden)return;if(d)return d.promise;if(!t&&Date.now()-k<15e3){g(s);return}clearTimeout(y);const e={epoch:++b,controller:new AbortController,promise:null};d=e,s.setAttribute("aria-busy","true");const n=setTimeout(()=>e.controller.abort(),8e3);return e.promise=(async()=>{try{const r=await fetch("/account/navigation",{credentials:"same-origin",cache:"no-store",redirect:"error",headers:{Accept:"text/html"},signal:e.controller.signal}),i=await L(r);if(e.epoch!==b||u||e.controller.signal.aborted)return;const l=new DOMParser().parseFromString(i,"text/html").querySelectorAll(".ak-session"),o=l[0];if(l.length!==1||!["signed-in","anonymous"].includes(o.dataset.sessionState)||o.querySelector("script,iframe,object,embed,form,input"))throw Error("navigation fragment");x!==i?(g(o),s.replaceChildren(document.importNode(o,!0)),x=i):g(s),A(o.dataset.sessionState==="signed-in"),v(),k=Date.now()}catch{e.epoch===b&&!u&&(E("unavailable",!0),k=Date.now())}finally{clearTimeout(n),d===e&&(d=null,s.removeAttribute("aria-busy"),!u&&!document.hidden&&(y=setTimeout(()=>f(!0),6e4)))}})(),e.promise}m.addEventListener("keydown",t=>{t.key==="Escape"&&m.querySelectorAll("details[open]").forEach(e=>{e.open=!1})}),m.addEventListener("click",t=>{t.target.closest("[data-session-retry]")&&(t.preventDefault(),f(!0))}),window.addEventListener("bonfyre:account-changed",()=>{w(),f(!0)}),window.addEventListener("pagehide",()=>{u=!0,w()}),window.addEventListener("pageshow",t=>{u=!1,f(t.persisted)}),document.addEventListener("visibilitychange",()=>{u=document.hidden,u?w():f(!0)}),window.addEventListener("focus",()=>{f(!1)}),document.addEventListener("DOMContentLoaded",()=>{g(s),v()},{once:!0}),g(s),f(!0)})();
 }
}

if(typeof document!=='undefined'){
 const start=()=>{mountNativeAccountNavigation();if(document.querySelector('[data-account-slot],[data-native-workspace][data-activity-context]'))mountAurekaiActivity();};
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
}
