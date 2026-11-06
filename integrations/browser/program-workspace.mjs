import {packet,activityBinding,sameActivityBinding} from './aurekai-activity-source.mjs';
import {workspaceElement as el,workspaceLink as link,workspaceButton as button,mountCollection,mountTabs,mountCommandPalette} from './interaction-collection.mjs';
import {openDestinationStore} from './workspace-store.mjs';
import {compareRecords,renderChanges} from './record-comparison.mjs';
import {parseContinuationManifest,projectSavedView} from './workspace-continuation.mjs';
import {mountProgramSourceWork} from './program-source-work.mjs';
import {programServiceContinuations,programToolChoices} from './program-service-continuations.mjs';
export {programToolChoices} from './program-service-continuations.mjs';

const contextPattern=/^(self|(institution|household):[a-z0-9-]{1,63})$/;
const instancePattern=/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/;
const preparedPattern=/^pws-[a-f0-9]{32}-[a-z0-9-]{1,64}$/;
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const requireValue=(yes,message)=>{if(!yes)throw Error(message);};
const requireBinding=(yes,message='The signed-in account changed. Refresh Programs.')=>{if(!yes)throw Object.assign(Error(message),{code:'binding_changed'});};
const accessFailure=error=>['access_required','binding_changed','invalid_binding'].includes(error?.code);
const bounded=(value,max)=>typeof value==='string'&&value.length<=max&&!value.includes('\0');
const list=(value,max=256)=>Array.isArray(value)&&value.length<=max;
const templateId=value=>bounded(value,160)&&/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value);

export async function programRequest(context,operation,{instance='',body,request,signal}={}){
 requireValue(contextPattern.test(context)&&['catalog','read','review','prepare','change','change-receipt','source-status','source-plan','source-recover','source-read','source-start','source-item','source-decision'].includes(operation),'Choose an admitted program destination.');
 requireValue(!instance||operation==='read'&&instancePattern.test(instance),'Choose an exact saved program.');
 const query=new URLSearchParams({context,operation});if(instance)query.set('instance',instance);
 const answer=await packet('/apps/_programs?'+query,{body,request,signal,allowErrorPacket:true});
 requireValue(list(answer,3)&&answer.length===3&&answer[0]===1&&typeof answer[1]==='boolean'&&object(answer[2]),'The program owner returned an incomplete answer.');
 const value=answer[2];
 if(!answer[1])throw Object.assign(Error(bounded(value.message,2000)?value.message:'The program owner could not complete this request.'),{code:value.code||'unavailable'});
 requireValue(value.operation===operation,'The program reply belongs to another operation.');
 let binding;try{binding=activityBinding(value.binding,context);}catch(error){throw Object.assign(error,{code:'binding_changed'});}
 return {...value,binding};
}

export function programTemplate(value){
 requireValue(object(value)&&templateId(value.template_id)&&bounded(value.name,512)&&bounded(value.description??'',24000)&&bounded(value.archetype,128)&&hex(value.digest),'A program template is incomplete.');
 for(const key of ['subject_kinds','semantic_slots','capability_families','owner_bindings','funding_candidates'])requireValue(list(value[key],64)&&value[key].every(x=>bounded(x,2000)), 'A program template contains an incomplete requirement.');
 return value;
}
export function installedProgramTools(reply,binding){
 requireValue(list(reply,4)&&reply.length===4&&reply[0]===1&&list(reply[3],64),'The installed tools could not be read.');
 requireBinding(reply[1]===binding.subject&&reply[2]===binding.scope,'The installed tools belong to a different destination. Refresh Programs.');
 const seen=new Set();
 return reply[3].map(row=>{
  requireValue(list(row,6)&&row.length===6&&/^ws-[a-f0-9]{40}$/.test(row[0])&&bounded(row[1],96)&&/^[a-z0-9-]+(?:\/[a-z0-9-]+)*\/$/.test(row[2])&&hex(row[3])&&bounded(row[4],256)&&typeof row[5]==='boolean'&&!seen.has(row[0]),'An installed tool has an incomplete binding.');
  seen.add(row[0]);return {id:row[0],pack:row[1],entry:row[2],release:row[3],label:row[4],ready:row[5],href:'/apps/'+row[0]+'/'+row[2]};
 });
}
const human=value=>String(value).replaceAll('_',' ').replaceAll('-',' ');
const ownerLabel=value=>({CapabilityClosure:'Capability check',Finance:'Finance review'})[value]||human(value||'Owner review');
function requirementTitle(row,instance){if(row.kind==='funding_scope'){const funding=instance.funding.find(item=>row.requirement_id===item.package_id+'-scope');return (funding?.funding_program||'Funding')+' · scope and costs';}const marker=row.requirement_id.indexOf('-needs-');if(row.kind==='capability'&&marker>=0)return 'Prepare '+human(row.requirement_id.slice(marker+7));return row.statement.length>110?row.statement.slice(0,107)+'…':row.statement;}
const done=value=>['met','complete','completed','closed'].includes(value);
const jsonCopy=value=>JSON.parse(JSON.stringify(value));
const sameValue=(left,right)=>JSON.stringify(left)===JSON.stringify(right);
const storedDate=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)?value:'';
const dateLabel=value=>storedDate(value)?new Intl.DateTimeFormat(undefined,{year:'numeric',month:'short',day:'numeric',timeZone:'UTC'}).format(new Date(value+'T12:00:00Z')):'No deadline set';
const recordTitle=row=>{const value=row?.value;return String(value?.title||value?.name||value?.label||row.key).slice(0,500);};
function savedWorkHref(row){
 requireValue(object(row)&&/^ws-[a-f0-9]{40}$/.test(row.workspace_binding)&&bounded(row.record_key,511)&&row.record_key.length>0&&Number.isSafeInteger(row.record_revision)&&row.record_revision>0&&hex(row.release),'An attached record has an incomplete identity.');
 const expected='/apps/'+row.workspace_binding+'/_views?'+new URLSearchParams({key:row.record_key,revision:String(row.record_revision),release:row.release});
 requireValue(row.href===expected,'The attached record has an inconsistent continuation.');return expected;
}
const newTab=(label,href)=>{const node=link(label,href);node.target='_blank';node.rel='noopener noreferrer';node.setAttribute('aria-label',label+' — opens in another tab');return node;};
const knownChangeRefusal=error=>['revision_changed','instance_changed','template_changed','requirement_unavailable','package_unavailable','source_collection_unavailable','funding_overlap','duplicate_declaration','invalid_change','work_unavailable','work_revision_changed','work_view_unavailable','work_target_unavailable','work_link_unavailable','unreviewed_instance'].includes(error?.code);
async function recordViews(tool,request){const response=await request('/apps/'+tool.id+'/CONTINUATIONS.tsv',{credentials:'same-origin',cache:'no-store',redirect:'error'});if(!response.ok)throw Object.assign(Error('The installed tool’s saved-record views could not be read.'),{status:response.status});const reader=response.body?.getReader();requireValue(reader,'The installed tool’s saved-record views are unavailable.');const pieces=[];let bytes=0;try{for(;;){const part=await reader.read();if(part.done)break;bytes+=part.value.length;requireValue(bytes<=32768,'The installed tool’s saved-record view declaration exceeds its limit.');pieces.push(part.value);}}finally{await reader.cancel();reader.releaseLock();}const value=new Uint8Array(bytes);let offset=0;for(const piece of pieces){value.set(piece,offset);offset+=piece.length;}return parseContinuationManifest(new TextDecoder('utf-8',{fatal:true}).decode(value));}
let surfaceId=0;

export function mountPrograms(root,{call=programRequest,readPacket=packet}={}){
 const context=root.dataset.context,canWrite=root.dataset.canWrite==='true';
 const destinationLabel=root.querySelector('#program-context option:checked')?.textContent||(context==='self'?'your personal workspace':'this workspace');
 requireValue(contextPattern.test(context),'Choose one account or organization.');
 const target=root.querySelector('[data-program-app]');requireValue(target,'The program surface is missing.');
 let generation=0,controller=null,binding=null,templates=[],instances=[],tools=[],selected='',mode='saved',review=null,busy=false,disposed=false,suspended=false,uncertain=false,toolsUnavailable=false;
 let activeInstance=null,workspaceView=null,pendingChange=null,sourceBusy=false;
 // Unsaved edits are disposable, account-bound view state. Native owners keep
 // the saved records and receipts; this map is never a second persistence store.
 const memories=new Map();
 const status=el('p','Reading this destination…','program-status');status.setAttribute('role','status');
 const toolbar=el('div','','program-toolbar'),refresh=button('Refresh',()=>void load({preserveDrafts:true})),work=link('All your tools','/apps?'+new URLSearchParams({context})),switcher=button('Browse programs',()=>setIndexOpen(layout.dataset.indexOpen!=='true'));switcher.className='program-switcher';toolbar.append(work,switcher,refresh);
 const layout=el('div','','program-layout'),aside=el('aside','','program-index'),main=el('section','','program-detail');main.tabIndex=-1;main.setAttribute('aria-label','Program details');
 aside.id='program-index-'+(++surfaceId);switcher.setAttribute('aria-controls',aside.id);setIndexOpen(true);
 const tabs=el('div','','program-tabs');tabs.setAttribute('role','group');tabs.setAttribute('aria-label','Browse programs');
 const savedTab=button('Your programs',()=>chooseMode('saved')),templateTab=button('Start from a template',()=>chooseMode('templates'));tabs.append(savedTab,templateTab);
 const searchLabel=el('label','Find a program'),search=document.createElement('input');search.type='search';search.placeholder='Search names or requirements';search.setAttribute('aria-label','Find a program');searchLabel.append(search);
 const index=el('ul','','program-list');index.setAttribute('aria-label','Programs');search.addEventListener('input',renderIndex);
 aside.append(tabs,searchLabel,index);layout.append(aside,main);target.replaceChildren(toolbar,status,layout);
 function current(ticket){return !disposed&&!suspended&&ticket===generation&&!controller?.signal.aborted;}
 function setIndexOpen(value){layout.dataset.indexOpen=String(value);switcher.setAttribute('aria-expanded',String(value));}
 function clearDetail(){workspaceView?.destroy();workspaceView=null;activeInstance=null;main.replaceChildren();}
 function invalidate(message,{keepDrafts=false}={}){generation++;controller?.abort();binding=null;review=null;templates=[];instances=[];tools=[];busy=sourceBusy=false;uncertain=false;pendingChange=null;index.replaceChildren();clearDetail();if(!keepDrafts)memories.clear();status.textContent=message;toolsUnavailable=false;controls();}
 function controls(){const blocked=busy||sourceBusy||!!pendingChange;refresh.disabled=switcher.disabled=blocked;search.disabled=blocked;savedTab.disabled=blocked;templateTab.disabled=blocked;for(const node of main.querySelectorAll('[data-program-write]'))node.disabled=!canWrite||blocked;for(const node of main.querySelectorAll('[data-program-recovery]'))node.disabled=busy||sourceBusy||(node.dataset.programRecovery==='write'&&!canWrite);workspaceView?.lock(busy||sourceBusy,busy||!!pendingChange);}
 function fail(error){if(accessFailure(error))invalidate(error.message);status.textContent=error.message||'Programs are unavailable.';status.dataset.error='true';}
 function setMode(value){mode=value;savedTab.setAttribute('aria-pressed',String(mode==='saved'));templateTab.setAttribute('aria-pressed',String(mode==='templates'));}
 function chooseMode(value){if(busy||sourceBusy||pendingChange)return;review=null;uncertain=false;selected='';setMode(value);clearDetail();setIndexOpen(true);renderIndex();}
 function renderIndex(){
  index.replaceChildren();const query=search.value.trim().toLowerCase(),rows=mode==='templates'?templates:instances;
  savedTab.textContent='Your programs'+(instances.length?' · '+instances.length:'');
  templateTab.textContent='Templates'+(templates.length?' · '+templates.length:'');
  for(const row of rows){
   const id=mode==='templates'?row.template_id:row.instance_id,name=row.name||templates.find(t=>t.template_id===row.template_id)?.name||row.template_id;
   if(query&&!(name+' '+(row.archetype||'')+' '+(row.description||'')).toLowerCase().includes(query))continue;
   const item=el('li'),open=button('',()=>void select(id));open.disabled=busy||sourceBusy||!!pendingChange;open.setAttribute('aria-current',String(id===selected));
   open.append(el('strong',name),el('small',human(row.archetype||'Program')));if(mode==='saved'&&Array.isArray(row.requirements)){const openCount=row.requirements.filter(r=>!done(r.state)).length;open.append(el('span',openCount+' open · '+(row.work_links?.length||0)+' saved records','program-index-count'));}item.append(open);index.append(item);
  }
  if(!index.childElementCount)index.append(el('li',query?'No programs match this search.':mode==='templates'?'No templates are available.':'Choose a template to prepare your first program.','program-empty'));
 }
 function names(title,rows){const section=el('section'),heading=el('h3',title),items=el('ul','','program-chips');for(const item of rows)items.append(el('li',human(item)));section.append(heading,items);return section;}
 function toolSection(roles){
  const section=el('section','','program-tools'),choices=programServiceContinuations(roles,tools,{context,toolsAvailable:!toolsUnavailable}),grid=el('div','','program-continuations');
  section.append(el('h3','Continue through Aurekai'),el('p','Each continuation keeps its own owner. Opening one does not copy this program, complete a requirement, or grant an effect.'));
  for(const choice of choices){
   const card=el('article','','program-continuation');card.dataset.continuationKind=choice.kind;card.dataset.continuationState=choice.state;
   const head=el('div','','program-continuation-head'),kind=el('span',choice.kind==='workspace'?(choice.state==='ready'?'Installed editor':choice.state==='unknown'?'Editor availability unknown':'Workspace setup'):choice.kind==='surface'?'Native surface':choice.kind==='account'?'Account prerequisite':choice.kind==='pending'?'Pending exact owner':'Unresolved facet','program-continuation-kind');
   head.append(kind);
   if(choice.href)head.append(link(choice.state==='not_installed'?'Set up '+choice.label:choice.state==='unknown'?'Review '+choice.label:choice.label,choice.href));
   else head.append(el('strong',choice.label));
   card.append(head,el('p',choice.description));
   if(choice.owner_families?.length)card.append(el('small','Owner family: '+choice.owner_families.join(', ')));
   if(choice.roles?.length)card.append(el('small','For: '+choice.roles.map(human).join(', ')));
   grid.append(card);
  }
  if(!choices.length)grid.append(el('p','No typed continuation is declared for these facets yet.','program-empty'));
  section.append(grid);
  if(toolsUnavailable)section.append(el('p','Installed editor availability could not be checked; native surfaces and account prerequisites remain usable.'));
  section.append(link('Review installed services','/apps?'+new URLSearchParams({context})),document.createTextNode(' · '),link('Set up services','/packs/setup?'+new URLSearchParams({context})));return section;
 }
 function showTemplate(template,{reviewed=false}={}){
  clearDetail();setIndexOpen(false);
  main.replaceChildren(el('p',human(template.archetype),'program-eyebrow'),el('h2',template.name),el('p',template.description||'Review the requirements before preparing this program.','program-description'));
  main.append(names('Work to prepare',template.capability_families),names('Parts of this program',template.semantic_slots));
  if(template.funding_candidates.length){const funding=names('Funding sources to investigate',template.funding_candidates);funding.append(el('p','Each source needs its own eligibility, scope and cost review.'));main.append(funding);}
  const actions=el('div','','program-actions');
  if(reviewed){
   main.append(el('p','This review is for '+destinationLabel+'. Preparing keeps a private program and its open requirements in this destination.','program-review'));
   const prepare=button(review.existing?'Open prepared program':'Prepare this program',()=>void prepareProgram(template));prepare.disabled=!canWrite||busy;actions.append(prepare,button('Back to template',()=>{review=null;showTemplate(template);}));
  }else {const eligible=template.subject_kinds.includes(context==='self'?'person':context.split(':')[0]),inspect=button('Review setup',()=>void reviewProgram(template));inspect.disabled=!canWrite||!eligible||busy;actions.append(inspect);
   if(!eligible)main.append(el('p','This template needs a destination of this kind: '+template.subject_kinds.map(human).join(', ')+'. Preparation is unavailable from this workspace.'));
   else if(!canWrite)main.append(el('p','You have read access here. A member with write access can prepare a program.'));}
  main.append(actions,toolSection(template.semantic_slots));
 }
 async function select(id){
  if(busy||sourceBusy||pendingChange)return;selected=id;review=null;uncertain=false;renderIndex();
  if(mode==='templates'){showTemplate(templates.find(t=>t.template_id===id));return;}
  await readInstance(id);
 }
 function memory(id){if(!memories.has(id))memories.set(id,{tab:'work',requirement:'',funding:'',drafts:new Map()});return memories.get(id);}
 function draftFor(instance,key,values){
  const drafts=memory(instance.instance_id).drafts;let draft=drafts.get(key);
  if(!draft||!draft.dirty){draft={values:jsonCopy(values),base:jsonCopy(values),revision:instance.revision,dirty:false};drafts.set(key,draft);}return draft;
 }
 function writeButton(label,action){const node=button(label,action);node.dataset.programWrite='';node.disabled=!canWrite||busy||sourceBusy||!!pendingChange;return node;}
 function fields(title,rows){const section=el('section','','program-facts');if(title)section.append(el('h4',title));const values=el('dl');for(const [label,value]of rows){values.append(el('dt',label),el('dd',value||'Not recorded'));}section.append(values);return section;}
 function validateInstance(instance){
  requireValue(object(instance)&&instancePattern.test(instance.instance_id)&&hex(instance.revision)&&list(instance.requirements,256)&&list(instance.bindings,64)&&list(instance.funding,64)&&list(instance.work_links,256),'The saved program is incomplete.');
  requireBinding(instance.owner_subject===binding?.subject,'The saved program belongs to another destination. Refresh Programs.');
  for(const row of instance.requirements)requireValue(object(row)&&bounded(row.requirement_id,240)&&bounded(row.statement,12000)&&bounded(row.state,80)&&bounded(row.owner,256),'A saved requirement is incomplete.');
  for(const row of instance.funding)requireValue(object(row)&&bounded(row.package_id,240)&&bounded(row.funding_program,1000)&&list(row.scope,256)&&list(row.cost_codes,256),'A funding work package is incomplete.');
  for(const row of instance.work_links)savedWorkHref(row);return instance;
 }
 function retainInstance(instance){validateInstance(instance);const at=instances.findIndex(row=>row.instance_id===instance.instance_id);if(at<0)instances.push(instance);else instances[at]=instance;return instance;}
 function planForm(host,instance,{key,title,description,fields:specification,values,change,reviewLabel,saveLabel,success,signal}){
  const draft=draftFor(instance,key,values),section=el('section','','program-plan'),form=el('form'),fieldset=el('fieldset'),state=el('p','','program-draft-state'),reviewHost=el('section','','program-plan-review');
  let revision=0,reviewCleanup=null;fieldset.dataset.programWrite='';fieldset.disabled=!canWrite||busy||!!pendingChange;section.append(el('h4',title),el('p',description));
  const inputs=new Map();
  for(const field of specification){const label=el('label',field.label),input=document.createElement(field.multiline?'textarea':'input');if(!field.multiline)input.type=field.type||'text';else input.rows=field.rows||4;input.value=draft.values[field.key];input.maxLength=field.maxLength||20000;input.name=field.key;input.setAttribute('aria-label',field.label);if(field.help)label.append(el('small',field.help));label.append(input);fieldset.append(label);inputs.set(field.key,input);input.addEventListener('input',()=>{draft.values[field.key]=input.value;draft.dirty=!sameValue(draft.values,values);revision++;reviewCleanup?.();reviewCleanup=null;reviewHost.replaceChildren();renderState();},{signal});}
  const reviewButton=button(reviewLabel,()=>{});reviewButton.type='submit';fieldset.append(reviewButton);form.append(fieldset);section.append(form,state,reviewHost);host.append(section);
  function renderState(){state.textContent=!canWrite?'You have read access to this program.':draft.dirty?(draft.revision===instance.revision?'Unsaved changes. They stay here while you open other work.':'Your draft is kept. Review it against the current saved program.'):'Saved values are shown.';}
  form.addEventListener('submit',async event=>{event.preventDefault();if(!canWrite||busy||pendingChange)return;const ticket=++revision;reviewHost.replaceChildren();reviewCleanup?.();reviewCleanup=null;
   try{const candidate=change(jsonCopy(draft.values)),current=change(jsonCopy(values));const differences=await compareRecords(current,candidate,{signal});if(signal.aborted||ticket!==revision)return;
    if(!differences.length){reviewHost.append(el('p','These values already match the saved program.'));return;}
    reviewHost.append(el('h4','Review this change'));reviewCleanup=renderChanges(reviewHost,differences,{labelBefore:'Saved',labelAfter:'To save'});
    reviewHost.append(writeButton(saveLabel,()=>void saveChange(instance,candidate,{draftKey:key,success})));controls();
   }catch(error){if(!signal.aborted)state.textContent=error.message;}
  },{signal});renderState();return()=>reviewCleanup?.();
 }
 function renderSavedWork(host,instance,targetKind,targetId,{signal}){
  const section=el('section','','program-saved-work');section.append(el('h4','Saved work'));
  const rows=instance.work_links.filter(row=>row.target_kind===targetKind&&row.target_id===targetId),items=el('ul','','program-records');
  const pickerHost=el('div','','program-record-picker');let pickerCleanup=null;
  function openPicker(row){if(row){const draft=draftFor(instance,'record:'+targetKind+':'+targetId,{workspace:'',label:''});draft.values.workspace=row.workspace_binding;draft.recordKey=row.record_key;draft.dirty=true;}pickerCleanup?.();pickerCleanup=recordPicker(pickerHost,instance,targetKind,targetId,{signal});attach.hidden=true;}
  for(const row of rows){const item=el('li'),open=newTab(row.label||row.record_key,savedWorkHref(row)),version=el('small','Saved version '+row.record_revision+' · linked '+new Date(row.linked_at).toLocaleDateString());open.className='program-record-title';item.append(open,version);const actions=el('div','','program-inline-actions');
   const check=button('Check current version',async()=>{if(signal.aborted)return;const ticket=generation,expected=binding;check.disabled=true;version.textContent='Checking the editor’s saved version…';try{const request=(url,options)=>fetch(url,{...options,signal:AbortSignal.any([controller.signal,signal])}),store=openDestinationStore('/apps/'+row.workspace_binding+'/_records',{request});const page=await store.readMetadata(row.record_key);if(signal.aborted||!current(ticket))return;requireBinding(sameActivityBinding(expected,binding));const actual=page.rows.find(record=>record.key===row.record_key);
    if(!actual){version.textContent='This saved record is no longer available. Choose another saved record or detach this reference.';open.removeAttribute('href');open.setAttribute('aria-disabled','true');}
    else if(actual.revision!==row.record_revision){version.textContent='The editor now has version '+actual.revision+'. Review that version to update this attachment.';open.removeAttribute('href');open.setAttribute('aria-disabled','true');}
    else{version.textContent='Saved version '+row.record_revision+' is current.';open.href=savedWorkHref(row);open.removeAttribute('aria-disabled');}
   }catch(error){if(signal.aborted||!current(ticket))return;if(error.status===401||error.status===403){invalidate('Access to this destination changed. Refresh Programs.');return;}version.textContent='The current saved version could not be checked. '+error.message;}finally{if(!signal.aborted&&current(ticket))check.disabled=false;}});
   actions.append(check,writeButton('Update saved version',()=>openPicker(row)),writeButton('Detach from program',()=>{const review=el('div','','program-plan-review');review.append(el('p','Detach “'+(row.label||row.record_key)+'” from this program? Its saved record stays in the editor.'),writeButton('Detach this reference',()=>void saveChange(instance,{kind:'work-unlink',link_id:row.link_id},{success:'Saved-work reference detached.'})),button('Keep attached',()=>review.remove()));item.append(review);controls();}));item.append(actions);items.append(item);}
  section.append(items);if(!rows.length)section.append(el('p','Attach a saved plan, task, board or source from an installed tool. You can reopen its exact saved version here.','program-help'));
  const attach=writeButton('Attach saved work',()=>{if(pickerCleanup)return;openPicker();});section.append(attach,pickerHost);host.append(section);
  return()=>pickerCleanup?.();
 }
 function recordPicker(host,instance,targetKind,targetId,{signal}){
  const ticket=generation,expected=binding,key='record:'+targetKind+':'+targetId,draft=draftFor(instance,key,{workspace:'',label:''});let collection=null,readEpoch=0,readController=null,destroyed=false,pageAt=0,lastPage=null,pageHistory=[];
  const label=el('label','Installed tool'),choice=document.createElement('select');choice.setAttribute('aria-label','Installed tool');const available=tools.filter(row=>row.ready);
  for(const tool of available){const option=el('option',tool.label);option.value=tool.id;choice.append(option);}if(available.some(row=>row.id===draft.values.workspace))choice.value=draft.values.workspace;label.append(choice);
  const hint=el('p','Open the tool to create or edit work. Return here to read and attach its saved version.','program-help'),state=el('p','','program-record-state'),working=el('div'),read=button('Read saved records',()=>void readRecords()),editor=newTab('Open installed tool','#'),actions=el('div','','program-inline-actions');actions.append(read,editor);
  const pager=el('nav','','program-inline-actions'),previous=button('Previous saved records',()=>{if(pageAt>0)void readRecords(pageHistory[pageAt-1]);}),next=button('Next saved records',()=>{if(lastPage?.more)void readRecords({cursor:lastPage.cursor,epoch:lastPage.epoch,index:pageAt+1});});pager.setAttribute('aria-label','Saved record pages');pager.append(previous,next);pager.hidden=true;
  host.replaceChildren(label,actions,hint,state,working,pager);
  if(!available.length||toolsUnavailable){host.replaceChildren(el('p',toolsUnavailable?'Installed tools could not be checked. Refresh Programs to try again.':'Enable a tool for this destination before attaching saved work.'),link('Review available tools','/packs/setup?'+new URLSearchParams({context})));return()=>{};}
  const live=epoch=>!destroyed&&!signal.aborted&&current(ticket)&&sameActivityBinding(expected,binding)&&epoch===readEpoch;
  function showEditor(){const tool=available.find(row=>row.id===choice.value);if(!tool)return;editor.href=tool.href;editor.textContent='Open '+tool.label;editor.setAttribute('aria-label','Open '+tool.label+' — opens in another tab');}showEditor();
  choice.addEventListener('change',()=>{readEpoch++;readController?.abort();collection?.destroy();collection=null;working.replaceChildren();state.textContent='';draft.values.workspace=choice.value;draft.dirty=true;read.disabled=false;pager.hidden=true;pageHistory=[];lastPage=null;showEditor();},{signal});
  async function readRecords({cursor='',epoch:recordEpoch=-1,index=0}={}){const epoch=++readEpoch;readController?.abort();readController=new AbortController();const tool=available.find(row=>row.id===choice.value);if(!tool)return;if(recordEpoch===-1)pageHistory=[];draft.values.workspace=tool.id;draft.dirty=true;read.disabled=choice.disabled=previous.disabled=next.disabled=true;pager.hidden=true;state.textContent='Reading saved records from '+tool.label+'…';collection?.destroy();collection=null;working.replaceChildren();
   try{const request=(url,options)=>fetch(url,{...options,signal:AbortSignal.any([controller.signal,signal,readController.signal])}),store=openDestinationStore('/apps/'+tool.id+'/_records',{request});const [page,views]=await Promise.all([store.readMetadata('',{cursor,epoch:recordEpoch}),recordViews(tool,request)]);if(!live(epoch))return;pageAt=index;lastPage=page;pageHistory[index]={cursor,epoch:page.epoch,index};pager.hidden=!page.more&&index===0;previous.disabled=index===0;next.disabled=!page.more;
    const rows=page.rows.filter(row=>views.some(view=>projectSavedView(view,row))).map(row=>({id:row.key,title:recordTitle(row),subtitle:human(row.value?.kind||row.value?.type||'Saved record'),status:'Version '+row.revision,category:human(row.value?.kind||row.value?.type||'Saved record'),version:String(row.revision),data:row}));
    state.textContent='Page '+(index+1)+' · '+rows.length+' saved '+(rows.length===1?'record':'records')+' with editor views'+(rows.length!==page.rows.length?' · '+(page.rows.length-rows.length)+' without a declared view':'')+'.';
    if(!rows.length){working.append(el('p',page.rows.length||page.more?'This page has no records with a declared editor view.':'No saved records are available in this tool yet.'));return;}
    collection=mountCollection(working,{rows,selection:false,label:'Saved records',renderInspector:(entry,detail,{signal:inspection})=>{const row=entry.data;draft.recordKey=row.key;detail.append(fields('',[['Tool',tool.label],['Saved version',String(row.revision)],['Attached files',String(row.fileCount)]]));
     const title=el('label','Name in this program'),input=document.createElement('input');input.type='text';input.maxLength=200;input.value=recordTitle(row).slice(0,200);input.setAttribute('aria-label','Name in this program');title.append(input);detail.append(title,el('p','This attaches a reference to this saved version. Continue editing in '+tool.label+'.','program-help'));
     detail.append(writeButton('Attach this saved version',()=>{const label=input.value.trim();if(!label){input.focus();input.setCustomValidity('Give this saved work a name.');input.reportValidity();return;}input.setCustomValidity('');void saveChange(instance,{kind:'work-link',target_kind:targetKind,target_id:targetId,workspace_binding:tool.id,record_key:row.key,record_revision:row.revision,label},{draftKey:key,success:'Saved work attached. Reopen its saved version from this program.'});}));controls();
    }});if(draft.recordKey&&rows.some(row=>row.id===draft.recordKey))collection.inspect(draft.recordKey);
   }catch(error){if(!live(epoch))return;if(error.status===401||error.status===403){invalidate('Access to this destination changed. Refresh Programs.');return;}state.textContent=error.message||'Saved records are unavailable.';}
   finally{if(live(epoch))read.disabled=choice.disabled=false;}
  }
  void readRecords();return()=>{destroyed=true;readEpoch++;readController?.abort();collection?.destroy();};
 }
 function showInstance(instance){
  validateInstance(instance);clearDetail();activeInstance=instance;setIndexOpen(false);
  const saved=memory(instance.instance_id),template=templates.find(t=>t.template_id===instance.template_id),open=instance.requirements.filter(row=>!done(row.state));
  const disposers=[],collections=[],header=el('header','','program-work-header'),heading=el('div'),commands=button('Actions',()=>{});let fundingCollection=null;
  heading.append(el('p',destinationLabel+' · '+human(instance.archetype||'Program'),'program-eyebrow'),el('h2',instance.name||template?.name||instance.template_id),el('p',open.length+' requirements still open · '+instance.work_links.length+' saved records','program-summary'));header.append(heading,commands);main.append(header);
  const recovery=el('section','','program-recovery');main.append(recovery);renderRecovery(recovery);
  const tablist=el('div','','program-work-tabs');tablist.setAttribute('aria-label','Program work areas');const panels=new Map(),entries=[];
  for(const [id,label]of [['work','Work'],['sources','Sources'],['funding','Funding'],['overview','Overview']]){const control=button(label,()=>{}),panel=el('section','','program-panel');panel.dataset.programArea=id;tablist.append(control);panels.set(id,panel);entries.push({id,button:control,panel});}
  main.append(tablist,...panels.values());const tabsView=mountTabs(tablist,entries,{selected:saved.tab,onSelect:id=>{saved.tab=id;}});disposers.push(()=>tabsView.destroy());
  const workPanel=panels.get('work'),workTitle=el('div','','program-area-heading');workTitle.append(el('h3','Requirements'),el('p','Choose a requirement, prepare its inputs and reopen the saved work that supports it.'));workPanel.append(workTitle);
  const workHost=el('div');workPanel.append(workHost);
  if(instance.requirements.length){const rows=[...open,...instance.requirements.filter(row=>done(row.state))].map(row=>({id:row.requirement_id,title:requirementTitle(row,instance),subtitle:ownerLabel(row.owner),search:row.statement,status:human(row.state)+(row.deadline?' · '+dateLabel(row.deadline):''),category:done(row.state)?'Resolved':'Open',version:instance.revision,data:row}));
   const collection=mountCollection(workHost,{rows,selection:false,label:'Program requirements',renderInspector:(entry,host,{signal})=>{const row=entry.data;saved.requirement=row.requirement_id;host.append(el('span',human(row.state),'program-state'),el('p',row.statement,'program-requirement-blocker'));
    if(row.owner==='Finance'){const funding=button('Prepare funding scope',()=>{tabsView.select('funding');const target=instance.funding.find(item=>row.requirement_id===item.package_id+'-scope');if(target)fundingCollection?.inspect(target.package_id);});funding.className='program-primary';host.append(funding);}
    else if(row.kind==='capability'||row.statement.toLowerCase().includes('source')){const find=button('Find supporting sources',()=>tabsView.select('sources'));find.className='program-primary';host.append(find);}
    const cleanups=[];cleanups.push(planForm(host,instance,{key:'deadline:'+row.requirement_id,title:'Plan this requirement',description:'Set the working deadline, then attach the saved material this requirement needs.',fields:[{key:'deadline',label:'Working deadline',type:'date'}],values:{deadline:storedDate(row.deadline)},change:values=>({kind:'requirement-plan',requirement_id:row.requirement_id,deadline:values.deadline}),reviewLabel:'Review deadline',saveLabel:'Save reviewed deadline',success:'Working deadline saved.',signal}));
    cleanups.push(renderSavedWork(host,instance,'requirement',row.requirement_id,{signal}));
    const owner=el('details','','program-owner-detail');owner.append(el('summary','Owner review · '+human(row.state)),fields('',[['Owner',ownerLabel(row.owner)],['Accepted evidence',row.evidence_ref||'No accepted evidence recorded'],['Satisfied by',row.satisfied_by||'Awaiting owner review']]));host.append(owner);
    controls();return()=>cleanups.forEach(fn=>fn?.());
   }});collections.push(collection);collection.inspect(rows.some(row=>row.id===saved.requirement)?saved.requirement:rows[0].id);
  }else workHost.append(el('p','This program has no recorded requirements.'));
  const fundingPanel=panels.get('funding');fundingPanel.append(el('h3','Funding work packages'),el('p','Prepare each package’s scope and cost codes, then continue its eligibility and approval review with the finance owner.'));
  if(instance.funding.length){const host=el('div');fundingPanel.append(host);const rows=instance.funding.map(row=>({id:row.package_id,title:row.funding_program,subtitle:row.scope.length+' scope entries · '+row.cost_codes.length+' cost codes',status:human(row.status||'Draft'),version:instance.revision,data:row}));
   const collection=mountCollection(host,{rows,selection:false,label:'Funding packages',renderInspector:(entry,detail,{signal})=>{const row=entry.data;saved.funding=row.package_id;detail.append(el('p',row.distinctness_rule||'Keep each package’s scope and costs distinct.','program-notice'));
    return planForm(detail,instance,{key:'funding:'+row.package_id,title:'Prepare the package',description:'List one scope item or cost code per line. Review the changes before saving.',fields:[{key:'scope',label:'Scope',multiline:true,rows:5,help:'Up to 32 entries, 400 characters each.'},{key:'cost_codes',label:'Cost codes',multiline:true,rows:4,help:'Up to 32 entries, 120 characters each.'}],values:{scope:row.scope.join('\n'),cost_codes:row.cost_codes.join('\n')},change:values=>{const output={kind:'funding-plan',package_id:row.package_id};for(const [field,max]of [['scope',400],['cost_codes',120]]){output[field]=values[field].split(/\r?\n/).map(value=>value.trim()).filter(Boolean);requireValue(output[field].length<=32&&output[field].every(value=>value.length<=max&&!/[\x00-\x1f\x7f]/.test(value)),'Use up to 32 '+human(field)+' entries of at most '+max+' characters each.');}return output;},reviewLabel:'Review funding plan',saveLabel:'Save reviewed funding plan',success:'Funding scope and cost codes saved.',signal});
   }});fundingCollection=collection;collections.push(collection);collection.inspect(rows.some(row=>row.id===saved.funding)?saved.funding:rows[0].id);
  }else fundingPanel.append(el('p','This template has no funding work package.'));
  const sources=panels.get('sources'),sourceExecution=el('div');sourceExecution.dataset.programSourceWork='';sources.append(sourceExecution);const localSources=el('details','','program-local-sources');localSources.append(el('summary','Local source collection'));
  const collection=instance.source_collection;
  if(collection){const selector=collection.source_selector||{},refs=[...(selector.resolved_source_refs||[]),...(selector.resolved_dependency_refs||[])];localSources.append(fields('Current source collection',[['Resolution',human(selector.resolution_state||'Not resolved')],['Source types',(collection.material_kind||[]).map(human).join(', ')],['Refresh policy',human(collection.refresh_policy||'On demand')]]));
   const refreshSources=writeButton('Refresh local source matches',()=>void saveChange(instance,{kind:'sources-refresh'},{success:'Local source matches refreshed.'}));localSources.append(refreshSources,el('p','Checks the retained local source index for this program’s declared needs.','program-help'));
   if(refs.length){const listNode=el('ul','','program-source-refs');for(const ref of refs)listNode.append(el('li',typeof ref==='string'?ref:JSON.stringify(ref)));localSources.append(el('h4','Retained local matches'),listNode);}
   else localSources.append(el('p','No local matches are retained yet.'));
  }else localSources.append(el('p','The prepared source collection is unavailable.'));sources.append(localSources);
  const overview=panels.get('overview');overview.append(el('h3','Program overview'),el('p',template?.description||'Keep this program’s requirements, saved work and source preparation together.','program-description'));
  const overviewSignal=new AbortController();disposers.push(()=>overviewSignal.abort());
  for(const row of instance.bindings){const section=el('section','','program-part');section.append(el('h4',human(row.program_role)),el('p',row.lowered_by?'Connected through '+human(row.lowered_by):'Awaiting its native editor connection.','program-help'));disposers.push(renderSavedWork(section,instance,'binding',row.binding_id,{signal:overviewSignal.signal}));overview.append(section);}
  const programWork=el('section');overview.append(programWork);disposers.push(renderSavedWork(programWork,instance,'program',instance.instance_id,{signal:overviewSignal.signal}));
  const roles=[...new Set([...(template?.semantic_slots||[]),...instance.bindings.filter(row=>bounded(row.program_role,128)).map(row=>row.program_role)])];overview.append(toolSection(roles));
  const pending=instance.bindings.filter(row=>!row.lowered_by);if(pending.length)overview.append(names('Parts awaiting an editor connection',pending.map(row=>row.program_role)));
  const address='/programs?'+new URLSearchParams({context,instance:instance.instance_id}),footer=el('div','','program-actions program-footer');footer.append(link('Link to this program',address),newTab('Open account research','/sources/web?'+new URLSearchParams({context})),newTab('Model Commons','/s/models/all?'+new URLSearchParams({context})));main.append(footer);
  const palette=mountCommandPalette(commands,{scope:main,label:'Program actions',commands:()=>[{id:'work',label:'Open requirements',run:()=>tabsView.select('work')},{id:'sources',label:'Find and review program sources',run:()=>tabsView.select('sources')},{id:'funding',label:'Prepare funding work packages',run:()=>tabsView.select('funding')},{id:'overview',label:'Open saved work and program parts',run:()=>tabsView.select('overview')},{id:'refresh',label:'Read the current saved program',disabled:busy||sourceBusy||!!pendingChange,reason:'Finish the current save or check its receipt first.',run:()=>readInstance(instance.instance_id)}],onError:fail});disposers.push(()=>palette.destroy());
  let sourceView=null;workspaceView={destroy(){sourceView?.destroy();for(const fn of disposers)fn();for(const item of collections)item.destroy();},lock(locked,sourceLocked){for(const item of collections)item.controller.lock(locked);sourceView?.setLocked(sourceLocked);},tabs:tabsView,sourceExecution};
  saved.source??={};sourceView=mountProgramSourceWork(sourceExecution,{context,binding,instance,call,canWrite,signal:controller.signal,memory:saved.source,onBusy(value){sourceBusy=value;controls();if(binding&&!disposed&&!suspended)renderIndex();},onAccessFailure:fail});controls();
  if(globalThis.location?.pathname==='/programs')history.replaceState(null,'',address);
 }
 function renderRecovery(host){
  host.replaceChildren();host.hidden=!pendingChange;if(!pendingChange)return;
  host.append(el('h3','Check the saved outcome'),el('p',pendingChange.phase==='not-found'?'This request has no saved receipt yet. Check again or retry the same change.':'The save has not been confirmed. Check its receipt before making another change.'));
  const check=button('Check saved change',()=>void checkChange());check.dataset.programRecovery='read';host.append(check);
  if(pendingChange.phase==='not-found'){const retry=button('Retry the same save',()=>void sendChange());retry.dataset.programRecovery='write';host.append(retry);}
 }
 function checkedChangeReply(value,pending,{receiptRequired=true}={}){
  requireBinding(sameActivityBinding(pending.request.expectedBinding,value.binding));validateInstance(value.instance);
  requireValue(value.instance.instance_id===pending.request.instance_id,'The saved change returned a different program.');
  if(receiptRequired){const receipt=value.receipt;requireValue(object(receipt)&&receipt.request_id===pending.request.request_id&&receipt.instance_id===pending.request.instance_id&&receipt.kind===pending.request.change.kind&&receipt.before_revision===pending.request.expected_revision&&hex(receipt.after_revision),'The saved change receipt is incomplete.');}
  return value.instance;
 }
 async function saveChange(instance,change,{draftKey='',success='Program change saved.'}={}){
  if(busy||sourceBusy||pendingChange||!canWrite)return;
  requireValue(activeInstance?.instance_id===instance.instance_id&&activeInstance.revision===instance.revision,'Read the current program before reviewing this change.');
  pendingChange={request:{expectedBinding:jsonCopy(binding),instance_id:instance.instance_id,request_id:crypto.randomUUID(),expected_revision:instance.revision,change:jsonCopy(change)},phase:'saving',draftKey,success};
  await sendChange();
 }
 async function sendChange(){
  if(!pendingChange||busy||!binding)return;const pending=pendingChange,ticket=generation;pending.phase='saving';busy=true;controls();renderIndex();status.textContent='Saving the reviewed program change…';
  try{const value=await call(context,'change',{body:pending.request,signal:controller.signal});if(!current(ticket)||pendingChange!==pending)return;
   const currentInstance=checkedChangeReply(value,pending);pendingChange=null;busy=false;if(pending.draftKey)memory(pending.request.instance_id).drafts.delete(pending.draftKey);retainInstance(currentInstance);showInstance(currentInstance);status.textContent=pending.success;delete status.dataset.error;
  }catch(error){if(!current(ticket)||pendingChange!==pending)return;
   busy=false;if(accessFailure(error)){fail(error);return;}
   if(knownChangeRefusal(error)){pendingChange=null;status.textContent=error.message+' Your draft is kept.';status.dataset.error='true';
    const notice=el('section','','program-recovery');notice.append(el('h3','Review the current program'),el('p',error.message),button('Read current program',()=>void readInstance(pending.request.instance_id)));main.querySelector('.program-recovery')?.replaceWith(notice);
   }else{pending.phase='uncertain';fail(error);const host=main.querySelector('.program-recovery');if(host)renderRecovery(host);}
  }finally{if(current(ticket)){busy=false;controls();renderIndex();}}
 }
 async function checkChange(){
  if(!pendingChange||busy||!binding)return;const pending=pendingChange,ticket=generation;busy=true;controls();status.textContent='Checking this change’s saved receipt…';
  try{const value=await call(context,'change-receipt',{body:{expectedBinding:pending.request.expectedBinding,instance_id:pending.request.instance_id,request_id:pending.request.request_id},signal:controller.signal});if(!current(ticket)||pendingChange!==pending)return;
   requireValue(typeof value.found==='boolean','The change receipt lookup is incomplete.');const currentInstance=checkedChangeReply(value,pending,{receiptRequired:value.found});retainInstance(currentInstance);busy=false;
   if(value.found){pendingChange=null;if(pending.draftKey)memory(pending.request.instance_id).drafts.delete(pending.draftKey);showInstance(currentInstance);status.textContent='Save confirmed. The current saved program is open.';delete status.dataset.error;}
   else{requireValue(value.receipt===null,'An unconfirmed change returned an unexpected receipt.');pending.phase='not-found';showInstance(currentInstance);status.textContent='No saved receipt was found yet. Retry keeps this same change identity.';delete status.dataset.error;}
  }catch(error){if(current(ticket)&&pendingChange===pending){busy=false;fail(error);if(pendingChange){pending.phase='uncertain';const host=main.querySelector('.program-recovery');if(host)renderRecovery(host);}}}
  finally{if(current(ticket)){busy=false;controls();renderIndex();}}
 }
 async function readInstance(id){
  if(!binding||busy||sourceBusy||pendingChange)return;const ticket=generation,expected=binding;busy=true;controls();clearDetail();main.append(el('p','Reading the saved program…'));
  try{const value=await call(context,'read',{instance:id,signal:controller.signal});if(!current(ticket))return;requireBinding(sameActivityBinding(expected,value.binding));
   requireValue(list(value.instances,256),'The saved program list is incomplete.');const instance=value.instances.find(row=>row.instance_id===id);requireValue(instance,'This program was not found in the current destination.');
   busy=false;uncertain=false;review=null;retainInstance(instance);showInstance(instance);status.textContent='Saved program opened in '+destinationLabel+'.';delete status.dataset.error;
  }catch(error){if(current(ticket)){clearDetail();if(uncertain&&!accessFailure(error))main.append(el('h2','Check the saved outcome'),el('p','The saved outcome could not be read. You can check the same program again.'),button('Check saved program',()=>void readInstance(id)));fail(error);}}finally{if(current(ticket)){busy=false;controls();renderIndex();}}
 }
 async function reviewProgram(template){
  const ticket=generation,expected=binding;busy=true;controls();main.querySelectorAll('button').forEach(node=>node.disabled=true);status.textContent='Reviewing the current template and destination…';
  try{const value=await call(context,'review',{body:{template_id:template.template_id,expectedBinding:expected},signal:controller.signal});if(!current(ticket))return;requireBinding(sameActivityBinding(expected,value.binding));requireValue(hex(value.review_digest)&&preparedPattern.test(value.instance_id),'The review is incomplete.');
   review=value;busy=false;showTemplate(programTemplate(value.template),{reviewed:true});status.textContent='Review ready. Preparation will use this exact template.';delete status.dataset.error;
  }catch(error){if(current(ticket)){review=null;busy=false;if(!accessFailure(error))showTemplate(template);fail(error);}}finally{if(current(ticket)){busy=false;controls();}}
 }
 async function prepareProgram(template){
  if(!review||busy||uncertain)return;const retained=review,ticket=generation,expected=binding;
  busy=true;controls();main.querySelectorAll('button').forEach(node=>node.disabled=true);status.textContent='Preparing this program…';
  try{const value=await call(context,'prepare',{body:{template_id:template.template_id,review_digest:retained.review_digest,expectedBinding:expected},signal:controller.signal});if(!current(ticket))return;
   requireBinding(sameActivityBinding(expected,value.binding));requireValue(object(value.instance)&&value.instance.instance_id===retained.instance_id,'The preparation acknowledgement is incomplete.');
   busy=false;setMode('saved');selected=value.instance.instance_id;if(!instances.some(r=>r.instance_id===selected))instances.push(value.instance);review=null;
   showInstance(value.instance);renderIndex();status.textContent=value.created?'Program prepared. Its requirements are ready to continue.':'The same prepared program is open.';delete status.dataset.error;
  }catch(error){if(current(ticket)){
   if(accessFailure(error)){fail(error);return;}
   uncertain=true;busy=false;fail(error);main.replaceChildren(el('h2','Check the saved outcome'),el('p','Preparation was not confirmed. Check this same program before preparing it again.'));
   main.append(button('Check saved program',()=>void readInstance(retained.instance_id)));
  }}finally{if(current(ticket)){busy=false;controls();}}
 }
 async function load({preserveDrafts=false}={}){
  if(disposed||suspended||busy||sourceBusy||pendingChange)return;const previous=preserveDrafts?binding:null,previousSelection=selected;invalidate('Reading programs for this destination…',{keepDrafts:!!previous});const ticket=generation;controller=new AbortController();busy=true;controls();delete status.dataset.error;
  try{
   const value=await call(context,'catalog',{signal:controller.signal});if(!current(ticket))return;
   requireValue(list(value.templates,256)&&list(value.instances,256),'The program catalog is incomplete.');
   binding=value.binding;if(previous&&!sameActivityBinding(previous,binding))memories.clear();templates=value.templates.map(programTemplate);instances=value.instances;
   const [saved,installed]=await Promise.allSettled([call(context,'read',{signal:controller.signal}),readPacket('/apps/_bindings?'+new URLSearchParams({context}),{signal:controller.signal})]);if(!current(ticket))return;
   if(saved.status==='rejected')throw saved.reason;
   requireBinding(sameActivityBinding(binding,saved.value.binding));requireValue(list(saved.value.instances,256),'The saved program list is incomplete.');instances=saved.value.instances;
   toolsUnavailable=false;if(installed.status==='fulfilled')try{tools=installedProgramTools(installed.value,binding);}catch(error){if(accessFailure(error))throw error;toolsUnavailable=true;}else {if(accessFailure(installed.reason))throw installed.reason;toolsUnavailable=true;}
   busy=false;setMode(instances.length?'saved':'templates');renderIndex();controls();status.textContent=toolsUnavailable?'Programs loaded. Installed tools could not be checked; reopen Your work to continue.':instances.length+' prepared programs · '+templates.length+' reusable templates';
   const initial=previous&&sameActivityBinding(previous,binding)&&previousSelection?previousSelection:root.dataset.instance,wanted=!initial&&templateId(root.dataset.template)?root.dataset.template:'',kept=wanted?instances.find(r=>r.template_id===wanted)?.instance_id:'',offered=wanted&&!kept?templates.find(t=>t.template_id===wanted):null,open=kept||initial;if(open&&instances.some(r=>r.instance_id===open)){selected=open;setMode('saved');showInstance(instances.find(r=>r.instance_id===open));renderIndex();}
   else if(offered){selected=wanted;setMode('templates');renderIndex();showTemplate(offered);status.textContent='Review this template before preparing a program for '+destinationLabel+'.';}else if(initial||root.dataset.template){main.replaceChildren(el('p',initial?'That program is not available in this destination.':'That template is not offered in this destination.'));}
   else {setIndexOpen(true);main.replaceChildren(el('h2',instances.length?'Continue where you left off':'Choose a starting point'),el('p',instances.length?'Open a program to review its requirements and continue in your tools.':'Browse an institutional or recurring-work template. Review its setup, then prepare a private program for this destination.'));}
  }catch(error){if(current(ticket)){templates=[];instances=[];tools=[];binding=null;memories.clear();clearDetail();index.replaceChildren(el('li','Programs could not be read. Refresh after access is available.','program-empty'));fail(error);}}finally{if(current(ticket)){busy=false;controls();}}
 }
 const accountChanged=()=>{invalidate('Your account changed. Refresh to read its programs.');};
 const activityInvalidated=()=>invalidate('Checking the current account destination…');
 const destinationChanged=event=>{const next=event.detail?.context;if(!contextPattern.test(next)||suspended)return;
  if(next!==context){invalidate('Opening the selected destination…');location.assign('/programs?'+new URLSearchParams({context:next,...(templateId(root.dataset.template)?{template:root.dataset.template}:{})}));}
  else {if(binding&&event.detail?.binding&&!sameActivityBinding(binding,event.detail.binding))invalidate('Checking the current account…');if(!binding&&!busy)void load();}
 };
 const hide=()=>{suspended=true;invalidate('Reopen Programs to check the current account.');},show=()=>{if(suspended){suspended=false;void load();}};
 window.addEventListener('bonfyre:account-changed',accountChanged);window.addEventListener('bonfyre:activity-context-invalidated',activityInvalidated);window.addEventListener('bonfyre:activity-context-changed',destinationChanged);window.addEventListener('pagehide',hide);window.addEventListener('pageshow',show);
 void load();
 return {reload:load,destroy(){invalidate('');disposed=true;window.removeEventListener('bonfyre:account-changed',accountChanged);window.removeEventListener('bonfyre:activity-context-invalidated',activityInvalidated);window.removeEventListener('bonfyre:activity-context-changed',destinationChanged);window.removeEventListener('pagehide',hide);window.removeEventListener('pageshow',show);}};
}
if(typeof document!=='undefined'){const root=document.querySelector('[data-program-workspace]');if(root)mountPrograms(root);}
