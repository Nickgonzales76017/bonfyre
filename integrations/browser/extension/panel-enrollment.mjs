// Projection of native enrollment work in the existing side panel.
// Browser context is presentation, not evidence of the provider's authenticated identity.
import {HUMAN_STAGES,REQUIRED_STAGES,completionBlockers,requireBrowserContext,assertReviewedItem} from './enrollment-integrity.mjs';
import {mountForeignSignIn} from './foreign-sign-in.mjs';
const box=document.getElementById('enroll'),say=document.getElementById('enroll-said');
const privateContext=chrome.extension.inIncognitoContext===true,identity=privateContext?'proton':'gmail';
const labels={existing_access_checked:'Checked for existing access',account_bound:'Confirmed the intended account',credential_sealed:'Credential sealed by native owner',credential_verified:'Provider verification',scope_confirmed:'Confirmed project / zone / scope',consumer_bound:'Native consumer binding'};
const filterLabels={all:'All setups',ready:'Ready to continue',open:'Open work',waiting:'Waiting',decisions:'Decisions',finished:'Finished'};
let current=null,busy=false,needsReload=false,connectionCard=null,credentialField='',disposed=false,expandedWork='';
let catalog=null,catalogOpen=false,filter='all',search='',catalogCursor='',catalogTrail=[];
const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
const said=(text,bad=false)=>{if(disposed)return;say.textContent=text;say.dataset.tone=bad?'bad':'';};
const ask=(operation,more={})=>{
  if(['enroll_list','enroll_inspect','enroll_status'].includes(operation))return window.aurekaiAsk(operation,more);
  if(!navigator.locks)throw Error('Browser work locking is unavailable; no setup was changed.');
  return navigator.locks.request('aurekai-enrollment-work-'+identity,()=>window.aurekaiAsk(operation,more));
};
const expected=()=>{if(!current)throw Error('Open the native item first.');return{work_id:current.item.work_id,revision:current.revision};};
async function contextWindow(){const w=await chrome.windows.getCurrent();requireBrowserContext(identity,privateContext,w);return w;}
async function exclusive(work){if(!navigator.locks)throw Error('Browser tab locking is unavailable; no provider tab was changed.');return navigator.locks.request('aurekai-enrollment-tabs-'+identity,work);}
function button(text,work,disabled=false){const b=el('button',text);b.type='button';b.disabled=busy||disabled;b.onclick=()=>perform(work);return b;}
async function perform(work){if(busy||disposed)return;busy=true;box.inert=true;connectionCard?.clear();connectionCard?.setDisabled(true);try{await work();}catch(error){said(error.message,true);}finally{busy=false;if(!disposed){box.inert=false;render();}}}
async function select(result){
  if(disposed)return;
  if(result?.item && (result.item.identity!==identity||!/^\d{3}-(?:gmail|proton)$/.test(result.item.work_id)||!result.item.work_id.endsWith('-'+identity)))throw Error('This setup belongs to another enrollment identity.');
  current=result?.item?result:null;needsReload=false;
  if(current)await chrome.storage.session.set({['enroll-view-'+identity]:{work_id:current.item.work_id}}); // Presentation only; reopening reads the native checkpoint.
}
async function readSetup(workId){
  await contextWindow();if(disposed)return;
  const a=await ask('enroll_inspect',{identity,work_id:workId});if(disposed)return;
  if(!a.ok||a.result?.item?.work_id!==workId){needsReload=true;throw Error(a.reason||a.refused||'The selected native setup is unavailable.');}
  await select(a.result);
  catalogOpen=false;
}
async function load(){
  await contextWindow();if(disposed)return;
  const key='enroll-view-'+identity,{[key]:saved}=await chrome.storage.session.get(key);
  if(disposed)return;
  const workId=current?.item.work_id||saved?.work_id;
  if(workId){await readSetup(workId);said('Setup restored for review. Continue this setup when its native queue is ready.');return;}
  await resumeNext();
}
async function resumeNext(){
  await contextWindow();if(disposed)return;
  const a=await ask('enroll_resume',{identity});if(disposed)return;
  if(!a.ok){needsReload=true;throw Error(a.reason||a.refused);}
  await select(a.result);
  said(current?(current.read_only?'The previous hold lapsed. This same setup is open for review.':'Native item ready. Its provider page can be reopened below.'):'No item is ready. Waiting work and decisions remain in the native queue.');
}
async function browseAt(cursor='',trail=[]){
  await contextWindow();if(disposed)return;
  const a=await ask('enroll_list',{identity,filter,search,cursor});if(disposed)return;
  if(!a.ok)throw Error(a.reason||a.refused||'The native setup list is unavailable.');
  catalog=a.result;catalogOpen=true;catalogCursor=cursor;catalogTrail=trail;
}
async function change(operation,more={},signal){
  if(needsReload)throw Error('Reload the native item before another change.');
  if(current?.read_only&&operation!=='enroll_continue')throw Error('Continue this setup before changing its progress.');
  await contextWindow();
  if(disposed||signal?.aborted)throw Error('This provider setup is closed. Reload the native item before continuing.');
  const review=expected();let a;
  try{a=await ask(operation,{...more,identity,expected:review,confirmed:true});}catch{needsReload=true;throw Error('The native result was not confirmed. Reload the native item before another change.');}
  if(disposed||signal?.aborted){needsReload=true;throw Error('Read the native setup to confirm the result.');}
  if(!a.ok){needsReload=true;throw Error((operation==='enroll_seal'?'The native credential owner did not confirm the seal.':a.reason||a.refused)+' Reload the native item before retrying.');}
  if(a.result?.state)current={...current,revision:a.result.revision,stages:a.result.state.stages};
  return {result:a.result,review};
}
async function continueSelected(){const {result}=await change('enroll_continue');await select(result);said(result?.review_required?'Saved progress changed during continuation. Review the updated setup, then choose Continue this setup.':'This reviewed setup is held. Continue its existing provider page or record progress below.');}
async function tabFor(record){const id=typeof record==='number'?record:record?.id;return Number.isInteger(id)?chrome.tabs.get(id).catch(()=>null):null;}
async function openPage(signal){
  const review=expected(),item=current.item;
  const live=()=>{if(disposed||signal?.aborted)throw Error('This provider setup is closed. Reload the native item before continuing.');};live();
  const address=new URL(item.signup_url);
  if(address.protocol!=='https:'||address.username||address.password)throw Error('Only the native item’s HTTPS provider address can be opened.');
  // Inspection never claims work or resumes a different item while reopening a provider page.
  const a=await ask('enroll_inspect',{identity,work_id:review.work_id});live();
  if(!a.ok)throw Error(a.reason||a.refused);
  assertReviewedItem(identity,'enroll-'+a.result.item?.work_id,a.result.revision,review);
  if(['identity','account','signup_url','credential_provider'].some(key=>a.result.item[key]!==item[key])){needsReload=true;throw Error('The provider or account changed. Reload the native setup before opening its page.');}
  return exclusive(async()=>{
    live();const w=await contextWindow(),key='enroll-tab-'+identity;
    const {[key]:record}=await chrome.storage.session.get(key),tab=await tabFor(record);live();
    if(tab){
      if(record?.work_id!==item.work_id||tab.incognito!==privateContext||tab.windowId!==w.id)
        throw Error('The retained tab belongs to another item or window. It was left untouched. Return to that setup or window before reopening.');
      await chrome.tabs.update(tab.id,{active:true}); // Preserve OAuth redirects and unsaved forms.
      said('Returned to the existing provider page.');return;
    }
    const made=await chrome.tabs.create({windowId:w.id,url:item.signup_url,active:true});
    if(made.incognito!==privateContext||made.windowId!==w.id||disposed||signal?.aborted){
      await chrome.tabs.remove(made.id).catch(()=>{});throw Error('This setup closed or the browser context changed; the new tab was closed.');
    }
    await chrome.storage.session.set({[key]:{id:made.id,work_id:item.work_id,window_id:w.id}});
    said('Provider page opened. Sign-in and consent stay on that site.');
  });
}
async function closePage(workId){
  return exclusive(async()=>{
    const w=await contextWindow(),key='enroll-tab-'+identity,{[key]:record}=await chrome.storage.session.get(key);
    if(!record)return;
    const tab=await tabFor(record);
    if(tab&&(record.work_id!==workId||tab.incognito!==privateContext||tab.windowId!==w.id))throw Error('Another item or window owns that tab; it was not closed.');
    if(tab)await chrome.tabs.remove(tab.id);
    await chrome.storage.session.remove(key);
  });
}
async function setAside(operation,why){
  const {review}=await change(operation,{why});
  // The selected work and its provider tab survive a wait. No other item is claimed.
  await readSetup(review.work_id);
  said(operation==='enroll_wait'?'Waiting saved. The provider page and this setup can be reopened when you return.':'Decision saved. This setup remains available for review; native reconciliation is required to continue it.');
}
function statusText(view){
  if(view.needs_decision)return 'Needs a decision';
  if(view.status==='retry_wait')return view.resumable?'Ready to continue':view.next_attempt_at_ms?'Waiting until '+new Date(view.next_attempt_at_ms).toLocaleString():'Waiting';
  return {ready:'Ready',running:view.read_only?'Held work':'Held for this setup',blocked:'Waiting on a dependency',complete:'Complete',cancelled:'Cancelled',cancel_requested:'Cancellation requested',dead_letter:'Needs native review'}[view.status]||'Native status unavailable';
}
function renderCatalog(){
  if(!catalog||!catalogOpen)return;
  const section=el('section');section.className='enroll-catalog';section.setAttribute('aria-label','Saved provider setups');
  const form=el('form'),searchLabel=el('label','Find a provider'),query=el('input'),filterLabel=el('label','Show'),choice=el('select');
  query.type='search';query.maxLength=100;query.value=search;query.setAttribute('aria-label','Find a provider');query.disabled=busy;
  for(const [value,label]of Object.entries(filterLabels)){const option=el('option',label);option.value=value;choice.append(option);}choice.value=filter;choice.setAttribute('aria-label','Setup status');choice.disabled=busy;
  searchLabel.append(query);filterLabel.append(choice);form.append(searchLabel,filterLabel,button('Find setups',async()=>{search=query.value.trim();filter=choice.value;await browseAt();}));
  form.onsubmit=event=>{event.preventDefault();return perform(async()=>{search=query.value.trim();filter=choice.value;await browseAt();});};section.append(form);
  if(!catalog.items.length)section.append(el('p',catalog.seeded?'No setups match this view.':'The native enrollment queue has not been seeded.'));
  const list=el('ul');
  for(const row of catalog.items){const li=el('li'),open=button('Review '+row.item.platform+' · '+row.item.work_id,async()=>{await readSetup(row.item.work_id);said('Saved setup opened for review.');});if(current?.item.work_id===row.item.work_id)open.setAttribute('aria-current','true');li.append(open,el('small',statusText(row)));list.append(li);}section.append(list);
  const pages=el('div');pages.className='enroll-actions';
  pages.append(button('Previous setups',()=>browseAt(catalogTrail.at(-1)||'',catalogTrail.slice(0,-1)),!catalogTrail.length),el('span','Page '+(catalogTrail.length+1)),button('More setups',()=>browseAt(catalog.cursor,[...catalogTrail,catalogCursor]),!catalog.more));section.append(pages);box.append(section);
}
function render(){
  if(disposed)return;
  connectionCard?.destroy();connectionCard=null;box.replaceChildren();
  box.append(el('p',identity==='gmail'?'Gmail · regular browser context':'Proton · incognito browser context'));
  const actions=el('div');actions.className='enroll-actions';actions.append(button(current?'Reload native item':'Continue native enrollment',load),button(catalogOpen?'Close setup list':current?'Find another setup':'Find a setup',()=>catalogOpen?catalogOpen=false:browseAt()));box.append(actions);renderCatalog();
  if(!current){box.append(el('p','Opening the panel does not claim work or open provider pages. Find a setup to inspect saved progress.'));return;}
  const {item,stages}=current,readOnly=current.read_only===true,details=el('div'),cardHost=el('div'),review=expected(),missing=completionBlockers(stages);
  const state=el('p',statusText(current));state.className='enroll-state';box.append(state);
  if(readOnly&&current.needs_decision)box.append(el('p','This setup is waiting for a decision. Its saved progress remains available.'));
  details.append(el('p',`${item.work_id} · revision ${current.revision}`));
  const where=el('p',item.signup_url);where.className='where';details.append(where,button('Unload provider page',()=>closePage(item.work_id)));
  if(current.reason||current.interruption)details.append(el('p',current.reason||current.interruption));
  const list=el('ol');
  for(const stage of REQUIRED_STAGES){
    const li=el('li'),present=stages[stage]&&!missing.includes(stage);
    if(HUMAN_STAGES.includes(stage)&&!readOnly)li.append(button((present?'✓ ':'')+labels[stage],async()=>{await change('enroll_record',{stage});said('Recorded as your observation, not provider verification.');},present||needsReload));
    else li.append(el('span',(present?'✓ ':'Pending: ')+labels[stage]));
    list.append(li);
  }
  details.append(list);
  if(current.notes?.length){details.append(el('h4','Recent setup notes'));const notes=el('ul');for(const note of current.notes)notes.append(el('li',[note.at,note.why,note.note].filter(Boolean).join(' · ')));details.append(notes);}
  const provider=item.credential_provider;
  const fields=!readOnly&&provider&&current.credential_fields_available===true&&Array.isArray(current.credential_fields)?current.credential_fields:[];
  if(!fields.some(field=>field.field===credentialField))credentialField=fields[0]?.field||'';
  const declared=fields.find(field=>field.field===credentialField);
  if(fields.length>1){const label=el('label','Credential to store'),choice=el('select');choice.setAttribute('aria-label','Credential to store');for(const field of fields){const option=el('option',field.label);option.value=field.field;choice.append(option);}choice.value=credentialField;choice.disabled=busy||needsReload;choice.onchange=()=>{if(busy)return;credentialField=choice.value;render();};label.append(choice);details.append(label);}
  if(provider)details.append(el('p','Native provider binding: '+provider));
  if(!readOnly&&!declared)details.append(el('p','Credential entry is unavailable until this item has a current provider field declaration.'));
  if(!readOnly){
    const waits=el('p');for(const [why,label]of [['email','Waiting for email'],['mfa','Waiting for a code'],['review','Waiting for review']])waits.append(button(label,()=>setAside('enroll_wait',why),needsReload),' ');details.append(waits);
    const holds=el('p');for(const [why,label]of [['captcha','Human check'],['paid','Plan decision'],['terms','Terms review'],['unavailable','No developer access']])holds.append(button(label,()=>setAside('enroll_decide',why),needsReload),' ');details.append(holds);
    details.append(el('p','Decision holds need native operator reconciliation; there is no automatic approval or signup retry.'));
    details.append(button('Complete verified capability',async()=>{const {review}=await change('enroll_done');await readSetup(review.work_id);said('Native completion recorded. This setup remains available for review.');},needsReload||missing.length>0));
  }
  if(missing.length)details.append(el('p','Still required: '+missing.map(s=>labels[s]).join(', ')+'.'));
  box.append(cardHost);
  try{connectionCard=mountForeignSignIn(cardHost,{
    origin:new URL(item.signup_url).origin,destination:item.platform+' · '+(privateContext?'Private window':'Regular window'),title:'Use this account?',
    account:{label:'Account for this setup',identifier:item.account,note:'Assigned identity. Your browser manages saved sign-ins.'},
    description:readOnly?'Your saved setup is ready to review. Continue it with this account when its current status permits.':'Sign-in and consent happen on the provider page. Aurekai keeps this setup beside it.',reviewKey:review.work_id+':'+review.revision,details,fieldsCollapsed:true,expanded:expandedWork===item.work_id,onExpandedChange:expanded=>{expandedWork=expanded?item.work_id:'';},disabled:busy||needsReload,
    fields:declared?[{name:'value',label:declared.label,kind:declared.sensitive?'secret':'text',required:true,maxLength:4094,help:declared.help}]:[],submitLabel:'Seal this credential',
    status:!missing.includes('credential_sealed')?'A credential was sealed by the native owner. Provider verification remains a separate step.':'',
    primaryLabel:'Use this account',primaryDisabled:needsReload||!current.resumable,
    onPrimary:readOnly?async(_values,{signal})=>{if(signal.aborted)return;assertReviewedItem(identity,'enroll-'+current?.item?.work_id,current?.revision,review);if(!current.resumable)throw Error('Read this setup’s current status before continuing.');await perform(continueSelected);}:undefined,
    openLabel:'Take over to sign in',onOpen:async(_values,{signal})=>{if(signal.aborted)return;assertReviewedItem(identity,'enroll-'+current?.item?.work_id,current?.revision,review);await perform(()=>openPage(signal));},
    onSubmit:declared?async(values,{signal})=>{if(signal.aborted)return;assertReviewedItem(identity,'enroll-'+current?.item?.work_id,current?.revision,review);const value=values.value;values.value='';await perform(async()=>{await change('enroll_seal',{provider,field:declared.field,value},signal);said('Sealed by BonfyreCredentials. Verification is a separate step.');});}:undefined,
    onDismiss:({pending})=>{if(pending)needsReload=true;expandedWork='';said(pending?'Read the native item before another change.':'Setup closed. Reload the native item to reopen its saved progress.');},
  });}catch{cardHost.append(el('p','This item’s provider address or credential declaration is unavailable. Reload its native setup.'));}
  if(needsReload)box.append(el('p','The last result is stale or uncertain. Reload reads this exact setup; it does not replay the change.'));
}
chrome.tabs.onReplaced.addListener((added,removed)=>{
  exclusive(async()=>{const key='enroll-tab-'+identity,{[key]:record}=await chrome.storage.session.get(key);if(record?.id!==removed)return;const tab=await chrome.tabs.get(added).catch(()=>null);if(tab?.incognito===privateContext)await chrome.storage.session.set({[key]:{...record,id:added,window_id:tab.windowId}});}).catch(error=>said(error.message,true));
});
window.addEventListener?.('pagehide',()=>{disposed=true;connectionCard?.destroy();connectionCard=null;});
render();
