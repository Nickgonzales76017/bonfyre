// Shared Aurekai card. Account scope and provider admission stay in WebWork;
// the installed extension alone controls the real provider tab and companion.
import {encode,decode} from '../workspace-msgpack.mjs';
import {providerReview,sameProviderBinding,providerEntries} from '../provider-contract.mjs';
const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);return n;};
const check=(value,message)=>{if(!value)throw Error(message);};
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const OWNER_RESPONSE_BYTES=512*1024;
const accessRefused=error=>error?.status===401||error?.status===403||error?.code==='binding_changed';
const nativeError=(message,status,code)=>Object.assign(Error(message),Number.isInteger(status)?{status,code}:code?{code}:{});
const requestId=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),n=>n.toString(16).padStart(2,'0')).join('');

async function nativePacket(operation,review,{request=fetch,signal,payload}={}){
 check(providerReview(review),'Choose an exact current native work card.');
 const query=new URLSearchParams({context:review.context,operation});if(operation==='get')query.set('job',review.jobId);
 const controller=new AbortController(),abort=()=>controller.abort(signal.reason),timer=setTimeout(()=>controller.abort(),15000);let bytes,reader;
 if(signal?.aborted){clearTimeout(timer);throw signal.reason||Error('The work card was closed.');}signal?.addEventListener('abort',abort,{once:true});
 try{
  if(payload)bytes=encode([1,payload]);
  const response=await request('/sources/_web?'+query,{method:payload?'POST':'GET',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack',...(payload?{'Content-Type':'application/msgpack'}:{})},body:bytes,signal:controller.signal});
  if(!response.ok){await response.body?.cancel();throw nativeError([401,403].includes(response.status)?'Current access to this destination was refused.':'The native provider owner is unavailable.',response.status);}
  const type=response.headers.get('content-type')?.split(';')[0].trim().toLowerCase(),length=response.headers.get('content-length');
  if(type!=='application/msgpack'||length!==null&&(!/^\d+$/.test(length)||Number(length)>OWNER_RESPONSE_BYTES)){await response.body?.cancel();throw Error('The native provider response is not a bounded MessagePack packet.');}
  reader=response.body?.getReader();check(reader,'The native provider response is empty.');const chunks=[];let size=0;
  for(;;){const part=await reader.read();if(part.done)break;size+=part.value.length;check(size<=OWNER_RESPONSE_BYTES,'The native provider response exceeds its limit.');chunks.push(part.value);}
  check(!controller.signal.aborted&&size>0,'The native provider read was interrupted.');const body=new Uint8Array(size);let at=0;for(const chunk of chunks){body.set(chunk,at);at+=chunk.length;}
  const packet=decode(body);check(Array.isArray(packet)&&packet.length===3&&packet[0]===1&&typeof packet[1]==='boolean'&&object(packet[2]),'The native provider response is incomplete.');
  if(!packet[1])throw nativeError(typeof packet[2].message==='string'?packet[2].message:'The native owner refused this operation.',packet[2].code==='forbidden'?403:undefined,packet[2].code);
  if(!sameProviderBinding(packet[2].binding,review.binding))throw nativeError('Your account or destination changed. Reopen the work card.',409,'binding_changed');return packet[2];
 }finally{bytes?.fill(0);if(payload&&Object.hasOwn(payload,'secret'))payload.secret='';clearTimeout(timer);signal?.removeEventListener('abort',abort);try{await reader?.cancel();}catch{}try{reader?.releaseLock();}catch{}}
}

export function createProviderContinuation({request=fetch,target=globalThis.window,timeoutMs=30000}={}){
 let disposed=false;const pending=new Map();
 function bridge(operation,review,{signal}={}){
  if(disposed)return Promise.reject(Error('The work card is closed.'));
  if(operation==='continue')check(providerReview(review),'Choose an exact native work card.');
  return new Promise((resolve,reject)=>{
   const id=requestId(),finish=(error,value)=>{const job=pending.get(id);if(!job)return;pending.delete(id);clearTimeout(job.timer);target.removeEventListener('aurekai:provider:reply',listener);signal?.removeEventListener('abort',abort);error?reject(error):resolve(value);};
   const abort=()=>finish(signal.reason||Error('This work card was closed.'));
   const listener=event=>{const value=event.detail;if(value?.requestId!==id)return;if(value.ok!==true){finish(nativeError(value.reason||'The native browser companion is unavailable.',value.status,value.code));return;}finish(null,value.result);};
   const timer=setTimeout(()=>finish(nativeError(operation==='continue'?'No browser acknowledgment arrived. Check this connection before trying again.':'Install or reload the Aurekai browser companion to continue alongside the provider.',undefined,operation==='continue'?'provider_outcome_unconfirmed':'companion_unavailable')),operation==='capabilities'?1500:timeoutMs);
   pending.set(id,{timer,finish});target.addEventListener('aurekai:provider:reply',listener);signal?.addEventListener('abort',abort,{once:true});
   if(signal?.aborted){abort();return;}
   // Synchronous dispatch preserves the user's browser activation for the
   // isolated content script. There is no web-accessible runtime API.
   target.dispatchEvent(new CustomEvent('aurekai:provider:request',{detail:{requestId:id,operation,...review?{review}:{}}}));
  });
 }
 return {
  capabilities(options={}){return bridge('capabilities',undefined,options);},
  async status(review,{signal}={}){
   check(!disposed&&providerReview(review),'Choose an exact native work card.');
   const [job,status]=await Promise.all([nativePacket('get',review,{request,signal}),nativePacket('status',review,{request,signal})]);
   check(job.id===review.jobId&&job.revision===review.revision,'This run changed after review. Reopen its details.');
   const capability=job.providerContinuation;check(object(capability)&&typeof capability.canContinue==='boolean'&&typeof capability.canAdminister==='boolean'&&Array.isArray(capability.modes),'The native provider capability is incomplete.');
   const provider=status.providers?.find(p=>p.id===capability.provider?.id),canAdminister=capability.canAdminister&&status.canAdminister===true;
   let companion;try{companion=await bridge('capabilities',undefined,{signal});}catch(error){companion={available:false,reason:error.message};}
   return {binding:job.binding,capability,provider,configured:status.configured===true,canAdminister,companion};
  },
  async continue(review,{signal}={}){
   const result=await bridge('continue',review,{signal});
   check(result&&['provider_open','provider_focused'].includes(result.state)&&result.work?.id===review.jobId&&result.work?.revision===review.revision&&sameProviderBinding(result.binding,review.binding),'The browser acknowledgment belongs to another work review.');return result;
  },
  connect(review,provider,secret,{signal}={}){return nativePacket('connect',review,{request,signal,payload:{id:review.jobId,revision:review.revision,binding:{...review.binding},provider,secret}});},
  disconnect(review,provider,{signal}={}){return nativePacket('disconnect',review,{request,signal,payload:{id:review.jobId,revision:review.revision,binding:{...review.binding},provider}});},
  dispose(){if(disposed)return;for(const job of [...pending.values()])job.finish(Error('The work card was closed. No native work was canceled.'));target.dispatchEvent(new CustomEvent('aurekai:provider:request',{detail:{requestId:requestId(),operation:'clear'}}));disposed=true;}
 };
}

// Presentation for an existing credential or provider-page adapter.
// This module owns no connection, browser tab, account or secret store.
let sequence=0;
const bounded=(value,maximum,name)=>{if(typeof value!=='string'||!value.trim()||value.length>maximum||value.includes('\0'))throw Error('Provide a bounded '+name+'.');return value;};
const node=(tag,text)=>{const element=document.createElement(tag);if(text!==undefined)element.textContent=text;return element;};
function providerButton(button,review){
 if(!providerReview(review))throw Error('Use a current provider review.');
 button.dataset.nativeProviderContinue='';button.dataset.nativeProviderContext=review.context;
 button.dataset.nativeProviderJob=review.jobId;button.dataset.nativeProviderRevision=String(review.revision);
}
function cardShell({origin,title,destination,account,description,workTitle}){
 const root=node('section');root.className='fsi-card';root.dataset.state='ready';root.dataset.expanded='false';
 const id='foreign-sign-in-'+(++sequence),head=node('header'),provider=node('div'),mark=node('span',new URL(origin).hostname.replace(/^www\./,'').slice(0,1).toUpperCase()),address=node('p',origin),brand=node('span','Aurekai');
 head.className='fsi-heading';provider.className='fsi-provider';mark.className='fsi-provider-mark';mark.setAttribute('aria-hidden','true');address.className='fsi-origin';brand.className='fsi-brand';provider.append(mark,address,brand);
 const heading=node('h3',title);heading.id=id;root.setAttribute('aria-labelledby',id);head.append(provider,heading);root.append(head);
 const descriptionNode=node('p',description||'');descriptionNode.className='fsi-description';descriptionNode.hidden=!description;root.append(descriptionNode);
 if(workTitle){const work=node('p',bounded(workTitle,400,'work title'));work.className='fsi-work-reference';work.append(document.createTextNode(' · Pending work'));root.append(work);}
 const preview=node('div'),identity=node('div'),avatar=node('span'),copy=node('div'),label=node('strong'),where=node('p',destination),note=node('p'),access=node('p');
 preview.className='fsi-account';identity.className='fsi-account-identity';avatar.className='fsi-account-avatar';avatar.setAttribute('aria-hidden','true');copy.className='fsi-account-copy';label.className='fsi-account-label';where.className='fsi-destination';note.className='fsi-account-note';access.className='fsi-access';
 const identifier=account?bounded(account.identifier,320,'account identity'):destination;
 label.textContent=identifier;avatar.textContent=identifier.slice(0,1).toUpperCase();
 where.textContent=account?.label?bounded(account.label,160,'account label'):destination;
 if(!account)where.hidden=true;
 note.textContent=account?.note?bounded(account.note,400,'account note'):'';note.hidden=!note.textContent;
 // This preview contains owner-supplied labels only, never saved passwords.
 copy.append(label,where,note);identity.append(avatar,copy);preview.append(identity,access);access.hidden=true;root.append(preview);
 const status=node('p');status.className='fsi-status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');root.append(status);
 const actions=node('footer');actions.className='fsi-actions';const leading=node('div'),trailing=node('div'),primaryGroup=node('div');leading.className='fsi-actions-leading';trailing.className='fsi-actions-trailing';primaryGroup.className='fsi-primary-group';trailing.append(primaryGroup);actions.append(leading,trailing);root.append(actions);
 return {root,id,head,heading,description:descriptionNode,preview,access,status,actions,leading,trailing,primaryGroup};
}
export function signInAccountChoices(values,selected){
 check(Array.isArray(values)&&values.length>0&&values.length<=8,'Use the current assigned account choices.');const ids=new Set();
 const rows=values.map(value=>{check(object(value)&&Object.keys(value).every(k=>['id','identifier','label','note'].includes(k))&&/^[a-z0-9_-]{1,64}$/.test(value.id)&&!ids.has(value.id),'Use unique account identities, without credential values.');ids.add(value.id);return Object.freeze({id:value.id,identifier:bounded(value.identifier,320,'account identity'),label:bounded(value.label,160,'account label'),note:value.note===undefined?'':bounded(value.note,400,'account context')});});
 check(ids.has(selected),'The selected account is no longer assigned.');return Object.freeze(rows);
}
function mountAccountChoices(group,{id,rows,selected,onChoose,onDetails}){
 const button=node('button');button.type='button';button.className='fsi-options fsi-account-options';button.setAttribute('aria-label','Choose account or sign-in option');button.setAttribute('aria-haspopup','menu');button.setAttribute('aria-expanded','false');button.setAttribute('aria-controls',id+'-accounts');group.append(button);
 const menu=node('div');menu.id=id+'-accounts';menu.className='fsi-account-menu';menu.setAttribute('role','menu');menu.setAttribute('aria-label','Saved setup accounts');menu.setAttribute('popover','manual');menu.hidden=true;document.body.append(menu);
 const heading=node('p','Accounts assigned to this setup');heading.className='fsi-menu-heading';heading.setAttribute('role','presentation');menu.append(heading);
 const items=[],events=new AbortController();let disabled=false,disposed=false;
 function close(focus=false){if(disposed)return;if(menu.matches(':popover-open'))menu.hidePopover();menu.hidden=true;button.setAttribute('aria-expanded','false');if(focus)button.focus();}
 function position(){const rect=button.getBoundingClientRect(),margin=12,width=Math.min(360,innerWidth-margin*2);menu.style.width=width+'px';menu.style.left=Math.max(margin,Math.min(rect.right-width,innerWidth-width-margin))+'px';menu.style.top='0px';const height=menu.getBoundingClientRect().height;menu.style.top=Math.max(margin,rect.top-height-8)+'px';}
 function open(){if(disabled||disposed)return;const layer=button.closest('dialog[open]')||document.body;if(menu.parentNode!==layer)layer.append(menu);menu.hidden=false;if(typeof menu.showPopover==='function')menu.showPopover();position();button.setAttribute('aria-expanded','true');items.find(n=>n.getAttribute('aria-checked')==='true')?.focus();}
 for(const row of rows){const item=node('button');item.type='button';item.className='fsi-account-choice';item.setAttribute('role','menuitemradio');item.setAttribute('aria-checked',String(row.id===selected));item.dataset.accountChoice=row.id;const name=node('strong',row.identifier),label=node('span',row.label),note=node('small',row.note);note.hidden=!row.note;item.append(name,label,note);item.onclick=()=>{if(disabled||disposed)return;close(true);if(row.id!==selected)onChoose(row.id);};menu.append(item);items.push(item);}
 if(onDetails){const details=node('button','Setup details');details.type='button';details.className='fsi-menu-details';details.setAttribute('role','menuitem');details.onclick=()=>{if(disabled||disposed)return;close(true);onDetails();};menu.append(details);items.push(details);}
 button.onclick=()=>menu.hidden?open():close(true);
 button.addEventListener('keydown',event=>{if(['ArrowDown','ArrowUp'].includes(event.key)){event.preventDefault();open();if(event.key==='ArrowUp')items.at(-1)?.focus();}},{signal:events.signal});
 menu.addEventListener('keydown',event=>{const index=items.indexOf(document.activeElement);if(event.key==='Escape'){event.preventDefault();event.stopPropagation();close(true);}else if(event.key==='Tab')close();else if(['ArrowDown','ArrowUp','Home','End'].includes(event.key)){event.preventDefault();const next=event.key==='Home'?0:event.key==='End'?items.length-1:(index+(event.key==='ArrowDown'?1:-1)+items.length)%items.length;items[next]?.focus();}},{signal:events.signal});
 document.addEventListener('pointerdown',event=>{if(!menu.hidden&&!menu.contains(event.target)&&!button.contains(event.target))close();},{signal:events.signal});
 window.addEventListener('resize',()=>close(),{signal:events.signal});window.addEventListener('blur',()=>close(),{signal:events.signal});
 return{button,close,setDisabled(value){disabled=!!value;button.disabled=disabled;if(disabled)close();},destroy(){if(disposed)return;close();disposed=true;events.abort();for(const item of items)item.onclick=null;button.onclick=null;menu.remove();button.remove();}};
}
export function foreignSignInFields(fields){
 if(!Array.isArray(fields)||fields.length>8)throw Error('Use at most eight declared fields.');
 const names=new Set();
 return fields.map(field=>{
  if(!field||Object.keys(field).some(key=>!['name','label','kind','required','maxLength','help'].includes(key))||!/^[a-z][a-z0-9_]{0,63}$/.test(field.name)||names.has(field.name))throw Error('Use unique declared field names without stored values.');
  names.add(field.name);const kind=field.kind??'secret',maxLength=field.maxLength??4094;
  if(!['secret','text'].includes(kind)||!Number.isInteger(maxLength)||maxLength<1||maxLength>4095||field.required!==undefined&&typeof field.required!=='boolean')throw Error('Use the declared field type and length.');
  if(field.help!==undefined&&(typeof field.help!=='string'||field.help.length>400))throw Error('Use a bounded field description.');
  return Object.freeze({name:field.name,label:bounded(field.label,120,'field label'),kind,required:field.required!==false,maxLength,help:field.help||''});
 });
}
function mountDeclaredSignInCard(host,options){
 const fields=foreignSignInFields(options.fields??[]),reviewKey=bounded(options.reviewKey,600,'review identity');
 const destination=bounded(options.destination,400,'destination'),title=bounded(options.title,160,'connection title');
 const openedMessage=options.openedMessage===undefined?'Continue sign-in on the provider page. Return here when it is finished.':bounded(options.openedMessage,600,'provider-page result');
 const address=new URL(options.origin);
 if(address.protocol!=='https:'||address.username||address.password||address.search||address.hash||address.pathname!=='/')throw Error('Name the exact HTTPS provider origin.');
 if(fields.length&&typeof options.onSubmit!=='function')throw Error('Declared fields need their existing credential adapter.');
 if(options.expanded!==undefined&&typeof options.expanded!=='boolean')throw Error('Use a boolean setup disclosure state.');
 if(options.openReview!==undefined&&!providerReview(options.openReview))throw Error('Use a current provider review.');
 let directAddress=null;
 if(options.directProviderURL!==undefined){directAddress=new URL(options.directProviderURL);check(directAddress.origin===address.origin&&directAddress.protocol==='https:'&&!directAddress.username&&!directAddress.password&&!directAddress.port&&typeof options.beforeDirectOpen==='function','A direct provider continuation needs its exact reviewed origin and current-account guard.');}
 let manualAddress=null;
 if(options.manualProviderURL!==undefined){manualAddress=new URL(options.manualProviderURL);check(manualAddress.origin===address.origin&&manualAddress.protocol==='https:'&&!manualAddress.username&&!manualAddress.password&&!manualAddress.port&&['regular','incognito'].includes(options.manualBrowserContext)&&typeof options.beforeManualCopy==='function','Manual sign-in needs its reviewed HTTPS provider link, assigned browser window and current-account guard.');}
 const manualWindow=options.manualBrowserContext==='incognito'?'private window':'regular window';
 const accountChoices=options.accountChoices===undefined?null:signInAccountChoices(options.accountChoices,options.selectedAccount);
 if(accountChoices)check(typeof options.onAccountChange==='function'&&accountChoices.find(row=>row.id===options.selectedAccount)?.identifier===options.account?.identifier,'Use the current account owner and matching account preview.');
 const shell=cardShell({origin:address.origin,title,destination,account:options.account,description:options.description?bounded(options.description,600,'connection description'):'',workTitle:options.workTitle});
 const {root,id,status,actions,leading,trailing,primaryGroup}=shell;root.dataset.expanded=String(options.expanded===true);
 const form=node('form');form.className='fsi-form';form.autocomplete='off';
 const controls=[],inputs=new Map(),reveals=[];
 let viewSwitch=null;
 if(typeof options.onExpandView==='function'){viewSwitch=node('button',bounded(options.expandViewLabel||'Open side view',80,'presentation label'));viewSwitch.type='button';viewSwitch.className='fsi-view-switch';viewSwitch.onclick=()=>{if(locked())return;clearFields();options.onExpandView();};shell.head.querySelector('.fsi-brand')?.replaceWith(viewSwitch);controls.push(viewSwitch);}
 for(const field of fields){
  const label=node('label'),caption=node('span',field.label),row=node('div'),input=node('input');row.className='fsi-field-row';
  input.name=field.name;input.type=field.kind==='secret'?'password':'text';input.required=field.required;input.maxLength=field.maxLength;input.autocomplete='off';input.spellcheck=false;input.setAttribute('autocapitalize','none');
  input.setAttribute('aria-label',field.label);inputs.set(field.name,input);controls.push(input);row.append(input);
  if(field.kind==='secret'){const reveal=node('button','Show');reveal.type='button';reveal.className='fsi-reveal';reveal.setAttribute('aria-label','Show '+field.label);reveal.setAttribute('aria-pressed','false');reveal.onclick=()=>{if(locked())return;const show=input.type==='password';input.type=show?'text':'password';reveal.textContent=show?'Hide':'Show';reveal.setAttribute('aria-label',(show?'Hide ':'Show ')+field.label);reveal.setAttribute('aria-pressed',String(show));};controls.push(reveal);reveals.push({input,reveal,label:field.label});row.append(reveal);}
  label.append(caption,row);if(field.help)label.append(node('small',field.help));form.append(label);
 }
 status.textContent=options.status||'';
 let submit=null,open=null,direct=null,manual=null,manualInput=null,manualCopy=null,manualSelect=null,manualCopying=false,expand=null,primary=null,accountMenu=null,returning=false,openAvailable=options.openAvailable!==false;
 const details=options.details||(options.fieldsCollapsed&&fields.length?node('div'):null);
 if(fields.length){submit=node('button',options.submitLabel||'Store credential');submit.type='submit';submit.className='fsi-primary';controls.push(submit);form.append(submit);if(options.fieldsCollapsed&&details)details.prepend(form);else root.insertBefore(form,status);}
 if(typeof options.onOpen==='function'){open=node('button',options.openLabel||'Continue on provider site');open.type='button';open.className='fsi-takeover';if(options.openReview)providerButton(open,options.openReview);open.onclick=()=>run('open',options.onOpen);controls.push(open);leading.append(open);}
 if(directAddress){direct=node('a',options.openLabel||'Open provider to sign in');direct.className='fsi-takeover fsi-direct';direct.href=directAddress.href;direct.target='_blank';direct.rel='noopener noreferrer';direct.title='Open the reviewed provider in a separate tab. This does not open a companion panel.';direct.onclick=event=>{if(locked()||openAvailable){event.preventDefault();return;}try{options.beforeDirectOpen({reviewKey});clearFields();root.dataset.state='provider-requested';if(primary&&typeof options.onReturn==='function'){returning=true;primary.textContent=options.returnLabel||'Check setup';shell.heading.textContent='Finished signing in?';shell.description.textContent='Sign-in is on the provider’s page. Return here to check this saved setup.';}status.textContent='A separate provider tab was requested. No password was read and no connection was marked complete.';controlState();}catch{event.preventDefault();needsRead=true;status.textContent='The account or setup changed. Reload it before opening the provider.';controlState();}};leading.append(direct);}
 if(manualAddress){
  manual=node('section');manual.className='fsi-manual';manual.setAttribute('aria-label','Manual provider sign-in');const instructions=node('p','Open a '+manualWindow+' and paste this link. Choose the account shown above.');instructions.id=id+'-manual';
  const label=node('label','Reviewed provider link'),row=node('div');row.className='fsi-manual-row';manualInput=node('input');manualInput.type='text';manualInput.readOnly=true;manualInput.id=id+'-provider-link';label.htmlFor=manualInput.id;manualInput.value=manualAddress.href;manualInput.setAttribute('aria-describedby',instructions.id);manualInput.autocomplete='off';manualInput.spellcheck=false;
  manualCopy=node('button','Copy provider link');manualCopy.type='button';manualSelect=node('button','Select link');manualSelect.type='button';manualSelect.hidden=true;row.append(manualInput,manualCopy,manualSelect);manual.append(instructions,label,row);root.insertBefore(manual,status);
  const prepare=()=>{if(locked()||openAvailable||manualCopying)return false;try{options.beforeManualCopy({reviewKey});clearFields();return true;}catch{needsRead=true;status.textContent='The account or setup changed. Check setup before copying its link.';options.onUnconfirmed?.({kind:'manual',reviewKey});controlState();return false;}};
  const prepared=()=>{root.dataset.state='manual';if(primary&&typeof options.onReturn==='function'){returning=true;shell.heading.textContent='Continue in a '+manualWindow;shell.description.textContent='Sign in on the provider site, then return here to check this saved setup.';}controlState();};
  manualSelect.onclick=()=>{if(!prepare())return;manualInput.focus();manualInput.select();prepared();status.textContent='The provider link is selected. Copy it and paste it in a '+manualWindow+'.';};
  manualCopy.onclick=async()=>{if(!prepare())return;const ticket=serial;manualCopying=true;controlState();try{if(typeof navigator.clipboard?.writeText!=='function')throw Error('Clipboard unavailable');await navigator.clipboard.writeText(manualAddress.href);if(!alive()||ticket!==serial)return;prepared();status.textContent='Provider link copied. Paste it in a '+manualWindow+' and choose the account shown above.';}catch{if(!alive()||ticket!==serial)return;manualSelect.hidden=false;manualInput.focus();manualInput.select();prepared();status.textContent='Copy was unavailable. The provider link is selected so you can copy it yourself.';}finally{if(alive()&&ticket===serial){manualCopying=false;controlState();}}};
 }
 if(typeof options.onPrimary==='function'){primary=node('button',options.primaryLabel||'Continue setup');primary.type='button';primary.className='fsi-primary';primary.onclick=()=>run(reading()?'return':'primary',reading()?options.onReturn:options.onPrimary);controls.push(primary);primaryGroup.append(primary);}
 if(details){details.className=(details.className?details.className+' ':'')+'fsi-details';details.hidden=options.expanded!==true;details.id=id+'-details';expand=node('button',details.hidden?options.detailsLabel||'Expand setup details':'Collapse setup details');expand.type='button';expand.className=primary?'fsi-options':'fsi-expand';expand.setAttribute('aria-controls',details.id);expand.setAttribute('aria-expanded',String(!details.hidden));expand.onclick=()=>{if(locked())return;setExpanded(details.hidden);};controls.push(expand);(primary?primaryGroup:leading).append(expand);}
 if(options.accountAccess){shell.access.hidden=false;shell.access.textContent=bounded(options.accountAccess,400,'account access description');shell.access.dataset.state='browser';}
 if(accountChoices&&primary){if(expand){expand.className='fsi-detail-toggle';expand.textContent='Setup details';expand.setAttribute('aria-label','Expand setup details');root.insertBefore(expand,status);}accountMenu=mountAccountChoices(primaryGroup,{id,rows:accountChoices,selected:options.selectedAccount,onChoose:accountId=>run('account',options.onAccountChange,{accountId}),onDetails:details?()=>setExpanded(!root.dataset.expanded||root.dataset.expanded==='false'):undefined});}
 const dismiss=node('button','Not now');dismiss.type='button';dismiss.className='fsi-dismiss';dismiss.onclick=()=>pause();trailing.prepend(dismiss);if(details)root.append(details);
 let disposed=false,paused=false,pending=false,disabled=!!options.disabled,needsRead=!!options.requiresRead,pendingKind=null,serial=0,controller=null;
 const submissions=new Set();
 const alive=()=>!disposed&&root.isConnected!==false;
 const locked=()=>!alive()||paused||pending||disabled||needsRead;
 const reading=()=>typeof options.onReturn==='function'&&(returning||needsRead);
 function clearFields(){for(const input of inputs.values())input.value='';for(const {input,reveal,label}of reveals){input.type='password';reveal.textContent='Show';reveal.setAttribute('aria-label','Show '+label);reveal.setAttribute('aria-pressed','false');}}
 function wipe(values){for(const key of Object.keys(values))values[key]='';}
 function controlState(){for(const control of controls)control.disabled=control===primary&&reading()?!alive()||paused||pending||disabled:locked()||control===open&&!openAvailable||control===primary&&options.primaryDisabled===true;if(primary)primary.textContent=reading()?options.returnLabel||'Check setup':options.primaryLabel||'Continue setup';if(direct){direct.hidden=openAvailable;direct.setAttribute('aria-disabled',String(locked()));direct.tabIndex=locked()?-1:0;if(open)open.hidden=!openAvailable;}if(manual){manual.hidden=openAvailable||paused;manualInput.disabled=locked();manualCopy.disabled=manualSelect.disabled=locked()||manualCopying;if(open)open.hidden=!openAvailable;}if(details)details.inert=locked();accountMenu?.setDisabled(locked());dismiss.disabled=disposed||paused;root.setAttribute('aria-busy',String(pending));}
 function setExpanded(value){if(!details)return;accountMenu?.close();details.hidden=!value;root.dataset.expanded=String(value);expand.setAttribute('aria-expanded',String(value));expand.setAttribute('aria-label',value?'Collapse setup details':'Expand setup details');expand.textContent=accountChoices?(value?'Hide setup details':'Setup details'):value?'Collapse setup details':options.detailsLabel||'Expand setup details';if(!value)clearFields();options.onExpandedChange?.(value);}
 async function run(kind,callback,values={}){
  if((kind==='return'?!alive()||paused||pending||disabled:locked())||kind==='open'&&!openAvailable||kind==='primary'&&options.primaryDisabled===true){wipe(values);return;}
  const ticket=++serial;controller=new AbortController();const signal=controller.signal;pending=true;pendingKind=kind;submissions.add(values);clearFields();controlState();status.textContent=kind==='submit'?'Saving the declared credential…':kind==='primary'?'Continuing this setup…':kind==='account'?'Reading the selected account’s setup…':kind==='return'?'Checking the saved setup…':'Opening the provider page…';
  try{
   await callback(values,{reviewKey,signal});
   if(!alive()||ticket!==serial||signal.aborted)return;
   root.dataset.state=needsRead?'uncertain':kind==='submit'?'stored':kind==='primary'?'continued':kind==='open'?'provider-page':'reviewed';
   if(kind==='open'&&primary&&typeof options.onReturn==='function'){returning=true;primary.textContent=options.returnLabel||'Check setup';shell.heading.textContent='Finished signing in?';shell.description.textContent='Return here when you are done on '+address.hostname+'. This check reads your saved setup.';open.textContent='Return to provider';}
   if(kind==='return'){returning=false;primary.textContent=options.primaryLabel||'Continue setup';shell.heading.textContent=title;shell.description.textContent=options.description||'';}
   status.textContent=kind==='submit'?'Stored by the native owner. Provider access still needs verification.':kind==='primary'?(options.primaryMessage||'Setup continued. Read the current step before making another change.'):kind==='return'?'Saved setup refreshed. This does not confirm a provider login.':kind==='account'?'Account setup read. Review it before continuing.':openedMessage;
  }catch{
   if(!alive()||ticket!==serial||signal.aborted)return;
   needsRead=true;root.dataset.state='uncertain';status.textContent=kind==='return'?'The current setup could not be confirmed. Check setup again before making a change.':kind==='submit'?'The credential outcome was not confirmed. Read the current owner status before trying again.':kind==='primary'?'The setup change was not confirmed. Read the current owner status before trying again.':'The provider page could not be opened. Read the current owner status before trying again.';
   options.onUnconfirmed?.({kind,reviewKey});
  }finally{wipe(values);submissions.delete(values);if(alive()&&ticket===serial){pending=false;pendingKind=null;controller=null;controlState();}}
 }
 form.onsubmit=event=>{event.preventDefault();if(locked()||!form.reportValidity())return;const values={};for(const field of fields){const value=inputs.get(field.name).value;if(new TextEncoder().encode(value).length>field.maxLength){status.textContent='The entered value exceeds its declared field limit.';clearFields();return;}values[field.name]=value;}return run('submit',options.onSubmit,values);};
 function pause(){if(disposed||paused)return;const wasPending=pending;serial++;controller?.abort();controller=null;pending=false;paused=true;accountMenu?.close();clearFields();for(const values of submissions)wipe(values);form.hidden=true;if(details)details.hidden=true;if(expand)expand.setAttribute('aria-expanded','false');root.dataset.expanded='false';root.dataset.state='paused';status.textContent=wasPending?'This request may still finish. Read the current owner status before continuing.':'Connection entry closed. Reopen setup when you are ready.';controlState();options.onDismiss?.({reviewKey,pending:wasPending});}
 root.addEventListener('keydown',event=>{if(event.key!=='Escape'||event.defaultPrevented)return;event.preventDefault();event.stopPropagation();if(details&&!details.hidden&&!locked()){setExpanded(false);expand.focus();}else pause();});
 host.replaceChildren(root);if(needsRead)root.dataset.state='uncertain';controlState();
 return {
  element:root,
  setDisabled(value){if(disposed)return;disabled=!!value;controlState();},
  // Only the owner can admit a fresh review by remounting this card. A reread
  // remains available here while the old review's changing controls stay locked.
  requireRead(){if(disposed)return;needsRead=true;root.dataset.state='uncertain';controlState();},
  setOpenAvailable(value,message){if(disposed)return;openAvailable=!!value;if(message&&!pending&&!needsRead&&!paused)status.textContent=String(message);controlState();},
  clear:clearFields,
  dismiss:pause,
  destroy(){if(disposed)return;const interrupted=pending,kind=pendingKind;disposed=true;serial++;controller?.abort();controller=null;accountMenu?.destroy();clearFields();for(const values of submissions)wipe(values);submissions.clear();form.onsubmit=null;dismiss.onclick=null;if(open)open.onclick=null;if(direct)direct.onclick=null;if(manualCopy)manualCopy.onclick=null;if(manualSelect)manualSelect.onclick=null;if(primary)primary.onclick=null;if(expand)expand.onclick=null;if(viewSwitch)viewSwitch.onclick=null;for(const {reveal}of reveals)reveal.onclick=null;controlState();root.remove();if(interrupted)options.onUnconfirmed?.({kind,reviewKey});},
 };
}

export function mountForeignSignIn(container,options={}){
 const stylesheet=new URL('./foreign-sign-in.css',import.meta.url).href;
 if(![...document.querySelectorAll('link[rel="stylesheet"]')].some(link=>link.href===stylesheet)){const style=el('link');style.rel='stylesheet';style.href=stylesheet;style.dataset.nativeProviderStyle='';document.head.append(style);}
 if(options.jobId===undefined)return mountDeclaredSignInCard(container,options);
 return mountWorkSignIn(container,options);
}

function mountWorkSignIn(container,options){
 const {context,jobId,revision,capability,binding,onChange=()=>{},onAccessRefused=()=>{},adapter=createProviderContinuation()}=options;
 const review={context,jobId,revision,binding},entry=providerEntries[capability?.provider?.id];
 check(container&&providerReview(review)&&capability?.canContinue===true&&entry,'The native owner has not admitted provider continuation.');
 const destination=bounded(options.destinationLabel||(context==='self'?'Personal account':context),400,'destination');
 const returnPath='/sources/web?'+new URLSearchParams({context,job:jobId});
 if(options.returnHref!==undefined){const url=new URL(options.returnHref,location.origin);check(url.origin===location.origin&&url.pathname==='/sources/web'&&!url.hash&&!url.username&&!url.password&&url.searchParams.size===2&&url.searchParams.get('context')===context&&url.searchParams.get('job')===jobId,'Return to the same reviewed work.');}
 const shell=cardShell({origin:new URL(entry.url).origin,title:'Continue with '+entry.name,destination,account:{identifier:destination,label:'API access for this '+(binding.scope==='self'?'account':'organization')},description:'Finish sign-in on the provider site, then return to this work.',workTitle:options.workTitle});
 const {root:section,id,heading,description,access,status,leading,trailing,primaryGroup}=shell;
 section.classList.add('ak-provider-card');section.setAttribute('aria-label','Provider connection');
 status.textContent='Checking this connection and the browser companion…';access.hidden=false;access.textContent='Checking saved API access…';
 const open=el('button','Take over to sign in');open.type='button';open.className='fsi-takeover';open.disabled=true;providerButton(open,review);leading.append(open);
 const dismiss=el('button','Not now');dismiss.type='button';dismiss.className='fsi-dismiss';trailing.prepend(dismiss);
 const resume=el('a','Continue work');resume.href=returnPath;resume.className='fsi-primary';primaryGroup.append(resume);
 const expand=el('button','Expand');expand.type='button';expand.className='fsi-options';expand.setAttribute('aria-expanded','false');expand.setAttribute('aria-controls',id+'-details');expand.setAttribute('aria-label','Expand connection details');primaryGroup.append(expand);
 const details=el('div');details.id=id+'-details';details.hidden=true;details.className='fsi-details ak-provider-details';
 details.append(el('h4','Connection details'),el('p','Use the provider page for passwords, MFA and consent. The browser companion keeps the same page attached to this work.'));
 const connection=el('p');connection.className='fsi-connection-note';
 const direct=el('a','Open '+entry.name+' directly');direct.href=entry.url;direct.target='_blank';direct.rel='noopener noreferrer';direct.className='fsi-direct';
 const directNote=el('p','Opens a separate tab when the browser companion is unavailable.');directNote.className='fsi-help';
 const form=el('form');form.className='fsi-form';form.autocomplete='off';
 const label=el('label',entry.name+' API key'),row=el('div'),input=el('input');row.className='fsi-field-row';
 input.type='password';input.autocomplete='off';input.spellcheck=false;input.maxLength=4095;input.minLength=8;input.required=true;input.setAttribute('aria-label',entry.name+' API key');row.append(input);label.append(row);
 const save=el('button','Save API key');save.type='submit';save.className='fsi-primary';form.append(label,save);form.hidden=true;
 const remove=el('button','Remove saved API key');remove.type='button';remove.hidden=true;
 const refresh=el('button','Check connection');refresh.type='button';refresh.className='fsi-check';
 const detailActions=el('div');detailActions.className='fsi-detail-actions';detailActions.append(remove,refresh);
 details.append(connection,form,detailActions,direct,directNote);section.append(details);container.replaceChildren(section);
 let destroyed=false,busy=false,current=null,uncertain=false,providerOpened=false;
 const controller=new AbortController();
 const writable=()=>{const required=Array.isArray(current?.provider?.fields)?current.provider.fields.filter(f=>f.required):[];return current?.canAdminister===true&&current.configured&&current.capability.credentialInput==='native_api_key'&&current.provider?.declared===true&&required.length===1&&required[0].name==='api_key';};
 function controls(){
  if(destroyed)return;
  const canWrite=writable()&&!uncertain&&!busy;
  open.disabled=busy||uncertain||!current?.capability.canContinue||!current?.companion?.available;
  save.disabled=input.disabled=remove.disabled=!canWrite;form.hidden=!writable();remove.hidden=!writable()||current.provider?.stored!==true;
  refresh.disabled=busy;section.setAttribute('aria-busy',String(busy));
  const canResume=!busy&&!uncertain&&!!current?.capability.canContinue;
  resume.setAttribute('aria-disabled',String(!canResume));resume.tabIndex=canResume?0:-1;
  direct.hidden=directNote.hidden=!current?.capability.canContinue;
 }
 function renderConnection(){
  const stored=current?.provider?.stored===true;
  section.dataset.state=uncertain?'uncertain':stored?'stored':'ready';
  heading.textContent=stored?'Use this saved connection?':'Continue with '+entry.name;
  description.textContent=stored?'An API key is saved for '+destination+'. Continue to the run to review its next step.':'Finish sign-in on the provider site, then return to this work.';
  access.textContent=stored?'API key saved for this destination':'No API key saved for this destination';access.dataset.state=stored?'stored':'missing';
  connection.textContent=stored?'Your API key is stored. A provider operation still needs to verify access.':'Add an API key here when the provider gives you one.';
  if(!current?.canAdminister)connection.append(' A destination administrator manages this key.');
  open.textContent=providerOpened?'Return to provider':'Take over to sign in';
 }
 function refused(error){
  if(!accessRefused(error))return false;
  input.value='';current=null;shell.preview.hidden=true;heading.textContent='Reopen this work to continue';description.textContent='Current access to this account could not be confirmed.';
  onAccessRefused(error);return true;
 }
 function setExpanded(value){
  if(destroyed)return;details.hidden=!value;section.dataset.expanded=String(value);
  expand.setAttribute('aria-expanded',String(value));expand.setAttribute('aria-label',value?'Collapse connection details':'Expand connection details');expand.textContent=value?'Compact':'Expand';
  if(!value)input.value='';options.onExpandedChange?.(value);
 }
 async function refreshState(){
  if(destroyed||busy)return;busy=true;input.value='';controls();
  try{
   const result=await adapter.status(review,{signal:controller.signal});if(destroyed)return;
   if(!sameProviderBinding(result.binding,binding))throw nativeError('The connection belongs to another account.',409,'binding_changed');
   check(result.capability?.provider?.id===entry.id,'The connection belongs to another provider.');
   current=result;uncertain=false;shell.preview.hidden=false;renderConnection();
   status.textContent=!result.capability.canContinue?result.capability.reason:result.companion?.available?'Your work stays here while you sign in.':'Browser companion unavailable. Expand details to open the provider directly.';
  }catch(error){
   if(destroyed)return;current=null;refused(error);
   if(!destroyed){section.dataset.state='unavailable';status.textContent='This connection could not be checked. Expand details and choose Check connection.';access.textContent='Saved access unavailable';connection.textContent='Refresh before changing this connection.';}
  }finally{if(!destroyed){busy=false;controls();}}
 }
 expand.addEventListener('click',()=>setExpanded(details.hidden));
 resume.addEventListener('click',event=>{if(resume.getAttribute('aria-disabled')==='true'){event.preventDefault();return;}options.onReturnToWork?.(event);});
 open.addEventListener('click',()=>{
  if(destroyed||open.disabled)return;
  let operation;
  // The content script admits a real click before any asynchronous work.
  try{operation=adapter.continue(review,{signal:controller.signal});}catch(error){status.textContent=error.message||'The provider page could not be opened.';return;}
  busy=true;controls();status.textContent='Opening the provider page for this work…';
  Promise.resolve(operation).then(result=>{
   if(destroyed)return;
   providerOpened=true;open.textContent='Return to provider';section.dataset.state='provider-page';
   status.textContent=(result.state==='provider_focused'?'Returned to the same provider tab.':'Provider tab opened.')+(result.companion==='toolbar_required'?' Open Aurekai from Chrome’s toolbar to show the companion.':' Aurekai stays alongside it.');
  },error=>{
   if(destroyed)return;refused(error);if(destroyed)return;
   uncertain=error?.code==='provider_outcome_unconfirmed';section.dataset.state=uncertain?'uncertain':'ready';status.textContent=error.message||'The provider page could not be opened.';
  }).finally(()=>{if(!destroyed){busy=false;controls();}});
 });
 async function change(clear){
  if(destroyed||busy||!writable()||uncertain)return;
  if(!clear&&(!form.reportValidity()||new TextEncoder().encode(input.value).length>4095))return;
  let secret=input.value;input.value='';busy=true;controls();status.textContent=clear?'Removing this saved API key…':'Saving this API key…';
  try{
   const result=clear?await adapter.disconnect(review,entry.id,{signal:controller.signal}):await adapter.connect(review,entry.id,secret,{signal:controller.signal});secret='';if(destroyed)return;
   check(sameProviderBinding(result.binding,binding),'The account changed during the credential operation.');
   const provider=result.providers?.find(p=>p.id===entry.id);check(provider&&provider.stored===!clear,'The requested credential state was not confirmed.');
   current={...current,provider,configured:result.configured===true,canAdminister:result.canAdminister===true&&current.capability.canAdminister};renderConnection();status.textContent=clear?'Saved API key removed.':'API key saved. Continue work when you are ready to review the run.';onChange();
  }catch(error){
   secret='';if(destroyed)return;uncertain=true;refused(error);
   if(!destroyed){section.dataset.state='uncertain';status.textContent='The credential change was not confirmed. Check connection before trying again.';}
  }finally{secret='';if(!destroyed){busy=false;controls();}}
 }
 function dispose(){if(destroyed)return;destroyed=true;input.value='';current=null;controller.abort();adapter.dispose();section.remove();}
 function defer(){if(destroyed)return;const pending=busy;dispose();options.onDismiss?.({reviewKey:context+':'+jobId+':'+revision,pending});}
 dismiss.addEventListener('click',defer);
 section.addEventListener('keydown',event=>{if(event.key!=='Escape'||event.defaultPrevented)return;event.preventDefault();event.stopPropagation();if(!details.hidden){setExpanded(false);expand.focus();}else defer();});
 form.addEventListener('submit',event=>{event.preventDefault();void change(false);});remove.addEventListener('click',()=>void change(true));refresh.addEventListener('click',()=>void refreshState());
 if(options.expanded===true)setExpanded(true);
 void refreshState();return {element:section,dispose,dismiss:defer,clear(){input.value='';}};
}
