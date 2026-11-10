import {mountReferencePicker} from './reference-picker.mjs';
import {readWorkspacePacket} from './workspace-destinations.mjs';
const assert=(ok,message)=>{if(!ok)throw Error(message);};
// These rules recognize a public locator. They do not verify profile control,
// authorize remote access or contact a provider. Auth still owns the saved link.
export function profileReference(value){
 assert(typeof value==='string'&&value.trim().length<=1100,'Paste one public profile address.');let url;try{url=new URL(value.trim());}catch{throw Error('Paste the complete HTTPS address of your public profile.');}
 assert(url.protocol==='https:'&&!url.username&&!url.password&&!url.port,'Use a public HTTPS address without a password or custom port.');
 assert(!/^(localhost$|127\.|0\.|10\.|169\.254\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|\[)/i.test(url.hostname)&&!url.hostname.endsWith('.local'),'Use the public profile page, not a private device address.');
 assert(!url.search&&!url.hash,'Use the profile address without tracking, access tokens, query fields or a fragment.');
 let path;try{path=decodeURIComponent(url.pathname);}catch{throw Error('This profile address is malformed.');}assert(!/[\u0000-\u0020\u007f\\]/.test(path),'This profile address is malformed.');
 const host=url.hostname.replace(/^www\./,''),parts=path.split('/').filter(Boolean),remote=parts.at(-1)||host;let provider='website',label='Website link',account=host+path,known=false;
 const single={ 'github.com':['github','GitHub'], 'twitch.tv':['twitch','Twitch'], 'soundcloud.com':['soundcloud','SoundCloud'], 'ko-fi.com':['ko-fi','Ko-fi'], 'huggingface.co':['hugging-face','Hugging Face'] };
 if(Object.hasOwn(single,host)){assert(parts.length===1&&!['login','signup','settings','search','explore','features','topics','pricing','orgs','organizations','collections','models','datasets','spaces','directory','downloads'].includes(parts[0].toLowerCase()),'Open your profile page rather than a search, settings or content page.');[provider,label]=single[host];account=parts[0];known=true;}
 else if(host==='bsky.app'){assert(parts.length===2&&parts[0]==='profile','Use the Bluesky profile address, not an individual post.');provider='bluesky';label='Bluesky';account=remote;known=true;}
 else if(host==='youtube.com'){assert((parts.length===1&&parts[0].startsWith('@'))||(parts.length===2&&['channel','user','c'].includes(parts[0])),'Use your YouTube channel address, not a video link.');provider='youtube';label='YouTube';account=remote;known=true;}
 else if(host==='linkedin.com'){assert(parts.length===2&&['in','company','school'].includes(parts[0]),'Use a LinkedIn profile or organisation page.');provider='linkedin';label='LinkedIn';account=remote;known=true;}
 else if(host==='patreon.com'){assert(parts.length===1||parts.length===2&&['c','user'].includes(parts[0]),'Use the Patreon creator page.');provider='patreon';label='Patreon';account=remote;known=true;}
 else if(host.endsWith('.bandcamp.com')){assert(parts.length===0,'Use the Bandcamp artist homepage rather than an album or track.');provider='bandcamp';label='Bandcamp';account=host.slice(0,-13);known=true;}
 assert(account.length>0&&account.length<=255,'This public profile address is too long to retain as an account link.');return {provider,label,account,url:url.href,known};
}
export const readAccountChoices=(path,signal,request=fetch)=>readWorkspacePacket(path,{signal,request,timeoutMs:10000,limit:128*1024});
const packet=readAccountChoices;

export function mountAccountChoices(root=document){
 const form=root.querySelector('#connect'),institution=root.querySelector('[data-institution-picker]'),spaces=root.querySelector('[data-space-picker]');if(!form||!institution||!spaces)return;
 const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);return n;},publicURL=form.elements.namedItem('profile_url'),provider=form.elements.namedItem('provider'),account=form.elements.namedItem('remote_account'),hint=form.querySelector('[data-profile-recognition]'),submit=form.querySelector('[type=submit]');let approved='',dialog=null,principal='',generation=0,spaceRows=null;
 const signature=()=>JSON.stringify([...new FormData(form)]);
 function describe(){approved='';provider.value=account.value='';hint.replaceChildren();if(!publicURL.value.trim())return;try{const r=profileReference(publicURL.value);provider.value=r.provider;account.value=r.account;hint.textContent=r.label+' \u00B7 '+r.account+(r.known?'':' \u00B7 public address, not an authorised account');}catch(e){hint.textContent=e.message;}}
 publicURL.addEventListener('input',describe);form.addEventListener('reset',()=>queueMicrotask(()=>{describe();dialog?.close();}));
 form.addEventListener('submit',event=>{
  if(approved&&approved===signature()){approved='';return;}
  event.preventDefault();event.stopImmediatePropagation();if(document.documentElement.dataset.accountState!=='authenticated')return;
  let reference;try{reference=profileReference(publicURL.value);}catch(e){hint.textContent=e.message;publicURL.focus();return;}
  provider.value=reference.provider;account.value=reference.account;publicURL.value=reference.url;const reviewed=signature(),owner=document.documentElement.dataset.accountPrincipal;
  dialog?.remove();dialog=el('dialog');dialog.className='account-choice-dialog';dialog.dataset.linkReview='';const title=el('h2','Review this public link'),facts=el('dl');title.id='profile-link-review-title';dialog.setAttribute('aria-labelledby',title.id);for(const[k,v]of [['Service',reference.label],['Profile',reference.account],['Address',reference.url]])facts.append(el('dt',k),el('dd',v));
  const explanation=el('p','This saves a public profile link. It does not sign in to the service, verify ownership, import content, or publish to your profile sites.'),buttons=el('footer'),cancel=el('button','Back'),confirm=el('button','Save public link');cancel.type=confirm.type='button';cancel.onclick=()=>dialog.close();confirm.onclick=()=>{if(document.documentElement.dataset.accountState!=='authenticated'||document.documentElement.dataset.accountPrincipal!==owner){dialog.close();return;}if(signature()!==reviewed){dialog.close();hint.textContent='The address changed. Review the current link before saving.';return;}approved=reviewed;dialog.close();form.requestSubmit(submit);};buttons.append(cancel,confirm);dialog.append(title,facts,explanation,buttons);document.body.append(dialog);dialog.addEventListener('close',()=>{submit.focus();},{once:true});dialog.showModal();
 },true);submit.disabled=false;
 const publicChoices=mountReferencePicker(institution,{label:'Find a school, library or organisation',help:'Search by any part of its name. Results come from public sources already known to Aurekai; choosing one does not join it.',load:async({query,cursor,signal})=>{const p=await packet('/account/choices/institutions?'+new URLSearchParams({q:query,after:cursor}),signal);assert(Array.isArray(p)&&p.length===3&&p[0]===1&&Array.isArray(p[1])&&p[1].length<=20&&typeof p[2]==='string','The institution directory is incomplete.');return {rows:p[1].map(row=>{assert(Array.isArray(row)&&row.length===3&&typeof row[0]==='string'&&/^[a-z0-9.-]+$/.test(row[0])&&typeof row[1]==='string'&&Number.isSafeInteger(row[2])&&row[2]>0,'Invalid institution result.');return {id:row[0],label:row[1],description:row[0]+' \u00B7 '+row[2]+' public source'+(row[2]===1?'':'s')};}),cursor:p[2]};},onSelect:(row,{host})=>{if(!row)return;const a=el('a','Open its public information');a.href='/s/institution/'+encodeURIComponent(row.id);host.append(a);}});
 const mySpaces=mountReferencePicker(spaces,{label:'Choose your personal or shared space',help:'Only destinations available to your current account are listed. Their work stays separate.',load:async({query,cursor,signal})=>{
  const ticket=generation,owner=principal,p=await packet('/apps/_destinations',signal);
  if(ticket!==generation||owner!==principal)throw Error('Your account changed. Choose again from the current account.');
  assert(Array.isArray(p)&&p.length===2&&p[0]===1&&Array.isArray(p[1])&&p[1].length<=201,'The current destination list is incomplete.');
  const all=p[1].map(r=>{
   assert(Array.isArray(r)&&r.length===3&&typeof r[0]==='string'&&/^(self|(institution|household):[a-zA-Z0-9._-]+)$/.test(r[0])&&typeof r[1]==='string'&&typeof r[2]==='boolean','Invalid destination choice.');
   return {id:r[0],label:r[1],canConfigure:r[2],description:r[0]==='self'?'Your private work':(r[0].startsWith('household:')?'Household':'Organisation')+' \u00B7 '+(r[2]?'You can configure applications':'Your current access applies')};
  });
  const offset=Number(cursor||0);assert(Number.isInteger(offset)&&offset>=0&&offset<=all.length,'Reload this destination list.');
  const filtered=all.filter(r=>(r.label+' '+r.description).toLowerCase().includes(query.toLowerCase())),rows=filtered.slice(offset,offset+20);
  return {rows,cursor:offset+20<filtered.length?String(offset+20):''};
 },onSelect:(row,{host})=>{if(!row)return;const a=el('a','Open this space');a.href='/apps?'+new URLSearchParams({context:row.id});host.append(a);if(row.canConfigure){const configure=el('a','Choose applications for this space');configure.href='/packs/setup?'+new URLSearchParams({context:row.id});host.append(configure);}}});
 function sync(){const current=document.documentElement.dataset.accountState==='authenticated'?document.documentElement.dataset.accountPrincipal||'':'';if(current===principal)return;generation++;principal=current;spaceRows=null;mySpaces.reset();publicChoices.reset();dialog?.close();approved='';describe();if(current){void mySpaces.search();void publicChoices.search();}}
 const observer=new MutationObserver(sync);observer.observe(document.documentElement,{attributes:true,attributeFilter:['data-account-state','data-account-principal']});sync();
 return {destroy(){generation++;observer.disconnect();publicChoices.destroy();mySpaces.destroy();dialog?.remove();}};
}
if(typeof document!=='undefined'){const mounted=mountAccountChoices();addEventListener('pagehide',event=>{if(!event.persisted)mounted?.destroy();});}
