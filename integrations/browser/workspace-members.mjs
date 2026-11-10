import './invitation-choice.mjs';
import {mountCollection} from './interaction-collection.mjs';
import {renderChanges} from './record-comparison.mjs';
const capabilities = {
  none: {read:false,write:false,configure:false,manage:false},
  read: {read:true,write:false,configure:false,manage:false},
  write: {read:true,write:true,configure:false,manage:false},
  admin: {read:true,write:true,configure:true,manage:true}
};
const capabilityLabels = {read:'Read shared work',write:'Edit shared work',configure:'Configure applications',manage:'Manage organization access'};
const roleLabels = {none:'Remove access',read:'Read shared work',write:'Edit shared work',admin:'Manage this organization'};
const isRole=value=>Object.hasOwn(capabilities,value);
export function memberRows(document, organization) {
  const result=[];
  for(const node of document.querySelectorAll('[data-org-member]')) {
    const form=node.querySelector('[data-org-member-form]');
    if(!form)continue; // The existing owner excludes self-modification.
    const value=name=>form.elements.namedItem(name)?.value;
    if(value('operation')!=='member'||value('organization')!==organization||!value('email')||!isRole(value('expected')))throw Error('The membership view does not match the selected organization.');
    const rawAction=form.getAttribute('action');if(rawAction!=='/organizations')throw Error('The membership operation has an unsupported destination.');
    const fields=Object.fromEntries([...form.querySelectorAll('input[type="hidden"]')].map(input=>[input.name,input.value]));
    if(Object.keys(fields).sort().join(',')!=='email,expected,expiry,operation,organization')throw Error('The member operation changed; reload its current interface.');
    const options=[...form.querySelectorAll('select[name="role"] option')].map(option=>option.value);
    const title=node.cells?.[0]?.childNodes?.[0]?.textContent.trim()||value('email');
    result.push({id:value('email'),title,subtitle:value('email'),version:value('expected')+'\n'+value('expiry'),
      status:value('expected')==='none'?'Removed':value('expected'),category:value('expected')==='none'?'Removed':'Members',
      data:{fields,options,role:value('expected'),expiry:value('expiry')}});
  }
  if(new Set(result.map(row=>row.id)).size!==result.length)throw Error('The member view has duplicate account identities.');
  return result;
}
export function accessChanges(row, target) {
  if(!isRole(target)||!row.data.options.includes(target))throw Error('That access level is not offered by the native owner.');
  const before=capabilities[row.data.role],after=capabilities[target];
  return Object.keys(before).filter(key=>before[key]!==after[key]).map(key=>({path:capabilityLabels[key],before:before[key]?'Allowed':'Not allowed',after:after[key]?'Allowed':'Not allowed'}));
}
export function mountMemberWorkspace(document=globalThis.document,{request=globalThis.fetch?.bind(globalThis),origin=globalThis.location?.origin}={}) {
  const sample=document?.querySelector('[data-org-member-form]');if(!sample||!origin||!request)return null;
  const organization=sample.elements.namedItem('organization').value,original=sample.closest('section');
  if(!original||original.previousElementSibling?.hasAttribute('data-member-workspace'))return null;
  const el=(tag,value)=>{const n=document.createElement(tag);if(value!==undefined)n.textContent=String(value);return n;};
  const section=el('section');section.dataset.memberWorkspace='';const heading=el('h2','People and access'),selectionHost=el('div'),controls=el('div');controls.className='ac-toolbar';
  const help=el('p','Select members, compare the access change, then apply it to those accounts. Your own access remains under another administrator’s control.');
  const notice=el('p');notice.setAttribute('role','status');notice.setAttribute('aria-live','polite');
  const review=el('div');review.hidden=true;const result=el('div');result.setAttribute('aria-live','polite');
  let busy=false,proposed=null,choices=[],inspected=null;
  const actions=[];
  let collection;
  const rows=memberRows(document,organization);
  collection=mountCollection(selectionHost,{rows,label:'Loaded organization members',onSelection(selected){choices=selected;if(!busy){proposed=null;review.hidden=true;}for(const button of actions)button.disabled=busy||!selected.length;},renderInspector(row,host){
    inspected=row.id;host.append(el('p','Current recorded access: '+row.data.role));
    if(row.data.expiry)host.append(el('p','Existing expiry: '+row.data.expiry+'. The owner retains it or shortens it to your administration window. Expired access is not renewed.'));
    const list=el('dl');
    for(const [key,allowed]of Object.entries(capabilities[row.data.role])){const item=el('div');item.className='ac-change';item.append(el('dt',capabilityLabels[key]),el('dd',allowed?'Included in this role':'Not included'));list.append(item);}host.append(list);
    host.append(el('p','These roles concern this organization’s shared work. They do not expose personal records or confer payment-provider authority.'));
  }});
  for(const role of ['read','write','admin','none']){const button=el('button',roleLabels[role]);button.type='button';button.disabled=true;button.dataset.proposeRole=role;button.onclick=()=>preview(role);actions.push(button);controls.append(button);}
  const truncated=original.textContent.includes('first 100');
  section.append(heading,help,selectionHost,controls,notice,review,result);
  if(truncated)section.append(el('p','This working set contains only the first 100 loaded members. Selection never includes accounts outside it.'));
  original.before(section);original.hidden=true;
  const fallback=el('button','Use individual native forms');fallback.type='button';fallback.style.minHeight='44px';fallback.onclick=()=>{if(!busy){collection.destroy();section.remove();original.hidden=false;}};section.append(fallback);
  function preview(role){
    if(busy||!choices.length)return;
    const selected=choices.filter(row=>row.data.role!==role);
    if(!selected.length){notice.textContent='Every selected member already has that recorded role.';return;}
    if(selected.some(row=>!row.data.options.includes(role))){notice.textContent='This access level is not available for every selected member.';return;}
    proposed={role,rows:selected};review.hidden=false;review.replaceChildren(el('h3','Review '+selected.length+' member '+(selected.length===1?'change':'changes')));
    for(const row of selected){const article=el('article');article.append(el('h4',row.title+' · '+row.data.role+' → '+role));renderChanges(article,accessChanges(row,role),{element:el,labelBefore:'Before',labelAfter:'After'});review.append(article);}
    review.append(el('p','Each native member change is confirmed separately. A conflict or uncertain response stops the remaining changes; already completed changes are shown and are not undone.'));
    const apply=el('button','Apply reviewed changes'),cancel=el('button','Cancel review');apply.type=cancel.type='button';apply.dataset.applyMembers='';apply.onclick=()=>perform(proposed);cancel.onclick=()=>{if(!busy){proposed=null;review.hidden=true;}};review.append(apply,cancel);notice.textContent=selected.length+' selected members will change. Other members are unaffected.';
  }
  async function currentRows(){
    const url=new URL('/organizations',origin);url.searchParams.set('organization',organization);
    const response=await request(url,{credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'text/html'}});
    if(!response.ok||!response.headers.get('content-type')?.startsWith('text/html'))throw Error('The current membership view is unavailable. No further changes were sent.');
    const html=await response.text();if(html.length>2*1024*1024)throw Error('The membership response exceeds this view’s bound.');
    const parsed=new DOMParser().parseFromString(html,'text/html');
    if(!parsed.querySelector('[data-org-member-form]'))throw Error('Current organization administration could not be confirmed.');
    return memberRows(parsed,organization);
  }
  async function perform(plan){
    if(busy||!plan||plan!==proposed)return;busy=true;collection.controller.lock();fallback.disabled=true;for(const b of review.querySelectorAll('button'))b.disabled=true;for(const b of actions)b.disabled=true;result.replaceChildren();
    const outcomes=[];let last=null;
    try{
      const fresh=await currentRows();
      for(const selected of plan.rows){const live=fresh.find(row=>row.id===selected.id);if(!live||live.version!==selected.version)throw Error('A reviewed member changed. Reopen the review before applying this set.');}
      for(const selected of plan.rows){
        notice.textContent='Changing '+selected.title+'…';last=selected;
        const body=new URLSearchParams({...selected.data.fields,role:plan.role});let response;
        try{response=await request(new URL('/organizations',origin),{method:'POST',credentials:'same-origin',cache:'no-store',redirect:'follow',headers:{'Content-Type':'application/x-www-form-urlencoded',Accept:'text/html'},body});}
        catch{throw Error('The response for '+selected.title+' was lost. Its outcome is unconfirmed; refresh before another change.');}
        if(!response.ok)throw Error('The native owner refused '+selected.title+' ('+response.status+'). No further member changes were sent.');
        const destination=new URL(response.url);if(destination.origin!==origin||destination.pathname!=='/organizations'||destination.searchParams.get('organization')!==organization)throw Error('The save returned to a different context. Stop and refresh this membership view.');
        const after=await currentRows(),saved=after.find(row=>row.id===selected.id);
        if(!saved||saved.data.role!==plan.role||(plan.role!=='none'&&selected.data.expiry&&(!saved.data.expiry||saved.data.expiry>selected.data.expiry)))throw Error('The current result for '+selected.title+' does not confirm the requested role. Stop and refresh.');
        outcomes.push({id:selected.id,title:selected.title,status:'Confirmed: '+plan.role});last=null;
        result.append(el('p',selected.title+' — confirmed '+plan.role+(saved.data.expiry?' until '+saved.data.expiry:'')));
      }
      notice.textContent=outcomes.length+' member '+(outcomes.length===1?'change':'changes')+' confirmed.';
    }catch(error){notice.textContent=error.message;if(last)result.append(el('p',last.title+' — not confirmed by this review'));const remaining=plan.rows.filter(row=>!outcomes.some(done=>done.id===row.id)&&row.id!==last?.id);if(remaining.length)result.append(el('p',remaining.length+' remaining '+(remaining.length===1?'change was':'changes were')+' not sent.'));}
    finally{
      collection.controller.lock(false);
      try{const next=await currentRows();collection.refresh(next);collection.controller.clear();if(inspected&&next.some(row=>row.id===inspected))collection.inspect(inspected);}catch{notice.textContent+=' The current member list could not be refreshed.';}
      busy=false;proposed=null;review.hidden=true;fallback.disabled=false;for(const button of actions)button.disabled=!choices.length;
    }
  }
  return {collection,section,destroy(){if(busy)throw Error('A member operation is still pending.');collection.destroy();section.remove();original.hidden=false;}};
}
// Stitch organization workspaces: display-only cuts over the exact account-owned
// organization cards emitted by BonfyreAuth. A filtered card is not a new grant.
export function mountOrganizationDirectory(doc=globalThis.document){
 const grid=doc?.querySelector('.org-grid');if(!grid||grid.dataset.orgFiltersMounted==='true')return null;
 const rows=[...grid.querySelectorAll(':scope > section[data-organization][data-org-role]')];
 if(!rows.length)return null;
 const el=(tag,value)=>{const node=doc.createElement(tag);if(value!==undefined)node.textContent=String(value);return node;};
 const controls=el('section');controls.className='org-directory-filters';controls.dataset.orgDirectory='';controls.setAttribute('aria-label','Find your organizations');
 const heading=el('h2','Your shared organizations');
 const searchLabel=el('label','Find an organization'),search=el('input');search.type='search';search.maxLength=160;search.setAttribute('aria-label','Find an organization');search.placeholder='Filter the organizations loaded here';searchLabel.append(search);
 const accessLabel=el('label','Current access'),access=el('select');access.setAttribute('aria-label','Filter by organization access');
 for(const [value,title] of [['all','All access'],['read','Read'],['write','Write'],['admin','Admin']]){const option=el('option',title);option.value=value;access.append(option);}
 accessLabel.append(access);
 const notice=el('p');notice.setAttribute('role','status');notice.setAttribute('aria-live','polite');notice.className='org-directory-count';
 controls.append(heading,searchLabel,accessLabel,notice);grid.before(controls);grid.dataset.orgFiltersMounted='true';
 const update=()=>{const query=search.value.trim().toLocaleLowerCase();let count=0;
  for(const row of rows){const title=row.querySelector('h2')?.textContent||'';const role=row.dataset.orgRole;
   const matches=(!query||title.toLocaleLowerCase().includes(query))&&(access.value==='all'||role===access.value);
   row.hidden=!matches;if(matches)count++;}
  const hasMore=!!doc.querySelector('a[href^="/organizations?after="]');
  notice.textContent=count+' of '+rows.length+' currently loaded organizations'+(hasMore?'; earlier or later pages are not searched.':'.');
 };
 search.addEventListener('input',update);access.addEventListener('change',update);update();
 return {controls,update,destroy(){grid.dataset.orgFiltersMounted='false';for(const row of rows)row.hidden=false;controls.remove();}};
}
if(globalThis.document){try{mountOrganizationDirectory();mountMemberWorkspace();}catch(error){const form=document.querySelector('[data-org-member-form]');if(form){const message=document.createElement('p');message.setAttribute('role','alert');message.textContent='Interactive people view unavailable. The individual native forms remain available.';form.closest('section')?.before(message);}}}
