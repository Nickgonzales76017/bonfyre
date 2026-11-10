import {openDestinationStore} from './workspace-store.mjs';
import {readWorkspacePacket} from './workspace-destinations.mjs';
import {digest} from './workspace-value.mjs';

export const crmFinancePrefix='crm-finance:';
const bindingPattern=/^ws-[a-f0-9]{40}$/;
const sourcePattern=/^opportunity:(?:feed|actor):[a-f0-9]{64}$/;
const financePattern=/^crm-finance:[a-f0-9]{64}$/;
const hex64=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const safeText=(v,n=1000)=>typeof v==='string'&&v.length>0&&v.length<=n&&!/[\u0000-\u001f\u007f]/.test(v);
const element=(name,content)=>{const n=document.createElement(name);if(content!==undefined)n.textContent=content;return n;};

export function validateCrmSource(row,key,expectedRevision){
  if(!sourcePattern.test(key)||!Number.isSafeInteger(expectedRevision)||expectedRevision<1||expectedRevision>2147483646)throw Error('Choose an exact saved CRM opportunity version.');
  if(!row||row.key!==key||row.revision!==expectedRevision||row.deleted)throw Error('The CRM opportunity changed. Reopen its current saved revision.');
  const value=row.value?.state?.relationship;
  if(value?.schema!=='bonfyre.relationship.v1'||!hex64(value.id)||!safeText(value.label,500)||value.state!=='observed'||value.actor?.state!=='unresolved')throw Error('The selected CRM record is not an unresolved reviewed source observation.');
  if(!value.source||!safeText(value.source.itemRef,160)||!hex64(value.source.snapshot)||!safeText(value.source.publicPath,240)||!value.source.publicPath.startsWith('/o/'))throw Error('This CRM opportunity has incomplete retained source evidence.');
  if(!value.opportunity||!['job-opening','observed-mention'].includes(value.opportunity.kind)||value.opportunity.stage!=='observed')throw Error('This relationship has no reviewable opportunity.');
  return value;
}
export function estimatedCents(amount){
  if(typeof amount!=='string'||!/^(?:0|[1-9][0-9]{0,8})(?:\.[0-9]{1,2})?$/.test(amount))throw Error('Enter a nonnegative USD estimate with at most two decimals.');
  const [d,c='']=amount.split('.'),value=Number(d)*100+Number((c+'00').slice(0,2));
  if(!Number.isSafeInteger(value)||value<=0||value>99999999999)throw Error('Enter a positive review estimate within the supported range.');
  return value;
}
export function crmFinanceDraft(sourceBinding,sourceKey,revision,relationship,amount,notes){
  if(!bindingPattern.test(sourceBinding)||!sourcePattern.test(sourceKey)||!Number.isSafeInteger(revision)||revision<1)throw Error('The receiving source coordinates are incomplete.');
  if(!safeText(notes||' ',2000))throw Error('Review notes exceed the supported length.');
  if(relationship?.schema!=='bonfyre.relationship.v1'||!hex64(relationship.source?.snapshot))throw Error('Reopen the source observation before preparing finance.');
  return {schema:'bonfyre.crm-finance-review.v1',title:relationship.label,status:'review-draft',
    source:{binding:sourceBinding,key:sourceKey,revision,snapshot:relationship.source.snapshot,itemRef:relationship.source.itemRef,publicPath:relationship.source.publicPath,relationshipId:relationship.id},
    estimate:{currency:'USD',amount_cents:estimatedCents(amount),notes:notes||'',approval:'not-requested'},
    effects:{invoice:false,ledger_posted:false,payment:false,contract:false,commitment:false}};
}
export function validateCrmFinanceDraft(row,key){
  if(!financePattern.test(key)||row?.key!==key||!Number.isSafeInteger(row.revision)||row.revision<1)throw Error('The selected finance review is unavailable.');
  const v=row.value;
  if(v?.schema!=='bonfyre.crm-finance-review.v1'||v.status!=='review-draft'||!safeText(v.title,500)||!bindingPattern.test(v.source?.binding)||!sourcePattern.test(v.source?.key)||!hex64(v.source?.snapshot)||!Number.isSafeInteger(v.source?.revision)||v.source.revision<1||v.estimate?.currency!=='USD'||!Number.isSafeInteger(v.estimate?.amount_cents)||v.estimate.amount_cents<=0||v.estimate.approval!=='not-requested'||!v.effects||Object.keys(v.effects).sort().join(',')!=='commitment,contract,invoice,ledger_posted,payment'||Object.values(v.effects).some(x=>x!==false))throw Error('This record does not satisfy the finance review boundary.');
  return v;
}
// Both workspaces must resolve in one current destination. Independent actor
// permissions to two institutions must never authorize cross-scope copies.
export async function verifySameCrmFinanceDestination({context,crmBinding,financeBinding,financeRelease,request=readWorkspacePacket,signal}={}){
  if(typeof context!=='string'||!/^(?:self|(?:institution|household):[A-Za-z0-9][A-Za-z0-9._-]{0,95})$/.test(context)||
     !bindingPattern.test(crmBinding)||!bindingPattern.test(financeBinding)||!hex64(financeRelease))
    throw Error('Review both editors in one current account destination.');
  const scope=context==='self'?'self':context.split(':')[0];
  const binding=async(pack,id)=>{
    const packet=await request('/apps/_bindings?'+new URLSearchParams({context,pack}),{signal});
    if(!Array.isArray(packet)||packet.length!==4||packet[0]!==1||packet[2]!==scope||
       !(context==='self'?/^user:[1-9][0-9]*$/.test(packet[1]):packet[1]===context)||
       !Array.isArray(packet[3])||packet[3].length>64)
      throw Error('The editor authority could not be verified in this destination.');
    const matches=packet[3].filter(row=>Array.isArray(row)&&row.length===6&&row[0]===id&&row[1]===pack&&
      typeof row[2]==='string'&&row[2].endsWith('/')&&hex64(row[3])&&row[5]===true);
    if(matches.length!==1)throw Error('The selected application is unavailable in the current destination.');
    return {subject:packet[1],release:matches[0][3]};
  };
  const [finance,crm]=await Promise.all([
    binding('experience.finance-workbooks.workspace',financeBinding),
    binding('experience.skill-instruments.workspace',crmBinding)
  ]);
  if(finance.subject!==crm.subject||finance.release!==financeRelease)
    throw Error('The account or Finance release changed. Reopen the installed Finance workspace.');
  return {subject:finance.subject,scope,financeRelease,crmRelease:crm.release};
}
const display=(root,label,value)=>root.append(element('p',label+': '+value));

export async function mountCrmFinanceHandoff(root){
  if(!root)throw Error('The CRM finance review host is missing.');
  const abort=new AbortController(),signal=abort.signal,clear=()=>{abort.abort();root.replaceChildren(element('h1','Reopen finance review'),element('p','The account or page context changed.'));};
  addEventListener('pagehide',clear,{signal});addEventListener('bonfyre:account-changed',clear,{signal});
  const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
  if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint||''))throw Error('Choose an installed Finance workspace.');
  const qs=new URL(location.href).searchParams,record=qs.get('record'),finance=openDestinationStore(endpoint,{signal});
  if(!record||qs.getAll('record').length!==1)throw Error('Open an exact CRM source or saved finance review.');
  root.replaceChildren(element('h1','CRM to Finance'),element('p','Review an estimate from an exact retained CRM source. This does not issue an invoice, post a ledger entry, approve spending or send a payment.'));
  if(financePattern.test(record)){
    if([...qs.keys()].some(k=>k!=='record'))throw Error('Reopen the saved finance record without source overrides.');
    const row=await finance.readVersion(record);signal.throwIfAborted();const saved=validateCrmFinanceDraft(row,record);
    display(root,'Review',saved.title);display(root,'Saved version',row.revision);display(root,'Source version',saved.source.revision);
    display(root,'Proposed estimate',(saved.estimate.amount_cents/100).toFixed(2)+' USD');
    display(root,'Status','Review draft; no economic effect');display(root,'Notes',saved.estimate.notes||'None');
    const a=element('a','Open credited feed item');a.href=saved.source.publicPath;root.append(a);root.dataset.ready='true';return;
  }
  const binding=qs.get('crm'),revision=Number(qs.get('revision'));
  if(qs.size!==3||qs.getAll('crm').length!==1||qs.getAll('revision').length!==1||!bindingPattern.test(binding)||!sourcePattern.test(record)||!Number.isSafeInteger(revision)||revision<1)throw Error('Open this review from an exact saved CRM opportunity.');
  await verifySameCrmFinanceDestination({
    context:document.querySelector('meta[name="bonfyre-workspace-context"]')?.content,
    financeRelease:document.querySelector('meta[name="bonfyre-workspace-release"]')?.content,
    financeBinding:endpoint.slice('/apps/'.length,-'/_records'.length),
    crmBinding:binding,signal
  });
  signal.throwIfAborted();
  const crm=openDestinationStore('/apps/'+binding+'/_records',{signal});
  const current=await crm.readVersion(record);signal.throwIfAborted();const relationship=validateCrmSource(current,record,revision);
  const key=crmFinancePrefix+await digest(new TextEncoder().encode(binding+'\n'+record));signal.throwIfAborted();
  const existing=await finance.readVersion(key);signal.throwIfAborted();
  if(existing&&!existing.deleted){
    validateCrmFinanceDraft(existing,key);display(root,'Existing draft','This opportunity already has a finance review. No new draft was created.');
    const a=element('a','Open existing finance review');a.href='?'+new URLSearchParams({record:key});root.append(a);root.dataset.ready='true';return;
  }
  if(existing?.deleted)throw Error('A previously removed finance review requires an explicit restore through its owner.');
  display(root,'CRM source',relationship.label);display(root,'Source revision',revision);
  const sourceLink=element('a','Open credited source');sourceLink.href=relationship.source.publicPath;root.append(sourceLink);
  const form=element('form'),amount=element('input'),notes=element('textarea'),button=element('button','Save finance review draft'),message=element('p');
  amount.type='text';amount.inputMode='decimal';amount.required=true;amount.placeholder='Amount in USD, e.g. 1250.00';amount.setAttribute('aria-label','Proposed amount in USD');notes.maxLength=2000;notes.setAttribute('aria-label','Estimate assumptions');button.type='submit';message.setAttribute('role','status');
  const amountLabel=element('label','Proposed estimate (USD)'),notesLabel=element('label','Estimate assumptions');amountLabel.append(amount);notesLabel.append(notes);form.append(amountLabel,notesLabel,button,message);root.append(form);
  let pending=false;
  form.addEventListener('submit',async e=>{
    e.preventDefault();if(pending||signal.aborted)return;pending=true;button.disabled=true;message.textContent='Checking the exact source and receiving workspace…';
    let attempted=false,prepared;
    try{
      prepared=crmFinanceDraft(binding,record,revision,relationship,amount.value.trim(),notes.value.trim());
      const latest=await crm.readVersion(record);signal.throwIfAborted();
      const same=validateCrmSource(latest,record,revision);
      if(same.source.snapshot!==prepared.source.snapshot||same.id!==prepared.source.relationshipId)throw Error('CRM source changed; reopen the current revision.');
      const retained=await finance.readVersion(key);signal.throwIfAborted();
      if(retained&&!retained.deleted)throw Error('A finance review already exists. Reopen the saved Finance collection.');
      if(retained?.deleted)throw Error('A removed finance review cannot be silently recreated.');
      attempted=true;await finance.saveLocal(key,prepared,0);signal.throwIfAborted();
      message.textContent='Finance review draft saved. No invoice or payment was created.';
      const a=element('a','Reopen saved Finance review');a.href='?'+new URLSearchParams({record:key});form.append(a);button.remove();
    }catch(error){
      if(signal.aborted)return;
      if(attempted){
        let retained;try{retained=await finance.readVersion(key);}catch{}
        if(retained&&!retained.deleted&&retained.value?.schema==='bonfyre.crm-finance-review.v1'&&retained.value.source?.snapshot===prepared.source.snapshot&&retained.value.estimate?.amount_cents===prepared.estimate.amount_cents){
          message.textContent='The saved finance draft was recovered from the native owner. No write was replayed.';
          const a=element('a','Reopen saved Finance review');a.href='?'+new URLSearchParams({record:key});form.append(a);button.remove();return;
        }
        message.textContent='Save outcome is uncertain. Reopen Finance before attempting another write.';
        return;
      }
      message.textContent=error.message;button.disabled=false;
    }finally{pending=false;}
  },{signal});root.dataset.ready='true';
}
if(typeof document!=='undefined'){
  const root=document.querySelector('[data-crm-finance-handoff]');
  if(root)mountCrmFinanceHandoff(root).catch(e=>{root.dataset.failed='true';root.replaceChildren(element('h1','Finance review unavailable'),element('p',e.message));});
}
