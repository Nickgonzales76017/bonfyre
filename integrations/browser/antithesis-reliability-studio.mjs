// Account-owned Antithesis reliability lab. Untrusted findings.json is read
// locally, never executed, published or sent to a model. Saving requires an
// explicit review and the existing Source Library original-byte owner.
import {reliabilityLanes} from '../bridge/antithesis-reliability.mjs';
import {prepareAntithesisTriageIntake} from '../bridge/antithesis-triage-intake.mjs';
import {prepareFile} from './source-material.mjs';
import {openDestinationStore} from '../bridge/workspace-store.mjs';
const root=document.querySelector('[data-antithesis-reliability]');
const create=(tag,text='')=>{const x=document.createElement(tag);x.textContent=text;return x;};
const action=(text,fn)=>{const x=create('button',text);x.type='button';x.addEventListener('click',fn);return x;};
function mount(){
 if(!root)return;
 const abort=new AbortController();
 const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content||'';
 const get=()=>/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint)?
  openDestinationStore(endpoint,{signal:abort.signal}):null;
 let closed=false,busy=false,reviewed=null,modal=null;
 root.append(create('h2','Reliability lab · Antithesis'),
  create('p','Review Bombadil, native assertions, SQL workloads, object-store failures, Snouty, and triage as one Source Library experience. All external platform execution requires separate admission.'),
  create('p','Six source-backed lanes · four P0, two P1. Reports from third-party agents are untrusted until their original bytes, run and tested commit are independently verified.'));
 const lanes=create('ul');lanes.setAttribute('aria-label','Antithesis reliability lanes');
 for(const lane of reliabilityLanes){
  const li=create('li',lane.priority+' · '+lane.id+' · '+lane.owner+' · '+lane.assets.join(', '));
  lanes.append(li);
 }
 root.append(lanes);
 const fields=create('fieldset');fields.append(create('legend','Review a triage-action findings.json original'));
 function field(label,placeholder,aria){
  const l=create('label',label),i=create('input');i.type='text';i.placeholder=placeholder;
  i.setAttribute('aria-label',aria);i.autocomplete='off';i.spellcheck=false;
  l.append(i);fields.append(l);return i;
 }
 const repo=field('Exact public GitHub repository','antithesishq/glitch-grid','Antithesis source repository');
 const commit=field('Tested Git commit (40 hexadecimal characters)','Git commit SHA','Antithesis tested Git commit');
 const sourceSHA=field('Retained source SHA-256 (64 hexadecimal characters)','Source digest','Antithesis source digest');
 const choose=create('input');choose.type='file';choose.accept='.json,application/json';
 choose.setAttribute('aria-label','Antithesis triage findings JSON');
 const chooseLabel=create('label','Original triage report (.json, maximum 256 KiB)');chooseLabel.append(choose);fields.append(chooseLabel);
 root.append(fields);
 const status=create('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 const results=create('section');results.setAttribute('aria-label','Antithesis reviewed findings');
 const keep=action('Review original retention',()=>void reviewOriginal());keep.disabled=true;
 const inspect=action('Inspect triage report',()=>void inspectTriage());
 const actions=create('div');actions.className='wp-actions';actions.append(inspect,keep);
 root.append(actions,status,results);
 const say=msg=>{if(!closed)status.textContent=msg;};
 function clear(){
  closed=true;abort.abort();reviewed=null;choose.value='';modal?.remove();modal=null;
  root.replaceChildren(create('p','Account changed. Untrusted triage report and review choices were cleared.'));
 }
 addEventListener('bonfyre:account-changed',clear,{signal:abort.signal});
 addEventListener('pagehide',clear,{signal:abort.signal});
 const invalidate=()=>{
  reviewed=null;keep.disabled=true;results.replaceChildren();say('Inputs changed. Recheck the exact original before retaining.');
 };
 for(const input of [repo,commit,sourceSHA])input.addEventListener('input',invalidate,{signal:abort.signal});
 choose.addEventListener('change',invalidate,{signal:abort.signal});
 async function inspectTriage(){
  if(closed||busy)return;
  reviewed=null;keep.disabled=true;results.replaceChildren();
  const file=choose.files?.[0];
  if(!file||choose.files.length!==1||!/\.json$/i.test(file.name)||file.size<2||file.size>256*1024){
   say('Choose one unmodified findings.json no larger than 256 KiB.');return;
  }
  busy=true;inspect.disabled=true;say('Validating bounded triage artifact locally…');
  try{
   const coordinates=Object.freeze({
    repository:repo.value.trim(),git_commit:commit.value.trim(),source_sha256:sourceSHA.value.trim()
   });
   const receipt=await prepareAntithesisTriageIntake(new Uint8Array(await file.arrayBuffer()),coordinates);
   abort.signal.throwIfAborted();
   reviewed={file,receipt,coordinates};keep.disabled=false;
   const heading=create('h3',receipt.candidate_count+' unverified failure candidates');
   const info=create('p','Original SHA-256 '+receipt.report_sha256+' · '+receipt.report_bytes+
     ' bytes · '+receipt.run_count+' runs. No Snouty calls, model generation, patches, or source writes.');
   const list=create('ul');
   for(const item of receipt.candidates){
    const li=create('li',item.source.run_id+' · '+item.source.property_name+' · '+item.key);
    li.style.overflowWrap='anywhere';list.append(li);
   }
   results.append(heading,info,list);
   say('Report structure and digest checked. Its claims and provider run remain unverified.');
  }catch(error){if(!abort.signal.aborted)say(error.message);}
  finally{busy=false;if(!closed)inspect.disabled=false;}
 }
 async function checkOriginal(store,prepared,receipt,coordinates){
  const row=await store.readVersion(prepared.key);
  if(!row)return {state:'absent'};
  if(row.deleted||row.revision<1||row.value?.kind!=='file'||
    row.value?.contentSHA256!==receipt.report_sha256||
    row.value?.filename!==prepared.value.filename||
    !(row.value.original instanceof Blob))return {state:'conflict'};
  const bytes=new Uint8Array(await row.value.original.arrayBuffer());
  if(bytes.length!==receipt.report_bytes)return {state:'conflict'};
  // Revalidate the original text when the saved record is found.
  const original=await prepareAntithesisTriageIntake(bytes,coordinates);
  return original.report_sha256===receipt.report_sha256?
    {state:'retained',revision:row.revision}:{state:'conflict'};
 }
 async function reviewOriginal(){
  if(closed||modal||!reviewed||!get())return;
  const {file,receipt,coordinates}=reviewed;
  let prepared,store,prior;
  try{
   prepared=await prepareFile(file,{title:'Antithesis triage · '+coordinates.repository+
     ' · '+coordinates.git_commit.slice(0,12),collection:'Antithesis Reliability Lab',
    tags:['antithesis','reliability','triage','unverified'],
    notes:'Unverified third-party findings.json; original SHA-256 '+receipt.report_sha256+
     '; source '+coordinates.repository+'@'+coordinates.git_commit+
     '. Imported statements are not verified run results or approved fixes.'});
   if(prepared.value.contentSHA256!==receipt.report_sha256||!prepared.value.textLike||
      prepared.value.original!==file)throw Error('Source Library original changed after review.');
   store=get();prior=await checkOriginal(store,prepared,receipt,coordinates);
   abort.signal.throwIfAborted();
  }catch(error){if(!abort.signal.aborted)say('Retention refused: '+error.message);return;}
  const dialog=create('dialog');modal=dialog;
  dialog.append(create('h3','Review original report retention'),
   create('p','Keep the exact JSON report in your private Source Library for human evidence review. This does not authenticate Antithesis, install Snouty, produce verified findings, run commands, apply patches or publish sources.'),
   create('p',receipt.report_sha256));
  const feedback=create('p');feedback.setAttribute('role','status');
  const save=action('Save original as reviewed source',()=>void commitSave());
  const close=action('Close',()=>{if(!working)dialog.close();});
  const link=create('a','Open exact saved source in Source Library');
  link.href='../?'+new URLSearchParams({record:prepared.key});link.hidden=true;
  let working=false,attempted=false;
  dialog.append(feedback,save,link,close);document.body.append(dialog);
  dialog.addEventListener('close',()=>{if(modal===dialog)modal=null;dialog.remove();});
  dialog.addEventListener('cancel',event=>{if(working)event.preventDefault();});
  if(prior.state==='retained'){
   feedback.textContent='This original already exists at revision '+prior.revision+'.';
   save.disabled=true;link.hidden=false;
  }else if(prior.state==='conflict'){
   feedback.textContent='A conflicting file identity is already retained. No overwrite allowed.';save.disabled=true;
  }else feedback.textContent='Review the original SHA-256 and choose Save to retain one immutable original.';
  dialog.showModal();
  async function commitSave(){
   if(closed||working||attempted||prior.state!=='absent')return;
   // A review is pinned to the exact input bytes and coordinates. Programmatic
   // or background-field edits after the modal opens cannot authorize a write.
   if(reviewed?.file!==file||reviewed?.receipt!==receipt||
      choose.files?.[0]!==file||
      repo.value.trim()!==coordinates.repository||
      commit.value.trim()!==coordinates.git_commit||
      sourceSHA.value.trim()!==coordinates.source_sha256){
    save.disabled=true;feedback.textContent='Inputs changed after review. No source was saved.';return;
   }
   attempted=working=true;save.disabled=close.disabled=true;
   feedback.textContent='Saving the reviewed original…';
   try{
    await store.saveLocal(prepared.key,prepared.value,0);
    abort.signal.throwIfAborted();
    const saved=await checkOriginal(store,prepared,receipt,coordinates);
    if(saved.state!=='retained')throw Error('Exact readback not established; no automatic retry.');
    link.hidden=false;feedback.textContent='Retained at revision '+saved.revision+'. No Antithesis execution occurred.';
   }catch(error){
    if(!abort.signal.aborted){
     // A lost acknowledgement might still mean a successful write. Never retry.
     const readback=await checkOriginal(store,prepared,receipt,coordinates).catch(()=>({state:'unknown'}));
     if(readback.state==='retained'){
      link.hidden=false;feedback.textContent='Original confirmed by exact readback at revision '+readback.revision+'.';
     }else feedback.textContent='Save result uncertain. Check Source Library before any new review; no retry sent.';
    }
   }finally{working=false;if(!closed)close.disabled=false;}
  }
 }
}
mount();
