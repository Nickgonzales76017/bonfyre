// Signed-in Source Library continuation for RepositoryFabric's immutable YAFF cuts.
// Does not fetch a Git provider, retain bytes, bind a PackGraph pack or execute writes.
import {readWorkspacePacket} from '../bridge/workspace-destinations.mjs';
import {openDestinationStore} from '../bridge/workspace-store.mjs';
import {prepareFile} from './source-material.mjs';
import {prepareRepositoryCutSource,inspectRepositoryCutSource,saveReviewedRepositoryCut}
 from '../bridge/repository-cut-intake.mjs';
import {warehouseInstalledTools} from '../bridge/warehouse-portfolio.mjs';
import {repositoryCutPackDemand} from '../bridge/repository-pack-intake.mjs';
import {prepareRepositoryFPQReview,reviewRepositoryFPQGeneration}
 from '../bridge/repository-fpq-review.mjs';
import {readRepositoryCutCollection,repositoryCutContinuations} from '../bridge/repository-cuts.mjs';

const root=document.querySelector('[data-repository-cut-studio]');
const context=document.querySelector('meta[name="bonfyre-workspace-context-selector"]')?.content||'self';
const create=(tag,text='')=>{const e=document.createElement(tag);if(text)e.textContent=text;return e;};
const button=(title,action)=>{const e=create('button',title);e.type='button';e.addEventListener('click',action);return e;};
const roles=['sources','research','analysis','finance','connections','programs','campaigns','commerce','creator','logistics'];
function mount(){
 if(!root)return;
 const abort=new AbortController();
 let closed=false,busy=false,cuts=[],tools=null,selected=null,filesByDigest=new Map(),reviewDialog=null;
 const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
 const title=create('h2','JOSH / Git repository cuts');
 const summary=create('p','Review the immutable repository-cut receipts produced by Bonfÿre RepositoryFabric. JOSH virtual commits are not automatically the upstream Git commit.');
 const limitation=create('p','A verified receipt digest checks its coordinates, not remote repository access, current branch state, canonical JOSH mappings, PackGraph approval, or reuse rights.');
 const choose=create('input');choose.type='file';choose.multiple=true;choose.accept='.yaff,.txt,text/plain';choose.setAttribute('aria-label','Choose native JOSH repository-cut receipts');
 const inputLabel=create('label','Immutable source-cut receipts (.yaff, maximum 24)');inputLabel.append(choose);
 const status=create('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 const run=button('Verify repository cuts',()=>void inspect());
 const library=create('a','Keep original receipts in Source Library');library.href='../';
 const actions=create('div');actions.className='wp-actions';actions.append(run,library);
 const list=create('section');list.className='wp-list';list.setAttribute('aria-label','Verified repository cuts');
 const detail=create('section');detail.className='wp-detail';detail.setAttribute('aria-label','Selected repository cut');
 const split=create('div');split.className='wp-split';split.append(list,detail);
 root.append(title,summary,limitation,inputLabel,actions,status,split);
 const say=text=>{if(!closed)status.textContent=text;};
 function clear(){
  closed=true;abort.abort();cuts=[];tools=null;selected=null;filesByDigest.clear();choose.value='';reviewDialog?.remove();reviewDialog=null;
  root.replaceChildren(create('p','Account context changed. Repository cut receipts and reviewer choices were cleared.'));
 }
 addEventListener('bonfyre:account-changed',clear,{signal:abort.signal});
 addEventListener('pagehide',clear,{signal:abort.signal});
 async function readTools(){
  const packet=await readWorkspacePacket('/apps/_bindings?'+new URLSearchParams({context}),{signal:abort.signal});
  abort.signal.throwIfAborted();return warehouseInstalledTools(packet,context);
 }
 async function inspect(){
  if(closed||busy)return;
  const files=[...choose.files];
  if(!files.length||files.length>24||files.some(file=>file.size<10||file.size>2048)){
   say('Choose one to 24 native YAFF receipts, no larger than 2 KiB each.');return;
  }
  busy=true;run.disabled=true;cuts=[];selected=null;filesByDigest.clear();paint();say('Checking immutable repository cut digests…');
  try{
   const source=[];
   for(const file of files){source.push(await file.text());abort.signal.throwIfAborted();}
   const current=await readRepositoryCutCollection(source);
   let available=null;
   try{available=await readTools();}catch(error){
    if(abort.signal.aborted)return;
    say('Receipts verified; current installed tools could not be checked. Owner links remain unavailable.');
   }
   abort.signal.throwIfAborted();
   cuts=current;filesByDigest=new Map(current.map((cut,i)=>[cut.cutDigest,files[i]]));tools=available;selected=null;paint();
   if(available)say(cuts.length+' immutable source-cut receipts verified. No repository access or data transfer occurred.');
  }catch(error){if(!abort.signal.aborted)say('Verification refused: '+error.message);}
  finally{busy=false;if(!closed)run.disabled=false;}
 }
 async function reviewRetention(){
  if(closed||reviewDialog||!selected)return;
  const cut=selected,file=filesByDigest.get(cut.cutDigest);
  if(!file||!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint||'')){
    say('The original file or exact destination binding is unavailable. Reopen Source Library.');return;
  }
  let prepared,store,prior;
  try{
   prepared=await prepareRepositoryCutSource(file,cut,prepareFile);
   abort.signal.throwIfAborted();
   store=openDestinationStore(endpoint,{signal:abort.signal});
   prior=await inspectRepositoryCutSource(store,prepared);
   abort.signal.throwIfAborted();
  }catch(error){if(!abort.signal.aborted)say(error.message);return;}
  const dialog=create('dialog'),heading=create('h3','Review repository-cut source retention');
  const info=create('p','Keep the exact original YAFF bytes in your current Source Library. This is a private saved source, not a verified upstream Git mapping, a new repository checkout, an installed pack, or permission to publish.');
  const summary=create('p',cut.org+'/'+cut.repository+' · '+cut.filter+' · '+cut.cutDigest);
  const state=create('p');state.setAttribute('role','status');
  const action=button('Save reviewed source cut',()=>void confirm());
  const close=button('Close',()=>{if(!working)dialog.close();});
  const link=create('a','Open saved original in Source Library');
  link.href='../?'+new URLSearchParams({record:prepared.change.key});
  link.hidden=true;
  let working=false,attempted=false;
  dialog.append(heading,info,summary,state,action,link,close);
  document.body.append(dialog);reviewDialog=dialog;
  dialog.addEventListener('close',()=>{if(reviewDialog===dialog)reviewDialog=null;dialog.remove();});
  dialog.addEventListener('cancel',event=>{if(working)event.preventDefault();});
  if(prior.state==='retained'){
    action.disabled=true;state.textContent='This exact original is already retained at revision '+prior.revision+'.';
    link.hidden=false;
  }else if(prior.state==='conflict'){
    action.disabled=true;state.textContent='The saved record identity conflicts. Reopen Source Library before changing anything.';
  }else state.textContent='No source has been written. Review the source-cut identity before confirming.';
  dialog.showModal();
  async function confirm(){
   if(closed||working||attempted||prior.state!=='absent')return;
   attempted=working=true;action.disabled=close.disabled=true;
   state.textContent='Saving the exact reviewed original…';
   try{
    const saved=await saveReviewedRepositoryCut(store,prepared);
    abort.signal.throwIfAborted();
    link.hidden=false;
    state.textContent=saved.outcome==='already_retained'?'Already retained at revision '+saved.revision+'.':
      'Source Library retained the exact original at revision '+saved.revision+'. No Git or publication action occurred.';
   }catch(error){
    if(!abort.signal.aborted){
     state.textContent=error.message;
     if(error.code==='source_save_uncertain'){
      const verify=button('Check saved source cut',async()=>{
       verify.disabled=true;
       try{
        const current=await inspectRepositoryCutSource(store,prepared);
        abort.signal.throwIfAborted();
        if(current.state==='retained'){
         link.hidden=false;state.textContent='Confirmed saved at revision '+current.revision+'. No second write sent.';
         verify.remove();
        }else state.textContent='Still unconfirmed. Reopen Source Library before another review; no retry was sent.';
       }catch(e){if(!abort.signal.aborted)state.textContent='Readback unavailable. Do not retry: '+e.message;}
       finally{verify.disabled=false;}
      });
      dialog.insertBefore(verify,close);
     }
    }
   }finally{working=false;if(!closed)close.disabled=false;}
  }
 }
 function paint(){
  list.replaceChildren(create('h3',cuts.length+' source cuts'));
  const ul=create('ul');
  for(const cut of cuts){
   const li=create('li');
   const b=button(cut.org+'/'+cut.repository+' '+cut.filter,()=>{selected=cut;paint();});
   b.className='wp-source';if(selected?.cutDigest===cut.cutDigest)b.setAttribute('aria-current','true');
   li.append(b);ul.append(li);
  }
  list.append(ul);detail.replaceChildren();
  if(!selected){
   detail.append(create('h3','Choose a repository cut'),
    create('p','Select the exact virtual tree to inspect source identity and owner continuations.'));return;
  }
  detail.append(create('h3',selected.org+'/'+selected.repository));
  const dl=create('dl');
  for(const [key,value] of [
   ['Git provider',selected.provider],['Requested ref',selected.requestedRef],
   ['JOSH filter',selected.filter],['Virtual commit',selected.virtualCommit],
   ['Virtual tree',selected.virtualTree],['Cut digest',selected.cutDigest],
   ['Canonical mapping',selected.canonicalMapping]
  ]){dl.append(create('dt',key),create('dd',value));}
  detail.append(dl,create('p','Digest self-consistent · no upstream commit mapping, remote content verification, installation, source reuse, or publication authority.'));
  const retain=button('Review retention in Source Library',()=>void reviewRetention());
  retain.disabled=!!reviewDialog;
  detail.append(retain);
  const packReview=create('section');packReview.className='repository-pack-review';
  const packLabel=create('label','Existing canonical PackGraph pack identifier');
  const packName=create('input');packName.type='text';packName.maxLength=95;
  packName.autocomplete='off';packName.placeholder='existing-pack';
  packName.setAttribute('aria-label','Existing canonical PackGraph pack identifier');
  packLabel.append(packName);
  const packPreview=create('section');packPreview.setAttribute('aria-label','PackGraph source demand draft');
  const previewButton=button('Prepare PackGraph demand draft',()=>{
    packPreview.replaceChildren();
    try{
      const demand=repositoryCutPackDemand(selected,{pack:packName.value.trim()});
      const notice=create('p','Draft only. Pack existence, source bytes, license, upstream JOSH mapping, and publication have NOT been verified. No CMS write was sent.');
      const dl=create('dl');
      for(const [k,v] of [
        ['Owner',demand.owner],['Content type',demand.content_type],
        ['Namespace',demand.namespace],['Pack',demand.attributes.pack],
        ['Demand kind',demand.attributes.demand_kind],['Needs',demand.attributes.needs],
        ['Preferred sources',demand.attributes.preferred_sources],
        ['Source coordinate',demand.attributes.fields],['Status',demand.attributes.status],
        ['Readiness',demand.readiness]
      ]){dl.append(create('dt',k),create('dd',String(v)));}
      packPreview.append(create('h4','Native PackGraph source proposal'),notice,dl);
    }catch(error){packPreview.append(create('p','Draft refused: '+error.message));}
  });
  packReview.append(create('h4','Git source → PackGraph'),packLabel,previewButton,packPreview);
  detail.append(packReview);
  const fpqReview=create('section');fpqReview.className='repository-fpq-review';
  const fpqTitle=create('h4','Local FPQ evidence review');
  const fpqIntro=create('p','Prepare a source-coordinate-only review for Bonfÿre’s QUIC/FPQ model. The browser does not execute models or send source contents. The native service must be available and independently admitted.');
  const fpqModeLabel=create('label','Review purpose'),fpqMode=create('select');
  fpqMode.setAttribute('aria-label','FPQ review purpose');
  for(const [value,label] of [['source-evidence','Source evidence'],['pack-demand','PackGraph source demand']]){
   const option=create('option',label);option.value=value;fpqMode.append(option);
  }
  fpqModeLabel.append(fpqMode);
  const fpqActions=create('div');fpqActions.className='wp-actions';
  const fpqPrompt=create('textarea');fpqPrompt.readOnly=true;fpqPrompt.rows=12;
  fpqPrompt.setAttribute('aria-label','Prepared native FPQ review prompt');fpqPrompt.hidden=true;
  const fpqInfo=create('p');fpqInfo.setAttribute('role','status');
  const fpqResult=create('section');fpqResult.setAttribute('aria-label','FPQ generation review');
  const fpqFile=create('input');fpqFile.type='file';fpqFile.accept='.json,application/json';
  fpqFile.setAttribute('aria-label','Native FPQ generation receipt');
  fpqFile.hidden=true;
  let preparedFPQ=null;
  const fpqPrepare=button('Prepare local FPQ review',()=>void (async()=>{
   preparedFPQ=null;fpqPrompt.hidden=true;fpqFile.hidden=true;fpqFile.value='';
   fpqResult.replaceChildren();
   const file=filesByDigest.get(selected.cutDigest);
   if(!file){fpqInfo.textContent='Choose the original immutable source cut again.';return;}
   try{
    const requested=fpqMode.value;
    preparedFPQ=await prepareRepositoryFPQReview(await file.text(),{
     intent:requested,pack:requested==='pack-demand'?packName.value.trim():''
    });
    abort.signal.throwIfAborted();
    fpqPrompt.value=preparedFPQ.prompt;fpqPrompt.hidden=false;
    fpqFile.hidden=false;
    fpqInfo.textContent='Prepared only · prompt SHA-256 '+preparedFPQ.prompt_sha256+
      ' · Native tool: '+preparedFPQ.nativeTool+
      ' · model execution and source access NOT performed.';
   }catch(error){if(!abort.signal.aborted)fpqInfo.textContent='FPQ preparation refused: '+error.message;}
  })());
  const fpqVerify=button('Verify FPQ generation receipt',()=>void (async()=>{
   fpqResult.replaceChildren();
   if(!preparedFPQ||!fpqFile.files?.length){fpqInfo.textContent='Prepare a review and choose its exact native FPQ generation receipt.';return;}
   const file=fpqFile.files[0];
   if(file.size<12||file.size>128*1024){
    fpqInfo.textContent='Use one bounded native FPQ receipt no larger than 128 KiB.';return;
   }
   try{
    const result=await reviewRepositoryFPQGeneration(JSON.parse(await file.text()),preparedFPQ);
    abort.signal.throwIfAborted();
    const label=create('h5','Generation receipt self-consistent · human review required');
    const summary=create('p','Pinned cut digest '+result.sourceCutDigest+
      ' · completion SHA-256 '+result.output_sha256+
      '. This does not authenticate a model runner or approve any source claim.');
    const answer=create('pre',result.output);answer.style.whiteSpace='pre-wrap';answer.style.overflowWrap='anywhere';
    fpqResult.append(label,summary,answer);
    fpqInfo.textContent='FPQ result reviewed as unverified model-generated candidate. No source, Git, pack, or publication record was changed.';
   }catch(error){if(!abort.signal.aborted)fpqInfo.textContent='FPQ receipt refused: '+error.message;}
  })());
  fpqMode.addEventListener('change',()=>{preparedFPQ=null;fpqPrompt.hidden=true;fpqFile.hidden=true;fpqResult.replaceChildren();fpqInfo.textContent='Review purpose changed. Prepare a new exact prompt.';},{signal:abort.signal});
  packName.addEventListener('input',()=>{preparedFPQ=null;fpqPrompt.hidden=true;fpqFile.hidden=true;fpqResult.replaceChildren();},{signal:abort.signal});
  fpqActions.append(fpqPrepare,fpqVerify);
  fpqReview.append(fpqTitle,fpqIntro,fpqModeLabel,fpqActions,fpqInfo,fpqPrompt,
   create('p','Use only the existing authorized bonfyre_quic_fpq_generate tool with the prepared prompt, max_tokens=128, temperature=0. Native service failure is NOT a generated answer.'),
   fpqFile,fpqResult);
  detail.append(fpqReview);
  if(!tools){
   detail.append(create('p','Owner bindings unavailable. Reopen after sign-in and destination access are verified.'));return;
  }
  const chooser=create('fieldset');chooser.append(create('legend','Continue review in'));
  const checkboxes=[];
  for(const role of roles){
   const label=create('label'),cb=create('input');cb.type='checkbox';cb.value=role;
   cb.checked=['sources','research','analysis'].includes(role);
   label.append(cb,document.createTextNode(' '+role));chooser.append(label);checkboxes.push(cb);
  }
  const outcomes=create('section');
  const proceed=button('Find review owners',()=>{
   outcomes.replaceChildren();
   const picked=checkboxes.filter(cb=>cb.checked).map(cb=>cb.value);
   if(!picked.length){outcomes.append(create('p','Choose at least one reviewed work domain.'));return;}
   const choices=repositoryCutContinuations(selected,tools,{context,roles:picked});
   outcomes.append(create('h4','Current installed product owners'));
   const ul=create('ul');
   for(const item of choices){
    const li=create('li');
    if(item.state==='ready'||item.kind==='account'){
     const link=create('a',item.label+' · '+(item.kind==='account'?'Review prerequisites':'Open editor'));
     link.href=item.href;li.append(link);
    }else li.append(create('span',item.label+' · '+item.state.replaceAll('_',' ')));
    li.append(create('small',' '+item.boundary));ul.append(li);
   }
   outcomes.append(ul);
  });
  detail.append(chooser,proceed,outcomes);
 }
 paint();
}
mount();
