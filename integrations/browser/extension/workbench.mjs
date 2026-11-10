import {installContentsWorkspace} from './contents-workspace.mjs';
import {contentReferences} from './contents.mjs';
import {recompileProgram} from './contents-operations.mjs';
import {mountWorkbenchShell} from './workbench-shell.mjs';
import {installInteractions} from './workbench-interactions.mjs';
import {reviewText} from './workbench-actions.mjs';
import {mountProgramEditor,selectedReferences,renderProgram,renderBudget,usdCents} from './program-editor.mjs';
import {capturePage} from './capture.mjs';
import {observe,cmsProjection,heldWorkEnvelope,compareObservations,compileReferencedDraft,operations,registerWebMCP} from './observation.mjs';
import {readLocal,saveLocal,saveLocalBatch,deleteLocal,deleteUnreferencedSource} from './store.mjs';
const catalog=await(await fetch('./catalog.json')).json(),workspace=mountWorkbenchShell(),ops=operations(catalog),$=id=>document.getElementById(id);
let observation=null,draft=null,previewDraft=null,savedBase=null,draftRevision=0,editor=null,cachedDrafts=[],saving=false;
const sources=new Map(),retainedSources=new Set(),selectedSources=new Set();
const nativeContext=document.querySelector('meta[name="bonfyre-workspace-context"]')?.content;
const storageLabel=nativeContext?'this native workspace':'this browser';
const show=(id,text)=>{$(id).textContent=text;};
const guarded=fn=>async(...args)=>{show('error','');try{await fn(...args)}catch(e){show('error',e.message||String(e))}};
function node(tag,value,klass=''){const n=document.createElement(tag);n.textContent=value;if(klass)n.className=klass;return n}
if(location.protocol!=='chrome-extension:'){
  for(const id of ['capture','open-collected','hold-work'])$(id).disabled=true;
  const back=document.querySelector('header a');back.href='https://aurekai-work-samples.pages.dev/';back.textContent='All work samples';
  const note=node('p','This web workspace saves captures and drafts in this browser. Import a saved capture or try the labeled sample. Capturing another tab and sending native work require the installed extension.','hint');
  if(!nativeContext){const link=document.createElement('a');link.href='https://aurekai.life/packs/setup?pack=experience.source-programs.workspace&context=self';link.textContent='Open an account-bound program workspace';note.append(document.createTextNode(' '),link);}
  note.id='web-workspace-notice';if(nativeContext){note.textContent='Sources and programs are saved in '+nativeContext+'. Current destination permissions apply to every read and save.';$('source-drop').textContent='Drop saved capture files here. Review them before retaining them in this workspace.';}document.querySelector('header').append(note);
  $('capture').title='Available in the installed browser extension';$('open-collected').title='Requires the native browser host';
}
const preview=node('div','','reviewed-preview');$('draft-summary').after(preview);
const selectedCount=node('p','','source-selection-count');selectedCount.id='selected-sources';$('observations').before(selectedCount);
const basketView=node('div','');basketView.id='basket';$('basket').replaceWith(basketView);
const style=document.createElement('link');style.rel='stylesheet';style.href='./program-editor.css';document.head.append(style);
function sourceCount(){selectedCount.textContent=selectedSources.size+' sources selected for this draft';for(const box of document.querySelectorAll('[data-source-id]'))box.checked=selectedSources.has(box.dataset.sourceId);}
function invalidateDraft(){
  draft=null;for(const id of ['save-draft','export-draft','export-portable','print-proposal'])$(id).disabled=true;
  show('draft-summary','Unsaved proposal. Review the current changes before saving.');show('review-state','This proposal changed. Review it again before saving.');
  previewDraft=null;try{if(editor)previewDraft=compileReferencedDraft(catalog,editor.read(),selectedReferences([...sources.values()],selectedSources));}catch{}
  renderProgram(preview,previewDraft);sourceCount();budgetPreview();document.dispatchEvent(new Event('workbench:edited'));
}
function budgetPreview(){try{const drafts=cachedDrafts.filter(r=>r.key!==previewDraft?.program_id).map(r=>recompileProgram(catalog,r.value,[...sources.values()]));if(previewDraft)drafts.push(previewDraft);renderBudget($('basket'),ops.preview_basket.run({drafts,budget_cents:usdCents($('budget').value)}));}catch(error){show('basket',error.message);}}
function download(name,data){const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)+'\n'],{type:'application/json'}));const a=node('a','');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}
async function accept(raw,{select=true,retained=null}={}){observation=retained??await observe(raw,catalog);const target=$('capture-result');target.replaceChildren();
  sources.set(observation.id,observation);if(select)selectedSources.add(observation.id);sourceCount();invalidateDraft();
  target.append(node('h3',observation.captured.title),node('small',observation.captured.url));
  if(observation.captured.source_receipt)target.append(node('p','Retained observation from '+observation.captured.source_receipt.observed_at+' · not a live-page read.'));
  if(observation.captured.fixture)target.append(node('p','FIXTURE — invented merchant and terms; not a live listing.','warning'));
  for(const p of observation.reference_profiles)target.append(node('span',p.name+' / '+p.pattern,'tag'));
  for(const o of observation.offers){const row=node('div','', 'row');row.append(node('strong',o.product_name),node('div',(o.price.raw??'No exact price')+' '+(o.price.currency??'currency unknown')),
    node('small','Availability: '+(o.availability??'not stated')+' · Shipping and taxes not quoted'));
    if(o.range)row.append(node('small','Aggregate range is not an exact order price.'));
    target.append(row);
  }
  for(const recipe of observation.recipes||[]){const box=node('section','', 'row');box.append(node('h3',recipe.title),node('small','Recipe from source · quantities and storage not validated'));for(const ingredient of recipe.ingredients)box.append(node('p',ingredient));for(const claim of recipe.nutrition)box.append(node('small',claim.field+': '+claim.raw_value+' · serving basis: '+claim.basis+' · source claim, not a plan total'));target.append(box);}
  for(const item of observation.media||[]){const box=node('section','', 'row');box.append(node('h3',item.title),node('small',item.kind+' · '+(item.creator||'Creator not stated')),node('p',item.description||''),node('small','Observed post metadata. No video was downloaded; no recipe or reuse right is inferred.'));if(item.transcript){const details=node('details','');details.append(node('summary','Source transcript (not recipe instructions)'),node('p',item.transcript));box.append(details);}target.append(box);}
  if(!observation.offers.length&&!observation.recipes?.length&&!observation.media?.length)target.append(node('p','No supported product offer found in the captured JSON-LD. The selected excerpts are still retained.'));
  for(const s of observation.captured.term_snippets)target.append(node('p',s));
  if(observation.captured.selected_text)target.append(node('blockquote',observation.captured.selected_text));
  $('save-observation').disabled=false;$('export-cms').disabled=false;$('hold-work').disabled=observation.captured.fixture||!globalThis.chrome?.runtime?.sendNativeMessage;
}
$('capture').onclick=guarded(async()=>{
  if(!globalThis.chrome?.scripting)throw Error('Install as an unpacked extension to capture another tab. The fixture and imported captures work in this local page.');
  const [tab]=await chrome.tabs.query({active:true,currentWindow:true});
  if(!tab?.id)throw Error('Select a website tab and click the Aurekai extension icon.');
  const [result]=await chrome.scripting.executeScript({target:{tabId:tab.id},func:capturePage});
  if(!result?.result)throw Error('Page capture returned no result');await accept(result.result);
});
$('open-collected').onclick=guarded(async()=>{
  if(!globalThis.chrome?.runtime?.sendNativeMessage)throw Error('The native host is required to read collected material.');
  const answer=await chrome.runtime.sendNativeMessage('com.bonfyre.browser_host',{
    id:1,operation:'collected',url:$('collected-url').value.trim(),collection:$('collection-id').value.trim()});
  if(!answer?.ok||answer.truncated)throw Error(answer?.reason||'The complete source projection was not returned');
  await accept(answer.result.capture);
});
$('fixture').onclick=guarded(()=>accept({url:'https://fixture.invalid/heat-box?variant=mild&token=private',title:'Fixture Pantry · Discovery box',captured_at:new Date().toISOString(),fixture:true,
  selected_text:'Example terms only.',term_snippets:['Fixture: billed monthly after review.','Fixture: shipping calculated at checkout; cancel before the next selection deadline.'],
  structured:[{'@context':'https://schema.org','@type':'Product',name:'Fixture Pantry Discovery',sku:'F-001',offers:{'@type':'Offer',price:'45.00',priceCurrency:'USD',availability:'https://schema.org/InStock'}}]}));
$('import').onchange=guarded(async event=>{const file=event.target.files[0];if(!file)return;if(file.size>512*1024)throw Error('Use a capture smaller than 512 KiB');
  const value=JSON.parse(await file.text());await accept(value.schema==='aurekai.browser-observation.v1'?value.captured:value);
});
$('save-observation').onclick=guarded(async()=>{if(!observation)return;const found=(await readLocal('observation:')).find(r=>r.key===observation.id);if(!found)await saveLocal(observation.id,observation,0);await refresh();show('comparison','Observation kept locally. No source was contacted and no order was placed.');});
$('export-cms').onclick=guarded(()=>{if(observation)download('aurekai-browser-observation.json',{schema:'aurekai.browser-native-handoff.v1',observation,
  native_projection:cmsProjection(observation),executed:false})});
$('hold-work').onclick=guarded(async()=>{
  if(!observation)throw Error('Capture or select an observation first');
  const envelope=heldWorkEnvelope(observation,$('work-purpose').value);
  // A discrete user-requested handoff to the existing host; no new native operation.
  const answer=await chrome.runtime.sendNativeMessage('com.bonfyre.browser_host',{id:1,...envelope});
  if(!answer?.ok)throw Error(answer?.reason||answer?.refused||'Native owner did not accept the request');
  show('work-result','Request held by '+(answer.result?.native_owner??'the native work owner')+'. Reference: '+(answer.result?.mission??'not returned')+'. No subscription, payment or supplier action was executed.');
});
$('template').replaceChildren(...catalog.templates.map(t=>{const o=node('option',t.name+' · '+catalog.patterns[t.pattern].label);o.value=t.id;return o}));
function choose(){if(saving)throw Error('Finish the current save before changing programs.');const t=catalog.templates.find(t=>t.id===$('template').value);show('template-description',t.promise);
  const input={program_id:'draft:'+t.id+':'+crypto.randomUUID().slice(0,8),template_id:t.id,owner_subject:'person:your-name',name:t.name,sample:true,
    terms:{currency:'USD',price_cents:4500,shipping_cents:500,max_members:20,quantity_per_member:1,start_date:'2026-11-01',
      cadence:{unit:t.pattern==='replenishment'?'usage':'month',every:1},auto_renew:t.pattern!=='finite_gift',
      cancellation:'Illustrative: replace with the terms you will actually offer.',return_policy:'Illustrative: replace with your return or service remedy policy.'},preferences:{}};
  if(t.pattern==='finite_gift')input.terms.gift_cycles=3;
  if(t.pattern==='try_before_buy')input.terms.style_fee_cents=2000;
  $('input').value=JSON.stringify(input,null,2);draftRevision=0;savedBase=null;editor?.load(input);invalidateDraft();
  if(editor)document.dispatchEvent(new Event('workbench:loaded'));
}
$('template').onchange=choose;choose();
editor=mountProgramEditor({input:$('input'),catalog,onEdit:invalidateDraft});editor.load(JSON.parse($('input').value));
$('compile').textContent='Review changes';
$('compile').onclick=guarded(async()=>{const input=editor.read();draft=compileReferencedDraft(catalog,input,selectedReferences([...sources.values()],selectedSources));
  previewDraft=draft;show('draft-summary','Current proposal reviewed.');
  show('draft-output',reviewText(draft,[...sources.values()]));renderProgram(preview,draft);for(const id of ['save-draft','export-draft','export-portable','print-proposal'])$(id).disabled=false;
  document.dispatchEvent(new Event('workbench:reviewed'));
});
$('save-draft').onclick=guarded(async()=>{
  if(!draft||saving)return;const reviewed=draft;saving=true;$('save-draft').disabled=true;
  try{
    const retained=await readLocal('observation:'),changes=[];
    for(const ref of reviewed.browser_evidence){const old=retained.find(r=>r.key===ref.id);const source=sources.get(ref.id);if(!source||source.captured_body_sha256!==ref.sha256)throw Error('A selected source changed. Review it again.');if(old){if(old.value.captured_body_sha256!==ref.sha256)throw Error('A retained source changed. Review it again.');changes.push({key:old.key,value:old.value,expectedRevision:old.revision,checkOnly:true});}else{if(retainedSources.has(ref.id))throw Error('A selected source was removed. Reimport it explicitly before saving.');changes.push({key:ref.id,value:source,expectedRevision:0});}}
    changes.push({key:reviewed.program_id,value:reviewed,expectedRevision:draftRevision});
    const saved=(await saveLocalBatch(changes)).find(r=>r.key===reviewed.program_id);draftRevision=saved.revision;savedBase=structuredClone(reviewed);await refresh();
    show('workspace-notice','Draft kept in '+storageLabel+'. It is not published or billed.');$('conflict-recovery').hidden=true;
    if(draft===reviewed)show('draft-summary','Draft kept in '+storageLabel+'.');
  }catch(error){$('conflict-recovery').hidden=false;throw error;}finally{saving=false;$('save-draft').disabled=!draft;}
});
$('export-portable').onclick=guarded(()=>{if(draft)download('recurring-program-draft.json',draft)});
$('budget').oninput=guarded(budgetPreview);
async function refresh(){
  const observations=(await readLocal('observation:')).sort((a,b)=>b.value.captured.captured_at.localeCompare(a.value.captured.captured_at));
  for(const row of observations)retainedSources.add(row.key);
  $('observations').replaceChildren();
  const query=$('source-search').value.trim().toLowerCase(),filter=$('source-filter').value;
  for(const r of observations){sources.set(r.value.id,r.value);if(query&&!(r.value.captured.title+' '+r.value.captured.url+' '+r.value.captured.selected_text).toLowerCase().includes(query))continue;if(filter==='selected'&&!selectedSources.has(r.key)||filter==='offers'&&!r.value.offers.length||filter==='media'&&!r.value.media?.length&&!r.value.recipes?.length)continue;const box=node('div','', 'row');box.append(node('strong',r.value.captured.title),node('small',r.value.captured.captured_at));
    const pick=document.createElement('input');pick.type='checkbox';pick.checked=selectedSources.has(r.value.id);pick.dataset.sourceId=r.value.id;
    const label=node('label','','source-pick');label.append(pick,document.createTextNode(' Include in this draft'));box.append(label);
    pick.onchange=()=>{pick.checked?selectedSources.add(r.value.id):selectedSources.delete(r.value.id);sourceCount();invalidateDraft();};
    const select=node('button','Open this capture'),remove=node('button','Remove');select.onclick=guarded(()=>accept(r.value.captured,{select:false,retained:r.value}));remove.onclick=guarded(async()=>{const drafts=await readLocal('draft:');if(drafts.some(d=>d.value.browser_evidence?.some(ref=>ref.id===r.key)))throw Error('This source is attached to a saved program. Remove that reference and save the program before removing the source.');if(!confirm('Remove this capture from this browser?'))return;await deleteUnreferencedSource(r.key,r.revision);selectedSources.delete(r.value.id);sources.delete(r.value.id);invalidateDraft();await refresh()});
    const a=node('div','', 'actions');a.append(select,remove);box.append(a);$('observations').append(box)}
  if(!observations.length)$('observations').append(node('p','No observations kept yet.'));
  sourceCount();

  const records=(await readLocal('draft:')).filter(r=>r.value?.schema==='aurekai.recurring-program.v1');cachedDrafts=records;$('drafts').replaceChildren();
  for(const r of records){const box=node('div','', 'row');box.append(node('strong',r.value.name),node('small',r.value.pattern+' · draft revision '+r.revision));
    const resume=node('button','Continue editing');resume.dataset.draftId=r.key;
    resume.onclick=guarded(()=>resumeDraft(r.key));
    const remove=node('button','Remove draft');remove.onclick=guarded(async()=>{if(saving)throw Error('Finish the current save before removing a draft.');if(!confirm('Remove this saved draft?'))return;await deleteLocal(r.key);if(editor.read().program_id===r.key)choose();await refresh()});box.append(resume,remove);$('drafts').append(box)}
  budgetPreview();document.dispatchEvent(new Event('workbench:refreshed'));
}
async function resumeDraft(id){
  if(document.body.dataset.programLoading==='true')throw Error('A saved program is already opening.');
  if(saving)throw Error('Finish the current save before opening another program.');
  document.body.dataset.programLoading='true';
  const panels=[...document.querySelectorAll('[data-panel]')];for(const panel of panels)panel.inert=true;
  for(const button of document.querySelectorAll('[data-view],#new-program'))button.disabled=true;
  try{
  const current=(await readLocal('draft:')).find(r=>r.key===id);if(!current)throw Error('Draft was removed.');
  const retained=await readLocal('observation:');for(const row of retained)sources.set(row.key,row.value);
  if((current.value.browser_evidence??[]).some(ref=>!retained.some(row=>row.key===ref.id)))throw Error('Reimport the missing source before editing this draft.');
  const missing=(current.value.browser_evidence??[]).filter(ref=>!sources.has(ref.id));if(missing.length)throw Error('Reimport the missing source before editing this draft.');
  $('template').value=current.value.template_id;editor.load(current.value);draftRevision=current.revision;savedBase=structuredClone(current.value);
  selectedSources.clear();for(const ref of current.value.browser_evidence??[])selectedSources.add(ref.id);
  invalidateDraft();await refresh();document.dispatchEvent(new Event('workbench:loaded'));workspace.open('schedule');
  }finally{document.body.dataset.programLoading='false';for(const panel of panels)panel.inert=false;for(const button of document.querySelectorAll('[data-view],#new-program'))button.disabled=false;}
  $('program-name').focus();
}
const toolLifetime=new AbortController();
addEventListener('pagehide',()=>toolLifetime.abort(),{once:true});
const webmcp=await registerWebMCP(catalog,globalThis.document?.modelContext,{signal:toolLifetime.signal});
$('capabilities').append(node('p','Captures and proposals are kept on this device.'),node('p',globalThis.chrome?.scripting?'The browser extension can capture your selected tab after your click.':'Capturing another tab and accessing the native work queue require the installed extension.'),node('p',webmcp.available?'Browser assistant tools are available for local calculations.':'Browser assistant tools are not available in this browser. The workspace still works.'));
await guarded(refresh)();
installInteractions({catalog,$,workspace,sources,selectedSources,editor,invalidate:invalidateDraft,refresh,getPreview:()=>previewDraft,getReviewed:()=>draft,getBase:()=>savedBase,choose,resume:resumeDraft});
installContentsWorkspace({catalog,workspace,read:()=>editor.read(),apply:input=>{if(saving)throw Error('Finish the current save before applying contents.');for(const id of contentReferences(input.terms.contents))selectedSources.add(id);editor.load(input);invalidateDraft();},observations:()=>[...sources.values()],records:()=>cachedDrafts,refresh,resume:resumeDraft,current:()=>previewDraft});
invalidateDraft();
if(['contents','upcoming','updates'].includes(location.hash.slice(1)))workspace.open(location.hash.slice(1));
const openDraft=new URL(location.href).searchParams.get('draft');if(openDraft)await guarded(()=>resumeDraft(openDraft))();
const selectedId=new URL(location.href).searchParams.get('capture');
if(selectedId){const saved=(await readLocal('observation:')).find(r=>r.key===selectedId);if(saved)await guarded(()=>accept(saved.value.captured))();}
