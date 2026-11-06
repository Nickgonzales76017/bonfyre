import {saveLocalBatch,readLocal} from './store.mjs';
import {compileDraft} from './recurring-commerce.mjs';
import {stableJSON} from './observation.mjs';
import {moveCycle,cycleMoveImpact,prepareCaptureBatch,batchWrites,comparisonRows,samePageChanges,adoptObservedPrice,reviseCycles,editHistory,reviewText,changeFields} from './workbench-actions.mjs';
const node=(tag,text,klass='')=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(klass)n.className=klass;return n;};
export function renderComparison(target,observations,onPrice){
  target.replaceChildren();
  if(!observations.length){target.append(node('p','Select sources from your library to compare their prices, terms and excerpts.','empty-state'));return;}
  if(observations.length>8){target.append(node('p','Select up to eight sources for a readable side-by-side comparison. All imported sources remain in your library.'));return;}
  const difference=samePageChanges(observations);
  if(difference)target.append(node('p',difference.changes.length?'These versions differ in '+difference.changes.map(c=>({term_snippets:'terms',offers:'product offers',recipes:'recipes',media:'media'}[c.field])).join(', ')+'.':'These two versions contain the same extracted offers and terms.','comparison-summary'));
  const grid=node('div',undefined,'comparison-grid');
  for(const row of comparisonRows(observations)){
    const card=node('article',undefined,'comparison-card');card.append(node('h3',row.title),node('p','Recorded '+new Date(row.date).toLocaleString()));
    const link=node('a',row.url);link.href=row.url;link.target='_blank';link.rel='noopener noreferrer';card.append(link);
    if(row.fixture)card.append(node('p','Sample source','tag'));
    for(const offer of row.offers){const section=node('section');section.append(node('h4',offer.name),node('strong',offer.price,'comparison-price'),node('p','Recorded availability: '+offer.availability),node('p','Shipping and tax: not quoted','hint'));
      const use=node('button','Use this price in my proposal');use.disabled=!offer.usable;use.dataset.usePrice=row.id;use.dataset.offerIndex=String(offer.index);use.onclick=()=>onPrice(row.id,offer.index);section.append(use);card.append(section);}
    if(!row.offers.length)card.append(node('p','No supported product price in this capture.'));
    card.append(node('h4','Source terms'));for(const line of row.terms)card.append(node('p',line));if(!row.terms.length)card.append(node('p','No term excerpt retained.'));
    if(row.excerpt)card.append(node('h4','Selected passage'),node('blockquote',row.excerpt));
    for(const recipe of row.recipes)card.append(node('h4',recipe.title),node('p',recipe.ingredients.join('; ')));
    for(const media of row.media)card.append(node('h4',media.title),node('p',media.description));
    grid.append(card);
  }
  target.append(grid);
}
export function renderChanges(target,before,after){
  target.replaceChildren();const rows=changeFields(before,after);
  if(!rows.length){target.append(node('p','No changes from the saved version.'));return;}
  const table=node('table'),head=node('tr');for(const text of ['Change','Saved version','This proposal'])head.append(node('th',text));const thead=node('thead');thead.append(head);table.append(thead);
  const body=node('tbody');for(const row of rows){const line=node('tr');line.append(node('th',row.label),node('td',row.before),node('td',row.after));body.append(line);}table.append(body);target.append(table);
}
export function textDownload(name,text){const url=URL.createObjectURL(new Blob([text],{type:'text/plain;charset=utf-8'})),a=node('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
export function installInteractions({catalog,$,workspace,sources,selectedSources,editor,invalidate,refresh,getPreview,getReviewed,getBase,choose,resume}){
  let importing=[],edits,restoring=false;
  const snapshot=()=>({input:editor.read(),selected:[...selectedSources]});
  const updateHistory=()=>{$('undo').disabled=!edits?.canUndo;$('redo').disabled=!edits?.canRedo;};
  const reset=()=>{edits=editHistory(snapshot());updateHistory();};reset();
  document.addEventListener('workbench:loaded',reset);
  document.addEventListener('workbench:edited',()=>{if(restoring)return;try{edits.record(snapshot());updateHistory();}catch{}});
  const action=fn=>async(...args)=>{try{$('error').textContent='';await fn(...args);}catch(e){$('error').textContent=e.message;}};
  const restore=async value=>{restoring=true;try{editor.load(value.input);selectedSources.clear();for(const id of value.selected)selectedSources.add(id);invalidate();await refresh();updateHistory();}finally{restoring=false;}};
  $('undo').onclick=action(()=>restore(edits.undo()));$('redo').onclick=action(()=>restore(edits.redo()));
  const alter=input=>{editor.load(input);invalidate();};
  connectCycleRescheduling({catalog,target:document.querySelector('.reviewed-preview'),read:()=>editor.read(),apply:alter,observations:()=>[...sources.values()],onError:error=>{$('error').textContent=error.message;}});
  const cycles=(dates,skip)=>{alter(reviseCycles(catalog,editor.read(),dates,skip));workspace.open('schedule');};
  document.addEventListener('click',event=>{const button=event.target.closest('[data-cycle-date]');if(button)action(()=>cycles([button.dataset.cycleDate],button.dataset.cycleSkip==='true'))();});
  const dates=()=>[...document.querySelectorAll('[data-cycle-select]:checked')].map(e=>e.dataset.cycleSelect);
  $('skip-selected').onclick=action(()=>cycles(dates(),true));$('restore-selected').onclick=action(()=>cycles(dates(),false));
  const compare=()=>renderComparison($('diff-output'),[...selectedSources].map(id=>sources.get(id)).filter(Boolean),action((id,index)=>{
    const source=sources.get(id),offer=source.offers[index];
    if(!confirm('Use '+offer.price.raw+' '+offer.price.currency+' as your proposed cycle price? Delivery cost, quantities and permissions stay unchanged.'))return;
    selectedSources.add(id);alter(adoptObservedPrice(editor.read(),source,index));workspace.open('schedule');$('workspace-notice').textContent='Observed price applied to your proposal. It is not a checkout quote.';
  }));
  document.addEventListener('workbench:edited',compare);document.addEventListener('workbench:refreshed',compare);compare();
  for(const id of ['source-search','source-filter'])$(id).addEventListener('input',()=>refresh());
  $('select-visible').onclick=action(()=>{for(const box of document.querySelectorAll('[data-source-id]'))selectedSources.add(box.dataset.sourceId);invalidate();return refresh();});
  $('clear-selection').onclick=action(()=>{selectedSources.clear();invalidate();return refresh();});
  async function previewFiles(files){
    if(!files.length)return;if(files.length>40||files.reduce((n,f)=>n+f.size,0)>8*1024*1024)throw Error('Choose up to 40 captures totaling at most 8 MB.');
    const inputs=[];for(const file of files){try{if(file.size>256*1024)throw Error('Capture exceeds 256 KiB.');inputs.push({name:file.name,value:JSON.parse(await file.text())});}catch(error){inputs.push({name:file.name,error:error.message});}}
    importing=await prepareCaptureBatch(inputs,catalog,await readLocal('observation:'));$('import-rows').replaceChildren();$('import-error').textContent='';
    const counts={new_source:0,new_version:0,unchanged:0,invalid:0};for(const row of importing)counts[row.state]++;
    $('import-summary').textContent=counts.new_source+' new pages; '+counts.new_version+' changed versions; '+counts.unchanged+' already retained; '+counts.invalid+' invalid files.';
    for(const row of importing){const label=node('label',undefined,'import-row'),check=node('input');check.type='checkbox';check.dataset.importRow=String(row.index);check.disabled=!['new_source','new_version'].includes(row.state);check.checked=!check.disabled;
      const words=node('div');words.append(node('strong',row.observation?.captured.title||row.name),node('p',row.explanation));if(row.observation)words.append(node('small',row.observation.captured.url));label.append(check,words);$('import-rows').append(label);}
    $('import-commit').disabled=counts.new_source+counts.new_version===0;$('import-dialog').showModal();
  }
  $('import').onchange=action(async event=>{const files=[...event.target.files];event.target.value='';await previewFiles(files);});
  const drop=$('source-drop');drop.onclick=()=>$('import').click();drop.onkeydown=event=>{if(['Enter',' '].includes(event.key)){event.preventDefault();$('import').click();}};
  drop.ondragover=event=>{event.preventDefault();drop.classList.add('dragging');};drop.ondragleave=()=>drop.classList.remove('dragging');drop.ondrop=event=>{event.preventDefault();drop.classList.remove('dragging');action(()=>previewFiles([...event.dataTransfer.files]))();};
  $('import-commit').onclick=async()=>{
    $('import-error').textContent='';$('import-commit').disabled=true;
    try{const changes=batchWrites(importing,[...document.querySelectorAll('[data-import-row]:checked')].map(e=>Number(e.dataset.importRow)));await saveLocalBatch(changes);
      for(const row of changes){sources.set(row.key,row.value);selectedSources.add(row.key);}invalidate();await refresh();$('import-dialog').close();$('workspace-notice').textContent=changes.length+' captures retained together. No source was contacted.';
    }catch(error){$('import-error').textContent=error.message+' Reopen these files to review the current library.';}finally{$('import-commit').disabled=false;}
  };
  $('new-program').onclick=action(()=>{if(getPreview()&&!confirm('Start another proposal? Save or export your current changes first.'))return;choose();document.dispatchEvent(new Event('workbench:loaded'));workspace.open('schedule');});
  $('export-draft').onclick=action(()=>{const draft=getReviewed();if(!draft)throw Error('Review the current proposal first.');textDownload('program-proposal.txt',reviewText(draft,[...sources.values()]));});
  $('export-conflict').onclick=action(()=>{const draft=getPreview();if(!draft)throw Error('Correct the incomplete fields before exporting.');textDownload('unsaved-program-proposal.txt',reviewText(draft,[...sources.values()]));});
  $('reload-draft').onclick=action(async()=>{if(!confirm('Replace these unsaved edits with the current saved version?'))return;await resume(editor.read().program_id);$('conflict-recovery').hidden=true;});
  $('print-proposal').onclick=action(()=>{if(!getReviewed())throw Error('Review the current proposal first.');workspace.open('review');window.print();});
  document.addEventListener('workbench:reviewed',()=>{renderChanges($('change-review'),getBase(),getReviewed());$('review-state').textContent='Current proposal reviewed. Save this version to keep it.';workspace.open('review');});
}

// Shared direct manipulation for the workspace and full studio.
export function connectCycleRescheduling({catalog,target,read,apply,observations=()=>[],onError=()=>{}}){
  const style=node('link');style.rel='stylesheet';style.href=new URL('./cycle-reschedule.css',import.meta.url).href;document.head.append(style);
  const dialog=node('dialog',undefined,'cycle-move-dialog');dialog.id='cycle-move-dialog';dialog.setAttribute('aria-labelledby','cycle-move-title');
  const title=node('h2','Reschedule one cycle');title.id='cycle-move-title';
  const context=node('p'),label=node('label','Proposed date'),date=node('input');date.type='date';date.id='cycle-move-date';label.append(date);
  const controls=node('div',undefined,'actions'),week=node('button','One week later'),original=node('button','Original date');controls.append(week,original);
  const impact=node('div');impact.id='cycle-move-impact';impact.setAttribute('aria-live','polite');
  const error=node('p');error.id='cycle-move-error';error.setAttribute('role','alert');
  const actions=node('div',undefined,'actions'),confirm=node('button','Move cycle in proposal','primary'),cancel=node('button','Cancel');confirm.id='cycle-move-confirm';cancel.id='cycle-move-cancel';actions.append(confirm,cancel);
  dialog.append(title,context,label,controls,impact,error,actions);document.body.append(dialog);
  let base,from,anchor,candidate,trigger;
  const money=n=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n/100);
  const shift=(day,n)=>new Date(Date.parse(day+'T12:00:00Z')+n*86400000).toISOString().slice(0,10);
  const preview=()=>{
    candidate=null;confirm.disabled=true;error.textContent='';impact.replaceChildren();
    try{
      const next=moveCycle(catalog,base,from,date.value),change=cycleMoveImpact(catalog,base,next,observations());
      impact.append(node('p',from+' → '+date.value),node('p','Only this cycle moves. Other cycle dates, source references and quantities stay unchanged.'));
      if(change.prepaid)impact.append(node('p','The prepaid term stays on its original date. This move creates no refund or second charge.'));
      else if(change.variable)impact.append(node('p','Final product prices are unresolved; no fixed-price total is inferred.'));
      else if(!change.months.length)impact.append(node('p','The modeled monthly costs stay the same.'));
      else{const table=node('table'),head=node('tr');for(const text of ['Month','Before','After'])head.append(node('th',text));const thead=node('thead');thead.append(head);table.append(thead);const body=node('tbody');for(const row of change.months){const tr=node('tr');tr.append(node('th',row.month),node('td',money(row.before)),node('td',money(row.after)));body.append(tr);}table.append(body);impact.append(table);}
      if(compileDraft(catalog,next).cycle_preview.find(c=>c.due_local_date===date.value)?.state==='skip_requested')impact.append(node('p','This cycle remains skipped until you restore it.'));
      candidate=next;confirm.disabled=date.value===from;
    }catch(e){error.textContent=e.message;}
  };
  target.addEventListener('click',event=>{const button=event.target.closest('[data-reschedule-date]');if(!button)return;
    try{base=structuredClone(read());from=button.dataset.rescheduleDate;const rows=compileDraft(catalog,base).cycle_preview,index=rows.findIndex(c=>c.due_local_date===from);if(index<0)throw Error('This cycle changed. Reopen it.');anchor=rows[index].original_local_date??from;trigger=button;title.textContent='Reschedule cycle '+rows[index].cycle;context.textContent='Current date: '+from+'. This edits your local proposal, not a merchant order.';date.min=index?shift(rows[index-1].due_local_date,1):shift(anchor,-366);date.max=index<rows.length-1?shift(rows[index+1].due_local_date,-1):shift(anchor,366);date.value=from;original.disabled=from===anchor;preview();dialog.showModal();date.focus();}catch(e){onError(e);}
  });
  date.addEventListener('input',preview);week.onclick=()=>{date.value=shift(from,7);preview();};original.onclick=()=>{date.value=anchor;preview();};cancel.onclick=()=>dialog.close();
  confirm.onclick=()=>{try{if(!candidate||confirm.disabled)return;if(stableJSON(read())!==stableJSON(base))throw Error('The proposal changed while this date was open. Cancel and reopen the cycle.');apply(candidate);dialog.close();}catch(e){error.textContent=e.message;confirm.disabled=true;}};
  dialog.addEventListener('close',()=>{if(trigger?.isConnected)trigger.focus();else target.querySelector('[data-reschedule-date]')?.focus();});
  return dialog;
}
