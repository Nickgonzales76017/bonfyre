import {replaceContents,projectContents} from './contents.mjs';
import {compileDraft,basketPreview} from './recurring-commerce.mjs';
import {stableJSON} from './observation.mjs';
import {sourceUpdatePlan,sourceUpdateWrites,upcomingPrograms} from './contents-operations.mjs';
import {readLocal,saveLocalBatch} from './store.mjs';
const node=(tag,text,klass='')=>{const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(klass)el.className=klass;return el;};
const money=n=>n===null||n===undefined?'Unresolved':new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n/100);
const $=id=>document.getElementById(id);
function button(text,fn,klass=''){const el=node('button',text,klass);el.type='button';el.onclick=fn;return el;}
function table(headers,rows){const el=node('table'),head=node('thead'),tr=node('tr'),body=node('tbody');for(const h of headers)tr.append(node('th',h));head.append(tr);for(const cells of rows){const row=node('tr');for(const [i,text]of cells.entries())row.append(node(i?'td':'th',text));body.append(row);}el.append(head,body);return el;}
export function installContentsWorkspace({catalog,workspace,read,apply,observations,records,refresh,resume,current}){
  const style=node('link');style.rel='stylesheet';style.href=new URL('./contents-workspace.css',import.meta.url).href;document.head.append(style);
  $('contents-workspace').innerHTML='<div class="contents-scope" id="contents-scope-note"></div><div class="contents-toolbar"><label>Edit contents for<select id="contents-scope"></select></label><label>Price agreement<select id="contents-pricing"><option value="itemized">Selected product prices</option><option value="fixed">Fixed bundle price</option></select></label><label>Find a product<input type="search" id="contents-search" placeholder="Name or SKU"></label><label>Retained source<select id="contents-source"></select></label></div><div class="contents-layout"><div><p id="contents-catalogue-note"></p><div id="contents-products" class="contents-products"></div><section class="contents-impact"><h2>Effect on the program</h2><p>Customized cycles keep their selections when you change the recurring defaults.</p><div id="contents-impact" class="contents-impact-wrap"></div></section></div><aside class="contents-selection"><h2 id="contents-selection-title">Your selection</h2><progress id="contents-progress" value="0" max="12"></progress><div id="contents-status" class="contents-status" aria-live="polite"></div><div id="contents-lines"></div><details class="contents-rule-panel"><summary>Bundle quantity rules</summary><div class="contents-rules"><label>Minimum units<input id="contents-minimum" type="number" min="0" max="1000"></label><label>Maximum units<input id="contents-maximum" type="number" min="1" max="1000"></label><label>Per product<input id="contents-per-product" type="number" min="1" max="1000"></label></div></details><p id="contents-error" class="contents-warning" role="alert"></p><div class="contents-controls"><button id="contents-use" class="primary">Use these contents</button><button id="contents-cancel">Cancel changes</button><button id="contents-default">Use recurring defaults</button><button id="contents-reset-all">Reset customized cycles</button></div><p>Recorded prices are estimates. Shipping uses your allowance; tax and stock are not confirmed.</p></aside></div>';
  $('upcoming-workspace').innerHTML='<div class="contents-toolbar"><label>Month<select id="upcoming-month"></select></label><label><input type="checkbox" id="upcoming-skipped" checked> Include skipped cycles</label><button id="upcoming-refresh">Refresh saved programs</button></div><p id="upcoming-note"></p><div id="upcoming-orders"></div>';
  $('updates-workspace').innerHTML='<p>Compare a later capture with the version used by saved programs. Choose which programs should adopt its recorded prices and products.</p><div class="contents-toolbar"><label>Replace this source version<select id="update-before"></select></label><label>With this later capture<select id="update-after"></select></label><button id="update-preview">Review affected programs</button><button id="update-refresh">Refresh sources</button></div><div id="update-programs"></div><div class="source-update-actions"><button id="update-apply" class="primary" disabled>Update selected saved programs</button><p id="update-result" class="source-update-result" role="status"></p></div>';
  let base=null,working=null,scope='defaults',opening=false,swapping=null,plan=null,updateBusy=false;
  const fail=error=>{$('contents-error').textContent=error.message;};
  const safe=fn=>(...args)=>{try{return fn(...args);}catch(error){fail(error);}};
  const items=()=>working.terms.contents.cycles.find(c=>c.on===scope)?.items??working.terms.contents.defaults;
  function start(requested='defaults'){
    base=structuredClone(read());working=structuredClone(base);swapping=null;$('compile').disabled=true;$('save-draft').disabled=true;
    const draft=compileDraft(catalog,base,observations());
    if(!draft.cycle_preview.length)throw Error('Choose a dated program before selecting cycle contents.');
    working.terms.contents??={pricing:['finite_gift','try_before_buy'].includes(draft.pattern)?'fixed':'itemized',minimum:1,maximum:12,per_product:6,defaults:[],cycles:[]};
    const rows=draft.cycle_preview;scope=requested==='defaults'?'defaults':rows.find(c=>c.due_local_date===requested||c.original_local_date===requested)?.original_local_date??rows.find(c=>c.due_local_date===requested)?.due_local_date;
    if(!scope)throw Error('This cycle changed. Reopen the current program.');
    $('contents-scope').replaceChildren();const def=node('option','Recurring default contents');def.value='defaults';$('contents-scope').append(def);
    for(const cycle of rows){const option=node('option','Cycle '+cycle.cycle+' \u00B7 '+cycle.due_local_date);option.value=cycle.original_local_date??cycle.due_local_date;$('contents-scope').append(option);}
    $('contents-scope').value=scope;$('contents-pricing').value=working.terms.contents.pricing;$('contents-pricing').disabled=['finite_gift','try_before_buy'].includes(draft.pattern);
    for(const [id,key]of [['minimum','minimum'],['maximum','maximum'],['per-product','per_product']])$('contents-'+id).value=working.terms.contents[key];
    $('contents-search').value='';$('contents-source').replaceChildren();const all=node('option','All retained product sources');all.value='';$('contents-source').append(all);
    for(const source of observations().filter(o=>o.offers.length)){const option=node('option',source.captured.title+' \u00B7 '+source.captured.captured_at.slice(0,10));option.value=source.id;$('contents-source').append(option);}
    opening=true;try{workspace.open('contents');}finally{opening=false;}render();
  }
  function revise(next){working=replaceContents(working,scope,next);render();}
  function chooseProduct(source,index){
    const selected=structuredClone(items());const existing=selected.find(x=>x.source===source.id&&x.offer===index);
    if(swapping){const target=selected.find(x=>x.id===swapping);if(!target)throw Error('Select the product to replace again.');if(existing&&existing.id!==target.id)throw Error('That product is already selected. Change its quantity instead.');target.source=source.id;target.offer=index;swapping=null;}
    else if(existing)existing.quantity++;
    else selected.push({id:'line:'+crypto.randomUUID(),source:source.id,offer:index,quantity:1});
    revise(selected);
  }
  function renderCatalogue(){
    const query=$('contents-search').value.trim().toLowerCase(),sourceId=$('contents-source').value,el=$('contents-products');el.replaceChildren();let count=0;
    for(const source of observations()){
      if(sourceId&&sourceId!==source.id)continue;
      for(const [index,offer]of source.offers.entries()){
        if(query&&!(offer.product_name+' '+(offer.sku??'')).toLowerCase().includes(query))continue;
        if(++count>150)continue;
        const card=node('article',undefined,'contents-product');card.append(node('small',source.captured.fixture?'LABELED SAMPLE SOURCE':'RETAINED PRODUCT'),node('h3',offer.product_name),node('p',offer.range?'Price range':money(offer.price.usd_cents)),node('small',(offer.sku?'SKU '+offer.sku+' \u00B7 ':'')+source.captured.captured_at.slice(0,10)));
        const link=node('a',new URL(source.captured.url).hostname);link.href=source.captured.url;link.target='_blank';link.rel='noopener noreferrer';card.append(link);
        const unavailable=/(?:OutOfStock|SoldOut|Discontinued)$/i.test(offer.availability??'');card.append(node('p',unavailable?'Recorded as unavailable':offer.availability?'Recorded availability: '+offer.availability.split('/').at(-1):'Availability was not stated','hint'));
        const add=button(swapping?'Use as replacement':'Add to selection',safe(()=>chooseProduct(source,index)));add.dataset.contentOffer=source.id+':'+index;add.disabled=unavailable;card.append(add);el.append(card);
      }
    }
    $('contents-catalogue-note').textContent=swapping?'Choose the replacement product. The line keeps its quantity and cycle scope.':count+' matching recorded offers'+(count>150?'. Showing 150; refine your search.':'.');
    if(!count)el.append(node('p','Import or retain product captures in Sources, then return here. No products are invented for your program.','contents-empty'));
  }
  function render(){
    $('contents-error').textContent='';$('contents-use').disabled=true;const lines=$('contents-lines');lines.replaceChildren();
    const fixed=working.terms.contents.pricing==='fixed';
    $('contents-scope-note').textContent=scope==='defaults'?'Recurring defaults for '+working.name+'. This changes cycles still using defaults; existing cycle-only choices stay intact.':'One-cycle selection for '+scope+'. Other cycles keep their products. Adding an extra product here does not create a recurring subscription.';
    $('contents-selection-title').textContent=scope==='defaults'?'Recurring contents':'This cycle\u2019s contents';$('contents-default').hidden=scope==='defaults';$('contents-reset-all').hidden=!working.terms.contents.cycles.length;
    for(const item of items()){
      const source=observations().find(o=>o.id===item.source),offer=source?.offers?.[item.offer],row=node('article',undefined,'contents-line');row.dataset.contentLine=item.id;row.append(node('strong',offer?.product_name??'Unavailable source'),node('small',money(offer?.price?.usd_cents)+' each'+(fixed?' \u00B7 included in fixed price':'')));
      const controls=node('div',undefined,'contents-line-actions'),qty=node('input');qty.type='number';qty.min=1;qty.max=1000;qty.value=item.quantity;qty.setAttribute('aria-label','Quantity for '+(offer?.product_name??'product'));qty.onchange=safe(()=>{const next=structuredClone(items());next.find(x=>x.id===item.id).quantity=Number(qty.value);revise(next);});
      const swap=button('Replace',()=>{swapping=item.id;renderCatalogue();$('contents-search').focus();}),remove=button('Remove',safe(()=>revise(items().filter(x=>x.id!==item.id))));swap.dataset.replaceLine=item.id;remove.dataset.removeLine=item.id;controls.append(qty,swap,remove);row.append(controls);lines.append(row);
    }
    try{
      const preview=compileDraft(catalog,working,observations()),anchor=scope==='defaults'?preview.cycle_preview[0].original_local_date??preview.cycle_preview[0].due_local_date:scope;
      const chosen=projectContents(scope==='defaults'?{...working.terms.contents,cycles:[]}:working.terms.contents,anchor,observations(),working.terms);
      $('contents-progress').max=working.terms.contents.maximum;$('contents-progress').value=chosen.units;
      $('contents-status').replaceChildren(node('strong',money(chosen.modeled_cents)),node('p',chosen.units+' of '+chosen.maximum+' allowed units \u00B7 minimum '+chosen.minimum),node('p',fixed?'Fixed bundle price; selected product prices do not replace it.':'Selected product subtotal plus your delivery allowance, per member. Capacity is not a multiplier.'));
      const problems=[...new Set([...chosen.problems,...preview.cycle_preview.flatMap(c=>c.state==='skip_requested'?[]:c.contents.problems)])];
      $('contents-error').textContent=problems.join(' ');$('contents-use').disabled=problems.length>0;
      $('contents-impact').replaceChildren(table(['Cycle','Contents','Modeled amount'],preview.cycle_preview.map(c=>[c.due_local_date+(c.state==='skip_requested'?' \u00B7 skipped':''),c.contents.units+' units \u00B7 '+c.contents.selection,c.state==='skip_requested'?'Skipped':preview.pattern==='finite_gift'?'Included in prepaid term':money(c.contents.modeled_cents)])));
    }catch(error){fail(error);$('contents-status').textContent='Correct the selection to preview its cost.';}
    renderCatalogue();
  }
  $('contents-scope').onchange=safe(()=>{scope=$('contents-scope').value;swapping=null;render();});
  $('contents-pricing').onchange=safe(()=>{working.terms.contents.pricing=$('contents-pricing').value;render();});
  for(const [id,key]of [['minimum','minimum'],['maximum','maximum'],['per-product','per_product']])$('contents-'+id).onchange=safe(()=>{working.terms.contents[key]=Number($('contents-'+id).value);render();});
  $('contents-search').oninput=renderCatalogue;$('contents-source').onchange=renderCatalogue;
  $('contents-default').onclick=safe(()=>{working=replaceContents(working,scope,null);render();});
  $('contents-reset-all').onclick=safe(()=>{if(!confirm('Return every customized cycle to the recurring default contents?'))return;working.terms.contents.cycles=[];render();});
  $('contents-cancel').onclick=()=>{base=working=null;$('compile').disabled=false;workspace.open('schedule');};
  $('contents-use').onclick=safe(()=>{if($('contents-use').disabled)return;if(stableJSON(read())!==stableJSON(base))throw Error('The open program changed. Cancel these contents and reopen the current version.');apply(working);base=working=null;$('compile').disabled=false;workspace.open('schedule');$('workspace-notice').textContent='Contents applied to the open proposal. Review and save to keep this version.';});
  document.addEventListener('click',event=>{const target=event.target.closest('[data-contents-date]');if(target)safe(()=>start(target.dataset.contentsDate))();});
  function renderUpcoming(){
    const data=upcomingPrograms(catalog,records(),observations(),current()),months=[...new Set(data.rows.map(r=>r.cycle.due_local_date.slice(0,7)))],selected=$('upcoming-month').value;
    $('upcoming-month').replaceChildren(...months.map(m=>{const option=node('option',m);option.value=m;return option;}));if(months.includes(selected))$('upcoming-month').value=selected;
    const rows=data.rows.filter(r=>r.cycle.due_local_date.startsWith($('upcoming-month').value)&&($('upcoming-skipped').checked||r.cycle.state!=='skip_requested')),target=$('upcoming-orders');target.replaceChildren();
    $('upcoming-note').textContent=rows.length+' proposed cycles. Programs sharing a date remain separate. The current open version replaces its saved copy in this view.';
    for(const issue of data.errors)target.append(node('p',issue.name+': '+issue.error,'contents-warning'));
    let day='',group=null;
    for(const row of rows){if(day!==row.cycle.due_local_date){day=row.cycle.due_local_date;target.append(node('h2',day,'order-month'));group=node('div',undefined,'program-orders');target.append(group);}
      const card=node('article',undefined,'program-order');card.dataset.upcomingProgram=row.program;card.append(node('small',row.cycle.state==='skip_requested'?'SKIPPED IN PROPOSAL':'PROPOSED CYCLE'),node('h3',row.name));
      if(row.cycle.contents){card.append(node('p',row.pattern==='finite_gift'?'Included in its prepaid term':money(row.cycle.contents.modeled_cents)));for(const line of row.cycle.contents.lines)card.append(node('p',line.quantity+' \u00D7 '+line.title));for(const issue of row.cycle.contents.problems)card.append(node('p',issue,'contents-warning'));}
      else card.append(node('p','This program uses its entered agreement amount. No products have been selected.'));
      card.append(button('Edit this cycle',async()=>{try{if(read().program_id!==row.program){if(!confirm('Open this saved program? Keep any unsaved changes in your current proposal first.'))return;await resume(row.program);}start(row.cycle.original_local_date??row.cycle.due_local_date);}catch(error){$('error').textContent=error.message;}}));group.append(card);
    }
    if(!rows.length)target.append(node('p','Create and save a dated program to see its upcoming work here.','contents-empty'));
  }
  $('upcoming-month').onchange=renderUpcoming;$('upcoming-skipped').onchange=renderUpcoming;$('upcoming-refresh').onclick=async()=>{await refresh();renderUpcoming();};
  function refreshUpdateChoices(){
    const rows=observations().filter(o=>o.offers.length).sort((a,b)=>a.captured.captured_at.localeCompare(b.captured.captured_at));
    $('update-before').replaceChildren(...rows.map(o=>{const option=node('option',o.captured.title+' \u00B7 '+o.captured.captured_at);option.value=o.id;return option;}));updateAfter();
  }
  function updateAfter(){
    const old=observations().find(o=>o.id===$('update-before').value),rows=observations().filter(o=>o.id!==old?.id&&o.captured.url===old?.captured.url&&o.captured.captured_at>old.captured.captured_at);
    $('update-after').replaceChildren(...rows.map(o=>{const option=node('option',o.captured.title+' \u00B7 '+o.captured.captured_at);option.value=o.id;return option;}));plan=null;$('update-apply').disabled=true;
    $('update-result').textContent=rows.length?'Review the programs using the selected older source.':'Import a later capture of this same page to compare source changes.';
  }
  $('update-before').onchange=updateAfter;$('update-after').onchange=()=>{plan=null;$('update-apply').disabled=true;};
  $('update-refresh').onclick=async()=>{await refresh();refreshUpdateChoices();};
  $('update-preview').onclick=async()=>{
    if(updateBusy)return;updateBusy=true;plan=null;$('update-apply').disabled=true;$('update-preview').disabled=true;$('update-programs').replaceChildren();$('update-result').textContent='Reading current source and program versions…';
    try{plan=sourceUpdatePlan(catalog,await readLocal('draft:'),await readLocal('observation:'),$('update-before').value,$('update-after').value);$('update-programs').replaceChildren();
      for(const row of plan.rows){const card=node('article',undefined,'update-program'),label=node('label'),pick=node('input');pick.type='checkbox';pick.dataset.updateProgram=row.key;pick.disabled=!!row.error;pick.checked=false;label.append(pick,node('strong',row.name+' \u00B7 saved revision '+row.revision));card.append(label);
        if(row.error)card.append(node('p',row.error,'contents-warning'));else{card.append(node('p','Product identities and quantities remain attached to this program. Only the selected source version changes.'));card.append(table(['Product','Old unit price','New unit price'],row.changes.map(c=>[c.title,money(c.before),money(c.after)])));if(row.months.length)card.append(table(['Month','Before','After'],row.months.map(m=>[m.month,money(m.before),money(m.after)])));else card.append(node('p','Modeled monthly costs do not change, including fixed-price agreements.'));}
        $('update-programs').append(card);
      }
      $('update-result').textContent=plan.rows.length+' affected saved programs. Select the programs to change; unselected versions stay intact.';$('update-apply').disabled=!plan.rows.some(r=>!r.error);
    }catch(error){$('update-result').textContent=error.message;}finally{updateBusy=false;$('update-preview').disabled=false;}
  };
  $('update-apply').onclick=async()=>{
    if(updateBusy||!plan)return;const selected=[...document.querySelectorAll('[data-update-program]:checked')].map(el=>el.dataset.updateProgram);
    if(!selected.length){$('update-result').textContent='Select at least one saved program.';return;}if(!confirm('Update exactly '+selected.length+' saved programs to the reviewed source version?'))return;
    updateBusy=true;$('update-apply').disabled=true;
    try{const saved=await saveLocalBatch(sourceUpdateWrites(plan,selected));await refresh();$('update-result').textContent=selected.length+' saved programs updated together. Unselected programs and both source captures were kept. Your open editor was not replaced; reopen an updated program before continuing.';for(const el of document.querySelectorAll('[data-update-program]'))el.disabled=true;plan=null;document.dispatchEvent(new Event('workbench:source-updated'));}
    catch(error){$('update-result').textContent=error.message+' Review current versions before trying again.';plan=null;}
    finally{updateBusy=false;}
  };
  document.addEventListener('workbench:view',event=>{if(event.detail==='contents'&&!opening)safe(()=>working&&stableJSON(read())===stableJSON(base)?render():start())();if(event.detail==='upcoming')renderUpcoming();if(event.detail==='updates')refreshUpdateChoices();});
  document.addEventListener('workbench:loaded',()=>{base=working=null;$('compile').disabled=false;});
  return {open:start,renderUpcoming};
}
