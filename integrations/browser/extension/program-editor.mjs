// Human authoring controls over the existing recurring-commerce draft contract.
// Source bodies stay in the shared observation store; this editor carries references.
const money=n=>new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n/100);
const el=(tag,text,classes='')=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(classes)n.className=classes;return n;};
export function usdCents(value) {
  if(typeof value!=='string'||!/^\d+(?:\.\d{1,2})?$/.test(value.trim()))throw Error('Enter a dollar amount with at most two decimal places.');
  const [whole,fraction='']=value.trim().split('.'),n=Number(whole)*100+Number(fraction.padEnd(2,'0'));
  if(!Number.isSafeInteger(n)||n<0||n>100000000)throw Error('Dollar amount is outside the supported range.');
  return n;
}
export function selectedReferences(rows,ids) {
  const wanted=new Set(ids),kept=[];
  for(const row of rows){const o=row.value??row;if(wanted.has(o.id)){if(!o.captured_body_sha256||!o.captured?.url)throw Error('Selected source has no retained identity.');kept.push(o);wanted.delete(o.id);}}
  if(wanted.size)throw Error('A selected source is no longer available. Reopen the source library.');
  return kept;
}
export function mountProgramEditor({input,catalog,onEdit=()=>{}}) {
  const surface=el('div',undefined,'program-editor');
  input.hidden=true;input.before(surface);
  const fields=new Map();let base;
  function add(key,title,value,{kind='text',min,max,step,choices,wide=false}={}) {
    const wrap=el('label',undefined,wide?'editor-field wide':'editor-field'),caption=el('span',title);
    const control=el(choices?'select':kind==='textarea'?'textarea':'input');
    control.id='program-'+key;control.name=key;
    if(choices)for(const [id,name]of choices){const o=el('option',name);o.value=id;control.append(o);}
    else if(kind!=='textarea')control.type=kind;
    if(kind==='checkbox')control.checked=Boolean(value);else control.value=String(value??'');
    if(min!==undefined)control.min=String(min);if(max!==undefined)control.max=String(max);if(step!==undefined)control.step=String(step);
    if(kind==='textarea'){control.rows=3;control.maxLength=2000;}
    if(kind==='text')control.maxLength=key==='name'?120:160;
    wrap.append(caption,control);surface.append(wrap);fields.set(key,control);
  }
  function load(value) {
    base=structuredClone(value);input.setCustomValidity('');input.value=JSON.stringify(base,null,2);surface.replaceChildren();fields.clear();
    const t=catalog.templates.find(t=>t.id===base.template_id);if(!t)throw Error('Select an existing program template.');
    const terms=base.terms??{};
    add('name','Program name',base.name,{wide:true});add('owner','Owner identity',base.owner_subject);
    add('price','Price per cycle · USD',(terms.price_cents/100).toFixed(2),{kind:'number',min:0,step:'0.01'});
    add('shipping','Shipping per cycle · USD',((terms.shipping_cents??0)/100).toFixed(2),{kind:'number',min:0,step:'0.01'});
    add('members','Member capacity',terms.max_members,{kind:'number',min:1,max:100000,step:1});
    add('units','Units per member',terms.quantity_per_member??1,{kind:'number',min:1,max:1000,step:1});
    add('start','First cycle',terms.start_date,{kind:'date'});
    add('cadence','Cadence',terms.cadence?.unit,{choices:[['month','Monthly'],['week','Weekly'],['day','Daily'],...(t.pattern==='replenishment'?[['usage','Stock-triggered']]:[])]});
    add('every','Every',terms.cadence?.every??1,{kind:'number',min:1,max:365,step:1});
    if(t.pattern==='finite_gift')add('gift','Prepaid cycles',terms.gift_cycles,{kind:'number',min:1,max:60,step:1});
    else add('renew','Propose automatic renewal',terms.auto_renew,{kind:'checkbox'});
    if(t.pattern==='try_before_buy')add('style','Styling fee · USD',((terms.style_fee_cents??0)/100).toFixed(2),{kind:'number',min:0,step:'0.01'});
    add('cancellation','Cancellation terms',terms.cancellation,{kind:'textarea',wide:true});
    add('returns','Returns or service remedy',terms.return_policy,{kind:'textarea',wide:true});
    const agreement=el('details',undefined,'wide agreement-fields');agreement.append(el('summary','People and agreement'));
    for(const key of ['owner','members','units','renew','cancellation','returns'])if(fields.has(key))agreement.append(fields.get(key).closest('label'));
    surface.append(agreement);
    if(terms.cycle_moves?.length){const reset=el('button','Reset moved dates');reset.type='button';reset.id='reset-moved-dates';reset.onclick=()=>{if(!confirm('Return all moved cycles to their original dates? Existing skips remain attached.'))return;const current=read();current.terms.cycle_moves=[];load(current);onEdit();};surface.append(reset);}
    surface.append(el('p',t.pattern==='replenishment'?'Stock-triggered programs require fresh stock observations before proposing a reorder.':t.pattern==='finite_gift'?'Gift cycles are prepaid and end without automatic renewal.':'Dates and amounts below are a preview. Nothing is enrolled or charged.','hint wide'));
  }
  function read() {
    if(input.validity.customError)throw Error('Correct the advanced interchange input before reviewing.');
    const text=key=>fields.get(key)?.value??'',integer=key=>{const v=text(key);if(!/^\d+$/.test(v))throw Error('Enter a whole number for '+key+'.');return Number(v);};
    const t=catalog.templates.find(t=>t.id===base.template_id),terms={...base.terms,currency:'USD',price_cents:usdCents(text('price')),shipping_cents:usdCents(text('shipping')),max_members:integer('members'),quantity_per_member:integer('units'),start_date:text('start'),cadence:{unit:text('cadence'),every:integer('every')},auto_renew:t.pattern==='finite_gift'?false:fields.get('renew').checked,cancellation:text('cancellation'),return_policy:text('returns')};
    if(t.pattern==='finite_gift')terms.gift_cycles=integer('gift');
    if(t.pattern==='try_before_buy')terms.style_fee_cents=usdCents(text('style'));
    terms.pause_dates=[...(base.terms.pause_dates??[])];
    const result={...base,name:text('name'),owner_subject:text('owner'),terms};input.value=JSON.stringify(result,null,2);return result;
  }
  surface.addEventListener('input',()=>{onEdit();try{read();}catch{}});
  // Interchange is an export operation, not an editable object in the interface.
  return {load,read};
}
export function renderProgram(target,draft,{contentsActions=true}={}) {
  target.replaceChildren();if(!draft)return;
  const head=el('div',undefined,'preview-heading');head.append(el('h2',draft.name),el('span','Program proposal','tag'));target.append(head);
  const variable=['try_before_buy','replenishment','hybrid'].includes(draft.pattern),cycleAmount=draft.terms.price_cents+(draft.terms.shipping_cents??0);
  target.append(el('p',draft.terms.contents?.pricing==='itemized'?'Product quantities and retained prices determine each cycle estimate; final tax and delivery remain unquoted.':variable?'Final pricing requires the selected products or separate quotes.':draft.pattern==='finite_gift'?money(cycleAmount*draft.terms.gift_cycles)+' prepaid term; skipping a delivery does not refund it.':money(cycleAmount)+' modeled per cycle, including your delivery allowance.'));
  const cycles=el('div',undefined,'cycle-preview');
  for(const cycle of draft.cycle_preview){
    const skipped=cycle.state==='skip_requested',card=el('article',undefined,skipped?'cycle-card skipped':'cycle-card');card.dataset.date=cycle.due_local_date;
    const choose=el('label',undefined,'cycle-selection'),box=el('input');box.type='checkbox';box.dataset.cycleSelect=cycle.due_local_date;box.setAttribute('aria-label','Select cycle '+cycle.due_local_date);choose.append(box,el('span','Cycle '+cycle.cycle));
    const date=new Date(cycle.due_local_date+'T12:00:00Z'),month=el('small',date.toLocaleDateString('en-US',{month:'long',year:'numeric',timeZone:'UTC'})),day=el('strong',String(date.getUTCDate()),'calendar-day');
    const button=el('button',skipped?'Restore cycle':'Skip in proposal');button.dataset.cycleDate=cycle.due_local_date;button.dataset.cycleSkip=String(!skipped);
    const move=el('button','Reschedule');move.dataset.rescheduleDate=cycle.due_local_date;move.setAttribute('aria-label','Reschedule cycle '+cycle.cycle);
    card.append(choose,month,day,el('span',skipped?'Skipped in this proposal':'Proposed cycle'));
    if(cycle.original_local_date)card.append(el('small','Originally '+cycle.original_local_date,'cycle-original'));
    if(cycle.contents){
      card.append(el('strong',draft.pattern==='finite_gift'?'Included in prepaid term':cycle.contents.modeled_cents===null?'Cost unresolved':money(cycle.contents.modeled_cents)));
      card.append(el('p',cycle.contents.lines.map(l=>l.quantity+' \u00D7 '+l.title).join('; ')||'No products selected','cycle-product-label'));
      card.append(el('small',cycle.contents.selection==='custom'?'Cycle-specific selection':'Recurring default contents'));
    }
    if(contentsActions){const contents=el('button',cycle.contents?'Edit contents':'Choose products','cycle-contents-button');contents.dataset.contentsDate=cycle.due_local_date;card.append(contents);}
    card.append(button,move);cycles.append(card);
  }
  target.append(cycles);if(!draft.cycle_preview.length)target.append(el('p','Stock-triggered programs need a fresh stock observation. No calendar dates are created.','empty-state'));
  const steps=el('details'),list=el('ol');steps.append(el('summary','What still requires an execution owner'));
  for(const step of draft.stage_plan)list.append(el('li',step.stage.replaceAll('_',' ')));steps.append(list,el('p','These operations have not run. Saving this proposal gives no supplier or payment authority.'));target.append(steps);
}
export function renderBudget(target,preview) {
  target.replaceChildren();target.className='budget-preview';
  for(const month of preview.months){const row=el('div',undefined,month.over_budget?'budget-month over':'budget-month');row.append(el('strong',month.month),el('span',money(month.modeled_cents)),el('small',month.over_budget?'Over your budget':'Within modeled budget'));target.append(row);}
  if(!preview.months.length)target.append(el('p','Save a dated program to compare its cycle costs.'));
  if(preview.variable_priced_programs.length)target.append(el('p',preview.variable_priced_programs.length+' variable-price programs remain outside the fixed-price total.','warning'));
}
