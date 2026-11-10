// UI commands reuse the observation and recurring-program contracts. No new store.
import {observe, stableJSON, compareObservations} from './observation.mjs';
import {compileDraft, basketPreview} from './recurring-commerce.mjs';
const clone = value => structuredClone(value);
const check = (ok, message) => { if (!ok) throw Error(message); };
const amount = cents => new Intl.NumberFormat('en-US', {style:'currency',currency:'USD'}).format(cents / 100);
const contentKey = observation => {
  const {captured_at, ...content} = observation.captured;
  return stableJSON(content);
};
export async function prepareCaptureBatch(inputs, catalog, existing = []) {
  check(Array.isArray(inputs) && inputs.length > 0 && inputs.length <= 40, 'Choose between 1 and 40 capture files.');
  const known = existing.map(row => row.value ?? row), byId = new Map(known.map(o => [o.id,o]));
  const meanings = new Map(known.map(o => [contentKey(o),o]));
  const rows = [];
  for (const [index, input] of inputs.entries()) {
    try {
      if (input.error) throw Error(input.error);
      const raw = input.value?.schema === 'aurekai.browser-observation.v1' ? input.value.captured : input.value;
      const observation = await observe(raw, catalog), same = byId.get(observation.id) ?? meanings.get(contentKey(observation));
      const versions = known.filter(o => o.captured.url === observation.captured.url);
      const state = same ? 'unchanged' : versions.length ? 'new_version' : 'new_source';
      rows.push({index, name:input.name || 'Capture '+(index+1), state, observation:same ?? observation,
        explanation:same ? 'Already retained. The original observation date is kept.' : versions.length ? 'A different version of this page. Earlier captures stay available.' : 'A new page in your source library.'});
      if (!same) {known.push(observation); byId.set(observation.id,observation); meanings.set(contentKey(observation),observation);}
    } catch (error) { rows.push({index,name:input.name || 'Capture '+(index+1),state:'invalid',explanation:error.message}); }
  }
  return rows;
}
export function batchWrites(rows, indexes) {
  const selected = new Set(indexes), ids = new Set(), changes = [];
  for (const row of rows) if (selected.has(row.index)) {
    check(['new_source','new_version'].includes(row.state), 'Select only new source material.');
    check(!ids.has(row.observation.id), 'A source is selected twice.'); ids.add(row.observation.id);
    changes.push({key:row.observation.id,value:row.observation,expectedRevision:0}); selected.delete(row.index);
  }
  check(selected.size === 0, 'The selected import row no longer exists.');
  check(changes.length > 0, 'Select at least one new capture.'); return changes;
}
export function reviseCycles(catalog, input, dates, skip) {
  check(typeof skip === 'boolean' && Array.isArray(dates) && dates.length > 0, 'Select the cycles to change.');
  const current = compileDraft(catalog,input), available = new Map(current.cycle_preview.map(c => [c.due_local_date,c.original_local_date??c.due_local_date]));
  check(dates.every(date => available.has(date)), 'A selected cycle is outside this program preview.');
  const next = clone(input), pauses = new Set(next.terms.pause_dates ?? []);
  for (const date of dates) skip ? pauses.add(available.get(date)) : pauses.delete(available.get(date));
  next.terms.pause_dates = [...pauses].sort(); compileDraft(catalog,next); return next;
}
export function moveCycle(catalog,input,date,to) {
  const current=compileDraft(catalog,input),cycle=current.cycle_preview.find(c=>c.due_local_date===date);
  check(cycle,'This cycle changed. Reopen it before choosing a date.');
  const anchor=cycle.original_local_date??cycle.due_local_date,next=clone(input);
  next.terms.cycle_moves=(next.terms.cycle_moves??[]).filter(m=>m.from!==anchor);
  if(to!==anchor)next.terms.cycle_moves.push({from:anchor,to});
  compileDraft(catalog,next);return next;
}
export function cycleMoveImpact(catalog,before,after,observations=[]) {
  const a=compileDraft(catalog,before,observations),b=compileDraft(catalog,after,observations);
  const totals=d=>new Map(basketPreview([d],100000000).months.map(m=>[m.month,m.modeled_cents]));
  const old=totals(a),next=totals(b),months=[...new Set([...old.keys(),...next.keys()])].sort();
  return {months:months.filter(m=>(old.get(m)??0)!==(next.get(m)??0)).map(month=>({month,before:old.get(month)??0,after:next.get(month)??0})),prepaid:b.pattern==='finite_gift',variable:['try_before_buy','replenishment','hybrid'].includes(b.pattern)};
}
export function adoptObservedPrice(input, observation, index) {
  const offer = observation?.offers?.[index];
  check(offer && !offer.range && Number.isSafeInteger(offer.price.usd_cents), 'This source has no exact USD price to use.');
  const next = clone(input); next.terms.price_cents = offer.price.usd_cents; return next;
}
export function comparisonRows(observations) {
  return observations.map(o => ({id:o.id,title:o.captured.title || o.captured.url,url:o.captured.url,date:o.captured.captured_at,
    fixture:o.captured.fixture, offers:o.offers.map((offer,index) => ({index,name:offer.product_name,
      price:offer.range ? 'Price range; not one quoted price' : offer.price.usd_cents === null ? [offer.price.raw,offer.price.currency].filter(Boolean).join(' ') || 'Not stated' : amount(offer.price.usd_cents),
      usable:!offer.range && Number.isSafeInteger(offer.price.usd_cents),availability:offer.availability?.split('/').at(-1) || 'Not stated'})),
    terms:o.captured.term_snippets,excerpt:o.captured.selected_text,recipes:o.recipes ?? [],media:o.media ?? []}));
}
export function changeFields(before, after) {
  const fields = [['name','Program name'],['terms.price_cents','Cycle price'],['terms.shipping_cents','Delivery allowance'],['terms.start_date','First cycle'],['terms.cadence.unit','Cadence'],['terms.cadence.every','Interval'],['terms.max_members','Capacity'],['terms.quantity_per_member','Units per member'],['terms.auto_renew','Renewal proposed'],['terms.gift_cycles','Prepaid cycles'],['terms.style_fee_cents','Styling fee'],['terms.pause_dates','Skipped cycles'],['terms.cycle_moves','Moved cycle dates'],['terms.cancellation','Cancellation terms'],['terms.return_policy','Return or service terms']];
  const get = (value,path) => path.split('.').reduce((v,k) => v?.[k],value);
  const readable = (value,path) => value === undefined ? 'Not set' : path.endsWith('_cents') ? amount(value) : Array.isArray(value) ? value.map(v=>path==='terms.cycle_moves'?v.from+' → '+v.to:v).join(', ') || 'None' : typeof value === 'boolean' ? value ? 'Yes' : 'No' : String(value);
  const rows = fields.filter(([path]) => stableJSON(get(before,path)) !== stableJSON(get(after,path))).map(([path,label]) => ({label,before:readable(get(before,path),path),after:readable(get(after,path),path)}));
  const contentsLabel=c=>c?c.pricing+'; '+c.defaults.reduce((n,x)=>n+x.quantity,0)+' default units; '+c.cycles.length+' customized cycles':'No product selection';
  if(stableJSON(before?.terms?.contents)!==stableJSON(after.terms.contents))rows.push({label:'Product contents',before:contentsLabel(before?.terms?.contents),after:contentsLabel(after.terms.contents)});
  for(const cycle of after.cycle_preview??[]){
    const anchor=cycle.original_local_date??cycle.due_local_date,old=before?.cycle_preview?.find(c=>(c.original_local_date??c.due_local_date)===anchor);
    const describe=c=>c?c.lines.map(l=>l.quantity+' \u00D7 '+l.title+' @ '+(l.unit_cents===null?'unknown price':amount(l.unit_cents))+' \u00B7 '+l.recorded_at).join('; ')||'No products':'No selected contents';
    if(stableJSON(old?.contents)!==stableJSON(cycle.contents)&&cycle.contents)rows.push({label:'Contents for '+anchor,before:describe(old?.contents),after:describe(cycle.contents)});
  }
  const a = before?.browser_evidence ?? [], b = after.browser_evidence ?? [];
  if (stableJSON(a) !== stableJSON(b)) rows.push({label:'Selected sources',before:a.length+' attached',after:b.length+' attached'});
  return rows;
}
export function samePageChanges(observations) {
  if (observations.length !== 2 || observations[0].captured.url !== observations[1].captured.url) return null;
  const [before,after] = [...observations].sort((a,b) => a.captured.captured_at.localeCompare(b.captured.captured_at));
  return compareObservations(before,after);
}
export function reviewText(draft, observations = []) {
  const labels = new Map(observations.map(o => [o.id,o.captured.title || o.captured.url]));
  const budget = basketPreview([draft],100000000);
  return [draft.name,'LOCAL PROGRAM PROPOSAL','',
    draft.terms.contents?.pricing==='itemized'?'Terms: selected product prices and quantities + '+amount(draft.terms.shipping_cents??0)+' delivery allowance per cycle.':'Terms: '+amount(draft.terms.price_cents)+' per cycle + '+amount(draft.terms.shipping_cents ?? 0)+' delivery allowance.',
    'Capacity: '+draft.terms.max_members+'; units per member: '+(draft.terms.quantity_per_member ?? 1)+'.',
    'Cancellation: '+draft.terms.cancellation,'Return or service remedy: '+draft.terms.return_policy,'',
    'CYCLE PREVIEW',...draft.cycle_preview.map(c => c.due_local_date+(c.original_local_date?' (moved from '+c.original_local_date+')':'')+' — '+(c.state === 'skip_requested' ? 'Skipped in this proposal' : 'Proposed cycle')),
    ...(draft.cycle_preview.length ? [] : ['Stock-triggered: no dates scheduled.']), '',
    ...(draft.terms.contents?['PRODUCT CONTENTS',...draft.cycle_preview.flatMap(c=>[
      c.due_local_date+' · '+c.contents.selection+' contents · '+(c.contents.modeled_cents===null?'Unresolved cost':amount(c.contents.modeled_cents)),
      ...c.contents.lines.map(l=>l.quantity+' × '+l.title+' · '+(l.unit_cents===null?'No exact USD price':amount(l.unit_cents))+' · '+l.url),
      ...c.contents.problems,''])]:[]),
    'MODELED COST BY MONTH',...budget.months.map(m => m.month+': '+amount(m.modeled_cents)),
    ...(budget.variable_priced_programs.length ? ['Variable pricing remains outside the fixed-price total.'] : []),
    'Taxes and final delivery charges are not quoted.','',
    'SOURCES',...(draft.browser_evidence ?? []).map(r => (labels.get(r.id) || r.url)+'\n'+r.url+'\nRetained source: '+r.sha256),'',
    'This is a local draft, not an active subscription, supplier approval, order or payment.'].join('\n');
}
export function editHistory(initial, limit = 80) {
  let entries = [clone(initial)], position = 0;
  return {
    record(value) {if (stableJSON(entries[position]) === stableJSON(value)) return; entries = entries.slice(0,position+1);entries.push(clone(value));if(entries.length > limit)entries.shift();position = entries.length-1;},
    undo() {if(position > 0)position--;return clone(entries[position]);},
    redo() {if(position < entries.length-1)position++;return clone(entries[position]);},
    get canUndo() {return position > 0;}, get canRedo() {return position < entries.length-1;}
  };
}
