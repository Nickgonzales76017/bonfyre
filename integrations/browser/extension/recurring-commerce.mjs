import {checkContents,projectContents} from './contents.mjs';
// Draft compiler for recurring-commerce programs: dates, try-on arithmetic,
// stock-triggered proposals, preference-aware selection, budget preview, and
// the native records a catalogue maps to. Lifted unchanged from the authoring
// studio delivered 2026-10-03 so the calculations can be tested and used
// without a browser. It computes drafts and previews; it connects to no
// merchant, charges nothing, reserves nothing and renews nothing.
// Pure recurring-commerce compilation. CMS owns drafts; existing owners execute effects.
// No scheduler, payment client, merchant impersonation, or second business database.
function requireValue(ok, message) { if (!ok) throw new Error(message); }
const integer = (n, label, min = 0, max = 100000000) => {
  requireValue(Number.isSafeInteger(n) && n >= min && n <= max, `${label}: invalid integer`);
  return n;
};
function dateOnly(value) {
  requireValue(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value), 'Use YYYY-MM-DD');
  const date = new Date(`${value}T00:00:00Z`);
  requireValue(Number.isFinite(+date) && date.toISOString().slice(0, 10) === value, 'Invalid calendar date');
  return date;
}
function cycleDates(start, cadence, count) {
  const first = dateOnly(start); integer(count, 'cycles', 1, 60);
  requireValue(['month', 'week', 'day'].includes(cadence.unit), 'Usage triggers need stock evidence, not calendar dates');
  integer(cadence.every, 'interval', 1, 365);
  return Array.from({length:count}, (_, i) => {
    let date;
    if (cadence.unit === 'month') {
      const month = first.getUTCMonth() + i * cadence.every;
      const last = new Date(Date.UTC(first.getUTCFullYear(), month + 1, 0)).getUTCDate();
      date = new Date(Date.UTC(first.getUTCFullYear(), month, Math.min(first.getUTCDate(), last)));
    } else date = new Date(+first + i * cadence.every * (cadence.unit === 'week' ? 7 : 1) * 86400000);
    return date.toISOString().slice(0,10);
  });
}
function tryOnCheckout(styleFeeCents, items) {
  integer(styleFeeCents, 'styling fee'); requireValue(Array.isArray(items), 'items must be an array');
  const kept = items.reduce((sum, item) => {
    integer(item.price_cents, 'item price'); requireValue(['keep','return'].includes(item.decision), 'Unresolved keep/return decision');
    return sum + (item.decision === 'keep' ? item.price_cents : 0);
  }, 0);
  integer(kept, 'kept total');
  return {kept_items_cents:kept, styling_fee_paid_cents:styleFeeCents,
    applied_credit_cents:Math.min(styleFeeCents,kept), remaining_balance_cents:Math.max(0,kept-styleFeeCents),
    total_paid_cents:Math.max(styleFeeCents,kept), status:'calculation_only_no_charge'};
}
function stockDecision(stock, asOf) {
  dateOnly(asOf); const observed = dateOnly(stock.observed_on);
  requireValue(['member_report','measurement'].includes(stock.basis), 'Stock must have an explicit observation basis');
  const age = (+dateOnly(asOf)-+observed)/86400000;
  requireValue(age>=0 && age<=7, 'Stock observation is future-dated or stale');
  requireValue(Number.isFinite(stock.quantity) && stock.quantity>=0, 'Invalid stock quantity');
  requireValue(Number.isFinite(stock.daily_use) && stock.daily_use>0, 'Positive daily use required');
  integer(stock.lead_days,'lead days',0,60); integer(stock.buffer_days??0,'buffer days',0,30);
  const days = Math.max(0, stock.quantity/stock.daily_use - age);
  return {days_remaining:days, proposal:days<=stock.lead_days+(stock.buffer_days??0)?'review_reorder':'keep_current_stock',
    permission_to_charge:false, basis:stock.basis, observed_on:stock.observed_on};
}
function selectItems(items, preferences = {}) {
  const rejected=[], eligible=[], excluded = new Set(preferences.excluded_ingredients??[]);
  for (const item of items) {
    const reasons=[];
    if (preferences.no_repeats && (preferences.received_ids??[]).includes(item.id)) reasons.push('already_received');
    if (excluded.size && !Array.isArray(item.ingredients)) reasons.push('ingredients_unverified');
    if ((item.ingredients??[]).some(x=>excluded.has(x))) reasons.push('ingredient_excluded');
    if (preferences.max_heat !== undefined && (!Number.isFinite(item.heat) || item.heat>preferences.max_heat)) reasons.push('heat_outside_preference');
    if (!Number.isSafeInteger(item.available_units) || item.available_units<1) reasons.push('no_verified_inventory');
    (reasons.length?rejected:eligible).push(reasons.length?{id:item.id,reasons}:item);
  }
  return {eligible,rejected,meaning:'Selection screening is not an allergen-safety certification or inventory reservation.'};
}
function compileDraft(catalog, input, observations = []) {
  const template = catalog.templates.find(t=>t.id===input.template_id);
  requireValue(template, 'Unknown program template');
  requireValue(/^[a-z0-9][a-z0-9:_-]{2,119}$/i.test(input.program_id??''), 'A stable program_id is required');
  requireValue(typeof input.owner_subject==='string' && input.owner_subject.includes(':'), 'Explicit owner subject required');
  requireValue(typeof input.name==='string' && input.name.trim().length>=3 && input.name.length<=120, 'Name must be 3–120 characters');
  const pattern=catalog.patterns[template.pattern], terms=structuredClone(input.terms??{});
  requireValue(terms.currency==='USD', 'This first compiler supports USD integer cents only');
  integer(terms.price_cents,'cycle price'); integer(terms.shipping_cents??0,'shipping');
  integer(terms.max_members,'capacity',1,100000); integer(terms.quantity_per_member??1,'units per member',1,1000);
  requireValue(typeof terms.cancellation==='string' && terms.cancellation.trim(), 'Explicit cancellation terms required');
  requireValue(typeof terms.return_policy==='string' && terms.return_policy.trim(), 'Explicit return/service remedy terms required');
  if(template.pattern==='finite_gift') {integer(terms.gift_cycles,'gift cycles',1,60);requireValue(terms.auto_renew===false,'Gift must end without auto-renewal');}
  if(template.pattern==='try_before_buy') integer(terms.style_fee_cents,'styling fee');
  requireValue(terms.auto_renew===false || terms.auto_renew===true,'Explicit renewal choice required');
  requireValue(terms.cadence?.unit!=='usage' || template.pattern==='replenishment','Only replenishment may use stock-triggered cadence');
  const dates=terms.cadence?.unit==='usage'?[]:cycleDates(terms.start_date,terms.cadence,Math.min(terms.gift_cycles??6,60));
  const pauses=(terms.pause_dates??[]).map(d=>{dateOnly(d);return d;});
  requireValue(new Set(pauses).size===pauses.length,'Duplicate pause dates');
  const moves=terms.cycle_moves??[];
  requireValue(Array.isArray(moves)&&moves.length<=60,'Use at most 60 cycle date changes');
  const overrides=new Map();
  for(const move of moves){
    requireValue(move&&Object.keys(move).sort().join(',')==='from,to','A date change needs its original cycle and proposed date');
    requireValue(dates.includes(move.from)&&!overrides.has(move.from),'A moved cycle no longer belongs to this schedule. Reset moved dates before changing the series');
    const to=dateOnly(move.to),from=dateOnly(move.from);
    requireValue(Math.abs((to-from)/86400000)<=366,'Move a cycle by at most 366 days');
    overrides.set(move.from,move.to);
  }
  const scheduled=dates.map((date,i)=>({cycle:i+1,due_local_date:overrides.get(date)??date,
    ...(overrides.has(date)?{original_local_date:date}:{}),state:pauses.includes(date)?'skip_requested':'preview_required',
    effect:'none',units_capacity:terms.max_members*(terms.quantity_per_member??1)}));
  requireValue(scheduled.every((row,i)=>!i||row.due_local_date>scheduled[i-1].due_local_date),'Choose a date after the previous cycle and before the next cycle');
  if(terms.cycle_moves)terms.cycle_moves=[...moves].sort((a,b)=>a.from.localeCompare(b.from));
  if(terms.contents){
    checkContents(terms.contents,dates,template.pattern);
    for(const cycle of scheduled)cycle.contents=projectContents(terms.contents,cycle.original_local_date??cycle.due_local_date,observations,terms);
  }
  const unresolved=['merchant_identity_and_authority','fulfillment_capacity','billing_provider','buyer_consent','tax_and_delivery_quote'];
  if(scheduled.some(c=>c.contents?.problems.length))unresolved.push('contents_selection');
  return {schema:'aurekai.recurring-program.v1',program_id:input.program_id,template_id:template.id,
    owner_subject:input.owner_subject,name:input.name.trim(),category:template.category,pattern:template.pattern,
    terms,preferences:structuredClone(input.preferences??{}),reference_profiles:template.reference_profiles,
    stage_plan:pattern.stages.map((stage,i)=>({ordinal:i+1,stage,state:'not_executed'})),cycle_preview:scheduled,
    native_owners:template.native_owners,readiness:{state:'configured_draft',unresolved},sample:input.sample===true,
    status:'draft',cannot_infer:'A configured draft or preview is not a merchant connection, consent, invoice, payment, stock reservation, shipment, service visit, or active renewal.'};
}
function basketPreview(drafts, monthlyBudgetCents) {
  integer(monthlyBudgetCents,'monthly budget'); const monthly={}; const variable=[];
  for(const draft of drafts) {
    if(draft.terms.contents && !['finite_gift','try_before_buy'].includes(draft.pattern)) {
      for(const cycle of draft.cycle_preview){
        if(cycle.state==='skip_requested')continue;
        const cents=cycle.contents?.modeled_cents;
        if(cents===null||cents===undefined){if(!variable.includes(draft.program_id))variable.push(draft.program_id);continue;}
        const key=cycle.due_local_date.slice(0,7);monthly[key]=(monthly[key]??0)+cents;integer(monthly[key],'basket total');
      }
      continue;
    }
    if(['try_before_buy','replenishment','hybrid'].includes(draft.pattern)) {variable.push(draft.program_id);continue;}
    if(draft.pattern==='finite_gift') {
      const key=draft.terms.start_date.slice(0,7);
      monthly[key]=(monthly[key]??0)+(draft.terms.price_cents+(draft.terms.shipping_cents??0))*draft.terms.gift_cycles;
      integer(monthly[key],'basket total'); continue;
    }
    for(const cycle of draft.cycle_preview) {
      if(cycle.state==='skip_requested')continue;
      const key=cycle.due_local_date.slice(0,7);
      // Finite gifts are prepaid, not billed once again for every shipment.
      const due=draft.terms.price_cents+(draft.terms.shipping_cents??0);
      monthly[key]=(monthly[key]??0)+due;integer(monthly[key],'basket total');
    }
  }
  return {months:Object.entries(monthly).sort().map(([month,cents])=>({month,modeled_cents:cents,over_budget:cents>monthlyBudgetCents})),
    variable_priced_programs:variable,complete_price:variable.length===0,taxes:'not_quoted',checkout:'not_connected'};
}
function nativeRecords(catalog) {
  requireValue(catalog.schema==='aurekai.recurring-commerce.catalog.v1','Unknown catalog');
  const seen=new Set();
  for(const t of catalog.templates){requireValue(!seen.has(t.id)&&catalog.patterns[t.pattern],'Duplicate template or unknown pattern');seen.add(t.id);}
  return {
    profiles:catalog.profiles.map(p=>({ext_id:`recurring-provider-${p.id}`,name:p.name,url:p.url,says:JSON.stringify({summary:p.summary,terms:p.terms,source_status:p.source_status,checked_on:p.checked_on}),estate:'recurring-commerce',grouping:p.category,functions:p.pattern,related:p.source_urls.join(' '),cannot_infer:'Independent reference profile. Not affiliated, merchant-claimed, connected, in stock, or available to purchase through Aurekai.',status:'published'})),
    templates:catalog.templates.map((t,i)=>({template_id:t.id,name:t.name,archetype:t.pattern,ordinal:1000+i,subject_kinds:['person','household','institution','project'],semantic_slots:t.semantic_slots,capability_families:['source_intake','identity','workflow','capacity_market','reporting'],owner_bindings:t.native_owners,funding_candidates:[],surface_policy:'bind_into_existing_world',page_policy:'no_dedicated_page_required',description:`${t.promise} Setup: ${t.configuration_fields.join(', ')}. Authoring pack: aurekai.recurring-commerce.catalog.v1 / ${t.id}.`,cannot_infer:'Reusable authoring template, not an operating seller, paid subscription, fulfillment capability, or brand affiliation.',status:'published'}))
  };
}

export { requireValue, dateOnly, cycleDates, tryOnCheckout, stockDecision, selectItems, compileDraft, basketPreview, nativeRecords };
