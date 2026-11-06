import {createWorkspaceLifetime} from '../../components/BonfyreServiceSite/workspace-lifetime.mjs';
import {releaseDigest} from '../../components/BonfyreServiceSite/catalogue-release.mjs';
import {openDestinationStore} from './workspace-store.mjs';
import {createProcurementKernel} from './procurement-wasm.mjs';
import {decode} from './workspace-msgpack.mjs';
import {enactableSupplierContracts,encodeSupplierAcceptance,decodeSupplierEnactment} from './procurement-enactment-client.mjs';

const root=document.querySelector('[data-purchase-lists]');
if(!root)throw Error('Room procurement host is missing.');

const ui=name=>root.querySelector('[data-ui="'+name+'"]');
const life=createWorkspaceLifetime();
const prefixes={
 purchase:'procurement-draft:',
 rfq:'procurement-rfq:',
 quote:'procurement-quote:',
 comparison:'procurement-comparison:',
 allocation:'procurement-allocation:',
 approval:'procurement-approval:',
 orderIntent:'procurement-order-intent:'
};
const el=(tag,text)=>{const node=document.createElement(tag);if(text!==undefined)node.textContent=text;return node;};
const clean=value=>String(value??'').trim();
const utcSecond=()=>new Date().toISOString().replace(/\.\d{3}Z$/,'Z');
const integer=(value,{nullable=false,positive=false}={})=>{
 const raw=clean(value);
 if(!raw&&nullable)return null;
 if(!/^\d+$/.test(raw))throw Error('Use whole non-negative cents and quantities.');
 const n=Number(raw);
 if(!Number.isSafeInteger(n)||(positive&&n<1))throw Error('Use a bounded whole number.');
 return n;
};
const exactUtc=value=>{
 const raw=clean(value);
 if(!raw)return null;
 if(!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(raw))throw Error('Use exact UTC seconds, for example 2026-10-25T18:00:00Z.');
 return raw;
};

let store,kernel,busy=false;
let rfqs=[],quotes=[],comparisons=[],allocations=[],approvals=[],orderIntents=[];
let rfqValue=null,rfqRevision=0,comparisonValue=null,comparisonCurrent=false,allocationValue=null,allocationRevision=0,approvalValue=null,approvalRevision=0,approvalStatus=null,orderIntentValue=null,orderIntentRevision=0,protocolPreflightValue=null,enactmentReadinessStatus=null,enactmentReceipt=null,downstreamStatus=null;

function installPanel(){
 if(ui('native-procurement'))return;
 const panel=el('section');
 panel.className='rp-card rp-native';
 panel.dataset.ui='native-procurement';
 panel.innerHTML='<style>.rp-native .rp-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:16px 0}.rp-native label{display:grid;gap:6px}.rp-native input{box-sizing:border-box;width:100%;min-width:0;min-height:44px;padding:10px 12px;border:1px solid #879d83;border-radius:8px;background:white;color:inherit;font:inherit}.rp-native fieldset{margin:12px 0;padding:14px;border:1px solid #dce3d8;border-radius:12px}.rp-native legend{padding:0 6px;font-weight:700}.rp-native [data-ui=native-rfq-review],.rp-native [data-ui=native-comparison-review]{margin-top:18px;padding-top:16px;border-top:1px solid #dce3d8} .rp-native [data-governed-proof]{margin:18px 0;padding:20px;border:1px solid #99ad93;border-radius:15px;background:#f5f8f0}.rp-native [data-governed-proof] summary{font:650 17px/1.3 system-ui;cursor:pointer}.rp-proof-identity{font:11px/1.7 ui-monospace,monospace;overflow-wrap:anywhere;color:#4b6551}.rp-proof-gates{padding:0;display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;list-style:none}.rp-proof-gates li{display:grid;gap:7px;align-content:start;padding:15px;border:1px solid #ccd7c5;border-radius:10px}.rp-proof-gates li[data-verified=true]{border-color:#618656;background:#e5efdf}.rp-proof-gates li[data-verified=false]{border-color:#b8a08c;background:#f7f0e5}.rp-proof-gates strong{font:700 11px/1.2 system-ui;text-transform:uppercase;letter-spacing:.05em}.rp-proof-gates span{font:13px/1.5 system-ui}@media(max-width:550px){.rp-native [data-governed-proof]{padding:15px}.rp-proof-gates{grid-template-columns:1fr}}</style>'
  +'<h2>Supplier requests and quotes</h2>'
  +'<p>Continue the selected saved purchase list into supplier-request drafts, retained quote observations and quote comparisons. These are preparation records only: no supplier contact, quote acceptance, purchase approval, reservation, order, payment, settlement, fulfillment or shipment occurs here.</p>'
  +'<div class="rp-grid"><label>Supplier ID (optional)<input data-ui="native-supplier-id" maxlength="256"></label><label>Supplier name (optional)<input data-ui="native-supplier-name" maxlength="512"></label></div>'
  +'<div class="rp-bar"><button data-ui="native-rfq-prepare" disabled>Prepare supplier request</button><button data-ui="native-rfq-save" disabled>Save supplier request</button></div>'
  +'<label>Saved supplier request<select data-ui="native-rfq-saved"><option value="">No supplier request selected</option></select></label>'
  +'<section data-ui="native-rfq-review" hidden></section>'
  +'<section data-ui="native-quote-section" hidden><h3>Retain a supplier quote observation</h3>'
  +'<div class="rp-grid"><label>Quote supplier ID (optional)<input data-ui="native-quote-supplier-id" maxlength="256"></label><label>Quote supplier name (optional)<input data-ui="native-quote-supplier-name" maxlength="512"></label><label>Supplier quote ID (optional)<input data-ui="native-supplier-quote-id" maxlength="256"></label><label>Currency<input data-ui="native-currency" value="USD" maxlength="3"></label><label>Valid until — exact UTC (optional)<input data-ui="native-valid-until" placeholder="2026-10-25T18:00:00Z" maxlength="20"></label><label>Source reference<input data-ui="native-source-ref" placeholder="file:quote.pdf" maxlength="4000"></label><label>Freight cents (blank if unknown)<input data-ui="native-freight" inputmode="numeric"></label><label>Tax cents (blank if unknown)<input data-ui="native-tax" inputmode="numeric"></label><label>Other fees cents (blank if unknown)<input data-ui="native-fees" inputmode="numeric"></label></div>'
  +'<div data-ui="native-quote-lines"></div><div class="rp-bar"><button data-ui="native-quote-save" disabled>Retain quote observation</button></div>'
  +'<label>Retained quote observation<select data-ui="native-quote-saved"><option value="">No quote selected</option></select></label></section>'
  +'<section data-ui="native-comparison-section" hidden><h3>Quote comparison</h3><div class="rp-bar"><button data-ui="native-compare-save" disabled>Compare current retained quotes</button></div><label>Saved comparison<select data-ui="native-comparison-saved"><option value="">No comparison selected</option></select></label><section data-ui="native-comparison-review" hidden></section>'
  +'<section data-ui="native-allocation-section" hidden><h3>Allocation plan</h3><p>Plan an exact split using the lowest known merchandise unit prices in this retained comparison. Freight, tax and quote-level fees remain unattributed, so this is not a landed-cost ranking. It does not accept a quote, approve a purchase, create a cart or commitment, place an order, pay, settle, fulfill or ship.</p><div class="rp-bar"><button data-ui="native-allocation-prepare" disabled>Prepare allocation plan</button><button data-ui="native-allocation-save" disabled>Save allocation plan</button></div><label>Saved allocation plan<select data-ui="native-allocation-saved"><option value="">No allocation selected</option></select></label><section data-ui="native-allocation-review" hidden></section>'
  +'<section data-ui="native-approval-section" hidden><h3>Approval request</h3><p>Prepare a review packet for AuthorityGraph from one exact saved allocation version. This does not grant approve or spend authority and does not accept a quote or create an order.</p><div class="rp-grid"><label>Requester<input data-ui="native-approval-requester" maxlength="256" placeholder="user:19"></label><label>Reason (optional)<input data-ui="native-approval-reason" maxlength="2000" placeholder="Why this allocation should be reviewed"></label></div><div class="rp-bar"><button data-ui="native-approval-prepare" disabled>Prepare approval request</button><button data-ui="native-approval-save" disabled>Save approval request</button></div><label>Saved approval request<select data-ui="native-approval-saved"><option value="">No approval request selected</option></select></label><section data-ui="native-approval-review" hidden></section><div class="rp-bar"><button data-ui="native-approval-reconcile" disabled>Check authority decision</button></div><section data-ui="native-authority-status" hidden></section><section data-ui="native-order-section" hidden><h3>Order intent proposal</h3><p>Prepare the next buyer-side handoff only after the saved approval reconciles against governance decision, human assent, and AuthorityGraph approve + spend grants. This still does not accept a quote, open a commitment, issue a purchase order, create a payable or pay.</p><div class="rp-bar"><button data-ui="native-order-prepare" disabled>Prepare order intent</button><button data-ui="native-order-save" disabled>Save order intent</button></div><label>Saved order intent<select data-ui="native-order-saved"><option value="">No order intent selected</option></select></label><section data-ui="native-order-review" hidden></section><section data-ui="native-protocol-section" hidden><h3>Protocol contract preflight</h3><p>Derive one effect-free <strong>quote-accept</strong> ProtocolContract candidate per exact supplier lot. Actor fields are proposals only: supplier identity binding, role authority, supplier acceptance, protocol enactment and CommitmentGraph obligations remain unverified and no contract is saved or enacted here.</p><label>Buyer actor<input data-ui="native-protocol-buyer" maxlength="256" placeholder="buyer:organization-or-user"></label><div data-ui="native-protocol-lots"></div><div class="rp-bar"><button data-ui="native-protocol-prepare" disabled>Prepare contract preflight</button></div><section data-ui="native-protocol-review" hidden></section></section><div class="rp-bar"><button data-ui="native-enactment-readiness" disabled>Check enactment readiness</button><button data-ui="native-order-reconcile" disabled>Reconcile downstream owners</button></div><section data-ui="native-enactment-status" hidden></section><section data-ui="native-supplier-acceptance" hidden><h3>Signed supplier acceptance</h3><p>Import a detached supplier signature for one exact cryptographically bound preflight lot. Quote identity, digest, amount, currency, actors and capability are derived from retained procurement state and are not editable here. Aurekai does not generate or impersonate the supplier signature.</p><label>Cryptographically bound supplier lot<select data-ui="native-enactment-contract"><option value="">No supplier lot selected</option></select></label><div class="rp-grid"><label>Supplier accepted at — exact UTC<input data-ui="native-enactment-accepted-at" maxlength="20" placeholder="2026-10-23T20:00:00Z"></label><label>Detached Ed25519 signature<input data-ui="native-enactment-signature" maxlength="128" autocomplete="off" spellcheck="false" placeholder="128 lowercase hex characters"></label></div><div class="rp-bar"><button data-ui="native-enactment-submit" disabled>Verify supplier acceptance and enact commitments</button></div><section data-ui="native-enactment-result" hidden></section></section><section data-ui="native-downstream-status" hidden></section></section></section></section></section>';
 root.append(panel);
}

function message(body,error=false){
 if(life.closed)return;
 const host=ui(error?'error':'status');
 if(host)host.textContent=body;
}

function matchingQuotes(){
 return rfqValue?quotes.filter(row=>row.value?.source?.rfq_id===rfqValue.id&&row.value?.source?.rfq_digest===rfqValue.digest):[];
}

function controls(){
 if(life.closed)return;
 ui('native-rfq-prepare').disabled=busy||!kernel;
 ui('native-rfq-save').disabled=busy||!rfqValue;
 ui('native-rfq-saved').disabled=busy;
 ui('native-quote-save').disabled=busy||!rfqValue;
 ui('native-quote-saved').disabled=busy;
 ui('native-compare-save').disabled=busy||!rfqValue||!matchingQuotes().length;
 ui('native-comparison-saved').disabled=busy;
 ui('native-allocation-prepare').disabled=busy||!rfqValue||!comparisonValue||!comparisonCurrent||!comparisonQuotes().length;
 ui('native-allocation-save').disabled=busy||!allocationValue;
 ui('native-allocation-saved').disabled=busy;
 const allocationCurrent=allocationValue&&allocationRevision>0&&recheckAllocation(allocationValue).current;
 ui('native-approval-prepare').disabled=busy||!allocationCurrent||!kernel;
 ui('native-approval-save').disabled=busy||!approvalValue;
 ui('native-approval-saved').disabled=busy;
 ui('native-approval-reconcile').disabled=busy||!approvalValue||approvalRevision<1;
 const orderEligible=approvalStatus?.state==='eligible'&&approvalStatus?.eligible_for_order_intent===true;
 ui('native-order-prepare').disabled=busy||!orderEligible||!allocationCurrent||!kernel;
 ui('native-order-save').disabled=busy||!orderIntentValue;
 ui('native-order-saved').disabled=busy;
 ui('native-protocol-prepare').disabled=busy||!orderIntentValue||orderIntentRevision<1||!kernel;
 ui('native-enactment-readiness').disabled=busy||!orderIntentValue||orderIntentRevision<1;
 const enactable=enactableSupplierContracts(protocolPreflightValue),selected=enactable.find(row=>row.exchange_id===ui('native-enactment-contract')?.value),signature=clean(ui('native-enactment-signature')?.value).toLowerCase(),acceptedAt=clean(ui('native-enactment-accepted-at')?.value);
 if(ui('native-enactment-contract'))ui('native-enactment-contract').disabled=busy||!enactable.length;
 if(ui('native-enactment-submit'))ui('native-enactment-submit').disabled=busy||enactmentReadinessStatus?.ready!==true||!selected||!/^[a-f0-9]{128}$/.test(signature)||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(acceptedAt);
 ui('native-order-reconcile').disabled=busy||!orderIntentValue||orderIntentRevision<1;
 for(const input of root.querySelectorAll('.rp-native input'))input.disabled=busy;
}

async function run(fn){
 if(busy||life.closed)return;
 busy=true;
 controls();
 try{await fn();}
 catch(error){message(error.message||String(error),true);}
 finally{busy=false;controls();}
}

async function readAll(){
 const [r,q,c,a,p,o]=await Promise.all([
  store.readLocal(prefixes.rfq),
  store.readLocal(prefixes.quote),
  store.readLocal(prefixes.comparison),
  store.readLocal(prefixes.allocation),
  store.readLocal(prefixes.approval),
  store.readLocal(prefixes.orderIntent)
 ]);
 life.assert();
 rfqs=r.filter(row=>row.value?.schema==='bonfyre.procurement.supplier-request.v1').sort((x,y)=>x.key.localeCompare(y.key));
 quotes=q.filter(row=>row.value?.schema==='bonfyre.procurement.supplier-quote-observation.v1').sort((x,y)=>x.key.localeCompare(y.key));
 comparisons=c.filter(row=>row.value?.schema==='bonfyre.procurement.quote-comparison.v1').sort((x,y)=>x.key.localeCompare(y.key));
 allocations=a.filter(row=>row.value?.schema==='bonfyre.procurement.allocation-plan.v1').sort((x,y)=>x.key.localeCompare(y.key));
 approvals=p.filter(row=>row.value?.schema==='bonfyre.procurement.approval-request.v1').sort((x,y)=>x.key.localeCompare(y.key));
 orderIntents=o.filter(row=>row.value?.schema==='bonfyre.procurement.order-intent.v1').sort((x,y)=>x.key.localeCompare(y.key));
 choices();
}

function choices(){
 const purchaseId=selectedPurchaseId();
 const rfqRows=purchaseId?rfqs.filter(row=>row.value?.source?.purchase_list_id===purchaseId):rfqs;
 const rfq=ui('native-rfq-saved'),quote=ui('native-quote-saved'),comparison=ui('native-comparison-saved'),allocation=ui('native-allocation-saved'),approval=ui('native-approval-saved'),orderIntent=ui('native-order-saved');
 const oldRfq=rfq.value,oldQuote=quote.value,oldComparison=comparison.value,oldAllocation=allocation.value,oldApproval=approval.value,oldOrderIntent=orderIntent.value;
 rfq.replaceChildren(new Option('No supplier request selected',''));
 for(const row of rfqRows)rfq.add(new Option((row.value.supplier?.name||row.value.supplier?.id||'Supplier not selected')+' · '+row.value.id+' · version '+row.revision,row.key));
 quote.replaceChildren(new Option('No quote selected',''));
 for(const row of (rfqValue?quotes.filter(item=>item.value?.source?.rfq_id===rfqValue.id):[]))quote.add(new Option((row.value.supplier?.name||row.value.supplier?.id||'Supplier unknown')+' · '+row.value.id+' · version '+row.revision,row.key));
 comparison.replaceChildren(new Option('No comparison selected',''));
 for(const row of (rfqValue?comparisons.filter(item=>item.value?.source?.rfq_id===rfqValue.id):[]))comparison.add(new Option(row.value.status+' · version '+row.revision,row.key));
 allocation.replaceChildren(new Option('No allocation selected',''));
 for(const row of (rfqValue?allocations.filter(item=>item.value?.source?.rfq_id===rfqValue.id):[]))allocation.add(new Option(row.value.status+' · '+row.value.id+' · version '+row.revision,row.key));
 approval.replaceChildren(new Option('No approval request selected',''));
 for(const row of (allocationValue?approvals.filter(item=>item.value?.source?.allocation_id===allocationValue.id):[]))approval.add(new Option(row.value.status+' · '+row.value.id+' · version '+row.revision,row.key));
 orderIntent.replaceChildren(new Option('No order intent selected',''));
 for(const row of (approvalValue?orderIntents.filter(item=>item.value?.source?.approval_id===approvalValue.id):[]))orderIntent.add(new Option(row.value.status+' · '+row.value.id+' · version '+row.revision,row.key));
 if(rfqRows.some(row=>row.key===oldRfq))rfq.value=oldRfq;
 if(quotes.some(row=>row.key===oldQuote))quote.value=oldQuote;
 if(comparisons.some(row=>row.key===oldComparison))comparison.value=oldComparison;
 if(allocations.some(row=>row.key===oldAllocation))allocation.value=oldAllocation;
 if(approvals.some(row=>row.key===oldApproval))approval.value=oldApproval;
 if(orderIntents.some(row=>row.key===oldOrderIntent))orderIntent.value=oldOrderIntent;
 controls();
}

function comparisonQuotes(){
 if(!comparisonValue||!Array.isArray(comparisonValue.quotes))return [];
 const result=[];
 for(const ref of comparisonValue.quotes){
  const row=quotes.find(item=>item.value?.id===ref.quote_id&&item.value?.digest===ref.digest);
  if(!row)return [];
  result.push(row);
 }
 return result;
}

function selectedPurchaseId(){
 const key=ui('saved')?.value||new URL(location.href).searchParams.get('list')||'';
 return key.startsWith(prefixes.purchase)?key.slice(prefixes.purchase.length):'';
}

async function selectedPurchase(){
 const key=ui('saved')?.value||new URL(location.href).searchParams.get('list');
 if(!key||!key.startsWith(prefixes.purchase))throw Error('Select and save a purchase list before preparing a supplier request.');
 const rows=await store.readLocal(key);
 life.assert();
 const row=rows.find(item=>item.key===key);
 if(!row||row.value?.schema!=='service-site.room-purchase-list.v1')throw Error('The selected purchase list is unavailable.');
 const sourceRows=await store.readLocal(row.value.source.key);
 life.assert();
 const source=sourceRows.find(item=>item.key===row.value.source.key);
 if(!source||source.revision!==row.value.source.revision||await releaseDigest(source.value)!==row.value.source.sha256)throw Error('The source room changed or is unavailable. Refresh and save the purchase list before continuing.');
 return row;
}

async function saveDigest(key,value,expectedRevision=0){
 try{
  const saved=await store.saveLocal(key,value,expectedRevision);
  life.assert();
  return saved;
 }catch(error){
  const rows=await store.readLocal(key).catch(()=>[]);
  life.assert();
  const current=rows.find(row=>row.key===key);
  if(!current||current.value?.digest!==value.digest)throw error;
  message('The save reply was lost; retained readback confirmed version '+current.revision+'.');
  return current;
 }
}

function setQuery(name,key){
 const url=new URL(location.href);
 if(key)url.searchParams.set(name,key);else url.searchParams.delete(name);
 history.replaceState(null,'',url);
}

function reset(){
 rfqValue=null;rfqRevision=0;comparisonValue=null;comparisonCurrent=false;allocationValue=null;allocationRevision=0;approvalValue=null;approvalRevision=0;approvalStatus=null;orderIntentValue=null;orderIntentRevision=0;protocolPreflightValue=null;enactmentReadinessStatus=null;enactmentReceipt=null;downstreamStatus=null;
 ui('native-rfq-saved').value='';
 ui('native-rfq-review').replaceChildren();ui('native-rfq-review').hidden=true;
 ui('native-quote-section').hidden=true;
 ui('native-comparison-section').hidden=true;
 ui('native-comparison-review').hidden=true;
 ui('native-allocation-section').hidden=true;
 ui('native-allocation-review').hidden=true;
 ui('native-approval-section').hidden=true;ui('native-approval-review').hidden=true;ui('native-authority-status').hidden=true;ui('native-order-section').hidden=true;ui('native-order-review').hidden=true;ui('native-protocol-section').hidden=true;ui('native-protocol-review').hidden=true;ui('native-enactment-status').hidden=true;ui('native-supplier-acceptance').hidden=true;ui('native-enactment-result').hidden=true;ui('native-enactment-signature').value='';ui('native-enactment-accepted-at').value='';ui('native-downstream-status').hidden=true;
 choices();
}

function renderQuoteLines(){
 const host=ui('native-quote-lines');
 host.replaceChildren();
 if(!rfqValue)return;
 for(const line of rfqValue.lines){
  const field=el('fieldset');
  field.className='rp-quote-line';
  field.dataset.lineId=line.line_id;
  const quantity=el('input'),unit=el('input'),availability=el('input'),lead=el('input');
  quantity.dataset.field='quantity';quantity.value=String(line.quantity_requested);quantity.inputMode='numeric';
  unit.dataset.field='unit';unit.inputMode='numeric';
  availability.dataset.field='availability';
  lead.dataset.field='lead';
  field.append(el('legend',line.label+' · '+line.option),el('label','Quantity'),quantity,el('label','Quoted unit cents (blank if unknown)'),unit,el('label','Availability statement (optional)'),availability,el('label','Lead time (optional)'),lead);
  host.append(field);
 }
}

function renderRFQ(){
 if(!rfqValue){reset();return;}
 const host=ui('native-rfq-review');
 host.replaceChildren();
 host.hidden=false;
 host.append(el('h3','Supplier request draft'),el('p','Purchase list '+rfqValue.source.purchase_list_id+' · version '+rfqValue.source.revision));
 host.append(el('p',rfqValue.supplier?.name||rfqValue.supplier?.id||'Supplier not selected'));
 const table=el('table'),head=el('tr');
 for(const title of ['Product and option','Requested quantity','Listed unit price'])head.append(el('th',title));
 const thead=el('thead');thead.append(head);table.append(thead);
 const body=el('tbody');
 for(const line of rfqValue.lines){
  const tr=el('tr');
  tr.append(el('td',line.label+' · '+line.option),el('td',String(line.quantity_requested)),el('td',String(line.listed_unit_cents)+'¢'));
  body.append(tr);
 }
 table.append(body);
 const wrap=el('div');wrap.className='rp-table';wrap.append(table);
 host.append(wrap,el('p','Native digest '+rfqValue.digest.slice(0,16)+'…'),el('p','This draft has not contacted a supplier and does not approve or place an order.'));
 ui('native-quote-section').hidden=false;
 ui('native-comparison-section').hidden=false;
 renderQuoteLines();
 choices();
}

function recheckComparison(saved){
 const currentRfq=rfqs.find(row=>row.value?.id===saved.source?.rfq_id)?.value;
 if(!currentRfq)return {...saved,status:'stale',stale:true,reason:'The bound supplier request is unavailable.'};
 const expected=(saved.quotes??[]).map(row=>({id:row.quote_id,digest:row.digest})),currentQuotes=[];
 for(const ref of expected){
  const row=quotes.find(item=>item.value?.id===ref.id);
  if(!row)return {...saved,status:'stale',stale:true,reason:'A bound quote observation is unavailable.'};
  currentQuotes.push(row.value);
 }
 try{
  const current=kernel.compareQuotes({rfq:currentRfq,quotes:currentQuotes,expected_rfq_digest:saved.source.rfq_digest,expected_quotes:expected},{asOf:utcSecond()});
  return {...current,reason:current.stale?'A bound supplier request or quote revision changed.':''};
 }catch(error){
  return {...saved,status:'stale',stale:true,reason:'Current source material no longer satisfies the saved comparison: '+(error.message||String(error))};
 }
}

function renderComparison(saved){
 const current=recheckComparison(saved),host=ui('native-comparison-review');
 comparisonValue=saved;comparisonCurrent=current.status==='current'&&!current.stale;
 host.replaceChildren();
 host.hidden=false;
 host.append(el('h4','Quote comparison'),el('p','Current status: '+current.status),el('p','Saved status: '+saved.status));
 host.append(el('p','Quotes: '+(current.quotes?.length??0)+' · eligible now: '+(current.eligible_quote_count??'?')+' · expired now: '+(current.expired_quote_count??'?')+' · currencies: '+(current.currencies??[]).join(', ')),el('p','Saved digest '+saved.digest.slice(0,16)+'…'));
 if(current.reason)host.append(el('p',current.reason));
 host.append(el('p','Rechecking does not accept a quote, approve a purchase, reserve inventory, place an order, pay, settle, fulfill or ship.'));
 ui('native-allocation-section').hidden=false;
 controls();
}

function recheckAllocation(saved){
 const comparison=comparisons.find(row=>row.value?.digest===saved.source?.comparison_digest);
 if(!comparison)return {current:false,reason:'The bound quote comparison is unavailable.'};
 const checked=recheckComparison(comparison.value);
 if(checked.status!=='current'||checked.stale)return {current:false,reason:'The bound quote comparison is no longer current.'};
 for(const allocation of saved.allocations??[]){
  const quote=quotes.find(row=>row.value?.id===allocation.quote_id&&row.value?.digest===allocation.quote_digest);
  if(!quote)return {current:false,reason:'A quote used by this allocation changed or is unavailable.'};
 }
 return {current:true,reason:''};
}

function renderAllocation(saved){
 allocationValue=saved;
 const state=recheckAllocation(saved),host=ui('native-allocation-review');
 host.replaceChildren();host.hidden=false;
 host.append(el('h4','Allocation plan'),el('p','Current source status: '+(state.current?'current':'stale')),el('p','Policy: lowest known exact-line merchandise'));
 const table=el('table'),head=el('tr');
 for(const title of ['Supplier','RFQ line','Quantity','Unit cents','Merchandise cents'])head.append(el('th',title));
 const thead=el('thead');thead.append(head);table.append(thead);const body=el('tbody');
 for(const row of saved.allocations??[]){
  const tr=el('tr');tr.append(el('td',row.supplier?.name||row.supplier?.id||'Supplier'),el('td',row.rfq_line_id),el('td',String(row.quantity)),el('td',String(row.unit_cents)),el('td',String(row.merchandise_cents)));body.append(tr);
 }
 table.append(body);const wrap=el('div');wrap.className='rp-table';wrap.append(table);host.append(wrap);
 host.append(el('p','Known merchandise: '+saved.known_merchandise_cents+' '+saved.currency+' cents. '+(saved.known_total_cents===null?'Exact total unresolved because at least one selected quote is only partially consumed or has unattributed quote-level fees.':'Exact attributed total: '+saved.known_total_cents+' '+saved.currency+' cents · fee attribution: '+saved.fees_attribution+'.')));
 host.append(el('p','CommerceChain: '+saved.commerce_chain?.status+' · next required stage: '+saved.commerce_chain?.next_required_stage+'. This plan has not accepted a quote, approved a purchase, created a cart or commitment, placed an order, paid, settled, fulfilled or shipped.'));
 if(state.reason)host.append(el('p',state.reason));
 ui('native-approval-section').hidden=!(allocationRevision>0&&state.current);
 controls();
}

function renderApproval(saved){
 approvalValue=saved;
 const allocation=allocations.find(row=>row.value?.id===saved.source?.allocation_id&&row.value?.digest===saved.source?.digest);
 const current=Boolean(allocation&&allocation.revision===saved.source?.revision&&recheckAllocation(allocation.value).current);
 const host=ui('native-approval-review');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Approval request'),el('p','Status: '+saved.status+' · source allocation: '+(current?'current':'stale')));
 host.append(el('p','Authority owner: '+saved.authority?.owner+' · required permissions: '+(saved.authority?.required_permissions??[]).join(' + ')+' · decision: '+saved.authority?.decision));
 host.append(el('p','Known merchandise: '+saved.known_merchandise_cents+' '+saved.currency+' cents. '+(saved.known_total_cents===null?'Exact spend total remains unresolved; this approval cannot become order-intent eligible.':'Exact spend total: '+saved.known_total_cents+' '+saved.currency+' cents · '+saved.fees_attribution+'.')));
 if(saved.reason)host.append(el('p','Reason: '+saved.reason));
 host.append(el('p','This request has not approved spend, accepted a quote, opened a commitment, created an order or presented payment.'));
 controls();
}

async function reconcileApprovalStatus(key){
 const url=new URL('../_approval_status',location.href);url.searchParams.set('key',key);
 const response=await fetch(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal:life.signal});
 const bytes=new Uint8Array(await response.arrayBuffer());life.assert();
 if(bytes.byteLength>8192)throw Error('Authority reconciliation exceeded its response bound.');
 if(!response.ok)throw Error(new TextDecoder().decode(bytes)||'Authority reconciliation is unavailable.');
 const value=decode(bytes);
 if(!Array.isArray(value)||value.length!==2||value[0]!==1||!value[1]||typeof value[1]!=='object')throw Error('Authority reconciliation returned an invalid envelope.');
 return value[1];
}

function renderAuthorityStatus(status){
 approvalStatus=status;const host=ui('native-authority-status');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Authority reconciliation'),el('p','State: '+status.state),el('p','Decision: '+(status.outcome||'pending')+' · assent: '+(status.assent_present?'yes':'no')+' · approve authority: '+(status.approve_authority?'yes':'no')+' · spend authority: '+(status.spend_authority?'yes':'no')+' · exact amount: '+(status.amount_known?status.known_total_cents+' cents':'unresolved')));
 const proof=el('details');proof.open=true;proof.dataset.governedProof='';proof.append(el('summary','Exact decision and authority gates'));
 const identity=el('p','Request: '+(status.record_key||'unavailable')+' · revision '+(status.revision??'unknown')+' · SHA-256 '+(status.body_sha256||'unavailable'));identity.className='rp-proof-identity';proof.append(identity);
 const gates=el('ol');gates.className='rp-proof-gates';
 for(const [field,label] of [['assent_present','Exact signed human assent'],['approve_authority','AuthorityGraph approval grant'],['spend_authority','AuthorityGraph spend grant'],['amount_known','Fully attributed spend amount'],['eligible_for_order_intent','Eligible only for order-intent preparation']]){
  const row=el('li'),passed=status[field]===true;row.dataset.gate=field;row.dataset.verified=String(passed);
  row.append(el('strong',passed?'Verified':'Not verified'),el('span',label));gates.append(row);
 }
 proof.append(gates);
 if(status.decision_receipt)proof.append(el('p','Governance receipt: '+status.decision_receipt));
 host.append(proof);
 if(status.decided_by)host.append(el('p','Decided by '+status.decided_by+' at '+status.decided_at+' · decision '+status.decision_id));
 host.append(el('p',status.cannot_infer||'Eligibility does not create a purchase order or payment.'));
 ui('native-order-section').hidden=!(status.state==='eligible'&&status.eligible_for_order_intent===true);
 controls();
}

function renderProtocolActorInputs(){
 const host=ui('native-protocol-lots');host.replaceChildren();
 if(!orderIntentValue)return;
 for(const [index,lot] of (orderIntentValue.order_lots??[]).entries()){
  const field=el('fieldset');field.dataset.quoteId=lot.quote_id;
  const supplier=lot.supplier?.name||lot.supplier?.id||'Supplier observation '+(index+1);
  field.append(el('legend',supplier+' · '+lot.quote_id+' · '+lot.known_total_cents+' '+orderIntentValue.currency+' cents'));
  for(const [name,label,placeholder] of [['seller','Seller actor','seller:explicit-subject'],['settlement','Settlement actor','settlement:explicit-subject'],['deliverer','Deliverer actor','deliverer:explicit-subject'],['capability','Capability reference','procurement-capability:exact-scope']]){
   const input=el('input');input.dataset.field=name;input.maxLength=name==='capability'?512:256;input.placeholder=placeholder;field.append(el('label',label),input);
  }
  host.append(field);
 }
}
function renderEnactmentChoices(){
 const section=ui('native-supplier-acceptance'),select=ui('native-enactment-contract'),rows=enactableSupplierContracts(protocolPreflightValue),prior=select.value;
 select.replaceChildren(new Option(rows.length?'Choose exact supplier lot':'No cryptographically bound supplier lot available',''));
 for(const contract of rows)select.append(new Option((contract.supplier_observation?.name||contract.supplier_observation?.id||contract.quote_id)+' · '+contract.quoted_amount_cents+' '+contract.currency+' cents',contract.exchange_id));
 if(rows.some(row=>row.exchange_id===prior))select.value=prior;
 section.hidden=false;
 if(!rows.length){const result=ui('native-enactment-result');result.replaceChildren(el('p','No preflight lot binds a retained supplier ID to the same ed25519 seller actor. Supplier acceptance cannot be imported for these lots.'));result.hidden=false;}
 controls();
}

function renderProtocolPreflight(value){
 protocolPreflightValue=value;enactmentReceipt=null;const host=ui('native-protocol-review');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Effect-free ProtocolContract preflight'),el('p','Protocol owner: '+value.protocol_owner+' · commitment owner: '+value.commitment_owner+' · effect free: '+value.effect_free));
 for(const contract of value.contracts??[]){
  const box=el('fieldset');box.append(el('legend',(contract.supplier_observation?.name||contract.supplier_observation?.id||'Supplier')+' · '+contract.quoted_amount_cents+' '+contract.currency+' cents'));
  box.append(el('p','Contract: '+contract.contract_name+' · '+contract.status+' · exchange '+contract.exchange_id));
  box.append(el('p','Actors proposed — buyer '+contract.proposed_actors?.buyer+' · seller '+contract.proposed_actors?.seller+' · settlement '+contract.proposed_actors?.settlement+' · deliverer '+contract.proposed_actors?.deliverer));
  box.append(el('p','Authority required: '+(contract.authority_requirements??[]).map(row=>row.role+' → '+row.permission).join(' · ')));
  box.append(el('p','Supplier actor binding verified: '+contract.supplier_actor_binding_verified+' · authority verified: '+contract.authority_verified+' · supplier acceptance present: '+contract.supplier_acceptance_present+' · protocol enacted: '+contract.protocol_enacted+' · commitment opened: '+contract.commitment_opened));
  box.append(el('p',contract.does_not_establish));host.append(box);
 }
 host.append(el('p',value.does_not_establish));renderEnactmentChoices();controls();
}
function renderOrderIntent(saved){
 orderIntentValue=saved;protocolPreflightValue=null;enactmentReadinessStatus=null;enactmentReceipt=null;downstreamStatus=null;const host=ui('native-order-review');host.replaceChildren();host.hidden=false;ui('native-enactment-status').hidden=true;ui('native-supplier-acceptance').hidden=true;ui('native-enactment-result').hidden=true;ui('native-enactment-signature').value='';ui('native-enactment-accepted-at').value='';ui('native-downstream-status').hidden=true;ui('native-protocol-review').hidden=true;
 host.append(el('h4','Order intent proposal'),el('p','Status: '+saved.status+' · authority eligible: '+(saved.authority_eligible?'yes':'no')+' · exact spend total: '+saved.known_total_cents+' '+saved.currency+' cents'));
 host.append(el('p','Authority decision: '+saved.authority_reconciliation?.outcome+' · '+saved.authority_reconciliation?.decided_by+' · '+saved.authority_reconciliation?.decided_at));
 host.append(el('p','CommitmentGraph handoff: '+saved.commitment_handoff?.status+' · proposed resource '+saved.commitment_handoff?.proposed_resource));
 host.append(el('p','Quote accepted: '+saved.quote_accepted+' · commitment opened: '+saved.commitment_opened+' · purchase order issued: '+saved.purchase_order_issued+' · payment presented: '+saved.payment_presented+' · shipped: '+saved.shipped));
 host.append(el('p',saved.does_not_establish));ui('native-protocol-section').hidden=false;renderProtocolActorInputs();
 controls();
}

async function enactmentReadiness(key){
 const url=new URL('../_procurement_enactment_readiness',location.href);url.searchParams.set('key',key);
 const response=await fetch(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal:life.signal});
 const bytes=new Uint8Array(await response.arrayBuffer());life.assert();
 if(bytes.byteLength>16384)throw Error('Procurement enactment readiness exceeded its response bound.');
 if(!response.ok)throw Error(new TextDecoder().decode(bytes)||'Procurement enactment readiness is unavailable.');
 const value=decode(bytes);
 if(!Array.isArray(value)||value.length!==2||value[0]!==1||!value[1]||typeof value[1]!=='object')throw Error('Procurement enactment readiness returned an invalid envelope.');
 return value[1];
}

function renderEnactmentReadiness(status){
 enactmentReadinessStatus=status;const host=ui('native-enactment-status');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Enactment readiness'),el('p','State: '+status.state));
 host.append(el('p','Native lineage attested: '+(status.native_lineage_attested?'yes':'no')+' · buyer approval eligible: '+(status.approval_eligible?'yes':'no')));
 if(status.order_digest)host.append(el('p','Exact order digest: '+status.order_digest));
 host.append(el('p','Supplier acceptance present: '+(status.supplier_acceptance_present?'yes':'no')));
 host.append(el('p',status.cannot_infer));controls();
}

async function enactSupplierAcceptance(key,contract,signature,acceptedAt){
 const body=encodeSupplierAcceptance({revision:orderIntentRevision,orderDigest:orderIntentValue?.digest,contract,signature,acceptedAt});
 const url=new URL('../_procurement_enactment',location.href);url.searchParams.set('key',key);
 const response=await fetch(url,{method:'POST',headers:{'Content-Type':'application/msgpack'},body,credentials:'same-origin',cache:'no-store',redirect:'error',signal:life.signal});
 const bytes=new Uint8Array(await response.arrayBuffer());life.assert();
 if(bytes.byteLength>16384)throw Error('Procurement enactment reply exceeded its response bound.');
 if(!response.ok)throw Error(new TextDecoder().decode(bytes)||'Signed supplier acceptance was refused.');
 const receipt=decodeSupplierEnactment(bytes);
 if(receipt.record_key!==key||receipt.revision!==orderIntentRevision||receipt.order_digest!==orderIntentValue.digest||receipt.quote_id!==contract.quote_id||receipt.exchange!==contract.exchange_id)throw Error('Procurement enactment receipt does not bind this exact saved lot.');
 return receipt;
}

function renderEnactmentReceipt(receipt){
 enactmentReceipt=receipt;const host=ui('native-enactment-result');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Supplier acceptance receipt'),el('p','State: '+receipt.state+' · supplier '+receipt.supplier_id),el('p','Quote '+receipt.quote_id+' · exchange '+receipt.exchange+' · accepted '+receipt.accepted_at));
 host.append(el('p','Supplier identity binding: verified · detached acceptance: verified · ProtocolContract enacted: yes · CommitmentGraph opened: yes'));
 host.append(el('p',receipt.cannot_infer||'No purchase order, payable, payment, settlement or shipment is established.'));
 controls();
}

async function reconcileDownstreamOwners(key){
 const url=new URL('../_procurement_downstream',location.href);url.searchParams.set('key',key);
 const response=await fetch(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal:life.signal});
 const bytes=new Uint8Array(await response.arrayBuffer());life.assert();
 if(bytes.byteLength>16384)throw Error('Downstream procurement reconciliation exceeded its response bound.');
 if(!response.ok)throw Error(new TextDecoder().decode(bytes)||'Downstream procurement reconciliation is unavailable.');
 const value=decode(bytes);
 if(!Array.isArray(value)||value.length!==2||value[0]!==1||!value[1]||typeof value[1]!=='object')throw Error('Downstream procurement reconciliation returned an invalid envelope.');
 return value[1];
}

function renderDownstreamStatus(status){
 downstreamStatus=status;const host=ui('native-downstream-status');host.replaceChildren();host.hidden=false;
 host.append(el('h4','Downstream owner reconciliation'),el('p','State: '+status.state));
 host.append(el('p','CommitmentGraph: '+(status.commitment_found?status.commitment_state+' · '+(status.commitment_actor||'actor unavailable'):'not materialized')));
 host.append(el('p','CommerceChain furthest stage: '+(status.commerce_furthest||'none established')));
 if(status.established?.length){
  const list=el('ul');for(const row of status.established)list.append(el('li',row.stage+' · '+row.owner+' · does not establish '+row.does_not_establish));host.append(el('h5','Established stages'),list);
 }
 if(status.outstanding?.length){
  const list=el('ul');for(const row of status.outstanding)list.append(el('li',row.stage+' · owner '+row.owner));host.append(el('h5','Still outstanding'),list);
 }
 host.append(el('p',status.cannot_infer));
 controls();
}

installPanel();

function listen(name,event,fn){
 ui(name).addEventListener(event,()=>run(fn),{signal:life.signal});
}

listen('native-rfq-prepare','click',async()=>{
 const purchase=await selectedPurchase();
 const supplierId=clean(ui('native-supplier-id').value),supplierName=clean(ui('native-supplier-name').value);
 const supplier=supplierId||supplierName?{...(supplierId?{id:supplierId}:{}),...(supplierName?{name:supplierName}:{})}:null;
 rfqValue=kernel.prepareRFQ({purchase_list:purchase.value,purchase_list_revision:purchase.revision,id:'rfq-'+crypto.randomUUID(),requested_at:utcSecond(),supplier,selected:null});
 rfqRevision=0;
 ui('native-rfq-saved').value='';
 renderRFQ();
 message('Supplier request prepared locally from purchase-list version '+purchase.revision+'. Nothing was sent.');
});

listen('native-rfq-save','click',async()=>{
 if(!rfqValue)throw Error('Prepare a supplier request first.');
 const key=prefixes.rfq+rfqValue.id,row=await saveDigest(key,rfqValue,rfqRevision);
 life.assert();
 rfqValue=row.value;rfqRevision=row.revision;
 await readAll();
 ui('native-rfq-saved').value=key;
 setQuery('rfq',key);
 renderRFQ();
 message('Supplier request saved as version '+rfqRevision+'. No supplier was contacted.');
});

listen('native-rfq-saved','change',async()=>{
 const key=ui('native-rfq-saved').value;
 if(!key){reset();setQuery('rfq','');return;}
 const rows=await store.readLocal(key);
 life.assert();
 const row=rows.find(item=>item.key===key);
 if(!row)throw Error('This supplier request is unavailable.');
 rfqValue=row.value;rfqRevision=row.revision;
 setQuery('rfq',key);
 renderRFQ();
});

listen('native-quote-save','click',async()=>{
 if(!rfqValue)throw Error('Open a supplier request first.');
 const supplierId=clean(ui('native-quote-supplier-id').value),supplierName=clean(ui('native-quote-supplier-name').value);
 const supplier=supplierId||supplierName?{...(supplierId?{id:supplierId}:{}),...(supplierName?{name:supplierName}:{})}:rfqValue.supplier;
 const lines=[...ui('native-quote-lines').querySelectorAll('fieldset')].map(row=>({
  id:'quote-line-'+crypto.randomUUID(),
  rfq_line_id:row.dataset.lineId,
  quantity:integer(row.querySelector('[data-field="quantity"]').value,{positive:true}),
  unit_cents:integer(row.querySelector('[data-field="unit"]').value,{nullable:true}),
  line_total_cents:null,
  substitution:null,
  availability:clean(row.querySelector('[data-field="availability"]').value)||null,
  lead_time:clean(row.querySelector('[data-field="lead"]').value)||null
 }));
 const observed=kernel.observeQuote({rfq:rfqValue,observation:{
  id:'quote-'+crypto.randomUUID(),
  supplier:supplier||null,
  supplier_quote_id:clean(ui('native-supplier-quote-id').value)||null,
  observed_at:utcSecond(),
  quoted_at:null,
  valid_until:exactUtc(ui('native-valid-until').value),
  currency:(clean(ui('native-currency').value)||'USD').toUpperCase(),
  source_ref:clean(ui('native-source-ref').value),
  freight_cents:integer(ui('native-freight').value,{nullable:true}),
  tax_cents:integer(ui('native-tax').value,{nullable:true}),
  other_fees_cents:integer(ui('native-fees').value,{nullable:true}),
  lines
 }});
 const key=prefixes.quote+observed.id,row=await saveDigest(key,observed,0);
 life.assert();
 await readAll();
 ui('native-quote-saved').value=key;
 setQuery('quote',key);
 message('Quote observation saved as version '+row.revision+'. It has not been accepted.');
});

listen('native-quote-saved','change',async()=>{
 const key=ui('native-quote-saved').value;
 if(!key){setQuery('quote','');return;}
 const rows=await store.readLocal(key);
 life.assert();
 const row=rows.find(item=>item.key===key);
 if(!row)throw Error('This quote observation is unavailable.');
 setQuery('quote',key);
 message('Retained quote '+row.value.id+' · version '+row.revision+'.');
});

listen('native-compare-save','click',async()=>{
 if(!rfqValue)throw Error('Open a supplier request first.');
 const current=matchingQuotes();
 if(!current.length)throw Error('Retain at least one quote observation for this exact supplier request.');
 const comparison=kernel.compareQuotes({rfq:rfqValue,quotes:current.map(row=>row.value),expected_rfq_digest:rfqValue.digest,expected_quotes:current.map(row=>({id:row.value.id,digest:row.value.digest}))},{asOf:utcSecond()});
 const key=prefixes.comparison+rfqValue.id+':'+crypto.randomUUID(),row=await saveDigest(key,comparison,0);
 life.assert();
 await readAll();
 ui('native-comparison-saved').value=key;
 setQuery('comparison',key);
 allocationValue=null;allocationRevision=0;ui('native-allocation-saved').value='';ui('native-allocation-review').hidden=true;
 renderComparison(row.value);
 message('Quote comparison saved as version '+row.revision+'.');
});

listen('native-comparison-saved','change',async()=>{
 const key=ui('native-comparison-saved').value;
 if(!key){ui('native-comparison-review').hidden=true;setQuery('comparison','');return;}
 const rows=await store.readLocal(key);
 life.assert();
 const row=rows.find(item=>item.key===key);
 if(!row)throw Error('This comparison is unavailable.');
 setQuery('comparison',key);
 allocationValue=null;allocationRevision=0;ui('native-allocation-saved').value='';ui('native-allocation-review').hidden=true;
 renderComparison(row.value);
});

listen('native-allocation-prepare','click',async()=>{
 if(!rfqValue||!comparisonValue)throw Error('Open a retained current comparison first.');
 const current=recheckComparison(comparisonValue);
 if(current.status!=='current'||current.stale)throw Error('This comparison is no longer current enough to allocate.');
 const selected=comparisonQuotes();if(!selected.length)throw Error('The exact retained quote set for this comparison is unavailable.');
 allocationValue=kernel.allocateQuotes({rfq:rfqValue,quotes:selected.map(row=>row.value),comparison:comparisonValue,id:'allocation-'+crypto.randomUUID(),prepared_at:utcSecond(),policy:'lowest_known_merchandise'});
 allocationRevision=0;ui('native-allocation-saved').value='';renderAllocation(allocationValue);
 message('Allocation plan prepared locally. Quote-level freight, tax and other fees remain unattributed; nothing was approved or ordered.');
});

listen('native-allocation-save','click',async()=>{
 if(!allocationValue)throw Error('Prepare an allocation plan first.');
 const key=prefixes.allocation+allocationValue.id,row=await saveDigest(key,allocationValue,allocationRevision);
 life.assert();allocationValue=row.value;allocationRevision=row.revision;approvalValue=null;approvalRevision=0;await readAll();ui('native-allocation-saved').value=key;ui('native-approval-saved').value='';setQuery('allocation',key);setQuery('approval','');renderAllocation(row.value);
 message('Allocation plan saved as version '+row.revision+'. No quote was accepted and no commerce commitment was created.');
});

listen('native-allocation-saved','change',async()=>{
 const key=ui('native-allocation-saved').value;
 if(!key){allocationValue=null;allocationRevision=0;ui('native-allocation-review').hidden=true;setQuery('allocation','');return;}
 const rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);if(!row)throw Error('This allocation plan is unavailable.');
 allocationValue=row.value;allocationRevision=row.revision;setQuery('allocation',key);
 const comparison=comparisons.find(item=>item.value?.digest===row.value?.source?.comparison_digest);
 if(comparison){comparisonValue=comparison.value;ui('native-comparison-saved').value=comparison.key;renderComparison(comparison.value);}
 renderAllocation(row.value);
});

listen('native-approval-prepare','click',async()=>{
 if(!allocationValue||allocationRevision<1)throw Error('Open a saved allocation plan first.');
 const state=recheckAllocation(allocationValue);if(!state.current)throw Error('This allocation is stale and cannot be submitted for approval review.');
 const requester=clean(ui('native-approval-requester').value);if(!requester)throw Error('Name the requester for this approval review.');
 approvalValue=kernel.prepareApprovalRequest({allocation:allocationValue,allocation_revision:allocationRevision,id:'approval-'+crypto.randomUUID(),requested_at:utcSecond(),requester,reason:clean(ui('native-approval-reason').value)||null});
 approvalRevision=0;approvalStatus=null;orderIntentValue=null;orderIntentRevision=0;ui('native-approval-saved').value='';ui('native-authority-status').hidden=true;ui('native-order-section').hidden=true;ui('native-order-review').hidden=true;renderApproval(approvalValue);
 message('Approval request prepared locally. AuthorityGraph has not approved it and no spend authority was granted.');
});

listen('native-approval-save','click',async()=>{
 if(!approvalValue)throw Error('Prepare an approval request first.');
 const key=prefixes.approval+approvalValue.id,row=await saveDigest(key,approvalValue,approvalRevision);
 life.assert();approvalValue=row.value;approvalRevision=row.revision;approvalStatus=null;orderIntentValue=null;orderIntentRevision=0;await readAll();ui('native-approval-saved').value=key;setQuery('approval',key);setQuery('orderIntent','');ui('native-authority-status').hidden=true;ui('native-order-section').hidden=true;ui('native-order-review').hidden=true;renderApproval(row.value);
 message('Approval request saved as version '+row.revision+'. Its AuthorityGraph decision remains pending.');
});

listen('native-approval-saved','change',async()=>{
 const key=ui('native-approval-saved').value;
 if(!key){approvalValue=null;approvalRevision=0;ui('native-approval-review').hidden=true;setQuery('approval','');return;}
 const rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);if(!row)throw Error('This approval request is unavailable.');
 const allocation=allocations.find(item=>item.value?.id===row.value?.source?.allocation_id&&item.value?.digest===row.value?.source?.digest);
 if(allocation){allocationValue=allocation.value;allocationRevision=allocation.revision;ui('native-allocation-saved').value=allocation.key;renderAllocation(allocation.value);}
 approvalValue=row.value;approvalRevision=row.revision;approvalStatus=null;orderIntentValue=null;orderIntentRevision=0;setQuery('approval',key);setQuery('orderIntent','');ui('native-authority-status').hidden=true;ui('native-order-section').hidden=true;ui('native-order-review').hidden=true;renderApproval(row.value);
});

listen('native-approval-reconcile','click',async()=>{
 if(!approvalValue||approvalRevision<1)throw Error('Open a saved approval request first.');
 const key=prefixes.approval+approvalValue.id,rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);
 if(!row||row.revision!==approvalRevision||row.value?.digest!==approvalValue.digest)throw Error('The approval request changed before authority reconciliation.');
 const status=await reconcileApprovalStatus(key);life.assert();
 if(status.revision!==approvalRevision||status.record_key!==key)throw Error('Authority reconciliation does not bind this saved approval version.');
 renderAuthorityStatus(status);
 message(status.state==='eligible'?'Approval reconciled as eligible for an order-intent proposal. No quote was accepted and no commitment or order was created.':'Approval reconciliation state: '+status.state+'. No order intent is available.');
});

listen('native-order-prepare','click',async()=>{
 if(!approvalValue||approvalRevision<1||approvalStatus?.state!=='eligible'||approvalStatus?.eligible_for_order_intent!==true)throw Error('Reconcile an eligible saved approval request first.');
 if(!allocationValue||!recheckAllocation(allocationValue).current)throw Error('The approved allocation source is stale.');
 orderIntentValue=kernel.prepareOrderIntent({allocation:allocationValue,allocation_revision:allocationRevision,approval:approvalValue,approval_revision:approvalRevision,authority_status:approvalStatus,id:'order-intent-'+crypto.randomUUID(),prepared_at:utcSecond()});
 orderIntentRevision=0;protocolPreflightValue=null;ui('native-order-saved').value='';renderOrderIntent(orderIntentValue);
 message('Order intent prepared from verified authority eligibility. No quote was accepted, commitment opened, purchase order issued or payment presented.');
});

listen('native-order-save','click',async()=>{
 if(!orderIntentValue)throw Error('Prepare an order intent first.');
 const key=prefixes.orderIntent+orderIntentValue.id,row=await saveDigest(key,orderIntentValue,orderIntentRevision);
 life.assert();orderIntentValue=row.value;orderIntentRevision=row.revision;protocolPreflightValue=null;downstreamStatus=null;await readAll();ui('native-order-saved').value=key;setQuery('orderIntent',key);renderOrderIntent(row.value);
 message('Order intent saved as version '+row.revision+'. CommitmentGraph remains unmaterialized and no purchase order exists.');
});

listen('native-order-saved','change',async()=>{
 const key=ui('native-order-saved').value;
 if(!key){orderIntentValue=null;orderIntentRevision=0;protocolPreflightValue=null;downstreamStatus=null;ui('native-order-review').hidden=true;ui('native-protocol-section').hidden=true;ui('native-protocol-review').hidden=true;ui('native-downstream-status').hidden=true;setQuery('orderIntent','');return;}
 const rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);if(!row)throw Error('This order intent is unavailable.');
 orderIntentValue=row.value;orderIntentRevision=row.revision;setQuery('orderIntent',key);renderOrderIntent(row.value);
});

listen('native-protocol-prepare','click',async()=>{
 if(!orderIntentValue||orderIntentRevision<1)throw Error('Save the exact order intent before preparing a protocol contract preflight.');
 const key=prefixes.orderIntent+orderIntentValue.id,rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);
 if(!row||row.revision!==orderIntentRevision||row.value?.digest!==orderIntentValue.digest)throw Error('The order intent changed before protocol preflight.');
 const buyer=clean(ui('native-protocol-buyer').value);if(!buyer)throw Error('Name the explicit buyer actor for this protocol proposal.');
 const lot_actors=[...ui('native-protocol-lots').querySelectorAll('fieldset')].map(field=>{
  const read=name=>clean(field.querySelector('[data-field="'+name+'"]')?.value);
  const actor={quote_id:field.dataset.quoteId,seller_actor:read('seller'),settlement_actor:read('settlement'),deliverer_actor:read('deliverer'),capability_ref:read('capability')};
  if(!actor.seller_actor||!actor.settlement_actor||!actor.deliverer_actor||!actor.capability_ref)throw Error('Every supplier lot needs explicit seller, settlement, deliverer and capability actors before preflight.');
  return actor;
 });
 protocolPreflightValue=kernel.prepareProtocolPreflight({order_intent:orderIntentValue,id:'protocol-preflight-'+crypto.randomUUID(),prepared_at:utcSecond(),buyer_actor:buyer,lot_actors});
 renderProtocolPreflight(protocolPreflightValue);
 message('Effect-free ProtocolContract preflight prepared. No supplier identity was bound, no authority was granted, no supplier accepted, no protocol was enacted and no commitment was opened.');
});

ui('native-enactment-contract').addEventListener('change',controls,{signal:life.signal});
ui('native-enactment-signature').addEventListener('input',controls,{signal:life.signal});
ui('native-enactment-accepted-at').addEventListener('input',controls,{signal:life.signal});

listen('native-enactment-readiness','click',async()=>{
 if(!orderIntentValue||orderIntentRevision<1)throw Error('Open a saved order intent first.');
 const key=prefixes.orderIntent+orderIntentValue.id,rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);
 if(!row||row.revision!==orderIntentRevision||row.value?.digest!==orderIntentValue.digest)throw Error('The order intent changed before enactment readiness could be checked.');
 const status=await enactmentReadiness(key);life.assert();
 if(status.record_key&&status.record_key!==key)throw Error('Enactment readiness does not bind this order intent.');
 if(status.revision&&status.revision!==orderIntentRevision)throw Error('Enactment readiness does not bind this saved order-intent version.');
 renderEnactmentReadiness(status);
 message(status.ready?'Native lineage and buyer approval are current. Supplier acceptance is the next separate boundary.':'This order intent is not currently ready for supplier acceptance: '+status.state+'.');
});

listen('native-enactment-submit','click',async()=>{
 if(!orderIntentValue||orderIntentRevision<1||enactmentReadinessStatus?.ready!==true)throw Error('Check current enactment readiness for this saved order intent first.');
 const key=prefixes.orderIntent+orderIntentValue.id,rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);
 if(!row||row.revision!==orderIntentRevision||row.value?.digest!==orderIntentValue.digest)throw Error('The order intent changed before supplier acceptance could be submitted.');
 const contract=enactableSupplierContracts(protocolPreflightValue).find(item=>item.exchange_id===ui('native-enactment-contract').value);
 if(!contract)throw Error('Choose one cryptographically bound supplier lot from the current preflight.');
 const acceptedAt=exactUtc(ui('native-enactment-accepted-at').value);if(!acceptedAt)throw Error('Import the supplier acceptance timestamp.');
 const signature=clean(ui('native-enactment-signature').value).toLowerCase();ui('native-enactment-signature').value='';controls();
 const receipt=await enactSupplierAcceptance(key,contract,signature,acceptedAt);life.assert();renderEnactmentReceipt(receipt);
 const status=await reconcileDownstreamOwners(key);life.assert();if(status.record_key!==key||status.revision!==orderIntentRevision)throw Error('Downstream readback does not bind the enacted order-intent version.');renderDownstreamStatus(status);
 message(receipt.replayed?'Exact supplier acceptance replayed safely; native commitments already matched.':'Supplier acceptance verified and ProtocolContract commitments opened. No purchase order, payable, payment, settlement or shipment was inferred.');
});

listen('native-order-reconcile','click',async()=>{
 if(!orderIntentValue||orderIntentRevision<1)throw Error('Open a saved order intent first.');
 const key=prefixes.orderIntent+orderIntentValue.id,rows=await store.readLocal(key);life.assert();const row=rows.find(item=>item.key===key);
 if(!row||row.revision!==orderIntentRevision||row.value?.digest!==orderIntentValue.digest)throw Error('The order intent changed before downstream reconciliation.');
 const status=await reconcileDownstreamOwners(key);life.assert();
 if(status.record_key!==key||status.revision!==orderIntentRevision)throw Error('Downstream reconciliation does not bind this saved order-intent version.');
 renderDownstreamStatus(status);
 message(status.state==='downstream_observed'?'Downstream owner state reconciled. Only the exact CommitmentGraph state and CommerceChain stages shown are established.':'No downstream CommitmentGraph or CommerceChain state has been materialized for this order intent.');
});

ui('saved')?.addEventListener('change',()=>{reset();setQuery('rfq','');setQuery('quote','');setQuery('comparison','');setQuery('allocation','');setQuery('approval','');setQuery('orderIntent','');},{signal:life.signal});

async function openFromQuery(){
 const params=new URL(location.href).searchParams;
 let rfqKey=params.get('rfq'),quoteKey=params.get('quote'),comparisonKey=params.get('comparison'),allocationKey=params.get('allocation'),approvalKey=params.get('approval'),orderIntentKey=params.get('orderIntent'),allocationRow=null,approvalRow=null,orderIntentRow=null;
 if(orderIntentKey){orderIntentRow=orderIntents.find(row=>row.key===orderIntentKey);if(orderIntentRow)approvalKey=prefixes.approval+orderIntentRow.value.source.approval_id;}
 if(approvalKey){
  approvalRow=approvals.find(row=>row.key===approvalKey);
  if(approvalRow){allocationRow=allocations.find(row=>row.value?.id===approvalRow.value?.source?.allocation_id&&row.value?.digest===approvalRow.value?.source?.digest)||null;if(allocationRow)allocationKey=allocationRow.key;}
 }
 if(allocationKey){
  allocationRow=allocations.find(row=>row.key===allocationKey);
  if(allocationRow){
   rfqKey=prefixes.rfq+allocationRow.value.source.rfq_id;
   const comparison=comparisons.find(row=>row.value?.digest===allocationRow.value.source.comparison_digest);
   if(comparison)comparisonKey=comparison.key;
  }
 }
 if(comparisonKey){
  const comparison=comparisons.find(row=>row.key===comparisonKey);
  if(comparison){rfqKey=prefixes.rfq+comparison.value.source.rfq_id;comparisonValue=comparison.value;ui('native-comparison-saved').value=comparisonKey;}
 }
 if(quoteKey){
  const quote=quotes.find(row=>row.key===quoteKey);
  if(quote)rfqKey=prefixes.rfq+quote.value.source.rfq_id;
 }
 if(rfqKey){
  const rfq=rfqs.find(row=>row.key===rfqKey);
  if(rfq){rfqValue=rfq.value;rfqRevision=rfq.revision;ui('native-rfq-saved').value=rfqKey;renderRFQ();}
 }
 if(comparisonValue)renderComparison(comparisonValue);
 if(quoteKey&&quotes.some(row=>row.key===quoteKey))ui('native-quote-saved').value=quoteKey;
 if(allocationRow){allocationValue=allocationRow.value;allocationRevision=allocationRow.revision;ui('native-allocation-saved').value=allocationRow.key;renderAllocation(allocationRow.value);}
 if(approvalRow){approvalValue=approvalRow.value;approvalRevision=approvalRow.revision;ui('native-approval-saved').value=approvalRow.key;renderApproval(approvalRow.value);}
 if(orderIntentRow){orderIntentValue=orderIntentRow.value;orderIntentRevision=orderIntentRow.revision;ui('native-order-saved').value=orderIntentRow.key;ui('native-order-section').hidden=false;renderOrderIntent(orderIntentRow.value);}
}

life.onClose(()=>{
 rfqs=[];quotes=[];comparisons=[];allocations=[];approvals=[];orderIntents=[];rfqValue=null;comparisonValue=null;allocationValue=null;approvalValue=null;approvalStatus=null;orderIntentValue=null;protocolPreflightValue=null;enactmentReadinessStatus=null;enactmentReceipt=null;downstreamStatus=null;store=null;kernel=null;
});

run(async()=>{
 const endpoint=document.querySelector('meta[name="bonfyre-workspace-store"]')?.content;
 if(!endpoint)throw Error('No native workspace binding is available.');
 store=openDestinationStore(endpoint,{signal:life.signal});
 const response=await fetch(new URL('./procurement.wasm',import.meta.url),{credentials:'same-origin',cache:'no-store',redirect:'error',signal:life.signal});
 if(!response.ok)throw Error('The native procurement kernel is unavailable.');
 const bytes=await response.arrayBuffer();
 life.assert();
 if(bytes.byteLength>2*1024*1024)throw Error('The native procurement kernel exceeds its browser bound.');
 kernel=await createProcurementKernel(bytes);
 await readAll();
 await openFromQuery();
 controls();
 root.dataset.nativeProcurementReady='true';
});
