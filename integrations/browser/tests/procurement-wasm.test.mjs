import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {createProcurementKernel} from '../procurement-wasm.mjs';

const H='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
const wasm=readFileSync(resolve(import.meta.dir,'../../../cmd/BonfyreProcurement/wasm/procurement.wasm'));
const random={getRandomValues(view){view.fill(7);return view;}};
const purchase={
  schema:'service-site.room-purchase-list.v1',id:'purchase-12345678',state:'draft',currency:'USD',sha256:H,commitment:null,
  source:{key:'fixture-plan:room',revision:7,sha256:H},
  lines:[{product_id:'product-1',variant_id:'variant-1',label:'Chair — café',option:'Walnut',source_url:'https://merchant.example/items/1',source_sha256:H,observed_at:'2026-10-23T12:00:00Z',unit_cents:1000,quantity:2,subtotal_cents:2000}],
  subtotal_cents:2000
};

test('browser adapter runs the native RFQ kernel deterministically without effects',async()=>{
  const kernel=await createProcurementKernel(wasm,{random});
  const input={purchase_list:purchase,purchase_list_revision:3,id:'rfq-12345678',requested_at:'2026-10-23T13:00:00Z',supplier:null,selected:null};
  const a=kernel.prepareRFQ(input),b=kernel.prepareRFQ(input);
  expect(a).toEqual(b); expect(a.status).toBe('prepared'); expect(a.lines).toHaveLength(1);
  expect(a.supplier_contacted).toBe(false); expect(a.purchase_approved).toBe(false); expect(a.order_committed).toBe(false);
  expect(a.does_not_establish).toContain('supplier contact');
});

test('browser adapter retains native quote and comparison semantics',async()=>{
  const kernel=await createProcurementKernel(wasm,{random});
  const rfq=kernel.prepareRFQ({purchase_list:purchase,purchase_list_revision:3,id:'rfq-quote-1234',requested_at:'2026-10-23T13:00:00Z',supplier:{id:'supplier-1',name:'Supplier One'},selected:null});
  const line=rfq.lines[0];
  const quote=kernel.observeQuote({rfq,observation:{id:'quote-12345678',supplier:{id:'supplier-1',name:'Supplier One'},supplier_quote_id:'SQ-1',observed_at:'2026-10-23T14:00:00Z',quoted_at:null,valid_until:'2026-10-25T18:00:00Z',currency:'USD',source_ref:'file:supplier-quote.pdf',freight_cents:null,tax_cents:null,other_fees_cents:null,lines:[{id:'quote-line-1',rfq_line_id:line.line_id,quantity:2,unit_cents:900,line_total_cents:null,substitution:null,availability:'In stock',lead_time:'5 days'}]}});
  expect(quote.schema).toBe('bonfyre.procurement.supplier-quote-observation.v1');
  expect(quote.freight_cents).toBeNull(); expect(quote.tax_cents).toBeNull();
  expect(quote.quote_accepted).toBe(false); expect(quote.reserved).toBe(false); expect(quote.order_authorized).toBe(false);
  const comparison=kernel.compareQuotes({rfq,quotes:[quote],expected_rfq_digest:rfq.digest,expected_quotes:[{id:quote.id,digest:quote.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(comparison.schema).toBe('bonfyre.procurement.quote-comparison.v1');
  expect(comparison.status).toBe('unpriced'); expect(comparison.stale).toBe(false);
  expect(comparison.approved).toBe(false); expect(comparison.committed).toBe(false); expect(comparison.ordered).toBe(false);
  const priced=kernel.observeQuote({rfq,observation:{id:'quote-priced-1',supplier:{id:'supplier-1',name:'Supplier One'},supplier_quote_id:'SQ-PRICED',observed_at:'2026-10-23T14:30:00Z',quoted_at:null,valid_until:null,currency:'USD',source_ref:'file:priced.pdf',freight_cents:0,tax_cents:0,other_fees_cents:0,lines:[{id:'quote-line-priced',rfq_line_id:line.line_id,quantity:2,unit_cents:900,line_total_cents:null,substitution:null,availability:'In stock',lead_time:'5 days'}]}});
  const current=kernel.compareQuotes({rfq,quotes:[priced],expected_rfq_digest:rfq.digest,expected_quotes:[{id:priced.id,digest:priced.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(current.status).toBe('current'); expect(current.stale).toBe(false);
  const stale=kernel.compareQuotes({rfq,quotes:[quote],expected_rfq_digest:H,expected_quotes:[{id:quote.id,digest:quote.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(stale.status).toBe('stale'); expect(stale.stale).toBe(true);
  const eur=kernel.observeQuote({rfq,observation:{id:'quote-eur-1234',supplier:{id:'supplier-1',name:'Supplier One'},supplier_quote_id:'SQ-2',observed_at:'2026-10-23T15:00:00Z',quoted_at:null,valid_until:null,currency:'EUR',source_ref:'file:supplier-quote-eur.pdf',freight_cents:0,tax_cents:0,other_fees_cents:0,lines:[{id:'quote-line-eur',rfq_line_id:line.line_id,quantity:2,unit_cents:850,line_total_cents:null,substitution:null,availability:null,lead_time:null}]}});
  const mixed=kernel.compareQuotes({rfq,quotes:[quote,eur],expected_rfq_digest:rfq.digest,expected_quotes:[{id:quote.id,digest:quote.digest},{id:eur.id,digest:eur.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(mixed.status).toBe('mixed_currency'); expect(mixed.mixed_currency).toBe(true);
});

test('browser adapter plans deterministic split allocation without creating commerce effects',async()=>{
  const kernel=await createProcurementKernel(wasm,{random});
  const rfq=kernel.prepareRFQ({purchase_list:purchase,purchase_list_revision:3,id:'rfq-allocation-1',requested_at:'2026-10-23T13:00:00Z',supplier:null,selected:null});
  const line=rfq.lines[0];
  const quote=(id,supplier,quantity,unit,valid_until)=>kernel.observeQuote({rfq,observation:{
    id,supplier:{id:supplier,name:supplier.replaceAll('-',' ')},supplier_quote_id:null,observed_at:'2026-10-23T15:00:00Z',quoted_at:null,valid_until,currency:'USD',source_ref:'file:'+id+'.pdf',
    freight_cents:0,tax_cents:0,other_fees_cents:0,
    lines:[{id:id+'-line',rfq_line_id:line.line_id,quantity,unit_cents:unit,line_total_cents:null,substitution:null,availability:'Observed',lead_time:null}]
  }});
  const a=quote('quote-a','supplier-a',1,800,'2026-10-30T00:00:00Z');
  const b=quote('quote-b','supplier-b',1,900,'2026-10-30T00:00:00Z');
  const comparison=kernel.compareQuotes({rfq,quotes:[a,b],expected_rfq_digest:rfq.digest,expected_quotes:[{id:a.id,digest:a.digest},{id:b.id,digest:b.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(comparison.status).toBe('current');
  const input={rfq,quotes:[a,b],comparison,id:'allocation-browser-1',prepared_at:'2026-10-24T00:00:00Z',policy:'lowest_known_merchandise'};
  const first=kernel.allocateQuotes(input),second=kernel.allocateQuotes(input);
  expect(first).toEqual(second);
  expect(first.schema).toBe('bonfyre.procurement.allocation-plan.v1');
  expect(first.allocations).toHaveLength(2);
  expect(first.allocations.map(row=>[row.quote_id,row.quantity,row.unit_cents])).toEqual([['quote-a',1,800],['quote-b',1,900]]);
  expect(first.known_merchandise_cents).toBe(1700);
  expect(first.known_total_cents).toBe(1700);
  expect(first.fees_attribution).toBe('full_quote');
  expect(first.order_lots).toHaveLength(2);
  expect(first.order_lots.every(row=>row.full_quote_consumed&&row.known_total_cents!==null)).toBe(true);
  expect(first.approval_required).toBe(true);
  expect(first.approved).toBe(false); expect(first.cart_created).toBe(false); expect(first.commitment_opened).toBe(false); expect(first.order_created).toBe(false);
  expect(first.commerce_chain).toMatchObject({owner:'CommerceChain',status:'unmaterialized',next_required_stage:'product'});
  const approval=kernel.prepareApprovalRequest({allocation:first,allocation_revision:4,id:'approval-browser-1',requested_at:'2026-10-24T00:01:00Z',requester:'user:19',reason:'Review exact allocation'});
  expect(approval.schema).toBe('bonfyre.procurement.approval-request.v1');
  expect(approval.status).toBe('pending');expect(approval.source).toEqual({allocation_id:first.id,revision:4,digest:first.digest});
  expect(approval.authority).toMatchObject({owner:'AuthorityGraph',required_permissions:['approve','spend'],decision:'pending',decided_by:null,decision_ref:null});
  expect(approval.known_total_cents).toBe(1700);
  expect(approval.fees_attribution).toBe('full_quote');expect(approval.approved).toBe(false);expect(approval.spend_authorized).toBe(false);expect(approval.quote_accepted).toBe(false);expect(approval.commitment_opened).toBe(false);expect(approval.order_created).toBe(false);expect(approval.payment_presented).toBe(false);
  const tampered=structuredClone(first);tampered.approved=true;delete tampered.digest;
  expect(()=>kernel.prepareApprovalRequest({allocation:tampered,allocation_revision:4,id:'approval-browser-bad',requested_at:'2026-10-24T00:01:00Z',requester:'user:19',reason:null})).toThrow();
  const authorityStatus={state:'eligible',record_key:'procurement-approval:'+approval.id,revision:5,body_sha256:H,authority_subject:approval.authority.subject,decision_id:'decision-browser-1',outcome:'approved',decided_by:'user:19',decided_at:'2026-10-24T00:02:00Z',assent_present:true,approve_authority:true,spend_authority:true,eligible_for_order_intent:true,decision_receipt:'fixture:decision'};
  const intentInput={allocation:first,allocation_revision:4,approval,approval_revision:5,authority_status:authorityStatus,id:'order-intent-browser-1',prepared_at:'2026-10-24T00:03:00Z'};
  const intent=kernel.prepareOrderIntent(intentInput),intentAgain=kernel.prepareOrderIntent(intentInput);
  expect(intent).toEqual(intentAgain);expect(intent.schema).toBe('bonfyre.procurement.order-intent.v1');expect(intent.status).toBe('prepared');expect(intent.authority_eligible).toBe(true);
  expect(intent.source).toMatchObject({allocation_id:first.id,allocation_revision:4,allocation_digest:first.digest,approval_id:approval.id,approval_revision:5,approval_digest:approval.digest,decision_id:'decision-browser-1'});
  expect(intent.commitment_handoff).toMatchObject({owner:'CommitmentGraph',status:'unmaterialized',requires_quote_acceptance:true,requires_supplier_counterparty_terms:true});
  for(const field of ['quote_accepted','reservation_created','cart_created','commitment_opened','order_created','purchase_order_issued','supplier_accepted','payable_created','payment_presented','settled','fulfilled','shipped','received'])expect(intent[field]).toBe(false);
  const lotActors=intent.order_lots.map((lot,index)=>({quote_id:lot.quote_id,seller_actor:'seller:'+(index+1),settlement_actor:'settlement:buyer',deliverer_actor:'deliverer:'+(index+1),capability_ref:'procurement-capability:'+(index+1)}));
  const preflightInput={order_intent:intent,id:'protocol-preflight-browser-1',prepared_at:'2026-10-24T00:04:00Z',buyer_actor:'buyer:acme',lot_actors:lotActors};
  const preflight=kernel.prepareProtocolPreflight(preflightInput),preflightAgain=kernel.prepareProtocolPreflight(preflightInput);
  expect(preflight).toEqual(preflightAgain);expect(preflight.schema).toBe('bonfyre.procurement.protocol-preflight.v1');expect(preflight.effect_free).toBe(true);expect(preflight.contracts).toHaveLength(2);
  for(const contract of preflight.contracts){expect(contract).toMatchObject({contract_name:'quote-accept',status:'authority_preflight_required',structurally_complete:true,supplier_actor_binding_verified:false,authority_verified:false,supplier_acceptance_present:false,protocol_enacted:false,commitment_opened:false});expect(contract.authority_requirements.map(row=>row.permission)).toEqual(['request:capability','quote:capability','accept:quote','deliver:capability']);expect(contract.proposed_commitments).toHaveLength(2);}
  expect(()=>kernel.prepareProtocolPreflight({...preflightInput,lot_actors:lotActors.slice(0,1)})).toThrow();

  expect(()=>kernel.prepareOrderIntent({...intentInput,authority_status:{...authorityStatus,spend_authority:false}})).toThrow();
  expect(()=>kernel.prepareOrderIntent({...intentInput,authority_status:{...authorityStatus,record_key:'procurement-approval:wrong'}})).toThrow();



  const expired=quote('quote-expired','supplier-cheap',2,100,'2026-10-23T23:00:00Z');
  const valid=quote('quote-valid','supplier-valid',2,950,'2026-10-30T00:00:00Z');
  const expiryComparison=kernel.compareQuotes({rfq,quotes:[expired,valid],expected_rfq_digest:rfq.digest,expected_quotes:[{id:expired.id,digest:expired.digest},{id:valid.id,digest:valid.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(expiryComparison.status).toBe('current');
  expect(expiryComparison.expired_quote_count).toBe(1);
  const expiryPlan=kernel.allocateQuotes({rfq,quotes:[expired,valid],comparison:expiryComparison,id:'allocation-browser-expiry',prepared_at:'2026-10-24T00:00:00Z',policy:'lowest_known_merchandise'});
  expect(expiryPlan.allocations).toHaveLength(1);
  expect(expiryPlan.allocations[0].quote_id).toBe('quote-valid');
  expect(expiryPlan.known_merchandise_cents).toBe(1900);

  const partial=quote('quote-partial','supplier-one',1,700,'2026-10-30T00:00:00Z');
  const partialComparison=kernel.compareQuotes({rfq,quotes:[partial],expected_rfq_digest:rfq.digest,expected_quotes:[{id:partial.id,digest:partial.digest}]},{asOf:'2026-10-24T00:00:00Z'});
  expect(partialComparison.status).toBe('partial');
  expect(()=>kernel.allocateQuotes({rfq,quotes:[partial],comparison:partialComparison,id:'allocation-browser-partial',prepared_at:'2026-10-24T00:00:00Z',policy:'lowest_known_merchandise'})).toThrow();
});

test('browser adapter preserves native refusal',async()=>{
  const kernel=await createProcurementKernel(wasm,{random});
  expect(()=>kernel.prepareRFQ({})).toThrow();
  expect(()=>kernel.compareQuotes({}, {asOf:'not-a-time'})).toThrow();
});
