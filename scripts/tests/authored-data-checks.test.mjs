import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {decode,encode} from '../../integrations/browser/workspace-msgpack.mjs';
import {prepareRecord,digest} from '../../integrations/browser/workspace-value.mjs';
import {validateDataChecksContract,validateDataChecksRecord,blankDataChecks,importDataChecksRows,importDataChecksCSV,evaluateDataChecks,exportDataChecksReport,exportDataChecksDraft} from '../../cmd/BonfyreSurfaceScene/runtime/authored-data-checks.mjs';
import {createAuthoredWorkspace,addInstrument,blankInstrument,reuseWorkspaceDesign,productionPrefix} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
import {publicProductionDesign,instantiatePublicDesign} from '../../cmd/BonfyreSurfaceScene/runtime/authored-publication.mjs';
import {inspectRecoveryMaterial,previewRecoveryWorkflow,commitRecoveryWorkflow,recoverySchema} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs';
import {prepareProductionTransfer,commitProductionTransfer} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-transfer.mjs';

// Only literal typed declarations are retained in this fixture. Source chat,
// application code, profile material and purported customer inputs are absent.
// Normalized from the five reviewed archive declarations, including their later
// exact-preimage edits; base64 is only the source-release text carrier.
const fixtureBytes=Uint8Array.from(Buffer.from(readFileSync(new URL('./fixtures/data-checks-contracts.mpk.b64',import.meta.url),'utf8').trim(),'base64'));
const contracts=decode(fixtureBytes);
const contract=id=>structuredClone(contracts.find(c=>c.id===id));
const hash='a'.repeat(64);
const digestValue=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
async function record(c,rows={},example=false){
  const tables=[];for(const e of c.entities){if(!Object.hasOwn(rows,e.id))continue;const columns=c.fields.filter(f=>f.entity===e.id).map(f=>f.name),cells=rows[e.id].map(r=>columns.map(k=>r[k]??null));tables.push(await importDataChecksRows(c,e.id,{columns,rows:cells},{name:e.id+' fixture',example}));}
  return {kind:'checks',title:c.id,contract:c,tables,selected:null};
}
function meetingRows(){return {transcript_segment:[{segment_id:'seg-1',action_id:'action-1',text:'Review the draft next week.',owner:'Reviewer',due_date:'2026-10-29',confidence:'0.90'}],tracker_item:[{action_id:'action-1',title:'Review draft',owner:'Reviewer',due_date:'2026-10-29'}],chat_message:[{message_id:'message-1',action_id:'action-1',approval_state:'approved',text:'The proposed follow-up text is approved.'}]};}
function contentRows(){return {media_item:[{source_hash:hash,duration_ns:'60000000000',materialization_capability:'supplied metadata only'}],transcript_segment:[{segment_id:'segment-1',source_media_hash:hash,text:'Opening section',confidence:'0.9',chapter_label:'Opening'}],creator_note:[{message_id:'note-1',media_ref:hash,publish_state:'approved',text:'Review this publishing draft.'}]};}
function audioRows(){return {edit_row:[{row_id:'edit-1',start_time_ns:'0',end_time_ns:'1000000000',operation_label:'gain',parameter_value:'3',source_hash:hash}],media_singleton:[{source_hash:hash,duration_ns:'60000000000',sample_rate:'48000',channel_count:'2',materialization_capability:'supplied metadata only'}]};}
function ledgerRows(){return {expected_payment:[{expected_id:'expected-1',amount:'120.25',currency:'USD',due_date:'2026-10-22',counterparty:'Example supplier',description:'Invoice fixture'}],actual_payment:[{actual_id:'actual-1',amount:'120.25',currency:'USD',paid_date:'2026-10-24',counterparty:'Example supplier',description:'Invoice fixture'}]};}
function erpRows(){return {sales_order_line:[{name:'order-line-1',sales_order:'order-1',item_code:'ITEM-1',item_label:'Fixture part',qty:'3',warehouse:'WH-1',currency:'USD',supplier_hint:'Supplier'}],stock_bin:[{name:'bin-1',item_code:'ITEM-1',item_label:'Fixture part',warehouse:'WH-1',actual_qty:'5',bin_confidence:'0.8'}],supplier_offer:[{name:'offer-1',item_code:'ITEM-1',offer_label:'Fixture offer',supplier:'Supplier',supplier_alias:'Supplier',currency:'USD',offer_confidence:'0.8'}],purchase_invoice:[{name:'invoice-1',supplier:'Supplier',outstanding_amount:'120.25',currency:'USD',due_date:'2026-10-22'}],payment_entry:[{name:'payment-1',party:'Supplier',allocated_amount:'120.25',currency:'USD',posting_date:'2026-10-24',payment_confidence:'0.8'}]};}
function exactStrategies(c){for(const r of c.relations)if(r.candidate_strategy.kind==='hybrid'){const exact=r.candidate_strategy.strategies.find(s=>s.kind==='key_eq');if(exact)r.candidate_strategy=structuredClone(exact);}return c;}

test('all five literal source contracts validate; absent and empty sources remain needs_input',async()=>{
  expect(await digest(fixtureBytes)).toBe('046833a0616d5cc2b7fc16e5354bf765d5903efa491560b644a0307b7fe4e8a3');
  expect(contracts).toHaveLength(5);for(const c of contracts){expect(validateDataChecksContract(c)).toEqual(c);const r=await evaluateDataChecks(await record(c));expect(r.summary.state).toBe('needs_input');expect(r.summary.rows).toBe(0);expect(r.draft_outputs).toHaveLength(0);expect(r.closures.every(r=>r.state==='unresolved')).toBe(true);expect(r.field_checks.every(r=>r.reason==='needs_input')).toBe(true);}
  const c=contract('meeting_followup_packet'),r=await evaluateDataChecks(await record(c,{transcript_segment:[],tracker_item:[],chat_message:[]}));expect(r.summary.state).toBe('needs_input');expect(r.inputs.every(i=>i.state==='needs_input')).toBe(true);
  expect((await evaluateDataChecks(blankDataChecks())).summary.state).toBe('needs_input');
});
test('a complete meeting flow produces real typed checks, two draft outputs and exact source-row identities',async()=>{
  const c=contract('meeting_followup_packet'),source=await record(c,meetingRows(),true),r=await evaluateDataChecks(source);
  expect(r.summary.state).toBe('checked');expect(r.summary.rows).toBe(3);expect(r.summary.examples).toBe(true);expect(r.summary.fields).toEqual({pass:14,fail:0,unresolved:0});expect(r.closures.map(x=>x.state)).toEqual(['pass','pass']);expect(r.draft_outputs.map(x=>x.label)).toEqual(['action_item_list','followup_message_draft']);
  expect(r.draft_outputs.every(x=>x.execution==='not_run'&&x.kind==='draft')).toBe(true);expect(r.execution).toBe('not_run');expect(r.scope).toBe('supplied_input_checks');
  for(const row of [...r.checks,...r.closures])for(const ref of row.inputs){expect(ref.source_sha256).toBe(source.tables.find(t=>t.entity===ref.entity).source_sha256);expect(ref.row).toBe(1);expect(ref.example).toBe(true);}
  const criterion=r.checks.find(x=>x.id==='meeting.transcript_confident');expect(criterion.state).toBe('pass');expect(criterion.observed[0].value).toBe('0.90');expect(r.declarations.numeric_compression.field_overrides['meeting_transcript.confidence']).toBe('q7');
  const exported=decode(encode(await exportDataChecksDraft(r)));expect(exported[2].execution).toBe('not_run');expect(exported[2].outputs[1].values.chat_message.text).toBe(meetingRows().chat_message[0].text);expect(await exportDataChecksReport(r)).toContain(source.tables[0].source_sha256);
});
test('missing meeting ownership and unapproved chat are failures, with no completed follow-up output',async()=>{
  const rows=meetingRows();rows.tracker_item[0].owner='';rows.chat_message[0].approval_state='pending';const r=await evaluateDataChecks(await record(contract('meeting_followup_packet'),rows));
  expect(r.field_checks.find(x=>x.id==='tracker_owner').state).toBe('fail');expect(r.checks.find(x=>x.id==='meeting.chat_approved').state).toBe('fail');expect(r.closures.every(x=>x.state==='fail')).toBe(true);expect(r.draft_outputs).toHaveLength(0);
});
test('unmatched exact rows retain their actual input identity instead of reporting an empty success',async()=>{
  const rows=meetingRows();rows.tracker_item[0].action_id='unmatched-action';const r=await evaluateDataChecks(await record(contract('meeting_followup_packet'),rows));
  const unresolved=r.closures.find(x=>x.id==='meeting.action_item_ready');expect(unresolved.state).toBe('fail');expect(unresolved.inputs[0].key).toBe('seg-1');expect(unresolved.inputs[0].row).toBe(1);expect(unresolved.reason).toBe('tracker_missing');expect(r.draft_outputs).toHaveLength(0);
});
test('content checks preserve low-confidence and approval failures; no media or publishing effect is implied',async()=>{
  const rows=contentRows();rows.transcript_segment[0].confidence='0.5';rows.creator_note[0].publish_state='pending';const r=await evaluateDataChecks(await record(contract('content_publishing_packet'),rows));
  expect(r.checks.find(x=>x.id==='content.segment_confident').reason).toBe('outside_numeric_range');expect(r.closures.every(x=>x.state==='fail')).toBe(true);expect(r.draft_outputs).toHaveLength(0);expect(r.declarations.materialization.planned_steps).toContain('emit_publish_packet');expect(r.execution).toBe('not_run');
});
test('audio bounds, gain limits and entity-wide overlap use actual rows and preserve the unconditional archived range',async()=>{
  const rows=audioRows();rows.edit_row.push({...rows.edit_row[0],row_id:'edit-2',start_time_ns:'500000000',end_time_ns:'70000000000',parameter_value:'30',source_hash:'b'.repeat(64)});const r=await evaluateDataChecks(await record(contract('audio_edit_contract'),rows));
  expect(r.checks.filter(x=>x.id==='audio.no_overlap').every(x=>x.state==='fail')).toBe(true);expect(r.checks.find(x=>x.id==='audio.end_in_bounds'&&x.inputs.some(i=>i.key==='edit-2')).state).toBe('fail');expect(r.checks.find(x=>x.id==='audio.gain_value_range'&&x.inputs.some(i=>i.key==='edit-2')).state).toBe('fail');expect(r.draft_outputs).toHaveLength(0);
  const trim=audioRows();trim.edit_row[0].operation_label='trim';trim.edit_row[0].parameter_value='';const literal=await evaluateDataChecks(await record(contract('audio_edit_contract'),trim));expect(literal.checks.find(x=>x.id==='audio.gain_value_required').reason).toBe('condition_not_required');expect(literal.checks.find(x=>x.id==='audio.gain_value_range').state).toBe('unresolved');expect(literal.closures[0].state).toBe('unresolved');
});
test('large integer timing stays exact above IEEE safe integer range',async()=>{
  const rows=audioRows();rows.edit_row[0].start_time_ns='9007199254740992';rows.edit_row[0].end_time_ns='9007199254740993';rows.media_singleton[0].duration_ns='9007199254740993';
  const r=await evaluateDataChecks(await record(contract('audio_edit_contract'),rows));expect(r.checks.find(x=>x.id==='audio.start_lt_end').state).toBe('pass');expect(r.checks.find(x=>x.id==='audio.end_in_bounds').state).toBe('pass');expect(r.closures[0].state).toBe('pass');
  rows.edit_row[0].end_time_ns='9007199254740994';const fail=await evaluateDataChecks(await record(contract('audio_edit_contract'),rows));expect(fail.checks.find(x=>x.id==='audio.end_in_bounds').state).toBe('fail');
});
test('ledger latent matching remains unresolved; an explicitly revised exact join exposes currency, amount and date failures',async()=>{
  const original=contract('ledger_reconcile_contract'),raw=ledgerRows(),latent=await evaluateDataChecks(await record(original,raw));expect(latent.summary.state).toBe('unresolved');expect(latent.relations[0].unresolved).toContain('latent_matching_not_run');expect(latent.draft_outputs).toHaveLength(0);
  const c=structuredClone(original);c.relations[0].candidate_strategy={kind:'key_eq',left:{entity:'expected_payment',field:'description'},right:{entity:'actual_payment',field:'description'}};raw.actual_payment[0].currency='EUR';raw.actual_payment[0].amount='130.25';raw.actual_payment[0].paid_date='2026-10-28';const r=await evaluateDataChecks(await record(c,raw));
  expect(r.checks.find(x=>x.id==='ledger.currency_eq').state).toBe('fail');expect(r.checks.find(x=>x.id==='ledger.amount_eq').state).toBe('fail');expect(r.checks.find(x=>x.id==='ledger.date_within').reason).toBe('outside_day_tolerance');expect(r.closures[0].state).toBe('fail');expect(original.relations[0].candidate_strategy.kind).toBe('hybrid');
});
test('strict date and equality semantics do not reproduce the legacy empty-string or permissive Date.parse coercions',async()=>{
  const c=contract('ledger_reconcile_contract');c.relations[0].candidate_strategy={kind:'key_eq',left:{entity:'expected_payment',field:'description'},right:{entity:'actual_payment',field:'description'}};const rows=ledgerRows();rows.actual_payment[0].paid_date='10/24/2026';rows.expected_payment[0].amount='';rows.actual_payment[0].amount='0';
  const r=await evaluateDataChecks(await record(c,rows));expect(r.field_checks.find(x=>x.id==='expected_amount').state).toBe('fail');expect(r.checks.find(x=>x.id==='ledger.amount_eq').state).toBe('unresolved');expect(r.checks.find(x=>x.id==='ledger.date_within').reason).toBe('iso_date_required');expect(r.draft_outputs).toHaveLength(0);
});
test('ERP exact candidate checks find real shortage and payment mismatch while hybrid search stays unresolved',async()=>{
  const rows=erpRows();rows.stock_bin[0].actual_qty='1';rows.payment_entry[0].allocated_amount='100';const c=contract('erpnext_backend_replacement_contract'),r=await evaluateDataChecks(await record(c,rows));
  expect(r.checks.find(x=>x.id==='stock.qty_available').state).toBe('fail');expect(r.checks.find(x=>x.id==='invoice.amount.eq').state).toBe('fail');expect(r.relations.every(x=>!x.complete)).toBe(true);expect(r.draft_outputs).toHaveLength(0);
  const configured=await evaluateDataChecks(await record(exactStrategies(c),erpRows()));expect(configured.closures.every(x=>x.state==='pass')).toBe(true);expect(configured.draft_outputs.map(x=>x.declared_next_state)).toEqual(['command','command','command']);expect(configured.draft_outputs.every(x=>x.execution==='not_run')).toBe(true);
});
test('unknown hash placeholders, duplicate entity keys and malformed numeric cells produce failures',async()=>{
  const rows=contentRows();rows.media_item[0].source_hash='media-hash-001';rows.transcript_segment[0].confidence='not-a-number';rows.creator_note.push({...rows.creator_note[0]});const r=await evaluateDataChecks(await record(contract('content_publishing_packet'),rows));
  expect(r.field_checks.find(x=>x.id==='media_hash').state).toBe('fail');expect(r.field_checks.find(x=>x.id==='segment_confidence').state).toBe('fail');expect(r.field_checks.filter(x=>x.id==='note_id').every(x=>x.reason==='duplicate_entity_key')).toBe(true);expect(r.draft_outputs).toHaveLength(0);
});
test('table bytes, mappings and example identity are retained without trusting a supplied hash',async()=>{
  const c=contract('meeting_followup_packet'),r=await record(c,meetingRows(),true);await expect(validateDataChecksRecord(r)).resolves.toEqual(r);const changed=structuredClone(r);changed.tables[0].body.rows[0][2]='Changed transcript field';await expect(evaluateDataChecks(changed)).rejects.toThrow('hash');
  const bad=structuredClone(r);bad.tables[0].mapping.transcript_text='missing column';await expect(validateDataChecksRecord(bad)).rejects.toThrow('Map declared fields');
  const out=await evaluateDataChecks(r);expect(out.inputs[0].source_sha256).toBe(r.tables[0].source_sha256);expect(out.inputs[0].mapping_sha256).toMatch(/^[a-f0-9]{64}$/);
});
test('CSV transport requires the existing parser callback and verifies exact retained file bytes',async()=>{
  const c=contract('meeting_followup_packet'),text='action_id,title,owner,due_date\nA1,Review,Owner,2026-10-22\n';let calls=0;
  const parseCSV=(body,limits)=>{calls++;expect(body).toBe(text);expect(limits.maxRows).toBe(1000);return {headers:['action_id','title','owner','due_date'],rows:[{row:2,values:['A1','Review','Owner','2026-10-22'],error:null}]};};
  await expect(importDataChecksCSV(c,'tracker_item',text)).rejects.toThrow('CSV reader');const table=await importDataChecksCSV(c,'tracker_item',text,{parseCSV,name:'uploaded.csv',example:false});const r={kind:'checks',title:'CSV source',contract:c,tables:[table],selected:'tracker_item'};
  const evaluated=await evaluateDataChecks(r,{parseCSV});expect(evaluated.inputs.find(x=>x.entity==='tracker_item').rows).toBe(1);expect(evaluated.field_checks.find(x=>x.entity==='tracker_item').inputs[0].row).toBe(2);expect(calls).toBe(2);r.tables[0].body+='tampered';await expect(evaluateDataChecks(r,{parseCSV:()=>({headers:['a'],rows:[]})})).rejects.toThrow('hash');
});
test('closed contracts refuse unknown effects, missing references, cycles and unsupported gates',()=>{
  for(const mutate of [c=>c.provider={endpoint:'https://example.test'},c=>c.relations[0].candidate_strategy.kind='execute',c=>c.predicates[0].field.field='missing',c=>c.relations[0].gates=['unreviewed'],c=>c.entities[0].key='missing']){const c=contract('meeting_followup_packet');mutate(c);expect(()=>validateDataChecksContract(c)).toThrow();}
  const cycle=contract('audio_edit_contract');cycle.predicates.push({id:'cycle',kind:'requires',when_predicate:'audio.gain_value_required',field:{entity:'edit_row',field:'parameter_value'}});cycle.predicates.find(p=>p.id==='audio.gain_value_required').when_predicate='cycle';expect(()=>validateDataChecksContract(cycle)).toThrow('cycle');
});
test('no input or check getters execute and oversized tables/candidate joins fail explicitly',async()=>{
  const c=contract('meeting_followup_packet');let invoked=0;Object.defineProperty(c,'secret',{enumerable:true,get(){invoked++;return 'x';}});expect(()=>validateDataChecksContract(c)).toThrow('ordinary');expect(invoked).toBe(0);
  await expect(importDataChecksRows(contract('meeting_followup_packet'),'tracker_item',{columns:['action_id'],rows:Array.from({length:1001},(_,i)=>[String(i)])})).rejects.toThrow('columns and rows');
  const audio=contract('audio_edit_contract'),rows=audioRows();rows.edit_row=Array.from({length:80},(_,i)=>({...rows.edit_row[0],row_id:'edit-'+i}));rows.media_singleton=Array.from({length:80},(_,i)=>({...rows.media_singleton[0],source_hash:i.toString(16).padStart(64,'0')}));await expect(evaluateDataChecks(await record(audio,rows))).rejects.toThrow('join inputs');
});
test('an input snapshot cannot change while hash work yields and exports reject mutated successful results',async()=>{
  const source=await record(contract('meeting_followup_packet'),meetingRows()),pending=evaluateDataChecks(source);source.tables[0].body.rows[0][2]='Caller mutation';const r=await pending;expect(r.draft_outputs[0].values.transcript_segment.text).toBe('Review the draft next week.');
  const changed=structuredClone(r);changed.field_checks[0].state='fail';await expect(exportDataChecksReport(changed)).rejects.toThrow('result changed');await expect(exportDataChecksDraft(changed)).rejects.toThrow('result changed');
});
test('canonical mapped-row identity preserves signed zero and import mappings are captured before awaits',async()=>{
  const c=contract('meeting_followup_packet'),mapping={owner:'person'},a=importDataChecksRows(c,'tracker_item',{columns:['person'],rows:[[-0]]},{mapping});mapping.owner='changed';const negative=await a,positive=await importDataChecksRows(c,'tracker_item',{columns:['person'],rows:[[0]]},{mapping:{owner:'person'}});
  expect(negative.source_sha256).not.toBe(positive.source_sha256);expect(negative.mapping).toEqual({owner:'person'});
});
function memoryStore(){const data=new Map();let writes=0;return {data,get writes(){return writes;},async readVersion(key){return structuredClone(data.get(key)??null);},async saveLocalBatch(changes){writes++;for(const c of changes)if(data.has(c.key))throw Error('Existing native record');const rows=changes.map(c=>({key:c.key,revision:1,value:structuredClone(c.value)}));for(const row of rows)data.set(row.key,row);return rows;}};}
async function recoveredChecks(){
  const value=await record(contract('meeting_followup_packet'),meetingRows(),true),production=addInstrument(createAuthoredWorkspace('Recovered input checks'),'checks');
  const envelope=[1,recoverySchema,{archive_sha256:'a'.repeat(64),workflows:[{recovery_key:'meeting-followup',source:{conversation_id:'fixture-source-session',message_ids:['session:fixture-source-session:line:42'],source_sha256:'b'.repeat(64)},production,records:[{key:production.panels[0].recordKey,value}]}]}];
  const validateInstrument=async(kind,value)=>{if(kind!=='checks')throw Error('Unexpected kind');await validateDataChecksRecord(value);return true;},material=await inspectRecoveryMaterial(envelope,{digestValue,validateInstrument}),target={context:'self',id:'ws-'+'c'.repeat(40),release:'d'.repeat(64),entry:'instruments/production/'},sourceStore=memoryStore(),opts={digestValue,validateInstrument,target,targetStore:sourceStore,verifyTarget:async()=>target};
  const review=await previewRecoveryWorkflow(material.workflows[0],opts),saved=await commitRecoveryWorkflow(review,opts);return {value,saved,sourceStore,validateInstrument,target};
}
test('recovered checks retain all private inputs, reopen as native work and publish only an empty reusable arrangement',async()=>{
  const f=await recoveredChecks(),saved=f.saved.production,child=f.sourceStore.data.get(saved.value.panels[0].recordKey);
  expect(child.value).toEqual(f.value);expect((await evaluateDataChecks(child.value)).draft_outputs).toHaveLength(2);expect(saved.value.recoveryOrigin.message_ids).toEqual(['session:fixture-source-session:line:42']);
  const design=publicProductionDesign(saved.value),serialized=new TextDecoder().decode(encode(design));expect(serialized).not.toContain('meeting_followup_packet');expect(serialized).not.toContain('source_sha256');expect(serialized).not.toContain('Review the draft next week');expect(serialized).not.toContain('recoveryOrigin');
  const instance=instantiatePublicDesign(design),reused=reuseWorkspaceDesign(saved.value);expect(instance.panels[0].kind).toBe('checks');expect(instance.panels[0].revision).toBe(0);expect(reused.recoveryOrigin).toBeUndefined();expect(reused.panels[0].recordKey).not.toBe(saved.value.panels[0].recordKey);
  const blank=blankInstrument(instance.panels[0].kind);await expect(validateDataChecksRecord(blank)).resolves.toEqual(blank);expect(blank.tables).toHaveLength(0);expect(blank.contract.fields).toHaveLength(0);expect((await evaluateDataChecks(blank)).summary.state).toBe('needs_input');
});
test('private destination transfer preserves checks only through the strict current validator and exact native closure',async()=>{
  const f=await recoveredChecks(),sourceEndpoint='/apps/'+f.target.id+'/_records',target={...f.target,id:'ws-'+'e'.repeat(40),release:'f'.repeat(64)},base={sourceEndpoint,sourceRelease:f.target.release,productionKey:f.saved.production.key,productionRevision:1,sourceStore:f.sourceStore,target,digestValue};
  await expect(prepareProductionTransfer(base)).rejects.toThrow('current native validator');const plan=await prepareProductionTransfer({...base,validateInstrument:f.validateInstrument}),targetStore=memoryStore();
  expect(plan.changes[0].value).toEqual(f.value);await expect(commitProductionTransfer(plan,{sourceStore:f.sourceStore,targetStore,verifyTarget:async()=>true,digestValue})).rejects.toThrow('current native validator');expect(targetStore.writes).toBe(0);
  const saved=await commitProductionTransfer(plan,{sourceStore:f.sourceStore,targetStore,verifyTarget:async()=>true,digestValue,validateInstrument:f.validateInstrument});expect(saved.key).toBe(productionPrefix+saved.value.id);expect(targetStore.data.get(saved.value.panels[0].recordKey).value).toEqual(f.value);expect(saved.value.recoveryOrigin).toEqual(f.saved.production.value.recoveryOrigin);expect(saved.value.transferFrom.productionKey).toBe(f.saved.production.key);expect(targetStore.writes).toBe(1);
  f.sourceStore.data.get(f.saved.production.value.panels[0].recordKey).value.tables[0].body.rows[0][2]='Corrupt input';await expect(prepareProductionTransfer({...base,validateInstrument:f.validateInstrument})).rejects.toThrow('hash');
});
