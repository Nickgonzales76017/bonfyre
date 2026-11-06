// Reviewed archive material becomes ordinary private, editable instrument
// records. The authenticated workspace owner keeps all grants and writes.
import {validateAuthoredWorkspace,validateRecoveryOrigin,prepareAuthoredBatch,validateAuthoredReceipt,productionPrefix} from './authored-workspace.mjs';

export const recoverySchema='bonfyre.authored-workspace-recovery.v1';
export const recoveryFileLimit=32*1024*1024;
const workflowSchema='bonfyre.authored-workspace-recovery-workflow.v1',reviewSchema='bonfyre.authored-workspace-recovery-review.v1';
const hashPattern=/^[a-f0-9]{64}$/,utf8=new TextEncoder();
const fail=(ok,message,code='recovery_invalid')=>{if(!ok){const e=Error(message);e.code=code;throw e;}};
const exact=(v,keys)=>v&&[Object.prototype,null].includes(Object.getPrototypeOf(v))&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const productionFields=['kind','schema','id','title','description','layout','appearance','panels','connections','focus'];
const instrumentFields={research:['kind','title','sources','findings','selectedSource','selectedFinding','tab'],product:['kind','title','stories','releases','selected'],traces:['kind','title','text','mapping','selectedCase','selectedEvent'],schema:['kind','title','bytes','selected','positions','acceptedSQL'],checks:['kind','title','contract','tables','selected']};

// Bound and reject accessors before taking a snapshot. The native record carrier
// retains large binary values as assets; no script or transcript is evaluated.
function materialSize(value,{native=false,max=recoveryFileLimit}={}){
  let nodes=0,assets=0;const active=new Set();
  function walk(v,depth=0,pathBytes=0){
    fail(depth<=30&&++nodes<=250000,'The recovery material exceeds its nesting or item limit.');
    if(typeof SharedArrayBuffer!=='undefined'&&(v instanceof SharedArrayBuffer||ArrayBuffer.isView(v)&&v.buffer instanceof SharedArrayBuffer))fail(false,'Shared mutable memory cannot enter a reviewed recovery.');
    if(v instanceof ArrayBuffer||v instanceof Uint8Array){const size=v.byteLength;fail(size<=24*1024*1024&&++assets<=64,'A recovery asset exceeds the native limit.');return native&&size>=65536?2048+pathBytes:size+5;}
    if(v===null||typeof v==='boolean')return 1;
    if(typeof v==='number'){fail(Number.isFinite(v),'Recovery numbers must be finite.');return 9;}
    if(typeof v==='string')return utf8.encode(v).length+5;
    fail(v&&typeof v==='object'&&!active.has(v),'Recovery material must be plain acyclic data.');
    const array=Array.isArray(v),keys=Reflect.ownKeys(v),descriptors=Object.getOwnPropertyDescriptors(v);
    fail(array||[Object.prototype,null].includes(Object.getPrototypeOf(v)),'Unsupported recovery value.');
    fail(keys.every(k=>typeof k==='string'&&(array&&k==='length'||descriptors[k].enumerable&&Object.hasOwn(descriptors[k],'value'))),'Recovery properties must be ordinary data.');
    fail(array?v.length<=20000&&keys.length===v.length+1:keys.length<=4096,'Recovery material has too many or missing properties.');
    active.add(v);let size=5;
    for(const key of keys){
      if(array&&key==='length')continue;
      fail(!array||/^(0|[1-9][0-9]*)$/.test(key)&&Number(key)<v.length,'Recovery arrays must be dense.');
      const n=utf8.encode(key).length;fail(n<=512,'Recovery property names are too long.');
      size+=(array?0:n+5)+walk(descriptors[key].value,depth+1,pathBytes+n+5);fail(size<=max,'The recovery material exceeds its byte limit.');
    }
    active.delete(v);return size;
  }
  const size=walk(value)+32;fail(size<=max,'The recovery material exceeds its byte limit.');return size;
}
function snapshot(value){materialSize(value);return structuredClone(value);}
async function hash(value,digestValue){fail(typeof digestValue==='function','Use the native byte-complete value digest.');const h=await digestValue(value);fail(hashPattern.test(h),'Invalid native material digest.');return h;}
function targetIdentity(target){
  fail(exact(target,['context','id','release','entry'])&&typeof target.context==='string'&&/^(self|[A-Za-z0-9][A-Za-z0-9:._-]{0,199})$/.test(target.context)&&/^ws-[a-f0-9]{40}$/.test(target.id)&&hashPattern.test(target.release)&&target.entry==='instruments/production/','Choose the currently admitted Production Studio destination.');return target;
}
async function admit(target,verifyTarget){
  fail(typeof verifyTarget==='function','Use current native destination admission.');const current=await verifyTarget(structuredClone(target));
  fail(current===true||current&&['context','id','release','entry'].every(k=>current[k]===target[k]),'The selected destination or its write admission changed. Review recovery again.','recovery_admission_changed');
}
async function recoveryId(origin,digestValue){
  return 'recovered-'+await hash({schema:'bonfyre.authored-workspace-recovery-key.v1',conversation_id:origin.conversation_id,message_ids:origin.message_ids,recovery_key:origin.recovery_key},digestValue);
}
function summary(production){return {title:production.title,description:production.description,recovery_key:production.recoveryOrigin.recovery_key,panels:production.panels.map(p=>({kind:p.kind,label:p.label,role:p.role})),recordCount:production.panels.length+1};}
async function instrument(kind,value,validateInstrument){
  fail(Object.hasOwn(instrumentFields,kind)&&exact(value,instrumentFields[kind])&&value.kind===kind,'An imported record differs from its declared native instrument.');
  // Native research adoption requires a current retained incoming receipt. An
  // archive cannot manufacture that receipt or claim its original authority.
  if(kind==='product')fail(Array.isArray(value.stories)&&value.stories.every(s=>s&&typeof s==='object'&&!Object.hasOwn(s,'origin')),'Recovered stories cannot claim native adoption receipts.');
  if(kind==='research')fail(Array.isArray(value.sources)&&value.sources.every(s=>s&&typeof s==='object'&&!Object.hasOwn(s,'publication')),'Recovered sources cannot claim native publication receipts.');
  materialSize(value,{native:true,max:256*1024});
  fail(typeof validateInstrument==='function'&&await validateInstrument(kind,structuredClone(value))===true,'Review every imported instrument with its native domain validator.');
}
async function workflowDigest(workflow,digestValue){
  let bytes=128;const changes=[];
  for(const c of workflow.changes){bytes+=materialSize(c.value,{native:true,max:256*1024})+utf8.encode(c.key).length+32;changes.push({key:c.key,expectedRevision:c.expectedRevision,digest:await hash(c.value,digestValue)});}
  fail(bytes<=512*1024,'The complete recovery exceeds the native atomic batch limit.');
  return hash({schema:workflow.schema,productionKey:workflow.productionKey,productionDigest:await hash(workflow.production,digestValue),summary:workflow.summary,changes},digestValue);
}
async function compileWorkflow(input,archiveHash,{digestValue,validateInstrument}){
  fail(exact(input,['recovery_key','source','production','records'])&&exact(input.source,['conversation_id','message_ids','source_sha256']),'Unsupported recovery workflow or source fields.');
  const origin=validateRecoveryOrigin({schema:'bonfyre.authored-workspace-recovery-origin.v1',archive_sha256:archiveHash,recovery_key:input.recovery_key,conversation_id:input.source.conversation_id,message_ids:[...input.source.message_ids].sort(),source_sha256:input.source.source_sha256});
  const original=validateAuthoredWorkspace(input.production);
  fail(exact(original,productionFields)&&original.panels.length>0&&original.panels.every(p=>p.revision===0&&exact(p,['id','kind','label','role','recordKey','revision']))&&original.connections.every(e=>exact(e,['from','to','operation'])),'Recover a new native composition with one to four unsaved instruments.');
  fail(Array.isArray(input.records)&&input.records.length===original.panels.length&&input.records.every(r=>exact(r,['key','value']))&&new Set(input.records.map(r=>r.key)).size===input.records.length,'Supply exactly the bound native instrument records.');
  const id=await recoveryId(origin,digestValue),next=structuredClone(original),ids=new Map(),changes=[],records=[];next.id=id;next.recoveryOrigin=origin;
  for(const [i,panel] of next.panels.entries()){
    const row=input.records.find(r=>r.key===panel.recordKey);fail(row,'A bound instrument is missing from recovery.');
    await instrument(panel.kind,row.value,validateInstrument);
    const newId=id+'-p'+(i+1);ids.set(panel.id,newId);panel.id=newId;panel.recordKey='draft:instruments:'+panel.kind+':'+newId;
    records.push({id:panel.id,kind:panel.kind,key:panel.recordKey,revision:0});changes.push({key:panel.recordKey,expectedRevision:0,value:structuredClone(row.value)});
  }
  next.connections=next.connections.map(e=>({...e,from:ids.get(e.from),to:ids.get(e.to)}));next.focus=ids.get(next.focus)??null;
  const prepared=prepareAuthoredBatch(next,0,records,changes),workflow={schema:workflowSchema,productionKey:productionPrefix+id,production:prepared.production,changes:prepared.changes,summary:summary(prepared.production)};
  workflow.digest=await workflowDigest(workflow,digestValue);return workflow;
}

export async function inspectRecoveryMaterial(decoded,options={}){
  const kept=snapshot(decoded);fail(Array.isArray(kept)&&kept.length===3&&kept[0]===1&&kept[1]===recoverySchema&&exact(kept[2],['archive_sha256','workflows'])&&hashPattern.test(kept[2].archive_sha256)&&Array.isArray(kept[2].workflows)&&kept[2].workflows.length>0&&kept[2].workflows.length<=1000,'Choose a bounded native recovery MessagePack file.');
  const workflows=[],keys=new Set();for(const input of kept[2].workflows){const w=await compileWorkflow(input,kept[2].archive_sha256,options);fail(!keys.has(w.productionKey),'The recovery file repeats the same source workflow.');keys.add(w.productionKey);workflows.push(w);}
  return {schema:recoverySchema,archive_sha256:kept[2].archive_sha256,workflows};
}
async function validateWorkflow(workflow,{digestValue,validateInstrument}){
  fail(exact(workflow,['schema','productionKey','production','changes','summary','digest'])&&workflow.schema===workflowSchema&&hashPattern.test(workflow.digest)&&Array.isArray(workflow.changes),'Unsupported reviewed recovery.');
  const p=validateAuthoredWorkspace(workflow.production,{saved:true}),origin=validateRecoveryOrigin(p.recoveryOrigin),id=await recoveryId(origin,digestValue);
  fail(exact(p,[...productionFields,'recoveryOrigin'])&&p.id===id&&workflow.productionKey===productionPrefix+id&&p.panels.length>0&&workflow.changes.length===p.panels.length+1,'The recovery source or record closure changed.');
  const records=[],offered=[];
  for(const [i,panel] of p.panels.entries()){
    fail(exact(panel,['id','kind','label','role','recordKey','revision'])&&panel.revision===1&&panel.id===id+'-p'+(i+1)&&panel.recordKey==='draft:instruments:'+panel.kind+':'+panel.id,'The recovery instrument coordinates changed.');
    const c=workflow.changes[i];fail(exact(c,['key','expectedRevision','value'])&&c.key===panel.recordKey&&c.expectedRevision===0,'Recovery only creates the exact reviewed new records.');
    await instrument(panel.kind,c.value,validateInstrument);records.push({id:panel.id,kind:panel.kind,key:panel.recordKey,revision:0});offered.push(c);
  }
  fail(p.connections.every(e=>exact(e,['from','to','operation'])),'Unsupported recovery handoff fields.');
  const root=workflow.changes.at(-1);fail(exact(root,['key','expectedRevision','value'])&&root.key===workflow.productionKey&&root.expectedRevision===0,'Recovery must create its composition in the same native batch.');
  const unsaved=structuredClone(p);unsaved.panels.forEach(panel=>panel.revision=0);const prepared=prepareAuthoredBatch(unsaved,0,records,offered);
  fail(await hash(root.value,digestValue)===await hash(prepared.production,digestValue)&&await hash(workflow.summary,digestValue)===await hash(summary(p),digestValue)&&await workflowDigest(workflow,digestValue)===workflow.digest,'The reviewed recovery changed. Inspect the file again before writing.');return workflow;
}
async function readClosure(workflow,store,digestValue){
  fail(typeof store?.readVersion==='function','Use the current native record store.');const rows=[],conflicts=[];
  for(const c of workflow.changes){const got=await store.readVersion(c.key),row=got===null?null:snapshot(got);rows.push(row);
    if(row){let reason=null;if(row.key!==c.key)reason='coordinates';else if(row.deleted)reason='deleted';else if(row.revision!==1)reason='revision';else if(await hash(row.value,digestValue)!==await hash(c.value,digestValue))reason='material';if(reason)conflicts.push({key:c.key,reason,revision:Number.isSafeInteger(row.revision)?row.revision:null});}}
  const present=rows.filter(Boolean).length;if(!present)return {state:'ready',rows:[],conflicts:[]};
  if(present!==rows.length)for(const [i,row]of rows.entries())if(!row)conflicts.push({key:workflow.changes[i].key,reason:'partial',revision:null});
  return conflicts.length?{state:'conflict',rows:[],conflicts}:{state:'retained',rows,conflicts:[]};
}
export async function reconcileRecoveryWorkflow(workflow,{targetStore,digestValue,validateInstrument}={}){
  const kept=snapshot(workflow);await validateWorkflow(kept,{digestValue,validateInstrument});return readClosure(kept,targetStore,digestValue);
}
async function reviewDigest(plan,digestValue){return hash({schema:plan.schema,target:plan.target,workflow:plan.workflow.digest,state:plan.state,conflicts:plan.conflicts,summary:plan.summary},digestValue);}
export async function previewRecoveryWorkflow(workflow,{target,targetStore,verifyTarget,digestValue,validateInstrument}={}){
  const kept=snapshot(workflow),destination=targetIdentity(snapshot(target));await validateWorkflow(kept,{digestValue,validateInstrument});await admit(destination,verifyTarget);
  const current=await readClosure(kept,targetStore,digestValue),plan={schema:reviewSchema,target:destination,workflow:kept,state:current.state,conflicts:current.conflicts,summary:structuredClone(kept.summary)};
  plan.digest=await reviewDigest(plan,digestValue);return plan;
}
async function validateReview(plan,options){
  fail(exact(plan,['schema','target','workflow','state','conflicts','summary','digest'])&&plan.schema===reviewSchema&&hashPattern.test(plan.digest)&&['ready','retained','conflict'].includes(plan.state)&&Array.isArray(plan.conflicts),'Unsupported recovery review.');targetIdentity(plan.target);await validateWorkflow(plan.workflow,options);
  fail(await hash(plan.summary,options.digestValue)===await hash(plan.workflow.summary,options.digestValue)&&await reviewDigest(plan,options.digestValue)===plan.digest,'The reviewed recovery or destination changed. Review it again.');
}
const result=(state,workflow,rows)=>({state,production:structuredClone(rows.find(r=>r.key===workflow.productionKey)),records:structuredClone(rows)});
async function confirmedResult(state,workflow,rows,target,verifyTarget){
  // The native record transaction cannot compare the receiving editor release.
  // Do not report its opener as ready if installation or admission changed
  // while writing or reconciling the retained composition and children.
  try{await admit(target,verifyTarget);}catch(error){
    const e=Error('The native work is retained, but its current editor admission is unconfirmed. Keep this review and reconcile the same work in its original destination before continuing.');e.code='recovery_uncertain';e.cause=error;throw e;
  }
  return result(state,workflow,rows);
}
export async function commitRecoveryWorkflow(plan,{targetStore,verifyTarget,digestValue,validateInstrument}={}){
  const kept=snapshot(plan);await validateReview(kept,{digestValue,validateInstrument});fail(kept.state!=='conflict','Existing recovery records changed. Open that retained work; no copy was overwritten.','recovery_conflict');
  fail(typeof targetStore?.saveLocalBatch==='function','Use the admitted native batch writer.');await admit(kept.target,verifyTarget);
  let current=await readClosure(kept.workflow,targetStore,digestValue);fail(current.state!=='conflict','Existing recovery records changed. Reconcile the complete work before continuing.','recovery_conflict');
  if(current.state==='retained')return confirmedResult('retained',kept.workflow,current.rows,kept.target,verifyTarget);
  fail(kept.state!=='retained','The previously retained recovery is no longer readable. Reconcile its original destination before creating anything.','recovery_conflict');
  let disposition='created';
  try{
    const rows=await targetStore.saveLocalBatch(structuredClone(kept.workflow.changes));validateAuthoredReceipt({production:kept.workflow.production,changes:kept.workflow.changes},rows);
    // A replayed native receipt may describe revision one after later edits.
    // Read current bytes for the whole closure before reporting attachment.
    current=await readClosure(kept.workflow,targetStore,digestValue);fail(current.state==='retained','The native work changed before complete readback. Reconcile it before continuing.','recovery_conflict');
  }catch(error){
    try{current=await readClosure(kept.workflow,targetStore,digestValue);}catch{fail(false,'The save outcome is unconfirmed. Keep this recovery file and check the same work before retrying.','recovery_uncertain');}
    if(current.state==='retained')disposition='recovered';
    if(current.state==='conflict')fail(false,'The retained work differs from the reviewed recovery. Reconcile all its instruments before continuing.','recovery_conflict');
    if(current.state!=='retained'){const e=Error('The recovery was not confirmed. The same source identities are retained for a reviewed retry.');e.code='recovery_unconfirmed';e.cause=error;throw e;}
  }
  return confirmedResult(disposition,kept.workflow,current.rows,kept.target,verifyTarget);
}
// Each production is one native atomic batch. Stop at the first unconfirmed
// result; reopening the same file reconciles its entire closure before resume.
export async function commitRecoveryBatch(plans,options={}){
  const kept=snapshot(plans),limit=options.limit??10;fail(Array.isArray(kept)&&kept.length>0&&kept.length<=1000&&Number.isInteger(limit)&&limit>=1&&limit<=25,'Choose one to twenty-five sequential recoveries per batch.');
  const selected=kept.slice(0,limit);for(const plan of selected){await validateReview(plan,options);fail(plan.state!=='conflict','Reconcile the conflicting work before selecting this batch.','recovery_conflict');}const results=[];
  for(const [index,plan]of selected.entries()){
    try{const saved=await commitRecoveryWorkflow(plan,options);results.push(saved);if(options.onResult)await options.onResult(structuredClone(saved),index);}
    catch(error){return {state:'stopped',results,nextIndex:results.length,error:{code:error.code??'recovery_unconfirmed',message:error.message,index}};}
  }
  return {state:kept.length>selected.length?'paused':'complete',results,nextIndex:results.length};
}
