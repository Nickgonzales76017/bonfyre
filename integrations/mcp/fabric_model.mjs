// A reviewed account delegation is the producer for model work. The existing
// Fabric store owns tasks, physical attempts, outputs and their scoped feed.
import {createHash,randomUUID} from 'node:crypto';
import {closeSync,constants,existsSync,fstatSync,fsyncSync,linkSync,lstatSync,mkdirSync,openSync,
  readSync,readdirSync,unlinkSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {encode,decode} from '../browser/workspace-msgpack.mjs';
import {FabricActivityError,fabricActivityBinding} from './fabric_store.mjs';
import {modelLimits} from './native_model.mjs';

const fail=(condition,code,message)=>{if(!condition)throw new FabricActivityError(code,message);};
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const digest=value=>sha(encode(value,{maxBytes:131072}));
const stamp=at=>new Date(at).toISOString().replace(/\.\d{3}Z$/,'Z');
const publicPrice=price=>price.state==='priced'?{...price,rateId:String(price.rateId)}:{state:'unknown'};
const equal=(a,b)=>a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope;
const requestId=value=>{fail(typeof value==='string'&&/^[A-Za-z0-9_.:-]{8,100}$/.test(value),'invalid','Retain one request identity for this action.');return value;};
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===keys.length&&Object.keys(value).every(key=>keys.includes(key));
const publicModel=model=>({id:model.id,label:model.label,catalogId:model.catalogId??null,provider:model.provider,execution:'local',available:true,maxTokens:model.maxTokens,maxTimeoutMs:model.maxTimeoutMs});
const phases={queued:'pending',running:'active',completed:'completed',failed:'failed',cancelled:'cancelled',blocked:'blocked',review_required:'review',uncertain:'uncertain'};

function privateIntents(home) {
  const root=join(home,'model-intents');
  function directory(){
    if(existsSync(root))fail(!lstatSync(root).isSymbolicLink()&&lstatSync(root).isDirectory(),'unavailable','The model review owner is unavailable.');
    else mkdirSync(root,{recursive:true,mode:0o700});
  }
  function path(id){fail(/^[a-f0-9]{48}$/.test(id),'invalid','Use the retained model review identity.');directory();return join(root,id+'.msgpack');}
  function read(id) {
    const file=path(id);let fd;
    try{fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);}catch(error){if(error.code==='ENOENT')return null;throw error;}
    try {
      const before=fstatSync(fd,{bigint:true});fail(before.isFile()&&before.size<=131072n,'unavailable','The retained model review is invalid.');
      const buffer=Buffer.alloc(Number(before.size)+1);let used=0;
      while(used<buffer.length){const count=readSync(fd,buffer,used,buffer.length-used,null);if(!count)break;used+=count;}
      const bytes=buffer.subarray(0,used),after=fstatSync(fd,{bigint:true});
      fail(bytes.length===Number(before.size)&&before.size===after.size&&before.mtimeNs===after.mtimeNs&&before.ctimeNs===after.ctimeNs,
        'unavailable','The retained model review changed during its read.');
      const value=decode(bytes,{maxBytes:131072});fail(value?.schema===1&&value.id===id,'unavailable','The retained model review is incomplete.');return value;
    }finally{closeSync(fd);}
  }
  function put(value) {
    const file=path(value.id),body=encode(value,{maxBytes:131072}),tmp=file+'.'+randomUUID()+'.tmp';let fd;
    try {
      const retained=readdirSync(root);fail(retained.length<2000,'capacity','The retained model review limit has been reached.');
      fd=openSync(tmp,'wx',0o600);writeFileSync(fd,body);fsyncSync(fd);closeSync(fd);fd=undefined;
      try{linkSync(tmp,file);}catch(error){if(error.code!=='EEXIST')throw error;}
      const owner=openSync(root,'r');try{fsyncSync(owner);}finally{closeSync(owner);}
    }finally{if(fd!==undefined)closeSync(fd);if(existsSync(tmp))unlinkSync(tmp);}
    const saved=read(value.id);fail(saved?.requestDigest===value.requestDigest,'conflict','This request identity already names another action.');return saved;
  }
  return {read,put};
}

export function createFabricModelWork({store,native,meter,authorizeAccount,admit,enqueue,execution,
  now=()=>Date.now(),reviewTtlMs=300000}={}) {
  fail(store&&native&&meter&&['authorizeAccount','admit','enqueue','execution'].every(key=>
    typeof ({authorizeAccount,admit,enqueue,execution})[key]==='function'),'unavailable','The native model owners are incomplete.');
  fail(Number.isSafeInteger(reviewTtlMs)&&reviewTtlMs>=1000&&reviewTtlMs<=600000,'unavailable','The model review lifetime is invalid.');
  const intents=privateIntents(store.home),reviews=new Map(),active=new Map(),settling=new Map();let closed=false;
  const clock=()=>{const at=now();fail(Number.isSafeInteger(at)&&at>=0,'unavailable','The model clock is unavailable.');return at;};
  const intentId=(kind,binding,id)=>digest([1,kind,binding.actor,binding.subject,binding.scope,id]).slice(0,48);
  const save=(id,kind,value)=>store.saveReceipt(id,kind,value,'msgpack');
  async function access(binding,permission) {
    fail(!closed,'unavailable','The model work owner is closed.');
    let allowed;try{allowed=await authorizeAccount({...binding,permission});}catch{throw new FabricActivityError('unavailable','The current native account authority is unavailable.');}
    fail(allowed===true,'admission','This account no longer has access to the selected destination.');
  }
  function expected(input,binding) {
    fail(equal(fabricActivityBinding(input.expectedBinding),binding),'conflict','The active account or destination changed. Reopen this review.');
  }
  function owned(id,binding,{initiator=false}={}) {
    fail(typeof id==='string'&&/^[a-f0-9]{24}$/.test(id),'invalid','Choose a retained model task.');
    const scope=store.taskScope(id);
    fail(scope&&scope.subject===binding.subject&&scope.scope===binding.scope&&(!initiator||scope.actor===binding.actor),
      'not_found','This model work is not available in the current destination.');
    const task=store.task(id);fail(task.kind==='model','not_found','This task is not model work.');return {task,scope};
  }
  function retained(id) {
    const value=store.readReceipt(id,'model-request')?.value;
    fail(value?.schema===1&&value.taskId===id&&value.calls===1,'unavailable','The admitted model request needs reconciliation.');return value;
  }
  const reviewView=value=>({id:value.id,requestId:value.requestId,modelId:value.model.id,label:value.model.label,
    promptSha256:value.promptSha256,promptBytes:Buffer.byteLength(value.prompt),maxTokens:value.maxTokens,
    timeoutMs:value.timeoutMs,expiresAt:value.expiresAt,pricing:publicPrice(value.pricing),execution:'local',calls:1});
  async function capabilities(binding) {
    await access(binding,'read');const models=await native.capabilities();
    fail(Array.isArray(models)&&models.length<=64,'unavailable','The native model inventory is incomplete.');
    return {binding,models:models.map(publicModel),limits:{maxPromptBytes:modelLimits.maxPromptBytes,maxTokens:modelLimits.maxTokens,maxTimeoutMs:modelLimits.maxTimeoutMs}};
  }
  async function review(binding,input) {
    fail(exact(input,['expectedBinding','requestId','modelId','prompt','maxTokens','timeoutMs']),'invalid','Use the exact model review fields.');
    expected(input,binding);requestId(input.requestId);
    fail(typeof input.prompt==='string'&&input.prompt.trim()&&!input.prompt.includes('\0')&&Buffer.byteLength(input.prompt)<=modelLimits.maxPromptBytes&&
      typeof input.modelId==='string'&&input.modelId.length<=220&&Number.isSafeInteger(input.maxTokens)&&input.maxTokens>=1&&input.maxTokens<=modelLimits.maxTokens&&
      Number.isSafeInteger(input.timeoutMs)&&input.timeoutMs>=1000&&input.timeoutMs<=modelLimits.maxTimeoutMs,
      'invalid','Choose a local model, prompt and bounded generation budget.');
    await access(binding,'write');
    const id=intentId('review',binding,input.requestId),requestDigest=digest([1,input.modelId,input.prompt,input.maxTokens,input.timeoutMs]);
    const previous=intents.read(id);
    if(previous){fail(previous.requestDigest===requestDigest&&equal(previous.binding,binding),'conflict','This review identity already names different work.');
      fail(clock()<previous.expiresAt,'conflict','This review expired. Create a new review.');return {binding,review:reviewView(previous)};}
    if(reviews.has(id)){await reviews.get(id);return review(binding,input);}
    const work=(async()=>{
      const model=await native.inspect(input.modelId);
      const pricing=await meter.quote({provider:model.provider,operation:'model-generation',at:stamp(clock())});
      fail(pricing&&['unknown','priced'].includes(pricing.state),'unavailable','The native cost-rate owner is unavailable.');
      const value={schema:1,id,requestId:input.requestId,requestDigest,binding,model,prompt:input.prompt,
        promptSha256:sha(Buffer.from(input.prompt)),maxTokens:input.maxTokens,timeoutMs:input.timeoutMs,
        pricing,createdAt:clock(),expiresAt:clock()+reviewTtlMs};
      const saved=intents.put(value);return {binding,review:reviewView(saved)};
    })();reviews.set(id,work);try{return await work;}finally{if(reviews.get(id)===work)reviews.delete(id);}
  }
  function submitReply(binding,id,replayed) {
    const row=store.task(id),witness=execution(id);
    const reconciliationRequired=row.state==='uncertain'||['queued','running'].includes(row.state)&&!witness.owned&&!witness.queued;
    return {binding,taskId:id,state:row.state,phase:reconciliationRequired?'uncertain':phases[row.state],replayed,reconciliationRequired};
  }
  async function submit(binding,input) {
    fail(exact(input,['expectedBinding','requestId','reviewId']),'invalid','Use the retained model review and submission identity.');
    expected(input,binding);requestId(input.requestId);fail(typeof input.reviewId==='string'&&/^[a-f0-9]{48}$/.test(input.reviewId),'invalid','Use the retained model review identity.');
    await access(binding,'write');
    const old=store.findScopedTask(binding,input.requestId);
    if(old){owned(old.task_id,binding,{initiator:true});const request=retained(old.task_id);
      fail(request.reviewId===input.reviewId&&request.requestId===input.requestId,'conflict','This submission identity already names another review.');
      const witness=execution(old.task_id);
      if(!active.has(old.task_id)&&!witness.owned&&!witness.queued&&store.readReceipt(old.task_id,'model-result')?.value?.state==='completed'&&
         (!store.readReceipt(old.task_id,'model-usage')||['running','uncertain'].includes(old.state)))
        await reconcileOwned(old.task_id);
      return submitReply(binding,old.task_id,true);}
    const value=intents.read(input.reviewId);
    fail(value&&equal(value.binding,binding),'not_found','This model review is not available in the current destination.');
    fail(clock()<value.expiresAt,'conflict','This review expired. Create a new review.');
    const current=await native.inspect(value.model.id);
    fail(current.witness===value.model.witness,'conflict','The native model changed after review. Review it again.');
    const price=await meter.quote({provider:value.model.provider,operation:'model-generation',at:stamp(clock())});
    fail(digest(price)===digest(value.pricing),'conflict','The admitted cost rate changed. Review the run again.');
    await access(binding,'write');
    const consumptionId=intentId('review-consumption',binding,value.id);
    intents.put({schema:1,id:consumptionId,binding,requestId:input.requestId,reviewId:value.id,
      requestDigest:digest([1,value.id,input.requestId])});
    // These synchronous steps share the Fabric admission/queue owner. A replay
    // never requeues work whose previous physical outcome may be uncertain.
    admit({enqueue:true});
    const made=store.createScopedTask({kind:'model',title:value.model.label.slice(0,450)+' · Model run',
      prompt:value.prompt,requestId:input.requestId,apply_to_project:false},binding);
    if(made.replayed){const request=retained(made.task_id);fail(request.reviewId===input.reviewId,'conflict','This submission identity names another review.');return submitReply(binding,made.task_id,true);}
    try {
      save(made.task_id,'model-request',{schema:1,taskId:made.task_id,binding,reviewId:value.id,
        requestId:input.requestId,model:value.model,promptSha256:value.promptSha256,maxTokens:value.maxTokens,
        timeoutMs:value.timeoutMs,pricing:value.pricing,calls:1,admittedAt:clock()});
      enqueue(made.task_id);
    }catch(error){store.updateTask(made.task_id,{state:'uncertain',completed_at:clock()/1000,error:'The model admission needs reconciliation before execution.'});throw error;}
    return submitReply(binding,made.task_id,false);
  }

  function view(binding,id,{canWrite=false}={}) {
    const {task,scope}=owned(id,binding),request=retained(id),attempt=store.readReceipt(id,'model-attempt')?.value,
      result=store.readReceipt(id,'model-result')?.value,usage=store.readReceipt(id,'model-usage')?.value,
      output=store.readReceipt(id,'model-output');
    const witness=execution(id),lost=['queued','running'].includes(task.state)&&!witness.owned&&!witness.queued;
    const uncertain=task.state==='uncertain'||lost||Boolean(result?.reconciliationRequired)||Boolean(attempt&&!result&&!witness.owned);
    let shown=null;
    if(output&&result?.state==='completed') {
      fail(typeof output.value==='string'&&Buffer.byteLength(output.value)<=modelLimits.maxOutputBytes&&
        sha(Buffer.from(output.value))===output.receipt.sha256,'unavailable','The retained model output failed its integrity check.');
      shown={id:output.receipt.receipt_id,text:output.value,digest:output.receipt.sha256,bytes:Buffer.byteLength(output.value)};
    }
    return {binding,taskId:id,state:task.state,phase:uncertain?'uncertain':phases[task.state],
      model:{id:request.model.id,label:request.model.label,provider:request.model.provider},output:shown,
      usage:usage?{state:'recorded',receiptId:store.readReceipt(id,'model-usage').receipt.receipt_id,
        attemptRef:usage.attemptRef,durationMs:usage.durationMs,outputBytes:usage.outputBytes,pricing:publicPrice(usage.pricing),
        ...(usage.pricing.state==='priced'&&Number.isFinite(usage.pricing.cost)?{cost:usage.pricing.cost}:{})}:attempt?{
        state:'pending',receiptId:null,attemptRef:attempt.attemptRef,durationMs:result?.durationMs??null,outputBytes:shown?.bytes??null,
        pricing:{state:'unknown'}}:null,
      error:task.error||result?.error||null,reconciliationRequired:uncertain,
      localTransferAllowed:canWrite&&scope.actor===binding.actor&&Boolean(shown),
      reconciliationAllowed:canWrite&&scope.actor===binding.actor&&Boolean(shown)&&(!usage||task.state==='uncertain'||lost)&&!witness.owned&&!witness.queued,
      cancellationAllowed:canWrite&&scope.actor===binding.actor&&['queued','running'].includes(task.state)};
  }
  async function read(binding,id) {
    await access(binding,'read');owned(id,binding);let canWrite=false;
    try{canWrite=await authorizeAccount({...binding,permission:'write'})===true;}catch{}
    return view(binding,id,{canWrite});
  }

  async function settle(id,request,result,output) {
    const attempt=store.readReceipt(id,'model-attempt')?.value;
    fail(attempt&&result&&output,'unavailable','The physical model attempt is incomplete.');
    const metered=await meter.record({key:request.binding.subject,operation:'model-generation',provider:request.model.provider,
      receiptRef:'sha256:'+output.sha256,attemptRef:attempt.attemptRef,realizationRef:attempt.realizationRef,
      at:attempt.at,bytes:result.bytes,durationMs:result.durationMs});
    fail(metered?.state==='recorded','unavailable','Native usage recording is pending.');
    save(id,'model-usage',{...metered,state:'recorded',durationMs:result.durationMs,outputBytes:result.bytes,
      attemptRef:attempt.attemptRef,pricing:metered.pricing||{state:'unknown'}});return metered;
  }

  async function reconcileOwned(id) {
    if(settling.has(id))return settling.get(id);
    const work=(async()=>{
      const request=retained(id),result=store.readReceipt(id,'model-result')?.value,
        output=store.readReceipt(id,'model-output'),attempt=store.readReceipt(id,'model-attempt')?.value,witness=execution(id);
      fail(result?.state==='completed'&&output&&attempt&&!active.has(id)&&!witness.owned&&!witness.queued,
        'conflict','Only a retained completed model output can finish its usage receipt.');
      fail(typeof output.value==='string'&&result.bytes===Buffer.byteLength(output.value)&&result.digest===output.receipt.sha256,
        'unavailable','The retained physical model output is incomplete.');
      // A crash can retain the completed output and Meter receipt before its
      // final task-state update. First admit that the old running row has no
      // live executor, then use the same exact receipt guard as every recovery.
      if(store.task(id).state==='running')store.updateTask(id,{state:'uncertain',completed_at:clock()/1000,
        error:'The completed model output needs its retained usage outcome reconciled.'});
      if(!store.readReceipt(id,'model-usage'))await settle(id,request,result,output.receipt);
      if(store.task(id).state==='uncertain')store.completeModelUsage(id,attempt.attemptRef);
    })();settling.set(id,work);try{return await work;}finally{if(settling.get(id)===work)settling.delete(id);}
  }
  async function reconcileRequest(binding,input) {
    fail(exact(input,['expectedBinding','requestId','taskId']),'invalid','Use the exact model usage reconciliation fields.');
    expected(input,binding);requestId(input.requestId);await access(binding,'write');owned(input.taskId,binding,{initiator:true});
    const id=intentId('reconcile',binding,input.requestId),value={schema:1,id,binding,requestId:input.requestId,
      taskId:input.taskId,requestDigest:digest([1,input.taskId])};
    const previous=intents.read(id);if(previous)fail(previous.requestDigest===value.requestDigest,'conflict','This recovery identity names different work.');else intents.put(value);
    await reconcileOwned(input.taskId);return view(binding,input.taskId,{canWrite:true});
  }

  async function executeOwned(id,controller) {
    const task=store.claimTask(id);if(!task)return;
    const binding=store.taskScope(id);let request,attempted=false,output,result;
    try {
      fail(binding&&task.kind==='model','admission','This model task has no trusted account ownership.');request=retained(id);
      fail(equal(request.binding,binding)&&sha(Buffer.from(task.prompt))===request.promptSha256,'admission','The model task differs from its admitted request.');
      fail(!store.readReceipt(id,'model-attempt'),'conflict','A prior physical attempt cannot be replayed.');
      const beforeDispatch=async()=>{await access(binding,'write');controller.signal.throwIfAborted();
        fail(!store.isTaskCancelled(id),'admission','This model work was cancelled.');admit();};
      result=await native.generate({...request,prompt:task.prompt},{signal:controller.signal,beforeDispatch,
        onAttempt:async nativeWitness=>{
          // This witness precedes the only permitted model request. A crash
          // after it requires reconciliation, never an automatic generation.
          const at=stamp(clock());
          const pricing=await meter.quote({provider:request.model.provider,operation:'model-generation',at});
          fail(digest(pricing)===digest(request.pricing),'conflict','The admitted cost rate changed while this run waited. Review a new run.');
          await beforeDispatch();
          const attemptRef='fabric-model:'+id+':'+randomUUID(),
            realizationRef='bonfyre-moe:'+digest([1,nativeWitness,request.model.witness]);
          save(id,'model-attempt',{schema:1,taskId:id,attemptRef,realizationRef,at,native:nativeWitness});attempted=true;
        }});
      fail(typeof result?.text==='string'&&result.text.trim()&&Buffer.byteLength(result.text)<=modelLimits.maxOutputBytes&&
        result.digest===sha(Buffer.from(result.text))&&result.bytes===Buffer.byteLength(result.text)&&
        Number.isSafeInteger(result.durationMs)&&result.durationMs>=0&&result.durationMs<=modelLimits.maxTimeoutMs+5000,
        'unavailable','The native model returned an invalid physical receipt.');
      output=store.saveReceipt(id,'model-output',result.text,'txt');
      save(id,'model-result',{schema:1,state:'completed',outputId:output.receipt_id,digest:output.sha256,
        bytes:result.bytes,durationMs:result.durationMs,usage:result.usage||null,reconciliationRequired:false});
      await settle(id,request,result,output);
      if(!store.isTaskCancelled(id))store.updateTask(id,{state:'completed',completed_at:clock()/1000,error:''});
    } catch(error) {
      const cancelled=controller.signal.aborted||store.isTaskCancelled(id),hasOutput=Boolean(output),
        unknown=attempted&&!hasOutput,message=hasOutput?'The output is retained; native usage recording needs reconciliation.':
          unknown?'The model request ended without a complete native outcome. Review is required before another run.':
          cancelled?'The model work was cancelled before inference.':error instanceof FabricActivityError?error.message:'The native model owner is unavailable.';
      if(!hasOutput)save(id,'model-result',{schema:1,state:cancelled?'cancelled':unknown?'uncertain':'blocked',reconciliationRequired:unknown,error:message});
      if(!store.isTaskCancelled(id))store.updateTask(id,{state:hasOutput||unknown?'uncertain':cancelled?'cancelled':'blocked',completed_at:clock()/1000,error:message});
    }
  }
  async function execute(id) {
    fail(!closed,'unavailable','The model work owner is closed.');
    if(active.has(id))return active.get(id).promise;
    const controller=new AbortController(),entry={controller,promise:null};active.set(id,entry);
    entry.promise=executeOwned(id,controller).finally(()=>{if(active.get(id)===entry)active.delete(id);});return entry.promise;
  }
  async function cancel(id) {
    const entry=active.get(id);store.cancelTask(id);entry?.controller.abort();if(entry)await entry.promise;
    return store.task(id);
  }
  async function cancelRequest(binding,input) {
    fail(exact(input,['expectedBinding','requestId','taskId']),'invalid','Use the exact model cancellation fields.');
    expected(input,binding);requestId(input.requestId);await access(binding,'write');owned(input.taskId,binding,{initiator:true});
    const id=intentId('cancel',binding,input.requestId),value={schema:1,id,binding,requestId:input.requestId,
      taskId:input.taskId,requestDigest:digest([1,input.taskId])};
    const prior=intents.read(id);if(prior)fail(prior.requestDigest===value.requestDigest,'conflict','This cancellation identity names different work.');else intents.put(value);
    await cancel(input.taskId);return view(binding,input.taskId,{canWrite:true});
  }
  async function dispatch(authority,operation,payload,{taskId=''}={}) {
    const binding=fabricActivityBinding(authority);
    if(operation==='model-capabilities'){fail(payload===undefined||payload===null,'invalid','The model inventory has no action payload.');return capabilities(binding);}
    if(operation==='model-review')return review(binding,payload);
    if(operation==='model-submit')return submit(binding,payload);
    if(operation==='model-read'){fail(payload===undefined||payload===null,'invalid','Read a retained model task.');return read(binding,taskId);}
    if(operation==='model-cancel')return cancelRequest(binding,payload);
    if(operation==='model-reconcile')return reconcileRequest(binding,payload);
    throw new FabricActivityError('invalid','Choose an exact model operation.');
  }
  const ownsTask=id=>{try{return store.task(id).kind==='model'&&Boolean(store.taskScope(id));}catch{return false;}};
  async function outputArtifact(authority,id,outputId) {
    const binding=fabricActivityBinding(authority);await access(binding,'read');owned(id,binding);
    const output=store.readReceipt(id,'model-output'),result=store.readReceipt(id,'model-result')?.value;
    fail(output&&result?.state==='completed'&&output.receipt.receipt_id===outputId&&
      typeof output.value==='string'&&result.digest===output.receipt.sha256&&result.bytes===Buffer.byteLength(output.value),
      'not_found','This model output is not available in the current destination.');
    return {path:output.receipt.path,digest:output.receipt.sha256,bytes:result.bytes};
  }
  async function close(){closed=true;for(const entry of active.values())entry.controller.abort();await Promise.allSettled([...active.values()].map(entry=>entry.promise).concat([...settling.values()]));}
  return Object.freeze({dispatch,execute,cancel,ownsTask,outputArtifact,close});
}
