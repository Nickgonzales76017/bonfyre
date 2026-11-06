import {packet,activityBinding,sameActivityBinding,readDestinations} from './aurekai-activity-source.mjs';

const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const text=(value,max=512)=>typeof value==='string'&&value.length<=max&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value);
const integer=(value,min=0,max=Number.MAX_SAFE_INTEGER)=>Number.isSafeInteger(value)&&value>=min&&value<=max;
const finite=value=>typeof value==='number'&&Number.isFinite(value)&&value>=0&&value<=Number.MAX_SAFE_INTEGER;
const opaque=value=>text(value,160)&&/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(value);
const taskId=value=>typeof value==='string'&&/^[a-f0-9]{24}$/.test(value);
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const contextValid=value=>value==='self'||typeof value==='string'&&/^(institution|household):[a-z0-9-]{1,63}$/.test(value);
const utf8=new TextEncoder();
const check=(ok,message='The model owner reply is incomplete.')=>{if(!ok)throw Object.assign(new Error(message),{code:'invalid_reply'});};
const refused=error=>[401,403].includes(error?.status)||error?.code==='access_required';
const sameContext=(a,b)=>contextValid(a)&&a===b;
const phases=new Set(['pending','active','completed','failed','cancelled','blocked','review','uncertain']);

async function operation(context,name,{body,task,expectedBinding,...options}={}){
 check(contextValid(context),'Choose a current model execution destination.');
 const query=new URLSearchParams({context,operation:name});if(task!==undefined){check(taskId(task));query.set('task',task);}
 const envelope=await packet('/apps/_fabric?'+query,{...options,body,allowErrorPacket:true});
 check(Array.isArray(envelope)&&envelope.length===3&&envelope[0]===1&&typeof envelope[1]==='boolean'&&record(envelope[2]));
 const value=envelope[2];
 if(!envelope[1]){
  check(text(value.code,64)&&/^[a-z][a-z0-9_]*$/.test(value.code)&&text(value.message,2000));
  throw Object.assign(new Error(value.message),{code:value.code,...value.code==='access_required'?{status:403}:{}});
 }
 const binding=activityBinding(value.binding,context);
 check(!expectedBinding||sameActivityBinding(binding,expectedBinding),'This model operation belongs to a different account.');
 return {...value,binding};
}
function pricing(value){
 check(record(value)&&['unknown','priced'].includes(value.state),'The native usage rate is incomplete.');
 if(value.state==='unknown')return {state:'unknown'};
 check(opaque(value.rateId)&&finite(value.amount)&&text(value.currency,16)&&value.currency&&text(value.unit,64)&&value.unit&&
  (text(value.effectiveAt,128)||finite(value.effectiveAt))&&text(value.evidenceRef,512)&&value.evidenceRef);
 return {state:'priced',rateId:value.rateId,amount:value.amount,currency:value.currency,unit:value.unit,effectiveAt:value.effectiveAt,evidenceRef:value.evidenceRef};
}
export async function readModelCapabilities(context,options={}){
 const value=await operation(context,'model-capabilities',options),limits=value.limits;
 check(record(limits)&&integer(limits.maxPromptBytes,1,32768)&&integer(limits.maxTokens,1,65536)&&integer(limits.maxTimeoutMs,1,3600000)&&Array.isArray(value.models)&&value.models.length<=128);
 const ids=new Set(),models=value.models.map(row=>{
  check(record(row)&&text(row.id,512)&&row.id&&text(row.label,320)&&row.label.trim()&&!ids.has(row.id)&&
   (row.catalogId===null||text(row.catalogId,512)&&row.catalogId)&&row.provider==='bonfyre-moe'&&row.execution==='local'&&typeof row.available==='boolean'&&
   integer(row.maxTokens,1,limits.maxTokens)&&integer(row.maxTimeoutMs,1,limits.maxTimeoutMs));ids.add(row.id);
  return {id:row.id,label:row.label,catalogId:row.catalogId,provider:row.provider,execution:row.execution,available:row.available,maxTokens:row.maxTokens,maxTimeoutMs:row.maxTimeoutMs};
 });
 return {binding:value.binding,models,limits:{maxPromptBytes:limits.maxPromptBytes,maxTokens:limits.maxTokens,maxTimeoutMs:limits.maxTimeoutMs}};
}
export async function reviewModel(context,input,options={}){
 check(record(input)&&sameActivityBinding(activityBinding(input.expectedBinding,context),input.expectedBinding)&&opaque(input.requestId)&&
  text(input.modelId,512)&&input.modelId&&text(input.prompt,32768)&&utf8.encode(input.prompt).length<=32768&&input.prompt.trim()&&
  integer(input.maxTokens,1,65536)&&integer(input.timeoutMs,1,3600000),'Review a bounded model request.');
 const value=await operation(context,'model-review',{...options,body:input,expectedBinding:input.expectedBinding}),row=value.review;
 check(record(row)&&opaque(row.id)&&row.requestId===input.requestId&&row.modelId===input.modelId&&text(row.label,320)&&row.label.trim()&&
  hex(row.promptSha256)&&row.promptBytes===utf8.encode(input.prompt).length&&row.maxTokens===input.maxTokens&&row.timeoutMs===input.timeoutMs&&
  integer(row.expiresAt,1,8640000000000000)&&row.execution==='local'&&row.calls===1);
 const digest=[...new Uint8Array(await crypto.subtle.digest('SHA-256',utf8.encode(input.prompt)))].map(n=>n.toString(16).padStart(2,'0')).join('');
 check(digest===row.promptSha256,'The reviewed prompt does not match these instructions.');
 return {binding:value.binding,review:{id:row.id,requestId:row.requestId,modelId:row.modelId,label:row.label,promptSha256:row.promptSha256,promptBytes:row.promptBytes,maxTokens:row.maxTokens,timeoutMs:row.timeoutMs,expiresAt:row.expiresAt,pricing:pricing(row.pricing),execution:'local',calls:1}};
}
export async function submitModel(context,input,options={}){
 check(record(input)&&opaque(input.requestId)&&opaque(input.reviewId));activityBinding(input.expectedBinding,context);
 const value=await operation(context,'model-submit',{...options,body:input,expectedBinding:input.expectedBinding});
 check(taskId(value.taskId)&&text(value.state,64)&&typeof value.replayed==='boolean');
 return {binding:value.binding,taskId:value.taskId,state:value.state,replayed:value.replayed};
}
async function runValue(value,id){
 check(value.taskId===id&&text(value.state,64)&&phases.has(value.phase)&&typeof value.cancellationAllowed==='boolean'&&record(value.model)&&text(value.model.id,512)&&text(value.model.label,320)&&value.model.label.trim()&&value.model.provider==='bonfyre-moe'&&(value.error===null||text(value.error,2000)));
 let output=null,usage=null;
 if(value.output!==null){
  const row=value.output;check(record(row)&&opaque(row.id)&&text(row.text,65536)&&hex(row.digest)&&integer(row.bytes,0,65536)&&utf8.encode(row.text).length===row.bytes);
  const digest=[...new Uint8Array(await crypto.subtle.digest('SHA-256',utf8.encode(row.text)))].map(n=>n.toString(16).padStart(2,'0')).join('');
  check(digest===row.digest,'The retained model output does not match its receipt.');output={id:row.id,text:row.text,digest:row.digest,bytes:row.bytes};
 }
 if(value.usage!==null){
  const row=value.usage;check(record(row)&&['recorded','pending'].includes(row.state)&&
   (row.receiptId==null&&row.state==='pending'||opaque(row.receiptId))&&text(row.attemptRef,256)&&row.attemptRef&&
   (finite(row.durationMs)||row.state==='pending'&&row.durationMs===null)&&
   (integer(row.outputBytes,0,65536)||row.state==='pending'&&row.outputBytes===null));
  usage={state:row.state,receiptId:row.receiptId??null,attemptRef:row.attemptRef,durationMs:row.durationMs,outputBytes:row.outputBytes,pricing:pricing(row.pricing)};
  if(Object.hasOwn(row,'cost')){check(usage.pricing.state==='priced'&&finite(row.cost));usage.cost=row.cost;}
 }
 check((value.reconciliationAllowed===undefined||typeof value.reconciliationAllowed==='boolean')&&
  (value.localTransferAvailable===undefined||typeof value.localTransferAvailable==='boolean'));
 return {binding:value.binding,taskId:id,state:value.state,phase:value.phase,cancellationAllowed:value.cancellationAllowed,reconciliationAllowed:value.reconciliationAllowed===true,localTransferAvailable:value.localTransferAvailable===true,model:{id:value.model.id,label:value.model.label,provider:value.model.provider},output,usage,error:value.error};
}
export async function readModelRun(context,id,{expectedBinding,...options}={}){
 check(taskId(id));return runValue(await operation(context,'model-read',{...options,task:id,expectedBinding}),id);
}
export async function cancelModelRun(context,input,options={}){
 check(record(input)&&taskId(input.taskId)&&opaque(input.requestId));activityBinding(input.expectedBinding,context);
 return runValue(await operation(context,'model-cancel',{...options,body:input,expectedBinding:input.expectedBinding}),input.taskId);
}
export async function reconcileModelRun(context,input,options={}){
 check(record(input)&&taskId(input.taskId)&&opaque(input.requestId));activityBinding(input.expectedBinding,context);
 return runValue(await operation(context,'model-reconcile',{...options,body:input,expectedBinding:input.expectedBinding}),input.taskId);
}
export async function verifyModelTransfer(context,input,options={}){
 check(record(input)&&taskId(input.taskId)&&taskId(input.outputId)&&opaque(input.requestId));activityBinding(input.expectedBinding,context);
 const value=await operation(context,'model-verify-transfer',{...options,body:input,expectedBinding:input.expectedBinding}),row=value.transfer;
 check(value.taskId===input.taskId&&value.outputId===input.outputId&&value.requestId===input.requestId&&typeof value.replayed==='boolean'&&
  record(row)&&['verified','uncertain'].includes(row.state)&&/^[a-f0-9]{32}$/.test(row.id)&&row.transport==='loopback-tcp'&&row.localOnly===true&&row.verificationOnly===true&&
  typeof row.senderConfirmed==='boolean'&&typeof row.receiverConfirmed==='boolean'&&
  (hex(row.sha256)||row.state==='uncertain'&&row.sha256===null)&&(integer(row.bytes,0,65536)||row.state==='uncertain'&&row.bytes===null)&&
  (taskId(row.receiptId)||row.state==='uncertain'&&row.receiptId===null));
 check(row.state!=='verified'||row.senderConfirmed&&row.receiverConfirmed);
 return {binding:value.binding,taskId:value.taskId,outputId:value.outputId,requestId:value.requestId,replayed:value.replayed,
  transfer:{state:row.state,id:row.id,sha256:row.sha256,bytes:row.bytes,senderConfirmed:row.senderConfirmed,receiverConfirmed:row.receiverConfirmed,receiptId:row.receiptId}};
}

const element=(tag,content)=>{const node=document.createElement(tag);if(content!==undefined)node.textContent=String(content);return node;};
const button=(content,action)=>{const node=element('button',content);node.type='button';node.addEventListener('click',action);return node;};
const field=(caption,control)=>{const label=element('label'),title=element('span',caption);label.append(title,control);return label;};
const rateCopy=rate=>rate.state==='unknown'?'Pricing unavailable. Usage will still be recorded.':rate.amount.toLocaleString(undefined,{maximumSignificantDigits:10})+' '+rate.currency+' per '+rate.unit+'.';
const stateCopy={pending:'Queued',active:'Running',completed:'Completed',failed:'Failed',cancelled:'Cancelled',blocked:'Stopped before execution',review:'Needs review',uncertain:'Outcome needs reconciliation'};
function workChanged(context,task){window.dispatchEvent(new CustomEvent('bonfyre:work-changed',{detail:{context,taskId:task}}));}
function workHref(context,id){return '/apps?'+new URLSearchParams({context})+'#fabric-task-'+id;}

export function mountModelRun(target,{context,binding,taskId:id,read=readModelRun,cancel=cancelModelRun,reconcile=reconcileModelRun,verifyTransfer=verifyModelTransfer,onChange=()=>{},onAccessRefused=()=>{},intervalMs=3000}={}){
 activityBinding(binding,context);check(taskId(id));let disposed=false,epoch=0,request=null,timer=null,snapshot=null,cancelling=false,transferAttempt=null,transferReceipt=null;
 const status=element('p','Reading this model run…');status.setAttribute('role','status');
 const content=element('div'),usage=element('div'),transferStatus=element('p'),actions=element('div');actions.className='ak-model-actions';transferStatus.setAttribute('role','status');transferStatus.hidden=true;
 const refreshButton=button('Refresh run',()=>void refresh()),cancelButton=button('Cancel run',()=>void stop()),reconcileButton=button('Finish usage receipt',()=>void finishUsage()),transferButton=button('Verify local transfer',()=>void transferOutput());
 cancelButton.hidden=reconcileButton.hidden=transferButton.hidden=true;actions.append(refreshButton,cancelButton,reconcileButton,transferButton);target.replaceChildren(status,content,usage,actions,transferStatus);
 const clear=()=>{content.replaceChildren();usage.replaceChildren();snapshot=null;transferStatus.hidden=true;cancelButton.hidden=reconcileButton.hidden=transferButton.hidden=true;cancelButton.disabled=reconcileButton.disabled=transferButton.disabled=true;};
 function transferControls(){
  const matches=!transferAttempt||snapshot?.output?.id===transferAttempt.payload.outputId&&snapshot.output.digest===transferAttempt.digest&&snapshot.output.bytes===transferAttempt.bytes;
  const verified=matches&&transferReceipt?.transfer.state==='verified';
  transferButton.hidden=!snapshot?.localTransferAvailable||!snapshot?.output;
  transferButton.disabled=cancelling||verified||!matches;
  transferButton.textContent=verified?'Local transfer verified':transferAttempt?'Recover transfer receipt':'Verify local transfer';
  transferStatus.hidden=transferButton.hidden;
  transferStatus.textContent=!matches?'The retained output changed. Reopen this run before verifying it.':verified?'Both local peers confirmed the exact output. The temporary verification copy was removed.':
   transferReceipt?.transfer.state==='uncertain'?'The local transfer outcome needs reconciliation. This request will not be transmitted again.':
   transferAttempt?'Read the retained result of this same transfer request.':'Checks this output between two peers on this device. Remote delivery requires a paired recipient.';
 }
 function render(row){
  snapshot=row;status.textContent=row.model.label+' · '+stateCopy[row.phase];clearTimeout(timer);content.replaceChildren();usage.replaceChildren();
  if(row.output){const heading=element('h4','Output'),body=element('pre',row.output.text);body.tabIndex=0;body.setAttribute('aria-label','Retained model output');content.append(heading,body);}
  else content.append(element('p',row.phase==='active'?'The local model is working. You can continue elsewhere in Aurekai.':row.phase==='pending'?'This run is waiting for its native execution slot.':'No retained output was confirmed.'));
  if(row.error)content.append(element('p',row.error));
  if(row.usage){
   usage.append(element('h4','Usage'));
   if(row.usage.durationMs!==null&&row.usage.outputBytes!==null)usage.append(element('p',(row.usage.durationMs/1000).toLocaleString(undefined,{maximumFractionDigits:2})+' seconds · '+row.usage.outputBytes.toLocaleString()+' output bytes'));
   usage.append(element('p',row.usage.state==='recorded'?'Usage recorded.':'Usage receipt is pending. Inference will not be repeated to repair it.'));
   usage.append(element('p',Object.hasOwn(row.usage,'cost')?'Recorded cost: '+row.usage.cost.toLocaleString(undefined,{maximumSignificantDigits:10})+' '+row.usage.pricing.currency+'.':rateCopy(row.usage.pricing)));
  }
  cancelButton.hidden=!row.cancellationAllowed||!['pending','active'].includes(row.phase);cancelButton.disabled=cancelling;
  reconcileButton.hidden=!row.reconciliationAllowed||!row.output;reconcileButton.disabled=cancelling;reconcileButton.textContent=row.usage?.state==='recorded'?'Finish run record':'Finish usage receipt';
  transferControls();
  if(['pending','active'].includes(row.phase)&&!document.hidden)timer=setTimeout(()=>void refresh(),intervalMs);
 }
 async function refresh(){
  if(disposed||document.hidden||cancelling)return;const ticket=++epoch,controller=new AbortController();request?.abort();request=controller;refreshButton.disabled=true;cancelButton.disabled=reconcileButton.disabled=transferButton.disabled=true;
  try{const row=await read(context,id,{expectedBinding:binding,signal:controller.signal});if(disposed||controller.signal.aborted||ticket!==epoch)return;check(sameActivityBinding(row.binding,binding));render(row);}
  catch(error){if(!disposed&&!controller.signal.aborted&&ticket===epoch){clear();status.textContent=refused(error)?'Current access to this run was removed.':'This run could not be read. Use Refresh run to reconcile its retained outcome.';if(refused(error))onAccessRefused(error);}}
  finally{if(!disposed&&ticket===epoch){request=null;refreshButton.disabled=false;}}
 }
 async function stop(){
  if(disposed||cancelling||request||!snapshot?.cancellationAllowed||!['pending','active'].includes(snapshot.phase))return;
  const ticket=++epoch,controller=new AbortController();request?.abort();request=controller;clearTimeout(timer);cancelling=true;refreshButton.disabled=true;cancelButton.disabled=reconcileButton.disabled=transferButton.disabled=true;status.textContent='Confirming cancellation…';
  try{const row=await cancel(context,{expectedBinding:binding,taskId:id,requestId:crypto.randomUUID()},{signal:controller.signal});if(disposed||controller.signal.aborted||ticket!==epoch)return;check(sameActivityBinding(row.binding,binding));render(row);workChanged(context,id);onChange(row);}
  catch(error){if(!disposed&&!controller.signal.aborted&&ticket===epoch){clear();status.textContent=refused(error)?'Current access to this run was removed.':'Cancellation could not be confirmed. Refresh this run before taking another action.';if(refused(error))onAccessRefused(error);}}
  finally{if(!disposed&&ticket===epoch){request=null;cancelling=false;refreshButton.disabled=false;cancelButton.disabled=!snapshot?.cancellationAllowed;}}
 }
 async function finishUsage(){
  if(disposed||cancelling||request||!snapshot?.reconciliationAllowed||!snapshot.output)return;
  const ticket=++epoch,controller=new AbortController();request=controller;clearTimeout(timer);cancelling=true;refreshButton.disabled=cancelButton.disabled=reconcileButton.disabled=transferButton.disabled=true;status.textContent=snapshot.usage?.state==='recorded'?'Completing the retained run record…':'Completing the retained usage receipt…';
  try{const row=await reconcile(context,{expectedBinding:binding,taskId:id,requestId:crypto.randomUUID()},{signal:controller.signal});if(disposed||controller.signal.aborted||ticket!==epoch)return;check(sameActivityBinding(row.binding,binding));render(row);workChanged(context,id);onChange(row);}
  catch(error){if(!disposed&&!controller.signal.aborted&&ticket===epoch){clear();status.textContent=refused(error)?'Current access to this run was removed.':'Usage recording could not be confirmed. Refresh this run to read its retained receipt.';if(refused(error))onAccessRefused(error);}}
  finally{if(!disposed&&ticket===epoch){request=null;cancelling=false;refreshButton.disabled=false;reconcileButton.disabled=!snapshot?.reconciliationAllowed;transferControls();}}
 }
 async function transferOutput(){
  if(disposed||cancelling||request||!snapshot?.localTransferAvailable||!snapshot.output||transferReceipt?.transfer.state==='verified')return;
  const output=snapshot.output;
  if(transferAttempt&&(transferAttempt.payload.outputId!==output.id||transferAttempt.digest!==output.digest||transferAttempt.bytes!==output.bytes)){transferStatus.hidden=false;transferStatus.textContent='The retained output changed. Reopen this run before verifying it.';return;}
  transferAttempt||={payload:{expectedBinding:binding,taskId:id,outputId:output.id,requestId:crypto.randomUUID()},digest:output.digest,bytes:output.bytes};
  const retained=transferAttempt,ticket=++epoch,controller=new AbortController();request=controller;clearTimeout(timer);cancelling=true;refreshButton.disabled=cancelButton.disabled=reconcileButton.disabled=transferButton.disabled=true;transferStatus.hidden=false;transferStatus.textContent='Checking the local transfer receipt…';
  try{
   const row=await verifyTransfer(context,retained.payload,{signal:controller.signal});if(disposed||controller.signal.aborted||ticket!==epoch)return;
   check(sameActivityBinding(row.binding,binding));check(row.transfer.state!=='verified'||row.transfer.sha256===retained.digest&&row.transfer.bytes===retained.bytes,'The local transfer receipt does not match this output.');
   transferReceipt=row;workChanged(context,id);
  }catch(error){if(!disposed&&!controller.signal.aborted&&ticket===epoch){
   if(refused(error)){clear();status.textContent='Current access to this run was removed.';onAccessRefused(error);}
   else transferStatus.textContent='The transfer receipt could not be read. Recover this same request to check its retained outcome.';
  }}finally{if(!disposed&&ticket===epoch){request=null;cancelling=false;refreshButton.disabled=false;reconcileButton.disabled=!snapshot?.reconciliationAllowed;transferControls();}}
 }
 const suspend=()=>{epoch++;request?.abort();request=null;cancelling=false;refreshButton.disabled=false;clearTimeout(timer);clear();status.textContent='Checking current run access…';};
 const visibility=()=>{if(document.hidden)suspend();else void refresh();};
 document.addEventListener('visibilitychange',visibility);
 const dispose=()=>{if(disposed)return;disposed=true;suspend();document.removeEventListener('visibilitychange',visibility);target.replaceChildren();};
 void refresh();return {dispose,refresh};
}

export function mountModelCommons(target,{modelId=target.dataset.aurekaiModel||null,destinations=readDestinations,capabilities=readModelCapabilities,review=reviewModel,submit=submitModel,runView=mountModelRun}={}){
 if(target.dataset.modelMounted)return null;target.dataset.modelMounted='true';
 let disposed=false,epoch=0,request=null,expiryTimer=null,currentBinding=null,choices=[],snapshot=null,proposal=null,attempt=null,run=null,busy=false,context=new URL(location.href).searchParams.get('context')||'self';
 if(!contextValid(context))context='self';
 const heading=element('div');heading.className='ak-model-heading';const headingCopy=element('div');headingCopy.append(element('p','MODEL COMMONS'),element('h2',modelId?'Run '+(target.dataset.modelName||'this model'):'Run a model'));
 const reload=button('Refresh models',()=>void load(context));heading.append(headingCopy,reload);
 const status=element('p','Checking current account access…');status.className='ak-model-status';status.setAttribute('role','status');
 const account=element('select');account.setAttribute('aria-label','Model run account or organization');account.disabled=true;
 const modelSelect=element('select');modelSelect.setAttribute('aria-label','Execution model');const modelField=field('Available local model',modelSelect);modelField.hidden=Boolean(modelId);
 const prompt=element('textarea');prompt.rows=5;prompt.maxLength=32768;prompt.placeholder='Describe what you want this model to do…';prompt.setAttribute('aria-label','Model instructions');
 const tokens=element('input');tokens.type='number';tokens.min='1';tokens.step='1';tokens.value='512';tokens.setAttribute('aria-label','Maximum output tokens');
 const seconds=element('input');seconds.type='number';seconds.min='1';seconds.step='1';seconds.value='60';seconds.setAttribute('aria-label','Maximum run seconds');
 const limits=element('div');limits.className='ak-model-limits';limits.append(field('Maximum output tokens',tokens),field('Maximum run seconds',seconds));
 const help=element('p','A run uses the selected account and continues in Activity.');help.className='ak-model-caption';
 const form=element('form'),reviewButton=element('button','Review run');reviewButton.type='submit';reviewButton.className='ak-model-primary';
 form.append(field('Run for',account),modelField,field('Instructions',prompt),limits,help,reviewButton);
 const reviewed=element('section');reviewed.className='ak-model-review';reviewed.hidden=true;
 const result=element('section');result.className='ak-model-result';result.hidden=true;
 target.replaceChildren(heading,status,form,reviewed,result);
 const cleanup=[];const on=(node,type,handler)=>{node.addEventListener(type,handler);cleanup.push(()=>node.removeEventListener(type,handler));};
 const chosenModel=()=>snapshot?.models.find(row=>modelId?row.catalogId===modelId:row.id===modelSelect.value);
 function controls(){
  const model=chosenModel(),allowed=Boolean(currentBinding&&choices.find(row=>row.context===context)?.canWrite&&model?.available&&!busy&&!attempt);
  for(const node of [modelSelect,prompt,tokens,seconds,reviewButton])node.disabled=!allowed;
  account.disabled=!choices.length;reload.disabled=busy;
 }
 function clearReview(){clearTimeout(expiryTimer);expiryTimer=null;proposal=attempt=null;reviewed.replaceChildren();reviewed.hidden=true;controls();}
 function clearPrivate(message){
  epoch++;request?.abort();request=null;clearReview();currentBinding=snapshot=null;choices=[];account.replaceChildren();busy=false;prompt.value='';modelSelect.replaceChildren();run?.dispose();run=null;result.replaceChildren();result.hidden=true;status.textContent=message;controls();
 }
 async function load(next){
  if(disposed)return;context=next;clearPrivate('Checking execution for this account…');const ticket=epoch,controller=new AbortController();request=controller;busy=true;controls();
  const current=()=>!disposed&&!controller.signal.aborted&&ticket===epoch&&sameContext(context,next);
  try{
   const all=await destinations({signal:controller.signal});if(!current())return;choices=all;account.replaceChildren();
   for(const row of all){const option=element('option',row.label);option.value=row.context;account.append(option);}account.value=context;
   if(!all.some(row=>row.context===context)){status.textContent='Choose an account or organization currently available to you.';return;}
   const value=await capabilities(context,{signal:controller.signal});if(!current())return;currentBinding=value.binding;snapshot=value;modelSelect.replaceChildren();
   for(const row of value.models){const option=element('option',row.label+(row.available?'':' · Unavailable'));option.value=row.id;option.disabled=!row.available;modelSelect.append(option);}
   const first=value.models.find(row=>row.available);if(first)modelSelect.value=first.id;
   const model=chosenModel();
   if(!all.find(row=>row.context===context)?.canWrite)status.textContent='You can view this destination. Running a model requires current write access.';
   else if(!model||!model.available)status.textContent=modelId?'This catalogue entry has no currently admitted local execution.':'No local model is currently available for this account.';
   else{status.textContent='Local execution · '+model.label;setLimits(model);}
   if(!model)help.textContent='Catalogue registration does not establish a runnable model. Refresh when its execution owner is available.';
  }catch(error){if(current()){currentBinding=snapshot=null;status.textContent=refused(error)?'Sign in with current access to run a model.':'Model execution could not be checked. Use Refresh models to try again.';}}
  finally{if(current()){request=null;busy=false;controls();}}
 }
 function setLimits(model){
  tokens.max=String(model.maxTokens);seconds.max=String(Math.max(1,Math.floor(model.maxTimeoutMs/1000)));
  tokens.value=String(Math.min(Number(tokens.value)||512,model.maxTokens));seconds.value=String(Math.min(Number(seconds.value)||60,Number(seconds.max)));
  help.textContent='One local run · Maximum instruction size: '+Math.floor(snapshot.limits.maxPromptBytes/1024)+' KB.';
 }
 async function makeReview(event){
  event.preventDefault();const model=chosenModel();if(disposed||busy||!currentBinding||!model?.available||reviewButton.disabled)return;
  const input={expectedBinding:currentBinding,requestId:crypto.randomUUID(),modelId:model.id,prompt:prompt.value,maxTokens:Number(tokens.value),timeoutMs:Number(seconds.value)*1000};
  if(!input.prompt.trim()||utf8.encode(input.prompt).length>snapshot.limits.maxPromptBytes){status.textContent='Enter instructions within the displayed byte limit.';prompt.focus();return;}
  const ticket=epoch,controller=new AbortController(),expected=currentBinding;request?.abort();request=controller;clearReview();busy=true;controls();status.textContent='Preparing an exact execution review…';
  const current=()=>!disposed&&!controller.signal.aborted&&ticket===epoch&&sameActivityBinding(expected,currentBinding);
  try{const value=await review(context,input,{signal:controller.signal});if(!current())return;check(sameActivityBinding(value.binding,expected));proposal=value.review;renderReview();status.textContent='Review the account, model and run limits before starting.';}
  catch(error){if(current()){status.textContent=error.message||'This request could not be reviewed.';if(refused(error))clearPrivate('Current account access is unavailable.');}}
  finally{if(current()){request=null;busy=false;controls();}}
 }
 function renderReview(){
  const row=proposal;reviewed.replaceChildren();reviewed.hidden=false;
  reviewed.append(element('h3','Ready to run'),element('p',(choices.find(item=>item.context===context)?.label||context)+' · '+row.label),
   element('p','One local call · Up to '+row.maxTokens.toLocaleString()+' output tokens · '+(row.timeoutMs/1000).toLocaleString()+' seconds'),
   element('p',rateCopy(row.pricing)));
  const expires=element('p','Review expires at '+new Date(row.expiresAt).toLocaleTimeString(undefined,{hour:'numeric',minute:'2-digit'}));expires.className='ak-model-caption';
  const actions=element('div');actions.className='ak-model-actions';const start=button('Start run',()=>void startRun(start)),edit=button('Edit instructions',()=>{clearReview();prompt.focus();});start.className='ak-model-primary';actions.append(start,edit);reviewed.append(expires,actions);
  const delay=row.expiresAt-Date.now();start.disabled=delay<=0;if(delay>0)expiryTimer=setTimeout(()=>{if(proposal===row&&!attempt){start.disabled=true;expires.textContent='This review expired. Review the current request again.';}},Math.min(delay,2147483647));
 }
 async function startRun(control){
  if(disposed||busy||!proposal||!currentBinding||control.disabled)return;
  if(!attempt&&proposal.expiresAt<=Date.now()){control.disabled=true;status.textContent='This review expired. Review the request again.';return;}
  attempt??={expectedBinding:currentBinding,requestId:crypto.randomUUID(),reviewId:proposal.id};
  const retained=attempt,ticket=epoch,expected=currentBinding,controller=new AbortController();request?.abort();request=controller;busy=true;controls();for(const node of reviewed.querySelectorAll('button'))node.disabled=true;status.textContent='Submitting this reviewed run…';
  const current=()=>!disposed&&!controller.signal.aborted&&ticket===epoch&&sameActivityBinding(expected,currentBinding);
  try{
   const value=await submit(context,retained,{signal:controller.signal});if(!current())return;check(sameActivityBinding(value.binding,expected));clearReview();status.textContent='Run admitted. Activity keeps it available while you move through Aurekai.';
   result.hidden=false;result.replaceChildren();const link=element('a','Reopen this run from Activity');link.href=workHref(context,value.taskId);result.append(link);const panel=element('div');result.append(panel);run?.dispose();run=runView(panel,{context,binding:expected,taskId:value.taskId,onChange:()=>workChanged(context,value.taskId),onAccessRefused:()=>clearPrivate('Current access to this run was removed.')});workChanged(context,value.taskId);
  }catch(error){
   if(current()){
    if(refused(error)){clearPrivate('Current account access is unavailable.');return;}
    if(['invalid','conflict','expired','admission','paused'].includes(error.code)){clearReview();status.textContent=error.message||'Review this request again before running.';}
    else{status.textContent='The submission could not be confirmed. Recover this exact request or check Activity before starting another run.';control.textContent='Recover this run';control.disabled=false;}
   }
  }finally{if(current()){request=null;busy=false;controls();}}
 }
 on(form,'submit',event=>void makeReview(event));
 for(const node of [prompt,tokens,seconds])on(node,'input',()=>{if(proposal)clearReview();});
 on(modelSelect,'change',()=>{clearReview();const model=chosenModel();if(model){setLimits(model);status.textContent='Local execution · '+model.label;}controls();});
 on(account,'change',()=>{const next=account.value;if(!choices.some(row=>row.context===next))return;void load(next);window.dispatchEvent(new CustomEvent('bonfyre:activity-context-request',{detail:{context:next}}));carryModelContext(next);});
 on(window,'bonfyre:activity-context-invalidated',event=>{if(contextValid(event.detail?.context))context=event.detail.context;clearPrivate('Checking the current account…');});
 on(window,'bonfyre:activity-context-changed',event=>{const next=event.detail?.context,owner=event.detail?.binding;if(contextValid(next)&&(!sameContext(next,context)||!sameActivityBinding(owner,currentBinding)))void load(next);});
 on(window,'bonfyre:account-changed',()=>{clearPrivate('Checking the current account…');void load(context);});
 on(window,'pagehide',()=>clearPrivate('Checking the current account…'));
 on(window,'pageshow',()=>void load(context));
 const dispose=()=>{if(disposed)return;disposed=true;clearPrivate('');for(const off of cleanup)off();delete target.dataset.modelMounted;target.replaceChildren();};
 void load(context);return {dispose,refresh:()=>load(context)};
}

export function carryModelContext(context){
 if(!contextValid(context))return;
 for(const anchor of document.querySelectorAll('a[href]')){
  const url=new URL(anchor.href,location.origin);if(url.origin!==location.origin||!/^\/s\/(?:model\/[^/]+|models\/all)$/.test(url.pathname))continue;
  url.searchParams.set('context',context);anchor.href=url.pathname+url.search+url.hash;
 }
}
if(typeof document!=='undefined'){
 const start=()=>{carryModelContext(new URL(location.href).searchParams.get('context')||'self');for(const target of document.querySelectorAll('[data-aurekai-model]'))mountModelCommons(target);};
 window.addEventListener('bonfyre:activity-context-changed',event=>carryModelContext(event.detail?.context));
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
}
