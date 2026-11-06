import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {encode,decode} from '../workspace-msgpack.mjs';
import {readModelCapabilities,reviewModel,submitModel,readModelRun,cancelModelRun,verifyModelTransfer} from '../aurekai-model.mjs';

const binding={actor:'user:1',subject:'institution:studio',scope:'institution'},context=binding.subject;
const digest=value=>createHash('sha256').update(value).digest('hex'),id='a'.repeat(24);
const response=value=>new Response(encode([1,true,value]),{headers:{'content-type':'application/msgpack'}});
const caps={binding,models:[{id:'local:fixture',label:'Local fixture',catalogId:'fixture',provider:'bonfyre-moe',execution:'local',available:true,maxTokens:2048,maxTimeoutMs:120000}],limits:{maxPromptBytes:32768,maxTokens:2048,maxTimeoutMs:120000}};
const draft={expectedBinding:binding,requestId:'review-request-1',modelId:'local:fixture',prompt:'Summarise this text.',maxTokens:512,timeoutMs:60000};
const reviewed={binding,review:{id:'b'.repeat(48),requestId:draft.requestId,modelId:draft.modelId,label:'Local fixture',promptSha256:digest(draft.prompt),promptBytes:Buffer.byteLength(draft.prompt),maxTokens:512,timeoutMs:60000,expiresAt:Date.now()+60000,pricing:{state:'unknown'},execution:'local',calls:1}};
const run={binding,taskId:id,state:'completed',phase:'completed',cancellationAllowed:false,model:{id:'local:fixture',label:'Local fixture',provider:'bonfyre-moe'},output:{id:'c'.repeat(24),text:'Actual output\n',digest:digest('Actual output\n'),bytes:14},usage:{state:'recorded',receiptId:'meter-receipt-1',attemptRef:'attempt-one',durationMs:320,outputBytes:14,pricing:{state:'unknown'}},error:null};

test('model capabilities keep only current owner candidates and exact catalogue mappings',async()=>{
 const calls=[];const value=await readModelCapabilities(context,{request:async(url,options)=>{calls.push({url,options});return response({...caps,privatePath:'/private/model.gguf'});}});
 expect(value).toEqual(caps);expect(value).not.toHaveProperty('privatePath');expect(calls[0].options.method).toBe('GET');
 expect(calls[0].url).toBe('/apps/_fabric?context=institution%3Astudio&operation=model-capabilities');
 for(const patch of [{binding:{...binding,subject:'institution:other'}},{models:[caps.models[0],caps.models[0]]},{models:[{...caps.models[0],execution:'remote'}]},{limits:{...caps.limits,maxPromptBytes:100000}}])
  await expect(readModelCapabilities(context,{request:async()=>response({...caps,...patch})})).rejects.toThrow();
});
test('review sends the exact bounded MessagePack proposal and checks its prompt hash',async()=>{
 let sent;const value=await reviewModel(context,draft,{request:async(_url,options)=>{sent=decode(options.body);return response(reviewed);}});
 expect(sent).toEqual([1,draft]);expect(value.review.pricing).toEqual({state:'unknown'});expect(value.review).not.toHaveProperty('cost');
 await expect(reviewModel(context,draft,{request:async()=>response({...reviewed,review:{...reviewed.review,promptSha256:'0'.repeat(64)}})})).rejects.toThrow('prompt');
 await expect(reviewModel(context,draft,{request:async()=>response({...reviewed,binding:{...binding,actor:'user:2'}})})).rejects.toThrow('different account');
 let calls=0;await expect(reviewModel(context,{...draft,prompt:'☃'.repeat(12000)},{request:async()=>{calls++;return response(reviewed);}})).rejects.toThrow();
 expect(calls).toBe(0);
});
test('submit recovery preserves its native request identity without an automatic retry',async()=>{
 const input={expectedBinding:binding,requestId:'submit-request-1',reviewId:reviewed.review.id};let count=0;
 await expect(submitModel(context,input,{request:async()=>{count++;throw Error('lost response');}})).rejects.toThrow();expect(count).toBe(1);
 let sent;const result=await submitModel(context,input,{request:async(_url,options)=>{sent=decode(options.body);return response({binding,taskId:id,state:'completed',replayed:true});}});
 expect(sent).toEqual([1,input]);expect(result.replayed).toBe(true);
});
test('model output is admitted only with matching task account digest and byte count',async()=>{
 expect(await readModelRun(context,id,{expectedBinding:binding,request:async()=>response(run)})).toEqual({...run,reconciliationAllowed:false,localTransferAvailable:false});
 for(const patch of [{binding:{...binding,actor:'user:2'}},{taskId:'d'.repeat(24)},{output:{...run.output,digest:'0'.repeat(64)}},{output:{...run.output,bytes:13}},{cancellationAllowed:'true'}])
  await expect(readModelRun(context,id,{expectedBinding:binding,request:async()=>response({...run,...patch})})).rejects.toThrow();
});
test('unit rates never become fabricated maxima and cost requires a retained priced event',async()=>{
 const pricing={state:'priced',rateId:'rate-1',amount:0.002,currency:'USD',unit:'output-kib',effectiveAt:'2026-10-21',evidenceRef:'receipt:rate'};
 const value=await readModelRun(context,id,{request:async()=>response({...run,usage:{...run.usage,pricing,cost:0.00003}})});
 expect(value.usage.cost).toBe(0.00003);expect(value.usage.pricing.amount).toBe(0.002);expect(value.usage.pricing).not.toHaveProperty('maximumCost');
 await expect(readModelRun(context,id,{request:async()=>response({...run,usage:{...run.usage,cost:0}})})).rejects.toThrow();
});
test('native access refusals and exact cancellation survive the browser carrier',async()=>{
 const failure=new Response(encode([1,false,{code:'access_required',message:'Current write access removed.'}]),{status:403,headers:{'content-type':'application/msgpack'}});
 await expect(readModelCapabilities(context,{request:async()=>failure})).rejects.toMatchObject({code:'access_required',status:403});
 let sent;const input={expectedBinding:binding,requestId:'cancel-request-1',taskId:id};
 const result=await cancelModelRun(context,input,{request:async(_url,options)=>{sent=decode(options.body);return response({...run,state:'cancelled',phase:'cancelled',output:null,usage:null});}});
 expect(result.state).toBe('cancelled');expect(sent).toEqual([1,input]);
 let calls=0;await expect(readModelRun(context,'../other',{request:async()=>{calls++;return response(run);}})).rejects.toThrow();expect(calls).toBe(0);
});

test('local transfer requires exact response binding and only explicit same-request recovery',async()=>{
 const input={expectedBinding:binding,requestId:'local-transfer-request-1',taskId:id,outputId:run.output.id};
 const confirmed={binding,taskId:id,outputId:run.output.id,requestId:input.requestId,replayed:false,
  transfer:{state:'verified',id:'e'.repeat(32),transport:'loopback-tcp',localOnly:true,verificationOnly:true,
   sha256:run.output.digest,bytes:run.output.bytes,senderConfirmed:true,receiverConfirmed:true,receiptId:'f'.repeat(24)}};
 let count=0;
 await expect(verifyModelTransfer(context,input,{request:async()=>{count++;throw Error('acknowledgement lost');}})).rejects.toThrow();expect(count).toBe(1);
 let sent,url;
 const result=await verifyModelTransfer(context,input,{request:async(path,options)=>{url=path;sent=decode(options.body);return response({...confirmed,replayed:true,privatePath:'/private/output',transfer:{...confirmed.transfer,keyFile:'/private/key'}});}});
 expect(sent).toEqual([1,input]);expect(url).toBe('/apps/_fabric?context=institution%3Astudio&operation=model-verify-transfer');
 expect(result.replayed).toBe(true);expect(result.transfer.sha256).toBe(run.output.digest);expect(result.transfer.bytes).toBe(run.output.bytes);
 expect(result).not.toHaveProperty('privatePath');expect(result.transfer).not.toHaveProperty('keyFile');
 const invalid=[
  {binding:{...binding,actor:'user:2'}},{taskId:'b'.repeat(24)},{outputId:'d'.repeat(24)},{requestId:'another-request'},
  ...[{transport:'remote-tcp'},{localOnly:false},{verificationOnly:false},{senderConfirmed:false},{receiverConfirmed:false},
      {sha256:'invalid'},{bytes:65537},{receiptId:null}].map(patch=>({transfer:{...confirmed.transfer,...patch}}))
 ];
 for(const patch of invalid)await expect(verifyModelTransfer(context,input,{request:async()=>response({...confirmed,...patch})})).rejects.toThrow();
 const uncertain=await verifyModelTransfer(context,input,{request:async()=>response({...confirmed,replayed:true,
  transfer:{...confirmed.transfer,state:'uncertain',sha256:null,bytes:null,receiptId:null,senderConfirmed:false,receiverConfirmed:false}})});
 expect(uncertain.transfer.state).toBe('uncertain');expect(uncertain.transfer.sha256).toBeNull();
});
