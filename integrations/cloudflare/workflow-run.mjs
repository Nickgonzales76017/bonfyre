import {checkPacket,checkCompletion,workflowId} from './workflow-contract.mjs';

export async function runActivation(env,event,step) {
  const intent=await step.do('validate-source-revision',async()=>checkPacket(event.payload,env.EDGE_SHARED_SECRET));
  if(event.instanceId!==await workflowId(intent))throw Error('workflow instance does not match source revision');
  await step.do('deliver-to-native-queue',{retries:{limit:3,delay:'5 seconds',backoff:'exponential'}},async()=>{
    await env.WORKGRAPH_QUEUE.send(JSON.stringify(event.payload),{contentType:'text'});
    return {mission:intent.mission,digest:intent.digest,queued:true};
  });
  const completed=await step.waitForEvent('wait-for-native-completion',{type:'native-completion',timeout:'24 hours'});
  return step.do('verify-native-completion',async()=>checkCompletion(completed.payload,intent,env.EDGE_SHARED_SECRET));
}
