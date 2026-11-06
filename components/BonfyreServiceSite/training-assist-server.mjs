import {createHash} from 'node:crypto';
import {checkAssistRequest,checkAssistProposal,proposalSchema} from './training-assist.mjs';
const digest=value=>createHash('sha256').update(value).digest('hex');
export function configuredModel(env=process.env){
  const endpoint=env.BONFYRE_TRAINING_MODEL_ENDPOINT,model=env.BONFYRE_TRAINING_MODEL;
  if(!endpoint&&!model)return null;
  if(!endpoint||!model)throw Error('Set both BONFYRE_TRAINING_MODEL_ENDPOINT and BONFYRE_TRAINING_MODEL.');
  const url=new URL(endpoint);
  if(url.username||url.password||url.search||url.hash||url.protocol!=='http:'||!['127.0.0.1','localhost','[::1]'].includes(url.hostname))throw Error('The training model route requires a local HTTP endpoint.');
  if(typeof model!=='string'||!model.trim()||model.length>160)throw Error('Invalid training model ID.');
  return {provider:'local',endpoint:url.href.replace(/\/$/,''),model};
}
export async function boundedText(response,maxBytes){
  const reader=response.body?.getReader();if(!reader)return '';
  const chunks=[];let bytes=0;
  try{for(;;){const {value,done}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>maxBytes)throw Error('Model request or response is too large.');chunks.push(value);}}
  finally{await reader.cancel();}
  return new TextDecoder().decode(Buffer.concat(chunks));
}
export function modelExecutor(route,{fetcher=fetch,timeoutMs=90000}={}){
  let running=false;
  return async(request,signal)=>{
    if(!route)throw Error('No model endpoint is connected to this training server.');
    checkAssistRequest(request);
    if(request.model.provider!==route.provider||request.model.endpoint.replace(/\/$/,'')!==route.endpoint||request.model.model!==route.model)throw Error('Model settings do not match the connected route.');
    if(running)throw Error('The model is working on another request. Try again when it finishes.');
    running=true;
    try{
      const input=JSON.stringify({instruction:request.instruction,exercise:request.exercise,session:request.session});
      const body=JSON.stringify({model:route.model,temperature:0.2,max_tokens:800,stream:false,response_format:{type:'json_object',schema:proposalSchema},messages:[
        {role:'system',content:'Adapt the exercise to satisfy the user request. Return only the required JSON object. Use the name of the actual replacement exercise. loadKg is external added weight in kilograms, never body mass. A request for no weights means loadKg must be 0 for every set. Duration is seconds; distance is meters. Follow requested set count, repetitions and rest exactly. Keep other targets only when compatible with the request. Give a short reason. No diagnoses, medical treatment, links or extra fields. Set unused metrics to zero and unused effort fields to null.'},
        {role:'user',content:input}
      ]});
      const path=route.endpoint.endsWith('/chat/completions')?route.endpoint:route.endpoint.replace(/\/v1$/,'')+'/v1/chat/completions';
      const response=await fetcher(path,{method:'POST',headers:{'Content-Type':'application/json'},body,redirect:'error',signal:signal?AbortSignal.any([signal,AbortSignal.timeout(timeoutMs)]):AbortSignal.timeout(timeoutMs)});
      if(!response.ok){await response.body?.cancel();throw Error(`The model endpoint returned HTTP ${response.status}.`);}
      const raw=await boundedText(response,256*1024),result=JSON.parse(raw),choice=result.choices?.[0];
      if(choice?.finish_reason!=='stop'||typeof choice.message?.content!=='string')throw Error('The model did not return a complete proposal.');
      const proposal=checkAssistProposal(JSON.parse(choice.message.content));
      return {proposal,receipt:{provider:route.provider,endpoint:route.endpoint,requestedModel:route.model,returnedModel:typeof result.model==='string'?result.model:null,completedAt:new Date().toISOString(),inputSha256:digest(body),outputSha256:digest(raw),data:request.session?'selected-session':'program-only'}};
    }finally{running=false;}
  };
}
