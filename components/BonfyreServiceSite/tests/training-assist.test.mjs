import {test,expect} from 'bun:test';
import {newTraining,trainingCommand as act} from '../training-model.mjs';
import {buildAssistRequest,checkAssistRequest,checkAssistProposal,applyAssistProposal} from '../training-assist.mjs';
import {configuredModel,modelExecutor,boundedText} from '../training-assist-server.mjs';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio'),routine=material.creators[0].routines[0],exercise=routine.exercises[0];
const route=configuredModel({BONFYRE_TRAINING_MODEL_ENDPOINT:'http://127.0.0.1:7000',BONFYRE_TRAINING_MODEL:'test-model'}),model={...route,data:'program-only'};
const initial=()=>act(newTraining('private-member'),{type:'routine.save',routine});
const request=(state=initial(),selected=model,session='')=>buildAssistRequest(state,routine.id,exercise.id,'Use bodyweight only.',selected,session);
const proposal=()=>({...request().exercise,name:'Bodyweight squat',kind:'bodyweight',sets:request().exercise.sets.map(s=>({...s,loadKg:0})),reason:'No equipment is required.'});
const completion=value=>new Response(JSON.stringify({model:'test-model',choices:[{finish_reason:'stop',message:{content:JSON.stringify(value)}}]}));
test('request projection excludes member, body weight, unrelated history and upstream archives',()=>{
  const state=initial();state.bodyweight=[{day:'2026-10-11',weightKg:80}];state.importArchive={secret:'unrelated-source'};state.routines[0].exercises[0].privateNote='private-note';
  const raw=JSON.stringify(request(state));for(const secret of ['private-member','"bodyweight":','"weightKg":','unrelated-source','private-note','creator'])expect(raw).not.toContain(secret);
  expect(()=>checkAssistRequest({...request(),history:[]})).toThrow();expect(()=>checkAssistRequest({...request(),model:{...model,apiKey:'never-send'}})).toThrow();
});
test('a completed exercise is shared only by explicit session selection under the matching scope',()=>{
  let state=act(initial(),{type:'start',routineId:routine.id,id:'w'},1000);state=act(state,{type:'set',exerciseId:exercise.id,setId:exercise.sets[0].id,values:{done:true}},2000);state=act(state,{type:'finish'},3000);
  expect(request(state).session).toBeNull();expect(()=>request(state,model,'w')).toThrow();
  const selected=request(state,{...model,data:'selected-session'},'w');expect(selected.session.endedAt).toBe(3000);expect(selected.session.exercise.sets[0].done).toBe(true);
  expect(JSON.stringify(selected)).not.toContain('private-member');expect(()=>request(state,{...model,data:'selected-session'},'missing')).toThrow();
});
test('applying a substitution changes exercise identity and preserves workout snapshots',()=>{
  const before=initial(),base={routine:structuredClone(before.routines[0]),exerciseId:exercise.id};
  let active=act(before,{type:'start',routineId:routine.id,id:'w'},1000);const snapshot=JSON.stringify(active.active);let id=0;
  const changed=applyAssistProposal(before.routines[0],base,proposal(),{requestedModel:'test-model'},()=>`new-${id++}`);
  active=act(active,{type:'routine.save',routine:changed},2000);
  expect(JSON.stringify(active.active)).toBe(snapshot);expect(changed.source.adapted).toBe(true);expect(changed.exercises[0].id).not.toBe(exercise.id);
  expect(changed.exercises[0].sets.every(s=>s.done===false)).toBe(true);expect(before.routines[0].exercises[0].name).toBe(exercise.name);
  expect(()=>applyAssistProposal(changed,base,proposal(),{})).toThrow('changed');
});
test('untrusted proposals cannot add commands, source fields, completion or invalid targets',()=>{
  for(const change of [p=>p.command='run',p=>p.source={creator:'someone'},p=>p.sets[0].done=true,p=>p.sets[0].loadKg=-1,p=>p.sets[0].reps=0,p=>p.sets=[],p=>p.restSeconds=10000]){
    const value=proposal();change(value);expect(()=>checkAssistProposal(value)).toThrow();
  }
});
test('model execution fixes the destination and produces a receipt from the actual response',async()=>{
  const calls=[];const run=modelExecutor(route,{fetcher:async(url,options)=>{calls.push({url,options});return completion(proposal());}});
  const result=await run(request());expect(calls[0].url).toBe('http://127.0.0.1:7000/v1/chat/completions');expect(calls[0].options.redirect).toBe('error');
  expect(result.receipt.inputSha256).toMatch(/^[a-f0-9]{64}$/);expect(result.receipt.outputSha256).toMatch(/^[a-f0-9]{64}$/);expect(result.receipt.data).toBe('program-only');
  await expect(run({...request(),model:{...model,endpoint:'http://127.0.0.1:9999'}})).rejects.toThrow('match');expect(calls).toHaveLength(1);
});
test('failed, oversized and incomplete model replies leave no proposal',async()=>{
  for(const response of [new Response('provider-secret',{status:500}),new Response('not-json'),new Response(JSON.stringify({choices:[{finish_reason:'length',message:{content:'{}'}}]})),completion({...proposal(),sets:[]})]){
    await expect(modelExecutor(route,{fetcher:async()=>response})(request())).rejects.toThrow();
  }
  await expect(boundedText(new Response('123456'),5)).rejects.toThrow('large');
});
test('one in-flight request owns the model lane and cancellation releases it',async()=>{
  const abort=new AbortController();const run=modelExecutor(route,{fetcher:async(_url,{signal})=>new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('aborted')),{once:true}))});
  const first=run(request(),abort.signal);await expect(run(request())).rejects.toThrow('another request');abort.abort();await expect(first).rejects.toThrow('aborted');
});
test('HTTP proposal boundary rejects cross-origin, unknown content and excess fields before inference',async()=>{
  let calls=0;const handler=trainingHandler(material,4190,{modelRoute:route,modelOptions:{fetcher:async()=>{calls++;return completion(proposal());}}});
  const send=(body=request(),extra={})=>new Request('http://127.0.0.1:4190/api/training/propose',{method:'POST',headers:{host:'127.0.0.1:4190',origin:'http://127.0.0.1:4190','content-type':'application/json',...extra},body:JSON.stringify(body)});
  expect((await handler(send(request(),{origin:'https://elsewhere.example'}))).status).toBe(403);
  expect((await handler(send(request(),{'content-type':'text/plain'}))).status).toBe(415);
  expect((await handler(send({...request(),history:[]}))).status).toBe(400);expect(calls).toBe(0);
  const result=await handler(send());expect(result.status).toBe(200);expect((await result.json()).proposal.name).toBe('Bodyweight squat');expect(calls).toBe(1);
});
test('server configuration accepts only explicit local routes',()=>{
  expect(configuredModel({})).toBeNull();
  for(const endpoint of ['https://remote.example','http://user:pass@localhost','http://localhost?secret=x'])expect(()=>configuredModel({BONFYRE_TRAINING_MODEL_ENDPOINT:endpoint,BONFYRE_TRAINING_MODEL:'model'})).toThrow();
});
