import {checkRoutine,makeSet} from './training-model.mjs';
import {checkSettings,defaultSettings} from './training-settings.mjs';
const clone=v=>structuredClone(v);
const keys=(value,allowed)=>{if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!allowed.includes(k)))throw Error('Unexpected model request field.');};
const setFields=['type','loadKg','reps','seconds','distanceM','rir','rpe'];
const exerciseFields=['name','kind','restSeconds','sets'];
function exerciseData(exercise,actual=false){
  return {name:exercise.name,kind:exercise.kind,restSeconds:exercise.restSeconds,sets:exercise.sets.map(s=>Object.fromEntries([...setFields,...(actual?['done']:[])].map(k=>[k,s[k]])))};
}
function validateExercise(exercise,actual=false){
  keys(exercise,exerciseFields);
  if(!Array.isArray(exercise.sets)||exercise.sets.length>12)throw Error('Model requests support up to 12 sets per exercise.');
  exercise.sets.forEach(s=>keys(s,[...setFields,...(actual?['done']:[])]));
  checkRoutine({id:'review',name:'Review',exercises:[{...exercise,id:'exercise',superset:null,sets:exercise.sets.map((s,i)=>({...s,id:`set-${i}`,done:actual?s.done:false}))}]});
  return exercise;
}
export function buildAssistRequest(state,routineId,exerciseId,instruction,model,sessionId=''){
  const routine=state.routines.find(r=>r.id===routineId),exercise=routine?.exercises.find(e=>e.id===exerciseId);
  if(!exercise)throw Error('Choose an exercise from your routines.');
  const request={model:clone(model),instruction,exercise:exerciseData(exercise),session:null};
  if(sessionId){
    if(model.data!=='selected-session')throw Error('Enable selected-session sharing before including a workout.');
    const session=state.history.find(w=>w.id===sessionId),actual=session?.exercises.find(e=>e.id===exerciseId);
    if(!actual)throw Error('That session does not contain this exercise.');
    request.session={endedAt:session.endedAt,exercise:exerciseData(actual,true)};
  }
  return checkAssistRequest(request);
}
export function checkAssistRequest(request){
  keys(request,['model','instruction','exercise','session']);
  checkSettings({...defaultSettings(),model:request.model});
  if(!['local','openai-compatible'].includes(request.model.provider))throw Error('Choose a connected local or compatible model.');
  if(typeof request.instruction!=='string'||!request.instruction.trim()||request.instruction.length>1200)throw Error('Describe the change in 1–1,200 characters.');
  validateExercise(request.exercise);
  if(request.session!==null){
    if(request.model.data!=='selected-session')throw Error('This model is limited to program data.');
    keys(request.session,['endedAt','exercise']);
    if(!Number.isSafeInteger(request.session.endedAt)||request.session.endedAt<0)throw Error('Session date is invalid.');
    validateExercise(request.session.exercise,true);
  }
  return request;
}
export function checkAssistProposal(proposal){
  keys(proposal,[...exerciseFields,'reason']);
  if(typeof proposal.reason!=='string'||!proposal.reason.trim()||proposal.reason.length>1200)throw Error('The model did not explain its proposed change.');
  validateExercise(Object.fromEntries(exerciseFields.map(k=>[k,proposal[k]])));
  if(proposal.sets.some(s=>['resistance','bodyweight'].includes(proposal.kind)?s.reps<1:s.seconds<=0))throw Error('The proposal needs a target for every set.');
  return proposal;
}
export function applyAssistProposal(current,base,proposal,receipt,uid=()=>crypto.randomUUID()){
  if(JSON.stringify(current)!==JSON.stringify(base.routine))throw Error('This routine changed. Request a new proposal before applying it.');
  checkAssistProposal(proposal);
  const routine=clone(current),index=routine.exercises.findIndex(e=>e.id===base.exerciseId),old=routine.exercises[index];
  if(!old)throw Error('Exercise no longer exists.');
  const replaced=old.name.trim().toLowerCase()!==proposal.name.trim().toLowerCase()||old.kind!==proposal.kind;
  routine.exercises[index]={id:replaced?uid():old.id,name:proposal.name,kind:proposal.kind,restSeconds:proposal.restSeconds,superset:old.superset,
    progression:replaced?null:clone(old.progression??null),sets:proposal.sets.map((s,i)=>makeSet(!replaced&&old.sets[i]?old.sets[i].id:uid(),clone(s)))};
  if(routine.source)routine.source.adapted=true;
  routine.lastModelChange={exerciseId:routine.exercises[index].id,reason:proposal.reason,receipt:clone(receipt)};
  return checkRoutine(routine);
}
export const proposalSchema={type:'object',additionalProperties:false,required:[...exerciseFields,'reason'],properties:{
  name:{type:'string'},kind:{type:'string',enum:['resistance','bodyweight','timed','cardio']},restSeconds:{type:'integer',minimum:0,maximum:900},reason:{type:'string'},
  sets:{type:'array',minItems:1,maxItems:12,items:{type:'object',additionalProperties:false,required:setFields,properties:{type:{type:'string',enum:['work','warmup','drop']},loadKg:{type:'number',minimum:0,maximum:2000},reps:{type:'integer',minimum:0,maximum:10000},seconds:{type:'number',minimum:0,maximum:86400},distanceM:{type:'number',minimum:0,maximum:1000000},rir:{type:['number','null'],minimum:0,maximum:10},rpe:{type:['number','null'],minimum:1,maximum:10}}}}
}};
