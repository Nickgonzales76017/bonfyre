// Interchange reader for the public openGym state contract; no upstream runtime code or media.
import {newTraining,checkTraining,makeSet,toKg} from './training-model.mjs';
const list=(v,name,max)=>{if(!Array.isArray(v)||v.length>max)throw Error(`${name} must be an array with at most ${max} items.`);return v;};
const id=(v,name)=>{if(typeof v!=='string'||!v||v.length>120)throw Error(`${name} needs a stable ID.`);return v;};
const numeric=(v,fallback=0)=>v==null?fallback:v;
function modeOf(row,parent={}){
  const mode=row.mode??parent.mode;
  if(mode==='cardio'||row.min!=null||row.speed!=null)return 'cardio';
  if(mode==='time'||row.sec!=null||row.seconds!=null||row.durationSec!=null)return 'timed';
  if(mode!=null&&!['reps','amrap'].includes(mode))throw Error(`Unsupported training mode: ${mode}`);
  return 'resistance';
}
function readSet(row,key,unit,kind,done){
  if(!row||typeof row!=='object')throw Error('Invalid openGym set.');
  if(typeof done!=='boolean')throw Error('A workout set is missing its completion flag.');
  let seconds=0,distanceM=0,reps=0;
  if(kind==='timed')seconds=numeric(row.sec??row.seconds??row.durationSec??row.r);
  else if(kind==='cardio'){
    seconds=numeric(row.min)*60;
    // The upstream speed contract stores km/h even for profiles displaying mph.
    distanceM=numeric(row.speed)*1000*seconds/3600;
  }else reps=numeric(row.r??row.reps);
  return makeSet(key,{type:row.phase==='warmup'||row.phase==='warm-up'||row.phase==='warm_up'||row.warmup===true?'warmup':'work',loadKg:toKg(numeric(row.w??row.weight),unit),reps,seconds,distanceM,rir:row.rir??null,rpe:row.rpe??null,done});
}
function readRows(rows,exerciseId,unit,kind){
  return list(rows,'Workout sets',100).flatMap((row,index)=>{
    const key=`${exerciseId}:${index}`,sideRows=row.sides?[['L',row.sides.L],['R',row.sides.R]]:[[null,row]];
    return sideRows.flatMap(([side,value])=>{
      if(!value)throw Error('Both sides of a unilateral set are required.');
      const set=readSet({...value,phase:row.phase,warmup:row.warmup},side?`${key}:${side}`:key,unit,kind,value.done);
      if(side)set.side=side;
      const result=[set];
      if(value.type==='dropset')for(const [i,drop] of list(value.drops??[],'Drop sets',100).entries())result.push({...readSet(drop,`${set.id}:drop:${i}`,unit,kind,value.done),type:'drop',...(side?{side}:{})});
      if(value.type==='restpause'&&value.clusters!=null)set.clusters=structuredClone(list(value.clusters,'Rest-pause clusters',100));
      if(value.failure===true)set.failure=true;
      return result;
    });
  });
}
export function importOpenGym(raw,member){
  if(typeof raw!=='string'||raw.length>20*1024*1024)throw Error('Training file exceeds 20 MB.');
  const source=JSON.parse(raw);
  if(!source||!['kg','lb'].includes(source.unit))throw Error('The openGym file must declare kg or lb.');
  const workouts=list(source.workouts,'openGym workouts',50000),routines=list(source.routines,'openGym routines',1000);
  const custom=list(source.customEx??[],'Custom exercises',10000),names=new Map(custom.map(e=>[e.id,e.n??e.name]));
  const state=newTraining(member);state.unit=source.unit;
  state.bodyweight=list(source.bodyweight??[],'Body-weight entries',50000).map(w=>({day:w.d,weightKg:toKg(w.w,source.unit)}));
  const unknownNames=new Set(),nameOf=e=>{const name=e.name??e.n??names.get(e.id);if(name)return name;unknownNames.add(e.id);return `Exercise ${e.id}`;};
  state.routines=routines.map(r=>({id:`opengym:${id(r.id,'Routine')}`,name:r.name,exercises:list(r.ex,'Routine exercises',100).map(e=>{
    const exerciseId=`opengym:${id(e.id,'Exercise')}`,kind=modeOf(e),count=e.sets;
    if(!Number.isInteger(count)||count<1||count>100)throw Error(`Routine ${r.id} has an invalid set count.`);
    return {id:exerciseId,name:nameOf(e),kind,restSeconds:numeric(e.restSec??source.restSec,90),superset:e.sg??e.superset??null,
      sets:Array.from({length:count},(_,i)=>readSet({w:numeric(e.weight),r:numeric(e.reps,8),sec:e.sec,min:e.min,speed:e.speed},`${exerciseId}:${i}`,source.unit,kind,false))};
  })}));
  state.history=workouts.map(w=>({id:`opengym:${id(w.id,'Workout')}`,routineId:`opengym:${id(w.routineId??w.id,'Workout routine')}`,name:w.name??'Imported workout',startedAt:w.start,endedAt:w.end,pausedAt:null,pausedMs:0,restUntil:0,
    exercises:list(w.entries,'Workout exercises',100).map(e=>{
      const exerciseId=`opengym:${id(e.id,'Exercise')}`;
      const modes=new Set(list(e.sets,'Workout sets',100).map(s=>modeOf(s,e.target??e)));
      if(modes.size!==1)throw Error(`Exercise ${e.id} mixes tracking modes in one workout.`);
      const kind=[...modes][0];
      return {id:exerciseId,name:nameOf(e),kind,restSeconds:numeric(e.restSec??source.restSec,90),superset:e.sg??e.superset??null,sets:readRows(e.sets,exerciseId,source.unit,kind)};
    })}));
  // Keep the original training records for re-export and future support of additional fields.
  // Provider settings, login/session state and credentials are outside this allowlist.
  state.importArchive={format:'opengym',training:{unit:source.unit,workouts:structuredClone(workouts),routines:structuredClone(routines)}};
  for(const key of ['bodyweight','customEx','week','dayPlan','queue'])if(source[key]!=null)state.importArchive.training[key]=structuredClone(source[key]);
  const notes=[];
  const extraTargets=new Set(routines.flatMap(r=>r.ex.flatMap(e=>Object.keys(e).filter(k=>!['id','name','n','sets','reps','weight','mode','sec','min','speed','restSec','sg','superset'].includes(k)))));
  if(extraTargets.size)notes.push(`Additional routine settings are retained in the export archive: ${[...extraTargets].sort().join(', ')}.`);
  if(unknownNames.size)notes.push(`${unknownNames.size} exercises retain their openGym IDs; their catalogue names are not in this file.`);
  if(source.bodyweight?.length)notes.push(`${source.bodyweight.length} body-weight entries imported.`);
  if(source.week||source.dayPlan||source.queue)notes.push('The original schedule is retained in the export archive.');
  if(source.active)notes.push('The unfinished openGym session is not imported. Finish it in openGym before migrating.');
  if(workouts.some(w=>w.media?.length)||custom.some(e=>e.media))notes.push('Media references are retained; media files are not included in this JSON import.');
  return {state:checkTraining(state),notes};
}
