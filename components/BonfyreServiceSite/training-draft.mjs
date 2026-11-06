import {checkRoutine} from './training-model.mjs';

// Drafts may have unfinished names; all numeric targets and structural limits still apply.
export function requestRoutine(raw,id,creator,{draft=false}={}){
  const routine={id:`delivery:${id}`,name:raw?.name,source:{creator,program:`request:${id}`,version:'1'},exercises:raw?.exercises?.map(e=>({id:e.id,name:e.name,kind:e.kind,restSeconds:e.restSeconds,superset:e.superset,progression:e.progression?Object.fromEntries(['kind','incrementKg','incrementSeconds','minReps','maxReps','deloadAfter','deloadPercent'].filter(k=>Object.hasOwn(e.progression,k)).map(k=>[k,e.progression[k]])):null,sets:e.sets?.map(s=>({id:s.id,type:s.type,loadKg:s.loadKg,reps:s.reps,seconds:s.seconds,distanceM:s.distanceM,rir:s.rir,rpe:s.rpe,done:false}))}))};
  const checked=structuredClone(routine);
  if(draft){for(const named of [checked,...(checked.exercises??[])]){if(typeof named.name!=='string'||named.name.length>160)throw Error('Draft names must be text up to 160 characters.');if(!named.name.trim())named.name='Unfinished';}}
  checkRoutine(checked);return routine;
}
