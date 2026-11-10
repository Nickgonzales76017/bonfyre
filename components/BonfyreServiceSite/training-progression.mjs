const within=(v,min,max,integer=false)=>typeof v==='number'&&Number.isFinite(v)&&v>=min&&v<=max&&(!integer||Number.isInteger(v));
export function checkProgression(rule,kind){
  if(rule==null)return;
  if(!['linear','double','time'].includes(rule.kind))throw Error('Choose a progression rule.');
  if(rule.kind==='time'){
    if(kind!=='timed'||!within(rule.incrementSeconds,1,3600,true))throw Error('Time progression needs a timed exercise and a positive seconds increment.');
  }else{
    if(!['resistance','bodyweight'].includes(kind)||!within(rule.incrementKg,0.001,100))throw Error('Load progression needs a positive load increment.');
    if(rule.kind==='double'&&(!within(rule.minReps,1,1000,true)||!within(rule.maxReps,rule.minReps,1000,true)))throw Error('Choose a valid repetition range.');
  }
  if(!within(rule.deloadAfter,0,10,true)||!within(rule.deloadPercent,1,90))throw Error('Choose a valid deload rule.');
}
export function planSignature(exercise){
  const r=exercise.progression;
  return JSON.stringify([exercise.kind,r?[r.kind,r.incrementKg??null,r.incrementSeconds??null,r.minReps??null,r.maxReps??null,r.deloadAfter,r.deloadPercent]:null,
    exercise.sets.map(s=>[s.id,s.type,s.loadKg,s.reps,s.seconds,s.distanceM,s.rir,s.rpe])]);
}
function metTargets(exercise){
  const targets=exercise.prescribedSets?.filter(s=>s.type==='work')??[];
  return targets.length>0&&targets.every(p=>{
    const s=exercise.sets.find(s=>s.id===p.id);
    const effort=s&&(p.rir===null||(s.rir!==null&&s.rir>=p.rir))&&(p.rpe===null||(s.rpe!==null&&s.rpe<=p.rpe));
    return s?.done&&s.type==='work'&&effort&&(exercise.kind==='timed'?s.seconds>=p.seconds:s.reps>=p.reps&&s.loadKg+1e-8>=p.loadKg);
  });
}
const tidy=v=>Math.round(v*1e6)/1e6;
export function nextTargets(exercise,routineId,history,now=Date.now()){
  const rule=exercise.progression,sets=structuredClone(exercise.sets).map(s=>({...s,done:false})),signature=planSignature(exercise);
  if(!rule)return {sets,signature,decision:'fixed',reason:'Use the routine targets.'};
  checkProgression(rule,exercise.kind);
  if(!sets.some(s=>s.type==='work'))return {sets,signature,decision:'fixed',reason:'This exercise has no working sets.'};
  const previous=history.filter(w=>w.routineId===routineId&&w.endedAt<=now).sort((a,b)=>b.endedAt-a.endedAt)
    .map(w=>({workout:w.id,exercise:w.exercises.find(e=>e.id===exercise.id&&e.planSignature===signature&&Array.isArray(e.prescribedSets))})).filter(x=>x.exercise);
  const latest=previous[0];
  if(!latest){
    if(rule.kind==='double')sets.filter(s=>s.type==='work').forEach(s=>{s.reps=rule.minReps;});
    return {sets,signature,decision:'initial',reason:'First session with this plan. Start with its targets.'};
  }
  const last=latest.exercise,passed=metTargets(last);let failures=0;
  for(const entry of previous){if(metTargets(entry.exercise))break;failures++;if(entry.exercise.progressionDecision==='deload')break;}
  const deload=!passed&&rule.deloadAfter>0&&failures>=rule.deloadAfter;
  let decision='hold',reason='Repeat the targets: one or more working sets were missed.';
  if(deload){decision='deload';reason=`Reduce the target by ${rule.deloadPercent}% after ${failures} missed sessions.`;}
  else if(passed){decision='increase';reason=rule.kind==='double'?'All working sets met their targets. Add a repetition within your range.':'All working sets met their targets. Apply your configured increment.';}
  const atTop=rule.kind==='double'&&passed&&last.prescribedSets.filter(s=>s.type==='work').every(p=>last.sets.find(s=>s.id===p.id).reps>=rule.maxReps);
  if(atTop)reason='All working sets reached the top of your range. Add load and return to the lower target.';
  for(const set of sets.filter(s=>s.type==='work')){
    const prescribed=last.prescribedSets.find(s=>s.id===set.id),actual=last.sets.find(s=>s.id===set.id);
    if(!prescribed)throw Error('The previous session is missing a target.');
    set.loadKg=prescribed.loadKg;set.reps=prescribed.reps;set.seconds=prescribed.seconds;
    if(deload){if(rule.kind==='time')set.seconds=Math.max(1,Math.floor(set.seconds*(1-rule.deloadPercent/100)));else set.loadKg=tidy(set.loadKg*(1-rule.deloadPercent/100));if(rule.kind==='double')set.reps=rule.minReps;}
    else if(passed){
      if(rule.kind==='time')set.seconds=Math.min(86400,Math.max(actual.seconds,prescribed.seconds)+rule.incrementSeconds);
      else if(rule.kind==='linear'||atTop){set.loadKg=Math.min(2000,tidy(Math.max(actual.loadKg,prescribed.loadKg)+rule.incrementKg));if(atTop)set.reps=rule.minReps;}
      else set.reps=Math.min(rule.maxReps,prescribed.reps+1);
    }
  }
  return {sets,signature,decision,reason,fromWorkout:latest.workout,missedSessions:failures};
}
