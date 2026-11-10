import {makeSet,checkRoutine,toKg,fromKg} from './training-model.mjs';
const esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const uid=()=>crypto.randomUUID();
const load=(kg,unit)=>String(Math.round(fromKg(kg,unit)*1000)/1000);
const options=(values,selected)=>values.map(([value,label])=>`<option value="${value}" ${value===selected?'selected':''}>${label}</option>`).join('');
const control=(label,action,extra='')=>`<button type="button" class="secondary" data-action="editor.${action}" ${extra}>${label}</button>`;
export function newExercise(){return {id:uid(),name:'',kind:'resistance',restSeconds:90,superset:null,sets:[makeSet(uid()),makeSet(uid()),makeSet(uid())]};}
export function routineDraft(routine){return routine?structuredClone(routine):{id:uid(),name:'',exercises:[newExercise()]};}
function progressionControls(e,unit){
  const r=e.progression??{kind:'none',incrementKg:2.5,incrementSeconds:5,minReps:8,maxReps:12,deloadAfter:0,deloadPercent:10};
  const modes=[['none','No automatic progression'],...(e.kind==='timed'?[['time','Add time']]:e.kind==='cardio'?[]:[['linear','Add load'],['double','Add reps, then load']])];
  return `<details class="progression-options" ${e.progression?'open':''}><summary>Progression</summary><label>Rule<select name="progression">${options(modes,r.kind)}</select></label><div class="fields"><label ${e.kind==='timed'||e.kind==='cardio'?'hidden':''}>Load increment (${unit})<input name="increment" type="number" min="0.001" step="any" value="${load(r.incrementKg??2.5,unit)}"></label><label ${e.kind!=='timed'?'hidden':''}>Seconds increment<input name="increment-seconds" type="number" min="1" max="3600" step="1" value="${r.incrementSeconds??5}"></label><label ${r.kind!=='double'?'hidden':''}>Lower rep target<input name="min-reps" type="number" min="1" max="1000" step="1" value="${r.minReps??8}"></label><label ${r.kind!=='double'?'hidden':''}>Upper rep target<input name="max-reps" type="number" min="1" max="1000" step="1" value="${r.maxReps??12}"></label><label>Deload after missed sessions<input name="deload-after" type="number" min="0" max="10" step="1" value="${r.deloadAfter}"><small>0 turns off automatic deloads</small></label><label>Deload reduction (%)<input name="deload-percent" type="number" min="1" max="90" step="any" value="${r.deloadPercent}"></label></div></details>`;
}
export function editorHTML(routine,unit,{showSourceNote=true}={}){
  return `<form id="routine-editor" class="routine-editor"><div class="section-title"><h2>${routine.name?'Edit routine':'Build a routine'}</h2>${control('Cancel','cancel')}</div><label>Routine name<input name="routine-name" value="${esc(routine.name)}" maxlength="160" required placeholder="Full body A"></label>${routine.source&&showSourceNote?'<p class="muted">Your edits apply to your copy of this program.</p>':''}${routine.exercises.map((e,ei)=>`<fieldset data-exercise="${esc(e.id)}"><legend>Exercise ${ei+1}</legend><div class="editor-actions">${control('Move up','up',`data-exercise="${esc(e.id)}" ${ei===0?'disabled':''}`)}${control('Move down','down',`data-exercise="${esc(e.id)}" ${ei===routine.exercises.length-1?'disabled':''}`)}${control('Remove exercise','remove',`data-exercise="${esc(e.id)}" ${routine.exercises.length===1?'disabled':''}`)}</div><label>Exercise name<input name="exercise-name" value="${esc(e.name)}" maxlength="160" required placeholder="Exercise name"></label><div class="fields"><label>Tracking<select name="kind">${options([['resistance','Weight and reps'],['bodyweight','Bodyweight'],['timed','Timed'],['cardio','Cardio']],e.kind)}</select></label><label>Rest after set (seconds)<input name="rest" type="number" min="0" max="900" step="1" value="${e.restSeconds}" required></label><label>Superset group<input name="superset" maxlength="80" value="${esc(e.superset??'')}" placeholder="Optional, e.g. A"></label></div>${progressionControls(e,unit)}<div class="editor-sets">${e.sets.map((s,si)=>`<div class="editor-set" data-set="${esc(s.id)}"><div class="set-title"><strong>Set ${si+1}</strong>${control('Remove','set-remove',`data-exercise="${esc(e.id)}" data-set="${esc(s.id)}" ${e.sets.length===1?'disabled':''}`)}</div><div class="fields"><label>Type<select name="type">${options([['work','Working'],['warmup','Warm-up'],['drop','Drop']],s.type)}</select></label><label ${e.kind==='cardio'?'hidden':''}>Load (${unit})<input name="load" type="number" step="any" min="0" max="${unit==='kg'?2000:4409}" value="${load(s.loadKg,unit)}" required></label><label ${e.kind==='timed'||e.kind==='cardio'?'hidden':''}>Reps<input name="reps" type="number" min="0" max="10000" step="1" value="${s.reps}" required></label><label ${e.kind==='resistance'||e.kind==='bodyweight'?'hidden':''}>Seconds<input name="seconds" type="number" min="0" max="86400" step="any" value="${s.seconds}" required></label><label ${e.kind!=='cardio'?'hidden':''}>Distance (meters)<input name="distance" type="number" min="0" max="1000000" step="any" value="${s.distanceM}" required></label></div><details class="effort-options" ${s.rir!==null||s.rpe!==null?'open':''}><summary>Effort target</summary><div class="fields"><label>Scale<select name="effort">${options([['none','None'],['rir','Reps in reserve'],['rpe','RPE']],s.rir!==null?'rir':s.rpe!==null?'rpe':'none')}</select></label><label>Effort target<input name="effort-value" type="number" step="any" min="0" max="10" value="${s.rir??s.rpe??''}"></label></div></details></div>`).join('')}</div>${control('Add set','set-add',`data-exercise="${esc(e.id)}"`)}</fieldset>`).join('')}<div class="editor-actions">${control('Add exercise','add')}<button type="submit">Save routine</button></div></form>`;
}
// Read without validating during structural edits, so a blank new exercise can be moved.
export function readEditor(form,previous,unit){
  const routine=structuredClone(previous);routine.name=form.elements.namedItem('routine-name').value;
  for(const field of form.querySelectorAll('fieldset[data-exercise]')){
    const e=routine.exercises.find(x=>x.id===field.dataset.exercise);
    const value=name=>field.querySelector(`[name="${name}"]`).value;
    e.name=value('exercise-name');e.kind=value('kind');e.restSeconds=Number(value('rest'));e.superset=value('superset').trim()||null;
    const rule=value('progression');
    e.progression=rule==='none'?null:{kind:rule,incrementKg:e.progression?.incrementKg!=null&&value('increment')===load(e.progression.incrementKg,unit)?e.progression.incrementKg:toKg(Number(value('increment')),unit),incrementSeconds:Number(value('increment-seconds')),minReps:Number(value('min-reps')),maxReps:Number(value('max-reps')),deloadAfter:Number(value('deload-after')),deloadPercent:Number(value('deload-percent'))};
    for(const row of field.querySelectorAll('[data-set].editor-set')){
      const s=e.sets.find(x=>x.id===row.dataset.set),v=name=>row.querySelector(`[name="${name}"]`).value;
      // Retain the exact canonical value when its rounded display was not changed.
      if(v('load')!==load(s.loadKg,unit))s.loadKg=toKg(Number(v('load')),unit);
      s.type=v('type');s.reps=Number(v('reps'));s.seconds=Number(v('seconds'));s.distanceM=Number(v('distance'));s.done=false;
      s.rir=v('effort')==='rir'?(v('effort-value')===''?NaN:Number(v('effort-value'))):null;
      s.rpe=v('effort')==='rpe'?(v('effort-value')===''?NaN:Number(v('effort-value'))):null;
    }
  }
  return routine;
}
export function changeDraft(previous,action,exerciseId,setId){
  const draft=structuredClone(previous),index=draft.exercises.findIndex(e=>e.id===exerciseId),e=draft.exercises[index];
  if(action==='add'){if(draft.exercises.length>=100)throw Error('A routine can hold up to 100 exercises.');draft.exercises.push(newExercise());}
  else if(!e)throw Error('Exercise not found.');
  else if(action==='up'||action==='down'){const target=index+(action==='up'?-1:1);if(target>=0&&target<draft.exercises.length)[draft.exercises[index],draft.exercises[target]]=[draft.exercises[target],e];}
  else if(action==='remove'){if(draft.exercises.length<2)throw Error('Keep at least one exercise.');draft.exercises.splice(index,1);}
  else if(action==='set-add'){if(e.sets.length>=100)throw Error('An exercise can hold up to 100 sets.');e.sets.push({...structuredClone(e.sets.at(-1)),id:uid(),done:false});}
  else if(action==='set-remove'){const i=e.sets.findIndex(s=>s.id===setId);if(i<0||e.sets.length<2)throw Error('Keep at least one set.');e.sets.splice(i,1);}
  else throw Error('Unknown routine edit.');
  return draft;
}
export function finishDraft(draft){
  const result=structuredClone(draft);
  result.exercises.forEach(e=>e.sets.forEach(s=>{s.done=false;}));
  if(result.source)result.source.adapted=true;
  return checkRoutine(result);
}
