// Member-owned training semantics. No institution, payment or provider authority lives here.
import {checkHeartRate,checkDeviceActivity} from './training-devices.mjs';
import {checkProgression,nextTargets,planSignature} from './training-progression.mjs';
import {checkPlan,checkEnrollment,checkPlanSession,planSchedule} from './training-plan.mjs';
export const TRAINING_SCHEMA = 'bonfyre.training.v1';
const copy = value => structuredClone(value);
const kinds = ['resistance', 'bodyweight', 'timed', 'cardio'];
function text(value, name, max = 160) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw Error(`${name} is required.`);
  return value;
}
function number(value, name, min = 0, max = 8.64e15, integer = false) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < min || value > max || (integer && !Number.isInteger(value)))
    throw Error(`${name} is out of range.`);
  return value;
}
function list(value, name, max) {
  if (!Array.isArray(value) || value.length > max) throw Error(`${name} is invalid.`);
  return value;
}
function unique(rows, name) {
  const ids = rows.map(r => text(r.id, name));
  if (new Set(ids).size !== ids.length) throw Error(`${name} contains duplicate IDs.`);
}
export function checkSet(set, kind) {
  if (!set || typeof set !== 'object') throw Error('Set is invalid.');
  text(set.id, 'Set ID');
  if (!['work', 'warmup', 'drop'].includes(set.type)) throw Error('Set type is invalid.');
  if (typeof set.done !== 'boolean') throw Error('Set completion is invalid.');
  number(set.loadKg, 'Load', 0, 2000);
  number(set.reps, 'Repetitions', 0, 10000, true);
  number(set.seconds, 'Duration', 0, 86400);
  number(set.distanceM, 'Distance', 0, 1e6);
  if (set.rir !== null) number(set.rir, 'Reps in reserve', 0, 10);
  if (set.rpe !== null) number(set.rpe, 'Effort', 1, 10);
  if (set.rir !== null && set.rpe !== null) throw Error('Choose RIR or RPE for a set.');
  if (set.done && ((kind === 'resistance' || kind === 'bodyweight') ? set.reps < 1 : set.seconds <= 0))
    throw Error(kind === 'resistance' || kind === 'bodyweight' ? 'Enter repetitions before completing this set.' : 'Enter a duration before completing this set.');
  return set;
}
function checkExercise(exercise) {
  text(exercise.id, 'Exercise ID'); text(exercise.name, 'Exercise name');
  if (!kinds.includes(exercise.kind)) throw Error('Exercise kind is invalid.');
  number(exercise.restSeconds, 'Rest', 0, 900, true);
  if (exercise.superset !== null) text(exercise.superset, 'Superset', 80);
  const sets = list(exercise.sets, 'Sets', 100); unique(sets, 'Set ID');
  if (!sets.length) throw Error('An exercise needs at least one set.');
  sets.forEach(s => checkSet(s, exercise.kind));
  checkProgression(exercise.progression,exercise.kind);
  if(exercise.prescribedSets!=null){
    const prescribed=list(exercise.prescribedSets,'Prescribed sets',100);unique(prescribed,'Prescribed set ID');
    prescribed.forEach(s=>checkSet(s,exercise.kind));
    if(prescribed.length!==sets.length||prescribed.some(p=>!sets.some(s=>s.id===p.id)))throw Error('Prescribed sets do not match the session.');
    text(exercise.planSignature,'Plan signature',65536);
  }
  return exercise;
}
export function checkRoutine(routine) {
  text(routine.id, 'Routine ID'); text(routine.name, 'Routine name');
  const exercises = list(routine.exercises, 'Exercises', 100); unique(exercises, 'Exercise ID');
  if (!exercises.length) throw Error('A routine needs at least one exercise.');
  exercises.forEach(checkExercise);
  if (routine.source != null) {
    text(routine.source.creator, 'Program creator'); text(routine.source.program, 'Program ID'); text(routine.source.version, 'Program version');
  }
  return routine;
}
function checkSession(session, completed) {
  checkHeartRate(session.heartRate,session.startedAt,session.endedAt??8.64e15);
  checkPlanSession(session.planSession);
  checkRoutine(session); text(session.routineId, 'Routine ID');
  number(session.startedAt, 'Start'); number(session.pausedMs, 'Paused time');
  number(session.restUntil, 'Rest deadline');
  if (session.pausedAt !== null) number(session.pausedAt, 'Pause', session.startedAt);
  if (completed) {
    number(session.endedAt, 'End', session.startedAt);
    if (session.pausedAt !== null || session.restUntil !== 0) throw Error('A finished workout cannot remain paused or resting.');
    if (!session.exercises.some(e => e.sets.some(s => s.done))) throw Error('Complete at least one set before finishing.');
  } else if (session.endedAt !== null) throw Error('An active workout cannot have an end time.');
  const end = completed ? session.endedAt : session.pausedAt;
  if (end !== null && session.pausedMs > end - session.startedAt) throw Error('Paused time exceeds the workout duration.');
  return session;
}
export function checkTraining(state) {
  if (state?.schema !== TRAINING_SCHEMA) throw Error('Unsupported training file.');
  text(state.member, 'Member');
  if (!['kg', 'lb'].includes(state.unit)) throw Error('Choose kg or lb.');
  unique(list(state.routines, 'Routines', 1000), 'Routine ID'); state.routines.forEach(checkRoutine);
  unique(list(state.plans??[], 'Training plans', 100), 'Plan enrollment ID');(state.plans??[]).forEach(checkEnrollment);
  unique(list(state.history, 'Workout history', 50000), 'Workout ID'); state.history.forEach(w => checkSession(w, true));
  unique(list(state.deviceActivities??[], 'Device workouts', 5000), 'Device workout ID');(state.deviceActivities??[]).forEach(checkDeviceActivity);
  const planSlots=new Set();for(const workout of [...state.history,...(state.active?[state.active]:[])])if(workout.planSession){checkPlanSession(workout.planSession);const slot=JSON.stringify([workout.planSession.enrollmentId,workout.planSession.slotId]);if(planSlots.has(slot))throw Error('A planned session already has a workout.');planSlots.add(slot);}
  const days=new Set();
  for(const entry of list(state.bodyweight??[], 'Body-weight entries', 50000)){
    checkDay(entry.day);number(entry.weightKg,'Body weight',0.1,1000);
    if(days.has(entry.day))throw Error('A body-weight entry already exists for this day.');days.add(entry.day);
  }
  if (state.active !== null) {
    checkSession(state.active, false);
    if (state.history.some(w => w.id === state.active.id)) throw Error('An active workout already exists in history.');
  }
  return state;
}
export function newTraining(member) {
  return checkTraining({schema:TRAINING_SCHEMA, member, unit:'kg', routines:[], plans:[], history:[], bodyweight:[], active:null});
}
export function checkDay(day){
  if(typeof day!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(day)||!Number.isFinite(Date.parse(day+'T00:00:00Z'))||new Date(day+'T00:00:00Z').toISOString().slice(0,10)!==day)throw Error('Choose a valid calendar date.');
  return day;
}
export function makeSet(id, values = {}) {
  return {id, type:'work', loadKg:0, reps:8, seconds:0, distanceM:0, rir:null, rpe:null, done:false, ...values};
}
export function toKg(value, unit) {
  number(value, 'Load', 0, 4409.24525);
  if (!['kg', 'lb'].includes(unit)) throw Error('Choose kg or lb.');
  return unit === 'lb' ? value * 0.45359237 : value;
}
export function fromKg(value, unit) {
  if (!['kg', 'lb'].includes(unit)) throw Error('Choose kg or lb.');
  return unit === 'lb' ? value / 0.45359237 : value;
}
export function elapsedSeconds(session, now = Date.now()) {
  return Math.max(0, Math.floor(((session.endedAt ?? session.pausedAt ?? now) - session.startedAt - session.pausedMs) / 1000));
}
export function restSeconds(session, now = Date.now()) {
  return Math.max(0, Math.ceil((session.restUntil - (session.pausedAt ?? now)) / 1000));
}
export function sessionSequence(session){
  const groups=new Map(),sequence=[];
  for(const e of session.exercises){const key=e.superset===null?`exercise:${e.id}`:`superset:${e.superset}`;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(e);}
  for(const [group,exercises] of groups){
    for(let round=0;round<Math.max(...exercises.map(e=>e.sets.length));round++){
      for(const e of exercises){const s=e.sets[round];if(s)sequence.push({exerciseId:e.id,setId:s.id,name:e.name,group,round,done:s.done});}
    }
  }
  return sequence;
}
export function nextTrainingSet(session){return sessionSequence(session).find(s=>!s.done)??null;}
export function workoutSummary(workout) {
  let sets = 0, reps = 0, volumeKg = 0, seconds = 0, distanceM = 0;
  for (const e of workout.exercises) for (const s of e.sets.filter(s => s.done)) {
    sets++; reps += s.reps; seconds += s.seconds; distanceM += s.distanceM;
    if (e.kind === 'resistance' || e.kind === 'bodyweight') volumeKg += s.loadKg * s.reps;
  }
  return {sets, reps, volumeKg, seconds, distanceM};
}
export function personalRecords(history) {
  const records = Object.create(null);
  for (const w of history) for (const e of w.exercises) for (const s of e.sets.filter(s => s.done && s.type !== 'warmup')) {
    const row = records[e.id] ??= {loadKg:0, reps:0, seconds:0, distanceM:0};
    for (const key of Object.keys(row)) row[key] = Math.max(row[key], s[key]);
  }
  return records;
}
// Commands are applied to a copy; a refusal leaves the caller's state untouched.
export function trainingCommand(previous, command, now = Date.now()) {
  checkTraining(previous); number(now, 'Clock');
  const state = copy(previous), a = state.active;
  if (a && now < (a.pausedAt ?? a.startedAt)) throw Error('The clock precedes this workout.');
  switch (command.type) {
    case 'routine.save': {
      const routine = copy(checkRoutine(command.routine));
      routine.exercises.forEach(e => e.sets.forEach(s => {s.done = false;}));
      const index = state.routines.findIndex(r => r.id === routine.id);
      if (index < 0) state.routines.push(routine); else state.routines[index] = routine;
      break;
    }
    case 'device.import': {
      const incoming=list(command.activities,'Device workouts',100);incoming.forEach(checkDeviceActivity);
      const rows=state.deviceActivities??=[];for(const activity of incoming)if(!rows.some(a=>a.id===activity.id))rows.push(copy(activity));break;
    }
    case 'device.remove': {state.deviceActivities=(state.deviceActivities??[]).filter(a=>a.id!==command.id);break;}
    case 'heart-rate': {
      if(!a||a.id!==command.workoutId||a.pausedAt!==null)throw Error('Heart rate needs an active workout.');
      checkHeartRate([command.sample],a.startedAt,now);const readings=a.heartRate??=[];
      if(readings.length&&command.sample.at<=readings.at(-1).at)throw Error('Heart-rate reading is older than the last saved reading.');
      readings.push(copy(command.sample));break;
    }
    case 'unit': state.unit = command.unit; break;
    case 'plan.adopt': {
      text(command.id,'Plan enrollment ID');const plan=copy(checkPlan(command.plan));checkDay(command.startDay);
      if((state.plans??[]).some(p=>p.id===command.id||(!p.archived&&p.plan.id===plan.id)))throw Error('This plan version is already active in your training.');
      (state.plans??=[]).push({id:command.id,plan,startDay:command.startDay,skipped:[],archived:false});break;
    }
    case 'plan.archive': {
      const plan=(state.plans??[]).find(p=>p.id===command.id);if(!plan)throw Error('Plan not found.');if(a?.planSession?.enrollmentId===plan.id)throw Error('Finish or discard the current workout before archiving this plan.');plan.archived=true;break;
    }
    case 'plan.skip': case 'plan.restore': {
      const plan=(state.plans??[]).find(p=>p.id===command.id&&!p.archived);if(!plan)throw Error('Active plan not found.');const slot=planSchedule(plan,state.history,a).find(s=>s.id===command.slotId);if(!slot||['active','completed'].includes(slot.status))throw Error('This session cannot be skipped or restored.');
      plan.skipped=plan.skipped.filter(id=>id!==slot.id);if(command.type==='plan.skip')plan.skipped.push(slot.id);break;
    }
    case 'weight.save': {
      const day=checkDay(command.day);number(command.weightKg,'Body weight',0.1,1000);
      state.bodyweight=(state.bodyweight??[]).filter(w=>w.day!==day);state.bodyweight.push({day,weightKg:command.weightKg});state.bodyweight.sort((a,b)=>a.day.localeCompare(b.day));break;
    }
    case 'weight.remove': {
      checkDay(command.day);if(!(state.bodyweight??[]).some(w=>w.day===command.day))throw Error('Weight entry not found.');
      state.bodyweight=state.bodyweight.filter(w=>w.day!==command.day);break;
    }
    case 'start': {
      if (a) throw Error('Finish or discard the current workout first.');
      let routine = state.routines.find(r => r.id === command.routineId),planSession;
      if(command.enrollmentId){const plan=(state.plans??[]).find(p=>p.id===command.enrollmentId&&!p.archived);if(!plan)throw Error('Active plan not found.');const slot=planSchedule(plan,state.history).find(s=>s.id===command.slotId);if(!slot||slot.status!=='planned')throw Error('Choose a planned session.');routine=slot.routine;planSession={enrollmentId:plan.id,slotId:slot.id,planName:plan.plan.name,scheduledDay:slot.day};}
      if (!routine) throw Error('Routine not found.');
      text(command.id, 'Workout ID');
      if (state.history.some(w => w.id === command.id)) throw Error('Workout ID already exists.');
      state.active = {...copy(routine), id:command.id, routineId:routine.id, startedAt:now, endedAt:null, pausedAt:null, pausedMs:0, restUntil:0};
      delete state.active.heartRate;delete state.active.planSession;if(planSession)state.active.planSession=planSession;
      // Prefill from the most recent completed occurrence of this exercise.
      for (const e of state.active.exercises) {
        if(e.progression){const target=nextTargets(e,routine.id,state.history,now);e.sets=target.sets;e.prescribedSets=copy(target.sets);e.planSignature=target.signature;e.progressionDecision=target.decision;e.progressionReason=target.reason;continue;}
        const prior = planSession?null:[...state.history].sort((x,y) => y.endedAt-x.endedAt).flatMap(w => w.exercises).find(x => x.id === e.id && x.kind === e.kind && x.sets.some(s => s.done));
        const occurrences={};
        e.sets.forEach(s => {s.done = false;const i=occurrences[s.type]??0;occurrences[s.type]=i+1;const old = prior?.sets.filter(x => x.done && x.type === s.type)[i]; if(old) s.loadKg = old.loadKg;});
      }
      for(const e of state.active.exercises){
        if(!e.prescribedSets){e.prescribedSets=copy(e.sets);e.planSignature=planSignature(e);}
        e.sets.forEach(s=>{s.rir=null;s.rpe=null;});
      }
      break;
    }
    case 'set': {
      if (!a || a.pausedAt !== null) throw Error('Resume a workout before changing sets.');
      const exercise = a.exercises.find(e => e.id === command.exerciseId);
      const set = exercise?.sets.find(s => s.id === command.setId);
      if (!set) throw Error('Set not found.');
      for (const [key, value] of Object.entries(command.values ?? {})) {
        if (!['loadKg','reps','seconds','distanceM','rir','rpe','done','type'].includes(key)) throw Error('Unknown set field.');
        set[key] = value;
      }
      checkSet(set, exercise.kind);
      if (command.values?.done === true) {
        const sequence=sessionSequence(a),index=sequence.findIndex(s=>s.exerciseId===exercise.id&&s.setId===set.id),current=sequence[index],next=sequence[index+1];
        const continuesRound=exercise.superset!==null&&next&&next.group===current.group&&next.round===current.round&&!next.done;
        a.restUntil = continuesRound?0:now + exercise.restSeconds * 1000;
      }
      break;
    }
    case 'pause':
      if (!a || a.pausedAt !== null) throw Error('No running workout to pause.');
      a.pausedAt = now; break;
    case 'resume':
      if (!a || a.pausedAt === null) throw Error('No paused workout to resume.');
      if (a.restUntil) a.restUntil += now - a.pausedAt;
      a.pausedMs += now - a.pausedAt; a.pausedAt = null; break;
    case 'rest.skip':
      if (!a) throw Error('No active workout.'); a.restUntil = 0; break;
    case 'finish':
      if (!a) throw Error('No active workout.');
      if (a.pausedAt !== null) a.pausedMs += now - a.pausedAt;
      a.pausedAt = null; a.endedAt = now; a.restUntil = 0;
      checkSession(a, true); state.history.push(a); state.active = null; break;
    case 'discard':
      if (!a) throw Error('No active workout.'); state.active = null; break;
    case 'history.remove': {
      const index = state.history.findIndex(w => w.id === command.id);
      if (index < 0) throw Error('Workout not found.'); state.history.splice(index, 1); break;
    }
    default: throw Error('Unknown training action.');
  }
  return checkTraining(state);
}
export function exportTraining(state) { return JSON.stringify(checkTraining(state), null, 2) + '\n'; }
export function importTraining(raw, member) {
  if (typeof raw !== 'string' || raw.length > 20 * 1024 * 1024) throw Error('Training file exceeds 20 MB.');
  const state = checkTraining(JSON.parse(raw));
  // Explicit member adoption occurs at the import boundary, not through creator identity.
  return checkTraining({...copy(state), member:text(member, 'Member')});
}
