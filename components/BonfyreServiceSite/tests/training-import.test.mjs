import {test,expect} from 'bun:test';
import {importOpenGym} from '../training-import.mjs';
import {workoutSummary,exportTraining,importTraining,personalRecords} from '../training-model.mjs';
const source=()=>({unit:'lb',restSec:60,customEx:[{id:'custom:squat',n:'Squat'}],routines:[{id:'r',name:'Strength',ex:[{id:'custom:squat',sets:2,reps:8,weight:100,sg:'A'}]}],workouts:[{id:'w',name:'Strength',routineId:'r',start:1791700000000,end:1791701800000,entries:[{id:'custom:squat',sg:'A',sets:[{w:50,r:5,phase:'warmup',done:true},{w:100,r:8,rir:2,done:true}]}]}]});
test('openGym load, effort, names, supersets and original records survive a native export round trip',()=>{
  const input=source(),result=importOpenGym(JSON.stringify(input),'member:one');
  expect(result.notes).toEqual([]);expect(result.state.history[0].exercises[0].name).toBe('Squat');
  expect(result.state.history[0].exercises[0].sets[1].loadKg).toBeCloseTo(45.359237,6);
  expect(result.state.history[0].exercises[0].sets[1].rir).toBe(2);expect(result.state.routines[0].exercises[0].superset).toBe('A');
  const again=importTraining(exportTraining(result.state),'member:two');expect(again.importArchive.training.workouts).toEqual(input.workouts);
  expect(workoutSummary(again.history[0]).volumeKg).toBeCloseTo(1050*0.45359237,6);
});
test('cardio always converts stored km/h, independent of the weight or displayed speed unit',()=>{
  const input=source();input.speedUnit='mph';input.workouts[0].entries=[{id:'treadmill',sets:[{mode:'cardio',min:30,speed:10,done:true}]}];
  const result=importOpenGym(JSON.stringify(input),'member');const set=result.state.history[0].exercises[0].sets[0];
  expect(set.seconds).toBe(1800);expect(set.distanceM).toBe(5000);expect(result.notes[0]).toContain('catalogue names');
});
test('unilateral drops count actual left/right work once; rest-pause totals are not doubled',()=>{
  const input=source();input.workouts[0].entries[0].sets=[{sides:{L:{w:20,r:8,done:true,type:'dropset',drops:[{w:10,r:4}]},R:{w:25,r:6,done:true}}},{w:20,r:15,done:true,type:'restpause',clusters:[{r:5,restSec:15}]}];
  const result=importOpenGym(JSON.stringify(input),'member').state;
  expect(result.history[0].exercises[0].sets).toHaveLength(4);
  expect(workoutSummary(result.history[0]).volumeKg).toBeCloseTo((160+40+150+300)*0.45359237,6);
  expect(result.history[0].exercises[0].sets[0].side).toBe('L');
});
test('import refuses malformed source before any mutation and does not retain credentials',()=>{
  for(const change of [s=>s.unit='stone',s=>s.workouts[0].end=0,s=>delete s.workouts[0].entries[0].sets[0].done,s=>s.workouts.push(s.workouts[0]),s=>s.routines[0].ex[0].sets=1000]){const input=source();change(input);expect(()=>importOpenGym(JSON.stringify(input),'member')).toThrow();}
  const input=source();input.coachApiKey='never-copy';input.bodyweight=[{d:'2026-10-01',w:160}];const result=importOpenGym(JSON.stringify(input),'member');
  expect(JSON.stringify(result.state)).not.toContain('never-copy');expect(result.notes[0]).toContain('body-weight');
  expect(result.state.bodyweight[0].day).toBe('2026-10-01');
  expect(result.state.bodyweight[0].weightKg).toBeCloseTo(72.5747792,6);
  expect(importTraining(exportTraining(result.state),'member').bodyweight).toEqual(result.state.bodyweight);
  input.bodyweight.push({...input.bodyweight[0]});expect(()=>importOpenGym(JSON.stringify(input),'member')).toThrow();
});
test('exercise IDs cannot mutate inherited properties through personal-record calculation',()=>{
  const input=source();input.workouts[0].entries[0].id='__proto__';const state=importOpenGym(JSON.stringify(input),'member').state;
  state.history[0].exercises[0].id='__proto__';const records=personalRecords(state.history);expect(Object.getPrototypeOf(records)).toBeNull();expect(records.__proto__.reps).toBe(8);expect({}.reps).toBeUndefined();
});
