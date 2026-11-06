import {test, expect} from 'bun:test';
import {newTraining, makeSet, trainingCommand as act, elapsedSeconds, restSeconds, workoutSummary, personalRecords, exportTraining, importTraining, toKg, fromKg} from '../training-model.mjs';

const routine = {id:'strength-a',name:'Strength A',source:{creator:'forge',program:'foundations',version:'1'},exercises:[
  {id:'squat',name:'Goblet squat',kind:'resistance',restSeconds:90,superset:null,sets:[makeSet('s1',{loadKg:16}),makeSet('s2',{loadKg:16})]},
  {id:'carry',name:'Carry',kind:'timed',restSeconds:30,superset:null,sets:[makeSet('s1',{seconds:30,reps:0})]}
]};
function start(){return act(act(newTraining('member:one'),{type:'routine.save',routine},0),{type:'start',id:'w1',routineId:routine.id},1000);}
const complete = (s, values={done:true}) => act(s,{type:'set',exerciseId:'squat',setId:'s1',values},11000);

test('current wall-clock timestamps work without a supplied test clock',()=>{
  const s=act(act(newTraining('member:one'),{type:'routine.save',routine}),{type:'start',id:'current',routineId:routine.id});
  expect(s.active.startedAt).toBeGreaterThan(1e12);
});

test('workout, rest, pause and resume survive serialization with real clock deadlines',()=>{
  let s=complete(start()); expect(restSeconds(s.active,31000)).toBe(70);
  s=act(s,{type:'pause'},31000); expect(elapsedSeconds(s.active,90000)).toBe(30);
  s=importTraining(exportTraining(s),'member:one'); expect(restSeconds(s.active,90000)).toBe(70);
  s=act(s,{type:'resume'},91000); expect(restSeconds(s.active,91000)).toBe(70);
  s=act(s,{type:'finish'},111000); expect(elapsedSeconds(s.history[0])).toBe(50);
  expect(workoutSummary(s.history[0])).toEqual({sets:1,reps:8,volumeKg:128,seconds:0,distanceM:0});
  expect(s.active).toBeNull(); expect(s.routines[0].exercises[0].sets[0].done).toBe(false);
});
test('rejected set, missing routine, duplicate workout and unfinished workout are atomic',()=>{
  const s=start(), before=exportTraining(s);
  for(const command of [{type:'start',id:'w2',routineId:'missing'}, {type:'finish'},
    {type:'set',exerciseId:'squat',setId:'s1',values:{reps:-1}},
    {type:'set',exerciseId:'squat',setId:'s1',values:{reps:0,done:true}},
    {type:'set',exerciseId:'squat',setId:'s1',values:{rir:2,rpe:8}},
    {type:'set',exerciseId:'squat',setId:'s1',values:{paid:true}}]) expect(()=>act(s,command,11000)).toThrow();
  expect(exportTraining(s)).toBe(before);
  const finished=act(complete(s),{type:'finish'},12000);
  expect(()=>act(finished,{type:'start',id:'w1',routineId:routine.id},13000)).toThrow('already exists');
});
test('unit choice cannot reinterpret history and portable import preserves source versions',()=>{
  let s=act(complete(start(),{done:true,loadKg:toKg(100,'lb')}),{type:'finish'},12000);
  s=act(s,{type:'unit',unit:'lb'},13000);
  const adopted=importTraining(exportTraining(s),'member:new-device');
  expect(adopted.history[0].exercises[0].sets[0].loadKg).toBeCloseTo(45.359237,6);
  expect(fromKg(adopted.history[0].exercises[0].sets[0].loadKg,'lb')).toBeCloseTo(100,6);
  expect(adopted.history[0].source).toEqual(routine.source);
  expect(adopted.member).toBe('member:new-device');
});
test('history changes recalculate records and next workout loads, warmups do not establish records',()=>{
  let s=act(complete(start(),{done:true,loadKg:20}),{type:'finish'},12000);
  expect(personalRecords(s.history).squat.loadKg).toBe(20);
  s=act(s,{type:'start',id:'w2',routineId:routine.id},13000);
  expect(s.active.exercises[0].sets[0].loadKg).toBe(20);
  s=act(s,{type:'set',exerciseId:'squat',setId:'s1',values:{loadKg:50,type:'warmup',done:true}},14000);
  s=act(s,{type:'finish'},15000); expect(personalRecords(s.history).squat.loadKg).toBe(20);
  s=act(s,{type:'history.remove',id:'w1'},16000); expect(personalRecords(s.history).squat).toBeUndefined();
});
test('paused finish excludes paused time; timed sets require duration; malformed imports are refused',()=>{
  let s=complete(start()); s=act(s,{type:'pause'},21000); s=act(s,{type:'finish'},121000);
  expect(elapsedSeconds(s.history[0])).toBe(20);
  expect(()=>act(start(),{type:'set',exerciseId:'carry',setId:'s1',values:{seconds:0,done:true}},2000)).toThrow('duration');
  for(const mutate of [s=>s.schema='unknown',s=>s.history.push(s.history[0]),s=>s.history[0].pausedMs=1e9,s=>s.history[0].exercises[0].sets[0].reps=1.5]){
    const bad=structuredClone(s);mutate(bad);expect(()=>importTraining(JSON.stringify(bad),'member:one')).toThrow();
  }
});
