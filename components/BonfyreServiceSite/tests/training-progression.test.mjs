import {test,expect} from 'bun:test';
import {newTraining,makeSet,trainingCommand as act,exportTraining,importTraining} from '../training-model.mjs';
import {nextTargets} from '../training-progression.mjs';
const rule={kind:'double',incrementKg:2.5,minReps:8,maxReps:10,deloadAfter:2,deloadPercent:10};
const routine=()=>({id:'r',name:'Strength',exercises:[{id:'squat',name:'Squat',kind:'resistance',restSeconds:60,superset:null,progression:{...rule},sets:[makeSet('warm',{type:'warmup',loadKg:5,reps:5}),makeSet('s1',{loadKg:20,reps:8}),makeSet('s2',{loadKg:20,reps:8})]}]});
const state=()=>act(newTraining('m'),{type:'routine.save',routine:routine()},0);
function session(s,id,values={}){
  const clock=(s.history.length+1)*10000;let next=act(s,{type:'start',id,routineId:'r'},clock);
  for(const set of next.active.exercises[0].sets.filter(x=>x.type==='work'))next=act(next,{type:'set',exerciseId:'squat',setId:set.id,values:{done:true,...values}},clock+1);
  return act(next,{type:'finish'},clock+2);
}
test('double progression adds reps, then load, with prescribed targets preserved',()=>{
  let s=state();s=session(s,'w1');expect(nextTargets(s.routines[0].exercises[0],'r',s.history).sets[1].reps).toBe(9);
  s=session(s,'w2');s=session(s,'w3');const next=nextTargets(s.routines[0].exercises[0],'r',s.history);
  expect(next.sets[1].loadKg).toBe(22.5);expect(next.sets[1].reps).toBe(8);expect(next.reason).toContain('top of your range');
  expect(s.history[0].exercises[0].prescribedSets[1].reps).toBe(8);expect(s.history[2].exercises[0].prescribedSets[1].reps).toBe(10);
  expect(next.sets[0].loadKg).toBe(5);
});
test('misses hold targets and configured consecutive failures deload once per cycle',()=>{
  let s=session(state(),'miss1',{reps:6});let next=nextTargets(s.routines[0].exercises[0],'r',s.history);
  expect(next.decision).toBe('hold');expect(next.sets[1].loadKg).toBe(20);
  s=session(s,'miss2',{reps:6});next=nextTargets(s.routines[0].exercises[0],'r',s.history);
  expect(next.decision).toBe('deload');expect(next.sets[1].loadKg).toBe(18);
  s=session(s,'deload-miss',{reps:6});next=nextTargets(s.routines[0].exercises[0],'r',s.history);expect(next.decision).toBe('hold');expect(next.sets[1].loadKg).toBe(18);
});
test('partial work, another routine and a changed prescription cannot justify an increase',()=>{
  let s=act(state(),{type:'start',id:'partial',routineId:'r'},10000);s=act(s,{type:'set',exerciseId:'squat',setId:'s1',values:{done:true}},10001);s=act(s,{type:'finish'},10002);
  expect(nextTargets(s.routines[0].exercises[0],'r',s.history).decision).toBe('hold');
  expect(nextTargets(s.routines[0].exercises[0],'different',s.history).decision).toBe('initial');
  const changed=structuredClone(s.routines[0]);changed.exercises[0].sets[1].loadKg=40;
  expect(nextTargets(changed.exercises[0],'r',s.history).decision).toBe('initial');
});
test('linear and timed increments follow actual completed work and survive export',()=>{
  const r=routine();r.exercises[0].progression={...rule,kind:'linear'};let s=act(newTraining('m'),{type:'routine.save',routine:r},0);
  s=session(s,'linear',{loadKg:25});expect(nextTargets(r.exercises[0],'r',s.history).sets[1].loadKg).toBe(27.5);
  r.exercises[0].kind='timed';r.exercises[0].sets=[makeSet('s1',{seconds:30,reps:0})];r.exercises[0].progression={kind:'time',incrementSeconds:5,deloadAfter:0,deloadPercent:10};
  s=act(newTraining('m'),{type:'routine.save',routine:r},0);s=session(s,'timed');s=importTraining(exportTraining(s),'m');
  expect(nextTargets(r.exercises[0],'r',s.history).sets[0].seconds).toBe(35);
});
test('body-weight dates and units round-trip without changing training or duplicating days',()=>{
  let s=state();s=act(s,{type:'weight.save',day:'2026-10-11',weightKg:80},1);s=act(s,{type:'weight.save',day:'2026-10-11',weightKg:81},2);
  expect(s.bodyweight).toEqual([{day:'2026-10-11',weightKg:81}]);s=act(s,{type:'unit',unit:'lb'},3);
  expect(importTraining(exportTraining(s),'m').bodyweight[0].weightKg).toBe(81);
  for(const day of ['2026-02-30','2026-13-01','yesterday'])expect(()=>act(s,{type:'weight.save',day,weightKg:80},4)).toThrow();
  expect(()=>act(s,{type:'weight.save',day:'2026-10-12',weightKg:0},4)).toThrow();
  s=act(s,{type:'weight.remove',day:'2026-10-11'},5);expect(s.bodyweight).toEqual([]);expect(s.history).toEqual([]);
});
test('effort targets are prescriptions, never recorded results, and gate increases',()=>{
  const r=routine();r.exercises[0].sets.filter(s=>s.type==='work').forEach(s=>{s.rir=2;});
  const initial=act(newTraining('m'),{type:'routine.save',routine:r},0);
  const started=act(initial,{type:'start',id:'check-effort',routineId:'r'},10000);
  expect(started.active.exercises[0].sets[1].rir).toBeNull();
  expect(started.active.exercises[0].prescribedSets[1].rir).toBe(2);
  const missing=session(initial,'missing');expect(nextTargets(r.exercises[0],'r',missing.history).decision).toBe('hold');
  const missed=session(initial,'missed',{rir:1});expect(nextTargets(r.exercises[0],'r',missed.history).decision).toBe('hold');
  const met=session(initial,'met',{rir:2});expect(nextTargets(r.exercises[0],'r',met.history).decision).toBe('increase');
  const changed=structuredClone(r.exercises[0]);changed.sets[1].rir=3;
  expect(nextTargets(changed,'r',met.history).decision).toBe('initial');
});
