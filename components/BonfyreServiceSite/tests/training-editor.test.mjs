import {test,expect} from 'bun:test';
import {routineDraft,changeDraft,finishDraft,editorHTML} from '../training-editor.mjs';
import {newTraining,trainingCommand as act,makeSet,sessionSequence,nextTrainingSet,restSeconds} from '../training-model.mjs';
const exercise=(id,sets=[makeSet(id+'1'),makeSet(id+'2')])=>({id,name:id,kind:'resistance',restSeconds:60,superset:'A',sets});
const routine={id:'r',name:'Pair',source:{creator:'c',program:'p',version:'1'},exercises:[exercise('squat'),exercise('row')]};
test('editing retains exercise and set identities, moves targets together, and marks a member adaptation',()=>{
  const original=JSON.stringify(routine);let d=routineDraft(routine);
  d=changeDraft(d,'down','squat');expect(d.exercises[0].id).toBe('row');
  d=changeDraft(d,'set-add','row');expect(d.exercises[0].sets).toHaveLength(3);
  expect(new Set(d.exercises[0].sets.map(s=>s.id)).size).toBe(3);
  d=changeDraft(d,'set-remove','row','row1');expect(d.exercises[0].sets[0].id).toBe('row2');
  d=changeDraft(d,'add');d.exercises.at(-1).name='Carry';d.exercises.at(-1).kind='timed';d.exercises.at(-1).sets.forEach(s=>{s.seconds=30;s.reps=0;});
  expect(finishDraft(d).source.adapted).toBe(true);expect(JSON.stringify(routine)).toBe(original);
  const html=editorHTML({...d,name:'<img src=x onerror=alert(1)>'},'lb');expect(html).not.toContain('<img');expect(html).toContain('&lt;img');
});
test('saved template edits cannot rewrite active or completed workout snapshots',()=>{
  let s=act(act(newTraining('member'),{type:'routine.save',routine},0),{type:'start',routineId:'r',id:'w'},1000);
  const edited=routineDraft(routine);edited.exercises[0].name='Different name';edited.exercises[0].sets[0].loadKg=90;
  s=act(s,{type:'routine.save',routine:finishDraft(edited)},2000);expect(s.active.exercises[0].name).toBe('squat');
  s=act(s,{type:'set',exerciseId:'squat',setId:'squat1',values:{done:true}},3000);s=act(s,{type:'finish'},4000);
  s=act(s,{type:'routine.save',routine},5000);expect(s.history[0].exercises[0].sets[0].loadKg).toBe(0);
  expect(s.history[0].exercises[0].sets[0].done).toBe(true);
});
test('supersets alternate exercises and rest after a round, including unequal set counts',()=>{
  const r=structuredClone(routine);r.exercises[1].sets.pop();
  let s=act(act(newTraining('member'),{type:'routine.save',routine:r},0),{type:'start',routineId:'r',id:'w'},1000);
  expect(sessionSequence(s.active).map(x=>x.setId)).toEqual(['squat1','row1','squat2']);
  s=act(s,{type:'set',exerciseId:'squat',setId:'squat1',values:{done:true}},2000);
  expect(restSeconds(s.active,2000)).toBe(0);expect(nextTrainingSet(s.active).exerciseId).toBe('row');
  s=act(s,{type:'set',exerciseId:'row',setId:'row1',values:{done:true}},3000);
  expect(restSeconds(s.active,3000)).toBe(60);expect(nextTrainingSet(s.active).setId).toBe('squat2');
});
test('last-load prefill counts each set type independently',()=>{
  const r=structuredClone(routine);r.exercises=[exercise('squat',[makeSet('warm',{type:'warmup',loadKg:5}),makeSet('work1',{loadKg:15}),makeSet('work2',{loadKg:20})])];
  let s=act(act(newTraining('member'),{type:'routine.save',routine:r},0),{type:'start',routineId:'r',id:'w'},1000);
  for(const set of r.exercises[0].sets)s=act(s,{type:'set',exerciseId:'squat',setId:set.id,values:{done:true}},2000);
  s=act(s,{type:'finish'},3000);r.exercises[0].sets.forEach(x=>{x.loadKg=0;});s=act(s,{type:'routine.save',routine:r},4000);
  s=act(s,{type:'start',routineId:'r',id:'w2'},5000);expect(s.active.exercises[0].sets.map(x=>x.loadKg)).toEqual([5,15,20]);
});
