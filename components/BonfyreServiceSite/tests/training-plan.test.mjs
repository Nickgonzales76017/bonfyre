import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {Database} from 'bun:sqlite';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
import {newTraining,trainingCommand,exportTraining,importTraining,personalRecords} from '../training-model.mjs';
import {planSchedule,dayOffset,checkPlan} from '../training-plan.mjs';
import {publishingStore,publicProgramRoutine} from '../training-publishing.mjs';
import {planStore} from '../training-plan-store.mjs';
import {creatorAuth} from '../training-creator.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio'),routine=()=>structuredClone(material.creators[0].routines[0]);
function plan(){const r=routine();return {id:'plan-test',name:'Three week strength',description:'Build, progress, then recover.',routines:[r],weeks:[{name:'Foundation',sessions:[{id:'one',day:1,routineId:r.id},{id:'two',day:4,routineId:r.id}]},{name:'Build',sessions:[{id:'three',day:2,routineId:r.id}]},{name:'Recovery',sessions:[]}]};}
test('calendar plans derive completion from workouts, preserve skips, and round-trip in member exports',()=>{
  let state=trainingCommand(newTraining('member:test'),{type:'plan.adopt',id:'enrollment',plan:plan(),startDay:'2026-10-30'});const rows=planSchedule(state.plans[0]);expect(rows.map(r=>r.day)).toEqual(['2026-10-30','2026-11-02','2026-11-07']);expect(dayOffset('2028-02-28',1)).toBe('2028-02-29');
  state=trainingCommand(state,{type:'start',id:'workout',enrollmentId:'enrollment',slotId:'one'},1000);expect(state.active.planSession.scheduledDay).toBe('2026-10-30');expect(planSchedule(state.plans[0],state.history,state.active)[0].status).toBe('active');
  expect(()=>trainingCommand(state,{type:'plan.skip',id:'enrollment',slotId:'one'},1100)).toThrow();expect(()=>trainingCommand(state,{type:'plan.archive',id:'enrollment'},1100)).toThrow();
  const e=state.active.exercises[0];state=trainingCommand(state,{type:'set',exerciseId:e.id,setId:e.sets[0].id,values:{done:true}},1200);state=trainingCommand(state,{type:'finish'},2000);expect(planSchedule(state.plans[0],state.history)[0].status).toBe('completed');
  expect(()=>trainingCommand(state,{type:'start',id:'duplicate',enrollmentId:'enrollment',slotId:'one'},3000)).toThrow();
  state=trainingCommand(state,{type:'plan.skip',id:'enrollment',slotId:'two'},3000);expect(planSchedule(state.plans[0],state.history)[1].status).toBe('skipped');state=trainingCommand(state,{type:'plan.restore',id:'enrollment',slotId:'two'},3100);expect(planSchedule(state.plans[0],state.history)[1].status).toBe('planned');
  const imported=importTraining(exportTraining(state),'member:other');expect(imported.plans).toEqual(state.plans);expect(imported.history).toEqual(state.history);
  const removed=trainingCommand(state,{type:'history.remove',id:'workout'},3200);expect(planSchedule(removed.plans[0],removed.history)[0].status).toBe('planned');
  const archived=trainingCommand(state,{type:'plan.archive',id:'enrollment'},3200);expect(archived.history).toEqual(state.history);expect(()=>trainingCommand(archived,{type:'start',id:'late',enrollmentId:'enrollment',slotId:'two'},3300)).toThrow();
});
test('planned session targets override last-workout prefill and independent publications cannot mix exercise records',()=>{
  const source=routine(),a=publicProgramRoutine(source,'forge',crypto.randomUUID(),1,'Strength'),b=publicProgramRoutine(source,'forge',crypto.randomUUID(),1,'Mobility');expect(a.exercises[0].id).not.toBe(b.exercises[0].id);
  const later=publicProgramRoutine(source,'forge',a.source.program,2,'Strength II');expect(later.exercises[0].id).toBe(a.exercises[0].id);
  let state=trainingCommand(newTraining('member:test'),{type:'routine.save',routine:source});state=trainingCommand(state,{type:'start',id:'heavy',routineId:source.id},1000);state=trainingCommand(state,{type:'set',exerciseId:source.exercises[0].id,setId:source.exercises[0].sets[0].id,values:{loadKg:90,done:true}},1200);state=trainingCommand(state,{type:'finish'},2000);
  const p=plan();p.routines[0].exercises[0].sets[0].loadKg=10;state=trainingCommand(state,{type:'plan.adopt',id:'recovery',plan:p,startDay:'2026-10-11'},2100);state=trainingCommand(state,{type:'start',id:'light',enrollmentId:'recovery',slotId:'one'},2200);expect(state.active.exercises[0].sets[0].loadKg).toBe(10);
  const records=personalRecords([{exercises:[{...a.exercises[0],sets:[{...a.exercises[0].sets[0],loadKg:50,done:true}]}]},{exercises:[{...b.exercises[0],sets:[{...b.exercises[0].sets[0],loadKg:5,done:true}]}]}]);expect(records[a.exercises[0].id].loadKg).toBe(50);expect(records[b.exercises[0].id].loadKg).toBe(5);
});
test('malformed schedules and duplicate enrollment are refused atomically',()=>{
  const original=newTraining('member:test');for(const broken of [{...plan(),weeks:[]},{...plan(),routines:[]},{...plan(),weeks:[{name:'Week',sessions:[{id:'x',day:8,routineId:routine().id}]}]}])expect(()=>trainingCommand(original,{type:'plan.adopt',id:'enroll',plan:broken,startDay:'2026-10-11'})).toThrow();expect(original.plans).toEqual([]);
  const state=trainingCommand(original,{type:'plan.adopt',id:'enroll',plan:plan(),startDay:'2026-10-11'});expect(()=>trainingCommand(state,{type:'plan.adopt',id:'other',plan:plan(),startDay:'2026-10-11'})).toThrow();expect(()=>trainingCommand(original,{type:'plan.adopt',id:'bad-date',plan:plan(),startDay:'9999-12-31'})).toThrow();
});
async function isolated(fn){const home=mkdtempSync(join(tmpdir(),'training-plans-')),publishing=publishingStore(home,material,null),store=planStore(home,material,publishing);try{await fn({home,publishing,store});}finally{rmSync(home,{recursive:true,force:true});}}
test('native plan drafts pin routine snapshots, refuse stale publication and retain public versions after edits',()=>isolated(({home,publishing,store})=>{
  const id=crypto.randomUUID(),p=plan();p.id=id;const first=store.saveDraft('forge','actor:editor',id,{plan:p,expectedRevision:0});expect(first.revision).toBe(1);expect(planStore(home,material,publishing).readDraft('forge',id)).toEqual(first);expect(store.saveDraft('forge','actor:editor',id,{plan:p,expectedRevision:0}).revision).toBe(1);
  const forged=structuredClone(p);forged.routines[0].exercises[0].sets[0].loadKg=1900;const second=store.saveDraft('forge','actor:editor',id,{plan:forged,expectedRevision:1});expect(second.plan.routines[0].exercises[0].sets[0].loadKg).toBe(routine().exercises[0].sets[0].loadKg);
  const input={publicationId:crypto.randomUUID(),planId:crypto.randomUUID(),expectedVersion:0,draftId:id,draftRevision:1};expect(()=>store.publish('forge','actor:editor',input)).toThrow('changed after review');const good={...input,draftRevision:2},release=store.publish('forge','actor:editor',good);expect(release.version).toBe(1);expect(JSON.stringify(store.list('forge',{publicOnly:true}))).not.toContain(id);expect(store.list('still',{publicOnly:true})).toEqual([]);
  const renamed={...second.plan,name:'New edition'};store.saveDraft('forge','actor:editor',id,{plan:renamed,expectedRevision:2});expect(store.list('forge',{publicOnly:true})[0].plan.name).toBe(p.name);expect(store.publish('forge','actor:editor',good).releaseId).toBe(release.releaseId);
  const next=store.publish('forge','actor:editor',{...good,publicationId:crypto.randomUUID(),expectedVersion:1,draftRevision:3});expect(next.version).toBe(2);store.withdraw('forge',next.releaseId);expect(store.list('forge',{publicOnly:true})).toEqual([]);expect(release.plan.name).toBe(p.name);
  expect(()=>store.saveDraft('forge','actor:editor',id,{plan:p,expectedRevision:1})).toThrow('another tab');expect(()=>store.readDraft('still',id)).toThrow('not found');
  const db=new Database(join(home,'catalogue.db'),{readonly:true});expect(db.query('select count(*) n from training_plan_release').get().n).toBe(2);db.close();
}));
test('plan routes require current studio program grants and never open member work',()=>isolated(async({home,publishing,store})=>{
  const db=join(home,'auth.db'),native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));native('http-signup',[],JSON.stringify({email:'plans@example.test',password:'plan-test-password',name:'Plan editor'}));native('actor-bind',['--user-id','1','--actor','actor:plans']);native('institution-grant',['--user-id','1','--institution','training-forge-programs','--role','read']);
  const handler=trainingHandler(material,4190,{requests:{},publishing,plans:store,auth:creatorAuth(db)}),origin='http://127.0.0.1:4190';let cookie='';const call=(path,body,headers={})=>handler(new Request(origin+'/api/training/creator'+path,{method:body===undefined?'GET':'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json',...headers},body:body===undefined?undefined:JSON.stringify(body)}));cookie=(await call('/login',{email:'plans@example.test',password:'plan-test-password'})).headers.get('set-cookie').split(';')[0];
  const id=crypto.randomUUID(),path='/studios/forge/plan-drafts/'+id,body={plan:plan(),expectedRevision:0};expect((await call('/studios/forge/plan-routines')).status).toBe(200);expect((await call(path,body)).status).toBe(403);native('institution-grant',['--user-id','1','--institution','training-forge-programs','--role','write']);expect((await call(path,body,{origin:'https://elsewhere.test'})).status).toBe(403);expect((await call(path,body)).status).toBe(200);expect((await call('/studios/still/plan-drafts/'+id,body)).status).toBe(403);
  let send;const stream=new ReadableStream({start(controller){send=()=>{native('institution-revoke',['--user-id','1','--institution','training-forge-programs']);controller.enqueue(new TextEncoder().encode(JSON.stringify({...body,expectedRevision:1})));controller.close();};}});const pending=handler(new Request(origin+'/api/training/creator'+path,{method:'POST',headers:{host:'127.0.0.1:4190',origin,cookie,'content-type':'application/json'},body:stream,duplex:'half'}));send();expect((await pending).status).toBe(403);expect(store.readDraft('forge',id).revision).toBe(1);
}));
