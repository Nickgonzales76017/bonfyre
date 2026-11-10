import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {trainingMaterial} from '../training-workspace.mjs';
import {requestStore} from '../training-requests.mjs';
const material=trainingMaterial('packs/aurekai-fitness-studio');
function isolated(fn){const home=mkdtempSync(join(tmpdir(),'training-draft-'));try{const store=requestStore(home,material),request=store.submit({id:crypto.randomUUID(),creatorId:'forge',programId:material.creators[0].routines[0].id,title:'Travel plan',details:'Please make a travel version.'});store.accept(request.id,'coach');fn({store,request,home});}finally{rmSync(home,{recursive:true,force:true});}}
test('unfinished names persist through native checkpoints and adapter restart without member delivery',()=>isolated(({store,request,home})=>{
  const draft=store.draftRead(request.id,'coach');expect(draft.revision).toBe(0);
  const routine=draft.sourceProgram;routine.name='';routine.exercises[0].name='';routine.privateHistory=['excluded'];routine.exercises[0].sets[0].done=true;
  const saved=store.saveDraft(request.id,routine,'coach',0);expect(saved.revision).toBe(1);
  const loaded=requestStore(home,material).draftRead(request.id,'coach');expect(loaded.routine.name).toBe('');expect(loaded.routine.exercises[0].name).toBe('');expect(loaded.routine.privateHistory).toBeUndefined();expect(loaded.routine.exercises[0].sets[0].done).toBe(false);
  expect(store.read(request.id,request.accessToken).delivery).toBeNull();expect(()=>store.deliverDraft(request.id,'coach',1)).toThrow('required');
}));
test('source program is the submission snapshot even when material changes',()=>isolated(({request,home})=>{
  const changed=structuredClone(material);changed.creators[0].routines[0].name='Changed after submission';
  const original=requestStore(home,changed).draftRead(request.id,'coach').sourceProgram;expect(original.name).toBe(material.creators[0].routines[0].name);
  original.exercises[0].sets[0].loadKg=999;expect(requestStore(home,changed).draftRead(request.id,'coach').sourceProgram.exercises[0].sets[0].loadKg).not.toBe(999);
}));
test('stale saves and stale review delivery preserve the newest draft and native state',()=>isolated(({store,request,home})=>{
  const routine=store.draftRead(request.id,'coach').sourceProgram;store.saveDraft(request.id,routine,'coach',0);routine.name='Newer draft';store.saveDraft(request.id,routine,'coach',1);
  expect(()=>store.saveDraft(request.id,routine,'coach',0)).toThrow('newer');expect(()=>store.deliverDraft(request.id,'coach',1)).toThrow('newer');
  expect(store.draftRead(request.id,'coach').revision).toBe(2);expect(store.read(request.id,request.accessToken).state).toBe('running');
  const db=new Database(join(home,'work.db'),{readonly:true});expect(db.query('select count(*) as n from workgraph_checkpoints').get().n).toBe(2);db.close();
  expect(store.deliverDraft(request.id,'coach',2).delivery.routine.name).toBe('Newer draft');expect(()=>store.saveDraft(request.id,routine,'coach',2)).toThrow('stale');
}));
test('another coach, expired claim, invalid targets and changed artifacts cannot save or deliver',()=>isolated(({store,request,home})=>{
  const routine=store.draftRead(request.id,'coach').sourceProgram;expect(()=>store.saveDraft(request.id,routine,'other',0)).toThrow('another coach');
  routine.exercises[0].sets[0].loadKg=-1;expect(()=>store.saveDraft(request.id,routine,'coach',0)).toThrow('range');routine.exercises[0].sets[0].loadKg=10;
  store.saveDraft(request.id,routine,'coach',0);const db=new Database(join(home,'work.db')),checkpoint=db.query('select artifact_uri from workgraph_checkpoints').get(),file=new URL(checkpoint.artifact_uri),original=readFileSync(file,'utf8'),artifact=JSON.parse(original);artifact.routine.name='Tampered';writeFileSync(file,JSON.stringify(artifact));
  expect(()=>store.draftRead(request.id,'coach')).toThrow('changed');expect(()=>store.deliverDraft(request.id,'coach',1)).toThrow('changed');writeFileSync(file,original);
  db.run('update workgraph_nodes set lease_expires_at_ms=1');db.close();expect(()=>store.saveDraft(request.id,routine,'coach',1)).toThrow('expired');expect(()=>store.deliverDraft(request.id,'coach',1)).toThrow('expired');
  expect(store.read(request.id,request.accessToken).delivery).toBeNull();
}));
