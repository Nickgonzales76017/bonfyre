import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {createOperatorStore} from '../operator_store.mjs';
const goal=(id,details='Exact source requirement; no completed implementation claimed.')=>({title:'Deliver '+id,details,priority:70,source:'reviewed-history',dedupe_key:'history:'+id,recurring:false,interval_seconds:0});
function fixture(work){const home=mkdtempSync(join(tmpdir(),'operator-history-'));const store=createOperatorStore({home,now:()=>100});try{return work(store,home);}finally{store.close();rmSync(home,{recursive:true,force:true});}}
test('bulk preparation is read-only; apply and exact replay keep stable goal identities',()=>fixture(store=>{
 const rows=[goal('a'),goal('b')];expect(store.importGoals(rows)).toMatchObject({applied:false,created:0,missing:2});
 const first=store.importGoals(rows,{apply:true});expect(first).toMatchObject({created:2,unchanged:0});
 expect(store.importGoals(rows,{apply:true})).toMatchObject({created:0,unchanged:2,goals:first.goals.map(r=>({...r,outcome:'unchanged'}))});
}));
test('a changed contract, priority or source refuses the whole import before adding another goal',()=>fixture(store=>{
 store.importGoals([goal('a')],{apply:true});
 for(const change of [{details:'different contract'},{priority:90},{source:'other-session'},{title:'Changed title'}])
   expect(()=>store.importGoals([goal('new'),{...goal('a'),...change}],{apply:true})).toThrow(/definitions changed/);
 expect(store.importGoals([goal('new')]).missing).toBe(1);
}));
test('replay leaves resolved, in-progress, blocked and retried state and failure history untouched',()=>fixture((store,home)=>{
 const states=['resolved','in_progress','blocked','retry','superseded'];const rows=states.map(s=>goal(s));
 store.importGoals(rows,{apply:true});const db=new Database(store.path);
 try{for(const state of states)db.query('UPDATE goals SET state=?,attempts=17,retry_at=999,last_error=?,updated_at=55 WHERE dedupe_key=?').run(state,'fixture retained history','history:'+state);
 const before=db.query('SELECT * FROM goals ORDER BY dedupe_key').all();store.importGoals(rows,{apply:true});
 expect(db.query('SELECT * FROM goals ORDER BY dedupe_key').all()).toEqual(before);
 expect(db.query('SELECT COUNT(*) n FROM runs').get().n).toBe(0);expect(db.query('SELECT COUNT(*) n FROM actions').get().n).toBe(0);
 }finally{db.close();}
 const stop=join(home,'STOP');writeFileSync(stop,'paused fixture');store.importGoals([goal('c')],{apply:true});expect(readFileSync(stop,'utf8')).toBe('paused fixture');
}));
test('invalid or duplicate rows and scheduling requests leave the entire batch absent',()=>fixture(store=>{
 for(const bad of [[goal('a'),goal('a')],[goal('a'),{...goal('b'),priority:0}],[goal('a'),{...goal('b'),state:'resolved'}],[{...goal('a'),recurring:true}],[{...goal('a'),details:'x\0y'}]])
   expect(()=>store.importGoals(bad,{apply:true})).toThrow();
 expect(store.importGoals([goal('a'),goal('b')]).missing).toBe(2);
}));
test('database insertion failure rolls back earlier members in the same batch',()=>fixture(store=>{
 store.importGoals([goal('baseline')],{apply:true});const db=new Database(store.path);
 try{db.exec("CREATE TRIGGER fixture_refusal BEFORE INSERT ON goals WHEN NEW.dedupe_key='history:b' BEGIN SELECT RAISE(ABORT,'fixture refusal'); END");
 expect(()=>store.importGoals([goal('a'),goal('b')],{apply:true})).toThrow(/fixture refusal/);
 expect(db.query("SELECT COUNT(*) n FROM goals WHERE dedupe_key IN ('history:a','history:b')").get().n).toBe(0);
 }finally{db.close();}
}));
test('input snapshot and source terminology survive without a new status or format store',()=>fixture(store=>{
 const text='Keep source object → selected part → contextual inclusion distinct.\nUnknown quantities remain unknown.\noriginal row\tPARTIAL 2026-10-21';const batch=[goal('unicode',text)];
 const result=store.importGoals(batch,{apply:true});batch[0].details='caller changed later';
 expect(store.getGoal(result.goals[0].id).details).toBe(text);expect(store.getGoal(result.goals[0].id).state).toBe('queued');
}));
