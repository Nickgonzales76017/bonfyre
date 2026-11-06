import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {homedir,tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createOperatorStore} from '../../../integrations/mcp/operator_store.mjs';
import {chapters,declarations,buildEstateDelivery,ownerView,verify,FIRST,BOUND} from './downstream-estates.mjs';
import {sha} from './downstream.mjs';
const plan=buildEstateDelivery();
const first=decode(readFileSync(join(homedir(),'.bonfyre/operator/history-material',FIRST+'.mpk')),BOUND);
function fixture(work){const home=mkdtempSync(join(tmpdir(),'estate-history-test-')),store=createOperatorStore({home,now:()=>200});try{return work(store,home);}finally{store.close();rmSync(home,{recursive:true,force:true});}}

test('chapter partition preserves preambles, nested lists, code fences and final bytes',()=>{
 const text='Original correction.\r\n# One\r\n## Nested\r\n- exact\r\n```text\n# not a split\n```\n# Two\nlast line';
 const rows=chapters(text);expect(rows).toHaveLength(3);expect(rows.map(r=>r.text).join('')).toBe(text);expect(rows[1].text).toContain('# not a split');expect(rows[1].text).toContain('## Nested');expect(rows[0].start).toBe(1);expect(rows.at(-1).end).toBe(9);
 expect(chapters('No headings.\nSecond line.').map(r=>r.text).join('')).toBe('No headings.\nSecond line.');
});
test('domain units preserve all preconditions, repeated field keys and neighboring defect notes',()=>{
 const text='pack fixture\nnote source scope\n\nwork wk:a\n  depends_on wk:b\n  evidence one\n  evidence two\ndefect d:one\n  detail retain me\nwork wk:b\n  authority human\n';
 const parsed=declarations(text,['work']);expect(parsed.units).toHaveLength(2);expect(parsed.preamble+parsed.units.map(r=>r.text).join('')).toBe(text);expect(parsed.units[0].text).toContain('evidence two');expect(parsed.units[0].text).toContain('defect d:one');expect(()=>declarations(text+'work wk:a\n',['work'])).toThrow(/Repeated/);
});
test('every full original brief is covered without shortening nested requirements',()=>{
 const originals=plan.references.filter(r=>r.kind==='original-user-message');expect(originals).toHaveLength(9);
 for(const ref of originals){const source=plan.sources.find(s=>s.label===ref.source),rows=plan.items.filter(i=>i.source===ref.source&&i.kind==='historical-workflow');expect(rows.length).toBeGreaterThan(0);const units=chapters(source.text);expect(units.map(u=>u.text).join('')).toBe(source.text);for(const unit of units){const item=rows.find(r=>r.key===unit.key);expect(item).toBeDefined();expect(item.unit_sha256).toBe(sha(unit.text));expect(item.goal.details).toContain(unit.text);}expect(ref.references.every(r=>r.path&&r.line>0&&r.timestamp)).toBe(true);}
 expect(plan.references.filter(r=>r.kind==='historical-operating-handoff-reference')).toHaveLength(16);
 expect(plan.items.filter(r=>r.kind==='setup-fleet')).toHaveLength(11);
});
test('mechanisms remain different from institutions, source records and actual relationships',()=>{
 const entries=plan.items.filter(i=>i.kind==='partner-mechanism');expect(entries).toHaveLength(112);expect(new Set(entries.map(i=>i.key)).size).toBe(112);
 expect(plan.references.find(r=>r.kind==='partner-populations')).toMatchObject({reconstruction:112,seed_profile_files:102});
 for(const row of entries){expect(row.classification).toBe('historical_mechanism_record');expect(row.goal.details).toContain('does_not_own');expect(row.goal.details).toContain('not a direction to restore it');}
 const lago=entries.find(i=>i.key==='Lago');expect(lago.goal.details).toContain('corporate accounting truth');expect(lago.goal.details).toContain('real consumer');
});
test('every receiving allocation resolves to an actual previously retained composition goal',()=>{
 const keys=new Set(first.items.map(i=>i.goal.dedupe_key)),current=new Set();
 for(const item of plan.items){expect(keys.has(item.goal.dedupe_key)).toBe(false);expect(current.has(item.goal.dedupe_key)).toBe(false);current.add(item.goal.dedupe_key);expect(item.composition_goals.length).toBeGreaterThan(0);for(const key of item.composition_goals)expect(keys.has(key)).toBe(true);expect(item.goal.recurring).toBe(false);expect(item.goal.interval_seconds).toBe(0);expect(item.goal.details).toContain('NOT present execution instructions');}
 expect(plan.shared).toContain('delegation is not inheritance');expect(plan.shared).toContain('grant-derived production flows');expect(plan.shared).toContain('per-group/per-item/per-segment overrides');
});
test('all actual source rows, H/X/N status, named roles and scheduled work survive',()=>{
 expect(plan.counts).toEqual({'historical-workflow':286,'setup-fleet':11,'partner-mechanism':112,'upper90-receiver':139,'operating-circuit':1,'institution-continuity':1,'institution-relationship':15,'white-label-product':9,'institution-setup-flow':8,'workspace-continuation':5,'reuse-candidate':163});
 expect(plan.items).toHaveLength(750);const c=plan.items.filter(i=>i.kind==='reuse-candidate');for(const item of c){const s=plan.sources.find(s=>s.label===item.source);expect(item.goal.details).toContain(s.text.split('\n')[item.start-1]);expect(item.goal.details).toContain('not a release or readiness status');}
 expect(plan.items.some(i=>i.key==='schedule_binding sb:payment-application')).toBe(true);expect(plan.items.some(i=>i.key==='product_basis pb:jrss-veterinarian')).toBe(true);expect(plan.items.find(i=>i.kind==='institution-continuity').goal.details).toContain('work_closed');
 verify(plan);const bytes=encode(plan,BOUND),back=decode(bytes,BOUND);expect(back.items).toEqual(plan.items);expect(back.sources.map(s=>s.sha256)).toEqual(plan.sources.map(s=>s.sha256));expect(sha(encode(buildEstateDelivery(),BOUND))).toBe(sha(bytes));
});
test('supplement imports once beside the original 803 without changing existing work or dispatching',()=>fixture(store=>{
 const original=store.importGoals(first.items.map(i=>i.goal),{apply:true}),db=new Database(store.path);
 try{db.query('UPDATE goals SET state=?,attempts=9,last_error=?,updated_at=99 WHERE id=?').run('blocked','retained native failure',original.goals[0].id);const before=db.query('SELECT * FROM goals ORDER BY id').all();
 const dry=store.importGoals(plan.items.map(i=>i.goal));expect(dry.missing).toBe(750);expect(db.query('SELECT COUNT(*) n FROM goals').get().n).toBe(803);
 const applied=store.importGoals(plan.items.map(i=>i.goal),{apply:true});expect(applied.created).toBe(750);const replay=store.importGoals(plan.items.map(i=>i.goal),{apply:true});expect(replay.created).toBe(0);expect(replay.unchanged).toBe(750);expect(replay.goals.map(r=>r.id)).toEqual(applied.goals.map(r=>r.id));
 for(const old of before)expect(db.query('SELECT * FROM goals WHERE id=?').get(old.id)).toEqual(old);expect(db.query('SELECT COUNT(*) n FROM runs').get().n).toBe(0);expect(db.query('SELECT COUNT(*) n FROM actions').get().n).toBe(0);
 }finally{db.close();}
}));
test('status reads current native dispositions and source identities without creating shadow state',()=>fixture(store=>{
 const added=store.importGoals(plan.items.map(i=>i.goal),{apply:true}),db=new Database(store.path);
 try{const one=added.goals[0];db.query('UPDATE goals SET state=?,attempts=2 WHERE id=?').run('in_progress',one.id);const before=db.query('SELECT * FROM goals ORDER BY id').all();
 const view=ownerView(plan,{database:store.path,query:one.id});expect(view.matched).toBe(1);expect(view.rows[0].state).toBe('in_progress');expect(view.rows[0].attempts).toBe(2);expect(view.rows[0].definition_matches).toBe(true);expect(view.states.in_progress).toBe(1);
 const search=ownerView(plan,{database:store.path,query:'kind:partner-mechanism Lago'});expect(search.matched).toBe(1);expect(search.rows[0].key).toBe('Lago');expect(ownerView(plan,{database:store.path,query:'kind:white-label-product',limit:2}).more).toBe(true);
 expect(ownerView(plan,{database:store.path,query:'lane:upper90 submission_date'}).matched).toBeGreaterThan(0);
 db.query('UPDATE goals SET details=? WHERE id=?').run('separately revised',one.id);expect(ownerView(plan,{database:store.path,query:one.id}).rows[0].definition_matches).toBe(false);db.query('UPDATE goals SET details=? WHERE id=?').run(before.find(r=>r.id===one.id).details,one.id);expect(db.query('SELECT * FROM goals ORDER BY id').all()).toEqual(before);
 }finally{db.close();}
}));
test('changed input refuses intake instead of silently changing a reviewed requirement',()=>{
 const home=mkdtempSync(join(tmpdir(),'history-source-change-'));try{const path=join(home,'source');writeFileSync(path,'one');const p={sources:[{path,label:'original',sha256:sha('one')}]};verify(p);writeFileSync(path,'two');expect(()=>verify(p)).toThrow(/Changed source/);}finally{rmSync(home,{recursive:true,force:true});}
});
