// Execute every calculation through the real native BonfyreQuery -> DuckDB owner.
// No model, upstream Python runtime, external connection, user store, or mock query.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=resolve(fileURLToPath(new URL('..',import.meta.url)));
const binary=process.env.BONFYRE_QUERY_TEST_BINARY || join(root,'bonfyre-query');
const program=readFileSync(join(root,'examples/prioritization.sql'),'utf8');
const fields=['ordinal','id','name','reach','impact','confidence','effort'];
const literal=s=>"'"+String(s).replaceAll("'","''")+"'";
const csv=v=>v===null||v===undefined?'\\N':'"'+String(v).replaceAll('"','""')+'"';
const row=(id,reach=100,impact='high',confidence='high',effort='s',extra={})=>({id,name:id,reach,impact,confidence,effort,...extra});
function run(rows,capacity='15',select='SELECT * FROM bf_priority_roadmap ORDER BY score_rank NULLS LAST,ordinal',settings=null){
 const dir=mkdtempSync(join(tmpdir(),'bf-priorities-'));
 try{
  const path=join(dir,"selected ' input.csv");
  writeFileSync(path,fields.join(',')+'\n'+rows.map((r,n)=>fields.map(k=>csv(k==='ordinal'?(r.ordinal??n+1):r[k])).join(',')).join('\n')+'\n');
  const sql=`CREATE TEMP TABLE bf_priority_input AS SELECT try_cast(ordinal AS BIGINT) AS ordinal,* EXCLUDE(ordinal) FROM read_csv(${literal(path)},header=true,all_varchar=true,nullstr='\\N');\nCREATE TEMP TABLE bf_priority_settings(capacity VARCHAR);\n`+
   (settings===null?`INSERT INTO bf_priority_settings VALUES (${literal(capacity)});`:settings)+program+'\n'+select+';';
  return JSON.parse(execFileSync(binary,['sql',':memory:',sql],{encoding:'utf8',timeout:60000,maxBuffer:8*1024*1024,stdio:['pipe','pipe','pipe']}));
 }finally{rmSync(dir,{recursive:true,force:true});}
}
const donor=[row('dashboard',5000,'high','high','l'),row('push',10000,'massive','medium','m'),row('dark',8000,'medium','high','s'),row('rate-limit',2000,'low','high','xs'),row('social',12000,'high','medium','m'),row('pdf',3000,'medium','low','s'),row('team',4000,'massive','low','xl'),row('search',15000,'high','high','m'),row('onboarding',20000,'massive','high','s'),row('analytics',6000,'high','medium','l')];
test('upstream ten-feature sample scores, stable order and valid sequential roadmap',()=>{
 const r=run(donor);
 assert.deepEqual(r.map(x=>x.id),['onboarding','search','push','social','dark','dashboard','analytics','rate-limit','pdf','team']);
 assert.deepEqual(r.map(x=>x.score_cents),[2000000,600000,480000,384000,266667,125000,120000,100000,50000,46154]);
 assert.deepEqual(r.map(x=>x.quarter_no),[1,1,1,2,2,3,4,4,4,5]);
 assert.ok(r.every(x=>x.capacity_remaining_after>=0));
});
test('portfolio retains effort, estimated reach, quick wins and big bets',()=>{
 const [r]=run(donor,'15','SELECT * FROM bf_priority_portfolio');
 assert.equal(r.valid_rows,10);assert.equal(r.held_rows,0);assert.equal(r.estimated_effort_months,54);
 assert.equal(r.reach_sum_not_unique_users,'85000');assert.equal(r.quick_wins,1);assert.equal(r.big_bets,3);
 assert.equal(r.average_score_cents,417182);
});
test('source ordinal resolves a rounded-score tie, never title sorting',()=>{
 const r=run([row('z',1,'minimal','low','xl'),row('a',1,'minimal','low','xl')]);
 assert.deepEqual(r.map(x=>x.id),['z','a']);
});
test('exact integer ties round to even in either direction',()=>{
 const r=run([row('even',8,'minimal','low','l'),row('odd',24,'minimal','low','l')]);
 assert.equal(r.find(x=>x.id==='even').score_display,'0.12');
 assert.equal(r.find(x=>x.id==='odd').score_display,'0.38');
});
test('malformed categorical values and reach remain held without source defaults',()=>{
 const r=run([row('bad-impact',100,'unknown'),row('bad-confidence',100,'high',''),row('bad-effort',100,'high','high','zero'),row('negative','-3'),row('nan','NaN'),row('decimal','1.5'),row('missing',null),row('large','1000000000001')]);
 assert.equal(r.length,8);assert.ok(r.every(x=>x.schedule_state==='held'&&x.score_cents===null&&x.quarter_no===null&&x.hold_reason));
});
test('oversize feature has a score but cannot overbook or create an empty quarter',()=>{
 const r=run([row('oversize',10000,'massive','high','xl'),row('fits',100,'high','high','s')],'10');
 assert.equal(r[0].schedule_state,'requires_split');assert.equal(r[0].quarter_no,null);
 assert.equal(r[1].quarter_no,1);assert.equal(r[1].quarter_used_after,3);
});
test('duplicate identities are held even when their estimates differ',()=>{
 const r=run([row('same',100),row('same',200),row('other',300)]);
 assert.equal(r.filter(x=>x.schedule_state==='held').length,2);assert.equal(r.find(x=>x.id==='other').quarter_no,1);
});
test('duplicate ordinals cannot silently change tie ordering',()=>{
 const r=run([row('one',100,'high','high','s',{ordinal:7}),row('two',200,'high','high','s',{ordinal:7})]);
 assert.ok(r.every(x=>x.hold_reason.includes('ordinal')&&x.quarter_no===null));
});
test('case-insensitive declared categories and zero estimated reach remain valid',()=>{
 const [r]=run([row('zero',0,'HIGH','MEDIUM','XS')]);assert.equal(r.score_cents,0);assert.equal(r.quarter_no,1);
});
test('source text including punctuation is retained as data',()=>{
 const name='Scope, "release"; SELECT 1;\nSecond line';const [r]=run([row('safe',100,'high','high','s',{name})]);assert.equal(r.name,name);
});
test('sensitivity uses the same input identities and explicit scenario factors',()=>{
 const r=run([row('feature',120,'high','medium','m')],'15','SELECT * FROM bf_priority_sensitivity ORDER BY scenario');
 assert.deepEqual(r.map(x=>[x.scenario,x.score_cents]),[['baseline',3840],['double_effort',1920],['double_reach',7680],['half_reach',1920]]);
});
test('empty inputs remain empty rather than receiving example data',()=>{
 assert.deepEqual(run([]),[]);const [r]=run([],'15','SELECT * FROM bf_priority_portfolio');assert.equal(r.input_rows,0);assert.equal(r.average_score_cents,null);
});
test('invalid capacity cannot silently become a default',()=>{
 for(const c of ['0','-2','1.5','unlimited','100001'])assert.throws(()=>run([row('a')],c));
});
test('zero or multiple capacity rows refuse the entire evaluation',()=>{
 assert.throws(()=>run([row('a')],'15','SELECT * FROM bf_priority_ranking',''));
 assert.throws(()=>run([row('a')],'15','SELECT * FROM bf_priority_ranking',"INSERT INTO bf_priority_settings VALUES ('10'),('20');"));
});
test('declared reach ceiling is calculated without overflow',()=>{
 const [r]=run([row('ceiling','1000000000000','massive','high','xs')]);assert.equal(r.score_cents,300000000000000);
});
test('ten thousand records evaluated by the native owner; bound refuses a larger batch',()=>{
 const input=Array.from({length:10000},(_,i)=>row('feature-'+i,i,'medium','medium','m'));
 const [r]=run(input,'15','SELECT count(*) AS rows,count(*) FILTER(WHERE hold_reason=\'\') AS valid FROM bf_priority_ranking');
 assert.equal(r.rows,10000);assert.equal(r.valid,10000);
 assert.throws(()=>run([...input,row('too-many')],'15','SELECT count(*) FROM bf_priority_ranking'));
});
test('repeat evaluation yields exactly equal results without persistent state',()=>{
 assert.deepEqual(run(donor),run(donor));
});
