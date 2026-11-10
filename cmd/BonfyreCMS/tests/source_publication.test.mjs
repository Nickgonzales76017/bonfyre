import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,mkdirSync,copyFileSync,rmSync,readdirSync,existsSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const binary=process.env.BONFYRE_CMS_TEST_BINARY||resolve(import.meta.dir,'../bonfyre-cms');
const input=(id='a'.repeat(64))=>[1,id,'foreign:workshop','workshop-feed','A community workshop','Read the organizer’s actual repair notices.','atlas'];
async function fixture(fn){
 const home=mkdtempSync(join(tmpdir(),'source-publication-')),path=join(home,'cms.db'),schemas=join(home,'schemas'),deltas=join(home,'deltas');mkdirSync(schemas);mkdirSync(deltas);
 for(const name of ['source_collection','world','internet_source'])copyFileSync(resolve(import.meta.dir,'../content-types/'+name+'.json'),join(schemas,name+'.json'));
 const env={...process.env,BONFYRE_FACT_DELTAS:deltas},args=a=>[...a,'--db',path,'--schemas',schemas];
 const raw=(a,stdin)=>Bun.spawnSync([binary,...args(a)],{env,stdin,stdout:'pipe',stderr:'pipe',timeout:20000});
 const migrated=raw(['schema','migrate']);expect(migrated.exitCode).toBe(0);
 const created=raw(['entry','create','internet_source','--ns','root',JSON.stringify({source:'workshop-feed',family:'news',label:'Workshop notices',says:'Organizer notices',url:'https://example.test/workshop/feed',state:'observed',status:'published'})]);expect(created.exitCode,created.stderr.toString()).toBe(0);
 const db=new Database(path),request=(operation,p,ns='actor-7')=>{const r=raw(['entry','source-publication','--ns',ns,'--operation',operation],p instanceof Uint8Array?p:encode(p));return {code:r.exitCode,packet:decode(new Uint8Array(r.stdout)),diagnostic:r.stderr.toString()};};
 const snapshot=()=>Object.fromEntries(['source_collection','world','_ops','_relations'].map(t=>[t,db.query('SELECT * FROM '+t+' ORDER BY rowid').all()]));
 try{await fn({home,path,db,env,args,raw,request,snapshot,deltas});}finally{db.close();rmSync(home,{recursive:true,force:true});}
}
test('both records commit once, read back exactly and reconcile a discarded acknowledgement',()=>fixture(async f=>{
 const p=input(),empty=f.request('read',[1,p[1]]);expect(empty.packet[1]).toBe('absent');
 const saved=f.request('publish',p);expect(saved.code,saved.diagnostic).toBe(0);expect(saved.packet[1]).toBe('committed');expect(saved.packet[7]).toEqual(p);
 expect(f.db.query('SELECT count(*) AS n FROM source_collection').get().n).toBe(1);expect(f.db.query('SELECT count(*) AS n FROM world').get().n).toBe(1);
 const before=f.snapshot(),files=readdirSync(f.deltas);const read=f.request('read',[1,p[1]]);expect(read.packet.slice(0,8)).toEqual(saved.packet.slice(0,8));
 const retried=f.request('publish',p);expect(retried.code).toBe(0);expect(retried.packet[8]).toBe('UNCHANGED');expect(f.snapshot()).toEqual(before);expect(readdirSync(f.deltas)).toEqual(files);
 expect(f.request('read',[1,p[1]],'actor-8').packet[1]).toBe('absent');
 const second=f.request('publish',p,'actor-8');expect(second.code).toBe(0);expect(second.packet[4]).not.toBe(saved.packet[4]);
}));
test('world failure rolls back its collection, operation history, hooks and notifications',()=>fixture(async f=>{
 const marker=join(f.home,'hook');f.db.query('INSERT INTO _hooks(content_type,event,command) VALUES(?,?,?)').run('source_collection','after_create',`printf called >> '${marker}'`);
 f.db.exec("CREATE TRIGGER refuse_world BEFORE INSERT ON world BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
 const before=f.snapshot(),files=readdirSync(f.deltas),result=f.request('publish',input());expect(result.code).toBe(4);expect(result.packet[1]).toBe('refused');expect(f.snapshot()).toEqual(before);expect(readdirSync(f.deltas)).toEqual(files);expect(existsSync(marker)).toBe(false);
 f.db.exec('DROP TRIGGER refuse_world');expect(f.request('publish',input()).code).toBe(0);expect(readFileSync(marker,'utf8')).toBe('called');
}));
test('changed input, withdrawal, duplicate world coordinates and partial pairs refuse instead of overwriting',()=>fixture(async f=>{
 const p=input(),saved=f.request('publish',p);expect(saved.code).toBe(0);const before=f.snapshot();
 for(const index of [2,3,4,5,6]){const altered=[...p];altered[index]=index===6?'dossier':p[index]+'-changed';expect(f.request('publish',altered).code).toBe(4);expect(f.snapshot()).toEqual(before);}
 f.db.query("UPDATE world SET status='draft' WHERE id=?").run(saved.packet[6]);const withdrawn=f.snapshot();expect(f.request('publish',p).code).toBe(4);expect(f.snapshot()).toEqual(withdrawn);
 f.db.query("UPDATE world SET status='published' WHERE id=?").run(saved.packet[6]);
 const row=f.db.query('SELECT * FROM world WHERE id=?').get(saved.packet[6]);delete row.id;delete row.namespace;delete row.created_at;delete row.updated_at;delete row.published_at;
 expect(f.raw(['entry','create','world','--ns','actor-7',JSON.stringify(row)]).exitCode).toBe(0);const duplicate=f.snapshot();expect(f.request('read',[1,p[1]]).packet[1]).toBe('conflict');expect(f.request('publish',p).code).toBe(4);expect(f.snapshot()).toEqual(duplicate);
 f.db.query('DELETE FROM world WHERE namespace=?').run('actor-7');const partial=f.snapshot();expect(f.request('publish',p).code).toBe(4);expect(f.snapshot()).toEqual(partial);
}));
test('parallel retries commit one pair without introducing a new record store',()=>fixture(async f=>{
 const p=input();const packets=await Promise.all(Array.from({length:3},async()=>{const child=Bun.spawn([binary,...f.args(['entry','source-publication','--ns','actor-7','--operation','publish'])],{env:f.env,stdin:encode(p),stdout:'pipe',stderr:'pipe'});const [body,diagnostic,code]=await Promise.all([new Response(child.stdout).arrayBuffer(),new Response(child.stderr).text(),child.exited]);expect(code,diagnostic).toBe(0);return decode(new Uint8Array(body));}));
 expect(new Set(packets.map(p=>p[5]+':'+p[6])).size).toBe(1);expect(f.db.query('SELECT count(*) AS n FROM world').get().n).toBe(1);expect(f.db.query('SELECT count(*) AS n FROM source_collection').get().n).toBe(1);
 expect(f.db.query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%source_publication%'").all()).toEqual([]);
}));
test('invalid binary requests and sources outside the current publication boundary write nothing',()=>fixture(async f=>{
 const p=input(),before=f.snapshot();for(const invalid of [[1,p[1]],p.concat('extra'),[2,...p.slice(1)],[1,'bad',...p.slice(2)],[1,p[1],p[2],p[3],'',p[5],p[6]],[1,p[1],p[2],p[3],p[4],'x'.repeat(7201),p[6]],[1,p[1],p[2],p[3],'bad\u0000title',p[5],p[6]],new Uint8Array(17000)]){expect(f.request('publish',invalid).code).not.toBe(0);expect(f.snapshot()).toEqual(before);}
 const unknown=[...p];unknown[3]='unadmitted';expect(f.request('publish',unknown).packet[1]).toBe('source_unavailable');expect(f.snapshot()).toEqual(before);
 f.db.exec("UPDATE internet_source SET status='draft'");expect(f.request('publish',p).packet[1]).toBe('source_unavailable');expect(f.snapshot()).toEqual(before);
}));
test('an operation-history failure aborts the full owner transaction',()=>fixture(async f=>{
 f.db.exec("CREATE TRIGGER refuse_history BEFORE INSERT ON _ops WHEN new.target='world' BEGIN SELECT RAISE(ABORT,'history unavailable'); END");const before=f.snapshot();expect(f.request('publish',input()).code).toBe(4);expect(f.snapshot()).toEqual(before);
}));
test('readback preserves punctuation and UTF-8 through the existing CMS fields',()=>fixture(async f=>{
 const p=input();p[4]='Repair “night” 🧭';p[5]='Line one\nLine two: \\ tools <not markup>';const r=f.request('publish',p);expect(r.code,r.diagnostic).toBe(0);expect(f.request('read',[1,p[1]]).packet[7]).toEqual(p);
}));
