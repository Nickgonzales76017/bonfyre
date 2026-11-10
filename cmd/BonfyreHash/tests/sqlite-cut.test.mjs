import {test,expect,beforeAll,afterAll} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,writeFileSync,readFileSync,realpathSync,rmSync,symlinkSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_HASH_BINARY||resolve(import.meta.dir,'../bonfyre-hash');
const temp=realpathSync(mkdtempSync(join(tmpdir(),'bonfyre-sqlite-cut-')));
let serial=0;
const setup=()=>{const path=join(temp,`source-${++serial}.db`);return {path,db:new Database(path)};};
const run=(path,table,bin=binary)=>Bun.spawnSync([bin,'sqlite-table',path,table],{stdout:'pipe',stderr:'pipe',timeout:10000});
function cut(path,table='data') {
  const r=run(path,table);expect(r.exitCode).toBe(0);
  expect(r.stdout.toString()).toMatch(/^BFSQL1\t[a-f0-9]{64}\t\d+\t\d+\n$/);
  const [,digest,rows,columns]=r.stdout.toString().trim().split('\t');return {digest,rows:Number(rows),columns:Number(columns)};
}
function reference(ddl,columns,rows) {
  const h=createHash('sha256');
  const u64=n=>{const b=Buffer.alloc(8);b.writeBigUInt64BE(BigInt.asUintN(64,BigInt(n)));h.update(b);};
  const span=value=>{const b=Buffer.from(value);u64(b.length);h.update(b);};
  const cell=([type,value])=>{h.update(Buffer.from([type]));if(type===1)u64(value);else if(type===2){const b=Buffer.alloc(8);b.writeDoubleBE(value);h.update(b);}else if(type===3||type===4)span(value);};
  span('bonfyre.sqlite-table-cut.v1');cell([3,ddl]);u64(columns.length);
  for(const [name,type]of columns){span(name);span(type);}
  for(const row of rows){h.update('R');for(const value of row)cell(value);}
  h.update('E');u64(rows.length);return h.digest('hex');
}
beforeAll(()=>{expect(existsSync(binary)).toBe(true);});
afterAll(()=>rmSync(temp,{recursive:true,force:true}));
test('native typed cut matches an independent encoder without narrowing integers or strings',()=>{
  const {path,db}=setup(),ddl='CREATE TABLE data(i INTEGER,r REAL,t TEXT,b BLOB,n)';db.exec(ddl);
  const text='private\0café\nline',blob=Buffer.from([0,255,39]);
  db.query('INSERT INTO data VALUES(?,?,?,?,?)').run(9223372036854775807n,1.25,text,blob,null);db.close();
  expect(cut(path)).toEqual({digest:reference(ddl,[['i','INTEGER'],['r','REAL'],['t','TEXT'],['b','BLOB'],['n','']],[[[1,9223372036854775807n],[2,1.25],[3,text],[4,blob],[5,null]]]),rows:1,columns:5});
});
test('empty tables retain schema identity and null text blob integer real are distinct',()=>{
  const {path,db}=setup(),ddl='CREATE TABLE data(v)';db.exec(ddl);
  expect(cut(path).digest).toBe(reference(ddl,[['v','']],[]));
  const seen=new Set();for(const sql of ['NULL',"''","X''",'1','1.0',"'1'","X'31'",'-9223372036854775808']){db.exec('DELETE FROM data; INSERT INTO data VALUES('+sql+')');seen.add(cut(path).digest);}
  expect(seen.size).toBe(8);db.close();
});
test('unrelated table edits and covering indexes do not invalidate the selected cut',()=>{
  const {path,db}=setup();db.exec("CREATE TABLE data(k TEXT,v TEXT); INSERT INTO data VALUES('z','one'),('a','two'); CREATE TABLE unrelated(v TEXT)");
  const first=cut(path);db.exec("INSERT INTO unrelated VALUES('changed'); CREATE INDEX covering ON data(v,k); CREATE TABLE extra(id INTEGER)");
  expect(cut(path)).toEqual(first);db.close();const bytes=readFileSync(path);expect(cut(path)).toEqual(first);expect(readFileSync(path).equals(bytes)).toBe(true);
});
test('changed rows and changed selected schema each invalidate their exact cut',()=>{
  const {path,db}=setup();db.exec("CREATE TABLE data(v TEXT); INSERT INTO data VALUES('a')");const a=cut(path).digest;
  db.exec("UPDATE data SET v='b'");const b=cut(path).digest;expect(b).not.toBe(a);
  db.exec('ALTER TABLE data ADD COLUMN extra BLOB');expect(cut(path).digest).not.toBe(b);db.close();
});
test('uncheckpointed WAL and WITHOUT ROWID tables are read through native SQLite',()=>{
  const {path,db}=setup();db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE data(k TEXT PRIMARY KEY,v BLOB) WITHOUT ROWID; INSERT INTO data VALUES('b',X'01'),('a',X'02')");
  const a=cut(path);expect(a.rows).toBe(2);db.exec("INSERT INTO data VALUES('c',X'03')");expect(cut(path).rows).toBe(3);expect(cut(path).digest).not.toBe(a.digest);db.close();expect(cut(path).rows).toBe(3);
});
test('quoted names are identifiers and views virtual tables missing and symlinked files are refused',()=>{
  const {path,db}=setup();db.exec('CREATE TABLE "odd"";name"(v TEXT); INSERT INTO "odd"";name" VALUES(\'body must never appear in stdout\'); CREATE TABLE data(v); CREATE VIEW reflected AS SELECT * FROM data; CREATE VIRTUAL TABLE searched USING fts5(v)');
  expect(cut(path,'odd";name').rows).toBe(1);db.close();symlinkSync(path,join(temp,'link.db'));
  for(const [p,t]of [[path,'reflected'],[path,'searched'],[path,'searched_data'],[path,'missing'],[path,'data"; DROP TABLE data;--'],[join(temp,'link.db'),'data'],[join(temp,'absent.db'),'data'],[temp,'data']]){const r=run(p,t);expect(r.exitCode).not.toBe(0);expect(r.stdout.length).toBe(0);}
  expect(existsSync(join(temp,'absent.db'))).toBe(false);expect(cut(path).rows).toBe(0);
});
test('native scan remains bounded in output across twenty thousand rows and refuses partial read failure',()=>{
  const {path,db}=setup();db.exec("CREATE TABLE data(id INTEGER,v BLOB); WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<20000) INSERT INTO data SELECT x,zeroblob(128) FROM n");db.close();
  expect(cut(path).rows).toBe(20000);expect(run(path,'data').stdout.length).toBeLessThan(100);
  const shim=join(temp,'fault.c'),obj=join(temp,'cut.o'),bad=join(temp,'bad-hash');
  writeFileSync(shim,'#include <sqlite3.h>\nint bf_fault_step(sqlite3_stmt *q){static int calls; if(++calls==6)return SQLITE_IOERR;return sqlite3_step(q);}\n');
  for(const args of [['-Dsqlite3_step=bf_fault_step','-c',root+'/cmd/BonfyreHash/src/sqlite_cut.c','-o',obj],[root+'/cmd/BonfyreHash/src/main.c',shim,obj,root+'/lib/libbonfyre/libbonfyre.a','-lsqlite3','-o',bad]]){
    const r=Bun.spawnSync(['cc','-std=c11','-Wall','-Wextra','-Werror','-I'+root+'/lib/libbonfyre/include',...args]);if(r.exitCode)throw Error(r.stderr.toString());
  }
  const result=run(path,'data',bad);expect(result.exitCode).not.toBe(0);expect(result.stdout.length).toBe(0);
});
