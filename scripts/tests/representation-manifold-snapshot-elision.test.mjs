import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,realpathSync,statSync,readFileSync,writeFileSync,rmSync,chmodSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {apply,verifyDemand} from '../representation-manifold-shift.mjs';
import {runNativeFabric,verifyRegisteredFabric} from '../representation-manifold-fabric.mjs';
const root=resolve(import.meta.dir,'../..');
const digest=path=>createHash('sha256').update(readFileSync(path)).digest('hex');
const quote=s=>"'"+s.replaceAll("'","'\\''")+"'";
async function fixture(run) {
  const dir=realpathSync(mkdtempSync(join(tmpdir(),'bf-snapshot-elision-'))),source=new Database(join(dir,'cms.db'));
  source.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE chosen(id INTEGER PRIMARY KEY,source TEXT,target TEXT); INSERT INTO chosen VALUES(1,'alpha','beta'); CREATE TABLE internet_source(source TEXT); INSERT INTO internet_source VALUES('seed-preserved'); CREATE TABLE unrelated(body BLOB); INSERT INTO unrelated VALUES(zeroblob(1048576))");
  const original=Database.prototype.exec,statements=[];
  Database.prototype.exec=function(sql,...args){if(typeof sql==='string'&&/^VACUUM INTO/.test(sql))statements.push(sql);return original.call(this,sql,...args);};
  const lower=(kinds=['cms.chosen'],extra={})=>apply(dir,join(dir,'no-substrates'),{kinds,...extra});
  const read=sql=>{const db=new Database(join(dir,'representation-manifold.db'),{readonly:true});try{return db.query(sql).all();}finally{db.close();}};
  const inject=(body,path=join(dir,'cms.db'))=>{const flag=join(dir,'inject-once'),binary=join(dir,'hash-injection');writeFileSync(binary,'#!/bin/sh\n'+quote(join(root,'cmd/BonfyreHash/bonfyre-hash'))+' "$@" || exit $?\nif [ "$2" = '+quote(path)+' ] && [ -f '+quote(flag)+' ]; then\nrm '+quote(flag)+'\n'+body+'\nfi\n');chmodSync(binary,0o700);return {hashBinary:binary,arm:()=>writeFileSync(flag,'test')};};
  try{await run({dir,source,lower,read,statements,inject});}finally{Database.prototype.exec=original;source.close();rmSync(dir,{recursive:true,force:true});}
}
test('warm demand performs no VACUUM and retains seed metadata plus all physical records',async()=>fixture(async({dir,lower,read,statements})=>{
  const kinds=['cms.chosen','cms.internet_source'],first=await lower(kinds),file=first.manifest.find(r=>r.kind==='cms.chosen').target;
  expect(first.demand.source_snapshots).toBe(1);expect(first.demand.source_snapshot_bytes).toBeGreaterThan(1048576);
  const before={hash:digest(file),mtime:statSync(file).mtimeMs,ino:statSync(file).ino},records=read('SELECT * FROM representation_instance ORDER BY id'),proofs=read('SELECT * FROM transformation_occurrence ORDER BY id');statements.length=0;
  const warm=await lower(kinds);expect(warm.demand).toMatchObject({source_snapshots:0,source_snapshot_bytes:0,source_snapshots_elided:1,reused:2,lowerings:0,held:0});expect(statements).toEqual([]);
  expect({hash:digest(file),mtime:statSync(file).mtimeMs,ino:statSync(file).ino}).toEqual(before);expect(read('SELECT * FROM representation_instance ORDER BY id')).toEqual(records);expect(read('SELECT * FROM transformation_occurrence ORDER BY id')).toEqual(proofs);
  expect(read('SELECT id FROM semantic_object')).toEqual([{id:'internet_source:seed-preserved'}]);expect(existsSync(join(dir,'representations/manifest.json'))).toBe(false);
}));
test('unselected same-database content and covering-index changes still require no source snapshot',async()=>fixture(async({source,lower,statements})=>{
  await lower();source.exec("INSERT INTO unrelated VALUES(zeroblob(1048576)); CREATE INDEX covering ON chosen(target,source)");statements.length=0;
  expect((await lower()).demand).toMatchObject({source_snapshots:0,reused:1,lowerings:0});expect(statements).toHaveLength(0);
}));
test('one changed owner is snapshotted once while another owner remains snapshot-free',async()=>fixture(async({dir,source,lower,statements})=>{
  const other=new Database(join(dir,'other.db'));other.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO chosen VALUES(1,'other owner')");other.close();
  const kinds=['cms.chosen','cms.internet_source','other.chosen'];expect((await lower(kinds)).demand.source_snapshots).toBe(2);
  source.exec("UPDATE chosen SET target='changed'");statements.length=0;const next=await lower(kinds);
  expect(next.demand).toMatchObject({source_snapshots:1,source_snapshots_elided:1,lowerings:1,reused:2,held:0});expect(statements).toHaveLength(1);expect(statements[0]).toContain('/cms.db');
}));
test('uncheckpointed selected WAL changes take a new snapshot and preserve both rows',async()=>fixture(async({source,lower,statements})=>{
  await lower();source.exec("INSERT INTO chosen VALUES(2,'new','from-wal')");statements.length=0;const next=await lower();
  expect(next.demand).toMatchObject({source_snapshots:1,source_snapshots_elided:0,lowerings:1,reused:0});expect(statements).toHaveLength(1);expect(next.manifest[0].frontier).toContain(';rows:2');
  statements.length=0;expect((await lower()).demand.source_snapshots).toBe(0);expect(statements).toHaveLength(0);
}));
test('warm reuse works without creating a temporary snapshot directory',async()=>fixture(async({dir,lower,statements})=>{
  await lower();const old=process.env.TMPDIR;process.env.TMPDIR=join(dir,'missing-scratch-parent');statements.length=0;
  try{expect((await lower()).demand).toMatchObject({source_snapshots:0,reused:1,held:0});expect(statements).toHaveLength(0);expect(existsSync(process.env.TMPDIR)).toBe(false);}finally{if(old===undefined)delete process.env.TMPDIR;else process.env.TMPDIR=old;}
}));
test('missing or corrupt output bodies cannot use the snapshot-free path',async()=>fixture(async({lower,statements})=>{
  const first=await lower(),file=first.manifest[0].target,expected=digest(file);rmSync(file);statements.length=0;
  expect((await lower()).demand).toMatchObject({source_snapshots:1,lowerings:1,reused:0});expect(statements).toHaveLength(1);expect(digest(file)).toBe(expected);
  writeFileSync(file,'not a parquet body');statements.length=0;expect((await lower()).demand.source_snapshots).toBe(1);expect(statements).toHaveLength(1);expect(digest(file)).toBe(expected);
}));
test('a committed source change during native warm hashing is held without publishing its stale result',async()=>fixture(async({dir,source,lower,statements,inject})=>{
  const fault=inject('/usr/bin/sqlite3 '+quote(join(dir,'cms.db'))+' '+quote("UPDATE chosen SET target='concurrent'"));
  const first=await lower(['cms.chosen'],fault),file=first.manifest[0].target,before=digest(file);fault.arm();statements.length=0;
  const refused=await lower(['cms.chosen'],fault);expect(refused.demand).toMatchObject({materialized:0,reused:0,lowerings:0,held:1,source_snapshots:0});expect(refused.manifest[0].error).toContain('source changed');expect(statements).toHaveLength(0);expect(digest(file)).toBe(before);
  expect((await lower(['cms.chosen'],fault)).demand).toMatchObject({lowerings:1,source_snapshots:1,held:0});expect(source.query('SELECT target FROM chosen').get().target).toBe('concurrent');
}));
test('a database replaced during warm hashing is held while the old Parquet remains intact',async()=>fixture(async({dir,source,lower,inject})=>{
  source.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');const replacement=join(dir,'new-source.tmp'),next=new Database(replacement);next.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,source TEXT,target TEXT); INSERT INTO chosen VALUES(1,'replacement','new')");next.close();
  const fault=inject('mv '+quote(join(dir,'cms.db'))+' '+quote(join(dir,'old-source.tmp'))+'\nmv '+quote(replacement)+' '+quote(join(dir,'cms.db')));
  const first=await lower(['cms.chosen'],fault),file=first.manifest[0].target,before=digest(file);fault.arm();const held=await lower(['cms.chosen'],fault);
  expect(held.demand).toMatchObject({held:1,reused:0,source_snapshots:0});expect(held.manifest[0].error).toContain('source changed');expect(digest(file)).toBe(before);
}));
test('later owner verification cannot hide an earlier reused artifact being modified',async()=>fixture(async({dir,lower,inject})=>{
  const otherPath=join(dir,'other.db'),other=new Database(otherPath);other.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO chosen VALUES(1,'other')");other.close();const firstPath=join(dir,'representations/parquet/cms/chosen.parquet');
  const fault=inject('printf corrupted >> '+quote(firstPath),otherPath),kinds=['cms.chosen','other.chosen'];await lower(kinds,fault);fault.arm();const held=await lower(kinds,fault);
  expect(held.demand).toMatchObject({held:1,reused:1,source_snapshots:0});expect(held.manifest.find(r=>r.kind==='cms.chosen').state).toBe('pending');expect(held.manifest.find(r=>r.kind==='other.chosen').state).toBe('physical');
}));
test('later owner verification cannot hide withdrawal of an earlier materialization receipt',async()=>fixture(async({dir,lower,inject})=>{
  const otherPath=join(dir,'other.db'),other=new Database(otherPath);other.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO chosen VALUES(1,'other')");other.close();
  const fault=inject('/usr/bin/sqlite3 '+quote(join(dir,'representation-manifold.db'))+' '+quote("UPDATE transformation_occurrence SET proof_state='revoked' WHERE input_ref LIKE '%/cms.db#chosen'"),otherPath),kinds=['cms.chosen','other.chosen'];await lower(kinds,fault);fault.arm();
  const held=await lower(kinds,fault);expect(held.demand).toMatchObject({held:1,reused:1,source_snapshots:0});expect(held.manifest.find(r=>r.kind==='cms.chosen').state).toBe('pending');
}));
test('full compilation preserves its historical source-snapshot behavior',async()=>fixture(async({dir,statements})=>{
  const first=await apply(dir,join(dir,'no-substrates'));expect(first.manifest).toHaveLength(3);statements.length=0;
  const repeat=await apply(dir,join(dir,'no-substrates'));expect(statements).toHaveLength(1);expect(repeat.manifest).toHaveLength(3);expect(existsSync(join(dir,'representations/manifest.json'))).toBe(true);
}));
test('read-only demand verification reports reusable or snapshot-required without changing owner records',async()=>fixture(async({dir,source,lower,read,statements})=>{
  await lower();const ledger=join(dir,'representation-manifold.db'),settle=new Database(ledger);
  // Finish the fixture writer's WAL checkpoint before measuring physical bytes.
  settle.exec('PRAGMA wal_checkpoint(TRUNCATE)');settle.close();const before=digest(ledger),records=read('SELECT * FROM manifold_meta ORDER BY k');statements.length=0;
  const reusable=await verifyDemand(dir,['cms.chosen']);expect(reusable.state).toBe('REUSABLE');expect(reusable.read_only).toBe(true);expect(reusable.demand.reused).toBe(1);expect(digest(ledger)).toBe(before);expect(read('SELECT * FROM manifold_meta ORDER BY k')).toEqual(records);expect(statements).toHaveLength(0);
  source.exec("UPDATE chosen SET target='new-revision'");const stale=await verifyDemand(dir,['cms.chosen']);expect(stale.state).toBe('SNAPSHOT_REQUIRED');expect(stale.demand.snapshot_required).toBe(1);expect(stale.demand.lowerings).toBe(0);expect(digest(ledger)).toBe(before);expect(statements).toHaveLength(0);
}));
test('missing source binding forces ordinary source verification and is never silently adopted',async()=>fixture(async({dir,lower,read,statements})=>{
  await lower();const db=new Database(join(dir,'representation-manifold.db'));db.exec('DELETE FROM source_binding');db.exec('PRAGMA wal_checkpoint(TRUNCATE)');db.close();const before=digest(join(dir,'representation-manifold.db'));statements.length=0;
  expect((await verifyDemand(dir,['cms.chosen'])).state).toBe('SNAPSHOT_REQUIRED');expect(digest(join(dir,'representation-manifold.db'))).toBe(before);expect(read('SELECT * FROM source_binding')).toHaveLength(0);expect(statements).toHaveLength(0);
  expect((await lower()).demand).toMatchObject({source_snapshots:1,held:0});expect(read('SELECT * FROM source_binding')).toHaveLength(1);
}));
test('source mutation during a snapshot-free seed read rolls back the tentative seed',async()=>fixture(async({dir,source,lower,read})=>{
  await lower(['cms.internet_source']);const db=new Database(join(dir,'representation-manifold.db'));db.exec('DELETE FROM semantic_object');db.close();
  const original=Database.prototype.query;let fired=false;
  Database.prototype.query=function(sql,...args){const q=original.call(this,sql,...args);if(sql==='SELECT "source" FROM "internet_source"')return {iterate:function*(...values){yield* q.iterate(...values);if(!fired){fired=true;source.exec("INSERT INTO internet_source VALUES('concurrent-seed')");}}};return q;};
  try{await expect(lower(['cms.internet_source'])).rejects.toThrow('source changed during no-copy seed read');}finally{Database.prototype.query=original;}
  expect(fired).toBe(true);expect(read('SELECT * FROM semantic_object')).toHaveLength(0);
}));
test('staged CLI verifies without JSON and reports unavailable reuse through a nonzero exit',async()=>fixture(async({dir,source,lower,statements})=>{
  const stage=join(dir,'stage'),install=Bun.spawnSync(['make','-C',join(root,'scripts'),'install-representation-manifold','MEMBRANE_ROOT='+stage],{stdout:'pipe',stderr:'pipe'});if(install.exitCode)throw Error(install.stderr.toString());
  const env={...process.env};delete env.BONFYRE_HASH_BINARY;const cli=(...args)=>Bun.spawnSync([process.execPath,join(stage,'scripts/representation-manifold-shift.mjs'),'--estate',dir,'--kind','cms.chosen',...args],{cwd:stage,env,stdout:'pipe',stderr:'pipe',timeout:15000});
  expect(cli('--apply').exitCode).toBe(0);const second=cli('--apply');expect(second.exitCode).toBe(0);expect(second.stdout.toString()).toContain('snapshots 0 created; 0 bytes; 1 avoided');
  const before=digest(join(dir,'representation-manifold.db')),verified=cli('--verify');expect(verified.exitCode).toBe(0);expect(verified.stdout.toString()).toContain('state REUSABLE');expect(verified.stdout.toString()).not.toContain('"manifest"');expect(digest(join(dir,'representation-manifold.db'))).toBe(before);
  source.exec("UPDATE chosen SET target='changed-again'");const stale=cli('--verify');expect(stale.exitCode).toBe(3);expect(stale.stdout.toString()).toContain('state SNAPSHOT_REQUIRED');expect(digest(join(dir,'representation-manifold.db'))).toBe(before);
  const invalid=cli('--verify','--apply');expect(invalid.exitCode).not.toBe(0);expect(invalid.stderr.toString()).toContain('cannot mutate or bind');expect(digest(join(dir,'representation-manifold.db'))).toBe(before);
}));
test('verification refuses missing or self-referential stores without creating materialization state',async()=>fixture(async({dir,lower})=>{
  await expect(verifyDemand(dir,['cms.chosen'])).rejects.toThrow('missing or unsafe manifold');expect(existsSync(join(dir,'representation-manifold.db'))).toBe(false);
  await expect(lower(['representation-manifold.representation_instance'])).rejects.toThrow('cannot snapshot itself');expect(existsSync(join(dir,'representation-manifold.db'))).toBe(false);
}));
(process.env.ARROW_RUNTIME_BUNDLE?test:test.skip)('snapshot-free reuse preserves native Arrow IPC bytes and binding while selected change refuses the old catalog',async()=>fixture(async({dir,source,lower,read})=>{
  const bundle=resolve(process.env.ARROW_RUNTIME_BUNDLE);await lower();
  const binding=await runNativeFabric(['bind','--bundle',bundle,'--estate',dir]);
  const stream=()=>Bun.spawnSync([process.execPath,join(root,'scripts/representation-manifold-fabric.mjs'),'stream','--bundle',bundle,'--estate',dir,'--catalog',binding.catalog,'--kind','cms.chosen'],{cwd:root,stdout:'pipe',stderr:'pipe',timeout:15000});
  const first=stream();expect(first.exitCode).toBe(0);expect(first.stdout.length).toBeGreaterThan(100);expect(first.stdout[0]).toBe(255);
  const catalog=digest(binding.catalog),transports=read('SELECT * FROM transport_binding ORDER BY transport'),executions=read('SELECT * FROM execution_binding ORDER BY engine');
  source.exec("INSERT INTO unrelated VALUES(zeroblob(1048576))");expect((await lower()).demand).toMatchObject({source_snapshots:0,reused:1,lowerings:0});
  await verifyRegisteredFabric(dir,bundle,binding.catalog,'cms.chosen');const second=stream();expect(second.exitCode).toBe(0);expect(Buffer.from(second.stdout).equals(Buffer.from(first.stdout))).toBe(true);
  expect(digest(binding.catalog)).toBe(catalog);expect(read('SELECT * FROM transport_binding ORDER BY transport')).toEqual(transports);expect(read('SELECT * FROM execution_binding ORDER BY engine')).toEqual(executions);
  source.exec("UPDATE chosen SET target='new-stream-source'");expect((await lower()).demand).toMatchObject({source_snapshots:1,lowerings:1});
  await expect(verifyRegisteredFabric(dir,bundle,binding.catalog,'cms.chosen')).rejects.toThrow('frontier changed');
  const stale=stream();expect(stale.exitCode).not.toBe(0);expect(stale.stdout.length).toBe(0);
}));
