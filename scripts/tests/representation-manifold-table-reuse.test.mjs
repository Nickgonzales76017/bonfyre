import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {mkdtempSync,realpathSync,readFileSync,writeFileSync,statSync,existsSync,rmSync,symlinkSync,unlinkSync,chmodSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {apply} from '../representation-manifold-shift.mjs';
import {registerVirtualGraph,queryVirtualGraph} from '../representation-manifold-forge.mjs';
const root=resolve(import.meta.dir,'../..'),hash=b=>createHash('sha256').update(b).digest('hex');
async function fixture(fn) {
  const dir=realpathSync(mkdtempSync(join(tmpdir(),'bf-table-reuse-'))),source=new Database(join(dir,'cms.db'));
  source.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE chosen(id INTEGER PRIMARY KEY,source TEXT,target TEXT); INSERT INTO chosen VALUES(1,'alpha','beta'); CREATE TABLE unrelated(id INTEGER PRIMARY KEY,note TEXT); INSERT INTO unrelated VALUES(1,'before');");
  const lower=(kinds=['cms.chosen'],extra={})=>apply(dir,join(dir,'absent-substrates'),{kinds,...extra});
  const inspect=sql=>{const db=new Database(join(dir,'representation-manifold.db'),{readonly:true});try{return db.query(sql).all();}finally{db.close();}};
  try {await fn({dir,source,lower,inspect});} finally {source.close();rmSync(dir,{recursive:true,force:true});}
}
const physical=path=>({digest:hash(readFileSync(path)),mtime:statSync(path).mtimeMs,ino:statSync(path).ino});
test('unrelated source changes reuse exact Parquet bytes frontier and transformation receipts',async()=>fixture(async({dir,source,lower,inspect})=>{
  const first=await lower(),row=first.manifest[0],bytes=physical(row.target);
  expect(first.demand.lowerings).toBe(1);expect(first.demand.reused).toBe(0);expect(row.frontier).toMatch(/^sqlite-table-sha256:[a-f0-9]{64};rows:1$/);
  const receipts=inspect('SELECT * FROM transformation_occurrence'),instances=inspect('SELECT * FROM representation_instance'),files=inspect('SELECT * FROM physical_realization');
  source.exec("UPDATE unrelated SET note='changed'; CREATE INDEX chosen_cover ON chosen(target,source)");
  const repeat=await lower();expect(repeat.demand).toMatchObject({materialized:1,lowerings:0,reused:1,held:0});
  expect(repeat.manifest[0].frontier).toBe(row.frontier);expect(physical(row.target)).toEqual(bytes);
  expect(inspect('SELECT * FROM transformation_occurrence')).toEqual(receipts);
  expect(inspect('SELECT * FROM representation_instance')).toEqual(instances);
  expect(inspect('SELECT * FROM physical_realization')).toEqual(files);
  expect(existsSync(join(dir,'representations/manifest.json'))).toBe(false);expect(existsSync(join(dir,'representation-portfolio.json'))).toBe(false);
}));
test('one changed demanded table rebuilds alone while the unchanged virtual graph remains usable',async()=>fixture(async({dir,source,lower,inspect})=>{
  const kinds=['cms.chosen','cms.unrelated'],first=await lower(kinds),chosen=first.manifest.find(r=>r.kind==='cms.chosen');
  const policy={schema:'fixture-directed-pairs.v1',directedPairs:[{source:'source',target:'target'}]};
  const plan=await registerVirtualGraph(dir,'cms.chosen',policy),virtual=inspect('SELECT * FROM virtual_graph_projection_v2');
  expect(plan.programs).toHaveLength(1);const original=physical(chosen.target);
  source.exec("UPDATE unrelated SET note='changed'");
  const next=await lower(kinds);expect(next.demand).toMatchObject({reused:1,lowerings:1,held:0});expect(physical(chosen.target)).toEqual(original);
  expect(inspect('SELECT * FROM virtual_graph_projection_v2')).toEqual(virtual);
  const query=await queryVirtualGraph(dir,'cms.chosen','source','target');expect(query.rows.map(r=>[r.src,r.dst])).toEqual([['alpha','beta']]);
  source.exec("UPDATE chosen SET target='gamma'");const changed=await lower(kinds);expect(changed.demand).toMatchObject({lowerings:1,reused:1});
  await expect(queryVirtualGraph(dir,'cms.chosen','source','target')).rejects.toThrow('frontier changed');
  await registerVirtualGraph(dir,'cms.chosen',policy);expect((await queryVirtualGraph(dir,'cms.chosen','source','target')).rows.map(r=>r.dst)).toEqual(['gamma']);
}));
test('identical table content in another source cannot borrow a materialization receipt',async()=>fixture(async({dir,lower,inspect})=>{
  const another=new Database(join(dir,'other.db'));another.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,source TEXT,target TEXT); INSERT INTO chosen VALUES(1,'alpha','beta')");another.close();
  await lower();const second=await lower(['other.chosen']);expect(second.demand).toMatchObject({lowerings:1,reused:0});
  const rows=inspect("SELECT input_ref,output_ref,input_digest,compiler FROM transformation_occurrence WHERE transform='sqlite-table->parquet' ORDER BY input_ref");
  expect(rows).toHaveLength(2);expect(rows[0].input_digest).toBe(rows[1].input_digest);expect(rows[0].input_ref).not.toBe(rows[1].input_ref);expect(rows[0].output_ref).not.toBe(rows[1].output_ref);
}));
test('a different real native compiler invalidates reuse even when source and output are identical',async()=>fixture(async({dir,lower,inspect})=>{
  const first=await lower(),binary=join(dir,'different-hash');
  const build=Bun.spawnSync(['cc','-O0','-std=c11','-I'+root+'/lib/libbonfyre/include',root+'/cmd/BonfyreHash/src/main.c',root+'/cmd/BonfyreHash/src/sqlite_cut.c',root+'/lib/libbonfyre/libbonfyre.a','-lsqlite3','-o',binary],{stdout:'pipe',stderr:'pipe'});
  if(build.exitCode)throw Error(build.stderr.toString());
  const second=await lower(['cms.chosen'],{hashBinary:binary});expect(second.demand.lowerings).toBe(1);expect(second.manifest[0].digest).toBe(first.manifest[0].digest);expect(second.manifest[0].compiler).not.toBe(first.manifest[0].compiler);
  expect(inspect("SELECT DISTINCT compiler FROM transformation_occurrence WHERE transform='sqlite-table->parquet'")).toHaveLength(2);
  expect((await lower(['cms.chosen'],{hashBinary:binary})).demand.reused).toBe(1);
}));
test('an invalid compiler or output receipt never attests reuse',async()=>fixture(async({dir,lower})=>{
  const first=await lower(),file=first.manifest[0].target;
  const db=new Database(join(dir,'representation-manifold.db'));
  db.exec("UPDATE transformation_occurrence SET compiler='unrelated-compiler'");
  expect((await lower()).demand.lowerings).toBe(1);
  db.exec("UPDATE physical_realization SET digest='unrelated-output'");
  expect((await lower()).demand.lowerings).toBe(1);db.close();
  const saved=readFileSync(file),corrupt=Buffer.from(saved);corrupt[corrupt.length-1]^=1;writeFileSync(file,corrupt);
  const rebuilt=await lower();expect(rebuilt.demand.lowerings).toBe(1);expect(hash(readFileSync(file))).toBe(hash(saved));expect(rebuilt.demand.reused).toBe(0);
}));
test('missing native owner fails before touching a new manifold and preserves an existing cut',async()=>fixture(async({dir,source,lower})=>{
  await expect(lower(['cms.chosen'],{hashBinary:join(dir,'missing')})).rejects.toThrow('hash owner unavailable');
  expect(existsSync(join(dir,'representation-manifold.db'))).toBe(false);
  const first=await lower(),before=physical(first.manifest[0].target);source.exec("UPDATE chosen SET target='later'");
  await expect(lower(['cms.chosen'],{hashBinary:join(dir,'missing')})).rejects.toThrow();expect(physical(first.manifest[0].target)).toEqual(before);
}));
test('selected schema changes and WAL inserts invalidate while unsafe output links remain refused',async()=>fixture(async({dir,source,lower})=>{
  const first=await lower();source.exec('ALTER TABLE chosen ADD COLUMN extra INTEGER');
  const schema=await lower();expect(schema.demand.lowerings).toBe(1);expect(schema.manifest[0].frontier).not.toBe(first.manifest[0].frontier);
  source.exec("INSERT INTO chosen VALUES(2,'second','target',7)");const inserted=await lower();expect(inserted.manifest[0].frontier).toContain(';rows:2');
  const file=inserted.manifest[0].target,external=join(dir,'unrelated-material');writeFileSync(external,'do not overwrite');unlinkSync(file);symlinkSync(external,file);
  const refused=await lower();expect(refused.demand.held).toBe(1);expect(refused.demand.lowerings).toBe(0);expect(readFileSync(external,'utf8')).toBe('do not overwrite');
}));
test('staged runtime uses the same native table-cut owner and installed CLI reuses its own exact cut',async()=>fixture(async({dir})=>{
  const stage=join(dir,'stage'),install=Bun.spawnSync(['make','-C',root+'/scripts','install-representation-manifold','MEMBRANE_ROOT='+stage],{stdout:'pipe',stderr:'pipe'});
  if(install.exitCode)throw Error(install.stderr.toString());
  expect(existsSync(join(stage,'cmd/BonfyreHash/bonfyre-hash'))).toBe(true);
  const args=[process.execPath,join(stage,'scripts/representation-manifold-shift.mjs'),'--estate',dir,'--apply','--kind','cms.chosen','--json'];
  // An absent override, rather than an empty invalid owner, selects the staged binary.
  const env={...process.env};delete env.BONFYRE_HASH_BINARY;
  const cli=()=>{const r=Bun.spawnSync(args,{cwd:stage,stdout:'pipe',stderr:'pipe',env});if(r.exitCode)throw Error(r.stderr.toString());return JSON.parse(r.stdout.toString());};
  expect(cli().demand.lowerings).toBe(1);expect(cli().demand).toMatchObject({lowerings:0,reused:1});
}));
test('zero-row cuts remain reusable and no virtual-table result masquerades as a native cut',async()=>fixture(async({source,lower})=>{
  source.exec('DELETE FROM chosen');expect((await lower()).manifest[0].frontier).toContain(';rows:0');expect((await lower()).demand.reused).toBe(1);
  source.exec('CREATE VIRTUAL TABLE search_docs USING fts5(content)');const virtual=await lower(['cms.search_docs']);expect(virtual.demand.materialized).toBe(0);expect(virtual.demand.held).toBe(1);
}));
test('source-path replacement after snapshot refuses stale publication and preserves the prior artifact',async()=>fixture(async({dir,source,lower})=>{
  source.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
  const first=await lower(),before=physical(first.manifest[0].target),replacement=join(dir,'replacement.tmp');
  const next=new Database(replacement);next.exec("CREATE TABLE chosen(id INTEGER PRIMARY KEY,source TEXT,target TEXT); INSERT INTO chosen VALUES(1,'new','owner')");next.close();
  const injected=join(dir,'replace-during-hash'),quote=s=>"'"+s.replaceAll("'","'\\''")+"'";
  // Failure injection surrounds the real hasher; only disposable fixture paths move.
  writeFileSync(injected,'#!/bin/sh\n'+quote(root+'/cmd/BonfyreHash/bonfyre-hash')+' "$@" || exit $?\n'+
    'mv '+quote(join(dir,'cms.db'))+' '+quote(join(dir,'prior-source.tmp'))+'\n'+
    'mv '+quote(replacement)+' '+quote(join(dir,'cms.db'))+'\n');chmodSync(injected,0o700);
  const refused=await lower(['cms.chosen'],{hashBinary:injected});expect(refused.demand).toMatchObject({materialized:0,lowerings:0,reused:0,held:1});
  expect(refused.manifest[0].error).toContain('source changed');expect(physical(first.manifest[0].target)).toEqual(before);
}));

// These witnesses execute the actual native Hash, DuckDB and Forge owners in
// isolated estates. They do not claim a live-estate rollout or access grants.
