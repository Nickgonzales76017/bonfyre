import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';

const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_HASH_BINARY||resolve(import.meta.dir,'../bonfyre-hash');
const temp=mkdtempSync(join(tmpdir(),'bonfyre-hash-file-'));
const injected=join(temp,'hash-fault');
const portable=join(temp,'hash-portable');
const run=(args,env={})=>Bun.spawnSync([binary,...args],{env:{...process.env,...env}});
const checkBuild=args=>{const result=Bun.spawnSync(['cc','-O2','-Wall','-Wextra','-Werror','-std=c11',...args],{cwd:root});if(result.exitCode)throw Error(result.stderr.toString());};
beforeAll(()=>{
  const includes=['-Ilib/libbonfyre/include'];
  checkBuild([...includes,'-Dfopen=bf_test_fopen','-Dfclose=bf_test_fclose','-c','lib/libbonfyre/src/bf_sha256.c','-o',join(temp,'sha.o')]);
  checkBuild([...includes,'-DBF_SHA256_FAULT_IO_ONLY','-c','lib/libbonfyre/tests/sha256_file_test.c','-o',join(temp,'fault.o')]);
  checkBuild([...includes,'cmd/BonfyreHash/src/main.c','cmd/BonfyreHash/src/sqlite_cut.c',join(temp,'sha.o'),join(temp,'fault.o'),'lib/libbonfyre/libbonfyre.a','-lsqlite3','-o',injected]);
  checkBuild([...includes,'-DBONFYRE_SHA256_PORTABLE','-c','lib/libbonfyre/src/bf_sha256.c','-o',join(temp,'portable.o')]);
  checkBuild([...includes,'cmd/BonfyreHash/src/main.c','cmd/BonfyreHash/src/sqlite_cut.c',join(temp,'portable.o'),'lib/libbonfyre/libbonfyre.a','-lsqlite3','-o',portable]);
});
afterAll(()=>rmSync(temp,{recursive:true,force:true}));

test('file digest matches an independent implementation across buffer boundaries',()=>{
  for(const size of [0,3,55,56,63,64,65,8191,8192,8193,1024*1024+17]){
    const bytes=Buffer.alloc(size);for(let i=0;i<size;i++)bytes[i]=i%251;
    const path=join(temp,`input-${size}`);writeFileSync(path,bytes);
    const result=run(['file',path]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout.toString()).toBe(`${createHash('sha256').update(bytes).digest('hex')}  ${path}\n`);
    const fallback=Bun.spawnSync([portable,'file',path]);
    expect(fallback.exitCode).toBe(0);
    expect(fallback.stdout.toString()).toBe(result.stdout.toString());
  }
});
test('missing inputs and directory reads emit no digest',()=>{
  for(const path of [join(temp,'missing'),temp]){
    const result=run(['file',path]);
    expect(result.exitCode).not.toBe(0);
    expect(result.stdout.toString()).toBe('');
    expect(result.stderr.toString()).toMatch(/Cannot (hash|open)/);
  }
});
test('duplicate detection still reports equal complete files',()=>{
  const dir=mkdtempSync(join(temp,'duplicates-'));
  writeFileSync(join(dir,'one'),'abc');writeFileSync(join(dir,'two'),'abc');writeFileSync(join(dir,'three'),'different');
  const result=run(['dedup',dir]);
  expect(result.exitCode).toBe(0);
  expect(result.stdout.toString()).toContain('1 duplicate pairs, 3 bytes reclaimable.');
});
test('a partial read error refuses both a file digest and the entire duplicate report',()=>{
  const dir=mkdtempSync(join(temp,'partial-')),path=join(dir,'broken');
  writeFileSync(path,'abc plus bytes the faulty stream cannot read');writeFileSync(join(dir,'complete'),'abc');
  for(const args of [['file',path],['dedup',dir]]){
    const result=Bun.spawnSync([injected,...args],{env:{...process.env,BF_TEST_SHA256_FAIL:path}});
    expect(result.exitCode).not.toBe(0);
    expect(result.stdout.toString()).toBe('');
    expect(result.stderr.toString()).toContain('Cannot hash');
  }
});
