import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,existsSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';

const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_INGEST_BINARY||resolve(import.meta.dir,'../bonfyre-ingest');
const temp=mkdtempSync(join(tmpdir(),'bonfyre-ingest-hash-'));
const injected=join(temp,'ingest-fault');
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
beforeAll(()=>{
  const build=args=>{const r=Bun.spawnSync(['cc','-O2','-Wall','-Wextra','-std=c11','-Ilib/libbonfyre/include',...args],{cwd:root});if(r.exitCode)throw Error(r.stderr.toString());};
  build(['-Dfopen=bf_test_fopen','-Dfclose=bf_test_fclose','-c','lib/libbonfyre/src/bf_sha256.c','-o',join(temp,'sha.o')]);
  build(['-DBF_SHA256_FAULT_IO_ONLY','-c','lib/libbonfyre/tests/sha256_file_test.c','-o',join(temp,'fault.o')]);
  build(['cmd/BonfyreIngest/src/main.c',join(temp,'sha.o'),join(temp,'fault.o'),'lib/libbonfyre/libbonfyre.a','-o',injected]);
});
afterAll(()=>rmSync(temp,{recursive:true,force:true}));

test('text, table and raw intake retain independently verified source and normalized digests',()=>{
  for(const [name,bytes] of [['source.txt',Buffer.from('Alpha\r\nBeta\r\n')],['source.csv',Buffer.from('name,quantity\r\nA,2\r\n')],['source.bin',Buffer.alloc(8193,0xa5)]]){
    const input=join(temp,name),output=join(temp,name+'-output');writeFileSync(input,bytes);
    const result=Bun.spawnSync([binary,input,output]);
    expect(result.exitCode,result.stderr.toString()).toBe(0);
    const manifest=JSON.parse(readFileSync(join(output,'intake-manifest.json'),'utf8'));
    const normalized=readFileSync(join(output,manifest.normalized_path));
    expect(manifest.status).toBe('ready');
    expect(manifest.source_content_hash).toBe(digest(bytes));
    expect(manifest.normalized_content_hash).toBe(digest(normalized));
    expect(manifest.content_hash).toBe(digest(normalized));
    expect(manifest.normalized_bytes).toBe(normalized.length);
    expect(readFileSync(join(output,'fabric-output.manifest'),'utf8')).toContain(manifest.normalized_path+'\n');
  }
});
const refused=(result,output)=>{
  expect(result.exitCode).not.toBe(0);
  expect(result.stdout.toString()).toBe('');
  expect(result.stderr.toString()).toContain('cannot hash');
  for(const name of ['intake-manifest.json','artifact.json','fabric-output.manifest'])expect(existsSync(join(output,name))).toBe(false);
};
test('absent and directory inputs cannot publish a ready intake',()=>{
  for(const [index,input] of [join(temp,'absent.txt'),temp].entries()){
    const output=join(temp,`refusal-${index}`);
    refused(Bun.spawnSync([binary,input,output]),output);
  }
});
test('partial source or normalized hash failure cannot publish ready manifests',()=>{
  const input=join(temp,'fault.txt');writeFileSync(input,'abc and further content\n');
  for(const stage of ['source','normalized']){
    const output=join(temp,`fault-${stage}`);
    const failed=stage==='source'?input:join(output,'normalized.txt');
    const result=Bun.spawnSync([injected,input,output],{env:{...process.env,BF_TEST_SHA256_FAIL:failed}});
    refused(result,output);
    expect(result.stderr.toString()).toContain(stage);
  }
});
