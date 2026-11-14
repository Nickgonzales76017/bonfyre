import {test,expect} from 'bun:test';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {performance} from 'node:perf_hooks';
import {compileNativeDensePack} from '../../../integrations/estate/dense_pack_bridge.mjs';
import {DenseMemoryReader,memoryManifestFromNative} from '../../../integrations/estate/dense_pack_memory_reader.mjs';
const binary=resolve(import.meta.dir,'../bonfyre-compress');
const ledger=resolve(import.meta.dir,'../../BonfyreProviderEstate/providers.tsv');
const wire=b=>'\\x'+Buffer.from(b).toString('hex');
async function fixture(fn){
 const tmp=mkdtempSync(join(tmpdir(),'bf-memory-'));
 try{
  const providerRows=readFileSync(ledger,'utf8').trim().split('\n').slice(1);
  const rows=providerRows.flatMap(line=>{
   const c=line.split('\t');
   return Array.from({length:4},(_,i)=>({id:'provider-'+c[1]+'-binding-'+i,
    title:c[2],provider:c[2],slug:c[1],family:c[3],tenancy:c[7],evidence:c[9],
    origin:'BonfyreProviderEstate:providers.tsv',synthetic_binding:i}));
  });
  const source=join(tmp,'source.jsonl'),factor=join(tmp,'factor.db');
  writeFileSync(source,rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
  const start=performance.now();
  execFileSync(binary,['factor-records',source,factor],{stdio:'pipe'});
  const compiled=await compileNativeDensePack({factorPath:factor,sourcePath:source,
   compressBinary:binary,verifiedClosure:'user:19',maxRecords:1000});
  await fn({rows,source,factor,compiled,providerCount:providerRows.length,compileMs:performance.now()-start});
 }finally{rmSync(tmp,{recursive:true,force:true})}
}
function remote(compiled,{down=false,corrupt=false}={}){
 const ns=wire(Buffer.from(compiled.namespace_digest,'hex'));
 const root=wire(Buffer.from(compiled.root_digest,'hex'));
 const atoms=new Map(compiled.atoms.map(a=>[a.atom_digest,a]));
 const counts={windows:0,directories:0,atoms:0};
 const request=async(url,options)=>{
  if(down)throw Error('remote offline');
  expect(options.headers.Authorization).toBe('Bearer '+('k'.repeat(40)));
  const a=JSON.parse(options.body),matches=a.p_namespace===ns&&a.p_root===root;
  const items=matches?compiled.pages.filter(p=>p.bucket>a.p_after_bucket).slice(0,a.p_max_pages):[];
  if(url.endsWith('/rpc/dense_pack_memory_directory')){
   counts.directories++;
   return{ok:true,json:async()=>items.map(p=>({bucket:p.bucket,record_count:p.record_count,
    codec:p.codec,raw_bytes:p.raw_bytes,stored_bytes:p.payload.length,
    page_digest:wire(Buffer.from(p.page_digest,'hex')),encoded_digest:wire(Buffer.from(p.encoded_digest,'hex'))}))};
  }
  if(url.endsWith('/rpc/dense_pack_memory_window')){
   counts.windows++;let total=0;
   return{ok:true,json:async()=>items.filter(p=>{total+=p.payload.length;return total<=a.p_budget_bytes}).map(p=>{
    const payload=Buffer.from(p.payload);if(corrupt)payload[payload.length-1]^=1;
    return{bucket:p.bucket,record_count:p.record_count,codec:p.codec,
     raw_bytes:p.raw_bytes,stored_bytes:payload.length,payload:wire(payload),
     page_digest:wire(Buffer.from(p.page_digest,'hex')),
     encoded_digest:wire(Buffer.from(p.encoded_digest,'hex'))};
   })};
  }
  if(url.endsWith('/rpc/dense_pack_atom_slice')){
   counts.atoms++;
   return{ok:true,json:async()=>a.p_digests.map(id=>{
    const atom=atoms.get(id.slice(2));
    return{atom_digest:id,raw_bytes:atom.raw_bytes,codec:atom.codec,
     dictionary_digest:null,payload:wire(atom.payload)};
   })};
  }
  throw Error('unrecognized RPC');
 };
 return{counts,request};
}
test('native-backed provider catalogue: directory, windows, cached hydration and metrics',async()=>{
 await fixture(async({rows,compiled,providerCount,compileMs})=>{
  expect(providerCount).toBeGreaterThan(100);
  expect(compiled.pages.length).toBeGreaterThan(1);
  const mock=remote(compiled);
  const reader=new DenseMemoryReader({nativeClosure:'user:19',
   manifest:memoryManifestFromNative(compiled),baseUrl:'https://fixture.supabase.co',
   serviceKey:'k'.repeat(40),request:mock.request});
  const directory=await reader.directory({maxPages:2});
  expect(directory.pages).toHaveLength(2);
  expect(directory.pages.every(x=>!('payload' in x))).toBe(true);
  const first=await reader.prefetch({maxPages:2});
  expect(first.pages).toHaveLength(2);
  expect((await reader.prefetch({afterBucket:first.next_bucket,maxPages:2})).pages.length).toBeGreaterThan(0);
  const got=await reader.readRecord({recordKey:rows[0].id});
  expect(got.residency).toBe('supabase');
  expect(got.candidate_only).toBe(true);
  expect(got.bytes).toEqual(Buffer.from(JSON.stringify(rows[0])+'\n'));
  const before=mock.counts.windows;
  expect((await reader.readRecord({recordKey:rows[0].id})).bytes).toEqual(got.bytes);
  expect(mock.counts.windows).toBe(before);
  expect(reader.stats().page_hits).toBeGreaterThan(0);
  console.log('native-memory-metrics',JSON.stringify({
   real_provider_rows:providerCount,synthetic_binding_rows:rows.length,
   source_bytes:compiled.metrics.source_bytes,
   logical_projected_bytes:compiled.metrics.logical_payload_bytes,
   factor_file_bytes:compiled.metrics.native_factor_file_bytes,
   logical_compression_ratio:compiled.metrics.source_to_logical_payload_ratio,
   pages:compiled.pages.length,compile_ms:compileMs,cache:reader.stats()}));
  reader.release();expect(reader.stats().cached_pages).toBe(0);
 });
});
test('manifest isolation, tampering, byte budget and eviction',async()=>{
 await fixture(async({compiled})=>{
  const manifest=memoryManifestFromNative(compiled),mock=remote(compiled);
  expect(()=>new DenseMemoryReader({nativeClosure:'user:20',manifest,
   baseUrl:'https://fixture.supabase.co',serviceKey:'k'.repeat(40),
   request:mock.request})).toThrow('manifest_or_closure_invalid');
  const capacity=Math.max(...compiled.pages.map(x=>x.payload.length))+1;
  const reader=new DenseMemoryReader({nativeClosure:'user:19',manifest,
   baseUrl:'https://fixture.supabase.co',serviceKey:'k'.repeat(40),
   request:mock.request,cacheMaxBytes:capacity});
  await reader.page(compiled.pages[0].bucket);
  await reader.page(compiled.pages[1].bucket);
  expect(reader.stats().evictions).toBeGreaterThan(0);
  const corrupt=new DenseMemoryReader({nativeClosure:'user:19',manifest,
   baseUrl:'https://fixture.supabase.co',serviceKey:'k'.repeat(40),
   request:remote(compiled,{corrupt:true}).request});
  await expect(corrupt.page(compiled.pages[0].bucket)).rejects.toThrow('page_payload_mismatch');
  await expect(corrupt.prefetch({budgetBytes:1000})).rejects.toThrow('window_request_invalid');
 });
});
test('existing native verified-source fallback handles remote outage',async()=>{
 await fixture(async({compiled,rows,source,factor})=>{
  const reader=new DenseMemoryReader({nativeClosure:'user:19',
   manifest:memoryManifestFromNative(compiled),baseUrl:'https://fixture.supabase.co',
   serviceKey:'k'.repeat(40),request:remote(compiled,{down:true}).request});
  const r=await reader.readRecord({recordKey:rows[3].id,
   nativeFallback:{factorPath:factor,sourcePath:source,compressBinary:binary}});
  expect(r.residency).toBe('native');
  expect(r.remote_status).toContain('remote_lookup_failed');
  expect(r.bytes).toEqual(Buffer.from(JSON.stringify(rows[3])+'\n'));
 });
});
