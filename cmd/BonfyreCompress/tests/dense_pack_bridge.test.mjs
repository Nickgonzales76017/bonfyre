import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {compileNativeDensePack,namespaceForNativeClosure,keyDigest,decodePage,
 hydrateDenseRecord,postgrestDenseRows,publishDenseRows,readDenseRecordFromSupabase,readDenseWithNativeFallback} from '../../../integrations/estate/dense_pack_bridge.mjs';
const compress=resolve(import.meta.dir,'../bonfyre-compress');
const fixture=async(fn)=>{
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-dense-'));
 try{
  const source=join(dir,'native.jsonl'),pack=join(dir,'native.factor.db'),rows=[];
  for(let i=0;i<320;i++)rows.push({
   id:'source-'+String(i).padStart(4,'0'),
   title:'Bound source '+(i%8),
   shared:{class:'published-sample',body:('One native base; repeated across many lawful bindings. ').repeat(14)},
   metadata:{index:i,same:i%3===0,kind:'reference',family:'source',witness:'source-record'},
   scope:{owner:'BonfyreCompress',cut:'exact-verified-source',status:'candidate'}
  });
  writeFileSync(source,rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
  execFileSync(compress,['factor-records',source,pack],{stdio:'pipe'});
  await fn({source,pack,rows});
 }finally{rmSync(dir,{recursive:true,force:true})}
};
test('native 320-record factor exports byte-exact compact page candidates with original fragments',async()=>{
 await fixture(async({source,pack,rows})=>{
  const c=await compileNativeDensePack({factorPath:pack,sourcePath:source,compressBinary:compress,
   verifiedClosure:'user:19'});
  expect(c.schema).toBe('bonfyre.dense-pack.v1');
  expect(c.root.record_count).toBe(320);
  expect(c.root.page_count).toBe(c.pages.length);
  expect(c.metrics.distinct_atoms).toBeLessThan(rows.length*6);
  expect(c.metrics.source_bytes).toBe(readFileSync(source).length);
  expect(c.passport.exactness_class).toBe('exact');
  expect(c.passport.quality_loss_budget).toBe(0);
  for(let i=0;i<rows.length;i++){
   const raw=hydrateDenseRecord(c,rows[i].id);
   expect(raw).toEqual(Buffer.from(JSON.stringify(rows[i])+'\n'));
  }
  expect(hydrateDenseRecord(c,'not present')).toBeNull();
  expect(c.metrics.logical_payload_bytes).toBeGreaterThan(0);
  // Logical payload metric excludes Postgres indexes, WAL and heap overhead.
  expect(c.metrics.logical_payload_bytes).toBe(
   c.metrics.atom_payload_bytes+c.metrics.dictionary_bytes+c.metrics.pointer_page_bytes);
  const rowsToShip=postgrestDenseRows(c);
  expect(rowsToShip.dense_pack_root).toHaveLength(1);
  expect(rowsToShip.dense_pack_passport[0].native_owner).toBe('BonfyreCompress');
  expect(rowsToShip.dense_pack_atom).toHaveLength(c.metrics.distinct_atoms);
  expect(rowsToShip.dense_pack_page).toHaveLength(c.pages.length);
  expect(rowsToShip.dense_pack_page[0].payload).toStartWith('\\x');
  expect(rowsToShip.dense_pack_page[0].page_digest).toStartWith('\\x');
  expect(JSON.stringify(rowsToShip)).not.toContain(JSON.stringify(rows[3]));
 });
});
test('page integrity, digest addressing and closure separation are deterministic',async()=>{
 await fixture(async({source,pack,rows})=>{
  const c=await compileNativeDensePack({factorPath:pack,sourcePath:source,compressBinary:compress,
   verifiedClosure:'user:19'}),p=c.pages[0];
  expect(decodePage(p.payload,{pageDigest:p.page_digest,encodedDigest:p.encoded_digest,
    codec:p.codec,rawBytes:p.raw_bytes,bucketBits:c.root.bucket_bits,bucketId:p.bucket})
    .length).toBe(p.record_count);
  expect(()=>decodePage(Buffer.concat([p.payload,Buffer.from('junk')]),{encodedDigest:p.encoded_digest,codec:p.codec}))
    .toThrow('page_encoded_digest_mismatch');
  const edited={...c,pages:c.pages.map(x=>({...x}))};
  edited.pages[0].payload=Buffer.from(edited.pages[0].payload);edited.pages[0].payload[8]^=2;
  expect(()=>hydrateDenseRecord(edited,rows.find(r=>
    c.pages[0].bucket===(parseInt(keyDigest(r.id).slice(0,4),16)>>(16-c.root.bucket_bits))).id))
   .toThrow('page_encoded_digest_mismatch');
  expect(namespaceForNativeClosure('user:19')).not.toBe(namespaceForNativeClosure('user:20'));
  expect(()=>namespaceForNativeClosure('institution:../../wrong')).toThrow('closure_invalid');
  expect(()=>keyDigest('')).toThrow('key_invalid');
  const other=await compileNativeDensePack({factorPath:pack,sourcePath:source,compressBinary:compress,
   verifiedClosure:'user:20'});
  expect(other.root_digest).not.toBe(c.root_digest);
 });
});
test('no external storage write on a dry run; a confirmed write is still not native activation',async()=>{
 await fixture(async({source,pack})=>{
  const c=await compileNativeDensePack({factorPath:pack,sourcePath:source,compressBinary:compress,
   verifiedClosure:'user:19'});
  let calls=0;const request=async(url,options)=>{calls++;
   expect(url.startsWith('https://fixture.supabase.co/rest/v1/dense_pack_')).toBe(true);
   expect(options.method).toBe('POST');expect(options.headers.Prefer).toContain('ignore-duplicates');
   expect(options.headers.Authorization).toBe('Bearer '+('x'.repeat(40)));
   return {ok:true,status:201}};
  const opts={compiled:c,baseUrl:'https://fixture.supabase.co',serviceKey:'x'.repeat(40),request};
  expect(await publishDenseRows(opts)).toMatchObject({state:'DRY_RUN',external_write:false});
  expect(calls).toBe(0);
  const result=await publishDenseRows({...opts,dryRun:false});
  expect(result.state).toBe('POST_CONFIRMED_NOT_NATIVE_ACTIVATION');
  expect(calls).toBe(result.posted_batches);
  expect(JSON.stringify(result)).not.toContain('x'.repeat(40));
  let attempted=0;
  await expect(publishDenseRows({...opts,dryRun:false,request:async()=>{
   attempted++;throw Error('timeout')}})).rejects.toThrow('uncertain_write');
  expect(attempted).toBe(1);
 });
});
test('exact native source verification cannot be bypassed by a changed input',async()=>{
 await fixture(async({source,pack})=>{
  writeFileSync(source,readFileSync(source).toString('utf8').replace('source-0001','source-XXXX'));
  await expect(compileNativeDensePack({factorPath:pack,sourcePath:source,compressBinary:compress,
    verifiedClosure:'user:19'})).rejects.toThrow('native_source_verification_failed');
 });
});
test('service-owned Supabase page + atom RPCs reconstruct only requested native-derived bytes',async()=>{
 await fixture(async({source,pack,rows})=>{
  const compiled=await compileNativeDensePack({factorPath:pack,sourcePath:source,
    compressBinary:compress,verifiedClosure:'user:19'});
  expect(compiled.pages.some(p=>p.codec===1)).toBe(true);
  const bytea=buf=>'\\x'+Buffer.from(buf).toString('hex');
  const namespace='\\x'+compiled.namespace_digest;
  const root='\\x'+compiled.root_digest;
  const atomMap=new Map(compiled.atoms.map(a=>[a.atom_digest,a]));
  const tally={pages:0,atoms:0};
  const request=async(url,options)=>{
    const args=JSON.parse(options.body);
    expect(options.headers.Authorization).toBe('Bearer '+('z'.repeat(40)));
    if(url.endsWith('/rpc/dense_pack_candidate_page_v2')){
     tally.pages++;
     if(args.p_namespace!==namespace||args.p_root!==root)return{ok:true,json:async()=>[]};
     const id=parseInt(args.p_key_digest.slice(2,6),16)>>(16-compiled.root.bucket_bits);
     const p=compiled.pages.find(x=>x.bucket===id);
     return{ok:true,json:async()=>p?[{bucket:p.bucket,record_count:p.record_count,
       codec:p.codec,raw_bytes:p.raw_bytes,payload:bytea(p.payload),
       page_digest:'\\x'+p.page_digest,encoded_digest:'\\x'+p.encoded_digest}]:[]};
    }
    expect(url.endsWith('/rpc/dense_pack_atom_slice')).toBe(true);
    tally.atoms++;
    expect(args.p_namespace).toBe(namespace);
    expect(args.p_digests.length).toBeLessThanOrEqual(16);
    return{ok:true,json:async()=>args.p_digests.map(id=>{
      const atom=atomMap.get(id.slice(2));
      return{atom_digest:id,raw_bytes:atom.raw_bytes,codec:atom.codec,
        dictionary_digest:null,payload:bytea(atom.payload)};
    })};
  };
  const opts={nativeClosure:'user:19',rootDigest:compiled.root_digest,
    baseUrl:'https://fixture.supabase.co',serviceKey:'z'.repeat(40),request};
  const result=await readDenseRecordFromSupabase({...opts,recordKey:rows[217].id});
  expect(result.state).toBe('PHYSICAL_BYTES_VERIFIED_NOT_SOURCE_AUTHORITY');
  expect(result.candidate_only).toBe(true);
  expect(result.bytes).toEqual(Buffer.from(JSON.stringify(rows[217])+'\n'));
  expect(tally.pages).toBe(1);
  expect(tally.atoms).toBeGreaterThan(0);
  expect(await readDenseRecordFromSupabase({...opts,nativeClosure:'user:20',
    recordKey:rows[217].id})).toBeNull();
  expect(await readDenseRecordFromSupabase({...opts,recordKey:'nonexistent'})).toBeNull();
 });
});
test('remote read refuses corrupted page and never promotes a retrieved body to evidence',async()=>{
 await fixture(async({source,pack,rows})=>{
  const c=await compileNativeDensePack({factorPath:pack,sourcePath:source,
   compressBinary:compress,verifiedClosure:'user:19'});
  const key=rows[0].id,bit=parseInt(keyDigest(key).slice(0,4),16)>>(16-c.root.bucket_bits);
  const p=c.pages.find(x=>x.bucket===bit);
  const changed=Buffer.from(p.payload);changed[changed.length-1]^=1;
  const request=async(url,options)=>({ok:true,json:async()=>[{
   bucket:p.bucket,record_count:p.record_count,codec:p.codec,raw_bytes:p.raw_bytes,
   payload:'\\x'+changed.toString('hex'),page_digest:'\\x'+p.page_digest,
   encoded_digest:'\\x'+p.encoded_digest
  }]});
  await expect(readDenseRecordFromSupabase({nativeClosure:'user:19',
    rootDigest:c.root_digest,recordKey:key,baseUrl:'https://fixture.supabase.co',
    serviceKey:'z'.repeat(40),request})).rejects.toThrow('page_encoded_digest_mismatch');
 });
});
test('Supabase loss falls back to exact native bytes without replaying a remote write',async()=>{
 await fixture(async({source,pack,rows})=>{
  const c=await compileNativeDensePack({factorPath:pack,sourcePath:source,
   compressBinary:compress,verifiedClosure:'user:19'});
  let requests=0;
  const input={nativeClosure:'user:19',rootDigest:c.root_digest,
   factorPath:pack,sourcePath:source,compressBinary:compress,
   baseUrl:'https://fixture.supabase.co',serviceKey:'z'.repeat(40),
   request:async()=>{requests++;throw Error('network down')}};
  const selected=await readDenseWithNativeFallback({...input,recordKey:rows[215].id});
  expect(selected.residency).toBe('native');
  expect(selected.state).toBe('PHYSICAL_BYTES_VERIFIED_NOT_SOURCE_AUTHORITY');
  expect(selected.candidate_only).toBe(true);
  expect(selected.bytes).toEqual(Buffer.from(JSON.stringify(rows[215])+'\n'));
  expect(selected.remote_status).toContain('remote_lookup_failed');
  expect(requests).toBe(1);
  expect(await readDenseWithNativeFallback({...input,recordKey:'does-not-exist'})).toBeNull();
  const absent=await readDenseWithNativeFallback({...input,recordKey:rows[1].id,
   request:async()=>({ok:true,json:async()=>[]})});
  expect(absent.residency).toBe('native');
  expect(absent.remote_status).toBe('projection_absent');
  writeFileSync(source,readFileSync(source).toString().replace('source-0001','source-XXXX'));
  await expect(readDenseWithNativeFallback({...input,recordKey:rows[215].id}))
   .rejects.toThrow('native_fallback_source_verification_failed');
 });
});
