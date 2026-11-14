import {test,expect} from 'bun:test';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {compileNativeDensePack} from '../../../integrations/estate/dense_pack_bridge.mjs';
import {publishNativeDenseMemory,reconcileNativeDenseMemory} from '../../../integrations/estate/dense_pack_memory_publisher.mjs';
const binary=resolve(import.meta.dir,'../bonfyre-compress');
const ledger=resolve(import.meta.dir,'../../BonfyreProviderEstate/providers.tsv');
const readBytea=x=>Buffer.from(x.slice(2),'hex');
async function fixture(fn){
 const dir=mkdtempSync(join(tmpdir(),'bf-publisher-'));
 try{
  const lines=readFileSync(ledger,'utf8').trim().split('\n').slice(1);
  const rows=lines.map(line=>{const c=line.split('\t');return{
   id:'native-provider-'+c[1],title:c[2],provider:c[2],slug:c[1],
   family:c[3],tenancy:c[7],source:'BonfyreProviderEstate:providers.tsv'}});
  const source=join(dir,'providers.jsonl'),factor=join(dir,'providers.factor.db');
  writeFileSync(source,rows.map(x=>JSON.stringify(x)).join('\n')+'\n');
  execFileSync(binary,['factor-records',source,factor],{stdio:'pipe'});
  const c=await compileNativeDensePack({factorPath:factor,sourcePath:source,
   compressBinary:binary,verifiedClosure:'user:19'});
  const pins={factorPath:factor,sourcePath:source,compressBinary:binary,nativeClosure:'user:19',
   expectedRootDigest:c.root_digest,expectedSourceReceiptDigest:c.passport.source_receipt_digest,
   baseUrl:'https://fixture.supabase.co',serviceKey:'s'.repeat(40)};
  await fn({compiled:c,rows,pins,source});
 }finally{rmSync(dir,{recursive:true,force:true})}
}
function provider({failTable=null}={}){
 const tables=new Map();
 const calls={post:0,get:0,rpc:0};
 const response=rows=>({ok:true,status:200,json:async()=>rows});
 const request=async(address,options)=>{
  const url=new URL(address),name=url.pathname.split('/').at(-1);
  expect(options.headers.Authorization).toBe('Bearer '+('s'.repeat(40)));
  if(url.pathname.startsWith('/rest/v1/rpc/')){
   calls.rpc++;
   const args=JSON.parse(options.body);
   const names=filter=>[...(tables.get(filter)||[])];
   const byteNs=args.p_namespace;
   if(name==='dense_pack_memory_root_readback'){
    const rootRow=names('dense_pack_root').find(r=>
     r.namespace_digest===byteNs&&r.root_digest===args.p_root);
    const passport=rootRow&&names('dense_pack_passport').find(p=>
     p.namespace_digest===byteNs&&p.passport_digest===rootRow.passport_digest);
    const dictionary=names('dense_pack_dictionary').find(d=>
     d.namespace_digest===byteNs&&d.dictionary_digest===args.p_dictionary);
    const pages=names('dense_pack_page').filter(p=>
     p.namespace_digest===byteNs&&p.root_digest===args.p_root);
    return response(rootRow&&passport&&dictionary?[{...rootRow,...passport,
     dictionary_digest:dictionary.dictionary_digest,
     dictionary_payload:dictionary.payload,observed_page_count:pages.length,
     observed_record_count:pages.reduce((n,p)=>n+p.record_count,0)}]:[]);
   }
   if(name==='dense_pack_memory_window'||name==='dense_pack_memory_directory'){
    let pages=names('dense_pack_page').filter(p=>p.namespace_digest===byteNs&&
      p.root_digest===args.p_root&&p.bucket>args.p_after_bucket)
      .sort((a,b)=>a.bucket-b.bucket).slice(0,args.p_max_pages);
    if(name==='dense_pack_memory_window'){
     let total=0;
     pages=pages.filter(p=>{total+=readBytea(p.payload).length;return total<=args.p_budget_bytes});
    }
    return response(pages.map(p=>({
     bucket:p.bucket,record_count:p.record_count,codec:p.codec,
     raw_bytes:p.raw_bytes,stored_bytes:readBytea(p.payload).length,
     page_digest:p.page_digest,encoded_digest:p.encoded_digest,
     ...(name==='dense_pack_memory_window'?{payload:p.payload}:{})})));
   }
   if(name==='dense_pack_atom_slice'){
    return response(args.p_digests.flatMap(id=>names('dense_pack_atom').filter(a=>
     a.namespace_digest===byteNs&&a.atom_digest===id)));
   }
   throw Error('unknown physical rpc '+name);
  }
  if(options.method==='POST'){
   calls.post++;
   if(name===failTable)throw Error('injected network loss');
   const rows=JSON.parse(options.body),stored=tables.get(name)||[];
   for(const row of rows){
    if(!stored.some(prev=>JSON.stringify(prev)===JSON.stringify(row)))stored.push(row);
   }
   tables.set(name,stored);return {ok:true,status:201};
  }
  if(options.method==='GET'){
   calls.get++;
   const rows=tables.get(name)||[];
   return response(rows.filter(row=>[...url.searchParams].every(([k,v])=>
     k==='limit'||row[k]===v.slice(3))).slice(0,2));
  }
  throw Error('unexpected request '+options.method);
 };
 return{request,calls,tables};
}
test('dry run uses verified native source, requires exact pins and performs no remote writes',async()=>{
 await fixture(async({pins,compiled,source})=>{
  const net=provider(),opts={...pins,request:net.request};
  const dry=await publishNativeDenseMemory(opts);
  expect(dry.state).toBe('NATIVE_VERIFIED_DRY_RUN');
  expect(dry.records).toBe(104);
  expect(dry.pages).toBe(compiled.pages.length);
  expect(dry.external_write).toBe(false);
  expect(net.calls.post+net.calls.get+net.calls.rpc).toBe(0);
  await expect(publishNativeDenseMemory({...opts,expectedRootDigest:'f'.repeat(64)}))
   .rejects.toThrow('selected_root');
  writeFileSync(source,readFileSync(source,'utf8').replace('native-provider','changed-provider'));
  await expect(publishNativeDenseMemory(opts)).rejects.toThrow('native_source_verification_failed');
 });
});
test('full physical projection gets exact byte readback, is never native activation',async()=>{
 await fixture(async({pins,compiled})=>{
  const net=provider(),opts={...pins,request:net.request};
  const result=await publishNativeDenseMemory({...opts,dryRun:false});
  expect(result.state).toBe('REMOTE_PHYSICAL_READBACK_VERIFIED_NOT_NATIVE_ACTIVATION');
  expect(result.native_activation).toBe(false);
  expect(result.candidate_only).toBe(true);
  expect(result.pages).toBe(compiled.pages.length);
  expect(result.atoms).toBe(compiled.atoms.length);
  expect(result.posted_batches).toBeGreaterThan(0);
  expect(net.calls.post).toBeGreaterThan(0);
  expect(net.calls.get).toBe(0);
  expect(net.calls.rpc).toBeGreaterThan(1);
  const before=net.calls.post;
  const reconciled=await reconcileNativeDenseMemory(opts);
  expect(reconciled.root_digest).toBe(compiled.root_digest);
  expect(net.calls.post).toBe(before);
  const passport=net.tables.get('dense_pack_passport')[0];
  const savedReceipt=passport.source_receipt_digest;
  passport.source_receipt_digest='\\x'+('0'.repeat(64));
  await expect(reconcileNativeDenseMemory(opts)).rejects.toThrow('root_source_receipt_digest');
  passport.source_receipt_digest=savedReceipt;
  const pages=net.tables.get('dense_pack_page');
  const missing=pages.pop();
  await expect(reconcileNativeDenseMemory(opts)).rejects.toThrow('root_page_census');
  pages.push(missing);
  const atom=net.tables.get('dense_pack_atom')[0];
  atom.payload='\\x00';
  await expect(reconcileNativeDenseMemory(opts)).rejects.toThrow('atom_payload');
 });
});
test('partial write fails closed; read-only reconciliation never retries',async()=>{
 await fixture(async({pins})=>{
  const net=provider({failTable:'dense_pack_root'}),opts={...pins,request:net.request};
  await expect(publishNativeDenseMemory({...opts,dryRun:false}))
   .rejects.toThrow('uncertain_write');
  const sent=net.calls.post;
  await expect(reconcileNativeDenseMemory(opts)).rejects.toThrow('table_missing_or_ambiguous');
  expect(net.calls.post).toBe(sent);
 });
});
