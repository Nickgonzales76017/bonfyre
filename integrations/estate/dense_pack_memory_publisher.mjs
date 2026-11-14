// Remote physical memory publication; never native semantic activation.
// The caller is an admitted service owner holding a BonfyreAuth closure.
// Recompilation verifies source bytes before every attempted external write.
import {createHash} from 'node:crypto';
import {zstdDecompressSync} from 'node:zlib';
import {compileNativeDensePack,postgrestDenseRows,publishDenseRows,
 namespaceForNativeClosure} from './dense_pack_bridge.mjs';
import {DenseMemoryReader,memoryManifestFromNative} from './dense_pack_memory_reader.mjs';
const MiB=1048576,H=/^[a-f0-9]{64}$/;
const requireTrue=(x,label)=>{if(!x)throw Error('dense_publish_'+label)};
const digest=b=>createHash('sha256').update(b).digest('hex');
const wire=h=>'\\x'+h;
const raw=x=>{
 requireTrue(typeof x==='string'&&/^\\x(?:[a-f0-9]{2})*$/i.test(x),'invalid_bytea');
 return Buffer.from(x.slice(2),'hex');
};
const digest32=x=>{const b=raw(x);requireTrue(b.length===32,'invalid_sha');return b.toString('hex')};
const expected=(a,b,label)=>requireTrue(a===b,'mismatch_'+label);
async function readRoot(compiled,{baseUrl,serviceKey,request}){
 let response;
 try{response=await request(baseUrl+'/rest/v1/rpc/dense_pack_memory_root_readback',{
  method:'POST',redirect:'error',headers:{apikey:serviceKey,
   Authorization:'Bearer '+serviceKey,'Content-Type':'application/json'},
  body:JSON.stringify({p_namespace:wire(compiled.namespace_digest),
   p_root:wire(compiled.root_digest),p_dictionary:wire(compiled.dictionary.dictionary_digest)}),
  signal:AbortSignal.timeout(30000)})}
 catch{throw Error('dense_publish_root_read_uncertain')}
 requireTrue(response?.ok,'root_read_failed');
 const announced=Number(response.headers?.get?.('content-length')??0);
 requireTrue(Number.isFinite(announced)&&announced>=0&&announced<=3*MiB,'root_length_unbounded');
 let rows;
 try{rows=await response.json();
  requireTrue(Buffer.byteLength(JSON.stringify(rows))<=3*MiB,'root_body_unbounded')}
 catch{throw Error('dense_publish_root_body_invalid')}
 requireTrue(Array.isArray(rows)&&rows.length===1,'root_missing_or_ambiguous');
 const r=rows[0],local=postgrestDenseRows(compiled);
 const check=(field,v)=>{
  if(v===null){expected(r[field],null,'root_'+field);return}
  if(typeof v==='string'&&/^\\x(?:[a-f0-9]{2})*$/i.test(v)){
   expected(digest(raw(r[field])),digest(raw(v)),'root_'+field);return
  }
  if(field==='quality_loss_budget')expected(Number(r[field]),Number(v),'root_'+field);
  else expected(String(r[field]),String(v),'root_'+field);
 };
 for(const [k,v] of Object.entries(local.dense_pack_root[0]))
  if(k!=='namespace_digest')check(k,v);
 for(const [k,v] of Object.entries(local.dense_pack_passport[0]))
  if(!['namespace_digest','passport_digest'].includes(k))check(k,v);
 check('dictionary_digest',local.dense_pack_dictionary[0].dictionary_digest);
 expected(digest(raw(r.dictionary_payload)),digest(compiled.dictionary.payload),'dictionary_body');
 expected(Number(r.observed_page_count),compiled.pages.length,'page_census');
 expected(Number(r.observed_record_count),compiled.root.record_count,'record_census');
}
// Exact census: root/passport/dictionary, every page and every atom.
// A verified readback is still a replaceable physical representation.
export async function verifyDensePhysicalReadback({compiled,nativeClosure,
 baseUrl,serviceKey,request=fetch}={}){
 requireTrue(compiled?.schema==='bonfyre.dense-pack.v1'&&
  namespaceForNativeClosure(nativeClosure)===compiled.namespace_digest,'closure_mismatch');
 requireTrue(typeof baseUrl==='string'&&/^https:\/\/[a-z0-9-]+\.supabase\.co$/.test(baseUrl)&&
  typeof serviceKey==='string'&&serviceKey.length>=20,'service_destination_required');
 await readRoot(compiled,{baseUrl,serviceKey,request});
 const reader=new DenseMemoryReader({nativeClosure,manifest:memoryManifestFromNative(compiled),
  baseUrl,serviceKey,request,cacheMaxBytes:MiB});
 let after=-1,pageCount=0,pageBytes=0,remoteCalls=1;
 try{
  while(pageCount<compiled.pages.length){
   const w=await reader.prefetch({afterBucket:after,maxPages:4,budgetBytes:MiB});
   requireTrue(w.pages.length>0,'incomplete_page_window');
   for(const page of w.pages){
    const original=compiled.pages[pageCount++];
    expected(page.bucket,original.bucket,'page_bucket');
    expected(digest(page.payload),digest(original.payload),'page_payload');
    pageBytes+=page.payload.length;
   }
   after=w.next_bucket;
  }
  const end=await reader.directory({afterBucket:after,maxPages:1});
  requireTrue(end.pages.length===0,'extra_remote_page');
  remoteCalls+=reader.stats().remote_calls;
 }finally{reader.release()}
 const atomByDigest=new Map(compiled.atoms.map(a=>[a.atom_digest,a]));
 const ids=[...atomByDigest.keys()].sort();
 let atomBytes=0,atomCount=0;
 for(let i=0;i<ids.length;i+=16){
  const part=ids.slice(i,i+16),remaining=new Set(part);
  let response;
  try{response=await request(baseUrl+'/rest/v1/rpc/dense_pack_atom_slice',{
   method:'POST',redirect:'error',headers:{apikey:serviceKey,
    Authorization:'Bearer '+serviceKey,'Content-Type':'application/json'},
   body:JSON.stringify({p_namespace:wire(compiled.namespace_digest),
    p_digests:part.map(wire)}),signal:AbortSignal.timeout(30000)})}
  catch{throw Error('dense_publish_atom_read_uncertain')}
  requireTrue(response?.ok,'atom_read_failed');
  const length=Number(response.headers?.get?.('content-length')??0);
  requireTrue(Number.isFinite(length)&&length>=0&&length<=36*MiB,'atom_length_unbounded');
  let atoms;
  try{
   atoms=await response.json();
   requireTrue(Buffer.byteLength(JSON.stringify(atoms))<=36*MiB,'atom_body_unbounded');
  }catch{throw Error('dense_publish_atom_body_invalid')}
  requireTrue(Array.isArray(atoms)&&atoms.length===part.length,'incomplete_atom_census');
  for(const a of atoms){
   const id=digest32(a.atom_digest);
   requireTrue(remaining.delete(id),'unexpected_or_duplicate_atom');
   const original=atomByDigest.get(id),payload=raw(a.payload);
   expected(Number(a.codec),original.codec,'atom_codec');
   expected(Number(a.raw_bytes),original.raw_bytes,'atom_raw_bytes');
   expected(a.dictionary_digest,null,'atom_dictionary');
   expected(digest(payload),digest(original.payload),'atom_payload');
   let plain;
   try{plain=original.codec===1?zstdDecompressSync(payload,{maxOutputLength:MiB}):payload}
   catch{throw Error('dense_publish_atom_decode_failed')}
   expected(plain.length,original.raw_bytes,'atom_size');
   expected(digest(plain),id,'atom_original_digest');
   atomBytes+=payload.length;atomCount++;
  }
  requireTrue(remaining.size===0,'missing_atom_digest');
  remoteCalls++;
 }
 return Object.freeze({state:'REMOTE_PHYSICAL_READBACK_VERIFIED_NOT_NATIVE_ACTIVATION',
  root_digest:compiled.root_digest,source_receipt_digest:compiled.passport.source_receipt_digest,
  namespace_digest:compiled.namespace_digest,pages:pageCount,atoms:atomCount,
  page_bytes:pageBytes,atom_bytes:atomBytes,dictionary_bytes:compiled.dictionary.payload.length,
  readback_calls:remoteCalls,candidate_only:true,native_activation:false,external_write:false});
}
async function selectedNative({factorPath,sourcePath,compressBinary,nativeClosure,
 expectedRootDigest,expectedSourceReceiptDigest,maxRecords=100000}){
 requireTrue(typeof expectedRootDigest==='string'&&H.test(expectedRootDigest)&&
  typeof expectedSourceReceiptDigest==='string'&&H.test(expectedSourceReceiptDigest),
  'native_selection_required');
 const c=await compileNativeDensePack({factorPath,sourcePath,compressBinary,
  verifiedClosure:nativeClosure,maxRecords});
 expected(c.root_digest,expectedRootDigest,'selected_root');
 expected(c.passport.source_receipt_digest,expectedSourceReceiptDigest,'selected_source');
 return c;
}
// Dry run by default. Live write requires external native authorization.
// On failure, do NOT retry: reconcile partial/uncertain writes read-only.
export async function publishNativeDenseMemory({dryRun=true,baseUrl,serviceKey,
 request=fetch,...native}={}){
 const compiled=await selectedNative(native);
 if(dryRun)return Object.freeze({state:'NATIVE_VERIFIED_DRY_RUN',external_write:false,
  root_digest:compiled.root_digest,source_receipt_digest:compiled.passport.source_receipt_digest,
  records:compiled.root.record_count,pages:compiled.pages.length,atoms:compiled.atoms.length,
  source_bytes:compiled.metrics.source_bytes,logical_bytes:compiled.metrics.logical_payload_bytes});
 const write=await publishDenseRows({compiled,baseUrl,serviceKey,request,dryRun:false});
 const proof=await verifyDensePhysicalReadback({compiled,nativeClosure:native.nativeClosure,
  baseUrl,serviceKey,request});
 return Object.freeze({...proof,posted_batches:write.posted_batches,external_write:true});
}
export async function reconcileNativeDenseMemory({baseUrl,serviceKey,
 request=fetch,...native}={}){
 const compiled=await selectedNative(native);
 return verifyDensePhysicalReadback({compiled,nativeClosure:native.nativeClosure,
  baseUrl,serviceKey,request});
}
