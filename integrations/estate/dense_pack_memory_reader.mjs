// Native-owned Dense Packs: bounded physical-memory read-through, never authority.
// Only a BonfyreAuth-verified closure and native-verified manifest may be passed.
import {createHash} from 'node:crypto';
import {namespaceForNativeClosure,keyDigest,decodePage,
  readDenseRecordFromSupabase,readDenseWithNativeFallback} from './dense_pack_bridge.mjs';
const MiB=1048576,H=/^[a-f0-9]{64}$/;
const assert=(v,s)=>{if(!v)throw Error('dense_memory_'+s)};
const integer=(v,a,b)=>Number.isSafeInteger(v)&&v>=a&&v<=b;
const hash=b=>createHash('sha256').update(b).digest('hex');
const wire=h=>'\\x'+h;
function bytea(s){assert(typeof s==='string'&&/^\\x(?:[a-f0-9]{2})*$/i.test(s),'invalid_bytea');return Buffer.from(s.slice(2),'hex')}
function sha(s){const b=bytea(s);assert(b.length===32,'invalid_sha');return b.toString('hex')}
export function memoryManifestFromNative(c){
 assert(c?.schema==='bonfyre.dense-pack.v1'&&H.test(c.namespace_digest)&&H.test(c.root_digest),'unverified_compilation');
 const bits=c.root.bucket_bits;
 assert(integer(bits,0,16)&&Array.isArray(c.pages)&&integer(c.pages.length,1,65536)&&c.pages.length===c.root.page_count,'invalid_page_count');
 let previous=-1;
 const pages=c.pages.map(p=>{
  assert(integer(p.bucket,0,(2**bits)-1)&&p.bucket>previous&&H.test(p.page_digest)&&H.test(p.encoded_digest)&&
   Buffer.isBuffer(p.payload)&&hash(p.payload)===p.encoded_digest&&integer(p.payload.length,8,MiB)&&
   integer(p.record_count,1,4096)&&integer(p.raw_bytes,8,MiB)&&[0,1].includes(p.codec),'invalid_native_page');
  previous=p.bucket;
  return Object.freeze({bucket:p.bucket,record_count:p.record_count,codec:p.codec,raw_bytes:p.raw_bytes,
   stored_bytes:p.payload.length,page_digest:p.page_digest,encoded_digest:p.encoded_digest});
 });
 return Object.freeze({namespace_digest:c.namespace_digest,root_digest:c.root_digest,bucket_bits:bits,
  source_receipt_digest:c.passport.source_receipt_digest,pages:Object.freeze(pages)});
}
export class DenseMemoryReader{
 constructor({nativeClosure,manifest,baseUrl,serviceKey,request=fetch,cacheMaxBytes=4*MiB}={}){
  const ns=namespaceForNativeClosure(nativeClosure);
  assert(manifest?.namespace_digest===ns&&H.test(manifest?.root_digest??'')&&integer(manifest?.bucket_bits,0,16)&&
   Array.isArray(manifest?.pages)&&integer(manifest.pages.length,1,65536),'manifest_or_closure_invalid');
  assert(typeof baseUrl==='string'&&/^https:\/\/[a-z0-9-]+\.supabase\.co$/.test(baseUrl)&&
   typeof serviceKey==='string'&&serviceKey.length>=20&&typeof request==='function','invalid_remote');
  assert(integer(cacheMaxBytes,128,64*MiB),'invalid_cache_budget');
  Object.assign(this,{nativeClosure,namespace:ns,root:manifest.root_digest,bits:manifest.bucket_bits,
   baseUrl,serviceKey,request,capacity:cacheMaxBytes,used:0,cache:new Map(),hits:0,misses:0,evictions:0,calls:0});
  this.proofs=new Map();let prior=-1;
  for(const p of manifest.pages){
   assert(integer(p.bucket,0,(2**this.bits)-1)&&p.bucket>prior&&H.test(p.page_digest)&&
    H.test(p.encoded_digest)&&integer(p.stored_bytes,8,MiB)&&integer(p.raw_bytes,8,MiB)&&
    integer(p.record_count,1,4096)&&[0,1].includes(p.codec),'invalid_manifest_page');
   this.proofs.set(p.bucket,Object.freeze({...p}));prior=p.bucket;
  }
  this.order=[...this.proofs.keys()];
 }
 stats(){return{cached_pages:this.cache.size,cached_bytes:this.used,page_hits:this.hits,
  page_misses:this.misses,evictions:this.evictions,remote_calls:this.calls,cache_budget_bytes:this.capacity}}
 release(){this.cache.clear();this.used=0}
 #copy(p){return {...p,payload:Buffer.from(p.payload),rows:p.rows.map(x=>({...x,atom_digests:[...x.atom_digests]}))}}
 #put(p){
  if(p.payload.length>this.capacity)return;
  const old=this.cache.get(p.bucket);
  if(old){this.cache.delete(p.bucket);this.used-=old.payload.length}
  while(this.used+p.payload.length>this.capacity){
   const b=this.cache.keys().next().value;if(b===undefined)break;
   this.used-=this.cache.get(b).payload.length;this.cache.delete(b);this.evictions++;
  }
  this.cache.set(p.bucket,p);this.used+=p.payload.length;
 }
 #cached(bucket){
  const p=this.cache.get(bucket);if(!p)return null;
  this.cache.delete(bucket);this.cache.set(bucket,p);this.hits++;
  return this.#copy(p);
 }
 async #rpc(name,args,limit){
  let res;
  try{res=await this.request(this.baseUrl+'/rest/v1/rpc/'+name,{
   method:'POST',redirect:'error',headers:{apikey:this.serviceKey,Authorization:'Bearer '+this.serviceKey,'Content-Type':'application/json'},
   body:JSON.stringify(args),signal:AbortSignal.timeout(30000)})}
  catch{throw Error('dense_memory_remote_unavailable')}
  assert(res?.ok,'remote_status_'+String(res?.status??'unknown'));
  const length=Number(res.headers?.get?.('content-length')??0);
  assert(Number.isFinite(length)&&length>=0&&length<=limit,'remote_length_exceeded');
  let rows;
  try{
   if(res.body?.getReader){
    const reader=res.body.getReader(),parts=[];let total=0;
    while(true){const next=await reader.read();if(next.done)break;
     total+=next.value.byteLength;
     if(total>limit){await reader.cancel();throw Error('size')}
     parts.push(Buffer.from(next.value));
    }
    rows=JSON.parse(Buffer.concat(parts,total).toString('utf8'));
   }else{
    rows=await res.json();assert(Buffer.byteLength(JSON.stringify(rows))<=limit,'oversized_mock_body');
   }
  }catch{throw Error('dense_memory_invalid_remote_body')}
  assert(Array.isArray(rows)&&rows.length<=64,'remote_row_count');
  this.calls++;return rows;
 }
 #next(after,n){return this.order.filter(b=>b>after).slice(0,n)}
 #check(row,proof,body){
  assert(row&&proof&&row.bucket===proof.bucket&&row.record_count===proof.record_count&&
   row.codec===proof.codec&&row.raw_bytes===proof.raw_bytes&&row.stored_bytes===proof.stored_bytes&&
   sha(row.page_digest)===proof.page_digest&&sha(row.encoded_digest)===proof.encoded_digest,
   'page_proof_mismatch');
  if(!body)return null;
  const payload=bytea(row.payload);
  assert(payload.length===proof.stored_bytes&&hash(payload)===proof.encoded_digest,'page_payload_mismatch');
  const records=decodePage(payload,{codec:proof.codec,rawBytes:proof.raw_bytes,pageDigest:proof.page_digest,
   encodedDigest:proof.encoded_digest,bucketBits:this.bits,bucketId:proof.bucket});
  assert(records.length===proof.record_count,'page_count_mismatch');
  return {...proof,payload,rows:records};
 }
 async directory({afterBucket=-1,maxPages=32}={}){
  assert(integer(afterBucket,-1,65535)&&integer(maxPages,1,64),'directory_request_invalid');
  const expected=this.#next(afterBucket,maxPages);
  const rows=await this.#rpc('dense_pack_memory_directory',{p_namespace:wire(this.namespace),p_root:wire(this.root),
   p_after_bucket:afterBucket,p_max_pages:maxPages},65536);
  assert(rows.length<=expected.length,'directory_extra_rows');
  rows.forEach((r,i)=>this.#check(r,this.proofs.get(expected[i]),false));
  return{state:!expected.length?'END_OF_ROOT':rows.length?'DIRECTORY_CANDIDATE':'PROJECTION_ABSENT',
   next_bucket:rows.length?rows.at(-1).bucket:afterBucket,pages:rows};
 }
 async prefetch({afterBucket=-1,maxPages=4,budgetBytes=MiB}={}){
  assert(integer(afterBucket,-1,65535)&&integer(maxPages,1,8)&&integer(budgetBytes,MiB,4*MiB),'window_request_invalid');
  const expected=this.#next(afterBucket,maxPages);
  if(!expected.length)return{state:'END_OF_ROOT',pages:[],next_bucket:afterBucket,stored_bytes:0};
  const rows=await this.#rpc('dense_pack_memory_window',{p_namespace:wire(this.namespace),
   p_root:wire(this.root),p_after_bucket:afterBucket,p_max_pages:maxPages,p_budget_bytes:budgetBytes},10*MiB);
  assert(rows.length<=expected.length,'window_extra_rows');
  let total=0;const pages=[];
  for(let i=0;i<rows.length;i++){
   const p=this.#check(rows[i],this.proofs.get(expected[i]),true);
   total+=p.payload.length;assert(total<=budgetBytes,'window_budget_exceeded');
   this.#put(p);pages.push(this.#copy(p));
  }
  return{state:pages.length?'PAGES_VERIFIED_CANDIDATES':'PROJECTION_ABSENT',
   next_bucket:pages.length?pages.at(-1).bucket:afterBucket,stored_bytes:total,pages};
 }
 async page(bucket){
  assert(integer(bucket,0,(2**this.bits)-1),'invalid_bucket');
  if(!this.proofs.has(bucket))return null;
  const cached=this.#cached(bucket);if(cached)return cached;
  this.misses++;
  const w=await this.prefetch({afterBucket:bucket-1,maxPages:1});
  assert(w.pages.length<=1,'single_page_overflow');
  if(!w.pages.length)return null;
  assert(w.pages[0].bucket===bucket,'page_address_changed');
  return w.pages[0];
 }
 async readRecord({recordKey,nativeFallback}={}){
  const key=keyDigest(recordKey),bucket=parseInt(key.slice(0,4),16)>>(16-this.bits);
  const adapter=async(url,options)=>{
   if(url.endsWith('/rpc/dense_pack_candidate_page_v2')){
    const args=JSON.parse(options.body);
    assert(args.p_namespace===wire(this.namespace)&&args.p_root===wire(this.root)&&
     args.p_key_digest===wire(key),'record_route_mismatch');
    const p=await this.page(bucket);
    return{ok:true,status:200,json:async()=>p?[{bucket:p.bucket,record_count:p.record_count,
     codec:p.codec,raw_bytes:p.raw_bytes,payload:wire(p.payload.toString('hex')),
     page_digest:wire(p.page_digest),encoded_digest:wire(p.encoded_digest)}]:[]};
   }
   return this.request(url,options);
  };
  const options={nativeClosure:this.nativeClosure,rootDigest:this.root,recordKey,
   baseUrl:this.baseUrl,serviceKey:this.serviceKey,request:adapter};
  if(nativeFallback)return readDenseWithNativeFallback({...options,
   factorPath:nativeFallback.factorPath,sourcePath:nativeFallback.sourcePath,
   compressBinary:nativeFallback.compressBinary});
  const r=await readDenseRecordFromSupabase(options);
  return r?{...r,residency:'supabase'}:null;
 }
}
