// Dense Packs v1: portable byte-exact lowering of BonfyreCompress record-factor
// SQLite. Supabase is a replaceable data projection, not an identity, admission,
// provenance, model comparability, Ghost acceptance, or execution authority.
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {createReadStream,statSync} from 'node:fs';
import {zstdDecompressSync} from 'node:zlib';
const hex=/^[0-9a-f]{64}$/;
const sha=b=>createHash('sha256').update(b).digest('hex');
const ensure=(condition,reason)=>{if(!condition)throw Error('dense_pack_'+reason)};
const digest=b=>{ensure(typeof b==='string'&&hex.test(b),'digest_invalid');return b};
const b32=h=>Buffer.from(digest(h),'hex');
const wire=h=>'\\x'+digest(h);
const size=b=>Buffer.byteLength(b);
const MAX=1024*1024;
export function namespaceForNativeClosure(closure){
 ensure(typeof closure==='string'&&/^(user:[1-9][0-9]{0,9}|institution:[a-z0-9-]{1,64}|household:[a-z0-9-]{1,64})$/.test(closure),'closure_invalid');
 // Only the BonfyreAuth native owner may establish the closure input; this
 // function derives a routing digest and cannot grant a right to use one.
 return sha(Buffer.from('bonfyre-dense-namespace-v1\0'+closure));
}
export function keyDigest(key){
 ensure(typeof key==='string'&&size(key)>0&&size(key)<=240,'key_invalid');
 return sha(Buffer.from('bonfyre-dense-key-v1\0'+key));
}
const bucket=(key,bits)=>(parseInt(key.slice(0,4),16)>>(16-bits));
const encoded=(value)=>Buffer.isBuffer(value)?value:Buffer.from(value??[]);
function openNativeFactor(path){
 const db=new Database(path,{readonly:true,strict:true});
 try{
  ensure(db.query('PRAGMA application_id').get().application_id===1112688198,'not_native_factor');
  ensure(db.query('PRAGMA user_version').get().user_version===2,'unsupported_factor_version');
  return db;
 }catch(e){db.close();throw e}
}
function verifiedNativeAtoms(binary,path){
 const result=Bun.spawnSync([binary,'factor-atoms',path],{
   stdout:'pipe',stderr:'pipe',timeout:120000});
 ensure(result.exitCode===0,'native_atom_export_failed');
 const b=Buffer.from(result.stdout);
 ensure(b.length>=8&&b.length<=128*1024*1024&&
   b.toString('ascii',0,4)==='BFAT'&&b[4]===1&&
   b.subarray(5,8).every(x=>x===0),'native_atom_stream_invalid');
 const atoms=new Map();let i=8;
 while(i<b.length){
  ensure(i+68<=b.length,'native_atom_frame_short');
  const digest=b.toString('ascii',i,i+64),n=b.readUInt32BE(i+64);i+=68;
  ensure(hex.test(digest)&&n<=MAX&&i+n<=b.length&&!atoms.has(digest),'native_atom_frame_invalid');
  const raw=b.subarray(i,i+n);i+=n;
  ensure(sha(raw)===digest,'native_atom_digest_mismatch');atoms.set(digest,raw);
 }
 return atoms;
}
function rawAtom(atom,dictionary){
 const payload=encoded(atom.payload);
 ensure(Number.isSafeInteger(atom.raw_bytes)&&atom.raw_bytes>=0&&atom.raw_bytes<=MAX,'atom_size_invalid');
 ensure(payload.length<=MAX,'atom_payload_unbounded');
 let result;
 try{
  if(atom.codec===0)result=payload;
  else if(atom.codec===1)result=zstdDecompressSync(payload,{maxOutputLength:MAX});
  else if(atom.codec===2&&dictionary.length)result=zstdDecompressSync(payload,{dictionary,maxOutputLength:MAX});
  else ensure(false,'atom_codec_invalid');
 }catch(error){throw Error('dense_pack_atom_decompress_failed:'+String(error?.message||error))}
 ensure(result.length===atom.raw_bytes&&sha(result)===(atom.atom_digest??atom.digest),'atom_digest_mismatch');
 return result;
}
export function decodePage(bytes,{pageDigest,encodedDigest,codec=0,rawBytes,bucketBits,bucketId}={}){
 const stored=encoded(bytes);
 if(encodedDigest)ensure(sha(stored)===encodedDigest,'page_encoded_digest_mismatch');
 ensure(codec===0||codec===1,'page_codec_invalid');
 let p;
 try{p=codec===1?Buffer.from(zstdDecompressSync(stored,{maxOutputLength:MAX})):stored}
 catch{throw Error('dense_pack_page_decompress_failed')}
 if(rawBytes!==undefined)ensure(p.length===rawBytes,'page_raw_size_mismatch');
 ensure(p.length>=8&&p.length<=MAX&&p.toString('ascii',0,4)==='BFDP'&&p[4]===1,'page_header_invalid');
 if(pageDigest)ensure(sha(p)===pageDigest,'page_digest_mismatch');
 const bits=p[5],count=p.readUInt16BE(6);
 ensure(bits<=16&&count>0&&count<=4096,'page_count_invalid');
 if(bucketBits!==undefined)ensure(bits===bucketBits,'page_bits_mismatch');
 const out=[];let i=8,prev='';
 for(let n=0;n<count;n++){
  ensure(i+70<=p.length,'page_truncated');
  const key=p.subarray(i,i+32).toString('hex'),recordDigest=p.subarray(i+32,i+64).toString('hex');
  const rawBytes=p.readUInt32BE(i+64),partCount=p.readUInt16BE(i+68);i+=70;
  ensure(key>prev&&partCount>=1&&partCount<=4096&&i+partCount*32<=p.length,'page_unsorted_or_truncated');
  if(bucketId!==undefined)ensure(bucket(key,bits)===bucketId,'page_bucket_mismatch');
  const atoms=[];for(let j=0;j<partCount;j++,i+=32)atoms.push(p.subarray(i,i+32).toString('hex'));
  out.push({key_digest:key,record_digest:recordDigest,raw_bytes:rawBytes,atom_digests:atoms});prev=key;
 }
 ensure(i===p.length,'page_trailing_bytes');
 return out;
}
function encodePage(records,bits){
 const p=Buffer.allocUnsafe(8+records.reduce((sum,r)=>sum+70+r.atom_digests.length*32,0));
 p.write('BFDP',0,'ascii');p[4]=1;p[5]=bits;p.writeUInt16BE(records.length,6);
 let i=8;for(const r of records){b32(r.key_digest).copy(p,i);b32(r.record_digest).copy(p,i+32);
   p.writeUInt32BE(r.raw_bytes,i+64);p.writeUInt16BE(r.atom_digests.length,i+68);i+=70;
   for(const h of r.atom_digests){b32(h).copy(p,i);i+=32}}
 ensure(i===p.length&&p.length<=MAX,'page_size_invalid');return p;
}
async function shaFile(path){
 const st=statSync(path);ensure(st.isFile(),'source_file_invalid');
 const h=createHash('sha256');for await(const b of createReadStream(path))h.update(b);return{digest:h.digest('hex'),bytes:st.size};
}
export async function compileNativeDensePack({
 factorPath,sourcePath,compressBinary,verifiedClosure,family='record_factor',
 nativeOwner='BonfyreCompress',maxRecords=100000
}={}){
 ensure(typeof factorPath==='string'&&typeof sourcePath==='string'&&
   typeof compressBinary==='string'&&compressBinary.startsWith('/'),'native_verifier_required');
 ensure(['record_factor','address_frontier','pack_overlay','embedding_projection',
   'ghost_reference','pointer_pack','receipt_index'].includes(family),'family_invalid');
 ensure(nativeOwner==='BonfyreCompress','native_owner_invalid');
 ensure(Number.isSafeInteger(maxRecords)&&maxRecords>=1&&maxRecords<=100000,'record_limit_invalid');
 const scope=namespaceForNativeClosure(verifiedClosure);
 // Never accept an alleged verification boolean. Run the existing native
 // owner against the original source before exporting a single derived byte.
 const proof=Bun.spawnSync([compressBinary,'factor-verify',factorPath,sourcePath],
   {stdout:'pipe',stderr:'pipe',timeout:120000});
 ensure(proof.exitCode===0&&Buffer.from(proof.stdout).toString().includes('complete_source'),
   'native_source_verification_failed');
 const source=await shaFile(sourcePath),nativeRaw=verifiedNativeAtoms(compressBinary,factorPath);
 const db=openNativeFactor(factorPath);
 try{
  const dr=db.query('SELECT payload,digest FROM record_dictionary WHERE id=1').get();
  ensure(dr&&hex.test(dr.digest),'dictionary_invalid');
  const dictionary=encoded(dr.payload);
  ensure(dictionary.length<=MAX&&sha(dictionary)===dr.digest,'dictionary_digest_mismatch');
  const records=db.query('SELECT seq,record_key,digest,raw_bytes,parts FROM record ORDER BY seq').all();
  ensure(records.length>=1&&records.length<=maxRecords,'record_count_unbounded');
  const totalAtoms=db.query('SELECT count(*) AS n FROM record_atom').get().n;
  ensure(nativeRaw.size===totalAtoms,'native_atom_census_mismatch');
  const ap=db.query('SELECT p.atom AS atom_id,a.digest,a.raw_bytes,a.codec,a.payload,p.ordinal FROM record_part p JOIN record_atom a ON a.id=p.atom WHERE p.record=?1 ORDER BY p.ordinal');
  const atoms=new Map(),entries=[],sourceHasher=createHash('sha256');let sourceBytes=0;
  for(const record of records){
   ensure(hex.test(record.digest)&&Number.isSafeInteger(record.raw_bytes)&&record.raw_bytes>=0&&record.raw_bytes<=MAX,'record_invalid');
   const parts=ap.all(record.seq);
   ensure(parts.length===record.parts&&parts.length>=1&&parts.length<=4096,'parts_invalid');
   const pieces=[],refs=[];
   for(let n=0;n<parts.length;n++){
    const atom=parts[n];ensure(atom.ordinal===n&&hex.test(atom.digest),'atom_order_invalid');
     const value=nativeRaw.get(atom.digest);
     ensure(value&&value.length===atom.raw_bytes&&sha(value)===atom.digest,'native_atom_digest_mismatch');
     pieces.push(value);refs.push(atom.digest);
     if(!atoms.has(atom.digest)){
      const compressed=Buffer.from(Bun.zstdCompressSync(value,{level:10}));
      const keep=compressed.length<value.length;
      const item={atom_digest:atom.digest,codec:keep?1:0,dictionary_digest:null,
        raw_bytes:atom.raw_bytes,payload:keep?compressed:Buffer.from(value)};
      ensure(rawAtom({...item,digest:item.atom_digest},Buffer.alloc(0)).equals(value),
        'portable_atom_roundtrip_failed');
      atoms.set(atom.digest,item);
     }
   }
   const content=Buffer.concat(pieces);
   ensure(content.length===record.raw_bytes&&sha(content)===record.digest,'record_digest_mismatch');
   sourceHasher.update(content);sourceBytes+=content.length;
   entries.push({key_digest:keyDigest(record.record_key),record_digest:record.digest,
     raw_bytes:record.raw_bytes,atom_digests:refs});
  }
  ensure(sourceBytes===source.bytes&&sourceHasher.digest('hex')===source.digest,
    'source_bytes_mismatch');
  let bits=Math.max(0,Math.min(16,Math.ceil(Math.log2(records.length/128))));
  let pages;
  while(true){
   const grouped=new Map();
   for(const r of entries){const id=bucket(r.key_digest,bits),a=grouped.get(id)||[];
     a.push(r);grouped.set(id,a)}
   const attempt=[];let fits=true;
   for(const [id,group]of grouped){group.sort((a,b)=>a.key_digest.localeCompare(b.key_digest));
    const total=8+group.reduce((sum,x)=>sum+70+x.atom_digests.length*32,0);
    if(total>MAX||group.length>4096){fits=false;break}
    const body=encodePage(group,bits);
    const compressed=Buffer.from(Bun.zstdCompressSync(body,{level:10}));
    const codec=compressed.length<body.length?1:0,payload=codec?compressed:body;
    const page={bucket:id,record_count:group.length,raw_bytes:body.length,
      codec,page_digest:sha(body),encoded_digest:sha(payload),payload};
    ensure(decodePage(page.payload,{
      pageDigest:page.page_digest,encodedDigest:page.encoded_digest,
      codec:page.codec,rawBytes:page.raw_bytes,
      bucketBits:bits,bucketId:id
    }).length===group.length,'page_roundtrip_failed');
    attempt.push(page);
   }
   if(fits){pages=attempt.sort((a,b)=>a.bucket-b.bucket);break}
   ensure(bits<16,'bucket_exhausted');bits++;
  }
  const sourceReceipt=sha(Buffer.concat([Buffer.from('native-factor-verified-v1\0'),
    b32(source.digest),Buffer.from(proof.stdout),Buffer.from(proof.stderr)]));
  const sourceGeneration=sha(Buffer.from('native-factor-generation-v1\0'+source.digest));
  const passportDigest=sha(Buffer.concat([
    Buffer.from('bonfyre-dense-passport-v1\0'+family+'\0'),
    b32(scope),b32(source.digest),b32(sourceReceipt),b32(dr.digest)]));
  const basisDigest=sha(Buffer.from('bonfyre-record-factor-basis-v2\0'+dr.digest));
  const rootDigest=sha(Buffer.concat([
    Buffer.from('bonfyre-dense-root-v1\0'),b32(scope),b32(passportDigest),
    ...pages.map(p=>Buffer.concat([Buffer.from(Uint8Array.of(p.bucket>>8,p.bucket&255)),b32(p.page_digest),b32(p.encoded_digest)])),
    ...[...atoms.values()].sort((a,b)=>a.atom_digest.localeCompare(b.atom_digest))
      .map(a=>Buffer.concat([b32(a.atom_digest),b32(sha(a.payload))]))]));
  const atomBytes=[...atoms.values()].reduce((n,a)=>n+a.payload.length,0),pageBytes=pages.reduce((n,p)=>n+p.payload.length,0);
  return {schema:'bonfyre.dense-pack.v1',namespace_digest:scope,root_digest:rootDigest,
    passport:{passport_digest:passportDigest,native_owner:nativeOwner,subject_family:family,
     representation_family:'bonfyre_record_factor_zstd',version:'2',exactness_class:'exact',
     measurement_state:'measured',quality_loss_budget:0,basis_digest:basisDigest,
     binding_digest:null,exception_digest:null,lift_ref:'BonfyreCompress:factor-read',
     thaw_ref:null,source_generation_digest:sourceGeneration,source_receipt_digest:sourceReceipt},
    dictionary:{dictionary_digest:dr.digest,payload:dictionary},atoms:[...atoms.values()],
    root:{passport_digest:passportDigest,family,bucket_bits:bits,record_count:records.length,
     page_count:pages.length,source_bytes:source.bytes,native_verified_receipt:sourceReceipt},
    pages,metrics:{source_bytes:source.bytes,native_factor_file_bytes:statSync(factorPath).size,
     logical_payload_bytes:dictionary.length+atomBytes+pageBytes,
     atom_payload_bytes:atomBytes,pointer_page_bytes:pageBytes,dictionary_bytes:dictionary.length,
     records:records.length,distinct_atoms:atoms.size,pages:pages.length,bucket_bits:bits,
     source_to_logical_payload_ratio:source.bytes/(dictionary.length+atomBytes+pageBytes)}};
 }finally{db.close()}
}
export function hydrateDenseRecord(compiled,key){
 const searched=keyDigest(key),bits=compiled.root.bucket_bits,id=bucket(searched,bits);
 const page=compiled.pages.find(p=>p.bucket===id);
 if(!page)return null;
 const rows=decodePage(page.payload,{pageDigest:page.page_digest,encodedDigest:page.encoded_digest,
   codec:page.codec,rawBytes:page.raw_bytes,bucketBits:bits,bucketId:id});
 const row=rows.find(r=>r.key_digest===searched);
 if(!row)return null;
 const dict=compiled.dictionary.payload;
 const atoms=new Map(compiled.atoms.map(a=>[a.atom_digest,a]));
 const chunks=row.atom_digests.map(id=>{const a=atoms.get(id);ensure(a,'atom_missing');return rawAtom(a,dict)});
 const raw=Buffer.concat(chunks);
 ensure(raw.length===row.raw_bytes&&sha(raw)===row.record_digest,'lift_digest_mismatch');
 return raw;
}
const data=v=>typeof v==='string'?v:Buffer.from(v);
const rowHex=(digest,payload)=>({namespace_digest:wire(digest),payload:'\\x'+data(payload).toString('hex')});
export function postgrestDenseRows(compiled){
 ensure(compiled?.schema==='bonfyre.dense-pack.v1','projection_invalid');
 const ns=compiled.namespace_digest;
 return Object.freeze({
  dense_pack_dictionary:[{...rowHex(ns,compiled.dictionary.payload),dictionary_digest:wire(compiled.dictionary.dictionary_digest)}],
  dense_pack_atom:compiled.atoms.map(a=>({...rowHex(ns,a.payload),
    atom_digest:wire(a.atom_digest),dictionary_digest:a.dictionary_digest?wire(a.dictionary_digest):null,
    raw_bytes:a.raw_bytes,codec:a.codec})),
  dense_pack_passport:[{namespace_digest:wire(ns),...Object.fromEntries(Object.entries(compiled.passport).map(
    ([k,v])=>['passport_digest','basis_digest','binding_digest','exception_digest','source_generation_digest','source_receipt_digest'].includes(k)&&v? [k,wire(v)] : [k,v]))}],
  dense_pack_root:[{namespace_digest:wire(ns),root_digest:wire(compiled.root_digest),...Object.fromEntries(
    Object.entries(compiled.root).map(([k,v])=>['passport_digest','native_verified_receipt'].includes(k)?[k,wire(v)]:[k,v]))}],
  dense_pack_page:compiled.pages.map(p=>({...rowHex(ns,p.payload),
    root_digest:wire(compiled.root_digest),bucket:p.bucket,record_count:p.record_count,
    raw_bytes:p.raw_bytes,codec:p.codec,page_digest:wire(p.page_digest),encoded_digest:wire(p.encoded_digest)}))
 });
}
export async function publishDenseRows({compiled,baseUrl,serviceKey,request=fetch,dryRun=true}={}){
 const tables=postgrestDenseRows(compiled);
 if(dryRun)return{state:'DRY_RUN',root_digest:compiled.root_digest,metrics:compiled.metrics,external_write:false};
 ensure(typeof baseUrl==='string'&&/^https:\/\/[a-z0-9-]+\.supabase\.co$/.test(baseUrl)&&
   typeof serviceKey==='string'&&serviceKey.length>=20,'destination_unconfigured');
 const sent=[];
 for(const table of ['dense_pack_dictionary','dense_pack_atom','dense_pack_passport','dense_pack_root','dense_pack_page']){
  const rows=tables[table];
  for(let i=0;i<rows.length;i+=100){
   let response;
   try{
    response=await request(baseUrl+'/rest/v1/'+table,{
     method:'POST',redirect:'error',headers:{apikey:serviceKey,Authorization:'Bearer '+serviceKey,
      'Content-Type':'application/json',Prefer:'resolution=ignore-duplicates,return=minimal'},
     body:JSON.stringify(rows.slice(i,i+100)),signal:AbortSignal.timeout(30000)});
   }catch{throw Error('dense_pack_uncertain_write_readback_required')}
   if(!response.ok)throw Error('dense_pack_write_unconfirmed_'+table+'_'+response.status);
   sent.push({table,rows:Math.min(100,rows.length-i)});
  }
 }
 return{state:'POST_CONFIRMED_NOT_NATIVE_ACTIVATION',root_digest:compiled.root_digest,
  posted_batches:sent.length,external_write:true,
  note:'Read back and verify every page and atom; HTTP acceptance is not native materialization or application admission'};
}
// An internal service-owner reader, not a public Supabase client. BonfyreAuth
// must derive nativeClosure and the native owner must select rootDigest BEFORE
// calling this routine. A retrieved physical body remains a candidate only.
const fromBytea=(x)=>{
 ensure(typeof x==='string'&&/^\\x(?:[a-f0-9]{2})*$/i.test(x),'remote_bytea_invalid');
 return Buffer.from(x.slice(2),'hex');
};
async function denseRpc(name,args,{baseUrl,serviceKey,request}){
 ensure(typeof baseUrl==='string'&&/^https:\/\/[a-z0-9-]+\.supabase\.co$/.test(baseUrl)&&
   typeof serviceKey==='string'&&serviceKey.length>=20,'destination_unconfigured');
 let response;
 try{
  response=await request(baseUrl+'/rest/v1/rpc/'+name,{
   method:'POST',redirect:'error',headers:{apikey:serviceKey,
    Authorization:'Bearer '+serviceKey,'Content-Type':'application/json'},
   body:JSON.stringify(args),signal:AbortSignal.timeout(30000)});
 }catch{throw Error('dense_pack_remote_lookup_failed')}
 ensure(response?.ok,'remote_status_unconfirmed_'+String(response?.status??'unknown'));
 const length=Number(response.headers?.get?.('content-length')??0);
 ensure(Number.isFinite(length)&&length>=0&&length<=20*MAX,'remote_content_length_invalid');
 let rows;
 try{rows=await response.json()}catch{throw Error('dense_pack_remote_body_invalid')}
 ensure(Array.isArray(rows)&&rows.length<=16,'remote_rows_unbounded');
 return rows;
}
export async function readDenseRecordFromSupabase({
 nativeClosure,rootDigest,recordKey,baseUrl,serviceKey,request=fetch
}={}){
 const namespace=namespaceForNativeClosure(nativeClosure);
 const root=digest(rootDigest),key=keyDigest(recordKey),remote={baseUrl,serviceKey,request};
 const pages=await denseRpc('dense_pack_candidate_page_v2',{
   p_namespace:wire(namespace),p_root:wire(root),p_key_digest:wire(key)
 },remote);
 ensure(pages.length<=1,'remote_page_non_unique');
 if(!pages.length)return null;
 const page=pages[0],codec=Number(page.codec),rawBytes=Number(page.raw_bytes),
   bucketId=Number(page.bucket),count=Number(page.record_count);
 ensure(Number.isSafeInteger(codec)&&[0,1].includes(codec)&&
   Number.isSafeInteger(rawBytes)&&rawBytes>=8&&rawBytes<=MAX&&
   Number.isSafeInteger(bucketId)&&bucketId>=0&&bucketId<65536&&
   Number.isSafeInteger(count)&&count>=1&&count<=4096,'remote_page_contract_invalid');
 const pageDigest=fromBytea(page.page_digest).toString('hex'),
   encodedDigest=fromBytea(page.encoded_digest).toString('hex');
 ensure(hex.test(pageDigest)&&hex.test(encodedDigest),'remote_page_digest_invalid');
 const decoded=decodePage(fromBytea(page.payload),{
   pageDigest,encodedDigest,codec,rawBytes,bucketId
 });
 ensure(decoded.length===count,'remote_page_count_mismatch');
 const record=decoded.find(r=>r.key_digest===key);
 if(!record)return null;
 const ids=[...new Set(record.atom_digests)],atoms=new Map();
 ensure(ids.length>=1&&ids.length<=4096,'remote_atom_count_invalid');
 for(let i=0;i<ids.length;i+=16){
  const part=ids.slice(i,i+16);
  const rows=await denseRpc('dense_pack_atom_slice',{
   p_namespace:wire(namespace),p_digests:part.map(wire)
  },remote);
  const expected=new Set(part);
  for(const item of rows){
   const name=fromBytea(item.atom_digest).toString('hex');
   ensure(expected.has(name)&&!atoms.has(name),'remote_atom_unexpected');
   const codec=Number(item.codec),rawBytes=Number(item.raw_bytes);
   ensure([0,1].includes(codec)&&Number.isSafeInteger(rawBytes)&&
     rawBytes>=0&&rawBytes<=MAX&&item.dictionary_digest===null,
     'remote_atom_codec_unsupported');
   const payload=fromBytea(item.payload);
   ensure(payload.length<=MAX,'remote_atom_payload_unbounded');
   const decoded=rawAtom({codec,raw_bytes:rawBytes,atom_digest:name,payload},Buffer.alloc(0));
   atoms.set(name,decoded);
  }
 }
 ensure(atoms.size===ids.length,'remote_atom_missing');
 const raw=Buffer.concat(record.atom_digests.map(id=>atoms.get(id)));
 ensure(raw.length===record.raw_bytes&&sha(raw)===record.record_digest,
   'remote_record_digest_mismatch');
 return {state:'PHYSICAL_BYTES_VERIFIED_NOT_SOURCE_AUTHORITY',
   candidate_only:true,namespace_digest:namespace,root_digest:root,
   record_digest:record.record_digest,bytes:raw};
}
// A bounded read-through fallback. A failed / empty remote projection is not
// a missing native source. Never retry remote writes or infer provider health.
export async function readDenseWithNativeFallback({
 nativeClosure,rootDigest,recordKey,factorPath,sourcePath,compressBinary,
 baseUrl,serviceKey,request=fetch
}={}){
 namespaceForNativeClosure(nativeClosure);digest(rootDigest);keyDigest(recordKey);
 ensure(typeof factorPath==='string'&&factorPath.startsWith('/')&&
   typeof sourcePath==='string'&&sourcePath.startsWith('/')&&
   typeof compressBinary==='string'&&compressBinary.startsWith('/'),
   'native_fallback_unconfigured');
 let remoteStatus='not_attempted';
 try{
  const result=await readDenseRecordFromSupabase({nativeClosure,rootDigest,
    recordKey,baseUrl,serviceKey,request});
  if(result)return {...result,residency:'supabase'};
  remoteStatus='projection_absent';
 }catch(error){remoteStatus=error instanceof Error?error.message:'remote_unavailable'}
 const verified=Bun.spawnSync([compressBinary,'factor-verify',factorPath,sourcePath],
   {stdout:'pipe',stderr:'pipe',timeout:120000});
 ensure(verified.exitCode===0&&Buffer.from(verified.stdout).toString('utf8').includes('complete_source'),
   'native_fallback_source_verification_failed');
 const db=openNativeFactor(factorPath);
 let row;
 try{row=db.query('SELECT seq,digest,raw_bytes FROM record WHERE record_key=?').get(recordKey)}
 finally{db.close()}
 if(!row)return null;
 ensure(hex.test(row.digest)&&Number.isSafeInteger(row.raw_bytes)&&row.raw_bytes<=MAX,
   'native_fallback_record_invalid');
 const restored=Bun.spawnSync([compressBinary,'factor-read',factorPath,String(row.seq)],
   {stdout:'pipe',stderr:'pipe',timeout:30000});
 ensure(restored.exitCode===0,'native_fallback_read_failed');
 const bytes=Buffer.from(restored.stdout);
 ensure(bytes.length===row.raw_bytes&&sha(bytes)===row.digest,'native_fallback_digest_mismatch');
 return {state:'PHYSICAL_BYTES_VERIFIED_NOT_SOURCE_AUTHORITY',candidate_only:true,
   residency:'native',record_digest:row.digest,bytes,
   remote_status:remoteStatus};
}
