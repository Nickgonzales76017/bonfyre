import {test,expect} from 'bun:test';
import {prepareRecord,restoreRecord,digest,blobLimit,typedBlobThreshold} from '../workspace-value.mjs';
import {encode,decode} from '../workspace-msgpack.mjs';
async function saved(value){const blobs=new Map(),prepared=prepareRecord(value),packet=await prepared.encodeWith(async blob=>{const hash=await digest(await blob.arrayBuffer());blobs.set(hash,blob);return hash;});return {packet,blobs,prepared,restore:()=>restoreRecord(packet,async hash=>blobs.get(hash))};}
test('existing small typed values and files retain v2 byte representation',async()=>{
  const bytes=new Uint8Array([1,2,255]),result=await saved({bytes});expect(result.packet).toEqual(encode([2,{bytes},[]]));expect(result.blobs.size).toBe(0);expect((await result.restore()).bytes).toBeInstanceOf(Uint8Array);
  const old=await saved({file:new File(['file bytes'],'sample.bin',{type:'application/octet-stream',lastModified:123})}),packet=decode(old.packet);expect(packet[0]).toBe(2);expect(packet[2][0]).toHaveLength(6);const restored=await old.restore();expect(restored.file).toBeInstanceOf(File);expect(restored.file.name).toBe('sample.bin');expect(restored.file.lastModified).toBe(123);
});
test('v3 ArrayBuffer values retain their type and profile in mixed existing records',async()=>{
  const result=await saved({array:new Uint8Array([8,9]).buffer,file:new File(['x'],'x.bin',{lastModified:1}),inline:new Uint8Array([2])}),packet=decode(result.packet);expect(packet[0]).toBe(3);expect(packet[2].map(r=>r[6])).toEqual([1,0]);const value=await result.restore();expect(value.array).toBeInstanceOf(ArrayBuffer);expect(value.inline).toBeInstanceOf(Uint8Array);expect(value.file).toBeInstanceOf(File);expect([...new Uint8Array(value.array)]).toEqual([8,9]);
});
test('64 KiB typed views become v4 references while smaller views stay inline',async()=>{
  for(const size of [typedBlobThreshold-1,typedBlobThreshold]){const result=await saved(new Uint8Array(size).fill(47)),packet=decode(result.packet);expect(packet[0]).toBe(size<typedBlobThreshold?2:4);if(size>=typedBlobThreshold){expect(packet[1]).toBeNull();expect(packet[2][0][6]).toBe(2);expect(result.prepared.bytes).toBeLessThan(200);}const value=await result.restore();expect(value).toBeInstanceOf(Uint8Array);expect(value.byteLength).toBe(size);expect(value.at(-1)).toBe(47);}
});
test('large typed subarrays and Buffer views snapshot only their exact range before asynchronous encoding',async()=>{
  for(const backing of [new Uint8Array(typedBlobThreshold+200),Buffer.alloc(typedBlobThreshold+200)]){
    backing.fill(22);const view=backing.subarray(70,70+typedBlobThreshold);view.fill(61);const prepared=prepareRecord({view}),bodies=[];backing.fill(0);
    const packet=await prepared.encodeWith(async blob=>{bodies.push(blob);return digest(await blob.arrayBuffer());}),result=await restoreRecord(packet,async()=>bodies[0]);expect(result.view).toBeInstanceOf(Uint8Array);expect(result.view.byteLength).toBe(typedBlobThreshold);expect(result.view[0]).toBe(61);expect(result.view.at(-1)).toBe(61);expect(result.view.byteOffset).toBe(0);
  }
});
test('16 MiB typed database bytes round-trip with v4 while other asset types retain their type',async()=>{
  const bytes=new Uint8Array(16*1024*1024);for(let n=0;n<bytes.length;n++)bytes[n]=n%251;
  const result=await saved({bytes,buffer:new Uint8Array([1,2]).buffer,file:new File(['payload'],'file.dat',{lastModified:20}),inline:new Uint8Array([3,4])}),packet=decode(result.packet),restored=await result.restore();
  expect(packet[0]).toBe(4);expect(packet[2].map(r=>r[6])).toEqual([2,1,0]);expect(result.prepared.bytes).toBeLessThan(1000);expect(restored.bytes).toBeInstanceOf(Uint8Array);expect(await digest(restored.bytes)).toBe(await digest(bytes));expect(restored.buffer).toBeInstanceOf(ArrayBuffer);expect(restored.file).toBeInstanceOf(File);expect([...restored.inline]).toEqual([3,4]);
});
test('unknown version-kind combinations and binary metadata refuse before fetching an asset',async()=>{
  const hash='a'.repeat(64),ref=[['bytes'],hash,1,'',null,null,2];let reads=0;
  const invalid=[ [3,{bytes:null},[ref]],[2,{bytes:null},[ref]],[5,{bytes:null},[ref]], ...[3,-1,1.5,'2'].map(kind=>[4,{bytes:null},[[...ref.slice(0,6),kind]]]),...[['application/octet-stream',null,null],['','database.bin',1],['',null,1],['','database.bin',null]].map(([mime,name,modified])=>[4,{bytes:null},[[ref[0],hash,1,mime,name,modified,2]]]) ];
  for(const packet of invalid)await expect(restoreRecord(encode(packet),async()=>{reads++;return new Blob(['a']);})).rejects.toThrow();expect(reads).toBe(0);
});
test('native typed assets preserve existing per-value count and size bounds and refuse shared mutable memory',()=>{
  expect(()=>prepareRecord(new Uint8Array(blobLimit+1))).toThrow('24 MiB');expect(()=>prepareRecord(Array.from({length:65},()=>new Uint8Array(typedBlobThreshold)))).toThrow('24 MiB');
  for(const value of [new SharedArrayBuffer(16),new Uint8Array(new SharedArrayBuffer(16)),new Uint8Array(new SharedArrayBuffer(typedBlobThreshold))])expect(()=>prepareRecord({value})).toThrow('Shared mutable memory');
});
