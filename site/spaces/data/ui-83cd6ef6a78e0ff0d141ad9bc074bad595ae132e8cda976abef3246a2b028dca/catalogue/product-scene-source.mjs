import {LIMITS,prepareAsset} from './product-scene-model.mjs';
/** Publisher-admitted asset. A public URL never establishes its product binding or licence. */
export async function publishedModelFiles(descriptor,{fetcher=fetch,origin=location.origin,signal}={}) {
 if(!descriptor||typeof descriptor.name!=='string'||!descriptor.name.endsWith('.glb')||!/^[a-f0-9]{64}$/.test(descriptor.sha256)||!descriptor.credit)throw Error('The published model needs its exact name, hash and credit.');
 const url=new URL(descriptor.path,origin);
 if(url.origin!==origin||!url.pathname.startsWith('/bonfyre/spaces/assets/product-models/')||url.search||url.hash)throw Error('Model must be in the declared local asset package.');
 const response=await fetcher(url,{redirect:'error',signal});if(!response.ok)throw Error('The model asset is unavailable.');
 const reader=response.body.getReader(),chunks=[];let count=0;
 try{for(;;){const {done,value}=await reader.read();if(done)break;count+=value.byteLength;if(count>LIMITS.bytes)throw Error('The model exceeds the input bound.');chunks.push(value);}}catch(error){await reader.cancel().catch(()=>{});throw error;}
 const blob=new Blob(chunks),digest=[...new Uint8Array(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer()))].map(b=>b.toString(16).padStart(2,'0')).join('');
 if(digest!==descriptor.sha256)throw Error('The published model changed. It has not been bound to this product.');
 return [new File([blob],descriptor.name,{type:'model/gltf-binary'})];
}
