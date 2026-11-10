/** Pure presentation operations. Product, price, plan and authority stay with their owners. */
export const FEET_TO_METRES = 0.3048;
export const LIMITS = Object.freeze({bytes: 24*1024*1024, files: 48, nodes: 2048, primitives: 2048, vertices: 1_000_000, materials: 128, textures: 32, depth: 96});
const fail = message => { throw new Error(message); };
const integer = (n, lo, hi) => Number.isSafeInteger(n) && n >= lo && n <= hi;
export function safeAssetPath(value) {
  if (typeof value !== 'string' || !value || value.length > 240) fail('Invalid local asset filename.');
  let name; try {name = decodeURIComponent(value);} catch {fail('Malformed asset path.');}
  if (/[\\\u0000-\u001f\u007f:#?]/.test(name) || name.startsWith('/')) fail('Only selected local model files may be read.');
  const parts = name.replace(/^\.\//,'').split('/');
  if (parts.some(p => !p || p === '.' || p === '..')) fail('Asset paths cannot traverse directories.');
  return parts.join('/');
}
function count(array, name, max) {
  if (array !== undefined && (!Array.isArray(array) || array.length > max)) fail(`The model exceeds the ${name} limit.`);
  return array ?? [];
}
export function inspectGLTF(document, {available = new Set(), binaryBytes = 0, decoders = new Set(), basePath=''} = {}) {
  if (!document || typeof document !== 'object' || document.asset?.version !== '2.0') fail('Use a glTF 2.0 model.');
  const nodes = count(document.nodes,'scene-node',LIMITS.nodes), meshes=count(document.meshes,'mesh',LIMITS.nodes);
  count(document.materials,'material',LIMITS.materials);count(document.textures,'texture',LIMITS.textures);
  const buffers=count(document.buffers,'buffer',LIMITS.files), views=count(document.bufferViews,'buffer-view',8192);
  const accessors=count(document.accessors,'accessor',8192), images=count(document.images,'image',LIMITS.textures);
  const supported = new Set(['KHR_materials_unlit','KHR_materials_clearcoat','KHR_materials_transmission','KHR_materials_ior','KHR_materials_specular','KHR_materials_sheen','KHR_materials_volume','KHR_materials_emissive_strength','KHR_materials_iridescence','KHR_materials_anisotropy','KHR_materials_variants','KHR_texture_transform','KHR_mesh_quantization','KHR_lights_punctual']);
  for (const e of document.extensionsRequired ?? []) if (!supported.has(e) && !decoders.has(e)) fail(`Required model extension is not installed: ${String(e).slice(0,100)}`);
  const references = new Set();
  const reference = uri => {const key=safeAssetPath(basePath+safeAssetPath(uri));if (!available.has(key)) fail(`Select the model's companion file: ${key}`);references.add(key);};
  let declaredBytes=0;
  for (const [i,b] of buffers.entries()) {
    if (!integer(b.byteLength,1,LIMITS.bytes)) fail('Invalid model buffer length.');
    declaredBytes += b.byteLength;
    if (declaredBytes > LIMITS.bytes) fail('Decoded buffers exceed the asset budget.');
    if (b.uri !== undefined) reference(b.uri); else if (i !== 0 || b.byteLength > binaryBytes) {if(!(decoders.has('EXT_meshopt_compression')&&b.extensions?.EXT_meshopt_compression?.fallback===true))fail('Model binary buffer is missing or truncated.');}
  }
  for (const v of views) if (!integer(v.buffer,0,buffers.length-1)||!integer(v.byteOffset??0,0,LIMITS.bytes)||!integer(v.byteLength,1,LIMITS.bytes)||(v.byteOffset??0)+v.byteLength>buffers[v.buffer].byteLength) fail('Invalid model buffer view.');
  for(const view of views){const c=view.extensions?.EXT_meshopt_compression;if(!c)continue;if(!integer(c.buffer,0,buffers.length-1)||!integer(c.byteOffset??0,0,LIMITS.bytes)||!integer(c.byteLength,1,LIMITS.bytes)||(c.byteOffset??0)+c.byteLength>buffers[c.buffer].byteLength||!integer(c.byteStride,1,256)||!integer(c.count,1,LIMITS.vertices*3)||c.count*c.byteStride>LIMITS.bytes||c.count*c.byteStride!==view.byteLength||!['ATTRIBUTES','TRIANGLES','INDICES'].includes(c.mode))fail('Invalid meshopt expansion bounds.');}
  for(const a of accessors){const components={SCALAR:1,VEC2:2,VEC3:3,VEC4:4,MAT2:4,MAT3:9,MAT4:16}[a.type],bytes={5120:1,5121:1,5122:2,5123:2,5125:4,5126:4}[a.componentType];if(!components||!bytes||!integer(a.count,1,LIMITS.vertices*3)||a.count*components*bytes>LIMITS.bytes)fail('Accessor exceeds the decoded memory bound.');}
  for (const image of images) {
    if (image.uri !== undefined) reference(image.uri);
    else if (!integer(image.bufferView,0,views.length-1)||!['image/png','image/jpeg','image/webp','image/ktx2'].includes(image.mimeType)) fail('Model image is missing or unsupported.');
  }
  let primitives=0,vertices=0;
  for (const mesh of meshes) {
    if (!Array.isArray(mesh.primitives)) fail('Model mesh has no primitives.');
    for (const p of mesh.primitives) {
      if (++primitives>LIMITS.primitives) fail('Too many mesh primitives.');
      const a=accessors[p.attributes?.POSITION];
      if (!a||a.type!=='VEC3'||!integer(a.count,1,LIMITS.vertices)) fail('Invalid model position accessor.');
      if ((vertices+=a.count)>LIMITS.vertices) fail('The model exceeds the vertex budget.');
      for (const v of [...(a.min??[]),...(a.max??[])]) if (!Number.isFinite(v)) fail('Nonfinite model bounds.');
      for(const field of ['min','max'])if(a[field]!==undefined&&(!Array.isArray(a[field])||a[field].length!==3))fail('Invalid model bounds.');
    }
  }
  if(!primitives)fail('The model contains no mesh primitives.');
  const parents=new Map();
  for(const [i,n] of nodes.entries()) {
    for (const field of ['translation','scale','rotation','matrix']) if(n[field]!==undefined&&(!Array.isArray(n[field])||n[field].length!==({translation:3,scale:3,rotation:4,matrix:16})[field]||n[field].some(x=>!Number.isFinite(x)||Math.abs(x)>1e9)))fail('Invalid model transform.');
    if(n.mesh!==undefined&&!integer(n.mesh,0,meshes.length-1))fail('Model node references an absent mesh.');
    for (const child of n.children??[]) {
      if(!integer(child,0,nodes.length-1)||parents.has(child))fail('Model hierarchy has an invalid or repeated parent.');
      parents.set(child,i);
    }
  }
  for(let i=0;i<nodes.length;i++) {let p=i,seen=new Set();while(parents.has(p)){if(seen.has(p)||seen.size>LIMITS.depth)fail('Model hierarchy is cyclic or too deep.');seen.add(p);p=parents.get(p);}}
  // Recursive scan prevents extension-specific URI paths from escaping the same boundary.
  const pending=[[document,0]];let visits=0;
  while(pending.length){const [value,depth]=pending.pop();if(++visits>200000||depth>LIMITS.depth)fail('Model metadata is too complex.');if(!value||typeof value!=='object')continue;for(const [key,v] of Object.entries(value)){if(key==='uri')reference(v);else if(v&&typeof v==='object')pending.push([v,depth+1]);}}
  return Object.freeze({vertices,primitives,nodes:nodes.length,materials:document.materials?.length??0,references:[...references],declaredBytes});
}
export function readGLB(bytes, options={}) {
  if (!(bytes instanceof ArrayBuffer)||bytes.byteLength<20||bytes.byteLength>LIMITS.bytes) fail('Choose a GLB between 20 bytes and 24 MiB.');
  const v=new DataView(bytes);
  if(v.getUint32(0,true)!==0x46546c67||v.getUint32(4,true)!==2||v.getUint32(8,true)!==bytes.byteLength)fail('Invalid GLB header or total length.');
  let at=12,document=null,binaryBytes=0,chunk=0,metadataSeen=false,binarySeen=false;
  while(at<bytes.byteLength){if(at+8>bytes.byteLength)fail('Truncated GLB chunk.');const length=v.getUint32(at,true),type=v.getUint32(at+4,true);at+=8;if(length%4||at+length>bytes.byteLength)fail('Invalid GLB chunk length.');
    if(chunk++===0&&type!==0x4e4f534a)fail('The first GLB chunk must contain glTF metadata.');
    if(type===0x4e4f534a){if(metadataSeen)fail('Duplicate GLB metadata chunk.');metadataSeen=true;try{document=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(new Uint8Array(bytes,at,length)));}catch{fail('Invalid glTF metadata.');}}
    else if(type===0x004e4942){if(binarySeen)fail('Duplicate binary chunk.');binarySeen=true;binaryBytes=length;}else fail('Unsupported GLB chunk.');at+=length;
  }
  return {document,summary:inspectGLTF(document,{...options,binaryBytes})};
}
export async function prepareAsset(files,{decoders=new Set()}={}) {
  if(!Array.isArray(files)||!files.length||files.length>LIMITS.files)fail('Select one model and its local companion files.');
  const entries=new Map();let size=0;
  for(const f of files){if(!(f instanceof Blob)||!f.name)fail('Expected selected local files.');size+=f.size;if(size>LIMITS.bytes)fail('Selected model files exceed 24 MiB.');const name=safeAssetPath(f.webkitRelativePath||f.name);if(entries.has(name))fail('Two selected files have the same path.');entries.set(name,f);}
  const models=[...entries.keys()].filter(k=>/\.(gltf|glb)$/i.test(k));if(models.length!==1)fail('Select exactly one GLB or glTF model.');
  const name=models[0],bytes=await entries.get(name).arrayBuffer(),basePath=name.includes('/')?name.slice(0,name.lastIndexOf('/')+1):'';let document,summary;
  if(/\.glb$/i.test(name))({document,summary}=readGLB(bytes,{available:new Set(entries.keys()),decoders,basePath}));
  else{try{document=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{fail('Invalid glTF metadata.');}summary=inspectGLTF(document,{available:new Set(entries.keys()),decoders,basePath});}
  const hashes=[];
  for(const [path,blob] of [...entries].sort(([a],[b])=>a.localeCompare(b))){const digest=await crypto.subtle.digest('SHA-256',await blob.arrayBuffer());hashes.push(`${path.length}:${path}:${[...new Uint8Array(digest)].map(x=>x.toString(16).padStart(2,'0')).join('')}`);}
  const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(hashes.join('\n')));
  return {name,bytes,document,summary,files:[...entries].map(([name,blob])=>({name,blob})),digest:[...new Uint8Array(digest)].map(x=>x.toString(16).padStart(2,'0')).join(''),size};
}
export function sourcePlacement(item, product, sizeMetres) {
  if(![item.x,item.y].every(n=>Number.isFinite(n)&&n>=0)||![0,90].includes(item.rotation))fail('Invalid fixture coordinates.');
  if(!sizeMetres||!['x','y','z'].every(k=>Number.isFinite(sizeMetres[k])&&sizeMetres[k]>=0)||Math.max(sizeMetres.x,sizeMetres.y,sizeMetres.z)<=0)fail('Invalid model size.');
  const rotated=item.rotation===90,w=(rotated?sizeMetres.z:sizeMetres.x),d=(rotated?sizeMetres.x:sizeMetres.z);
  // World X/Z in metres; domain X/Y retain quarter-foot coordinates. Height is not invented.
  const listed=product.dimensions?.width&&product.dimensions?.depth?{w:(rotated?product.dimensions.depth:product.dimensions.width)*.0254,d:(rotated?product.dimensions.width:product.dimensions.depth)*.0254}:null;
  return {x:item.x*FEET_TO_METRES+w/2,z:item.y*FEET_TO_METRES+d/2,y:0,rotationY:-item.rotation*Math.PI/180,w,d,listed,mismatch:listed?Math.max(Math.abs(w-listed.w)/listed.w,Math.abs(d-listed.d)/listed.d)>.05:null};
}
export function movedFixture(plan,key,{x,z},sizeMetres) {
  const item=plan.items.find(i=>i.key===key);if(!item)fail('Selected fixture is no longer in this plan.');
  if(![x,z].every(Number.isFinite))fail('Invalid 3D move.');
  const rotated=item.rotation===90,w=rotated?sizeMetres.z:sizeMetres.x,d=rotated?sizeMetres.x:sizeMetres.z;
  const next=structuredClone(plan),target=next.items.find(i=>i.key===key);
  target.x=Math.max(0,Math.min(60,Math.round((x-w/2)/FEET_TO_METRES*4)/4));
  target.y=Math.max(0,Math.min(60,Math.round((z-d/2)/FEET_TO_METRES*4)/4));
  return next;
}
export function rotateFixture(plan,key) {const next=structuredClone(plan),item=next.items.find(i=>i.key===key);if(!item)fail('Select an existing fixture.');item.rotation=item.rotation===90?0:90;return next;}
export function fitReview(product,size) {
  const listed=product.dimensions??{},axes=[['width','x'],['height','y'],['depth','z']];
  return axes.map(([label,axis])=>({label,modelMetres:size[axis],listedMetres:Number.isFinite(listed[label])?listed[label]*.0254:null,agreement:Number.isFinite(listed[label])?Math.abs(size[axis]-listed[label]*.0254)<=Math.max(.003,listed[label]*.0254*.05):null}));
}
/** Cache references never union source scopes; rejection is evicted so retry is possible. */
export class ScopedAssetCache {
  #entries=new Map();
  async acquire(scope,digest,load) {if(!scope||!/^[a-f0-9]{64}$/.test(digest))fail('An asset needs a source scope and digest.');const key=`${scope.length}:${scope}:${digest}`;let row=this.#entries.get(key);if(!row){row={refs:0,promise:Promise.resolve().then(load)};this.#entries.set(key,row);row.promise.catch(()=>{if(this.#entries.get(key)===row)this.#entries.delete(key);});}row.refs++;try{return await row.promise;}catch(error){row.refs--;throw error;}}
  release(scope,digest,dispose) {const key=`${scope.length}:${scope}:${digest}`,row=this.#entries.get(key);if(!row)return;row.refs--;if(row.refs<=0){this.#entries.delete(key);row.promise.then(dispose,()=>{});}}
  get size(){return this.#entries.size;}
}

/** Resolve only an explicit source-bound model/merchant mapping, never a color-name guess. */
export function resolveCommercialVariant(product, modelDigest, modelVariant, bindings=[]) {
  const rows=bindings.filter(b=>b.productId===product.id&&b.store===product.store&&b.sourceDigest===product.source_sha256&&b.modelDigest===modelDigest&&b.modelVariant===modelVariant);
  if(!rows.length)return null;
  if(rows.length!==1)throw Error('The model variant has an ambiguous merchant-option mapping.');
  const option=product.variants?.find(v=>v.id===rows[0].variantId);
  if(!option)throw Error('The mapped merchant option is absent from the retained source.');
  return {productId:product.id,store:product.store,sourceDigest:product.source_sha256,variantId:option.id,sku:option.sku??'',listedPrice:option.price,modelDigest,modelVariant};
}
