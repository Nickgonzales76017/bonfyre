/** Exact, self-hosted renderer and decoder release. Nothing is fetched from a third-party CDN. */
export const SCENE_EXTENSIONS = new Set(['KHR_draco_mesh_compression','EXT_meshopt_compression','KHR_texture_basisu','EXT_texture_webp']);
export async function loadProductSceneEngine() {
 const base=new URL('/bonfyre/spaces/assets/product-scene/three-0.180.0/',globalThis.location?.href??import.meta.url),load=path=>import(new URL(path,base).href);
 const [THREE,{OrbitControls},{TransformControls},{GLTFLoader},{clone},{RoomEnvironment},{DRACOLoader},{KTX2Loader},{MeshoptDecoder}]=await Promise.all([
  load('build/three.module.js'),load('examples/jsm/controls/OrbitControls.js'),load('examples/jsm/controls/TransformControls.js'),
  load('examples/jsm/loaders/GLTFLoader.js'),load('examples/jsm/utils/SkeletonUtils.js'),load('examples/jsm/environments/RoomEnvironment.js'),
  load('examples/jsm/loaders/DRACOLoader.js'),load('examples/jsm/loaders/KTX2Loader.js'),load('examples/jsm/libs/meshopt_decoder.module.js')]);
 if(THREE.REVISION!=='180')throw Error('The renderer does not match the reviewed release.');
 let draco=null,ktx=null;
 return {THREE,OrbitControls,TransformControls,GLTFLoader,clone,RoomEnvironment,extensions:SCENE_EXTENSIONS,
  configureLoader(loader,renderer){
   // Decoder resource requests use a separate trusted local manager. Asset URIs still
   // resolve only against the files explicitly admitted by the product asset boundary.
   draco??=new DRACOLoader().setDecoderPath(new URL('examples/jsm/libs/draco/gltf/',base).href).setWorkerLimit(2);
   ktx??=new KTX2Loader().setTranscoderPath(new URL('examples/jsm/libs/basis/',base).href).setWorkerLimit(2).detectSupport(renderer);
   loader.setDRACOLoader(draco).setKTX2Loader(ktx).setMeshoptDecoder(MeshoptDecoder);
  },
  dispose(){draco?.dispose();ktx?.dispose();draco=null;ktx=null;}
 };
}
