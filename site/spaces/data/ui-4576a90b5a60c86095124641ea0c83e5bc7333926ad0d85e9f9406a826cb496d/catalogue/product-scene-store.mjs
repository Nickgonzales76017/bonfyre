import {prepareAsset} from './product-scene-model.mjs';
/** Adapter over the existing revision-checked browser workbench. No new IndexedDB database. */
export function productAssetStore({scope,readLocal,saveLocal,decoders=new Set()}) {
  if(!/^[a-f0-9]{64}$/.test(scope)||typeof readLocal!=='function'||typeof saveLocal!=='function')throw Error('Use the current catalogue scope and existing browser store.');
  const prefix=`product-model:${scope}:`,bodyKey=digest=>`${prefix}body:${digest}`;
  const bindingKey=id=>{if(typeof id!=='string'||!id||id.length>160)throw Error('Invalid source product reference.');return `${prefix}binding:${encodeURIComponent(id)}`;};
  const exact=async key=>(await readLocal(key)).find(row=>row.key===key);
  return {
    async save(productId,asset,expectedRevision,{unit='metres',variant=null}={}) {
      if(!['metres','centimetres','millimetres'].includes(unit)||variant!==null&&(typeof variant!=='string'||variant.length>160))throw Error('Invalid model presentation settings.');
      const checked=await prepareAsset(asset.files.map(f=>new File([f.blob],f.name)),{decoders});
      if(checked.digest!==asset.digest)throw Error('Source asset changed before saving.');
      const body=bodyKey(checked.digest),previous=await exact(body);
      if(!previous){try{await saveLocal(body,{files:checked.files},0);}catch(error){if(!await exact(body))throw error;}}
      // Retain source bytes first. A rejected head save cannot create a dangling binding.
      return saveLocal(bindingKey(productId),{scope,productId,digest:checked.digest,name:checked.name,unit,variant},expectedRevision);
    },
    async read(productId) {
      const row=await exact(bindingKey(productId));if(!row)return null;
      const binding=row.value;
      if(binding.scope!==scope||binding.productId!==productId||!/^[a-f0-9]{64}$/.test(binding.digest)||!['metres','centimetres','millimetres'].includes(binding.unit))throw Error('Saved model binding is invalid.');
      const body=await exact(bodyKey(binding.digest));if(!body?.value?.files)throw Error('The saved model bytes are unavailable. Reimport the original file.');
      const asset=await prepareAsset(body.value.files.map(f=>new File([f.blob],f.name)),{decoders});
      if(asset.digest!==binding.digest)throw Error('Saved model bytes failed their digest check.');
      return {asset,binding,revision:row.revision};
    }
  };
}
