const signature=[137,80,78,71,13,10,26,10];
export function studioMarkIcon(brand){
  if(![brand.accent,brand.ground].every(c=>/^#[0-9a-f]{6}$/i.test(c)))throw Error('Invalid studio colors.');
  return 'data:image/svg+xml,'+encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="${brand.ground}"/><path d="M18 46L46 18M22 18h24v24" fill="none" stroke="${brand.accent}" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"/></svg>`);
}
export function pngDimensions(bytes,maxSide=2048){
  if(!(bytes instanceof Uint8Array)||bytes.length<33||signature.some((v,i)=>bytes[i]!==v))throw Error('Choose a PNG image.');
  const view=new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength);
  if(view.getUint32(8)!==13||String.fromCharCode(...bytes.slice(12,16))!=='IHDR')throw Error('The PNG header is invalid.');
  const width=view.getUint32(16),height=view.getUint32(20);
  if(!width||!height||width>maxSide||height>maxSide)throw Error(`Choose an image no larger than ${maxSide} × ${maxSide} pixels.`);
  return {width,height};
}
export function checkBrandAsset(asset){
  if(asset==null)return asset;
  if(typeof asset!=='object'||Array.isArray(asset)||Object.keys(asset).some(k=>!['dataUrl','width','height'].includes(k)))throw Error('Invalid brand image.');
  if(typeof asset.dataUrl!=='string'||asset.dataUrl.length>700000||!/^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/.test(asset.dataUrl))throw Error('Brand images must be embedded PNG files under 512 KB.');
  const binary=atob(asset.dataUrl.slice(22));if(binary.length>512*1024)throw Error('Brand image exceeds 512 KB.');
  const dims=pngDimensions(Uint8Array.from(binary,c=>c.charCodeAt(0)),512);
  if(asset.width!==dims.width||asset.height!==dims.height)throw Error('Brand image dimensions do not match.');
  return asset;
}
export async function importBrandAsset(file,kind){
  if(!['logo','icon'].includes(kind))throw Error('Unknown brand image.');
  if(file.size>2*1024*1024)throw Error('Choose a PNG file under 2 MB.');
  const bytes=new Uint8Array(await file.arrayBuffer());pngDimensions(bytes);
  const bitmap=await createImageBitmap(new Blob([bytes],{type:'image/png'}));
  try{
    const limit=kind==='icon'?64:512,scale=Math.min(1,limit/Math.max(bitmap.width,bitmap.height));
    const canvas=document.createElement('canvas');canvas.width=Math.max(1,Math.round(bitmap.width*scale));canvas.height=Math.max(1,Math.round(bitmap.height*scale));
    canvas.getContext('2d').drawImage(bitmap,0,0,canvas.width,canvas.height);
    return checkBrandAsset({dataUrl:canvas.toDataURL('image/png'),width:canvas.width,height:canvas.height});
  }finally{bitmap.close();}
}
const esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
export function assetPreview(asset,kind){return asset?`<img src="${esc(asset.dataUrl)}" alt="${kind==='logo'?'Studio logo':'Browser icon'} preview"><button type="button" class="text-button" data-action="brand.remove" data-kind="${kind}">Remove</button>`:'<span class="muted">Using the studio mark</span>';}
export function brandAssetControls(brand,{hint='Save settings to apply.'}={}){return `<div class="brand-assets">${['logo','icon'].map(kind=>`<div><h3>${kind==='logo'?'Studio logo':'Browser icon'}</h3><div class="brand-asset-preview" id="brand-${kind}-preview">${assetPreview(brand[kind],kind)}</div><label class="button secondary">Choose PNG<input type="file" data-brand-asset="${kind}" accept="image/png" hidden></label></div>`).join('')}</div><p class="muted">PNG, up to 2 MB and 2,048 pixels per side. ${esc(hint)}</p>`;}
