import {test,expect} from 'bun:test';
import {checkBrandAsset,pngDimensions,studioMarkIcon} from '../training-brand-assets.mjs';
import {checkSettings,defaultSettings} from '../training-settings.mjs';
const png='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7i8AAAAASUVORK5CYII=';
const asset=()=>({dataUrl:'data:image/png;base64,'+png,width:1,height:1});
test('embedded brand assets survive settings validation without altering another studio',()=>{
  const first=defaultSettings(),second=defaultSettings();first.brand.logo=asset();first.brand.icon=asset();
  const saved=checkSettings(JSON.parse(JSON.stringify(first)));expect(saved.brand.logo).toEqual(asset());expect(second.brand.logo).toBeUndefined();
  first.brand.logo=null;expect(checkSettings(first).brand.logo).toBeNull();expect(first.brand.icon).toEqual(asset());
});
test('external URLs, executable formats, extra fields and mismatched dimensions are refused',()=>{
  for(const value of [{...asset(),dataUrl:'https://remote.example/logo.png'},{...asset(),dataUrl:'data:image/svg+xml,<svg onload="alert(1)"/>'},{...asset(),dataUrl:'javascript:alert(1)'},{...asset(),width:2},{...asset(),onload:'alert(1)'},{...asset(),dataUrl:'data:image/png;base64,AAAA'}])expect(()=>checkBrandAsset(value)).toThrow();
});
test('pixel and encoded-byte limits are enforced before image decoding',()=>{
  const bytes=Uint8Array.from(atob(png),c=>c.charCodeAt(0));expect(pngDimensions(bytes)).toEqual({width:1,height:1});
  new DataView(bytes.buffer).setUint32(16,100000);expect(()=>pngDimensions(bytes)).toThrow('2');
  expect(()=>checkBrandAsset({...asset(),dataUrl:'data:image/png;base64,'+'A'.repeat(700000)})).toThrow();
});
test('fallback icons use only the selected studio colors',()=>{
  const b=defaultSettings().brand,other={...b,accent:'#eead77',ground:'#ffffff'};
  expect(studioMarkIcon(b)).not.toBe(studioMarkIcon(other));expect(decodeURIComponent(studioMarkIcon(other))).toContain('#eead77');
  expect(()=>studioMarkIcon({...b,accent:'"><script>'})).toThrow();
});
