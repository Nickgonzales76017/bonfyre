import {checkBrandAsset} from './training-brand-assets.mjs';
export const defaultSettings = () => ({schema:'bonfyre.training-settings.v1', brand:{name:'Training',accent:'#b5df32',ground:'#0d130f',ink:'#dde6da',font:'sans',density:'comfortable'},bridges:[],model:{provider:'none',model:'',endpoint:'',data:'program-only'}});
const plain = (s,max=160) => typeof s === 'string' && s.length <= max && !/[\x00-\x1f]/.test(s);
function luminance(hex) {
  const rgb=[1,3,5].map(i=>parseInt(hex.slice(i,i+2),16)/255).map(v=>v<=0.04045?v/12.92:((v+0.055)/1.055)**2.4);
  return rgb[0]*0.2126+rgb[1]*0.7152+rgb[2]*0.0722;
}
function contrast(a,b){const x=luminance(a),y=luminance(b);return (Math.max(x,y)+0.05)/(Math.min(x,y)+0.05);}
export function brandColors(brand){return {scheme:contrast(brand.ground,'#000000')>=contrast(brand.ground,'#ffffff')?'light':'dark',accentText:contrast(brand.accent,brand.ground)>=4.5?brand.accent:brand.ink,onAccent:contrast(brand.accent,'#000000')>=contrast(brand.accent,'#ffffff')?'#000000':'#ffffff'};}
function endpoint(value, local=false) {
  const url=new URL(value);
  if(url.username || url.password || url.search || url.hash || !(url.protocol==='https:' || (local && url.protocol==='http:' && ['localhost','127.0.0.1','[::1]'].includes(url.hostname)))) throw Error('Use an HTTPS endpoint, or a loopback address for a local model.');
  return value;
}
export function checkSettings(settings) {
  if(settings?.schema!=='bonfyre.training-settings.v1') throw Error('Unsupported settings.');
  const b=settings.brand;
  if(!b || !plain(b.name,80) || !b.name.trim()) throw Error('Enter a brand name.');
  for(const key of ['accent','ground','ink']) if(!/^#[0-9a-f]{6}$/i.test(b[key])) throw Error('Choose a six-digit color.');
  if(contrast(b.ink,b.ground)<4.5) throw Error('Choose text and background colors with more contrast.');
  if(!['sans','serif','mono'].includes(b.font) || !['compact','comfortable'].includes(b.density)) throw Error('Choose a supported style.');
  checkBrandAsset(b.logo);checkBrandAsset(b.icon);
  const m=settings.model;
  if(!m || !['none','local','openai-compatible','provider-router'].includes(m.provider) || !['program-only','selected-session'].includes(m.data) || !plain(m.model) || !plain(m.endpoint,2048)) throw Error('Model settings are invalid.');
  if(Object.keys(m).some(k=>!['provider','model','endpoint','data'].includes(k))) throw Error('Store model credentials in the provider owner.');
  if(m.provider!=='none') {if(!m.model.trim()) throw Error('Enter a model ID.'); if(m.provider!=='provider-router') endpoint(m.endpoint,m.provider==='local');}
  if(!Array.isArray(settings.bridges) || settings.bridges.length>20) throw Error('Too many connections.');
  const ids=new Set();
  for(const bridge of settings.bridges) {
    if(!plain(bridge.id,80)||!bridge.id||ids.has(bridge.id)) throw Error('Connection IDs must be unique.'); ids.add(bridge.id);
    if(!['wordpress','frappe','generic'].includes(bridge.kind) || !['programs','requests'].includes(bridge.scope) || !plain(bridge.name,80)||!bridge.name.trim()) throw Error('Connection settings are invalid.');
    endpoint(bridge.endpoint);
    if(bridge.direction!=='export') throw Error('This release supports reviewed exports.');
    if(Object.keys(bridge).some(k=>!['id','name','kind','scope','endpoint','direction'].includes(k))) throw Error('Store credentials in the provider owner.');
  }
  return settings;
}
// Export is explicit, bounded to program material, and never reads member history.
export function exportPrograms(routines, bridge) {
  if(bridge.scope!=='programs') throw Error('Choose a program connection.');
  const programs=routines.map(r=>({id:r.id,title:r.name,source:r.source??null,exercises:r.exercises.map(e=>({id:e.id,name:e.name,kind:e.kind,restSeconds:e.restSeconds,superset:e.superset,sets:e.sets.map(({done,...s})=>s)}))}));
  if(bridge.kind==='frappe') return JSON.stringify(programs.map(p=>({doctype:'Website Article',title:p.title,route:`programs/${encodeURIComponent(p.id)}`,published:0,content:JSON.stringify(p,null,2)})),null,2);
  if(bridge.kind==='wordpress') {
    const xml=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&apos;');
    return `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:wp="http://wordpress.org/export/1.2/"><channel><title>Training programs</title><link>${xml(bridge.endpoint)}</link><description>Training programs</description><wp:wxr_version>1.2</wp:wxr_version>${programs.map(p=>`<item><title>${xml(p.title)}</title><content:encoded>${xml(JSON.stringify(p,null,2))}</content:encoded><wp:post_type>post</wp:post_type><wp:status>draft</wp:status><wp:post_name>${xml(p.id)}</wp:post_name></item>`).join('')}</channel></rss>`;
  }
  return JSON.stringify({schema:'bonfyre.training-programs.v1',programs},null,2);
}
