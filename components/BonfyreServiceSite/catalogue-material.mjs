// Package native-query output for the browser. Source ownership stays with the retained catalogue.
import{readFileSync}from'node:fs';import{createHash}from'node:crypto';import{join,resolve}from'node:path';
import { prepareCatalogueRow } from './catalogue-baseline.mjs';
import { baseline, makeDraft } from './catalogue-model.mjs';
import { plannerFiles } from './planner-material.mjs';
function underSource(value,origin,path){
 try{const base=new URL(origin),url=new URL(value);return base.protocol==='https:'&&base.origin===origin&&url.protocol==='https:'&&!url.username&&!url.password&&url.origin===origin&&/^\/(?:[a-z0-9_-]+\/)+$/.test(path)&&url.pathname.startsWith(path)&&url.pathname.length>path.length;}catch{return false;}
}
export function catalogueFiles(rows,stores,{repo=resolve(import.meta.dir,'../..'),samples={},fixtureCatalogues=[]}={}){
 if(!Array.isArray(rows)||!rows.length||!Array.isArray(stores)||!stores.length)throw Error('Catalogue material is empty.');
 if(!Array.isArray(fixtureCatalogues)||fixtureCatalogues.length>20)throw Error('Invalid fixture collections.');
 const fixture_catalogues=fixtureCatalogues.map(({digest,...input})=>{
  const checked=plannerFiles(input).catalogue;
  if(checked.digest!==digest)throw Error('Fixture collection digest does not match its source.');
  return checked;
 });
 if(new Set(fixture_catalogues.map(c=>c.digest)).size!==fixture_catalogues.length)throw Error('Duplicate fixture collection.');
 rows=rows.map(prepareCatalogueRow);
 rows=rows.map(p=>{
  const sample=samples[p.store+':'+p.id];if(!sample)return p;
  if(sample.source_url!==p.source_url)throw Error('Prepared sample source differs from product.');
  const original=baseline(p),edited={...original,...sample.fields,variants:original.variants};
  return {...p,prepared_sample:makeDraft(p,original,edited,sample.prepared_at)};
 });
 const files={},entries=[],ids=new Set();
 for(const p of rows){const s=stores.find(s=>s.id===p.store),key=p.store+':'+p.id;if(!s||p.source_sha256!==s.sha256||ids.has(key))throw Error('Catalogue identity mismatch.');ids.add(key);if(!/^\d+$/.test(p.id)||!Array.isArray(p.variants)||!p.variants.length||p.variants.some(v=>!/^\d+$/.test(v.id)))throw Error('Product options are incomplete.');if(!underSource(p.source_url,s.origin,s.product_path??'/products/'))throw Error('Product URL mismatch.');if(p.image){let shopify=false;try{const u=new URL(p.image);shopify=u.protocol==='https:'&&u.hostname==='cdn.shopify.com'&&!u.username&&!u.password;}catch{}if(!shopify&&!(s.image_path&&underSource(p.image,s.origin,s.image_path)))throw Error('Unexpected image source.');}}
 for(let at=0;at<rows.length;at+=32){const chunk='products-'+at/32+'.json.gz',part=rows.slice(at,at+32);files[chunk]=Bun.gzipSync(Buffer.from(JSON.stringify(part)),{level:9});for(const p of part)entries.push({id:p.id,store:p.store,title:p.title,image:p.image,kind:p.kind,source_url:p.source_url,before_sha256:p.before_sha256,variant_ids:p.variants.map(v=>v.id),variant_count:p.variants.length,missing_skus:p.variants.filter(v=>!v.sku?.trim()).length,chunk,chunk_sha256:createHash('sha256').update(files[chunk]).digest('hex')});}
 for(const name of ['workspace-lifetime.mjs','catalogue-client.mjs','catalogue-model.mjs','catalogue-review.mjs','catalogue-preparation.mjs','catalogue-desk.mjs','catalogue-grid.mjs','catalogue-grid-model.mjs','catalogue-release.mjs','catalogue-release-view.mjs','planner-model.mjs','workspace-navigation.mjs'])files[name]=readFileSync(join(import.meta.dir,name));
 files['ag-grid-community.min.js']=readFileSync(join(import.meta.dir,'vendor/ag-grid-community-36.2.0/ag-grid-community.min.js'));
 files['ag-grid-LICENSE.txt']=readFileSync(join(import.meta.dir,'vendor/ag-grid-community-36.2.0/LICENSE.txt'));
 files['store.mjs']=readFileSync(join(repo,'integrations/browser/extension/store.mjs'));
 return{files,entries,stores,fixture_catalogues};
}
