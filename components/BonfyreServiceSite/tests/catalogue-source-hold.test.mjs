// Missing optional seller-copy evidence must not erase original product records.
import {test,expect} from 'bun:test';
import {mkdtempSync,copyFileSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {catalogueWithoutPreparedCopy} from '../catalogue-material.mjs';
import {withMaterial} from '../site-material.mjs';

const site=join(import.meta.dir,'../../../packs/aurekai-work-samples/site');
test('catalogue survives unavailable prepared copy using only retained source identities',()=>{
 const material=catalogueWithoutPreparedCopy(site);
 expect(material.entries.length).toBeGreaterThan(0);
 const red3=material.entries.find(x=>x.store==='red3-display'&&x.id==='7631');
 expect(red3).toBeDefined();
 const chunk=JSON.parse(Buffer.from(Bun.gunzipSync(material.files[red3.chunk])).toString());
 const original=chunk.find(p=>p.store===red3.store&&p.id===red3.id);
 expect(original.source_url).toBe(red3.source_url);
 expect(original.prepared_sample).toBeUndefined();
 const hold=JSON.parse(material.files['source-review-hold.json'].toString());
 expect(hold).toMatchObject({state:'held',prepared_copy_loaded:false,merchant_edits_published:false});
 expect(hold.original_catalogue_sha256).toMatch(/^[a-f0-9]{64}$/);
});
test('only a cyclic optional catalogue-review attachment permits source-only material',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-catalogue-hold-'));
 try{
  for(const name of ['catalogue-input.json.gz','fixture-planner.json'])copyFileSync(join(site,name),join(dir,name));
  writeFileSync(join(dir,'material.mjs'),"export function products(){const e=new Error('cyclic optional review');e.code='ELOOP';throw e;}\nexport function other(){const e=new Error('different material failure');e.code='ELOOP';throw e;}\n");
  const section={id:'product-workspace',kind:'catalogue',title:'Products',material:'products',review:true};
  const filled=await withMaterial({sections:[],pages:[{sections:[section]}]},dir);
  expect(filled.pages[0].sections[0].source_review_hold?.state).toBe('held');
  expect(filled.pages[0].sections[0].data).toBe('/data/product-workspace/');
  expect(filled._files['data/product-workspace/source-review-hold.json']).toBeDefined();
  await expect(withMaterial({sections:[],pages:[{sections:[{...section,kind:'schema',material:'other'}]}]},dir))
   .rejects.toThrow('different material failure');
 }finally{rmSync(dir,{recursive:true,force:true});}
});
