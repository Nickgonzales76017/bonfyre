import {test,expect} from 'bun:test';
import {priceCents,money,baseline,review,makeDraft,restoreDraft} from '../catalogue-model.mjs';
import {catalogueFiles} from '../catalogue-material.mjs';
import {createHash} from 'node:crypto';
const product={id:'123',store:'store-a',title:'Original ring',tags:['Ring'],source_url:'https://store.example/products/ring',source_sha256:'a'.repeat(64),observed_at:'2026-10-07T00:00:00Z',variants:[{id:'456',label:'8',sku:null,price:'199.00',available:true}],image:null,kind:'goods'};
const original=()=>baseline(product,'Plain description');
test('exact cent arithmetic and formatting',()=>{for(const [text,cents] of [['0',0n],['0.01',1n],['199.00',19900n],['999999999.99',99999999999n]])expect(priceCents(text)).toBe(cents);expect(money(18999n)).toBe('189.99');});
test('invalid prices do not become quotes',()=>{for(const value of ['-1','NaN','Infinity','1e2','12.345','1,200','1000000000','',null,1])expect(()=>priceCents(value)).toThrow();});
test('unchanged product has no draft',()=>{const b=original();expect(review(product,b,b)).toEqual({errors:[],changes:[]});expect(()=>makeDraft(product,b,b)).toThrow();});
test('title required and option identity preserved',()=>{const b=original(),e=structuredClone(b);e.title=' ';expect(review(product,b,e).errors[0].field).toBe('title');e.title='Changed';e.variants[0].id='999';expect(review(product,b,e).errors[0].field).toBe('variants');});
test('save and restore preserve source and proposed values',()=>{const b=original(),e=structuredClone(b);e.title='Revised ring';e.variants[0].price='189.99';e.variants[0].sku='RING-8';const d=makeDraft(product,b,e,'2026-10-07T01:00:00Z');expect(d.state).toBe('draft');expect(d.changes.length).toBe(3);expect(restoreDraft(product,b,d)).toEqual(e);expect(product.variants[0].sku).toBe(null);expect(product.title).toBe('Original ring');});
test('equivalent source price representations restore correctly',()=>{const p={...product,variants:[{...product.variants[0],price:'199'}]},b=baseline(p,'Text'),e=structuredClone(b);e.variants[0].price='199.25';expect(restoreDraft(p,b,makeDraft(p,b,e)).variants[0].price).toBe('199.25');});
test('wrong product, source snapshot and duplicate field are refused',()=>{const b=original(),e={...b,title:'Changed'},d=makeDraft(product,b,e);expect(()=>restoreDraft(product,b,{...d,product_id:'124'})).toThrow();expect(()=>restoreDraft(product,b,{...d,source_sha256:'b'.repeat(64)})).toThrow();expect(()=>restoreDraft(product,b,{...d,changes:[...d.changes,...d.changes]})).toThrow();});
test('changed original is not silently overwritten',()=>{const b=original(),d=makeDraft(product,b,{...b,title:'Changed'});d.changes[0].before='Someone else edited';expect(()=>restoreDraft(product,b,d)).toThrow();});
test('missing SKU stays empty until supplied',()=>{const b=original();expect(b.variants[0].sku).toBe('');const e=structuredClone(b);e.variants[0].sku='RING-8';expect(makeDraft(product,b,e).changes[0].before).toBe('');});
test('material packages exact checksummed pieces and canonical store',()=>{const stores=[{id:product.store,origin:'https://store.example',sha256:product.source_sha256}],m=catalogueFiles([product],stores),x=m.entries[0];expect(x.chunk_sha256).toBe(createHash('sha256').update(m.files[x.chunk]).digest('hex'));const packed=JSON.parse(Buffer.from(Bun.gunzipSync(m.files[x.chunk])).toString());expect(packed).toHaveLength(1);expect(packed[0]).toMatchObject(product);expect(typeof packed[0].description_text).toBe('string');expect(x.before_sha256.title).toBe(createHash('sha256').update(product.title).digest('hex'));expect(m.files['store.mjs'].length).toBeGreaterThan(0);expect(()=>catalogueFiles([product,product],stores)).toThrow();});

test('legacy product paths use the same workspace without rewriting source URLs',()=>{
 const store={id:'legacy',origin:'https://legacy.example',sha256:'a'.repeat(64),product_path:'/retail_fixtures/',image_path:'/wp-content/uploads/'};
 const p={...product,store:'legacy',source_url:store.origin+'/retail_fixtures/easel/',image:store.origin+'/wp-content/uploads/easel.jpg'};
 const out=catalogueFiles([p],[store]);const kept=JSON.parse(Buffer.from(Bun.gunzipSync(out.files[out.entries[0].chunk])).toString())[0];
 expect(kept.source_url).toBe(p.source_url);expect(kept.image).toBe(p.image);expect(kept.variants[0].sku).toBe(null);
 for(const source_url of ['https://legacy.example.evil.test/retail_fixtures/easel/','https://user@legacy.example/retail_fixtures/easel/','https://legacy.example/retail_fixtures/%2e%2e/private/easel/','http://legacy.example/retail_fixtures/easel/'])expect(()=>catalogueFiles([{...p,source_url}],[store])).toThrow('Product URL mismatch');
 expect(()=>catalogueFiles([{...p,image:'https://other.example/easel.jpg'}],[store])).toThrow('Unexpected image source');
 expect(()=>catalogueFiles([{...p,image:store.origin+'/private/easel.jpg'}],[store])).toThrow('Unexpected image source');
 expect(()=>catalogueFiles([p],[{...store,product_path:'//retail_fixtures/'}])).toThrow('Product URL mismatch');
 expect(()=>catalogueFiles([p],[{...store,origin:'https://legacy.example/'}])).toThrow('Product URL mismatch');
});
