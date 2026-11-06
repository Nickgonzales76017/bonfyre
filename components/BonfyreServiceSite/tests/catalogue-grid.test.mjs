import {test,expect} from 'bun:test';
import {catalogueSession,inventoryRows,inventoryProductRows,editInventoryCell,bulkInventoryProposal,inventorySaveBatch,inventoryDelta} from '../catalogue-grid-model.mjs';
import {baseline,makeDraft,draftKey} from '../catalogue-model.mjs';
const products=[{id:'1',store:'first',title:'Walnut ring',description_text:'Wood ring',tags:['Wood'],source_url:'https://shop.example/products/walnut',source_sha256:'a'.repeat(64),observed_at:'2026-10-15',variants:[{id:'11',label:'Small',sku:'W-S',price:'19.99'},{id:'12',label:'Large',sku:'',price:'29.95'}]},{id:'2',store:'first',title:'Metal ring',description_text:'Metal ring',tags:[],source_url:'https://shop.example/products/metal',source_sha256:'a'.repeat(64),observed_at:'2026-10-15',variants:[{id:'21',label:'Small',sku:'M-S',price:'30.00'}]}];
test('product and option layouts project the same source and missing identifiers',()=>{
 const s=catalogueSession(products,new Map()),p=inventoryProductRows(s);expect(inventoryRows(s)).toHaveLength(3);expect(p).toHaveLength(2);expect(p[0]).toMatchObject({price:'19.99 – 29.95',missing:1,option:'2 options',sku:'1 missing'});
});
test('bulk percentages round exact cents, affect selected variants, and leave source untouched',()=>{
 const s=catalogueSession(products,new Map()),rows=inventoryRows(s),p=bulkInventoryProposal(s,[rows[0],rows[2]],'percent','-15');
 expect(p.changes.map(c=>c.after)).toEqual(['16.99','25.50']);expect(s.get('1').edit.variants[0].price).toBe('19.99');expect(p.copies.get('1').edit.variants[1].price).toBe('29.95');expect(products[0].variants[0].price).toBe('19.99');expect(inventorySaveBatch(p.copies)).toHaveLength(2);
});
test('bulk validation rejects the whole proposal before changing any live edit',()=>{
 const s=catalogueSession(products,new Map()),rows=inventoryRows(s);
 for(const [action,value]of [['percent','-100.01'],['percent','NaN'],['price','-2'],['price','3.333'],['tag',''],['tag','a,b']])expect(()=>bulkInventoryProposal(s,rows,action,value)).toThrow();
 expect(()=>bulkInventoryProposal(s,[],'price','1')).toThrow();expect(inventorySaveBatch(s)).toEqual([]);
 const free=bulkInventoryProposal(s,rows,'percent','-100');expect(free.changes.every(c=>c.after==='0.00')).toBe(true);
});
test('tags apply once per product and existing tags survive',()=>{
 const s=catalogueSession(products,new Map()),p=bulkInventoryProposal(s,inventoryRows(s),'tag','Summer');expect(p.changes).toHaveLength(2);expect(p.copies.get('1').edit.tags).toBe('Wood, Summer');expect(()=>bulkInventoryProposal(p.copies,inventoryRows(p.copies),'tag','summer')).toThrow('already');
});
test('inline edits validate, keep option identity, and produce the existing draft contract',()=>{
 const s=catalogueSession(products,new Map()),r=inventoryRows(s)[0];editInventoryCell(s,r,'title','Revised walnut');editInventoryCell(s,r,'price','22.50');
 expect(inventoryRows(s)[1].title).toBe('Revised walnut');expect(()=>editInventoryCell(s,r,'price','bad')).toThrow();expect(s.get('1').edit.variants[0].price).toBe('22.50');
 const batch=inventorySaveBatch(s);expect(batch[0]).toMatchObject({key:'draft:catalogue:first:1',expectedRevision:0,value:{schema:'service-site.product-draft.v1',product_id:'1',source_sha256:products[0].source_sha256}});
 expect(batch[0].value.changes.map(c=>c.field)).toEqual(['title','price:11']);
});
test('saved drafts resume with exact revisions and obsolete source drafts stay blocked',()=>{
 const original=baseline(products[0]),edit=structuredClone(original);edit.variants[0].price='22.00';const draft=makeDraft(products[0],original,edit);
 const s=catalogueSession(products,new Map([[draftKey(products[0]),{revision:4,value:draft}]]));const row=inventoryRows(s)[0];expect(row.price).toBe('22.00');editInventoryCell(s,row,'price','23.00');expect(inventoryDelta(s.get('1'))[0]).toMatchObject({before:'22.00',after:'23.00'});expect(inventorySaveBatch(s)[0].expectedRevision).toBe(4);
 const broken=catalogueSession(products,new Map([[draftKey(products[0]),{revision:4,value:{...draft,source_sha256:'b'.repeat(64)}}]]));expect(inventoryRows(broken)[0].status).toBe('Needs checking');expect(()=>bulkInventoryProposal(broken,inventoryRows(broken),'price','12')).toThrow();
});
