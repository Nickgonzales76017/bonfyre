import {test,expect} from 'bun:test';
import {draftRows,deskView} from '../catalogue-desk.mjs';
import {draftKey} from '../catalogue-model.mjs';
const config={stores:[{id:'a',sha256:'source-a'},{id:'b',sha256:'source-b'}],entries:[{store:'a',id:'1',title:'First'},{store:'a',id:'2',title:'Second'},{store:'b',id:'1',title:'Other store'}]};
const record=(p,overrides={})=>({revision:1,value:{schema:'service-site.product-draft.v1',store:p.store,product_id:p.id,source_sha256:'source-'+p.store,saved_at:'2026-10-15T01:00:00Z',changes:[{field:'title',after:'Draft <title>'}],...overrides}});
test('draft desk keeps store identity, stale sources and receipts separate',()=>{
  const records=new Map(config.entries.map(p=>[draftKey(p),record(p,p.id==='2'?{source_sha256:'old'}:{})]));
  const receipt={reference:'lead:catalogue:abc'};
  const rows=draftRows(config,records,'a',()=>receipt);
  expect(rows).toHaveLength(2);expect(rows.map(r=>r.product.store)).toEqual(['a','a']);
  expect(rows[0].receipt).toBe(receipt);expect(rows[1].status).toBe('Needs checking');expect(rows[1].receipt).toBeNull();
  const html=deskView(rows,'sent');expect(html).toContain('Draft &lt;title&gt;');expect(html).not.toContain('Second');expect(html).not.toContain('Other store');
  expect(deskView(rows,'drafts','no match')).toContain('No matching drafts');
});
