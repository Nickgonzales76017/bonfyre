import { test } from 'node:test';
import assert from 'node:assert/strict';
import { collapseExactPassages } from '../recall_results.mjs';

test('exact repeated authority passages collapse to one preview without losing any source IDs', () => {
  const rows = [
    {id:'outbound_message:16',text:'Contact is not authority.',score:0.04,vector_rank:1},
    {id:'outbound_message:18',text:'Contact is not authority.',score:0.035,vector_rank:2},
    {id:'work:24',text:'Source proof is required.',score:0.033,word_rank:3},
    {id:'outbound_message:19',text:'Contact is not authority.',score:0.03,word_rank:4},
    {id:'work:25',text:'Authority is not cash.',score:0.028,word_rank:5}
  ];
  const original=structuredClone(rows);
  const result=collapseExactPassages(rows,2);
  assert.equal(result.input_candidates,5);
  assert.equal(result.exact_text_duplicate_rows,2);
  assert.equal(result.selected,3);
  assert.deepEqual(result.passages.map(r=>r.id),['outbound_message:16','work:24']);
  assert.deepEqual(result.passages[0].same_text_ids,['outbound_message:18','outbound_message:19']);
  assert.equal(result.passages[0].score,0.04);
  assert.deepEqual(rows,original);
});
test('near-identical text and different evidence are never merged',()=>{
  const results=collapseExactPassages([
    {id:'source:a',text:'Finance invoice is not paid.'},
    {id:'source:b',text:'Finance invoice is not paid. '},
    {id:'source:c',text:'finance invoice is not paid.'}
  ],3);
  assert.equal(results.passages.length,3);
  assert.equal(results.exact_text_duplicate_rows,0);
});
test('malformed inputs, untrusted rank rows and unbounded limits are refused',()=>{
  assert.throws(()=>collapseExactPassages(null,1),/Invalid candidate/);
  assert.throws(()=>collapseExactPassages([],51),/Invalid candidate/);
  assert.throws(()=>collapseExactPassages([{id:3,text:'hello'}],1),/Recall candidate/);
  assert.throws(()=>collapseExactPassages([{id:'a',text:null}],1),/Recall candidate/);
});
