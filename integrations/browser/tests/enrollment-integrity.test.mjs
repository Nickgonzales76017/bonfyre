import {test} from 'node:test';
import assert from 'node:assert/strict';
import {assertReviewedItem, humanStage, completionBlockers, requireBrowserContext, REQUIRED_STAGES} from '../extension/enrollment-integrity.mjs';

const expected = {work_id:'028-gmail', revision:2};
test('review is bound to the exact identity, mission and revision', () => {
  assert.doesNotThrow(()=>assertReviewedItem('gmail','enroll-028-gmail',2,expected));
  for (const input of [undefined, {}, {work_id:'028-gmail'}, {work_id:'028-gmail',revision:'2'},
    {work_id:'028-gmail',revision:1}, {work_id:'028-proton',revision:2}, {work_id:'029-gmail',revision:2}])
    assert.throws(()=>assertReviewedItem('gmail','enroll-028-gmail',2,input));
});
test('a replaced active item is not writable by an old panel', () => {
  assert.throws(()=>assertReviewedItem('gmail','enroll-029-gmail',2,expected), /stale/);
});
test('human review cannot assert sealed, verified or consumer-bound milestones', () => {
  for (const s of ['credential_sealed','credential_verified','consumer_bound','toString','__proto__'])
    assert.throws(()=>humanStage(s,'button'), /native owner/);
  assert.equal(humanStage('existing_access_checked','reviewed').evidence_kind, 'human_assertion');
});
test('partial work and legacy clicked milestones never count as completed enrollment', () => {
  assert.deepEqual(completionBlockers({}),REQUIRED_STAGES);
  const partial={existing_access_checked:humanStage('existing_access_checked','reviewed')};
  assert.equal(completionBlockers(partial).length,5);
  const legacy=Object.fromEntries(REQUIRED_STAGES.map(s=>[s,{at:'2026-10-19T00:00:00Z',note:'clicked'}]));
  assert.deepEqual(completionBlockers(legacy),REQUIRED_STAGES);
});
test('completion demands owner results and receipt references, not status labels', () => {
  const s=Object.fromEntries(['existing_access_checked','account_bound','scope_confirmed'].map(k=>[k,humanStage(k,'reviewed')]));
  s.credential_sealed={at:'now',evidence_kind:'native_seal',owner:'BonfyreCredentials'};
  s.credential_verified={at:'now',evidence_kind:'native_verification',owner:'BonfyreCredentials'};
  s.consumer_bound={at:'now',evidence_kind:'native_consumer_binding',owner:'a_consumer'};
  assert.deepEqual(completionBlockers(s),['credential_verified','consumer_bound']);
  s.credential_verified.receipt_ref='fixture:verified'; s.consumer_bound.receipt_ref='fixture:bound';
  assert.deepEqual(completionBlockers(s),[]);
});
test('browser context must match both identity and target window', () => {
  assert.doesNotThrow(()=>requireBrowserContext('gmail',false,{id:10,incognito:false,type:'normal'}));
  assert.doesNotThrow(()=>requireBrowserContext('proton',true,{id:20,incognito:true,type:'normal'}));
  for (const args of [['proton',false,{id:10,incognito:false}],['gmail',true,{id:20,incognito:true}],
    ['proton',true,{id:10,incognito:false}],['gmail',false,{id:20,incognito:true}],
    ['gmail',false,{id:10,incognito:false,type:'popup'}],['gmail',false,{id:10}],['bad',false,{}]])
    assert.throws(()=>requireBrowserContext(...args));
});
