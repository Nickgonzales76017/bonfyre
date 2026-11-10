import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,statSync,readFileSync,writeFileSync,mkdirSync,symlinkSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {writeCheckpointMaterial as put,readCheckpointMaterial as get} from '../onboarding/checkpoint-material.mjs';
function fixture(t){const root=mkdtempSync(join(tmpdir(),'enrollment-material-'));t.after(()=>rmSync(root,{recursive:true,force:true}));return root;}
const state={stages:{existing_access_checked:{at:'2026-10-19',evidence_kind:'human_assertion',note:'reviewed'}}};
test('candidate material roundtrips in existing MessagePack format and is private',t=>{
  const r=fixture(t), uri=put(r,'028-gmail',1,state);assert.match(uri,/\.mpk$/);assert.deepEqual(get(r,'028-gmail',uri),state);
  assert.equal(statSync(fileURLToPath(uri)).mode & 0o777,0o600);
  assert.equal(existsSync(join(r,'checkpoints','028-gmail.json')),false);
});
test('competing candidates at the same revision cannot overwrite a winner',t=>{
  const r=fixture(t),a=put(r,'028-gmail',1,state),original=readFileSync(fileURLToPath(a));
  const b=put(r,'028-gmail',1,{...state,notes:['other candidate']});assert.notEqual(a,b);
  assert.deepEqual(readFileSync(fileURLToPath(a)),original);assert.deepEqual(get(r,'028-gmail',a),state);
  assert.equal(put(r,'028-gmail',1,state),a);
});
test('tampered candidate cannot be read or quietly overwritten',t=>{
  const r=fixture(t),a=put(r,'028-gmail',1,state);writeFileSync(fileURLToPath(a),'tampered');
  assert.throws(()=>get(r,'028-gmail',a),/digest/);assert.throws(()=>put(r,'028-gmail',1,state),/digest/);
});
test('work identity and URI boundaries refuse sibling or external material',t=>{
  const r=fixture(t),a=put(r,'028-gmail',1,state);
  assert.throws(()=>get(r,'028-proton',a),/another item/);
  assert.throws(()=>get(r,'028-gmail','https://example.com/data'));
  assert.throws(()=>put(r,'../../escape',1,state));
  assert.throws(()=>get(r,'028-gmail',pathToFileURL(join(r,'not-a-checkpoint')).href));
});
test('legacy checkpoints are read-only compatibility and no latest file is written',t=>{
  const r=fixture(t);mkdirSync(join(r,'checkpoints'));const old=join(r,'checkpoints','028-gmail.json');
  writeFileSync(old,JSON.stringify(state));const original=readFileSync(old);
  assert.deepEqual(get(r,'028-gmail',pathToFileURL(old).href),state);put(r,'028-gmail',2,state);
  assert.deepEqual(readFileSync(old),original);
});
test('symlinked checkpoint directories cannot redirect writes',t=>{
  const r=fixture(t),outside=fixture(t);symlinkSync(outside,join(r,'checkpoints'));
  assert.throws(()=>put(r,'028-gmail',1,state),/Symlink/);assert.equal(existsSync(join(outside,'028-gmail')),false);
});
test('oversized checkpoint candidate is refused before a body is written',t=>{
  const r=fixture(t);assert.throws(()=>put(r,'028-gmail',1,{stages:{},note:'x'.repeat(600000)}),/too large/);
});
