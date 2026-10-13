import {test} from 'node:test';
import assert from 'node:assert/strict';
import {compileCreatorProgram,creatorTimelineDigest,editCreatorTimeline,
  creatorInvalidations,samplesAtFrame} from '../src/creator-render-program.mjs';
const hex = ch => ch.repeat(64);
const timeline = () => ({
  schema:'aurekai.creator.timeline.v1',
  scope:{owner:'creator-fixture',purpose:'preview',authority_digest:hex('a'),frontier_digest:hex('b')},
  fps:{num:30000,den:1001},sample_rate:48000,frames:600,
  sources:[{id:'picture',kind:'video',sha256:hex('c'),frames:600},
    {id:'sound',kind:'audio',sha256:hex('d'),frames:600}],
  tracks:[{id:'v',kind:'video',clips:[{id:'clip-v',source_id:'picture',at:0,in:0,frames:600}]},
    {id:'a',kind:'audio',clips:[{id:'clip-a',source_id:'sound',at:0,in:0,frames:600,gain:0.75}]}]
});
test('exact output, bounded nonmaterialized shards and shared source bases', () => {
  const source=timeline(), before=JSON.stringify(source);
  const a=compileCreatorProgram(source,{shardFrames:100});
  assert.deepEqual(a,compileCreatorProgram(source,{shardFrames:100}));
  assert.equal(JSON.stringify(source),before);
  assert.equal(a.windows.length,6);
  assert.equal(a.metrics.resolved_segments,12);
  assert.equal(a.source_bases.length,2);
  assert.equal(a.media_bytes_materialized,0);
  assert.equal(a.effect_authorized,false);
  for(const w of a.windows){
    const s=samplesAtFrame(w.start_frame,source.fps,source.sample_rate);
    const e=samplesAtFrame(w.start_frame+w.frames,source.fps,source.sample_rate);
    assert.equal(w.start_sample,s);
    assert.equal(w.samples,(BigInt(e)-BigInt(s)).toString());
    assert(w.segments.every(v=>Number.isInteger(v.source_basis)&&v.source_digest===undefined));
  }
});
test('viewport selects exact cut and does not expand neighboring work', () => {
  const source=timeline(),all=compileCreatorProgram(source,{shardFrames:60});
  const selected=compileCreatorProgram(source,{shardFrames:60,range:{start:126,end:151}});
  assert.deepEqual(selected.windows.map(w=>w.index),[2]);
  assert.equal(selected.windows[0].body_key,all.windows[2].body_key);
  assert.equal(creatorInvalidations(all,all).length,0);
});
test('agent edits are candidate-only and revision-bound', () => {
  const source=timeline(),base=creatorTimelineDigest(source);
  const changed=editCreatorTimeline(source,{expected_digest:base,op:'gain',track_id:'a',clip_id:'clip-a',value:0.4});
  assert.equal(changed.edit.state,'candidate_uncommitted');
  assert.equal(source.tracks[1].clips[0].gain,0.75);
  assert.equal(creatorInvalidations(compileCreatorProgram(source,{shardFrames:60}),
    compileCreatorProgram(changed.timeline,{shardFrames:60})).length,10);
  assert.throws(()=>editCreatorTimeline(source,{expected_digest:hex('1'),op:'gain',track_id:'a',clip_id:'clip-a',value:0.3}),/stale/);
});
test('identical authorized bodies reuse within a scope, never across scope', () => {
  const t=timeline();t.frames=6;t.fps={num:30,den:1};
  t.sources=[{id:'sample',kind:'audio',sha256:hex('e'),frames:3}];
  t.tracks=[{id:'beats',kind:'audio',clips:[
    {id:'one',source_id:'sample',at:0,in:0,frames:3},
    {id:'two',source_id:'sample',at:3,in:0,frames:3}
  ]}];
  const a=compileCreatorProgram(t,{shardFrames:3});
  assert.equal(a.metrics.reused_window_bodies,1);
  const b=compileCreatorProgram({...t,scope:{...t.scope,authority_digest:hex('f')}},{shardFrames:3});
  assert.notEqual(a.windows[0].body_key,b.windows[0].body_key);
});
test('invalid sources, rights coordinates, clock and duplicate mutations refuse', () => {
  const t=timeline();
  for(const patch of [{fps:{num:0,den:1}},
    {scope:{...t.scope,authority_digest:'unknown'}},{sources:[...t.sources,...t.sources]},
    {tracks:[...t.tracks,...t.tracks]}])
    assert.throws(()=>compileCreatorProgram({...t,...patch}));
  assert.throws(()=>compileCreatorProgram(t,{range:{start:5,end:5}}),/range/);
  assert.throws(()=>editCreatorTimeline(t,{expected_digest:creatorTimelineDigest(t),op:'duplicate',
    track_id:'a',clip_id:'clip-a',new_id:'clip-v',at:0}),/duplicate clip/);
});
test('10k-clip synthetic viewport stays local and reports unmeasured physical costs', () => {
  const t=timeline();t.frames=1000000;
  t.sources=[{id:'tone',kind:'audio',sha256:hex('9'),frames:1000000}];
  t.tracks=[{id:'a',kind:'audio',clips:Array.from({length:10000},(_,i)=>({
    id:'c'+i,source_id:'tone',at:i*80,in:0,frames:10}))}];
  const p=compileCreatorProgram(t,{shardFrames:256,range:{start:100100,end:100130}});
  assert.equal(p.windows.length,1);
  assert.equal(p.metrics.clip_count,10000);
  assert.equal(p.metrics.measured_cpu_ms,null);
  assert.equal(p.metrics.observed_peak_memory_bytes,null);
});
