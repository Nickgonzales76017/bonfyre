import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,writeFileSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {compileCreatorProgram} from '../src/creator-render-program.mjs';
import {planNativePcmWindow,planNativePcmBatch} from '../src/creator-native-pcm.mjs';
const h=x=>x.repeat(64);
const fixture=()=>({schema:'aurekai.creator.timeline.v1',
  scope:{owner:'synthetic',purpose:'offline',authority_digest:h('a'),frontier_digest:h('b')},
  fps:{num:30,den:1},sample_rate:48000,frames:4,
  sources:[{id:'a',kind:'audio',sha256:h('c'),frames:4},
    {id:'b',kind:'audio',sha256:h('d'),frames:4},
    {id:'unused',kind:'audio',sha256:h('e'),frames:4}],
  tracks:[{id:'audio',kind:'audio',clips:[
    {id:'one',source_id:'a',at:0,in:0,frames:2,gain:0.5},
    {id:'two',source_id:'b',at:1,in:0,frames:2,gain:1}
  ]}]});
const packed=(n,value)=>{const b=Buffer.alloc(n*4);for(let i=0;i<n;i++)b.writeFloatLE(value,i*4);return b;};

test('Creator source cut lowers to exact native PCM and mixes under synthetic approval',()=>{
  const t=fixture(),p=compileCreatorProgram(t,{shardFrames:3,sourceProjection:'used'});
  const dir=mkdtempSync(join(tmpdir(),'aurekai-pcm-plan-'));
  try{
    const files={a:join(dir,'a.f32'),b:join(dir,'b.f32')},out=join(dir,'mixed.f32');
    writeFileSync(files.a,packed(6400,1));writeFileSync(files.b,packed(6400,1));
    const bindings={a:{path:files.a,sha256:h('c')},b:{path:files.b,sha256:h('d')}};
    const candidate=planNativePcmWindow(p,bindings,{windowIndex:0,channels:1,output:out});
    assert.equal(candidate.execution_authority_inferred,false);
    assert.equal(candidate.source_file_digest_verified,false);
    assert.equal(candidate.source_bindings.length,2);
    assert.equal(candidate.window_body_key,p.windows[0].body_key);
    assert.equal(candidate.argv[1],'4800');
    const compile=spawnSync('cc',['-O3','-std=c11','-ffp-contract=off','-Wall','-Wextra','-Werror',
      new URL('../native/creator/mix.c',import.meta.url).pathname,
      new URL('../native/creator/mix_kernel.c',import.meta.url).pathname,
      '-lm','-o',join(dir,'mix')],{encoding:'utf8'});
    assert.equal(compile.status,0,compile.stderr);
    // Only this synthetic test explicitly admits local process execution.
    const run=spawnSync(join(dir,'mix'),candidate.argv,{encoding:'utf8'});
    assert.equal(run.status,0,run.stderr);
    const buffer=readFileSync(out);assert.equal(buffer.length,4800*4);
    const sparseCandidate=planNativePcmWindow(p,bindings,{
      windowIndex:0,channels:1,output:join(dir,'mixed-sparse.f32'),sparseSilence:true
    });
    assert.equal(sparseCandidate.output_representation,'sparse_file_holes_candidate');
    assert.equal(sparseCandidate.sparse_silence_requested,true);
    assert.equal(sparseCandidate.argv.at(-1),'--sparse-silence');
    assert.equal(sparseCandidate.execution_authority_inferred,false);
    const sparseRun=spawnSync(join(dir,'mix'),sparseCandidate.argv,{encoding:'utf8'});
    assert.equal(sparseRun.status,0,sparseRun.stderr);
    assert.equal(JSON.parse(sparseRun.stdout).sparse_silence,true);
    assert.equal(readFileSync(join(dir,'mixed-sparse.f32')).equals(buffer),true);
    for(const [at,expected] of [[0,.5],[1599,.5],[1600,1.5],[3199,1.5],[3200,1],[4799,1]])
      assert.equal(buffer.readFloatLE(at*4),expected);
    assert.equal(JSON.parse(run.stdout).execution_authority_inferred,false);
    console.log(JSON.stringify({kind:'synthetic_creator_to_pcm',
      samples:4800,native_backend:true,output_parity:'exact',source_body_verified:false}));
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('native PCM candidate refuses unbound source claims and changed scope',()=>{
  const p=compileCreatorProgram(fixture(),{shardFrames:3});
  const bindings={a:{path:'/tmp/a.f32',sha256:h('c')},b:{path:'/tmp/b.f32',sha256:h('d')}};
  const opts={windowIndex:0,channels:1,output:'/tmp/render.f32'};
  assert.equal(planNativePcmWindow(p,bindings,opts).source_bindings.length,2);
  for(const patch of [{channels:0},{windowIndex:99},{output:'relative.f32'},
    {sparseSilence:'true'}])
    assert.throws(()=>planNativePcmWindow(p,bindings,{...opts,...patch}));
  assert.throws(()=>planNativePcmWindow(p,{...bindings,b:{...bindings.b,sha256:h('f')}},opts));
  assert.throws(()=>planNativePcmWindow({...p,effect_authorized:true},bindings,opts));
  assert.throws(()=>planNativePcmWindow({...p,scope_digest:'unbound'},bindings,opts));
  assert.throws(()=>planNativePcmWindow({...p,windows:p.windows.map(w=>({
    ...w,segments:w.segments.map(s=>({...s,samples:'9999999'}))
  }))},bindings,opts));
});


test('128 repeated Creator PCM tiles collapse to one exact physical candidate',()=>{
  const t=fixture();
  t.frames=384;
  t.fps={num:30,den:1};
  t.sources=[{id:'a',kind:'audio',sha256:h('c'),frames:3}];
  t.tracks=[{id:'beats',kind:'audio',clips:Array.from({length:128},(_,i)=>({
    id:'beat'+i,source_id:'a',at:i*3,in:0,frames:3,gain:0.5
  }))}];
  const plan=compileCreatorProgram(t,{shardFrames:3,sourceProjection:'used'});
  const bindings={a:{path:'/tmp/creator-verified-when-owner-checks.f32',sha256:h('c')}};
  const batch=planNativePcmBatch(plan,bindings,{
    channels:1,outputDirectory:'/tmp/creator-reuse-candidates'});
  assert.equal(batch.requested_windows,128);
  assert.equal(batch.unique_render_candidates,1);
  assert.equal(batch.reuse_candidates,127);
  assert.equal(batch.media_bytes_materialized,0);
  assert.equal(batch.execution_authority_inferred,false);
  assert.equal(batch.source_file_digest_verified,false);
  assert.equal(batch.renders[0].window_index,0);
  assert(batch.reuse.every(row=>row.reuse_window_index===0 &&
    row.window_body_key===batch.renders[0].window_body_key &&
    row.execution_authority_inferred===false));
  const sparse=planNativePcmBatch(plan,bindings,{channels:1,
    outputDirectory:'/tmp/creator-reuse-candidates',sparseSilence:true});
  assert.equal(sparse.unique_render_candidates,1);
  assert.equal(sparse.renders[0].argv.at(-1),'--sparse-silence');
  for(const invalid of [{windowIndices:[0,0]},{windowIndices:[0,129]},
      {channels:0},{outputDirectory:'relative'}])
    assert.throws(()=>planNativePcmBatch(plan,bindings,{
      channels:1,outputDirectory:'/tmp/creator-reuse-candidates',...invalid}));
  console.log(JSON.stringify({workload:'128 repeated clips from one exact source',
    candidate_windows:batch.requested_windows,physical_render_demands:1,
    reuse_candidates:127,materialized_bytes:0,owner_approval_required:true}));
});

test('same render-body digest cannot hide differing physical binding paths',()=>{
  const t=fixture();t.frames=6;
  t.sources=[{id:'a',kind:'audio',sha256:h('c'),frames:3},
    {id:'b',kind:'audio',sha256:h('c'),frames:3}];
  t.tracks=[{id:'beats',kind:'audio',clips:[
    {id:'beat-a',source_id:'a',at:0,in:0,frames:3,gain:1},
    {id:'beat-b',source_id:'b',at:3,in:0,frames:3,gain:1}
  ]}];
  const plan=compileCreatorProgram(t,{shardFrames:3});
  assert.equal(plan.windows[0].body_key,plan.windows[1].body_key);
  const bindings={a:{sha256:h('c'),path:'/tmp/source-a.f32'},
    b:{sha256:h('c'),path:'/tmp/source-b.f32'}};
  const batch=planNativePcmBatch(plan,bindings,{channels:1,outputDirectory:'/tmp/reuse'});
  assert.equal(batch.unique_render_candidates,2);
  assert.equal(batch.reuse_candidates,0);
  assert.throws(()=>planNativePcmBatch(plan,{...bindings,b:{...bindings.b,sha256:h('d')}},{
    channels:1,outputDirectory:'/tmp/reuse'}));
});
