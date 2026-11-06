import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,existsSync,rmSync,openSync,ftruncateSync,closeSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {creatorTimelineDigest} from '../src/creator-render-program.mjs';
const cli=resolve(import.meta.dirname,'../bin/akai.mjs');
const timeline=()=>({schema:'aurekai.creator.timeline.v1',scope:{owner:'synthetic',purpose:'test',authority_digest:'a'.repeat(64),frontier_digest:'b'.repeat(64)},fps:{num:30,den:1},sample_rate:48000,frames:4,sources:[{id:'a',kind:'audio',sha256:'c'.repeat(64),frames:4}],tracks:[{id:'audio',kind:'audio',clips:[{id:'clip',source_id:'a',at:0,in:0,frames:4,gain:0.5}]}]});
const run=(...args)=>spawnSync(process.execPath,[cli,...args],{encoding:'utf8',timeout:5000});

test('public CLI compiles and lowers metadata without reading PCM or creating output',()=>{
  const dir=mkdtempSync(join(tmpdir(),'aurekai-creator-cli-'));
  try{
    const input=join(dir,'timeline.json'),planFile=join(dir,'plan.json'),bindings=join(dir,'bindings.json'),output=join(dir,'output.f32');
    writeFileSync(input,JSON.stringify(timeline()));
    const compile=run('creator:compile','--input',input,'--shard-frames','2','--from','0','--to','2','--source-projection','used');
    assert.equal(compile.status,0,compile.stderr);
    const plan=JSON.parse(compile.stdout);assert.equal(plan.windows.length,1);assert.equal(plan.media_bytes_materialized,0);assert.equal(plan.effect_authorized,false);
    writeFileSync(planFile,compile.stdout);
    writeFileSync(bindings,JSON.stringify({a:{path:join(dir,'absent.f32'),sha256:'c'.repeat(64)}}));
    const lowered=run('creator:pcm-plan','--plan',planFile,'--bindings',bindings,'--window-index','0','--channels','1','--output',output,'--sparse-silence');
    assert.equal(lowered.status,0,lowered.stderr);
    const candidate=JSON.parse(lowered.stdout);assert.equal(candidate.source_file_digest_verified,false);assert.equal(candidate.execution_authority_inferred,false);assert.equal(candidate.argv.at(-1),'--sparse-silence');assert.equal(existsSync(output),false);
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('CLI edits require the exact revision and leave source metadata unchanged',()=>{
  const dir=mkdtempSync(join(tmpdir(),'aurekai-creator-edit-'));
  try{
    const input=join(dir,'timeline.json'),edit=join(dir,'edit.json'),body=JSON.stringify(timeline());writeFileSync(input,body);
    const mutation={op:'gain',track_id:'audio',clip_id:'clip',value:0.25,expected_digest:creatorTimelineDigest(timeline())};writeFileSync(edit,JSON.stringify(mutation));
    const changed=run('creator:edit','--input',input,'--edit',edit);assert.equal(changed.status,0,changed.stderr);
    assert.equal(JSON.parse(changed.stdout).timeline.tracks[0].clips[0].gain,0.25);assert.equal(readFileSync(input,'utf8'),body);
    writeFileSync(edit,JSON.stringify({...mutation,expected_digest:'d'.repeat(64)}));
    const stale=run('creator:edit','--input',input,'--edit',edit);assert.notEqual(stale.status,0);assert.equal(stale.stdout,'');assert.match(stale.stderr,/stale/);
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('CLI refuses malformed options, unsupported files and oversized metadata',()=>{
  const dir=mkdtempSync(join(tmpdir(),'aurekai-creator-refusal-'));
  try{
    const input=join(dir,'timeline.json'),huge=join(dir,'large.json');writeFileSync(input,JSON.stringify(timeline()));
    const fd=openSync(huge,'wx');ftruncateSync(fd,16*1024*1024+1);closeSync(fd);
    for(const args of [
      ['--input',input,'--from','0'],['--input',input,'--shard-frames','NaN'],
      ['--input',input,'--shard-frames','9007199254740992'],['--input',input,'--input',input],
      ['--input',input,'--execute'],['--input'],['--input',dir],['--input',huge]
    ]){const result=run('creator:compile',...args);assert.notEqual(result.status,0);assert.equal(result.stdout,'');assert.equal(result.signal,null);}
  }finally{rmSync(dir,{recursive:true,force:true});}
});
