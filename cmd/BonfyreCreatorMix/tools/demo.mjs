// A reproducible synthetic fixture. It grants no authority over user media.
import {mkdirSync,writeFileSync,readFileSync,statSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dirname,'../../..');
const out=resolve(process.argv[2]||'packs/no-simulation/creator-upstream-20261013/demo');
const binary=process.env.BONFYRE_CREATOR_MIX_BINARY||join(root,'cmd/BonfyreCreatorMix/bonfyre-creator-mix');
const run=(exe,args)=>{const r=spawnSync(exe,args,{encoding:'utf8',timeout:30000});if(r.status!==0)throw Error(r.stderr||`${exe}: ${r.status}`);return r.stdout;};
const digest=body=>createHash('sha256').update(body).digest('hex');
const json=(name,value)=>writeFileSync(join(out,name),JSON.stringify(value,null,2)+'\n');
mkdirSync(resolve(out,'..'),{recursive:true});
mkdirSync(out); // Retain prior evidence; each demo run needs a fresh directory.
writeFileSync(join(out,'index.html'),readFileSync(join(import.meta.dirname,'demo.html')));
const rate=48000,channels=2,frames=180,samples=rate*6;
const specs=[{id:'pulse',hz:110,left:1,right:.4},{id:'chime',hz:440,left:.4,right:1},{id:'harmony',hz:660,left:.7,right:.7}];
const bodies=new Map(),bindings={},sources=[];
for(const spec of specs){
  const body=Buffer.alloc(rate*2*channels*4);
  for(let i=0;i<rate*2;i++){
    const t=i/rate,envelope=Math.min(1,t/.02)*Math.exp(-2*t),value=Math.sin(2*Math.PI*spec.hz*t)*envelope;
    body.writeFloatLE(value*spec.left,i*8);body.writeFloatLE(value*spec.right,i*8+4);
  }
  const path=join(out,spec.id+'.f32'),sha256=digest(body);writeFileSync(path,body);
  bodies.set(spec.id,body);bindings[spec.id]={path,sha256};sources.push({id:spec.id,kind:'audio',sha256,frames:60});
}
const clips=[{id:'pulse',source_id:'pulse',at:0,in:0,frames:30,gain:.4},{id:'chime',source_id:'chime',at:45,in:0,frames:45,gain:.5},{id:'harmony',source_id:'harmony',at:60,in:0,frames:45,gain:.5}];
const timeline={schema:'aurekai.creator.timeline.v1',scope:{owner:'synthetic-demo',purpose:'offline-verification',authority_digest:digest('synthetic-local-fixture'),frontier_digest:digest('six-second-audio-example')},fps:{num:30,den:1},sample_rate:rate,frames,sources,tracks:[{id:'audio',kind:'audio',clips}]};
json('timeline.json',timeline);json('bindings.json',bindings);
const cli=join(root,'Aurekai/bin/akai.mjs');
const plan=JSON.parse(run('node',[cli,'creator:compile','--input',join(out,'timeline.json'),'--shard-frames','180','--source-projection','used']));json('plan.json',plan);
const receipts=[];
for(const sparse of [false,true]){
  const output=join(out,sparse?'mix-sparse.f32':'mix.f32');
  const argv=[cli,'creator:pcm-plan','--plan',join(out,'plan.json'),'--bindings',join(out,'bindings.json'),'--window-index','0','--channels',String(channels),'--output',output];
  if(sparse)argv.push('--sparse-silence');
  const candidate=JSON.parse(run('node',argv));json(sparse?'candidate-sparse.json':'candidate.json',candidate);
  for(const binding of candidate.source_bindings)if(digest(readFileSync(binding.path))!==binding.sha256)throw Error('source digest mismatch');
  const receipt=JSON.parse(run(binary,candidate.argv));json(sparse?'render-sparse.json':'render.json',receipt);receipts.push(receipt);
}
const expected=Buffer.alloc(samples*channels*4);
for(const clip of clips){
  const body=bodies.get(clip.source_id),at=clip.at*1600,from=clip.in*1600,n=clip.frames*1600,gain=Math.fround(clip.gain);
  for(let i=0;i<n*channels;i++){
    const offset=(at*channels+i)*4;
    expected.writeFloatLE(Math.fround(expected.readFloatLE(offset)+Math.fround(body.readFloatLE((from*channels+i)*4)*gain)),offset);
  }
}
const dense=readFileSync(join(out,'mix.f32')),sparse=readFileSync(join(out,'mix-sparse.f32'));
if(!dense.equals(expected)||!sparse.equals(expected))throw Error('sample oracle mismatch');
for(const binding of Object.values(bindings))if(digest(readFileSync(binding.path))!==binding.sha256)throw Error('source changed');
run('ffmpeg',['-v','error','-nostdin','-f','f32le','-ar',String(rate),'-ac',String(channels),'-i',join(out,'mix.f32'),'-c:a','pcm_s16le',join(out,'mix.wav')]);
const denseStat=statSync(join(out,'mix.f32')),sparseStat=statSync(join(out,'mix-sparse.f32'));
json('verification.json',{synthetic:true,seconds:6,sample_rate:rate,channels,source_digests_verified_before_and_after:true,all_float32_samples_exact:true,dense_sha256:digest(dense),sparse_sha256:digest(sparse),output_bytes:dense.length,dense_allocated_bytes:denseStat.blocks*512,sparse_allocated_bytes:sparseStat.blocks*512,binary_sha256:digest(readFileSync(binary)),receipts});
console.log(JSON.stringify({output:out,samples,exact:true,dense_allocated_bytes:denseStat.blocks*512,sparse_allocated_bytes:sparseStat.blocks*512}));
