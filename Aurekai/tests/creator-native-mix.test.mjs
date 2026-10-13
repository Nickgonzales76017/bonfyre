import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const native=resolve(import.meta.dirname,'../native/creator');
const cc=process.env.CC||'cc';
const encode=values=>{const b=Buffer.alloc(values.length*4);values.forEach((v,i)=>b.writeFloatLE(v,i*4));return b;};
const decode=buffer=>Array.from({length:buffer.length/4},(_,i)=>buffer.readFloatLE(i*4));
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const make=()=>{
  const dir=mkdtempSync(join(tmpdir(),'aurekai-native-mix-')),exe=join(dir,'creator-mix');
  const build=spawnSync(cc,['-O3','-std=c11','-ffp-contract=off',
    '-Wall','-Wextra','-Wpedantic','-Werror',join(native,'mix.c'),
    join(native,'mix_kernel.c'),'-lm','-o',exe],{encoding:'utf8'});
  assert.equal(build.status,0,build.stderr);return {dir,exe};
};

test('native bounded mixer exactly respects ordered gain and offset arithmetic',()=>{
  const {dir,exe}=make();
  try{
    const first=join(dir,'a;.f32'),second=join(dir,'b.f32'),out=join(dir,'mixed.f32');
    const a=encode([1,0, 2,0, 3,0, 4,0, 5,0, 6,0]);
    const b=encode([.5,.5, .75,.75, 1,1, 1.25,1.25]);
    writeFileSync(first,a);writeFileSync(second,b);
    const result=spawnSync(exe,['--samples','6','--channels','2','--output',out,
      '--stem',first,'0','0','6','0.5','--stem',second,'2','1','3','-1'],{encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);
    const receipt=JSON.parse(result.stdout);
    assert.equal(receipt.bytes,48);assert.equal(receipt.state,'materialized_unverified');
    assert.equal(receipt.execution_authority_inferred,false);
    const mixed=decode(readFileSync(out));
    const expected=[.5,0,1,0,1.5-.75,-.75,2-1,-1,2.5-1.25,-1.25,3,0];
    expected.forEach((v,i)=>assert(Math.abs(v-mixed[i])<1e-6,'sample '+i));
    assert.equal(sha(readFileSync(first)),sha(a));assert.equal(sha(readFileSync(second)),sha(b));
    const refused=spawnSync(exe,['--samples','6','--channels','2','--output',out,
      '--stem',first,'0','0','6','1']);
    assert.notEqual(refused.status,0);assert.equal(readFileSync(out).length,48);
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('native refuses truncated, nonfinite, out-of-range and malformed audio without publishing',()=>{
  const {dir,exe}=make();
  try{
    const file=join(dir,'one'),out=join(dir,'out');writeFileSync(file,encode([1,2,3]));
    const args=['--samples','4','--channels','1','--output',out];
    for(const tail of [
      ['--stem',file,'0','0','4','1'],
      ['--stem',file,'3','0','2','1'],
      ['--stem',file,'0','0','3','NaN'],
      ['--stem',file,'0','0','3','infinity'],
      ['--stem',file,'0','0','3','9'],
    ]){
      assert.notEqual(spawnSync(exe,[...args,...tail]).status,0);
      assert.equal(existsSync(out),false);
    }
    writeFileSync(file,encode([1,NaN,3]));
    assert.notEqual(spawnSync(exe,[...args,'--stem',file,'0','0','3','1']).status,0);
    assert.equal(existsSync(out),false);
    const wrong=join(dir,'non-f32');writeFileSync(wrong,Buffer.alloc(7));
    assert.notEqual(spawnSync(exe,[...args,'--stem',wrong,'0','0','1','1']).status,0);
    assert.equal(existsSync(out),false);
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('native processes a million exact float32 samples using bounded blocks',()=>{
  const {dir,exe}=make();
  try{
    const n=1_000_000,src=join(dir,'source'),out=join(dir,'output');
    const body=Buffer.alloc(n*4);
    for(let i=0;i<n;i++)body.writeFloatLE((i%200-100)/200,i*4);
    writeFileSync(src,body);
    const r=spawnSync(exe,['--samples',String(n),'--channels','1','--output',out,
      '--stem',src,'0','0',String(n),'1'],{encoding:'utf8'});
    assert.equal(r.status,0,r.stderr);
    assert.equal(readFileSync(out).equals(body),true);
    const receipt=JSON.parse(r.stdout);
    assert.equal(receipt.sample_frames,n);
    assert.equal(receipt.source_read_calls,Math.ceil(n/1024));
    assert.equal(receipt.source_explicit_seek_calls,1);
    assert.equal(receipt.sequential_seek_elisions,Math.ceil(n/1024)-1);
    assert.equal(receipt.source_bytes_read,n*4);
    console.log(JSON.stringify({workload:'synthetic one-million-float sequential stem',
      source_read_calls:receipt.source_read_calls,
      explicit_seeks:receipt.source_explicit_seek_calls,
      seeks_skipped:receipt.sequential_seek_elisions,
      output_exact_f32_bytes:true,
      performance_elapsed_gain_proven:false}));
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('browser WASM uses the exact allocation-free native f32 mix kernel',async t=>{
  const dir=mkdtempSync(join(tmpdir(),'aurekai-creator-wasm-'));
  try{
    const out=join(dir,'kernel.wasm');
    const cmd=spawnSync('clang',['--target=wasm32','-O3','-nostdlib','-ffp-contract=off',
      '-Wl,--no-entry','-Wl,--export=aurekai_mix_f32','-Wl,--export-memory',
      join(native,'mix_kernel.c'),'-o',out],{encoding:'utf8'});
    if(cmd.error||cmd.status!==0) {
      t.skip('clang wasm32/lld toolchain unavailable on this host: '+(cmd.stderr||cmd.error?.message||''));
      return;
    }
    const module=await WebAssembly.instantiate(readFileSync(out));
    const memory=module.instance.exports.memory;
    const mix=module.instance.exports.aurekai_mix_f32;
    const floats=new Float32Array(memory.buffer);
    floats.set([1,2,3,4],32);
    floats.set([2,2,2,2],64);
    mix(32*4,64*4,0.5,4);
    assert.deepEqual(Array.from(floats.slice(32,36)),[2,3,4,5]);
    assert.equal(typeof mix,'function');
    console.log(JSON.stringify({kernel:'aurekai_mix_f32',
      backend:'WASM',native_code_shared:true,samples:4}));
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('per-stem sequential cursors preserve ordered overlapping source windows',()=>{
  const {dir,exe}=make();
  try{
    const n=8192,first=join(dir,'one'),second=join(dir,'two'),out=join(dir,'out');
    const data=Buffer.alloc(n*4);
    for(let i=0;i<n;i++)data.writeFloatLE((i%128)/128,i*4);
    writeFileSync(first,data);writeFileSync(second,data);
    const args=['--samples','6144','--channels','1','--output',out,
      '--stem',first,'0','100','6144','0.5',
      '--stem',second,'512','300','4096','0.25'];
    const run=spawnSync(exe,args,{encoding:'utf8'});
    assert.equal(run.status,0,run.stderr);
    const result=readFileSync(out);
    assert.equal(result.byteLength,6144*4);
    for(const index of [0,511,512,1023,1024,3072,4607,4608,6143]){
      const left=(data.readFloatLE((100+index)*4))*0.5;
      const right=(index>=512&&index<4608)?(data.readFloatLE((300+index-512)*4))*0.25:0;
      // f32 output performs a float store after each stem, not a double sum.
      const expected=Math.fround(Math.fround(left)+Math.fround(right));
      assert.equal(result.readFloatLE(index*4),expected,'sample '+index);
    }
    const receipt=JSON.parse(run.stdout);
    assert(receipt.source_read_calls>receipt.source_explicit_seek_calls);
    assert.equal(receipt.source_explicit_seek_calls,2);
    assert.equal(receipt.sequential_seek_elisions,
      receipt.source_read_calls-receipt.source_explicit_seek_calls);
  }finally{rmSync(dir,{recursive:true,force:true});}
});

test('event-driven 64-stem schedule preserves byte-exact mixing while eliminating inactive probes',()=>{
  const {dir,exe}=make();
  try {
    const frames=1_000_000,block=1024,stems=64,source=join(dir,'source.f32'),out=join(dir,'out.f32');
    const sourceSamples=Array.from({length:32},(_,i)=>(i-16)/32);
    writeFileSync(source,encode(sourceSamples));
    const argv=['--samples',String(frames),'--channels','1','--output',out];
    for(let i=0;i<stems;i++){
      argv.push('--stem',source,String(i*12*block),'0','32','0.5');
    }
    const result=spawnSync(exe,argv,{encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);
    const receipt=JSON.parse(result.stdout);
    const legacy=Math.ceil(frames/block)*stems;
    assert.equal(receipt.stems,64);
    assert.equal(receipt.legacy_stem_overlap_probes,legacy);
    assert.equal(receipt.stem_overlap_probes,stems);
    assert.equal(receipt.stem_overlap_probes_avoided,legacy-stems);
    assert.equal(receipt.source_read_calls,stems);
    const expected=Buffer.alloc(frames*4);
    for(let i=0;i<stems;i++){
      for(let j=0;j<sourceSamples.length;j++)
        expected.writeFloatLE(Math.fround(sourceSamples[j]*0.5),(i*12*block+j)*4);
    }
    assert.equal(readFileSync(out).equals(expected),true);
    console.log(JSON.stringify({workload:'synthetic 64 sparse stems / one million samples',
      legacy_overlap_probes:legacy,actual_overlap_probes:stems,
      overlap_probes_avoided:legacy-stems,source_f32_bytes_exact:true,
      elapsed_time_speedup_proven:false}));
  } finally {rmSync(dir,{recursive:true,force:true});}
});

test('stems inside the same block retain original accumulation order and boundaries',()=>{
  const {dir,exe}=make();
  try {
    const frames=2049,body=encode(Array.from({length:2049},(_,i)=>(i%17-8)/9));
    const source=join(dir,'source.f32'),out=join(dir,'out.f32');
    writeFileSync(source,body);
    const args=['--samples',String(frames),'--channels','1','--output',out,
      '--stem',source,'900','100','400','0.5',
      '--stem',source,'902','150','7','-0.25',
      '--stem',source,'903','90','3','1.5',
      '--stem',source,'1024','400','1025','0.125'];
    const r=spawnSync(exe,args,{encoding:'utf8'});
    assert.equal(r.status,0,r.stderr);
    const expected=Buffer.alloc(frames*4);
    const active=[
      {at:900,from:100,n:400,gain:0.5},
      {at:902,from:150,n:7,gain:-0.25},
      {at:903,from:90,n:3,gain:1.5},
      {at:1024,from:400,n:1025,gain:0.125}
    ];
    for(let pos=0;pos<frames;pos++){
      let outSample=0;
      for(const item of active){
        if(pos>=item.at&&pos<item.at+item.n) {
          const read=body.readFloatLE((item.from+pos-item.at)*4);
          outSample=Math.fround(outSample+Math.fround(read*item.gain));
        }
      }
      expected.writeFloatLE(outSample,pos*4);
    }
    assert.equal(readFileSync(out).equals(expected),true);
    assert(JSON.parse(r.stdout).stem_overlap_probes>0);
  }finally{rmSync(dir,{recursive:true,force:true});}
});
