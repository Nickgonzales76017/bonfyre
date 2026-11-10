import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,existsSync,copyFileSync,mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {validatePlan,srt,exportClip} from '../tools/export_clip.mjs';
const plan={start:0,end:1,title:'A short sample',brand:'Practice edit',credit:'Synthetic test footage',captions:[{start:0,end:0.5,text:'First phrase.'},{start:0.5,end:1,text:'Second phrase.'}]};
test('invalid cuts and overlapping, empty or out-of-range captions refuse',()=>{
  expect(validatePlan(plan)).toBe(plan);
  expect(validatePlan({...plan,captions:[]})).toMatchObject({captions:[]}); // private exports need not fabricate a transcript
  for(const patch of [{start:-1},{end:0},{end:181},{crop_x:2},{title:'bad%[fx:1]'},{captions:[{start:0,end:2,text:'too long'}]},{captions:[{start:0,end:0.8,text:'A'},{start:0.5,end:1,text:'B'}]}]){
    expect(()=>validatePlan({...plan,...patch})).toThrow();
  }
  expect(srt(plan.captions)).toContain('00:00:00,500 --> 00:00:01,000');
});
test('installed native exporter creates and decodes portrait video and refuses overwriting or empty media',()=>{
  const dir=mkdtempSync(join(tmpdir(),'clip export '));
  const run=(bin,args,env)=>{const r=spawnSync(bin,args,{encoding:'utf8',env:env||process.env});if(r.status!==0)throw Error(r.stderr);return r;};
  try{
    const source=join(dir,'source.mp4'),font=process.env.BONFYRE_TEST_FONT||'/System/Library/Fonts/Supplemental/Arial.ttf';
    run('ffmpeg',['-v','error','-f','lavfi','-i','testsrc2=size=320x240:rate=30','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','1','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac',source]);
    const p={...plan,source,font},file=join(dir,'plan.json'),prefix=join(dir,'installed'),out=join(dir,'render');
    writeFileSync(file,JSON.stringify(p));
    run('make',['-C',join(import.meta.dir,'..'),'install-export','MANAGED_PREFIX='+prefix]);
    const r=run(join(prefix,'bin/bonfyre-clips'),['export',file,out],{...process.env,BONFYRE_BUN_BINARY:process.execPath,BONFYRE_CLIP_EXPORT:join(prefix,'lib/clips/export_clip.mjs')});
    const receipt=JSON.parse(r.stdout);
    for(const [field,path] of [['source_sha256',source],['output_sha256',join(out,'clip.mp4')]]){
      expect(receipt[field]).toBe(run('shasum',['-a','256',path]).stdout.trim().split(/\s/)[0]);
    }
    expect(receipt.decode_verified).toBe(true);expect(receipt.video).toEqual({width:720,height:1280,codec:'h264'});
    expect(receipt.duration_seconds).toBeCloseTo(1,1);expect(receipt.caption_count).toBe(2);
    expect(readFileSync(join(out,'captions.srt'),'utf8')).toContain('Second phrase.');
    expect(existsSync(join(out,'preview.png'))).toBe(true);
    expect(()=>exportClip(p,out)).toThrow('already exists');
    expect(()=>exportClip({...p,end:2},join(dir,'too-long'))).toThrow('source duration');
    const empty=join(dir,'empty.mp4');writeFileSync(empty,Buffer.alloc(0));
    expect(()=>exportClip({...p,source:empty},join(dir,'empty-out'))).toThrow();
    expect(existsSync(join(dir,'empty-out'))).toBe(false);
    if(process.env.BONFYRE_CLIP_WITNESS_DIR){
      mkdirSync(process.env.BONFYRE_CLIP_WITNESS_DIR,{recursive:true});
      for(const name of ['preview.png','clip.mp4','receipt.json'])copyFileSync(join(out,name),join(process.env.BONFYRE_CLIP_WITNESS_DIR,name));
    }
  }finally{rmSync(dir,{recursive:true,force:true})}
});
