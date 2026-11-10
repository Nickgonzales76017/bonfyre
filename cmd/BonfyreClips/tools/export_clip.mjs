import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,renameSync,rmSync,existsSync,realpathSync,statSync,openSync,readSync,closeSync,fstatSync,constants} from 'node:fs';
import {resolve,join,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
export function sha256File(path){
  const fd=openSync(path,constants.O_RDONLY|constants.O_NONBLOCK);
  try{
    const before=fstatSync(fd,{bigint:true});
    if(!before.isFile())throw Error('hash input must be a regular file');
    const hash=createHash('sha256'),buffer=Buffer.allocUnsafe(1024*1024);
    let size=0n;
    for(;;){
      let count;
      try{count=readSync(fd,buffer,0,buffer.length,null);}
      catch(error){if(error.code==='EINTR')continue;throw error;}
      if(!count)break;
      hash.update(buffer.subarray(0,count));size+=BigInt(count);
    }
    const after=fstatSync(fd,{bigint:true}),named=statSync(path,{bigint:true});
    if(size!==before.size||['dev','ino','size','mtimeNs','ctimeNs'].some(key=>before[key]!==after[key]||before[key]!==named[key]))
      throw Error('file changed while hashing');
    return hash.digest('hex');
  }finally{closeSync(fd);}
}
const run=(bin,args)=>{const r=spawnSync(bin,args,{encoding:'utf8',maxBuffer:2*1024*1024});
  if(r.error||r.status!==0)throw Error(bin+': '+(r.error?.message||r.stderr?.slice(-2000)||'failed'));
  return r.stdout;};
const text=(s,max)=>typeof s==='string'&&s.trim()&&s.length<=max&&!/[\x00-\x08\x0b-\x1f%]/.test(s);
const local=p=>{if(typeof p!=='string'||!p||!existsSync(p)||!statSync(p).isFile())throw Error('input must be an existing local file');return realpathSync(p);};
export function validatePlan(p){
  if(!p||!Number.isFinite(p.start)||!Number.isFinite(p.end)||p.start<0||p.end<=p.start||p.end-p.start>180)throw Error('clip requires 0 <= start < end, at most 180 seconds');
  if(!text(p.title,80)||!text(p.brand,50)||!text(p.credit,100))throw Error('title, brand and credit are required bounded text');
  if(!Array.isArray(p.captions)||p.captions.length>100)throw Error('captions must be a bounded optional sequence');
  let previous=0;
  for(const c of p.captions){
    if(!Number.isFinite(c.start)||!Number.isFinite(c.end)||c.start<previous||c.end<=c.start||c.end>p.end-p.start+0.001||!text(c.text,110))throw Error('captions must be ordered, nonoverlapping and within the clip');
    previous=c.end;
  }
  if(p.crop_x!==undefined&&(!Number.isFinite(p.crop_x)||p.crop_x<0||p.crop_x>1))throw Error('crop_x must be 0..1');
  return p;
}
const stamp=n=>{let ms=Math.round(n*1000);const h=Math.floor(ms/3600000);ms%=3600000;const m=Math.floor(ms/60000);ms%=60000;const s=Math.floor(ms/1000);return [h,m,s].map(x=>String(x).padStart(2,'0')).join(':')+','+String(ms%1000).padStart(3,'0');};
export function srt(captions){return captions.map((c,i)=>(i+1)+'\n'+stamp(c.start)+' --> '+stamp(c.end)+'\n'+c.text+'\n').join('\n');}
export const vtt = captions => "WEBVTT\n\n" + captions.map((c,i) =>
  (i+1)+"\n"+stamp(c.start).replace(",",".")+" --> "+stamp(c.end).replace(",",".")+"\n"+c.text+"\n").join("\n");
const htmlEsc = s => String(s ?? "").replace(/[&<>"']/g, ch =>
  ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[ch]));
export function reviewHtml(p, receipt) {
  const buttons = p.captions.map((c,i) =>
    '<button type="button" data-seek="'+c.start.toFixed(3)+'"><b>'+
    String(i+1).padStart(2,"0")+'</b><span>'+htmlEsc(c.text)+'</span></button>').join("\n");
  const ref = p.source_reference && /^https?:\/\//.test(p.source_reference) &&
    !/[\x00-\x20"'<>]/.test(p.source_reference) ?
    '<a href="'+htmlEsc(p.source_reference)+'" rel="noopener noreferrer">Provided source reference (unverified) ↗</a>' :
    '<p>Original source URL not supplied; exported from a local media file.</p>';
  return [
    '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">',
    '<meta name="robots" content="noindex,nofollow"><title>'+htmlEsc(p.title)+' — Aurekai Clip Review</title>',
    '<style>*{box-sizing:border-box}body{margin:0;background:#0d130f;color:#e6eee2;font:15px/1.55 system-ui,sans-serif}a{color:#b5df32}main{max-width:1180px;margin:0 auto;padding:36px 22px}small{color:#b5df32;letter-spacing:.13em;text-transform:uppercase;font:12px monospace}h1{letter-spacing:-.055em;line-height:1.08;font-size:clamp(32px,5vw,65px);margin:13px 0 10px}.lede{max-width:650px;color:#a8b9a7}.grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(260px,.65fr);gap:24px;margin-top:30px}.pane{background:#17251b;border:1px solid #375139;border-radius:15px;padding:18px}.player{width:100%;max-height:76vh;aspect-ratio:9/16;background:black;border-radius:9px}.cues{display:grid;gap:8px;max-height:370px;overflow:auto}.cues button{display:flex;gap:15px;text-align:left;color:#e8f2df;background:#24352a;border:1px solid #456145;border-radius:9px;padding:13px;min-height:44px;cursor:pointer}.cues button b{color:#b5df32}.cues button:hover,.cues button:focus-visible{border-color:#b5df32}.facts{font:12px/1.75 monospace;overflow-wrap:anywhere;color:#b3c8b0}.actions{display:flex;flex-wrap:wrap;gap:12px}.actions a{padding:11px 14px;background:#b5df32;color:#112116;border-radius:8px;font-weight:700}.warning{border-left:3px solid #ecb86c;padding-left:12px;color:#e2c994;margin:18px 0}@media(max-width:800px){.grid{grid-template-columns:1fr}}@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto}}</style></head><body>',
    '<main><small>Aurekai / native media review</small><h1>'+htmlEsc(p.title)+'</h1>',
    '<p class="lede">'+htmlEsc(p.brand)+' · '+htmlEsc(p.credit)+' · '+receipt.duration_seconds.toFixed(2)+' seconds, verified decoded MP4.</p>',
    '<div class="grid"><section class="pane"><video class="player" controls playsinline preload="metadata" poster="preview.png"><source src="clip.mp4" type="video/mp4"><track kind="captions" src="captions.vtt" srclang="und" label="Provided captions (language not verified)"></video>',
    '<div class="actions"><a href="clip.mp4" download>Download video</a><a href="captions.srt" download>Download captions</a></div>',
    '<p class="warning">Private export for review. External rights, caption accuracy and publication permission have not been established by this operation.</p></section>',
    '<section class="pane"><h2>Transcript navigation</h2><div class="cues">'+(buttons || '<p>No time-aligned transcript supplied. You can still review the actual video.</p>')+'</div>',
    '<h2>Source and readback</h2>'+ref+'<div class="facts"><p>Source SHA256<br>'+receipt.source_sha256+'</p><p>Decoded output SHA256<br>'+receipt.output_sha256+'</p>',
    '<p>Start: '+p.start.toFixed(3)+'s · End: '+p.end.toFixed(3)+'s · Caption cues: '+p.captions.length+'</p></div></section></div></main>',
    '<script>document.querySelectorAll("[data-seek]").forEach(b=>b.addEventListener("click",()=>{const v=document.querySelector("video");v.currentTime=Number(b.dataset.seek);void v.play()}));</script></body></html>'
  ].join("\n");
}
export function exportClip(plan,out,{ffmpeg=process.env.BONFYRE_FFMPEG||'ffmpeg',ffprobe=process.env.BONFYRE_FFPROBE||'ffprobe',magick=process.env.BONFYRE_MAGICK||'magick'}={}){
  const p=validatePlan(plan),source=local(p.source),font=local(p.font),logo=p.logo?local(p.logo):null;
  const duration=p.end-p.start;
  const sourceDigest=sha256File(source);
  const info=JSON.parse(run(ffprobe,['-v','error','-protocol_whitelist','file,pipe','-show_streams','-show_format','-of','json',source]));
  if(!info.streams?.some(s=>s.codec_type==='video')||!info.streams.some(s=>s.codec_type==='audio'))throw Error('source must contain video and audio');
  if(!Number.isFinite(Number(info.format?.duration))||p.end>Number(info.format.duration)+0.05)throw Error('clip exceeds verified source duration');
  out=resolve(out);if(existsSync(out))throw Error('output directory already exists; choose a new version');
  const published=out;
  if(!existsSync(dirname(published)))throw Error('output parent must exist');
  out=mkdtempSync(join(dirname(published),'.'+basename(published)+'.staging-'));
  let committed=false;
  try {
  const assets=join(out,'assets');mkdirSync(assets);
  const makeText=(name,value,height,size,color='#f4f2eb')=>{
    const input=join(assets,name+'.txt'),output=join(assets,name+'.png');writeFileSync(input,value,{mode:0o600});
    run(magick,['-background','#101820','-fill',color,'-font',font,'-pointsize',String(size),'-size','656x'+height,'-gravity','center','caption:@'+input,'-gravity','center','-extent','720x'+height,output]);return output;
  };
  const images=[
    {path:makeText('brand',p.brand,60,23,'#d6b67c'),y:12},
    {path:makeText('title',p.title,140,42),y:72},
    {path:makeText('credit',p.credit,60,18,'#b6bdc3'),y:1190},
    ...p.captions.map((c,i)=>({path:makeText('caption-'+i,c.text,200,37),y:960,start:c.start,end:c.end}))
  ];
  if(logo){const file=join(assets,'logo.png');run(magick,[logo,'-resize','120x48>',file]);images.push({path:file,y:20,x:24});}
  // Text never enters FFmpeg filter grammar or a shell.
  const args=['-hide_banner','-loglevel','error','-nostdin','-protocol_whitelist','file,pipe','-ss',String(p.start),'-i',source];
  for(const item of images)args.push('-i',item.path);
  const filters=['[0:v]setpts=PTS-STARTPTS,scale=720:720:force_original_aspect_ratio=increase,crop=720:720:(iw-ow)*'+(p.crop_x??0.5)+':(ih-oh)/2,setsar=1,pad=720:1280:0:220:color=0x101820[v0]'];
  images.forEach((item,i)=>filters.push('[v'+i+']['+(i+1)+':v]overlay='+(item.x??0)+':'+item.y+(item.start===undefined?'':":enable='gte(t,"+item.start+')*lt(t,'+item.end+")'")+'[v'+(i+1)+']'));
  args.push('-filter_complex',filters.join(';'),'-map','[v'+images.length+']','-map','0:a:0','-t',String(duration),'-r','30','-c:v','libx264','-preset','fast','-crf','20','-pix_fmt','yuv420p','-af','asetpts=PTS-STARTPTS,loudnorm=I=-16:TP=-1.5:LRA=11','-c:a','aac','-b:a','128k','-movflags','+faststart',join(out,'clip.mp4'));
  writeFileSync(join(out,'captions.srt'),srt(p.captions),{mode:0o600});
  writeFileSync(join(out,'captions.vtt'),vtt(p.captions),{mode:0o600});
  writeFileSync(join(out,'timeline.edl'),[
    '# Aurekai verified private clip decision',
    'source_sha256 ' + sourceDigest, 'source_in_seconds ' + p.start.toFixed(3),
    'source_out_seconds ' + p.end.toFixed(3), 'caption_cues ' + p.captions.length
  ].join('\n')+'\n',{mode:0o600});
  run(ffmpeg,args);
  const output=join(out,'clip.mp4'),result=JSON.parse(run(ffprobe,['-v','error','-show_streams','-show_format','-of','json',output]));
  const video=result.streams.find(s=>s.codec_type==='video');
  if(video?.width!==720||video?.height!==1280||!result.streams.some(s=>s.codec_type==='audio')||Math.abs(Number(result.format.duration)-duration)>0.15)throw Error('rendered dimensions, audio or duration failed verification');
  run(ffmpeg,['-v','error','-xerror','-i',output,'-f','null','-']);
  run(ffmpeg,['-hide_banner','-loglevel','error','-ss',String(Math.min(duration/2,5)),'-i',output,'-frames:v','1',join(out,'preview.png')]);
  if(sha256File(source)!==sourceDigest)throw Error('source changed during media rendering; export refused');
  const receipt={schema:'bonfyre.clip.export.v1',source,source_sha256:sourceDigest,source_reference:p.source_reference||null,start:p.start,end:p.end,
    output:join(published,'clip.mp4'),output_sha256:sha256File(output),caption_count:p.captions.length,
    video:{width:video.width,height:video.height,codec:video.codec_name},duration_seconds:Number(result.format.duration),
    decode_verified:true,rights_review:'not_established',publication_authorized:false,
    logo_sha256:logo?sha256File(logo):null,
    cannot_infer:'Decoded media does not establish caption accuracy, rights, approval, publication permission or accepted delivery.'};
  writeFileSync(join(out,'receipt.json'),JSON.stringify(receipt,null,2)+'\n',{mode:0o600});
  writeFileSync(join(out,'index.html'),reviewHtml(p,receipt),{mode:0o600});
  if(existsSync(published))throw Error('output created during rendering; refusing replacement');
  renameSync(out,published);
  try{
    if(sha256File(join(published,'clip.mp4'))!==receipt.output_sha256)
      throw Error('published clip bytes failed independent output readback');
  }catch(error){
    rmSync(published,{recursive:true,force:true});
    throw error;
  }
  committed=true;
  return receipt;
  } finally {if(!committed)rmSync(out,{recursive:true,force:true});}
}
if(import.meta.main){try{
  if(process.argv.length!==4)throw Error('usage: bonfyre-clips export PLAN.json OUT_DIR');
  console.log(JSON.stringify(exportClip(JSON.parse(readFileSync(process.argv[2],'utf8')),process.argv[3]),null,2));
}catch(e){console.error(e.message);process.exitCode=1;}}
