// Batch private media cut, using BonfyreClips native discovery as the ranking
// owner and the decoded FFmpeg exporter as material. Never a public release.
import { existsSync, mkdtempSync, mkdirSync, readFileSync, writeFileSync,
  renameSync, rmSync } from "node:fs";
import { join, resolve, dirname, basename } from "node:path";
import { spawnSync } from "node:child_process";
import { parseSrt, parseArgs } from "./cut_clip.mjs";
import { sha256File, exportClip } from "./export_clip.mjs";
const esc = s => String(s ?? "").replace(/[&<>"']/g, ch =>
  ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[ch]));
function num(s, min, max, type) {
  const n=Number(s);
  if(!Number.isFinite(n)||n<min||n>max||(type==="integer"&&!Number.isInteger(n)))
    throw Error("invalid "+type+" range: "+s);
  return n;
}
export function parseOptions(argv) {
  const fields=new Set(["source","captions","out","title","credit","font",
    "brand","source-ref","top","min-duration","max-duration"]);
  const opts={};
  for(let i=0;i<argv.length;i+=2){
    const field=argv[i]?.replace(/^--/,"");
    if(!argv[i]?.startsWith("--")||!fields.has(field)||!argv[i+1]||opts[field])
      throw Error("unknown, repeated or missing batch clip argument: "+(argv[i]||""));
    opts[field]=argv[i+1];
  }
  for(const field of ["source","captions","out","title","credit"])
    if(!opts[field])throw Error("missing --"+field);
  opts.top=num(opts.top??"3",1,5,"integer");
  opts.min=num(opts["min-duration"]??"4",.2,180,"duration");
  opts.max=num(opts["max-duration"]??"90",opts.min,180,"duration");
  return opts;
}
function run(bin,args) {
  const r=spawnSync(bin,args,{encoding:"utf8",timeout:600000,maxBuffer:4*1024*1024});
  if(r.error||r.status!==0)throw Error(bin+" refused: "+(r.stderr||r.error?.message||r.stdout||"unknown").slice(-900));
  return r.stdout;
}
const clipId=i=>"clip-"+String(i+1).padStart(2,"0");
function landing(opts,clips,digest) {
  return [
    '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow">',
    '<title>'+esc(opts.title)+' — Aurekai Native Clip Review</title>',
    '<style>*{box-sizing:border-box}body{margin:0;background:#0d130f;color:#e6eee2;font:15px/1.55 system-ui,sans-serif}main{max-width:1200px;margin:auto;padding:36px 20px}small{color:#b5df32;font:12px monospace;letter-spacing:.12em}h1{font-size:clamp(33px,6vw,65px);line-height:1.05;letter-spacing:-.055em}h2{font-size:24px;margin:0 0 8px}.list{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:18px}.card{border:1px solid #456245;background:#1a2a20;border-radius:15px;padding:14px}video{width:100%;max-height:430px;background:#050805;border-radius:10px;aspect-ratio:9/16}.card a{color:#b5df32;font-weight:700}.source{font:12px monospace;color:#b3c4ae;overflow-wrap:anywhere}.notice{color:#e0c894;border-left:3px solid #dcae57;padding-left:13px}.count{color:#b5df32;font:12px monospace}.actions{display:flex;gap:13px;flex-wrap:wrap;margin:16px 0}.actions a{padding:10px 12px;border:1px solid #b5df32;border-radius:8px}</style></head><body>',
    '<main><small>Aurekai / real media material · native ranked cuts</small><h1>'+esc(opts.title)+'</h1>',
    '<p>Selected and rendered from locally supplied, time-coded media. These are playable private exports, not an approximation of the requested cuts.</p>',
    '<p class="notice">Review every caption, source right, and final edit. No remote posting, public release, acknowledgement or external consent occurred.</p>',
    '<div class="list">'+clips.map((clip,i)=>{
      const path=clipId(i)+"/",name=esc(clip.title),description=esc(clip.text);
      return '<article class="card"><span class="count">CUT '+String(i+1).padStart(2,"0")+
        ' · Heuristic native score '+clip.score.toFixed(1)+'</span><h2>'+name+'</h2>'+
        '<video controls playsinline preload="metadata" poster="'+path+'preview.png"><source src="'+path+'clip.mp4" type="video/mp4">'+
        '<track src="'+path+'captions.vtt" kind="captions" srclang="und" label="Unverified caption language"></video>'+
        '<p>'+description+'</p><p class="source">Source '+clip.start.toFixed(3)+'s — '+clip.end.toFixed(3)+'s</p>'+
        '<div class="actions"><a href="'+path+'index.html">Inspect timeline ↗</a><a href="'+path+'clip.mp4" download>Download MP4</a></div></article>';
    }).join("\n")+'</div><p class="source">Source SHA256: '+digest+'</p></main></body></html>'
  ].join("\n");
}
export function harvest(argv) {
  const opts=parseOptions(argv),source=resolve(opts.source),out=resolve(opts.out);
  if(!existsSync(source)||!existsSync(opts.captions))
    throw Error("existing local video and time-coded SRT required");
  if(existsSync(out))throw Error("output already exists; source archive must not be replaced");
  if(!existsSync(dirname(out)))throw Error("output parent must already exist");
  const sourceDigest=sha256File(source);
  const all=parseSrt(opts.captions,0,86400,4096);
  if(!all.length)throw Error("no source-timed caption segments");
  const scratch=mkdtempSync(join(dirname(out),".aurekai-discovery-"));
  let staging=null,renamed=false;
  try {
    const transcript=join(scratch,"transcript.json"),discoverDir=join(scratch,"native");
    mkdirSync(discoverDir);
    writeFileSync(transcript,JSON.stringify({segments:all}),{mode:0o600});
    // Resolution works both in the checkout (tools/../bonfyre-clips) and
    // the managed installation (lib/clips/../../bin/bonfyre-clips).
    const candidate=[process.env.BONFYRE_CLIPS_BIN,
      resolve(import.meta.dir,"../bonfyre-clips"),
      resolve(import.meta.dir,"../../bin/bonfyre-clips")].filter(Boolean);
    const native=candidate.find(file=>existsSync(file));
    if(!native)throw Error("native clip scoring binary is not installed");
    run(native,["discover",transcript,discoverDir,"--top",String(opts.top),
      "--min-dur",String(opts.min),"--max-dur",String(opts.max)]);
    const result=JSON.parse(readFileSync(join(discoverDir,"clips.json"),"utf8"));
    const picked=result.clips||[];
    if(!picked.length)throw Error("native clip discovery returned no ranked cuts for this source");
    staging=mkdtempSync(join(dirname(out),"."+basename(out)+".staging-"));
    const reviewed=[];
    for(let i=0;i<picked.length;i++){
      const c=picked[i];
      if(!Number.isFinite(c.start)||!Number.isFinite(c.end)||c.end<=c.start)
        throw Error("native scorer returned invalid clip coordinates");
      const args=["--source",source,"--start",String(c.start),"--end",String(c.end),
        "--out",join(staging,clipId(i)),"--title",opts.title+" · Cut "+(i+1),
        "--credit",opts.credit,"--brand",opts.brand||"Aurekai",
        "--captions",opts.captions];
      if(opts.font)args.push("--font",opts.font);
      if(opts["source-ref"])args.push("--source-ref",opts["source-ref"]);
      const {plan,output}=parseArgs(args);
      const receipt=exportClip(plan,output);
      if(receipt.source_sha256!==sourceDigest)throw Error("source changed while creating cuts");
      const finalReceipt={...receipt,output:join(out,clipId(i),"clip.mp4")};
      writeFileSync(join(output,"receipt.json"),JSON.stringify(finalReceipt,null,2)+"\n",{mode:0o600});
      reviewed.push({start:c.start,end:c.end,score:Number(c.score)||0,
        title:"Cut "+(i+1),text:String(c.text||"")});
    }
    if(sha256File(source)!==sourceDigest)throw Error("source changed during batch rendering");
    writeFileSync(join(staging,"index.html"),landing(opts,reviewed,sourceDigest),{mode:0o600});
    if(existsSync(out))throw Error("another process created the output path");
    renameSync(staging,out);renamed=true;
    for(let i=0;i<reviewed.length;i++){
      const file=join(out,clipId(i),"clip.mp4");
      const record=JSON.parse(readFileSync(join(out,clipId(i),"receipt.json"),"utf8"));
      if(sha256File(file)!==record.output_sha256)throw Error("published clip readback mismatch");
    }
    return {review:join(out,"index.html"),cuts:reviewed.length,source_sha256:sourceDigest};
  }catch(error){
    if(renamed)rmSync(out,{force:true,recursive:true});
    throw error;
  }finally{
    rmSync(scratch,{recursive:true,force:true});
    if(staging&&!renamed)rmSync(staging,{recursive:true,force:true});
  }
}
if(import.meta.main){
  try{const outcome=harvest(process.argv.slice(2));
    process.stdout.write("Native-ranked private clips rendered, decoded and read back.\n"+
      "Open: "+outcome.review+"\nClips: "+outcome.cuts+
      "\nSource SHA256: "+outcome.source_sha256+
      "\nPublication and rights: unreviewed, not authorized.\n");
  }catch(error){console.error("clip harvest refused: "+error.message);process.exitCode=1;}
}
