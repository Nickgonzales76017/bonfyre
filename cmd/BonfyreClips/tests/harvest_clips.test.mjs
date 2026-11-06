import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import { parseOptions } from "../tools/harvest_clips.mjs";
import { withChrome } from "../../../integrations/browser/capture/chrome-session.mjs";

const bin = resolve(import.meta.dir,"../bonfyre-clips");
function run(cmd,args,env=process.env) {
  return spawnSync(cmd,args,{env,encoding:"utf8",timeout:120000,maxBuffer:2*1024*1024});
}
const font=process.env.BONFYRE_TEST_FONT || "/System/Library/Fonts/Supplemental/Arial.ttf";
test("harvest is bound to a source and a real transcript, not arbitrary intent",()=>{
  expect(()=>parseOptions(["--topic","fire breathing dragon"])).toThrow("unknown");
  expect(()=>parseOptions(["--source","movie"])).toThrow("missing");
  expect(()=>parseOptions(["--source","movie","--captions","file",
    "--out","new","--title","yes","--credit","yes","--top","0"])).toThrow();
});
test("native-ranked source time ranges become real private video clips in an atomic gallery",async ()=>{
  const work=mkdtempSync(join(tmpdir(),"aurekai-clip-batch-"));
  try {
    const source=join(work,"meeting.mp4"), captions=join(work,"meeting.srt"), output=join(work,"gallery");
    const sourceRun=run("ffmpeg",["-v","error","-f","lavfi","-i","testsrc2=size=192x128:rate=24",
      "-f","lavfi","-i","sine=frequency=280:sample_rate=48000",
      "-t","9","-c:v","libx264","-pix_fmt","yuv420p","-c:a","aac",source]);
    expect(sourceRun.status).toBe(0);
    writeFileSync(captions,[
      "1\n00:00:00,000 --> 00:00:02,000\nThe problem is the damaged bridge.",
      "2\n00:00:02,000 --> 00:00:04,000\nWe need to start right now.",
      "3\n00:00:04,000 --> 00:00:06,000\nHere is the measured result.",
      "4\n00:00:06,000 --> 00:00:08,000\nDo this before the next meeting."
    ].join("\n\n")+"\n");
    const prefix=join(work,".bonfyre");
    expect(run("make",["-C",resolve(import.meta.dir,".."),"install-export",
      "MANAGED_PREFIX="+prefix]).status).toBe(0);
    const env={...process.env,HOME:work,BONFYRE_BUN_BINARY:process.execPath};
    const named=["harvest","--source",source,"--captions",captions,"--out",output,
      "--title","Dallas meeting highlights","--credit","Local authorized sample",
      "--font",font,"--top","2","--min-duration","4","--max-duration","4"];
    const done=run(join(prefix,"bin/bonfyre-clips"),named,env);
    expect(done.status).toBe(0);
    expect(done.stdout).toContain("Native-ranked private clips rendered");
    expect(done.stdout).toContain("Clips: 2");
    const gallery=readFileSync(join(output,"index.html"),"utf8");
    expect(gallery).toContain("native ranked cuts");
    expect(gallery).toContain('clip-01/clip.mp4');
    expect(gallery).toContain('clip-02/clip.mp4');
    expect(gallery).toContain("No remote posting, public release");
    for (let i=1;i<=2;i++){
      const part=join(output,"clip-"+String(i).padStart(2,"0"));
      expect(existsSync(join(part,"clip.mp4"))).toBe(true);
      expect(existsSync(join(part,"index.html"))).toBe(true);
      const receipt=JSON.parse(readFileSync(join(part,"receipt.json"),"utf8"));
      expect(receipt.decode_verified).toBe(true);
      expect(receipt.publication_authorized).toBe(false);
      expect(receipt.output).toBe(join(part,"clip.mp4"));
      expect(run("shasum",["-a","256",receipt.output]).stdout.split(/\s/)[0])
        .toBe(receipt.output_sha256);
      expect(readFileSync(join(part,"captions.vtt"),"utf8")).toContain("WEBVTT");
    }
    // Actual browser journey through the batch: play two clips, open the
    // individual timeline and seek from one caption at phone resolution.
    const allowed=new Set(["index.html", ...[1,2].flatMap(i=>{
      const prefix="clip-"+String(i).padStart(2,"0")+"/";
      return ["index.html","clip.mp4","preview.png","captions.vtt"].map(n=>prefix+n);
    })]);
    const server=Bun.serve({hostname:"127.0.0.1",port:0,async fetch(req){
      const path=decodeURIComponent(new URL(req.url).pathname).slice(1);
      if(!allowed.has(path))return new Response("Not found",{status:404});
      return new Response(Bun.file(join(output,path)),{headers:{"Cache-Control":"no-store"}});
    }});
    try {
      await withChrome(async ({open,events})=>{
        const tab=await open();
        const evaluate=async expression=>{
          const r=await tab.page("Runtime.evaluate",{expression,awaitPromise:true,returnByValue:true});
          if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));
          return r.result.value;
        };
        await tab.go("http://127.0.0.1:"+server.port+"/index.html");
        expect(await evaluate('document.querySelectorAll("video").length')).toBe(2);
        const meta=await evaluate('Promise.all([...document.querySelectorAll("video")].map(v=>new Promise((yes,no)=>{if(v.readyState>=1)return yes(v.duration);v.onloadedmetadata=()=>yes(v.duration);v.onerror=()=>no(Error("clip missing"));setTimeout(()=>no(Error("metadata timeout")),10000)})))');
        expect(meta.every(seconds=>seconds>3.9 && seconds<4.1)).toBe(true);
        await tab.page("Emulation.setDeviceMetricsOverride",{width:390,height:820,deviceScaleFactor:1,mobile:true});
        const bounds=await evaluate('({inner:document.documentElement.clientWidth,width:document.documentElement.scrollWidth})');
        expect(bounds.width).toBeLessThanOrEqual(bounds.inner+1);
        await tab.go("http://127.0.0.1:"+server.port+"/clip-02/index.html");
        expect(await evaluate('document.querySelectorAll("[data-seek]").length')).toBe(2);
        const moved=await evaluate('new Promise(done=>{const v=document.querySelector("video");document.querySelectorAll("[data-seek]")[1].click();setTimeout(()=>done(v.currentTime),110)})');
        expect(moved).toBeGreaterThan(1.8);
        expect(moved).toBeLessThan(2.3);
        expect(events.filter(e=>e.method==="Runtime.exceptionThrown")).toHaveLength(0);
        expect(events.filter(e=>e.method==="Network.requestWillBeSent"&&
          !["GET","HEAD"].includes(e.params.request.method))).toHaveLength(0);
      },{timeoutMs:50000,flags:["--autoplay-policy=no-user-gesture-required"]});
    } finally {server.stop(true);}
    // Idempotent display: never replace a previously reviewed batch.
    const repeat=run(join(prefix,"bin/bonfyre-clips"),named,env);
    expect(repeat.status).not.toBe(0);
    expect(existsSync(join(output,"clip-01/clip.mp4"))).toBe(true);
    // One real FFmpeg failure cancels the whole collection.
    const failed=join(work,"failed-gallery");
    const bad=run(join(prefix,"bin/bonfyre-clips"),named.map(x=>x===output?failed:x),
      {...env,BONFYRE_FFMPEG:"ffmpeg-intentionally-missing"});
    expect(bad.status).not.toBe(0);
    expect(existsSync(failed)).toBe(false);
    expect(readdirSync(work).some(n=>n.startsWith(".failed-gallery.staging-"))).toBe(false);
  } finally {rmSync(work,{recursive:true,force:true});}
},180_000);
