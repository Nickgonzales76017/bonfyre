import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { parseArgs } from "../tools/cut_clip.mjs";
import { withChrome } from "../../../integrations/browser/capture/chrome-session.mjs";
import { vtt } from "../tools/export_clip.mjs";
const binary = resolve(import.meta.dir,"../bonfyre-clips");
const font = process.env.BONFYRE_TEST_FONT ||
  "/System/Library/Fonts/Supplemental/Arial.ttf";
const argsFor = (source,out,extra=[]) => ["cut","--source",source,"--start","1.0",
  "--end","2.2","--out",out,"--title","City meeting excerpt",
  "--credit","Permitted local fixture", "--font",font,...extra];
function spawn(bin,args,env=process.env) {
  return spawnSync(bin,args,{encoding:"utf8",env,maxBuffer:2*1024*1024,timeout:90000});
}
test("direct media cut contract refuses arbitrary or malformed inputs",()=>{
  expect(()=>parseArgs(["--source","anything"])).toThrow("missing");
  expect(()=>parseArgs(["--bogus","x"])).toThrow("unknown");
  expect(()=>parseArgs(["--title","something","--title","again"])).toThrow("duplicate");
  expect(vtt([{start:.1,end:.9,text:"Cue"}])).toContain("00:00:00.100 --> 00:00:00.900");
});
test("native cut exports playable, hashed private media and reviewable synchronized timeline",async ()=>{
  const work = mkdtempSync(join(tmpdir(),"aurekai-real-clip-"));
  try {
    const source=join(work,"source.mp4"),out=join(work,"reviewed-cut");
    const fixture=spawn("ffmpeg",["-v","error","-f","lavfi","-i","testsrc2=size=192x128:rate=24",
      "-f","lavfi","-i","sine=frequency=440:sample_rate=48000","-t","2.5",
      "-c:v","libx264","-pix_fmt","yuv420p","-c:a","aac",source]);
    expect(fixture.status).toBe(0);
    const captions=join(work,"meeting.srt");
    writeFileSync(captions,"1\n00:00:01,200 --> 00:00:01,800\nThe council meets.\n\n"
       +"2\n00:00:01,850 --> 00:00:02,400\nSecond <script>not executable</script>.\n");
    const prefix=join(work,".bonfyre");
    expect(spawn("make",["-C",resolve(import.meta.dir,".."),"install-export",
      "MANAGED_PREFIX="+prefix]).status).toBe(0);
    const env={...process.env,HOME:work,BONFYRE_BUN_BINARY:process.execPath};
    const real=spawn(join(prefix,"bin/bonfyre-clips"),argsFor(source,out,
      ["--captions",captions,"--source-ref","https://example.org/city/meeting"]),env);
    expect(real.status).toBe(0);
    expect(real.stdout).toContain("Decoded media clip created and independently verified.");
    expect(real.stdout).toContain("Rights and publication: not authorized");
    for(const file of ["index.html","clip.mp4","preview.png","captions.vtt",
      "captions.srt","timeline.edl","receipt.json"]) expect(existsSync(join(out,file))).toBe(true);
    expect(existsSync(join(out,"plan.json"))).toBe(false);
    const html=readFileSync(join(out,"index.html"),"utf8");
    expect(html).toContain('controls playsinline');
    expect(html).toContain('data-seek="0.200"');
    expect(html).toContain("&lt;script&gt;not executable&lt;/script&gt;");
    expect(html).toContain("rights, caption accuracy and publication permission");
    const subtitles=readFileSync(join(out,"captions.srt"),"utf8");
    expect(subtitles).toContain("00:00:00,200 --> 00:00:00,800");
    const receipt=JSON.parse(readFileSync(join(out,"receipt.json"),"utf8"));
    expect(receipt.decode_verified).toBe(true);
    expect(receipt.publication_authorized).toBe(false);
    expect(receipt.rights_review).toBe("not_established");
    expect(receipt.duration_seconds).toBeCloseTo(1.2,1);
    const check=spawn("shasum",["-a","256",join(out,"clip.mp4")]);
    expect(check.status).toBe(0);
    expect(check.stdout.split(/\s/)[0]).toBe(receipt.output_sha256);
    const ffprobe=spawn("ffprobe",["-v","error","-show_entries",
      "stream=codec_type,width,height","-of","csv=p=0",join(out,"clip.mp4")]);
    expect(ffprobe.status).toBe(0);
    expect(ffprobe.stdout).toContain("720,1280");
    // The result is not merely a receipt: a real browser must play the decoded
    // local video, navigate synchronized cues and work at a phone width.
    const exposed=new Set(["index.html","clip.mp4","preview.png","captions.vtt","captions.srt"]);
    const server=Bun.serve({hostname:"127.0.0.1",port:0,fetch(req){
      const name=new URL(req.url).pathname.slice(1);
      return exposed.has(name)?new Response(Bun.file(join(out,name)),{headers:{"Cache-Control":"no-store"}})
        :new Response("No such private artifact",{status:404});
    }});
    try {
      await withChrome(async ({open,events})=>{
        const tab=await open();
        const evalJs=async expression=>{
          const r=await tab.page("Runtime.evaluate",{expression,awaitPromise:true,returnByValue:true});
          if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));
          return r.result.value;
        };
        await tab.go("http://127.0.0.1:"+server.port+"/index.html");
        const readiness=await evalJs('new Promise((resolve,reject)=>{const v=document.querySelector("video");if(!v)return reject(Error("video missing"));if(v.readyState>=1)return resolve({duration:v.duration,controls:v.controls});v.onloadedmetadata=()=>resolve({duration:v.duration,controls:v.controls});v.onerror=()=>reject(Error("video could not load"));setTimeout(()=>reject(Error("video metadata timeout")),10000)})');
        expect(readiness.controls).toBe(true);
        expect(readiness.duration).toBeGreaterThan(1.0);
        const seek=await evalJs('new Promise(resolve=>{document.querySelector("[data-seek]").click();setTimeout(()=>resolve(document.querySelector("video").currentTime),100)})');
        expect(seek).toBeGreaterThan(0.17);
        expect(seek).toBeLessThan(0.6);
        expect(await evalJs('document.querySelectorAll(".cues button").length')).toBe(2);
        expect(await evalJs('document.querySelectorAll("script").length')).toBe(1);
        await tab.page("Emulation.setDeviceMetricsOverride",{width:390,height:800,deviceScaleFactor:1,mobile:true});
        const width=await evalJs('({viewport:document.documentElement.clientWidth,scroll:document.documentElement.scrollWidth})');
        expect(width.scroll).toBeLessThanOrEqual(width.viewport+1);
        expect(events.filter(e=>e.method==="Network.requestWillBeSent" &&
          !["GET","HEAD"].includes(e.params.request.method))).toHaveLength(0);
        expect(events.filter(e=>e.method==="Runtime.exceptionThrown")).toHaveLength(0);
      },{timeoutMs:45000,flags:["--autoplay-policy=no-user-gesture-required"]});
    } finally {server.stop(true);}
    // A rejected overwriting attempt never changes existing bytes.
    const before=receipt.output_sha256;
    const replay=spawn(join(prefix,"bin/bonfyre-clips"),argsFor(source,out),env);
    expect(replay.status).not.toBe(0);
    expect(spawn("shasum",["-a","256",join(out,"clip.mp4")]).stdout.split(/\s/)[0]).toBe(before);
    // Render failure is an all-or-nothing operation: no partial directory.
    const failed=join(work,"should-not-exist");
    const err=spawn(join(prefix,"bin/bonfyre-clips"),argsFor(source,failed),
      {...env,BONFYRE_FFMPEG:"ffmpeg-intentionally-missing"});
    expect(err.status).not.toBe(0);
    expect(existsSync(failed)).toBe(false);
    expect(readdirSync(work).some(name=>name.startsWith(".should-not-exist.staging-"))).toBe(false);
  } finally { rmSync(work,{recursive:true,force:true}); }
},120_000);
