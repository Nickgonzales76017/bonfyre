import { expect,test } from "bun:test";
import { mkdtempSync,readFileSync,writeFileSync,mkdirSync,rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runSitePublication,verifyWorkSamplesGuides,verifyWorkSamplesReadback } from "../site-publication.mjs";

test("push preserves native CMS schema and entry identity",async()=>{
 const dir=mkdtempSync(join(tmpdir(),"site-p2-push-")),file=join(dir,"content.json");
 try {
  const source={site_id:"sample",business:{name:"Sample"},pages_project:"sample",preview:true};
  writeFileSync(file,JSON.stringify(source));
  const calls=[],events=[];
  const out=await runSitePublication({verb:"push",file,dir,rows:()=>[],type:"service_site",ns:"service-site",
    local:async()=>source,cms:argv=>{calls.push(argv);return {data:{id:27}}},
    report:x=>events.push(x)});
  expect(out).toEqual({pushed:"sample",created:true,id:27});
  expect(calls[0].slice(0,5)).toEqual(["entry","create","service_site","--ns","service-site"]);
  expect(JSON.parse(calls[0][5]).content_json).toBe(JSON.stringify(source));
  expect(events).toEqual([out]);
 }finally{rmSync(dir,{recursive:true,force:true})}
});
test("pull and publish validate CMS source before any deploy",async()=>{
 const dir=mkdtempSync(join(tmpdir(),"site-p2-pull-")),file=join(dir,"content.json");
 try {
  const content={site_id:"sample",pages_project:"sample",preview:false};
  writeFileSync(file,JSON.stringify({site_id:"sample",preview:true}));
  const events=[],calls=[];
  const args={file,dir,rows:()=>[{site_id:"sample",id:8,content_json:JSON.stringify(content)}],
    validate:async()=>{},report:x=>events.push(x),cms:()=>{throw Error("unexpected write")}};
  const pulled=await runSitePublication({...args,verb:"pull"});
  expect(pulled.pulled).toBe("sample");
  expect(JSON.parse(readFileSync(file,"utf8"))).toEqual(content);
  const result=await runSitePublication({...args,verb:"publish",
    buildSite:async()=>({out:"/fixture/public"}),
    deploySite:async(...params)=>{calls.push(params);return {published:true}}});
  expect(result).toEqual({published:true});
  expect(calls).toEqual([["/fixture/public","sample",undefined]]);
  expect(events).toHaveLength(3);
  await expect(runSitePublication({...args,verb:"unknown"})).rejects.toThrow("unknown publication");
 }finally{rmSync(dir,{recursive:true,force:true})}
});

test("gallery publication refuses missing origin and missing linked guides before touching export",async()=>{
 const dir=mkdtempSync(join(tmpdir(),"site-guide-preflight-")),file=join(dir,"content.json");
 try{
  const initial={site_id:"aurekai-work-samples",preview:true};
  writeFileSync(file,JSON.stringify(initial));
  const content={site_id:"aurekai-work-samples",pages_project:"aurekai-work-samples",
   sections:[{items:[{title:"Use source programs",href:"https://docs.aurekai.work/source-program-workspace"}]}]};
  let deployed=0,requests=0;
  const args={verb:"publish",file,dir,rows:()=>[{id:3,site_id:initial.site_id,content_json:JSON.stringify(content)}],
   validate:async()=>{},cms:()=>{throw Error("unexpected CMS write");},
   buildSite:async()=>({out:"/fixture/public"}),deploySite:async()=>{deployed++;return {published:true}},
   guideRequest:async()=>{requests++;return new Response("missing",{status:404})}};
  await expect(runSitePublication(args)).rejects.toThrow("requires public_origin");
  expect(requests).toBe(0);
  content.public_origin="https://aurekai-work-samples.pages.dev";
  await expect(runSitePublication(args)).rejects.toThrow("linked guide unavailable");
  expect(requests).toBe(1);
  expect(deployed).toBe(0);
  expect(JSON.parse(readFileSync(file,"utf8"))).toEqual(initial);
  let readbacks=0;
  const published=await runSitePublication({...args,guideRequest:async()=>new Response("guide available"),
    publicReadback:async()=>{readbacks++;return {state:"LIVE_VERIFIED",files_checked:4}}});
  expect(published).toEqual({published:true,live_readback:{state:"LIVE_VERIFIED",files_checked:4}});
  expect(deployed).toBe(1);
  expect(readbacks).toBe(1);
  expect(JSON.parse(readFileSync(file,"utf8"))).toEqual(content);
 }finally{rmSync(dir,{recursive:true,force:true})}
});

test("gallery guide preflight inspects nested authored links without following cross-page redirects",async()=>{
 const guide="https://docs.aurekai.work/native-source-reuse";
 const content={site_id:"aurekai-work-samples",public_origin:"https://aurekai-work-samples.pages.dev",
  navigation:[{label:"Home",href:"/"}],
  pages:[{path:"guide",sections:[{kind:"hero",secondary:{label:"Native source",href:guide}}]}],
  footer:{terms:{href:guide}}};
 const hits=[];
 await expect(verifyWorkSamplesGuides(content,{request:async url=>{
   hits.push(url);
   return {ok:true,status:200,url:"https://docs.aurekai.work/irrelevant",body:null};
 }})).rejects.toThrow("linked guide unavailable");
 expect(hits).toEqual([guide]);
 await expect(verifyWorkSamplesGuides({...content,public_origin:"https://different.pages.dev"},{
   request:async()=>{throw Error("must not contact docs");}
 })).rejects.toThrow("does not match");
 expect((await verifyWorkSamplesGuides({...content,site_id:"other"},{
   request:async()=>{throw Error("must not contact docs");}
 })).checked).toBe(0);
 const result=await verifyWorkSamplesGuides(content,{request:async url=>{
   expect(url).toBe(guide);
   return {ok:true,status:200,url:guide+"/",body:null};
 }});
 expect(result).toEqual({checked:1});
});

test("gallery live readback checks built bytes and retains honest deployment failures",async()=>{
 const dir=mkdtempSync(join(tmpdir(),"site-public-readback-"));
 try{
  mkdirSync(join(dir,"guide"),{recursive:true});mkdirSync(join(dir,"assets"),{recursive:true});
  const files={"index.html":"<h1>Fresh homepage</h1>","guide/index.html":"<h1>Guide page</h1>",
   "assets/site.css":"body{font-size:16px}","robots.txt":"User-agent: *\nAllow: /\n",
   "sitemap.xml":"<urlset>current</urlset>"};
  for(const [path,body] of Object.entries(files))writeFileSync(join(dir,path),body);
  const content={site_id:"aurekai-work-samples",pages:[{path:"guide"}]};
  const seen=[];
  const request=async url=>{
   const path=new URL(url).pathname.replace(/^\//,"");
   const file=path?path.endsWith("/")?path+"index.html":path:"index.html";
   seen.push(file);
   return new Response(files[file]??"not found",{status:files[file]?200:404});
  };
  const live=await verifyWorkSamplesReadback(content,{out:dir},{request,attempts:1});
  expect(live).toEqual({state:"LIVE_VERIFIED",origin:"https://aurekai-work-samples.pages.dev",files_checked:5});
  expect(seen).toHaveLength(5);
  await expect(verifyWorkSamplesReadback(content,{out:dir},{
   request:async url=>new Response(new URL(url).pathname==="/sitemap.xml"?"stale":files[new URL(url).pathname.endsWith("/")?new URL(url).pathname.slice(1)+"index.html":new URL(url).pathname.slice(1)]??files["index.html"]),
   attempts:1
  })).rejects.toThrow("sitemap.xml (served bytes differ)");
  await expect(verifyWorkSamplesReadback(content,{out:dir},{request:async()=>({ok:true,url:"https://other.example/page",status:200,body:null}),attempts:1}))
   .rejects.toThrow("unexpected redirect");
 }finally{rmSync(dir,{recursive:true,force:true})}
});

test("accepted deployment with failed live readback is not reported as verified",async()=>{
 const dir=mkdtempSync(join(tmpdir(),"site-readback-failure-")),file=join(dir,"content.json");
 try{
  const content={site_id:"aurekai-work-samples",public_origin:"https://aurekai-work-samples.pages.dev",
   pages_project:"aurekai-work-samples",sections:[]};
  writeFileSync(file,JSON.stringify({site_id:content.site_id}));
  const events=[];let deployed=0;
  await expect(runSitePublication({verb:"publish",file,dir,
   rows:()=>[{id:3,site_id:content.site_id,content_json:JSON.stringify(content)}],
   validate:async()=>{},report:value=>events.push(value),
   guideRequest:async()=>{throw Error("No guide URLs were authored");},
   buildSite:async()=>({out:"/fixture/dist"}),
   deploySite:async()=>{deployed++;return {address:"https://aurekai-work-samples.pages.dev/",deployment_id:"deployment-accepted"};},
   publicReadback:async()=>{throw Error("served sitemap differs");}
  })).rejects.toThrow("served sitemap differs");
  expect(deployed).toBe(1);
  expect(events.at(-1)).toEqual({deployment_accepted:{
   address:"https://aurekai-work-samples.pages.dev/",deployment_id:"deployment-accepted"
  },live_readback:"UNVERIFIED",error:"served sitemap differs"});
  expect(events.some(e=>e.live_readback?.state==="LIVE_VERIFIED")).toBe(false);
 }finally{rmSync(dir,{recursive:true,force:true})}
});
