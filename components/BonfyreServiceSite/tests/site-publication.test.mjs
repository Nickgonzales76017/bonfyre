import { expect,test } from "bun:test";
import { mkdtempSync,readFileSync,writeFileSync,rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runSitePublication } from "../site-publication.mjs";

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
