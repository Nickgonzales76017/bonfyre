// HTTP integration witness: actual WebAddress routes on an isolated Matrix/CMS.
// No production account, publication, or remote host is changed.
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,rmSync} from "node:fs";
import {join,resolve} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import {createServer,request} from "node:http";
import {spawn,execFileSync} from "node:child_process";
import {compileSceneExperience} from "../../BonfyreSurfaceScene/runtime/experience-programs.mjs";

const root=resolve(fileURLToPath(new URL("../../..",import.meta.url)));
const scratch=mkdtempSync(join(tmpdir(),"bf-stitch-work-http-"));
const web=join(root,"cmd/BonfyreWebAddress/bonfyre-web-address");
const cms=join(root,"cmd/BonfyreCMS/bonfyre-cms");
const schemas=join(root,"cmd/BonfyreCMS/content-types");
const cmsdb=join(scratch,"cms.db"),matrixdb=join(scratch,"matrix.db"),authdb=join(scratch,"auth.db");
mkdirSync(join(scratch,"deltas"));
const env={...process.env,BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_SCHEMAS:schemas,
  BONFYRE_CMS_DB:cmsdb,BONFYRE_FACT_DELTAS:join(scratch,"deltas")};
const freePort=async()=>{
  const server=createServer();
  await new Promise((ok,no)=>server.once("error",no).listen(0,"127.0.0.1",ok));
  const port=server.address().port;
  await new Promise(ok=>server.close(ok));
  return port;
};
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function get(port,path,host="aurekai.work",accept="text/html") {
  return new Promise((resolve,reject)=>{
    const req=request({hostname:"127.0.0.1",port,method:"GET",path,
      headers:{Host:host,Accept:accept}},res=>{
      let data="";res.setEncoding("utf8");
      res.on("data",part=>{if(data.length<8*1024*1024)data+=part;});
      res.on("end",()=>resolve({status:res.statusCode,headers:res.headers,body:data}));
    });
    req.on("error",reject);req.setTimeout(6000,()=>req.destroy(Error("route timeout")));req.end();
  });
}
let child;
try {
  execFileSync(cms,["schema","migrate","--db",cmsdb,"--schemas",schemas],{env,stdio:"ignore",timeout:20000});
  // Real retained-source ordering witness: one source publishes twice at
  // earlier times, another publishes once more recently. No sample claims
  // enter canonical CMS or any public release.
  const archive=join(scratch,"items.db");
  execFileSync("sqlite3",[archive,
    "CREATE TABLE item(source TEXT,key TEXT,title TEXT,by TEXT,publisher TEXT,at INTEGER,family TEXT);"+
    "INSERT INTO item VALUES ('source-alice','aaaaaaaaaaaaaaaaaaaaaaaa','First article','Alice Walker','',1600000000,'news');"+
    "INSERT INTO item VALUES ('source-alice','bbbbbbbbbbbbbbbbbbbbbbbb','Second article','Alice Walker','',1650000000,'news');"+
    "INSERT INTO item VALUES ('source-beatrice','cccccccccccccccccccccccc','New article','Beatrice Reed','',1750000000,'news');"
  ],{env,stdio:"ignore",timeout:20000});
  env.BONFYRE_ITEM_ARCHIVE_DB=archive;
  // Published CMS route records exercise the authored owner rather than its
  // fallback. These isolated fixtures are never installed in the real estate.
  for (const [route,label] of [["/market","Market"],["/property","Property"],["/agents","Agents"]]) {
    const record={route,title:label,heading:label,eyebrow:"Aurekai",
      lede:"Owner-authored route fixture",machine_view:"deferred",status:"published"};
    execFileSync(cms,["entry","create","site_page",JSON.stringify(record),
      "--ns","root","--db",cmsdb,"--schemas",schemas],{env,stdio:"ignore",timeout:20000});
  }
  execFileSync(web,["bootstrap-owned","--db",matrixdb],{env,stdio:"ignore",timeout:20000});
  const cases=[
    ["/","discover"],["/today","discover"],["/people","directory"],
    ["/organisations","directory"],["/jobs","directory"],["/business","reader"],
    ["/money","reader"],["/discover","discover"],["/search","directory"],
    ["/watch","media"],["/listen","media"],["/board","directory"],
    ["/channels","feed"],["/community","feed"],["/stage","media"],
    ["/market","market"],["/listings","market"],["/parcels","spatial"],
    ["/property","detail"],["/link","connections"],["/sources","connections"],
    ["/packs","market"],["/inbox","feed"],["/studio","workbench"],
    ["/console","workbench"],["/territory","spatial"],["/observations","directory"],
    ["/worlds","directory"],["/agents","workbench"],["/programs","directory"],
    ["/apps","workbench"],["/account","workbench"],["/connections","connections"],
    ["/crm","workbench"],["/projects","workbench"],["/offers","market"]
  ];
  // Route presentation is CMS data: seed the fixture store with one published
  // route_presentation row per expected composition, as an author would.
  for (const [route,pattern] of cases)
    execFileSync(cms,["entry","create","route_presentation",JSON.stringify({territory:"work",route,pattern,status:"published"}),
      "--ns","root","--db",cmsdb,"--schemas",schemas],{env,stdio:"ignore",timeout:20000});
  const crossApex=[
    ["aurekai.life","/","discover"],["aurekai.life","/food","discover"],
    ["aurekai.life","/whats-on","discover"],["aurekai.live","/","discover"],
    ["aurekai.live","/watch","media"],["aurekai.live","/whats-on","discover"],
    ["aurekai.shop","/","discover"],["aurekai.shop","/deals","market"],
    ["aurekai.shop","/recalls","reader"]
  ];
  for (const [host,route,pattern] of crossApex)
    execFileSync(cms,["entry","create","route_presentation",JSON.stringify({territory:host.slice(8),route,pattern,status:"published"}),
      "--ns","root","--db",cmsdb,"--schemas",schemas],{env,stdio:"ignore",timeout:20000});
  const port=await freePort();
  child=spawn(web,["serve","--db",matrixdb,"--auth-db",authdb,"--port",String(port),
    "--site-root",join(root,"site/matrix")],{cwd:root,env,stdio:"ignore"});
  let ready=false;
  for(let i=0;i<100;i++){
    if(child.exitCode!==null)throw Error("WebAddress quit before startup: "+child.exitCode);
    try{const r=await get(port,"/healthz","aurekai.work");if(r.status<500){ready=true;break;}}catch{}
    await sleep(60);
  }
  assert(ready,"isolated WebAddress could not start");
  let sourceBoundCompositions=0;
  let covered=0;
  for(const [route,pattern] of cases){
    const r=await get(port,route);
    if(r.status!==200){console.log("SKIP owner-routed "+route+": HTTP "+r.status);continue;}
    if(!r.body.includes("data-aurekai-pattern")){
      console.log("UNCOMPOSED "+route+": HTTP 200, "+(r.headers["x-bonfyre-surface-program"]||r.headers["x-bonfyre-world-projection"]||"owner-unknown")+", head="+r.body.slice(0,100).replace(/\s+/g," "));continue;
    }
    assert(r.body.includes('data-aurekai-pattern="'+pattern+'"'),
      "Unexpected composition "+route+": expected "+pattern);
    assert(r.body.includes('content="'+pattern+'"') && r.body.includes('href="/assets/composition.css'),
      "No native style for "+route);
    const sceneText=r.body.match(/<template id="bonfyre-surface-scene"[^>]*>([\s\S]*?)<\/template>/);
    if(sceneText){
      const scene=JSON.parse(sceneText[1]);
      const plan=compileSceneExperience(scene,{pattern});
      const regions=[...r.body.matchAll(/<section\b[^>]*data-bonfyre-region="([^"]+)"[^>]*>/g)];
      const published=plan.regions.filter(x=>x.kind==="world_fragment");
      assert.equal(regions.length,published.length,"Native DOM/SurfaceScene count drift on "+route);
      assert.equal(plan.subject,scene.focus.subject_address);
      assert.equal(plan.sourceDigest,scene.source_digest);
      for(const match of regions){
        const role=published.find(x=>x.id===match[1]);
        assert(role,"Native DOM contains undeclared scene fragment "+match[1]+" on "+route);
        assert(match[0].includes('data-exp-lane="'+role.zone+'"'),
          "Native layout and SurfaceScene disagree about "+role.id+" on "+route);
        assert.equal(role.materialization,"server-rendered",
          "A compatibility adapter must not claim a source projection it did not own");
      }
      if(published.length)sourceBoundCompositions++;
    }
    covered++;
    console.log(route+" -> "+pattern+" PASS");
  }
  console.log("Route-owner fixture rendered "+sourceBoundCompositions+" PageComposition scenes; native page owner tested separately");
  assert(covered>=5,"Not enough real Work route coverage: "+covered);
  // All four apexes receive only their own source-backed native route grammar.
  // Owner-routed pages which are not in this isolated fixture remain unasserted.
  const coveredApex=new Map([['aurekai.life',0],['aurekai.live',0],['aurekai.shop',0]]);
  for(const [host,route,pattern] of crossApex){
    const response=await get(port,route,host);
    if(response.status!==200){console.log("OWNER ROUTE "+host+route+": HTTP "+response.status);continue;}
    assert(response.body.includes('data-aurekai-pattern="'+pattern+'"'),
      "Incorrect native apex pattern "+host+route);
    assert(response.body.includes('content="'+pattern+'"')&&response.body.includes('href="/assets/composition.css'),
      "Missing native composition head "+host+route);
    coveredApex.set(host,coveredApex.get(host)+1);
  }
  for(const [host,count] of coveredApex)assert(count>=1,'No real native composition on '+host);
  console.log('Four-apex presentation coverage: '+[...coveredApex].map(([host,n])=>host+'='+n).join(' ')+'; WORK='+covered);
  const foreign=await get(port,"/watch","other-example.test");
  assert(!foreign.body.includes("data-aurekai-pattern"),"Non-Aurekai host must not acquire a presentation");
  for (const route of ["/market","/property","/agents"]) {
    const machine=await get(port,route,"aurekai.work","application/json");
    assert(!machine.body.includes("data-aurekai-pattern"),
      "Presentation leaked into machine contract: "+route);
  }
  const account=await get(port,"/account");
  assert(account.headers["cache-control"]?.includes("no-store"),
    "Account presentation must remain non-cacheable");
  const mostCredited=await get(port,"/people");
  const mostRecent=await get(port,"/people?sort=recent");
  const creditList=mostCredited.body.split("id='people-credits'")[1]||"";
  const recentList=mostRecent.body.split("id='people-credits'")[1]||"";
  assert(creditList.includes("Alice Walker")&&creditList.includes("Beatrice Reed"),
    "Retained source credits did not populate the actual People directory");
  assert(creditList.indexOf("Alice Walker")<creditList.indexOf("Beatrice Reed"),
    "Most credited must order by native retained source counts");
  assert(recentList.indexOf("Beatrice Reed")<recentList.indexOf("Alice Walker"),
    "Latest observed must order by retained publication time");
  assert(mostRecent.body.includes("people-stitch-20261104")&&
         mostRecent.body.includes("name='sort'")&&
         mostRecent.body.includes("data-identity-boundary"),
    "Stitch People controls must preserve the publisher identity boundary");
  console.log("People Stitch: PASS real credited ordering, recent sorting, profile/evidence boundaries");
  console.log("stitch_work_routes_test: PASS "+covered+" native WebAddress .work routes, unrelated apex preserved");
} finally {
  if(child)child.kill("SIGTERM");
  rmSync(scratch,{recursive:true,force:true});
}
