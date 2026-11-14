// Multi-page candidate from exact account-owned CMS snapshots, never an
// importer, external publication or an unreviewed release. Pages retain their
// native source revisions, with one renderer, CSS and interaction controller.
import {readFileSync,writeFileSync,mkdtempSync,mkdirSync,existsSync,
 renameSync,rmSync} from "node:fs";
import {dirname,join,resolve,sep} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
import {compileAuthoredCandidate} from "./authored-static-candidate.mjs";
import {compileCss} from "./site-styles.mjs";
const root=fileURLToPath(new URL("../../",import.meta.url));
const digest=x=>createHash("sha256").update(x).digest("hex");
const fail=x=>{throw Error("authored site candidate: "+x);};
const esc=x=>String(x??"").replace(/[&<>"']/g,c=>({
 "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
}[c]));
function basePath(x){
 if(x===undefined||x===null||x==="")return "";
 if(typeof x!=="string"||!/^\/[a-z0-9/-]+$/.test(x)||x.endsWith("/")||
    x.includes("//")||x.includes(".."))fail("invalid deployment base");
 return x;
}
function pageRoute(entry){
 const route=entry.snapshot?.route;
 if(typeof route!=="string"||route.length>180||
    !/^\/(?:[a-z0-9-]+\/?)*$/.test(route))fail("invalid page route");
 return route;
}
const fileOf=route=>{
 const inner=route.replace(/^\/|\/$/g,"");
 return inner?inner+"/index.html":"index.html";
};
const rootSafe=path=>{
 const dest=resolve(path),live=join(root,"site");
 if(dest===live||dest.startsWith(live+sep))fail("cannot overwrite live public source");
 return dest;
};
const applyBase=(html,base)=>base?html.replace(/\b(href|src)="\/(?!\/)/g,
 (_all,attr)=>attr+'="'+base+'/'):html;
function directoryHTML(pages,base){
 const links=pages.map(p=>'<li><a href="'+esc(base+p.route)+'">'+
  esc(p.title)+'</a><small>'+esc(p.route)+'</small></li>').join("");
 return '<!doctype html><html lang="en"><head><meta charset="utf-8">'+
  '<meta name="viewport" content="width=device-width,initial-scale=1">'+
  '<meta name="robots" content="noindex,nofollow">'+
  '<title>Pages · Aurekai candidate</title>'+
  '<link rel="stylesheet" href="'+esc(base)+'/assets/site.css">'+
  '<style>body{font:16px/1.6 system-ui;background:#f6f9f3;color:#20412f;margin:0}'+
  'main{max-width:1040px;margin:60px auto;padding:24px}h1{font:700 44px Georgia,serif}'+
  'ul{display:grid;grid-template-columns:repeat(auto-fit,minmax(235px,1fr));'+
  'gap:14px;list-style:none;padding:0}li{background:white;padding:18px;'+
  'border:1px solid #c5d6c7;border-radius:11px;display:grid;gap:4px}'+
  'a{color:#245b42;font-weight:700}small{color:#64766b}</style></head>'+
  '<body><main><h1>Pages in this site</h1><p>Account-owned candidate drafts. '+
  'Publication requires separate review.</p><ul>'+links+'</ul></main></body></html>';
}
function renderAll(snapshots,{base,name,theme}){
 if(!Array.isArray(snapshots)||!snapshots.length||snapshots.length>512)
  fail("expected 1–512 exact CMS pages");
 const namespace=snapshots[0]?.snapshot?.namespace;
 if(typeof namespace!=="string"||!namespace)fail("missing account namespace");
 const entries=[],seen=new Set();
 for(const entry of snapshots){
  if(entry?.schema!=="bonfyre.page-snapshot.v1"||
     entry.snapshot?.namespace!==namespace||!Number.isInteger(entry.id)||entry.id<1||
     !/^[a-f0-9]{64}$/.test(entry.digest||""))
   fail("missing exact committed CMS identity");
  const route=pageRoute(entry);
  if(seen.has(route))fail("duplicate CMS page route "+route);
  seen.add(route);
  if(entry.snapshot.page?.status!=="draft"||entry.snapshot.page?.route!==route)
   fail("candidate requires a retained unpublished page draft");
  entries.push({entry,route,title:entry.snapshot.page.title});
 }
 entries.sort((a,b)=>a.route.localeCompare(b.route));
 const directoryRoute=seen.has("/")?"/site-pages/":"/";
 if(seen.has(directoryRoute))fail("page directory collides with authored page");
 const catalogue=entries.map(({route,title})=>({route,title}));
 const pages=entries.map(item=>{
  const group=item.route.split("/").filter(Boolean).slice(0,2).join("/");
  const peers=entries.filter(x=>x.route.split("/").filter(Boolean).slice(0,2).join("/")===group);
  const navigation=[{label:"All pages",href:directoryRoute},
   ...peers.slice(0,20).map(x=>({label:x.title,href:x.route}))];
  const candidate=compileAuthoredCandidate(item.entry,{
   name:item.title||name||"Aurekai site",theme,navigation});
  return {route:item.route,title:item.title,file:candidate.file,
   html:applyBase(candidate.result.html,base),
   theme:candidate.result.theme,revision:item.entry.digest};
 });
 const directory={route:directoryRoute,file:fileOf(directoryRoute),
  html:directoryHTML(catalogue,base),title:"Page directory",revision:"generated-directory"};
 return {namespace,pages,directory,catalogue};
}
function findUnresolved(html,base,known){
 const unresolved=[];
 for(const match of html.matchAll(/\b(href|src)="([^"]*)"/g)){
  let href=match[2].replaceAll("&amp;","&");
  if(!href.startsWith("/"))continue;
  if(base){
   if(!href.startsWith(base+"/")){unresolved.push(href);continue;}
   href=href.slice(base.length);
  }
  const address=href.split(/[?#]/,1)[0];
  if(!known.has(address)&&!known.has(address.replace(/\/+$/,"")+"/"))
   unresolved.push(href);
 }
 return [...new Set(unresolved)].sort();
}
export function compileAuthoredSiteCandidate(snapshots,{basePath:prefix="",name,theme}={}){
 const base=basePath(prefix),state=renderAll(snapshots,{base,name,theme});
 const outputs=[...state.pages,state.directory],known=new Set(outputs.map(x=>x.route));
 known.add("/assets/site.css");
 if(state.pages.some(p=>p.html.includes("pattern-runtime.js")))
  known.add("/assets/pattern-runtime.js");
 const unresolved=outputs.flatMap(p=>findUnresolved(p.html,base,known).map(href=>({
  page:p.route,href})));
 return {...state,base,outputs,unresolved};
}
export function writeAuthoredSiteCandidate(snapshots,{output,name,basePath:prefix="",theme,
 requireLinkClosure=false}={}){
 if(typeof output!=="string"||!output)fail("new candidate directory is required");
 const destination=rootSafe(output);
 if(existsSync(destination))fail("candidate destination already exists");
 const result=compileAuthoredSiteCandidate(snapshots,{basePath:prefix,name,theme});
 if(requireLinkClosure&&result.unresolved.length)fail("unresolved local links: "+result.unresolved.length);
 const work=mkdtempSync(join(tmpdir(),"bonfyre-authored-site-")),records=[];
 try{
  const write=(path,bytes)=>{
   const target=resolve(work,path);
   if(!target.startsWith(work+sep))fail("candidate path escapes output root");
   mkdirSync(dirname(target),{recursive:true});
   writeFileSync(target,bytes,{flag:"wx"});
   records.push({path,sha256:digest(bytes)});
  };
  for(const page of result.outputs)write(page.file,page.html);
  const css=join(work,"assets","site.css");
  mkdirSync(dirname(css),{recursive:true});
  compileCss(result.pages.map(p=>({html:p.html})),result.pages[0].theme,css);
  records.push({path:"assets/site.css",sha256:digest(readFileSync(css))});
  if(result.pages.some(p=>p.html.includes("pattern-runtime.js")))
   write("assets/pattern-runtime.js",
    readFileSync(fileURLToPath(new URL("./authoring/pattern-runtime.js",import.meta.url))));
  write("SITE-PAGES.tsv","route\ttitle\tnative_revision\thtml_sha256\n"+
   result.outputs.map(x=>[x.route,x.title,x.revision,digest(x.html)].join("\t")).join("\n")+"\n");
  write("UNRESOLVED-LINKS.tsv","page\thref\n"+
   result.unresolved.map(x=>x.page+"\t"+x.href).join("\n")+"\n");
  const receipt=[
   ["schema","bonfyre.authored-site-candidate.v1"],
   ["publication","NOINDEX-CANDIDATE-ONLY"],
   ["account_namespace",result.namespace],
   ["base_path",result.base||"/"],
   ["pages",result.pages.length],
   ["route_directory",result.directory.route],
   ["unresolved_links",result.unresolved.length],
   ["local_interactions",result.pages.some(p=>p.html.includes("pattern-runtime.js"))?"shared-script":"none"],
   ["rights","rendering does not establish source or media clearance"]
  ].map(x=>x.join("\t")).join("\n")+"\n";
  write("CANDIDATE.tsv",receipt);
  write("FILES.tsv","path\tsha256\n"+records.slice().sort((a,b)=>a.path.localeCompare(b.path))
   .map(x=>x.path+"\t"+x.sha256).join("\n")+"\n");
  mkdirSync(dirname(destination),{recursive:true});
  if(existsSync(destination))fail("candidate destination became occupied");
  renameSync(work,destination);
  return {directory:destination,site_id:result.namespace,
   page_count:result.pages.length,unresolved_links:result.unresolved.length,
   candidate:true,base_path:result.base};
 }finally{if(existsSync(work))rmSync(work,{recursive:true,force:true});}
}
