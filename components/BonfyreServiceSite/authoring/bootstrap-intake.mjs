// First-party source-bound intake, not a second site authority. No remote fetch,
// effect, raw HTML CMS write, or publication takes place in this module.
// Bootstrap upstream: v5.3.8, code MIT, docs CC BY 3.0.
import {readFileSync,readdirSync,statSync,existsSync} from "node:fs";
import {join,relative,resolve,sep} from "node:path";
import {createHash} from "node:crypto";
import {newAuthoringTree,validateAuthoringTree} from
 "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";

export const BOOTSTRAP_TAG="v5.3.8";
export const BOOTSTRAP_COMMIT="25aa8cc0b32f0d1a54be575347e6d84b70b1acd7";
export const BOOTSTRAP_ORIGIN="https://github.com/twbs/bootstrap/blob/"+BOOTSTRAP_TAG+"/";
const fail=message=>{throw Error("Bootstrap intake: "+message);};
const sha=bytes=>createHash("sha256").update(bytes).digest("hex");
const slug=x=>String(x).toLowerCase().replace(/[^a-z0-9]+/g,"-").replace(/^-|-$/g,"");
const read=(root,path)=>readFileSync(join(root,path));
const assetPaths=Object.freeze({
 css:"dist/css/bootstrap.min.css",
 js:"dist/js/bootstrap.bundle.min.js"
});
const groups=[
 "getting-started","customize","layout","content","forms","components",
 "helpers","utilities","extend","about"
];
function paths(root){
 const all=[];
 function go(directory){
  for(const part of readdirSync(join(root,directory),{withFileTypes:true})){
   const path=directory?directory+"/"+part.name:part.name;
   if(part.isSymbolicLink())continue;
   if(part.isDirectory())go(path);
   else if(part.isFile())all.push(path);
  }
 }
 go("");return all.sort();
}
function metadata(source,basename){
 const front=source.match(/^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/);
 const fields={};
 if(front)for(const line of front[1].split(/\r?\n/)){
  const match=line.match(/^(title|description|toc):\s*(.*?)\s*$/);
  if(match)fields[match[1]]=match[2].replace(/^["']|["']$/g,"");
 }
 return {title:fields.title||basename.replace(/-/g," "),
  description:fields.description||"",body:front?source.slice(front[0].length):source};
}
function sourceText(raw){
 return raw
  .replace(/^import .+$/gm,"")
  .replace(/^\s*export .+$/gm,"")
  .replace(/\[\[docsref:([^\]]+)\]\]/g,(_all,path)=>"/docs/5-3"+path)
  .replace(/\[\[config:[^\]]+\]\]/g,"[configured for the original Bootstrap release]")
  .replace(/<([A-Z][a-zA-Z]+)(?:\s[^<>]*?)?\/>/g,(_s,kind)=>"["+kind+" source reference]")
  .replace(/<\/?[A-Za-z][^>]*>/g,"")
  .replace(/&nbsp;/g," ")
  .replace(/\r\n/g,"\n");
}
function examplesOf(body){
 const entries=[],parts=[],regex=/<Example\b/g;
 let previous=0,match;
 while((match=regex.exec(body))){
  const start=match.index;
  if(start<previous)continue;
  const marker=body.indexOf("code={\x60",start);
  if(marker<0||marker-start>750)continue;
  let at=marker+"code={\x60".length,escaped=false,end=-1;
  for(let i=at;i<body.length;i++){
   const c=body[i];
   if(escaped){escaped=false;continue;}
   if(c==="\\"){escaped=true;continue;}
   if(c==="\x60"&&body[i+1]==="}"){end=i;break;}
  }
  if(end<0)continue;
  const close=body.indexOf("/>",end+2);
  if(close<0||close-end>600)continue;
  parts.push(body.slice(previous,start));
  const snippet=body.slice(at,end).replace(/\\\x60/g,"\x60");
  entries.push({number:entries.length+1,sha256:sha(snippet),code:snippet,
   sandbox_safe:!/<\s*(?:script|iframe|object|embed|svg)\b|on[a-z]+\s*=|javascript:|data:text\/html|<\/\s*style/i.test(snippet)});
  parts.push("\n\n[Official Bootstrap example "+entries.length+"; source-backed preview below]\n\n");
  previous=close+2;
 }
 parts.push(body.slice(previous));
 return {body:parts.join(""),entries};
}
function textSections(raw){
 const withoutCode=raw.replace(/(^[ \t]*\x60{3,}[^\n]*\n)[\s\S]*?^[ \t]*\x60{3,}[^\n]*$/gm,
  "\n[Source code is retained with the original document]\n");
 const cleaned=sourceText(withoutCode);
 const pieces=cleaned.split(/\n(?=#{1,4} )/);
 const sections=[];
 for(const piece of pieces){
  const heading=piece.match(/^#{1,4} ([^\n]*)/);
  const title=heading?heading[1].slice(0,218).trim():"Overview";
  let content=(heading?piece.slice(heading[0].length):piece)
   .replace(/^[ \t]*\n/gm,"\n").replace(/\n{3,}/g,"\n\n").trim();
  if(!content)continue;
  // Do not truncate documents: partition long sections into complete,
  // independently editable typed blocks inside the native 5k limit.
  while(content.length){
   let cut=Math.min(4600,content.length);
   if(cut<content.length){
    const boundary=content.lastIndexOf("\n",cut);
    if(boundary>cut/2)cut=boundary;
   }
   const pieceText=content.slice(0,cut).trim();
   if(pieceText)sections.push({heading:title,body:pieceText});
   content=content.slice(cut).trimStart();
  }
 }
 return sections;
}
function pageRoute(category,basename){
 return "/docs/5-3/"+slug(category)+"/"+slug(basename)+"/";
}
function docTree(item,site_id){
 const tree=newAuthoringTree(site_id,item.route);
 const provenance={source_ref:item.url,source_digest:item.sha256};
 tree.blocks.push({id:"page-introduction",kind:"profile-hero",props:{
  heading:item.title.slice(0,220),body:item.description.slice(0,5000),...provenance},
  children:[]});
 const bits=textSections(item.body);
 bits.forEach((section,i)=>{
  tree.blocks.push({id:"section-"+String(i+1).padStart(4,"0"),kind:"profile-story",
   props:{heading:section.heading,body:section.body,...provenance},
   children:[]});
 });
 item.examples.forEach((example,i)=>{
  tree.blocks.push({id:"example-"+String(i+1).padStart(4,"0"),kind:"profile-card",
   props:{heading:"Example "+(i+1),body:"Source-backed component preview",
    label:"Open original source",href:item.url,...provenance},
   children:[]});
 });
 if(tree.blocks.length>512)fail("native page exceeds 512 blocks "+item.route);
 return validateAuthoringTree(tree);
}
export function bootstrapCorpus(root,{strict=false,site_id="bootstrap-reference-v5-3-8"}={}){
 if(typeof root!=="string"||!root)fail("upstream directory required");
 const home=resolve(root),packagePath=join(home,"package.json");
 if(!existsSync(packagePath))fail("upstream Bootstrap package.json missing");
 const pkg=JSON.parse(readFileSync(packagePath,"utf8"));
 if(pkg.name!=="bootstrap"||pkg.version!=="5.3.8")fail("expected pinned Bootstrap 5.3.8 source");
 const contentRoot="site/src/content/docs/";
 const exampleRoot="site/src/assets/examples/";
 const files=paths(home),docs=[],assets={},examples=[];
 const docFiles=files.filter(x=>x.startsWith(contentRoot)&&x.endsWith(".mdx"));
 const exampleFiles=files.filter(x=>x.startsWith(exampleRoot));
 for(const path of docFiles){
  const source=read(home,path),raw=source.toString("utf8");
  const file=relative(contentRoot.replace(/\/$/,""),path).split(sep).join("/");
  const category=file.split("/")[0],name=file.split("/").at(-1).replace(/\.mdx$/,"");
  const meta=metadata(raw,name),extracted=examplesOf(meta.body);
  const item={kind:"documentation",category,name,title:meta.title,description:meta.description,
   route:pageRoute(category,name),source:path,sha256:sha(source),
   url:BOOTSTRAP_ORIGIN+path,body:extracted.body,examples:extracted.entries};
  item.tree=docTree(item,site_id);
  docs.push(item);
 }
 const dirs=new Map();
 for(const path of exampleFiles){
  const remain=path.slice(exampleRoot.length),name=remain.split("/")[0];
  if(!name)continue;
  if(!dirs.has(name))dirs.set(name,[]);
  dirs.get(name).push(path);
 }
 for(const [name,related]of [...dirs].sort(([a],[b])=>a.localeCompare(b))){
  const main=related.find(x=>x.endsWith("/index.astro"));
  if(!main)fail("example family missing its page "+name);
  const source=read(home,main),file=source.toString("utf8");
  const item={kind:"example-page",category:"examples",name,title:name.replace(/-/g," "),
   description:"Official Bootstrap example, preserved with its original files.",
   route:"/examples/"+name+"/",source:main,sha256:sha(source),
   url:BOOTSTRAP_ORIGIN+main,files:related.slice().sort(),body:file,examples:[]};
  item.tree=newAuthoringTree(site_id,item.route);
  item.tree.blocks=[{id:"example-introduction",kind:"profile-hero",
   props:{heading:item.title,body:item.description,
    source_ref:item.url,source_digest:item.sha256},children:[]},
   {id:"example-source",kind:"profile-story",
    props:{heading:"Source-backed example",
     body:"This candidate retains the exact Astro, stylesheet and script sources. The original Astro execution is not claimed until the upstream build is reproduced.",
     label:"Open upstream source",href:item.url},children:[]}];
  validateAuthoringTree(item.tree);
  examples.push(item);
 }
 for(const [kind,path]of Object.entries(assetPaths)){
  if(!files.includes(path))fail("compiled "+kind+" missing at "+path);
  const bytes=read(home,path);assets[kind]={source:path,sha256:sha(bytes),bytes};
 }
 const seen=new Set();
 for(const x of [...docs,...examples]){
  if(seen.has(x.route))fail("route collision "+x.route);
  seen.add(x.route);
 }
 const components=docs.filter(x=>x.category==="components");
 if(strict&&(components.length<24||docs.length<100||examples.length<40))
  fail("incomplete upstream inventory: "+docs.length+" documents, "+
   components.length+" components, "+examples.length+" example families");
 const site={schema:"bonfyre.bootstrap-intake.v1",site_id,version:"5.3.8",
  ref:BOOTSTRAP_TAG,upstream_commit:BOOTSTRAP_COMMIT,
  license_code:"MIT",license_docs:"CC-BY-3.0",docs,examples,assets,
  totals:{docs:docs.length,component_docs:components.length,
   example_families:examples.length,
   component_examples:docs.reduce((n,x)=>n+x.examples.length,0),
   routes:seen.size,groups:[...new Set(docs.map(x=>x.category))].sort()}};
 return site;
}
