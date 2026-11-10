// Complete upstream-file candidate builder: one route for every .mdx document
// and one for every Astro example family, not a curated feature subset.
// This produces noindex candidate files and typed authoring trees; it does not
// publish, claim Astro execution, or write into a user's live CMS namespace.
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,renameSync,
 rmSync,existsSync,copyFileSync} from "node:fs";
import {dirname,join,resolve,sep} from "node:path";
import {tmpdir} from "node:os";
import {createHash} from "node:crypto";
import {bootstrapCorpus,BOOTSTRAP_ORIGIN} from "./bootstrap-intake.mjs";
import {renderAuthoredProfilePreview} from "../authored-page-renderer.mjs";
import {compileCss} from "../site-styles.mjs";
const escape=x=>String(x??"").replace(/[&<>"']/g,c=>
 ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const hash=x=>createHash("sha256").update(x).digest("hex");
const fail=x=>{throw Error("Bootstrap candidate: "+x);};
const safePath=x=>{
 if(typeof x!=="string"||!/^\/[a-z0-9/-]*\/$/.test(x)||
    x.includes("//")||x.includes(".."))fail("invalid output route");
 return x;
};
const pagesOf=site=>[...site.docs,...site.examples];
const sourceBasename=route=>route.replace(/^\/|\/$/g,"")+"/index.html";
const prefix=route=>"../".repeat(route.split("/").filter(Boolean).length);
function navContent(site,active){
 const navigation=[...new Set(site.docs.map(x=>x.category))].map(group=>{
  const members=site.docs.filter(x=>x.category===group);
  const heading='<h3><a href="/#'+escape(group)+'">'+escape(
   group.replace(/-/g," "))+"</a></h3>";
  return "<section class=\"bs-nav-group\">"+heading+"<ul>"+
   members.map(item=>'<li><a '+(item.route===active?'aria-current="page" ':'')+
    'href="'+escape(item.route)+'">'+escape(item.title)+"</a></li>").join("")+
   "</ul></section>";
 }).join("");
 return '<aside class="bs-sidebar" aria-label="Documentation navigation">'+
  '<p class="bs-sidebar-eyebrow">Bootstrap 5.3.8 · indexed source</p>'+
  '<nav>'+navigation+"</nav></aside>";
}
function safeSandboxCode(code){
 if(typeof code!=="string"||code.length>20000)return false;
 return !/<\s*(?:script|iframe|form|object|embed|svg|video|audio|meta|link|style|base)\b|on[a-z]+\s*=|javascript:|data:text\/html|srcdoc\s*=|(?:src|href)\s*=\s*["'](?:https?:)?\/\//i.test(code);
}
function liveSamples(item){
 if(!item.examples?.length)return "";
 const cards=item.examples.map(example=>{
  const title="Example "+example.number;
  let body='<p class="bs-preview-note">Exact example source: '+
   '<code>'+escape(example.sha256.slice(0,16))+"</code></p>";
  if(example.sandbox_safe&&safeSandboxCode(example.code)){
   const document="<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"+
    "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"+
    "<link rel=\"stylesheet\" href=\"/assets/bootstrap.min.css\">"+
    "<style>body{padding:16px;min-height:140px}</style></head><body>"+
    example.code+"<script src=\"/assets/bootstrap.bundle.min.js\"></script></body></html>";
   body+='<iframe class="bs-sandbox" title="'+escape(title)+'" loading="lazy" '+
    'referrerpolicy="no-referrer" sandbox="allow-scripts" srcdoc="'+escape(document)+'"></iframe>';
  }else body+='<p class="bs-preview-note">This source uses additional effects or embeds; preview disabled. Open the original source for the complete implementation.</p>';
  body+='<details><summary>Inspect retained markup</summary><pre><code>'+
   escape(example.code)+"</code></pre></details>";
  return '<article class="bs-sample" data-example-sha256="'+example.sha256+'">'+
   "<h3>"+escape(title)+"</h3>"+body+"</article>";
 });
 return '<section id="component-previews" class="bs-samples" aria-label="Bootstrap component examples">'+
  '<h2>Upstream component previews</h2><p>Source-backed, sandboxed examples. '+
  'These are external Bootstrap behavior, not native effect authorizations.</p>'+
  cards.join("")+"</section>";
}
function styles(){
 return [
  "html{scroll-behavior:smooth}body{margin:0;background:#f5f7f9;color:#1d303f}",
  ".bs-top{position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;gap:12px;background:#1c3342;color:white;padding:14px 3vw;box-shadow:0 3px 15px #0002}",
  ".bs-top a{color:inherit;text-decoration:none}.bs-top strong{font-weight:750;letter-spacing:-.03em}",
  ".bs-top small{font-size:12px;opacity:.85}",
  ".bs-top .bs-top-actions{display:flex;gap:12px;flex-wrap:wrap}",
  ".bs-app{max-width:1680px;margin:auto;display:grid;grid-template-columns:minmax(220px,280px) minmax(0,1fr);gap:22px;padding:20px clamp(12px,2vw,28px)}",
  ".bs-sidebar{position:sticky;top:80px;max-height:calc(100vh - 100px);overflow-y:auto;background:white;border:1px solid #d7dfe1;border-radius:14px;padding:16px}",
  ".bs-sidebar-eyebrow{font-size:12px;color:#566c77;text-transform:uppercase;letter-spacing:.12em}",
  ".bs-nav-group{margin:18px 0}.bs-nav-group h3{font-size:14px;font-weight:750;text-transform:capitalize}",
  ".bs-nav-group ul{list-style:none;padding:0;display:grid;gap:6px}",
  ".bs-nav-group li a{color:#33536e;text-decoration:none;font-size:13px;display:block;padding:5px 7px;border-radius:5px}",
  ".bs-nav-group li a:hover,.bs-nav-group li a[aria-current=page]{background:#e7eff4;color:#1e3f5a}",
  ".bs-main{min-width:0}.bs-main .authored-content{width:100%}",
  ".bs-main .authored-site{background:transparent}",
  ".bs-main .authored-profile-story{background:white;white-space:pre-wrap;overflow-wrap:anywhere}",
  ".bs-main .authored-profile-story h2{font:700 25px/1.3 Georgia,serif}",
  ".bs-main .authored-profile-hero{margin-top:0}",
  ".bs-metrics{display:flex;flex-wrap:wrap;gap:14px;margin:18px 0}.bs-metric{background:white;padding:15px 23px;border:1px solid #d7dfe1;border-radius:10px}",
  ".bs-metric strong{font-size:25px;display:block;font-variant-numeric:tabular-nums}",
  ".bs-index{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:16px}",
  ".bs-index-card{padding:22px;background:white;border:1px solid #d7dfe1;border-radius:12px;min-width:0}",
  ".bs-index-card h2{font-size:19px;text-transform:capitalize;margin-bottom:12px}",
  ".bs-index-card ul{padding-left:18px;font-size:14px;line-height:1.8}",
  ".bs-search{width:min(100%,600px);padding:12px 16px;border:2px solid #8ba3b2;border-radius:9px;background:white;color:#152b37}",
  ".bs-samples{margin:40px auto;max-width:1280px}.bs-samples>h2{font:700 29px Georgia,serif}",
  ".bs-sample{margin:18px 0;padding:20px;border:1px solid #d7dfe1;border-radius:12px;background:white}",
  ".bs-sample h3{font-size:20px}.bs-sandbox{width:100%;height:240px;border:1px solid #c5d2d6;border-radius:8px;background:white}",
  ".bs-sample pre{overflow:auto;max-height:360px;background:#152c39;color:#e1edf1;padding:16px;font-size:13px}",
  ".bs-preview-note{color:#4a606d;font-size:13px}.bs-license{margin:45px 0;padding:22px;border-top:1px solid #c6d2d7;color:#52636e;font-size:12px}",
  ".bs-license a{color:#275a7d}.bs-breadcrumbs{margin:8px 0 20px;color:#587082;font-size:13px}",
  "@media(max-width:900px){.bs-app{display:block}.bs-sidebar{position:static;max-height:320px;margin-bottom:16px}.bs-top{flex-wrap:wrap}}",
  "@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}"
 ].join("\n");
}
const footer='<footer class="bs-license"><p><strong>Source rights:</strong> Bootstrap code © 2011–2026 Bootstrap Authors (MIT); '+
 'Bootstrap documentation (CC BY 3.0). This is an Aurekai import candidate with source digests, '+
 'not a publication or an independent claim over Bootstrap authorship.</p>'+
 '<p><a href="https://github.com/twbs/bootstrap/tree/v5.3.8">Original source</a> · '+
 '<a href="https://creativecommons.org/licenses/by/3.0/">Documentation license</a> · '+
 '<a href="https://getbootstrap.com/docs/5.3/">Official documentation</a></p></footer>';
function pageHTML(site,item){
 const preview=renderAuthoredProfilePreview(item.tree,{
  name:item.title,navigation:[{label:"All documentation",href:"/"},
   {label:"Example gallery",href:"/#examples"}]});
 const depth=prefix(item.route);
 const crumb='<nav class="bs-breadcrumbs" aria-label="Breadcrumb"><a href="/">Reference</a> / '+
  escape(item.category)+" / "+escape(item.title)+"</nav>";
 const main=preview.html.match(/<main id="content" class="authored-content">([\s\S]*?)<\/main>/);
 if(!main)fail("authoring renderer no longer provides a main region");
 const document='<main id="content" class="bs-main">'+crumb+
  '<div class="authored-site"><div class="authored-content">'+main[1]+
  '</div></div>'+liveSamples(item)+footer+"</main>";
 const page='<!doctype html><html lang="en"><head><meta charset="utf-8">'+
  '<meta name="viewport" content="width=device-width,initial-scale=1">'+
  '<meta name="robots" content="noindex,nofollow">'+
  '<title>'+escape(item.title)+" · Bootstrap 5.3.8 · Aurekai candidate</title>"+
  '<link rel="stylesheet" href="'+depth+'assets/bootstrap.min.css">'+
  '<link rel="stylesheet" href="'+depth+'assets/site.css">'+
  '<link rel="stylesheet" href="'+depth+'assets/bootstrap-candidate.css"></head>'+
  '<body><header class="bs-top"><div><strong><a href="/">Bootstrap · Aurekai</a></strong> '+
  '<small>v5.3.8 · source-backed</small></div>'+
  '<nav class="bs-top-actions"><a href="/">Catalogue</a><a href="/#examples">Examples</a>'+
  '<a href="'+escape(item.url)+'">Original source</a></nav></header>'+
  '<div class="bs-app">'+navContent(site,item.route)+document+'</div></body></html>';
 return page;
}
function indexHTML(site){
 const t=site.totals,links=pagesOf(site);
 const groups=[...new Set(site.docs.map(x=>x.category))];
 const cards=[...groups.map(category=>({
  category,items:site.docs.filter(x=>x.category===category)})),
  {category:"examples",items:site.examples}].map(({category,items})=>
   '<section class="bs-index-card" id="'+escape(category)+'" data-group="'+escape(category)+'">'+
   "<h2>"+escape(category.replace(/-/g," "))+" · "+items.length+"</h2><ul>"+
   items.map(x=>'<li data-search="'+escape((x.title+" "+x.category).toLowerCase())+'">'+
    '<a href="'+escape(x.route)+'">'+escape(x.title)+"</a></li>").join("")+
   "</ul></section>").join("");
 const intro='<h1>One source, every Bootstrap page.</h1>'+
  '<p>A source-bound, editable Aurekai candidate generated from the complete '+
  'Bootstrap 5.3.8 documentation and example tree. This is not a published site.</p>';
 const metrics=[["Documentation pages",t.docs],["Component references",t.component_docs],
  ["Example families",t.example_families],["Component previews",t.component_examples]].
  map(([key,value])=>'<div class="bs-metric"><strong>'+value+'</strong>'+escape(key)+'</div>').join("");
 const filterScript='document.getElementById("query").addEventListener("input",function(){'+
  'const q=this.value.trim().toLowerCase();for(const item of document.querySelectorAll("[data-search]"))'+
  'item.hidden=!!q&&!item.dataset.search.includes(q);'+
  'for(const group of document.querySelectorAll("[data-group]"))'+
  'group.hidden=!group.querySelector("[data-search]:not([hidden])")});';
 return '<!doctype html><html lang="en"><head><meta charset="utf-8">'+
  '<meta name="viewport" content="width=device-width,initial-scale=1">'+
  '<meta name="robots" content="noindex,nofollow"><title>Bootstrap · Aurekai full-corpus candidate</title>'+
  '<link rel="stylesheet" href="/assets/bootstrap.min.css">'+
  '<link rel="stylesheet" href="/assets/bootstrap-candidate.css"></head>'+
  '<body><header class="bs-top"><strong>Bootstrap · Aurekai</strong>'+
  '<nav class="bs-top-actions"><a href="#components">Components</a>'+
  '<a href="#examples">Examples</a><a href="https://getbootstrap.com/docs/5.3/">Official docs</a></nav></header>'+
  '<div class="bs-app">'+navContent(site,"/")+'<main class="bs-main">'+intro+
  '<div class="bs-metrics">'+metrics+'</div>'+
  '<label for="query">Find a component, reference, or example</label>'+
  '<div><input type="search" id="query" class="bs-search" placeholder="Search all imported pages"></div>'+
  '<div class="bs-index">'+cards+'</div>'+footer+
  '</main></div><script>'+filterScript+'</script></body></html>';
}
function safeWrite(dir,relativeName,bytes){
 const home=resolve(dir),target=resolve(dir,relativeName);
 if(!target.startsWith(home+sep))fail("output escaped its candidate root");
 mkdirSync(dirname(target),{recursive:true});
 writeFileSync(target,bytes,{flag:"wx"});
 return {path:relativeName,sha256:hash(bytes)};
}
export function writeBootstrapCandidate(source,output,{strict=false}={}){
 if(typeof output!=="string"||!output)fail("explicit candidate destination required");
 const dest=resolve(output);
 if(existsSync(dest))fail("candidate destination already exists");
 const site=bootstrapCorpus(source,{strict});
 const work=mkdtempSync(join(tmpdir(),"bonfyre-bootstrap-"));
 const written=[],manifest=[];
 try{
  const pages=pagesOf(site);
  for(const item of pages){
   const page=pageHTML(site,item),path=sourceBasename(item.route);
   written.push(safeWrite(work,path,page));
   // Retain the full upstream source; the typed projection is an editable
   // derived view, not a lossy replacement of the original evidence.
   const bytes=readFileSync(join(source,item.source));
   written.push(safeWrite(work,"_source/"+item.source,bytes));
   manifest.push([item.kind,item.route,item.source,item.sha256,
    hash(page),item.tree.blocks.length,item.examples.length]);
  }
  written.push(safeWrite(work,"index.html",indexHTML(site)));
  for(const [name,asset]of Object.entries(site.assets))
   written.push(safeWrite(work,"assets/"+(name==="css"?
    "bootstrap.min.css":"bootstrap.bundle.min.js"),asset.bytes));
  const localStyles=styles();
  written.push(safeWrite(work,"assets/bootstrap-candidate.css",localStyles));
  // The native ServiceSite renderer compiles all shared utility styles once
  // from every authored page, rather than per example or per publisher.
  const style=join(work,"assets","site.css");
  compileCss(pages.map(item=>({html:pageHTML(site,item)})),
   renderAuthoredProfilePreview(pages[0].tree,{name:pages[0].title}).theme,style);
  const stylesheet=readFileSync(style);
  written.push({path:"assets/site.css",sha256:hash(stylesheet)});
  const ledger="kind\troute\tsource\tsha256\thtml_sha256\tblocks\tsnippets\n"+
   manifest.map(row=>row.join("\t")).join("\n")+"\n";
  written.push(safeWrite(work,"CANDIDATE-PAGES.tsv",ledger));
  const receipts=[
   ["schema","bonfyre.bootstrap-site-candidate.v1"],
   ["source_ref",BOOTSTRAP_ORIGIN.slice(0,-1)],
   ["publication","noindex-candidate-only"],
   ["docs_license","CC-BY-3.0"],
   ["code_license","MIT"],
   ["upstream_tag",site.ref],
   ["upstream_commit",site.upstream_commit],
   ...Object.entries(site.totals).filter(([,v])=>!Array.isArray(v))
  ].map(([k,v])=>k+"\t"+v).join("\n")+"\n";
  written.push(safeWrite(work,"PROVENANCE.tsv",receipts));
  const digestList="path\tsha256\n"+written.sort((a,b)=>a.path.localeCompare(b.path))
   .map(x=>x.path+"\t"+x.sha256).join("\n")+"\n";
  safeWrite(work,"FILES.tsv",digestList);
  if(existsSync(dest))fail("candidate destination became occupied");
  mkdirSync(dirname(dest),{recursive:true});
  renameSync(work,dest);
  return {directory:dest,files:written.length+1,totals:site.totals,
   source_tag:site.ref,candidate:true};
 }finally{if(existsSync(work))rmSync(work,{recursive:true,force:true});}
}
if(import.meta.main){
 const args=process.argv.slice(2);
 const get=flag=>{const index=args.indexOf(flag);return index>=0?args[index+1]:undefined;};
 if(!get("--source")||!get("--out")){
  console.error("usage: bun bootstrap-candidate.mjs --source UPSTREAM_BOOTSTRAP_DIR --out NEW_CANDIDATE_DIR [--strict]");
  process.exit(2);
 }
 const result=writeBootstrapCandidate(get("--source"),get("--out"),{
  strict:args.includes("--strict")});
 console.log("Bootstrap candidate, NOT published: "+JSON.stringify(result));
}
