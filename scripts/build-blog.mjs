import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),"..");
const source=path.join(root,"editorial/posts");
const output=path.join(root,"site/blog");
const site="https://nickgonzales76017.github.io/bonfyre/blog/";
const check=process.argv.includes("--check");
const allowed=["Business","Applications","Commerce","Publishing","Engineering","Research"];
const esc=x=>String(x).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;");
const slugify=x=>x.toLowerCase().replace(/[^a-z0-9]+/g,"-").replace(/^-|-$/g,"");
const icon=(n,base)=>`<span class="glyph" aria-hidden="true"><svg viewBox="0 0 24 24"><use href="${base}assets/product-icons.svg#${esc(n)}"></use></svg></span>`;
const time=p=>Math.max(2,Math.ceil(p.body.split(/\s+/).length/210));

function readPost(file){
 const s=fs.readFileSync(file,"utf8").replace(/\r\n/g,"\n");
 const match=/^---\n([\s\S]*?)\n---\n([\s\S]*)$/.exec(s);
 if(!match)throw Error("Missing front matter: "+file);
 const obj={};
 for(const row of match[1].split("\n")){
   const i=row.indexOf(":");if(i<1)throw Error("Bad metadata: "+file);
   obj[row.slice(0,i).trim()]=row.slice(i+1).trim();
 }
 for(const k of ["slug","title","description","category","icon","published","origin","topics","status"])if(!obj[k])throw Error("Missing "+k+" in "+file);
 if(obj.status!=="edited-public"||!allowed.includes(obj.category))throw Error("Not public: "+file);
 if(!/^[a-z0-9]+(-[a-z0-9]+)*$/.test(obj.slug)||path.basename(file)!==obj.slug+".md")throw Error("Invalid slug: "+file);
 obj.body=match[2].trim();obj.file="editorial/posts/"+path.basename(file);
 if(obj.body.length<450)throw Error("Short article: "+file);
 return obj;
}
function inline(s){
 return esc(s).replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g,(_a,label,url)=>'<a href="'+url+'" rel="noopener noreferrer">'+label+'</a>')
 .replace(/\*\*([^*]+)\*\*/g,"<strong>$1</strong>");
}
function markdown(body){
 return body.trim().split(/\n\s*\n/).map(p=>{
  if(p.startsWith("## "))return '<h2 id="'+slugify(p.slice(3))+'">'+inline(p.slice(3))+'</h2>';
  if(p.startsWith("### "))return '<h3 id="'+slugify(p.slice(4))+'">'+inline(p.slice(4))+'</h3>';
  const lines=p.split("\n");if(lines.every(x=>x.startsWith("- ")))return "<ul>"+lines.map(x=>"<li>"+inline(x.slice(2))+"</li>").join("")+"</ul>";
  return "<p>"+inline(lines.join(" "))+"</p>";
 }).join("\n");
}
function write(file,text){
 fs.mkdirSync(path.dirname(file),{recursive:true});
 if(check){if(!fs.existsSync(file)||fs.readFileSync(file,"utf8")!==text)throw Error("Outdated blog output: "+file);}
 else fs.writeFileSync(file,text,"utf8");
}
function layout({title,description,url,base,content,article=false}){
 return `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#f5f3eb">
<title>${esc(title)}</title><meta name="description" content="${esc(description)}">
<meta property="og:title" content="${esc(title)}"><meta property="og:description" content="${esc(description)}">
<meta property="og:type" content="${article?"article":"website"}">
<link rel="canonical" href="${url}"><link rel="alternate" type="application/rss+xml" title="Aurekai Journal" href="${site}feed.xml">
<link rel="stylesheet" href="${base}assets/aurekai-field-guide.css">
<link rel="stylesheet" href="${base}assets/aurekai-research.css">
<link rel="stylesheet" href="${base}blog/blog.css">
</head><body><a class="skip" href="#main">Skip to content</a>
<header class="top"><div class="shell"><a class="brand" href="${base}"><span class="mark">A</span> AUREKAI <small>/ JOURNAL</small></a>
<nav aria-label="Site navigation"><a href="${base}">Home</a><a href="${base}explore.html">Products</a>
<a href="${base}market.html">Research</a><a href="${base}blog/">Journal</a><a href="${base}docs.html">Bonfÿre</a></nav></div></header>
<main id="main">${content}</main>
<footer class="footer"><div class="shell"><span>Aurekai / Bonfÿre · Edited project notes</span>
<span><a href="${base}blog/">All articles</a> · <a href="${base}blog/feed.xml">RSS</a> · <a href="${base}docs.html">Technology</a></span></div></footer>
</body></html>
`;
}
function card(p,base){
 return `<article class="journal-card" data-article data-category="${esc(p.category)}" data-search="${esc((p.title+" "+p.description+" "+p.topics+" "+p.category).toLowerCase())}">
<a href="${base}blog/${p.slug}/" class="journal-card-link"><div class="journal-card-top">${icon(p.icon,base)}
<span class="eyebrow">${esc(p.category)}</span></div><h3>${esc(p.title)}</h3>
<p>${esc(p.description)}</p><div class="journal-card-bottom"><span>${time(p)} min read</span><span>Read ↗</span></div>
</a></article>`;
}
const posts=fs.readdirSync(source).filter(n=>n.endsWith(".md")).map(n=>readPost(path.join(source,n)));
if(posts.length<16||new Set(posts.map(p=>p.slug)).size!==posts.length)throw Error("Expected sixteen distinct posts");
const featureOrder=["why-ai-needs-an-operating-company","a-job-posting-has-more-than-one-job","a-publisher-deserves-a-home","what-a-legacy-migration-really-moves","the-memory-wall-in-local-models"];
posts.sort((a,b)=>{let x=featureOrder.indexOf(a.slug),y=featureOrder.indexOf(b.slug);return (x<0?999:x)-(y<0?999:y)||a.title.localeCompare(b.title);});
const count=posts.length,feature=posts[0];
const groups=["All",...allowed.filter(x=>posts.some(p=>p.category===x))];
const controls=groups.map((g,i)=>`<button type="button" class="journal-filter${i===0?" active":""}" data-filter="${g}" aria-pressed="${i===0?"true":"false"}">${g}</button>`).join("");
const index=`<section class="journal-hero"><div class="shell"><div class="eyebrow">Aurekai / Bonfÿre · Journal</div>
<h1>Notes from<br><em>building the work.</em></h1>
<p class="lead">We've spent this year looking at software, businesses, local models, publishing, creators and things that already exist but could be more useful. These are the questions, experiments and ideas worth writing down as we work.</p>
<div class="journal-feature"><div><span class="eyebrow">Start here</span><h2>${esc(feature.title)}</h2><p>${esc(feature.description)}</p><a class="button primary" href="./${feature.slug}/">Read this article ↗</a></div>
<div class="journal-feature-aside"><span class="eyebrow">From our notebooks</span><p>These are edited from earlier project discussions, with links to products, source code and outside research where relevant. They aren't raw chat transcripts.</p><a href="../market.html">Market research ↗</a></div></div></div></section>
<section class="block journal-listing" id="articles"><div class="shell">
<div class="head"><div><span class="eyebrow">All articles / ${count}</span><h2>Things we've been figuring out.</h2></div><p>Browse by subject or search for a specific question. Each article stands on its own and points to the work behind it.</p></div>
<div class="journal-tools"><div class="journal-filters" role="group" aria-label="Filter articles">${controls}</div>
<label class="journal-search-label" for="journal-search">Search the journal<input id="journal-search" type="search" placeholder="Search topics and articles…" autocomplete="off"></label></div>
<p class="journal-count" id="journal-result-count" aria-live="polite">Showing ${count} articles</p>
<div class="journal-grid">${posts.map(p=>card(p,"../")).join("\n")}</div>
<p id="journal-empty" class="journal-empty" hidden>No articles match that search. Try another topic.</p>
</div></section>
<section class="block alt"><div class="shell"><div class="head"><div><span class="eyebrow">Continuing the work</span><h2>From conversations to software.</h2></div>
<p>For now these are static articles with their own addresses. Their authoring files and stable IDs can later move into Bonfÿre's native publishing system.</p></div>
<div class="journal-footer-links"><a href="../docs.html">Architecture ↗</a><a href="../explore.html">Products ↗</a><a href="./feed.xml">RSS feed ↗</a></div></div></section>
<script src="./filter.js" defer></script>`;
write(path.join(output,"index.html"),layout({title:"Aurekai Journal — Notes from building the work",description:"Articles about Aurekai and Bonfÿre: business software, source libraries, agents, creator tools and new markets.",url:site,base:"../",content:index}));
for(const p of posts){
 const others=[...posts.filter(x=>x.slug!==p.slug&&x.category===p.category),...posts.filter(x=>x.slug!==p.slug&&x.category!==p.category)].slice(0,3);
 const nav=p.body.split("\n").filter(l=>l.startsWith("## ")).map(l=>`<a href="#${slugify(l.slice(3))}">${esc(l.slice(3))}</a>`).join("");
 const article=`<section class="journal-article-hero"><div class="shell journal-article-head">
<nav class="journal-breadcrumb" aria-label="Breadcrumb"><a href="../../blog/">Journal</a><span aria-hidden="true"> / </span>${esc(p.category)}</nav>
${icon(p.icon,"../../")}<span class="eyebrow">${esc(p.category)} · ${time(p)} min read</span>
<h1>${esc(p.title)}</h1><p class="lead">${esc(p.description)}</p>
<div class="journal-meta"><span>Aurekai / Bonfÿre</span><span>November 7, 2026</span><span>Edited field notes</span></div></div></section>
<section class="journal-article-shell"><div class="shell journal-article-layout">
<article class="article-body">${markdown(p.body)}
<div class="journal-source-note"><strong>From our notebooks.</strong> This post adapts earlier design conversations and experiments: ${esc(p.origin)}. It's written for public reading and isn't a verbatim chat export.</div></article>
<aside class="journal-article-side"><div class="journal-side-sticky"><span class="eyebrow">In this article</span>${nav}
<hr><p>Learn more about <a href="../../docs.html">Bonfÿre</a> and <a href="../../explore.html">Aurekai's products</a>.</p></div></aside></div></section>
<section class="block alt"><div class="shell"><div class="head"><div><span class="eyebrow">Keep reading</span><h2>More from the notebooks.</h2></div><p>Other ideas we're pursuing across Aurekai and Bonfÿre.</p></div>
<div class="journal-grid">${others.map(x=>card(x,"../../")).join("")}</div></div></section>`;
 write(path.join(output,p.slug,"index.html"),layout({title:p.title+" — Aurekai Journal",description:p.description,url:site+p.slug+"/",base:"../../",content:article,article:true}));
}
const items=posts.map(p=>`<item><title>${esc(p.title)}</title><link>${site+p.slug}/</link><guid isPermaLink="true">${site+p.slug}/</guid><pubDate>${new Date(p.published+"T12:00:00Z").toUTCString()}</pubDate><category>${esc(p.category)}</category><description>${esc(p.description)}</description></item>`).join("\n");
write(path.join(output,"feed.xml"),`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Aurekai Journal</title><link>${site}</link><description>Notes from building Aurekai and Bonfÿre</description><language>en-us</language>${items}</channel></rss>
`);
write(path.join(output,"sitemap.xml"),`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${[site,...posts.map(p=>site+p.slug+"/")].map(u=>"<url><loc>"+esc(u)+"</loc><lastmod>2026-11-07</lastmod></url>").join("\n")}
</urlset>`);
const manifest=["subject_id\tslug\tcategory\tpublished\torigin\tsource_path\tbody_sha256\tcanonical_url",...posts.map(p=>
 ["post:"+p.slug,p.slug,p.category,p.published,p.origin,p.file,createHash("sha256").update(p.body).digest("hex"),site+p.slug+"/"].join("\t"))].join("\n")+"\n";
write(path.join(output,"post-index.tsv"),manifest);
console.log("Aurekai Journal: "+posts.length+" articles "+(check?"checked":"built"));
