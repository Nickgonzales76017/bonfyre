// 90 actual source-owned Journal essays -> BFAU2 -> native C/MD4C HTML.
// Every projection has original, binary, C-reemitted and HTML SHA-256 receipts.
// Review output only. This is not publication.
import {readFileSync,writeFileSync,mkdirSync} from "node:fs";
import {resolve,join} from "node:path";
import {fileURLToPath} from "node:url";
import {spawnSync} from "node:child_process";
import {createHash} from "node:crypto";
import assert from "node:assert/strict";
import {exportAllJournal,journalArticles,authoringFromJournal,journalArtDirection} from "./import_journal.mjs";
const root=resolve(fileURLToPath(new URL("..",import.meta.url)));
const repo=resolve(root,"../.."),sourceDir=join(repo,"editorial/posts");
const output=resolve(process.argv[2]||join(root,"build/journal-native"));
const wireDir=join(output,"sources");
mkdirSync(output,{recursive:true});
const sha=x=>createHash("sha256").update(x).digest("hex");
const escape=s=>s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;");
function run(bin,args){
 const x=spawnSync(bin,args,{encoding:"utf8",timeout:60000,maxBuffer:3*1024*1024});
 if(x.status!==0)throw Error("native authoring "+bin+" failed: "+(x.stderr||x.error||x.stdout));
}
const exe=join(output,"native-authoring");
run(process.env.CC||"cc",["-std=c11","-Wall","-Wextra","-Werror",
 join(root,"tests/native_authoring_carrier_main.c"),
 join(root,"vendor/md4c/md4c.c"),join(root,"vendor/md4c/md4c-html.c"),
 join(root,"vendor/md4c/entity.c"),"-o",exe]);
const sourceMedia=readFileSync(join(repo,"editorial/media-sources.tsv"),"utf8")
 .split(/\r?\n/).filter(x=>x&&!x.startsWith("#")).slice(1);
const creditedMedia=new Map(sourceMedia.map(line=>{
 const [key,url,caption,credit]=line.split("\t");
 if(!key||!caption||!url||!(url.startsWith("https://")||url.startsWith("local:")))
  throw Error("Malformed source-reviewed media");
 return [key,{src:url.startsWith("local:")?
  "https://nickgonzales76017.github.io/bonfyre/"+url.slice(6):url,caption,credit}];
}));
if(creditedMedia.size!==12)throw Error("Reviewed photo catalogue changed");
const imported=exportAllJournal({out:wireDir});
assert.equal(imported.articles,90);
let manifest="slug\tsource_sha256\tcarrier_sha256\treopened_sha256\thtml_sha256\turl\n";
const rows=[],articles=journalArticles(sourceDir);
let htmlBytes=0,sourceBytes=0;
for(const file of articles){
 const slug=file.slice(0,-3),src=readFileSync(join(sourceDir,file));
 const doc=authoringFromJournal(file,src);
 const carrier=join(wireDir,slug+".bfau");
 const round=join(output,slug+".reopened.bfau");
 const html=join(output,slug+".html");
 run(exe,["--roundtrip",carrier,round]);
 assert.deepEqual(readFileSync(carrier),readFileSync(round),"C dropped source: "+slug);
 run(exe,[carrier,html]);
 const buffer=readFileSync(html),page=buffer.toString("utf8");
 for(const required of ['data-release="draft-preview-only"',
   '<div class="bf-markdown">','data-route="'+doc.page+'"','data-source-ref="editorial/posts/'+file+'"',
   'data-edition-format="'+doc.blocks[0].props.metadata.edition_format+'"',
   'data-colorway="'+doc.blocks[0].props.metadata.colorway+'"'])
  assert.ok(page.includes(required),"native output lost "+required+" for "+slug);
 assert.ok(page.includes("<h2 id=\"chapter-01\">"),"source native chapter IDs absent "+slug);
 assert.ok(page.includes('class="bf-article-toc"'),"chapter navigation missing "+slug);
 const art=journalArtDirection(slug);
 if(art.media){
  assert.ok(page.includes('class="bf-edition-image"'),"credited image missing "+slug);
  assert.ok(page.includes('data-media-key="'+art.media+'"'),"wrong image source "+slug);
  assert.ok(page.includes("<figcaption>"),"attribution missing "+slug);
  assert.ok(!page.includes("Assigned credited art; native asset-rights binding remains review-held"),
   "retired placeholder returned "+slug);
 }else{
  assert.ok(page.includes('class="bf-chapter-cover"'),"authored typographic cover missing "+slug);
  assert.ok(!page.includes('class="bf-edition-image"'),"unsourced photo invented "+slug);
 }
 assert.doesNotMatch(page,/<script\b|<iframe\b|<object\b/i,"unsafe native output "+slug);
 manifest+=[slug,sha(src),sha(readFileSync(carrier)),sha(readFileSync(round)),sha(buffer),doc.page].join("\t")+"\n";
 const chapter=(src.toString("utf8").match(/^## (.+)$/m)||[])[1]||"Inside the investigation";
 rows.push({slug,title:doc.blocks[0].props.heading,
  desc:doc.blocks[0].props.body,category:doc.blocks[0].props.metadata.category,
  format:art.format,color:art.colorway,media:art.media,chapter});
 sourceBytes+=src.length;htmlBytes+=buffer.length;
}
assert.equal(rows.length,90);
assert.equal(manifest.trimEnd().split("\n").length,91);
writeFileSync(join(output,"SOURCE.tsv"),manifest);
const covers=rows.map((record,index)=>{
 const image=record.media?creditedMedia.get(record.media):null;
 if(record.media&&!image)throw Error("Uncredited media key "+record.media);
 const panel=image?
  '<div class="card-art card-photo" data-image="'+escape(record.media)+'"><img src="'+escape(image.src)+'" alt="" loading="lazy" decoding="async" width="960" height="720"></div>':
  '<div class="card-art card-typography"><span>Inside this article</span><strong>'+escape(record.chapter)+'</strong></div>';
 const first=index===0?' ed-lead':'';
 return '<article class="edition'+first+'" data-format="'+escape(record.format)+
  '" data-colorway="'+escape(record.color)+'"><a href="./'+record.slug+'.html">'+
  '<div class="card-cover"><div class="card-mast"><span>AUREKAI / JOURNAL</span><span>'+String(index+1).padStart(2,"0")+' / 90</span></div>'+
  panel+'<div class="card-kicker"><span>'+escape(record.format.replaceAll("-"," "))+'</span><span>'+escape(record.category)+'</span></div></div>'+
  '<div class="card-detail"><p class="category">'+escape(record.category)+'</p><h2>'+escape(record.title)+
  '</h2><p class="summary">'+escape(record.desc)+'</p><span class="read">Open native edition</span></div></a></article>';
});
if(covers.length!==90||rows.filter(x=>x.media).length!==12)
 throw Error("Source-reviewed cover set incomplete");
const css=[
":root{font-family:system-ui,-apple-system,Arial,sans-serif;color:#1c2925;background:#f0f0e8}",
"*,*:before,*:after{box-sizing:border-box}body{margin:0}a{color:inherit}",
".masthead{padding:clamp(30px,6vw,106px);background:#112a24;color:#fbf5e9;border-bottom:9px solid #af8a58}",
".masthead>p{color:#bbd0c6;letter-spacing:.17em;text-transform:uppercase;font-size:12px;font-weight:750}",
".masthead h1{font:500 clamp(47px,7vw,109px)/1.01 Georgia,serif;max-width:15ch;letter-spacing:-.048em;margin:.28em 0}",
".masthead .deck{font:clamp(16px,2vw,22px)/1.5 Georgia,serif;max-width:65ch;color:#d2e2d9}",
".count-line{display:flex;flex-wrap:wrap;gap:24px;margin-top:40px;border-top:1px solid #547264;padding-top:16px;font:700 12px system-ui;letter-spacing:.07em}",
".shell{max-width:1500px;margin:auto;padding:clamp(15px,3.5vw,55px)}",
".shelf-controls{display:flex;flex-wrap:wrap;align-items:center;gap:15px;margin-bottom:22px;border-bottom:1px solid #adbcb1;padding:0 0 17px}",
".shelf-controls h2{font:500 clamp(24px,3vw,40px) Georgia,serif;letter-spacing:-.02em;margin:0 auto 0 0}",
".view-toggle{position:absolute;opacity:0;width:1px;height:1px}",
".view-label{border:1px solid #899c90;padding:9px 14px;font:750 12px system-ui;cursor:pointer;border-radius:3px}",
"#list-view:focus-visible+.view-label{outline:3px solid #b7784e;outline-offset:3px}",
"#list-view:checked+.view-label{background:#1f493b;color:white}",
".grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px}",
".edition{border:1px solid #d0d9cf;background:#fff;min-width:0}",
".edition a{display:flex;flex-direction:column;height:100%;text-decoration:none}",
".edition a:focus-visible{outline:4px solid #a96536;outline-offset:2px}",
".card-cover{min-height:270px;display:flex;flex-direction:column;background:var(--tone,#234c3d);color:var(--paper,#f7f1e7);overflow:hidden}",
".card-mast,.card-kicker{display:flex;justify-content:space-between;gap:10px;align-items:center;padding:17px 20px;font:750 10px system-ui;letter-spacing:.1em;text-transform:uppercase}",
".card-mast{border-bottom:1px solid #ffffff55}.card-kicker{border-top:1px solid #ffffff55;margin-top:auto}",
".card-art{height:190px;min-width:0;overflow:hidden}",
".card-photo img{display:block;width:100%;height:100%;object-fit:cover}",
".card-typography{padding:19px 23px;display:flex;flex-direction:column;justify-content:center;gap:14px}",
".card-typography span{font:700 10px system-ui;opacity:.78;letter-spacing:.12em;text-transform:uppercase}",
".card-typography strong{font:500 clamp(19px,2.1vw,30px)/1.17 Georgia,serif;letter-spacing:-.025em;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}",
".card-detail{flex:1;display:flex;flex-direction:column;padding:clamp(20px,2vw,34px);gap:13px}",
".category{margin:0;color:#607367;font:800 11px system-ui;text-transform:uppercase;letter-spacing:.13em}",
".card-detail h2{font:500 clamp(23px,2.1vw,35px)/1.15 Georgia,serif;letter-spacing:-.035em;margin:0}",
".summary{margin:0;font:14px/1.6 system-ui;color:#59675f;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}",
".read{margin-top:auto;padding-top:18px;border-top:1px solid #d7dfd7;font:800 11px system-ui;letter-spacing:.1em;text-transform:uppercase}",
".edition a:hover .read{text-decoration:underline;text-underline-offset:4px}",
".ed-lead{grid-column:span 2}.ed-lead .card-cover{min-height:330px}",
".ed-lead .card-art{height:235px}.ed-lead .card-detail h2{font-size:clamp(32px,3.7vw,58px);max-width:19ch}",
".edition[data-colorway=forest]{--tone:#204b3d;--paper:#f3eee6}",
".edition[data-colorway=ink]{--tone:#1d2930;--paper:#f5f0e9}",
".edition[data-colorway=olive]{--tone:#48533a;--paper:#f5f0e5}",
".edition[data-colorway=brick]{--tone:#753d34;--paper:#f4eee7}",
".edition[data-colorway=copper]{--tone:#734d39;--paper:#f6f0e7}",
".edition[data-colorway=slate]{--tone:#314753;--paper:#eff1ed}",
"#list-view:checked~.grid{display:grid;grid-template-columns:1fr;gap:12px}",
"#list-view:checked~.grid .edition{grid-column:auto}",
"#list-view:checked~.grid .edition a{display:grid;grid-template-columns:minmax(180px,30%) minmax(0,1fr)}",
"#list-view:checked~.grid .card-cover{min-height:170px}",
"#list-view:checked~.grid .card-art{height:120px}",
"#list-view:checked~.grid .card-detail{padding:20px 24px}",
"#list-view:checked~.grid .card-detail h2{font-size:clamp(24px,2.7vw,39px);max-width:35ch}",
"@media(max-width:1050px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}",
"@media(max-width:650px){.grid{grid-template-columns:1fr}.ed-lead{grid-column:auto}",
".ed-lead .card-detail h2{font-size:32px}#list-view:checked~.grid .edition a{display:block}",
".card-cover,.ed-lead .card-cover{min-height:220px}.card-art,.ed-lead .card-art{height:155px}",
".card-typography strong{font-size:22px}.shell{padding:14px}}",
"@media(prefers-reduced-motion:no-preference){.edition a:hover .card-photo img{transform:scale(1.035);transition:transform .25s ease}}"
].join("");
const index='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Aurekai / Journal · Native editorial reading room</title><style>'+
 css+'</style></head><body data-release="draft-preview-only"><header class="masthead"><p>AUREKAI / JOURNAL · Native C editorial proof</p><h1>The work behind the web.</h1><p class="deck">90 independent investigations, 14 authored editorial formats and 12 credited source images. All editions are derived from original source and rendered through Bonfÿre C. Review-only, never a published replacement.</p><div class="count-line"><span>90 source-owned essays</span><span>14 editorial formats</span><span>12 source-credited features</span><span>0 unpublished rights inferred</span></div></header>'+
 '<main class="shell"><div class="shelf-controls"><h2>Explore the investigations</h2><span>Grid / list</span></div>'+
 '<input class="view-toggle" type="checkbox" id="list-view" aria-label="Switch to article list"><label class="view-label" for="list-view">List view</label>'+
 '<section class="grid" aria-label="All ninety native editorial editions">'+covers.join("\n")+
 '</section></main></body></html>';
writeFileSync(join(output,"index.html"),index);
console.log("AUREKAI_90_NATIVE_MARKDOWN_PAGES_PASS",rows.length,"source-bytes",sourceBytes,"html-bytes",htmlBytes);
console.log("AUREKAI_90_NATIVE_SOURCE_DIGESTS_PASS",join(output,"SOURCE.tsv"));
