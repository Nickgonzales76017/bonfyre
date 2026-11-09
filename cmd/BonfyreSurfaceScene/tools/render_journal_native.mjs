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
 rows.push({slug,title:doc.blocks[0].props.heading,
  desc:doc.blocks[0].props.body,category:doc.blocks[0].props.eyebrow});
 sourceBytes+=src.length;htmlBytes+=buffer.length;
}
assert.equal(rows.length,90);
assert.equal(manifest.trimEnd().split("\n").length,91);
writeFileSync(join(output,"SOURCE.tsv"),manifest);
const links=rows.map((r,i)=>'<article class="edition"><a href="./'+r.slug+'.html"><span class="eyebrow">EDITION '+String(i+1).padStart(2,"0")+'/90 · '+escape(r.category)+'</span><h2>'+escape(r.title)+'</h2><p>'+escape(r.desc)+'</p><span class="read">Open C-rendered edition</span></a></article>').join("\n");
const css=[
":root{font-family:system-ui;color:#202d2c;background:#f4f3ec}*,*:before,*:after{box-sizing:border-box}",
"body{margin:0}header{padding:clamp(28px,7vw,112px);background:#132c28;color:#fff}",
"header>p{color:#b6d0c4;letter-spacing:.19em;text-transform:uppercase;font-size:12px;font-weight:700}",
"header h1{font:500 clamp(43px,7vw,100px)/1.05 Georgia,serif;max-width:12ch;letter-spacing:-.05em;margin:.4em 0}",
"header .sub{font:19px/1.55 Georgia,serif;color:#d0e3d8;max-width:65ch}",
"main{max-width:1460px;margin:0 auto;padding:clamp(18px,4vw,62px)}",
".grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px}",
".edition{background:#fff;border:1px solid #d8ded6;min-height:340px}",
".edition a{display:flex;flex-direction:column;height:100%;gap:17px;padding:clamp(22px,2.6vw,39px);color:inherit;text-decoration:none}",
".edition a:hover{background:#f9faf6}.edition a:focus-visible{outline:3px solid #a95b30;outline-offset:-3px}",
".eyebrow{color:#6c746e;font-size:10px;font-weight:800;letter-spacing:.15em;text-transform:uppercase}",
".edition h2{font:500 clamp(23px,2vw,34px)/1.16 Georgia,serif;letter-spacing:-.032em;margin:0}",
".edition p{font:15px/1.55 system-ui;color:#53615c;margin:0;max-width:45ch}",
".read{margin-top:auto;font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.1em;border-top:1px solid #dbe1d8;padding-top:17px}",
"@media(max-width:1030px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}",
"@media(max-width:620px){.grid{grid-template-columns:1fr}.edition{min-height:0}}"
].join("");
const index='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Aurekai / 90 native editions</title><style>'+css+'</style></head><body data-release="draft-preview-only"><header><p>AUREKAI / JOURNAL · NATIVE C AUTHORING PROOF</p><h1>90 articles. One complete document contract.</h1><p class="sub">Original authored essays, preserved byte for byte and rendered with MD4C in Bonfÿre C. Review-only output, not a production replacement.</p></header><main><div class="grid">'+links+'</div></main></body></html>';
writeFileSync(join(output,"index.html"),index);
console.log("AUREKAI_90_NATIVE_MARKDOWN_PAGES_PASS",rows.length,"source-bytes",sourceBytes,"html-bytes",htmlBytes);
console.log("AUREKAI_90_NATIVE_SOURCE_DIGESTS_PASS",join(output,"SOURCE.tsv"));
