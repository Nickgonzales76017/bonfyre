// Read-only source sweep. Counts are replacement candidates, not proven savings.
import {readdirSync,readFileSync,lstatSync} from "node:fs";
import {join,relative} from "node:path";
import {fileURLToPath} from "node:url";
const root=fileURLToPath(new URL("..",import.meta.url));
const skip=new Set([".git","vendor","node_modules","generated","build","dist","tests","fixtures","archives","results","outputs",".tmp","site"]);
const patterns=[
 ["mustach","HTML markup assembled inside C",[".c",".h"],/\b(?:fputs|printf|fprintf|snprintf)\s*\([^;\n]{0,180}<(?:section|article|div|header|footer|nav|main|aside|form|h[1-6]|p|a|table|li)\b/g],
 ["mustach","JavaScript embedded in C",[".c",".h"],/"(?:function |const |let |async function |document\.)[^"\n]{2,500}\\n"/g],
 ["md4c","hand-coded Markdown parser",[".c",".h"],/\b(?:rich_text|inline_text|normalize_markdown|markdown_fence_width|markdown_fence_closes|table_row)\s*\(/g],
 ["cmark-gfm","Markdown extensions and table support",[".c",".h"],/(?:markdown|CommonMark|code fence|fenced code|GFM|render_markdown|markdown_to_html)/gi],
 ["shared-escaping","duplicated HTML escaping",[".c",".h",".mjs",".js"],/(?:&amp;|&lt;|&quot;|&#39;|escape_html|html_escape|html_esc|escapeHtml|function\s+esc\s*\(|const\s+esc\s*=)/g],
 ["shared-components","handwritten client DOM",[".js",".mjs"],/\b(?:innerHTML|outerHTML|insertAdjacentHTML|contenteditable|createElement)\b/g],
 ["shared-components","CSS inside native renderers",[".c",".h"],/fputs\s*\(\s*"<style>|printf\s*\(\s*"<style>|<link rel=\\?"stylesheet/g]
];
const inputs=[],exts=new Set([".c",".h",".js",".mjs"]);
function scan(dir,depth=0){
 if(depth>12)return;
 for(const item of readdirSync(dir,{withFileTypes:true})){
  if(item.name.startsWith("."))continue;
  const full=join(dir,item.name);
  if(item.isSymbolicLink())continue;
  if(item.isDirectory()){if(!skip.has(item.name))scan(full,depth+1);continue;}
  if(!item.isFile())continue;
  const ext=item.name.slice(item.name.lastIndexOf("."));
  if(!exts.has(ext)||/\.(?:archive|bak)\./.test(item.name))continue;
  try{if(lstatSync(full).size>400000)continue;}catch{continue;}
  inputs.push([full,ext]);
 }
}
for(const dir of ["cmd","components","lib","scripts"])scan(join(root,dir));
const rows=[];
for(const [path,ext]of inputs){
 const body=readFileSync(path,"utf8");
 for(const [library,signal,allowed,rx]of patterns){
  if(!allowed.includes(ext))continue;
  const hits=[...body.matchAll(rx)];if(!hits.length)continue;
  rows.push({library,signal,path:relative(root,path),hits:hits.length,line:body.slice(0,hits[0].index).split("\n").length});
 }
}
rows.sort((a,b)=>b.hits-a.hits||a.path.localeCompare(b.path));
const affected=new Set(rows.map(x=>x.path)).size;
if(process.argv.includes("--verify")){
 const want=["cmd/BonfyreWebAddress/src/site_mcp.c","cmd/BonfyreWebAddress/src/profile_home.h","cmd/BonfyreNarrate/src/main.c"];
 if(want.some(x=>!inputs.some(([path])=>relative(root,path)===x)))throw Error("expected native owners missing");
 if(!rows.some(x=>x.library==="md4c"&&x.path.endsWith("site_mcp.c")))throw Error("native Markdown parser not discovered");
 if(!rows.some(x=>x.library==="mustach"))throw Error("native HTML renderer not discovered");
 console.log("native replacement scan PASS",inputs.length,"files,",affected,"candidate files,",rows.length,"signals");
}else{
 console.log("library\tsignal\thits\tpath\tfirst_line");
 const n=Number(process.argv.find(x=>/^--top=\d+$/.test(x))?.slice(6)||60);
 for(const x of rows.slice(0,Math.min(n,250)))console.log([x.library,x.signal,x.hits,x.path,x.line].join("\t"));
 console.log("# scanned",inputs.length,"files; candidate files",affected,"signals",rows.length);
}
