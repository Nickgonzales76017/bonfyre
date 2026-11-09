// Source-owned editorial visual registry, consumed by the native C renderer.
// Image credit records may only originate in the reviewed Journal catalogue.
// This grants no publication right or external-use right to an account.
import {readFileSync,writeFileSync} from "node:fs";
import {resolve} from "node:path";
import {fileURLToPath} from "node:url";
const root=resolve(fileURLToPath(new URL("../../..",import.meta.url)));
const input=readFileSync(resolve(root,"editorial/art-direction.tsv"),"utf8");
const mediaInput=readFileSync(resolve(root,"editorial/media-sources.tsv"),"utf8");
const id=/^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const media=new Map();
const rows=mediaInput.split(/\r?\n/).filter(x=>x&&!x.startsWith("#"));
if(rows.shift()!=="key\turi\tcaption\tcredit_url")throw Error("bad reviewed media source header");
for(const line of rows){
 const bits=line.split("\t");if(bits.length!==4)throw Error("media columns changed");
 const [key,uri,caption,credit]=bits;
 if(!id.test(key)||media.has(key)||!caption||/[<>\r\n]/.test(caption)||
  !(uri.startsWith("https://")||uri.startsWith("local:"))||
  (uri.startsWith("https://")&&!credit)|| (credit&&!credit.startsWith("https://")))
  throw Error("Invalid reviewed media: "+key);
 let url=uri;
 if(uri.startsWith("local:")){
  const local=uri.slice(6);
  if(!/^(spaces|instruments)\/shots\/[a-z0-9-]+\.png$/.test(local))throw Error("Local image path escapes reviewed site scope");
  url="https://nickgonzales76017.github.io/bonfyre/"+local;
 }
 media.set(key,{url,caption,credit});
}
if(media.size!==12)throw Error("Expected twelve retained approved-source media keys");
const entries=[],used=new Set();
const lines=input.split(/\r?\n/);
if(lines[2]!=="slug\tformat\tcolorway\tmedia")throw Error("Art direction header drift");
for(const line of lines.slice(3)){
 if(!line||line.startsWith("#"))continue;
 const bits=line.split("\t");if(bits.length!==4)throw Error("Art row invalid");
 const [slug,fmt,color,mediaKey]=bits;
 if(!id.test(slug)||used.has(slug)||!id.test(fmt)||!id.test(color))throw Error("Art assignment invalid: "+slug);
 if(mediaKey&&!media.has(mediaKey))throw Error("Missing credited image: "+slug);
 used.add(slug);
 const source=media.get(mediaKey);
 entries.push({slug,fmt,color,key:mediaKey,url:source?.url||"",caption:source?.caption||"",credit:source?.credit||""});
}
if(entries.length!==90)throw Error("Not all ninety articles have assigned design");
for(const key of media.keys())if(!entries.some(e=>e.key===key))
 throw Error("A media source is unassigned; review before including: "+key);
const quoted=x=>JSON.stringify(x);
const values=entries.map(e=>"  {"+[e.slug,e.fmt,e.color,e.key,e.url,e.caption,e.credit].map(quoted).join(",")+"}").join(",\n");
const source="/* GENERATED from editorial/art-direction.tsv + editorial/media-sources.tsv.\n"+
" * Source-reviewed imagery is public Journal presentation only. This\n"+
" * registry is not consent, rights or account-level publication authority.\n"+
" * Run node cmd/BonfyreSurfaceScene/tools/compile_editorial_media.mjs --check.\n"+
" */\n#ifndef BONFYRE_NATIVE_EDITORIAL_MEDIA_H\n#define BONFYRE_NATIVE_EDITORIAL_MEDIA_H\n"+
"typedef struct {const char *slug,*format,*color,*key,*url,*caption,*credit;} BfEditorialVisual;\n"+
"static const BfEditorialVisual bf_reviewed_journal_visuals[]={\n"+values+"\n};\n"+
"static const unsigned bf_reviewed_journal_visual_count=sizeof(bf_reviewed_journal_visuals)/sizeof(bf_reviewed_journal_visuals[0]);\n"+
"#endif\n";
const dest=resolve(root,"cmd/BonfyreSurfaceScene/generated/native_editorial_media.h");
if(process.argv.includes("--check")){
 if(readFileSync(dest,"utf8")!==source)throw Error("Native visual registry diverged from reviewed editorial sources");
 console.log("EDITORIAL_NATIVE_MEDIA_CHECK_PASS 90 assignments / 12 sources");
}else{
 writeFileSync(dest,source);
 console.log("EDITORIAL_NATIVE_MEDIA_COMPILED 90 assignments / 12 sources");
}
