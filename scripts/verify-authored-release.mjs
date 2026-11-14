/* Aurekai published-site archive admission for GitHub Pages.
 * Never deploy the hard-coded checkout site, a candidate preview, or a digest
 * without the CMS publication identity that produced its exact page bytes.
 * Node builtins only; tar is parsed here to reject links and traversal.
 */
import {readFileSync,writeFileSync,mkdirSync,renameSync,rmSync,existsSync} from "node:fs";
import {dirname,join,resolve,sep} from "node:path";
import {gunzipSync} from "node:zlib";
import {createHash} from "node:crypto";
import {fileURLToPath} from "node:url";
const hash=body=>createHash("sha256").update(body).digest("hex");
const fail=reason=>{throw Error("Aurekai publication refused: "+reason);};
const hex=s=>/^[a-f0-9]{64}$/.test(s);
const maxArchive=96*1024*1024;
const field=(header,start,length)=>header.subarray(start,start+length).toString("utf8").split("\0")[0];
const octal=(header,start,length)=>{
 const s=field(header,start,length).trim();
 if(!/^[0-7]+$/.test(s))fail("invalid tar integer");
 return parseInt(s,8);
};
function readTar(compressed){
 if(compressed.length>maxArchive)fail("archive compressed size exceeds admission");
 const body=gunzipSync(compressed,{maxOutputLength:maxArchive});
 const files=new Map();let cursor=0,entries=0,total=0,terminated=false;
 while(cursor+512<=body.length){
  const header=body.subarray(cursor,cursor+512);
  if(header.every(value=>value===0)){terminated=true;break;}
  entries++;if(entries>12000)fail("too many tar entries");
  const declared=octal(header,148,8);
  let checksum=0;
  for(let i=0;i<512;i++)checksum+=i>=148&&i<156?32:header[i];
  if(declared!==checksum)fail("tar header checksum does not match");
  const name=field(header,0,100),prefix=field(header,345,155);
  const path=prefix?prefix+"/"+name:name;
  if(!path||path.startsWith("/")||path.includes("\\")||
     /[\x00-\x1f\x7f]/.test(path)||
     path.split("/").some(x=>!x||x==="."||x==="..")&&
       !path.endsWith("/"))fail("unsafe archive path");
  const key=path.endsWith("/")?path.slice(0,-1):path;
  if(!/^[A-Za-z0-9_.\/-]+$/.test(key)||
     !(key==="site"||key.startsWith("site/")||
       ["RELEASE.tsv","FILES.tsv","SITE-PAGES.tsv"].includes(key)))
   fail("archive attempted to write an undeclared path");
  const size=octal(header,124,12),type=header[156];
  if(![0,48,53].includes(type))fail("tar links, devices, extensions and executable types are forbidden");
  if(type===53&&size!==0)fail("directory contains tar file body");
  cursor+=512;
  if(size>16*1024*1024||cursor+size>body.length)fail("invalid tar member size");
  total+=size;if(total>80*1024*1024)fail("site payload exceeds admission");
  if(type!==53){
   if(files.has(key))fail("duplicate archive member "+key);
   files.set(key,Buffer.from(body.subarray(cursor,cursor+size)));
  }
  cursor+=Math.ceil(size/512)*512;
 }
 if(!terminated)fail("archive has no terminating tar block");
 if(files.size<5)fail("not a complete published site");
 return files;
}
function tsv(bytes,label){
 if(!bytes||bytes.length>512*1024)fail("missing or oversized "+label);
 const lines=bytes.toString("utf8").trimEnd().split("\n");
 return lines.map(line=>line.replace(/\r$/,"").split("\t"));
}
function metadata(rows){
 const result=new Map();
 for(const row of rows){
  if(row.length!==2||!row[0]||result.has(row[0]))fail("invalid duplicate release metadata");
  result.set(row[0],row[1]);
 }
 return result;
}
function verify(files){
 const meta=metadata(tsv(files.get("RELEASE.tsv"),"RELEASE.tsv"));
 if(meta.get("schema")!=="bonfyre.authored-site-release.v1"||
    meta.get("publication")!=="published")
  fail("not a published Aurekai CMS release");
 const namespace=meta.get("account_namespace")||"";
 if(!/^person-[a-z0-9-]{2,80}-(work|life|live|shop)$/.test(namespace))
  fail("release has no exact account-owned namespace");
 const release=meta.get("release_id"),source=meta.get("cms_revision");
 if(!hex(release)||!hex(source))fail("release identity or CMS revision missing");
 const records=tsv(files.get("FILES.tsv"),"FILES.tsv");
 if(records.shift().join("\t")!=="path\tsha256")fail("file evidence header invalid");
 const observed=new Set();
 for(const entry of records){
  if(entry.length!==2)fail("file evidence malformed");
  const [path,digest]=entry;
  if(!path.startsWith("site/")||!hex(digest)||observed.has(path))
   fail("file evidence ambiguous");
  const bytes=files.get(path);
  if(!bytes||hash(bytes)!==digest)fail("file differs from release evidence: "+path);
  observed.add(path);
 }
 for(const path of files.keys())if(path.startsWith("site/")&&!observed.has(path))
  fail("unrecorded publishable file "+path);
 const pages=tsv(files.get("SITE-PAGES.tsv"),"SITE-PAGES.tsv");
 if(pages.shift().join("\t")!=="route\ttitle\tnative_revision\thtml_sha256")
  fail("published route manifest missing");
 if(!pages.length||pages.length>1000)fail("published page count invalid");
 const routes=new Set();
 for(const row of pages){
  if(row.length!==4)fail("page evidence malformed");
  const [route,title,revision,htmlHash]=row;
  if(!/^\/(?:[a-z0-9-]+\/)*$/.test(route)||routes.has(route)||
     !title||!hex(revision)||!hex(htmlHash))fail("untrusted route or revision");
  routes.add(route);
  const file="site/"+(route==="/"
   ?"index.html":route.slice(1)+"index.html");
  const bytes=files.get(file),html=bytes?.toString("utf8")||"";
  if(!bytes||hash(bytes)!==htmlHash)fail("page release bytes mismatch: "+route);
  if(!/<!doctype html/i.test(html)||!/<html\b/i.test(html)||
     !html.includes('data-authored-publication="'+release+'"')||
     !html.includes('data-authored-profile="'+namespace+'"')||
     /<meta\b[^>]*name=["']robots["'][^>]*noindex/i.test(html))
   fail("page is not a published Aurekai authored render: "+route);
 }
 if(Number(meta.get("pages"))!==pages.length||!routes.has("/")||
    !files.has("site/assets/site.css"))
  fail("release home, page count, or shared stylesheet missing");
 return {namespace,release,pages:pages.length,files:observed.size};
}
export function verifyAndStage(archive,sha,{output="site"}={}){
 if(!hex(sha))fail("expected immutable SHA-256 receipt");
 const content=readFileSync(archive);
 if(hash(content)!==sha)fail("archive bytes differ from sealed release receipt");
 const files=readTar(content),receipt=verify(files);
 const destination=resolve(output),staging=destination+".aurekai-stage-"+process.pid;
 if(existsSync(staging))fail("staging directory already exists");
 mkdirSync(staging,{recursive:false});
 try{
  for(const [name,bytes]of files){
   if(!name.startsWith("site/"))continue;
   const relative=name.slice(5),target=resolve(staging,relative);
   if(!target.startsWith(staging+sep))fail("path escaped staged site");
   mkdirSync(dirname(target),{recursive:true});
   writeFileSync(target,bytes,{flag:"wx",mode:0o644});
  }
  // No live/public state changes are attempted here. This stages one exact
  // verified artifact into the ephemeral Actions checkout for its Pages job.
  rmSync(destination,{recursive:true,force:true});
  renameSync(staging,destination);
 }catch(error){rmSync(staging,{recursive:true,force:true});throw error;}
 return receipt;
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===resolve(process.argv[1])){
 const [, ,archive,sha]=process.argv;
 if(!archive||!sha)fail("usage: node scripts/verify-authored-release.mjs archive.tar.gz sha256");
 const r=verifyAndStage(archive,sha);
 console.log("Aurekai CMS release verified: "+r.release+
  ", "+r.pages+" pages, "+r.files+" files; namespace "+r.namespace);
}
