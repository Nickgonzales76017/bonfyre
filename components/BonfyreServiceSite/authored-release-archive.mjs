/* Deterministic, bounded ustar+gzip transport for a separately admitted
 * Aurekai authored release. Paths and member hashes come ONLY from the
 * producer's FILES.tsv, not from a recursive checkout directory walk.
 * Writing an archive does not publish or upload anything. */
import {readFileSync,writeFileSync,existsSync,lstatSync} from "node:fs";
import {resolve,join,sep} from "node:path";
import {createHash} from "node:crypto";
import {gzipSync} from "node:zlib";
const sha=bytes=>createHash("sha256").update(bytes).digest("hex");
const fail=why=>{throw Error("authored release archive: "+why);};
const okPath=p=>typeof p==="string"&&p.startsWith("site/")&&
 /^[a-zA-Z0-9._/-]+$/.test(p)&&
 !p.split("/").some(x=>!x||x===".."||x===".")&&p.length<256;
const field=(dst,start,len,value)=>{
 const bytes=Buffer.from(value,"utf8");
 if(bytes.length>len)fail("ustar field too wide");
 bytes.copy(dst,start);
};
const octal=(dst,start,len,value)=>{
 const s=value.toString(8);
 if(s.length>len-2)fail("ustar integer too wide");
 field(dst,start,len,s.padStart(len-1,"0")+"\0");
};
function header(name,size){
 if(!okPath(name)&&!["RELEASE.tsv","FILES.tsv","SITE-PAGES.tsv"].includes(name))
  fail("archive member outside the published site");
 const bits=Buffer.from(name),file=Buffer.alloc(512,0);
 if(bits.length>100){
  const split=name.lastIndexOf("/",name.length-2);
  if(split<1||split>155||name.length-split-1>100)fail("ustar long path out of bounds");
  field(file,345,155,name.slice(0,split));
  field(file,0,100,name.slice(split+1));
 }else field(file,0,100,name);
 octal(file,100,8,0o644);octal(file,108,8,0);
 octal(file,116,8,0);octal(file,124,12,size);
 octal(file,136,12,0);
 file.fill(32,148,156);
 field(file,156,1,"0");field(file,257,6,"ustar\0");
 field(file,263,2,"00");
 let checksum=0;
 for(const byte of file)checksum+=byte;
 octal(file,148,8,checksum);
 return file;
}
function parseRows(content,heading){
 const lines=content.trimEnd().split("\n");
 if(lines.shift()!==heading)fail("invalid FILES.tsv heading");
 return lines.map(line=>{
  const row=line.split("\t");
  if(row.length!==2||!okPath(row[0])||!/^[a-f0-9]{64}$/.test(row[1]))
   fail("invalid recorded artifact path or digest");
  return row;
 });
}
function pinnedMembers(dir){
 const root=resolve(dir);
 const named=["RELEASE.tsv","FILES.tsv","SITE-PAGES.tsv"];
 const records=parseRows(readFileSync(join(root,"FILES.tsv"),"utf8"),"path\tsha256");
 if(!records.length||records.length>12000)fail("no emitted site files");
 const paths=new Set(),items=[];
 for(const path of named){
  const file=join(root,path);
  if(!lstatSync(file).isFile())fail("manifest isn't regular file");
  const body=readFileSync(file);
  if(body.length>512*1024)fail("manifest too large");
  items.push([path,body]);
 }
 let total=0;
 for(const [path,digest]of records){
  if(paths.has(path))fail("duplicate file manifest row");
  paths.add(path);
  const file=resolve(root,path);
  if(!file.startsWith(root+sep))fail("path traversal");
  const stat=lstatSync(file);
  if(!stat.isFile()||stat.size>16*1024*1024)fail("unapproved asset or symlink");
  const content=readFileSync(file);
  if(sha(content)!==digest)fail("asset changed after release compilation");
  total+=content.length;
  if(total>80*1024*1024)fail("site exceeds public archive limit");
  items.push([path,content]);
 }
 if(!paths.has("site/index.html")||!paths.has("site/assets/site.css"))
  fail("published homepage or shared CSS absent");
 return items.sort(([a],[b])=>a.localeCompare(b));
}
export function buildAuthoredReleaseArchive(directory){
 const items=pinnedMembers(directory),chunks=[];
 let bytes=1024;
 for(const [path,content]of items){
  bytes+=512+Math.ceil(content.length/512)*512;
  if(bytes>96*1024*1024)fail("uncompressed tar exceeds limits");
  chunks.push(header(path,content.length),content);
  const remainder=content.length%512;
  if(remainder)chunks.push(Buffer.alloc(512-remainder));
 }
 chunks.push(Buffer.alloc(1024));
 const compressed=gzipSync(Buffer.concat(chunks),{level:9,mtime:0});
 if(compressed.length>96*1024*1024)fail("compressed tar exceeds limits");
 return {bytes:compressed,sha256:sha(compressed),file_count:items.length};
}
export function writeAuthoredReleaseArchive(directory,archive){
 if(typeof archive!=="string"||!archive.endsWith(".tar.gz")||existsSync(archive))
  fail("new .tar.gz destination required");
 const result=buildAuthoredReleaseArchive(directory);
 writeFileSync(archive,result.bytes,{flag:"wx",mode:0o644});
 return {archive,sha256:result.sha256,files:result.file_count,
  bytes:result.bytes.length};
}
