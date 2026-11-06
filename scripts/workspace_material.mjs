// Physical storage for the existing workspace release owner. The manifest still
// identifies the release; storage sharing grants no new read or effect rights.
import {constants} from 'node:fs';
import {open,lstat,realpath,mkdir,mkdtemp,rename,link,rm} from 'node:fs/promises';
import {join,resolve,dirname} from 'node:path';
import {createHash} from 'node:crypto';

export const workspaceLayout = 'layout\tshared-bodies-v1\n';
export const workspaceLayoutFile = 'STORAGE.tsv';
const HEADER='path\tsha256\tbytes\n', FILE_LIMIT=16*1024*1024, TOTAL_LIMIT=96*1024*1024;
const MANIFEST_LIMIT=256*1024, MAX_FILES=1000;
const hash=body=>createHash('sha256').update(body).digest('hex');
const digest=value=>typeof value==='string' && /^[a-f0-9]{64}$/.test(value);
const safe=path=>typeof path==='string' && path.length<=1000 && /^[a-zA-Z0-9_./-]+$/.test(path)
  && !path.startsWith('/') && !path.split('/').some(p=>!p||p==='.'||p==='..');
const reserved=path=>path==='MANIFEST.tsv'||path===workspaceLayoutFile;
const missing=error=>error?.code==='ENOENT';

export function manifestRows(text) {
  if(typeof text!=='string'||Buffer.byteLength(text)>MANIFEST_LIMIT||!text.startsWith(HEADER)||!text.endsWith('\n'))
    throw Error('Unsupported source manifest.');
  const lines=text.slice(HEADER.length,-1).split('\n');
  if(!lines.length||lines.length>MAX_FILES)throw Error('Unsupported source manifest.');
  const seen=new Set(), sizes=new Map(); let total=0;
  const rows=lines.map(line=>{
    const [path,sha,raw,...extra]=line.split('\t'), bytes=Number(raw);
    if(extra.length||!safe(path)||reserved(path)||seen.has(path)||!digest(sha)||!/^\d+$/.test(raw??'')
      ||!Number.isSafeInteger(bytes)||bytes<0||bytes>FILE_LIMIT||(total+=bytes)>TOTAL_LIMIT
      ||(sizes.has(sha)&&sizes.get(sha)!==bytes))throw Error('Unsafe source manifest entry.');
    seen.add(path);sizes.set(sha,bytes);return {path,hash:sha,bytes};
  });
  // A file cannot also be a directory, including a later manifest member.
  for(const {path}of rows){const parts=path.split('/');parts.pop();while(parts.length){if(seen.has(parts.join('/')))throw Error('Source manifest path conflict.');parts.pop();}}
  return rows;
}

async function rootPath(root,create=false) {
  root=resolve(root);
  if(create)await mkdir(root,{recursive:true});
  const st=await lstat(root);
  if(!st.isDirectory()||st.isSymbolicLink())throw Error('Workspace root must be a real directory.');
  // The explicitly configured root may have platform aliases in its ancestors.
  // Every path below this canonical root is checked independently.
  return realpath(root);
}
async function directory(root,path,create=false) {
  let at=root;
  for(const part of path.split('/')){
    at=join(at,part);
    if(create)try{await mkdir(at,{mode:0o700});}catch(e){if(e.code!=='EEXIST')throw e;}
    const st=await lstat(at);if(!st.isDirectory()||st.isSymbolicLink())throw Error('Unsafe workspace directory.');
  }
  return at;
}
const fingerprint=st=>[st.dev,st.ino,st.size,st.mtimeNs,st.ctimeNs].join(':');
// Publishing uses link(staged, body), then removes the staging link. That
// retirement changes ctime without changing bytes. Retry only that shape;
// every attempt still checks a stable descriptor and its caller checks SHA-256.
const retiredLink=(before,after)=>before.nlink>after.nlink&&after.nlink>=1n
  &&['dev','ino','size','mtimeNs','mode','uid','gid'].every(key=>before[key]===after[key]);
async function readRegular(root,path,limit) {
  if(!safe(path))throw Error('Unsafe workspace path.');
  for(let attempt=0;attempt<3;attempt++){
    if(path.includes('/'))await directory(root,path.slice(0,path.lastIndexOf('/')));
    const full=join(root,path), before=await lstat(full,{bigint:true});
    if(!before.isFile()||before.isSymbolicLink()||before.size>BigInt(limit))throw Error('Unsafe workspace file.');
    const fd=await open(full,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
    try{
      const opened=await fd.stat({bigint:true});
      if(!opened.isFile()||fingerprint(opened)!==fingerprint(before)){
        if(attempt<2&&opened.isFile()&&retiredLink(before,opened))continue;
        throw Error('Workspace file changed while opening.');
      }
      const bytes=Buffer.alloc(Number(opened.size));let at=0;
      while(at<bytes.length){const {bytesRead}=await fd.read(bytes,at,bytes.length-at,at);if(!bytesRead)throw Error('Workspace file shortened while reading.');at+=bytesRead;}
      const end=await fd.stat({bigint:true});
      if(fingerprint(end)!==fingerprint(opened)){
        if(attempt<2&&retiredLink(opened,end))continue;
        throw Error('Workspace file changed while reading.');
      }
      return bytes;
    }finally{await fd.close();}
  }
  throw Error('Workspace file did not stabilize.');
}
async function syncDirectory(path) {
  const fd=await open(path,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);
  try{await fd.sync();}finally{await fd.close();}
}
async function writeDurable(path,body) {
  const fd=await open(path,'wx',0o600);
  try{await fd.writeFile(body);await fd.chmod(0o400);await fd.sync();}finally{await fd.close();}
}
const bodyPath=sha=>`_bodies/sha256/${sha.slice(0,2)}/${sha}`;
function checked(body,row) {
  if(body.length!==row.bytes||hash(body)!==row.hash)throw Error('Source changed: '+row.path);
  return body;
}
async function releaseInfo(root,id) {
  if(!digest(id))throw Error('Invalid workspace release.');
  try{await directory(root,id);}catch(e){if(missing(e))return null;throw e;}
  // An existing incomplete release is a conflict, never a cache miss to repair.
  let body;try{body=await readRegular(root,id+'/MANIFEST.tsv',MANIFEST_LIMIT);}catch(e){throw Error('Incomplete workspace release.',{cause:e});}
  const text=body.toString('utf8');if(!body.equals(Buffer.from(text))||hash(body)!==id)throw Error('Package identity conflict.');
  const rows=manifestRows(text);let shared=false;
  try{
    const layout=await readRegular(root,id+'/'+workspaceLayoutFile,128);
    if(!layout.equals(Buffer.from(workspaceLayout)))throw Error('Unsupported workspace storage layout.');
    shared=true;
  }catch(e){if(!missing(e))throw e;}
  return {root,id,dir:join(root,id),text,rows,shared};
}
async function readMember(info,row) {
  const path=info.shared?bodyPath(row.hash):info.id+'/'+row.path;
  return checked(await readRegular(info.root,path,FILE_LIMIT),row);
}
async function verifyInfo(info) {
  const seen=new Set();let verifiedBytes=0;
  for(const row of info.rows){const key=info.shared?row.hash:row.path;if(seen.has(key))continue;
    verifiedBytes+=(await readMember(info,row)).length;seen.add(key);}
  return verifiedBytes;
}
export async function verifyWorkspacePackage(root,id) {
  const info=await releaseInfo(await rootPath(root),id);if(!info)throw Error('Workspace release is unavailable.');
  const verifiedBytes=await verifyInfo(info);
  return {id,dir:info.dir,files:info.rows.length,layout:info.shared?'shared-bodies-v1':'legacy-tree',verifiedBytes};
}
export async function readWorkspaceFile(root,id,path) {
  const info=await releaseInfo(await rootPath(root),id);if(!info)throw Error('Workspace release is unavailable.');
  const row=info.rows.find(r=>r.path===path);if(!row)throw Error('Workspace member is not declared.');
  return readMember(info,row);
}
// Only the source materializer uses this lookup, after reading a reviewed source
// manifest. No public endpoint accepts a bare body digest as read permission.
export async function readWorkspaceBody(root,sha,bytes) {
  if(!digest(sha)||!Number.isSafeInteger(bytes)||bytes<0||bytes>FILE_LIMIT)throw Error('Invalid workspace body coordinate.');
  try{return checked(await readRegular(await rootPath(root),bodyPath(sha),FILE_LIMIT),{path:sha,hash:sha,bytes});}
  catch(e){if(missing(e))return null;throw e;}
}

export async function retainWorkspaceMaterial(files,{root,manifest}={}) {
  if(!(files instanceof Map)||!files.size)throw Error('Provide the exact reviewed workspace files.');
  // Freeze caller-owned bytes before the first await; no later mutation changes
  // the material between its admission hash and its publication.
  const selected=new Map();let total=0;
  for(const [path,value]of files){
    if(!Buffer.isBuffer(value)&&!(value instanceof Uint8Array))throw Error('Workspace files must be byte bodies.');
    if(!safe(path)||reserved(path)||value.byteLength>FILE_LIMIT||(total+=value.byteLength)>TOTAL_LIMIT||selected.size>=MAX_FILES)
      throw Error('Unsafe source manifest entry.');
    selected.set(path,Buffer.from(value));
  }
  const text=manifest??(HEADER+[...selected].sort(([a],[b])=>a.localeCompare(b,'en'))
    .map(([path,body])=>`${path}\t${hash(body)}\t${body.length}\n`).join(''));
  const rows=manifestRows(text);
  if(rows.length!==selected.size)throw Error('Source manifest does not match the reviewed files.');
  const unique=new Map();for(const row of rows){const body=selected.get(row.path);if(!body)throw Error('Source manifest member is missing.');checked(body,row);unique.set(row.hash,{row,body});}
  root=await rootPath(root,true);const id=hash(text), old=await releaseInfo(root,id);
  if(old){if(old.text!==text)throw Error('Package identity conflict.');const verifiedBytes=await verifyInfo(old);
    return {id,dir:old.dir,files:rows.length,layout:old.shared?'shared-bodies-v1':'legacy-tree',newBodies:0,writtenBodyBytes:0,reusedBodyBytes:total,verifiedBytes,manifestBytes:0};}
  const temp=await mkdtemp(join(root,'.workspace-'));let newBodies=0,writtenBodyBytes=0,verifiedBytes=0;
  try{
    for(const {row,body}of unique.values()){
      const relative=bodyPath(row.hash), parent=await directory(root,dirname(relative),true);
      try{verifiedBytes+=checked(await readRegular(root,relative,FILE_LIMIT),row).length;continue;}catch(e){if(!missing(e))throw e;}
      const staged=join(temp,row.hash);await writeDurable(staged,body);
      try{await link(staged,join(root,relative));await syncDirectory(parent);newBodies++;writtenBodyBytes+=body.length;}
      catch(e){if(e.code!=='EEXIST')throw e;verifiedBytes+=checked(await readRegular(root,relative,FILE_LIMIT),row).length;}
      await rm(staged);
    }
    // Directory metadata reaches disk before a release can name its bodies.
    await syncDirectory(join(root,'_bodies/sha256'));await syncDirectory(join(root,'_bodies'));await syncDirectory(root);
    await writeDurable(join(temp,'MANIFEST.tsv'),Buffer.from(text));
    await writeDurable(join(temp,workspaceLayoutFile),Buffer.from(workspaceLayout));
    await syncDirectory(temp);
    try{await rename(temp,join(root,id));await syncDirectory(root);}
    catch(e){if(!['EEXIST','ENOTEMPTY'].includes(e.code))throw e;
      const winner=await releaseInfo(root,id);if(!winner||winner.text!==text)throw Error('Concurrent workspace publication conflict.');
      verifiedBytes+=await verifyInfo(winner);}
    return {id,dir:join(root,id),files:rows.length,layout:'shared-bodies-v1',newBodies,writtenBodyBytes,
      reusedBodyBytes:total-writtenBodyBytes,verifiedBytes,manifestBytes:Buffer.byteLength(text)+Buffer.byteLength(workspaceLayout)};
  }finally{
    // A failed run removes only its own staging directory. Already published
    // bodies may be referenced by a concurrent run and are not reclaimed here.
    await rm(temp,{recursive:true,force:true});
  }
}

export async function loadWorkspaceFiles(root,id) {
  const info=await releaseInfo(await rootPath(root),id);if(!info)throw Error('Workspace release is unavailable.');
  const files=new Map(),seen=new Map();
  for(const row of info.rows){const key=info.shared?row.hash:row.path;let body=seen.get(key);
    if(!body){body=await readMember(info,row);seen.set(key,body);}files.set(row.path,body);}
  return files;
}
