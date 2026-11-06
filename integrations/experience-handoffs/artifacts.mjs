import {mkdirSync,openSync,closeSync,readFileSync,writeFileSync,linkSync,unlinkSync,fsyncSync,fstatSync,constants,readdirSync} from 'node:fs';
import {resolve,join,dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import {sha256,validId,validSha} from './contract.mjs';
const extPattern=/^(yaff|md)$/;
const MAX=262144;
const bounded=bytes=>{const b=Buffer.isBuffer(bytes)?bytes:Buffer.from(bytes,'utf8');if(b.length>MAX)throw Error('Handoff artifact exceeds 256 KiB.');return b;};
export function artifacts(home){
  const root=resolve(home);
  mkdirSync(root,{recursive:true,mode:0o700});
  function read(file){
    const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);
    try{const stat=fstatSync(fd);if(!stat.isFile()||stat.size>MAX)throw Error('Invalid artifact file.');return readFileSync(fd);}
    finally{closeSync(fd);}
  }
  function immutable(file,content){
    const bytes=bounded(content),temporary=file+'.'+randomUUID()+'.tmp';
    mkdirSync(dirname(file),{recursive:true,mode:0o700});
    const fd=openSync(temporary,'wx',0o600);
    try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
    try{
      try{linkSync(temporary,file);}
      catch(error){if(error.code!=='EEXIST')throw error;if(!read(file).equals(bytes))throw Error('Immutable handoff artifact changed.');}
      const folder=openSync(dirname(file),'r');try{fsyncSync(folder);}finally{closeSync(folder);}
    }finally{unlinkSync(temporary);}
    return file;
  }
  function path(digest,ext){
    if(!validSha(digest)||!extPattern.test(ext))throw Error('Invalid artifact identity.');
    return join(root,'objects',digest.slice(0,2),digest+'.'+ext);
  }
  function put(bytes,ext){
    const value=bounded(bytes),digest=sha256(value);
    immutable(path(digest,ext),value);
    return {sha256:digest,path:path(digest,ext),bytes:value.length};
  }
  function get(digest,ext){
    const value=read(path(digest,ext));
    if(sha256(value)!==digest)throw Error('Handoff content digest mismatched.');
    return value;
  }
  function envelopePath(org,target,id){
    if(!validId(org)||!validId(target)||!validSha(id))throw Error('Invalid inbox identity.');
    return join(root,'inbox',org,target,id+'.yaff');
  }
  function inbox(org,target){
    const dir=dirname(envelopePath(org,target,'0'.repeat(64)));
    try{return readdirSync(dir).filter(x=>/^[a-f0-9]{64}\.yaff$/.test(x)).map(x=>x.slice(0,64)).sort();}
    catch(e){if(e.code==='ENOENT')return [];throw e;}
  }
  return {root,put,get,read,immutable,envelopePath,inbox};
}
