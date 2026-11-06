import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync,mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const exporter=process.env.BONFYRE_CLIP_EXPORT||resolve(import.meta.dir,'../tools/export_clip.mjs');
const {sha256File}=await import(pathToFileURL(exporter).href);

test('chunked file hashes match independent byte hashing at buffer boundaries',()=>{
  const root=mkdtempSync(join(tmpdir(),'clip-hash-'));
  try{
    const file=join(root,'media.bin');
    for(const size of [0,1,1048575,1048576,1048577,3145745]){
      const bytes=Buffer.alloc(size);
      for(let i=0;i<size;i++)bytes[i]=(i*71+(i>>>8))&255;
      writeFileSync(file,bytes);
      expect(sha256File(file)).toBe(createHash('sha256').update(bytes).digest('hex'));
    }
    expect(()=>sha256File(join(root,'missing'))).toThrow();
    mkdirSync(join(root,'directory'));
    expect(()=>sha256File(join(root,'directory'))).toThrow('regular file');
  }finally{rmSync(root,{recursive:true,force:true});}
});

test('short reads and interruptions preserve the digest; read failures and changed files refuse',()=>{
  const root=mkdtempSync(join(tmpdir(),'clip-hash-fault-'));
  try{
    const input=join(root,'source.bin'),script=join(root,'fault.mjs');
    const bytes=Buffer.alloc(2*1024*1024+29,0xa5),expected=createHash('sha256').update(bytes).digest('hex');
    writeFileSync(script,`
import {mock} from 'bun:test';
import * as fs from 'node:fs';
const [file,mode,module]=process.argv.slice(2);
const realRead=fs.readSync,realClose=fs.closeSync,realWrite=fs.writeFileSync,realRename=fs.renameSync;
let reads=0,closes=0;
mock.module('node:fs',()=>({...fs,
  readSync(fd,buffer,offset,length,position){
    reads++;
    if(mode==='interrupt'&&reads===1)throw Object.assign(Error('interrupted'),{code:'EINTR'});
    if(mode==='io-error'&&reads===2)throw Object.assign(Error('read failed'),{code:'EIO'});
    const count=realRead(fd,buffer,offset,mode==='short'?Math.min(length,4093):length,position);
    if(reads===1&&mode==='truncate')realWrite(file,'changed');
    if(reads===1&&mode==='replace'){realWrite(file+'.new',Buffer.alloc(2*1024*1024+29,0xa5));realRename(file+'.new',file);}
    return count;
  },
  closeSync(fd){closes++;return realClose(fd);}
}));
const {sha256File}=await import(module);
let digest=null,error=null;
try{digest=sha256File(file);}catch(e){error=e.message;}
console.log(JSON.stringify({digest,error,reads,closes}));
`);
    for(const mode of ['short','interrupt','io-error','truncate','replace']){
      writeFileSync(input,bytes);
      const result=Bun.spawnSync([process.execPath,script,input,mode,pathToFileURL(exporter).href],{timeout:10000});
      expect(result.exitCode,result.stderr.toString()).toBe(0);
      const observation=JSON.parse(result.stdout.toString());
      expect(observation.closes).toBe(1);
      if(mode==='short'||mode==='interrupt'){
        expect(observation.error).toBeNull();expect(observation.digest).toBe(expected);
      }else{
        expect(observation.digest).toBeNull();expect(observation.error).toContain(mode==='io-error'?'read failed':'changed while hashing');
      }
    }
  }finally{rmSync(root,{recursive:true,force:true});}
},30000);
