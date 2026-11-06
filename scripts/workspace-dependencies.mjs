// Build-time provider for the exact open-source docking dependency. No install
// scripts, live CDN imports, enterprise modules or new browser record store.
import {readFile,writeFile,mkdir,mkdtemp,rm,stat} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const providers={
 core:{name:'dockview-core',integrity:'sha512-lUNJj4uvE36ks59pMeLeinzvVr0EVkGiA/c1kuUdR2V49CAIGx+bhiu0FuZNh8iqcggu/5h1v1U0KC0JrUuPSw=='},
 entry:{name:'dockview',integrity:'sha512-ThlNLXBHP1TBMlsAXTWvoJl17nPYVO8WTtUMyoGf15K6DBCRnoKPzIcISlWben9XBx99txUfSjkVBZ9szGA9iQ=='}
};
const files=[
 ['dockview-8.5.1.mjs','core','package/dist/package/main.esm.mjs','ccd1a49ec50a5cc33a2ab245b42b9e826742a9e84685e6d7e3e126ffc51fc39c'],
 ['dockview-8.5.1.css','entry','package/dist/styles/dockview.css','72a757b4d2d0d0f1e38b928b1343c85a7b09766bb2dc2c16c9e3c880f231545d'],
 ['LICENSE.md','core','package/LICENCE.md','1d56757fdc06c9b33c0f80821efe4f4b11960dd7fb6544232cff61752101f513']
];
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
async function optional(path){try{return await readFile(path);}catch(error){if(error.code==='ENOENT')return null;throw error;}}
export async function ensureWorkspaceDocking(base,{input=process.env.WORKSPACE_INPUTS??'workspace-inputs'}={}){
 const destination=join(base,'assets/workspace-docking'),missing=[];
 for(const row of files){const bytes=await optional(join(destination,row[0]));if(bytes&&hash(bytes)!==row[3])throw Error('Existing docking dependency differs from its admitted version: '+row[0]);if(!bytes)missing.push(row);}
 if(missing.length){
  const temp=await mkdtemp(join(tmpdir(),'bf-docking-')),archives=new Map();
  try{
   for(const [,provider]of missing){if(archives.has(provider))continue;const p=providers[provider],name=p.name+'-8.5.1.tgz',url=`https://registry.npmjs.org/${p.name}/-/${name}`;
    let bytes=await optional(join(input,name));
    if(!bytes){const response=await fetch(url,{redirect:'error',signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Docking source unavailable: '+response.status);const chunks=[];let size=0;for await(const chunk of response.body){size+=chunk.length;if(size>2000000)throw Error('Docking archive exceeds its bound.');chunks.push(chunk);}bytes=Buffer.concat(chunks);}
    if(bytes.length>2000000||'sha512-'+createHash('sha512').update(bytes).digest('base64')!==p.integrity)throw Error('Pinned docking archive integrity mismatch.');
    const path=join(temp,name);await writeFile(path,bytes);archives.set(provider,path);
   }
   const outputs=missing.map(([name,provider,member,digest])=>{const bytes=execFileSync('tar',['-xOzf',archives.get(provider),member],{maxBuffer:2000000,timeout:15000});if(hash(bytes)!==digest)throw Error('Pinned module identity mismatch: '+name);return [name,bytes];});
   await mkdir(destination,{recursive:true});for(const [name,bytes]of outputs)await writeFile(join(destination,name),bytes);
  }finally{await rm(temp,{recursive:true,force:true});}
 }
 const provenance='Dockview 8.5.1. MIT core docking only.\n'+Object.values(providers).map(p=>`https://registry.npmjs.org/${p.name}/-/${p.name}-8.5.1.tgz\n${p.integrity}`).join('\n')+'\n';
 await writeFile(join(destination,'SOURCE.txt'),provenance);
 return destination;
}
