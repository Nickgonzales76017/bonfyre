// Pinned build inputs for the public-address QR projection. No runtime requests,
// install hooks, private files, credentials, or externally hosted QR generator.
import {readFile,lstat,mkdir,mkdtemp,writeFile,rename,rm} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';

export const pin='720f62bddb7226106071d4728c292cb1df519ceb';
export const inputs=Object.freeze([
  {name:'qrcodegen.c',bytes:41771,sha256:'300eff07ee25baaa7578f20284411638154716379437391e7e689c0e6ce81403'},
  {name:'qrcodegen.h',bytes:17866,sha256:'e82df4bff37d18b5863b9e7486fe6bda1b6cda8c3b9ecebfec473907265cb589'},
]);
const base='https://raw.githubusercontent.com/nayuki/QR-Code-generator/'+pin+'/c/';
const digest=body=>createHash('sha256').update(body).digest('hex');
const here=dirname(fileURLToPath(import.meta.url));
export const buildDirectory=resolve(here,'../build');
export const inputDirectory=join(buildDirectory,'public-code');
export function verifyInput(body,input) {
  if(body.length!==input.bytes||digest(body)!==input.sha256)throw Error('QR build input does not match its reviewed source: '+input.name);
  if(!body.subarray(0,1800).toString('utf8').includes('Copyright (c) Project Nayuki. (MIT License)'))throw Error('QR build-input notice is missing.');
  return body;
}
async function directory(path,create=false) {
  if(create)await mkdir(path,{recursive:true});
  const st=await lstat(path);
  if(!st.isDirectory()||st.isSymbolicLink())throw Error('Build input directory must be a real directory.');
}
async function regular(path,input) {
  const st=await lstat(path);
  if(!st.isFile()||st.isSymbolicLink()||st.size!==input.bytes)throw Error('QR build input must be an exact regular file: '+input.name);
  return verifyInput(await readFile(path),input);
}
async function fetchInput(input) {
  const response=await fetch(base+input.name,{redirect:'error',credentials:'omit',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw Error('QR source returned HTTP '+response.status);
  const reader=response.body.getReader(),parts=[];let size=0;
  try { for(;;) {const part=await reader.read();if(part.done)break;size+=part.value.byteLength;
    if(size>input.bytes){await reader.cancel();throw Error('QR source exceeded the pinned length.');}parts.push(part.value);}
  } finally {reader.releaseLock();}
  return verifyInput(Buffer.concat(parts,size),input);
}
export async function verifyPrepared() {
  await directory(buildDirectory);await directory(inputDirectory);
  for(const input of inputs)await regular(join(inputDirectory,input.name),input);
  const manifest=await readFile(join(inputDirectory,'SOURCE.tsv'),'utf8');
  if(manifest!==sourceManifest())throw Error('QR build-input manifest changed.');
  const source=await regular(join(inputDirectory,'qrcodegen.c'),inputs[0]);
  if((await readFile(join(inputDirectory,'LICENSE.txt'),'utf8'))!==notice(source))throw Error('QR build-input licence changed.');
  return true;
}
function sourceManifest(){return 'source\tsha256\tbytes\n'+inputs.map(x=>base+x.name+'\t'+x.sha256+'\t'+x.bytes+'\n').join('');}
function notice(source){const text=source.toString('utf8');const end=text.indexOf('*/');if(end<0||end>2000)throw Error('Upstream notice boundary changed.');return text.slice(0,end+2)+'\n';}
export async function prepare({paths=null,allowFetch=false}={}) {
  try {await verifyPrepared();return {cached:true,directory:inputDirectory};}
  catch(error){if(error.code!=='ENOENT')throw error;}
  if(paths&&(!Array.isArray(paths)||paths.length!==inputs.length))throw Error('Supply both retained source bodies in C/header order.');
  if(!paths&&!allowFetch)throw Error('Missing pinned QR inputs. Run: bun tools/prepare_public_code.mjs --fetch; or supply the two retained public source body paths.');
  const bodies=[];
  // Read/validate every input before making any output visible to the compiler.
  for(let i=0;i<inputs.length;i++)bodies.push(paths?await regular(paths[i],inputs[i]):await fetchInput(inputs[i]));
  await directory(buildDirectory,true);
  const stage=await mkdtemp(join(buildDirectory,'.public-code-'));
  try {
    for(let i=0;i<inputs.length;i++)await writeFile(join(stage,inputs[i].name),bodies[i],{flag:'wx',mode:0o400});
    await writeFile(join(stage,'SOURCE.tsv'),sourceManifest(),{flag:'wx',mode:0o400});
    await writeFile(join(stage,'LICENSE.txt'),notice(bodies[0]),{flag:'wx',mode:0o400});
    try {await rename(stage,inputDirectory);}
    catch(error){if(!['EEXIST','ENOTEMPTY'].includes(error.code))throw error;await verifyPrepared();}
    await verifyPrepared();
    return {cached:false,directory:inputDirectory};
  } finally {await rm(stage,{recursive:true,force:true});}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const args=process.argv.slice(2);
  if(args.length===1&&args[0]==='--check')await verifyPrepared();
  else if(args.length===1&&args[0]==='--fetch')console.log(await prepare({allowFetch:true}));
  else if(args.length===2&&!args.some(x=>x.startsWith('--')))console.log(await prepare({paths:args}));
  else throw Error('Use --check, --fetch, or two exact retained source body paths.');
}
