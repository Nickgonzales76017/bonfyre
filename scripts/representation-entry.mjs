#!/usr/bin/env bun
// Installed entry point for the existing Manifold owners. No checkout fallback.
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,readdirSync,readFileSync,realpathSync} from 'node:fs';
import {dirname,join,resolve,relative} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath} from 'node:url';
export const digestBytes=bytes=>createHash('sha256').update(bytes).digest('hex');
export function releaseFiles(root){
  const files=[];
  function visit(dir){for(const name of readdirSync(dir).sort()){
    const path=join(dir,name),s=lstatSync(path),rel=relative(root,path);
    if(s.isSymbolicLink())throw Error('Release members cannot be symlinks.');
    if(s.isDirectory())visit(path);
    else if(s.isFile()&&rel!=='release.tsv'){
      if(!/^[A-Za-z0-9_./-]+$/.test(rel)||s.size>8*1024*1024)throw Error('Unsupported release member.');
      files.push({path:rel,sha256:digestBytes(readFileSync(path)),bytes:s.size});
    }else if(!s.isFile())throw Error('Release members must be regular files.');
  }}
  visit(root);return files.sort((a,b)=>a.path.localeCompare(b.path,'en'));
}
export function verifyRelease(root,expected=undefined){
  root=realpathSync(root);
  const manifest=join(root,'release.tsv');
  if(!lstatSync(manifest).isFile())throw Error('Missing regular release manifest.');
  const bytes=readFileSync(manifest),digest=digestBytes(bytes);
  if(!/^[a-f0-9]{64}$/.test(expected??root.split('/').at(-1))||digest!==(expected??root.split('/').at(-1)))throw Error('Release identity does not match its manifest.');
  const files=releaseFiles(root),actual=files.map(x=>x.sha256+'\t'+x.path+'\n').join('');
  if(bytes.toString()!==actual)throw Error('Release member digest or inventory mismatch.');
  for(const required of ['scripts/representation-entry.mjs','scripts/representation-manifold-shift.mjs','scripts/representation-manifold-table-cut.mjs','cmd/BonfyreHash/bonfyre-hash'])
    if(!files.some(f=>f.path===required))throw Error('Missing required representation owner: '+required);
  return {root,digest,files:files.length,bytes:files.reduce((n,x)=>n+x.bytes,0)};
}
export async function representationMain(args){
  const [verb='help',...rest]=args;
  if(verb==='help'||verb==='--help'){
    console.log('bonfyre-runtime representation doctor|status|verify|apply\n  status [--estate DIR]\n  verify --estate DIR --kind DATABASE.TABLE [--kind DATABASE.TABLE]\n  apply --estate DIR --kind DATABASE.TABLE [--kind DATABASE.TABLE]\nVerification saves no materializations. Apply requires an explicit source demand.');return 0;
  }
  if(!['doctor','status','verify','apply'].includes(verb))throw Error('Unknown representation operation.');
  let estate=null;const kinds=[];
  for(let i=0;i<rest.length;i+=2){const key=rest[i],value=rest[i+1];if(!value||value.startsWith('--'))throw Error('Each option requires a value.');
    if(key==='--estate'&&estate===null)estate=resolve(value);else if(key==='--kind')kinds.push(value);else throw Error('Unknown or repeated representation option.');}
  if(verb==='doctor'&&rest.length)throw Error('Doctor takes no options.');
  if(verb==='status'&&kinds.length)throw Error('Status takes no demand.');
  if(['verify','apply'].includes(verb)&&(!estate||!kinds.length))throw Error('An explicit --estate and --kind are required.');
  const release=verifyRelease(resolve(dirname(fileURLToPath(import.meta.url)),'..'));
  if(verb==='doctor'){console.log('state INSTALLED_VERIFIED\nrelease '+release.digest+'\nfiles '+release.files+'\nroot '+release.root+'\nowner BonfyreHash + RepresentationManifold');return 0;}
  const owner=await import('./representation-manifold-shift.mjs');
  const options={hashBinary:join(release.root,'cmd/BonfyreHash/bonfyre-hash')};
  const result=verb==='status'?owner.status(estate??join(homedir(),'.bonfyre/estate-local')):
    verb==='verify'?await owner.verifyDemand(estate,kinds,options):await owner.apply(estate,undefined,{...options,kinds});
  console.log('release '+release.digest+'\nstate '+(result.state??result.status?.state));
  if(result.demand){const d=result.demand;console.log('demand '+d.kinds.join(', ')+'\nreused '+d.reused+'\nlowered '+d.lowerings+'\nheld '+d.held+'\nsource_snapshots '+d.source_snapshots+'\nsource_snapshot_bytes '+d.source_snapshot_bytes+'\nsnapshot_required '+(d.snapshot_required??0));}
  if(result.counts)for(const [key,value]of Object.entries(result.counts))console.log(key+' '+value);
  if(verb==='verify'&&result.state!=='REUSABLE')return 3;
  if(verb==='apply'&&result.demand.held)return 3;
  return 0;
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===realpathSync(process.argv[1])){
  try{process.exitCode=await representationMain(process.argv.slice(2));}catch(error){console.error(error.message);process.exitCode=1;}
}
