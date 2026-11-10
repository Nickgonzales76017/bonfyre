#!/usr/bin/env bun
// Build with the existing installer, verify a complete release, then activate it.
import {mkdirSync,mkdtempSync,readFileSync,writeFileSync,lstatSync,existsSync,realpathSync,renameSync,symlinkSync,rmSync,chmodSync} from 'node:fs';
import {join,resolve,dirname} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {digestBytes,releaseFiles,verifyRelease} from './representation-entry.mjs';
const repo=resolve(dirname(fileURLToPath(import.meta.url)),'..');
function currentRelease(base){
  const current=join(base,'current');let s;
  try{s=lstatSync(current);}catch(e){if(e.code==='ENOENT')return null;throw e;}
  if(!s.isSymbolicLink())throw Error('Current release must be an owned release link.');
  const target=realpathSync(current),id=target.split('/').at(-1);
  if(target!==join(base,'releases',id)||!/^[a-f0-9]{64}$/.test(id))throw Error('Current link escapes representation releases.');
  verifyRelease(target);return id;
}
export async function installRepresentation({prefix=join(homedir(),'.bonfyre'),expectedCurrent}={}){
  prefix=resolve(prefix);mkdirSync(prefix,{recursive:true});prefix=realpathSync(prefix);
  const base=join(prefix,'share/bonfyre/representation'),releases=join(base,'releases'),bin=join(prefix,'bin');
  for(const path of [join(prefix,'share'),join(prefix,'share/bonfyre'),base,releases,bin]){
    if(existsSync(path)&&!lstatSync(path).isDirectory())throw Error('Install directory must be a regular directory.');mkdirSync(path,{recursive:true});
    if(realpathSync(path)!==path)throw Error('Install directory cannot traverse a symlink.');
  }
  const lock=join(base,'activation.lock');mkdirSync(lock);
  let stage,temporary,link;
  try{
    const previous=currentRelease(base);
    if(expectedCurrent!==undefined&&previous!==expectedCurrent)throw Error('Active release changed before installation.');
    stage=mkdtempSync(join(releases,'.stage-'));
    const make=Bun.which('make',{PATH:process.env.PATH});
    if(!make)throw Error('Representation build failed: make is unavailable.');
    const built=Bun.spawnSync([make,'-C',join(repo,'scripts'),'install-representation-fabric','MEMBRANE_ROOT='+stage],{cwd:repo,env:{...process.env},stdout:'pipe',stderr:'pipe',timeout:60000});
    if(built.exitCode)throw Error('Representation build failed: '+built.stderr.toString().slice(-1800));
    const entry=readFileSync(join(repo,'scripts/representation-entry.mjs'));writeFileSync(join(stage,'scripts/representation-entry.mjs'),entry);
    const files=releaseFiles(stage),manifest=files.map(f=>f.sha256+'\t'+f.path+'\n').join(''),digest=digestBytes(manifest);
    writeFileSync(join(stage,'release.tsv'),manifest);verifyRelease(stage,digest);
    const release=join(releases,digest);
    if(existsSync(release)){verifyRelease(release);rmSync(stage,{recursive:true});}else renameSync(stage,release);
    stage=null;
    const doctor=Bun.spawnSync([process.execPath,join(release,'scripts/representation-entry.mjs'),'doctor'],{cwd:prefix,stdout:'pipe',stderr:'pipe',timeout:15000});
    if(doctor.exitCode||!doctor.stdout.toString().includes('state INSTALLED_VERIFIED'))throw Error('Installed release did not pass its own entry point.');
    // Resolve only the selected release. A missing member never falls back to the checkout.
    const launcher='#!/bin/sh\nset -eu\nbase=$(CDPATH= cd -- "$(dirname -- "$0")/../share/bonfyre/representation" && pwd)\n'+
      'bun=${BONFYRE_BUN_BINARY:-}\nif [ -z "$bun" ]; then bun=$(command -v bun || true); fi\n'+
      'if [ -z "$bun" ]; then bun="$HOME/.bun/bin/bun"; fi\n'+
      'exec "$bun" "$base/current/scripts/representation-entry.mjs" "$@"\n';
    const launcherPath=join(bin,'bonfyre-representation');
    if(existsSync(launcherPath)&&(!lstatSync(launcherPath).isFile()||readFileSync(launcherPath,'utf8')!==launcher))throw Error('Existing representation launcher is not owned by this installer.');
    if(currentRelease(base)!==previous)throw Error('Active release changed while building.');
    temporary=join(bin,'.representation-'+randomUUID());writeFileSync(temporary,launcher,{mode:0o755,flag:'wx'});renameSync(temporary,launcherPath);temporary=null;
    link=join(base,'.current-'+randomUUID());symlinkSync(join('releases',digest),link);renameSync(link,join(base,'current'));link=null;
    if(currentRelease(base)!==digest)throw Error('Release activation readback failed.');
    return {digest,previous,release,launcher:launcherPath,files:files.length,activated:true};
  }finally{if(stage)rmSync(stage,{recursive:true,force:true});if(temporary)rmSync(temporary,{force:true});if(link)rmSync(link,{force:true});rmSync(lock,{recursive:true,force:true});}
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===realpathSync(process.argv[1])){
  try{const args=process.argv.slice(2);if(args.length&&!(args.length===2&&args[0]==='--prefix'))throw Error('usage: representation-release.mjs [--prefix DIR]');
    const r=await installRepresentation(args.length?{prefix:args[1]}:{});console.log('state ACTIVATED\nrelease '+r.digest+'\nprevious '+(r.previous??'none')+'\nlauncher '+r.launcher+'\nfiles '+r.files);
  }catch(error){console.error(error.message);process.exitCode=1;}
}
