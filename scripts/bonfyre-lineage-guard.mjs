#!/usr/bin/env bun
// Source-lineage guard: read only; refuses unreviewed merges or publication.
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {lstatSync,realpathSync,readFileSync,writeFileSync,existsSync} from 'node:fs';
import {resolve,relative,isAbsolute} from 'node:path';
const argv=process.argv.slice(2);
const option=(k)=>{const i=argv.indexOf(k);return i<0?null:argv[i+1]};
const refuse=m=>{console.error('REFUSED: '+m);process.exit(2)};
if(argv.includes('--help')){console.log('bun scripts/bonfyre-lineage-guard.mjs --primary /abs/Bonfyre --secondary /abs/Bonfyre-converge [--assert-root /abs/Bonfyre] [--output /abs/report.json]');process.exit(0)}
function inspect(dir){
 if(!dir||!isAbsolute(dir))refuse('absolute repository path required');
 const root=realpathSync(dir);
 const git=(args)=>{const r=spawnSync('git',args,{cwd:root,encoding:'buffer',maxBuffer:32*1024*1024,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_TERMINAL_PROMPT:'0'}});if(r.error||r.status!==0)refuse('git '+args.join(' ')+' failed');return r.stdout};
 if(git(['rev-parse','--show-toplevel']).toString().trim()!==root)refuse('not the worktree root');
 const head=git(['rev-parse','HEAD']).toString().trim();
 const branch=git(['branch','--show-current']).toString().trim();
 const dirty=git(['status','--porcelain=v1','-z','--untracked-files=all']).length>0;
 const files=git(['ls-files','-z']).toString().split('\0').filter(Boolean);
 const map=new Map();
 for(const name of files){
  if(name.split('/').includes('..'))refuse('unsafe tracked path');
  const p=resolve(root,name);if(relative(root,p).startsWith('..'))refuse('escaped worktree');
  if(!existsSync(p)){map.set(name,{state:'deleted'});continue}
  const st=lstatSync(p);
  if(!st.isFile()||st.isSymbolicLink()){map.set(name,{state:'not-regular-file'});continue}
  map.set(name,{state:'file',sha256:createHash('sha256').update(readFileSync(p)).digest('hex'),bytes:st.size});
 }
 return {root,head,branch,dirty,tracked:files.length,map};
}
const primary=inspect(option('--primary')),secondary=inspect(option('--secondary'));
if(primary.root===secondary.root)refuse('identical worktrees');
if(option('--assert-root')&&primary.root!==realpathSync(option('--assert-root')))refuse('MCP root drift');
const paths=[...new Set([...primary.map.keys(),...secondary.map.keys()])].sort();
const mismatches=paths.filter(path=>primary.map.get(path)?.sha256!==secondary.map.get(path)?.sha256||primary.map.get(path)?.state!==secondary.map.get(path)?.state).map(path=>({path,primary:primary.map.get(path)||{state:'absent'},secondary:secondary.map.get(path)||{state:'absent'},action:'manual-review-only'}));
const describe=({root,head,branch,dirty,tracked})=>({root,head,branch,dirty,tracked});
const report={schema:'bonfyre.lineage.guard.v1',primary:describe(primary),secondary:describe(secondary),mismatch_count:mismatches.length,mismatches,auto_merge:false,release_authorized:false};
const out=option('--output');
if(out){if(!isAbsolute(out)||existsSync(out))refuse('output must be absolute and not already exist');writeFileSync(out,JSON.stringify(report,null,2)+'\n',{flag:'wx',mode:0o600})}
console.log(JSON.stringify({primary:report.primary,secondary:report.secondary,mismatch_count:report.mismatch_count,output:out,release_authorized:false}));
if(mismatches.length)process.exitCode=1;
