#!/usr/bin/env bun
// Non-destructive source convergence; differences are evidence, never a merge grant.
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {realpathSync,lstatSync,existsSync,readFileSync,writeFileSync} from 'node:fs';
import {resolve,relative,isAbsolute} from 'node:path';
const argv=process.argv.slice(2);
const option=name=>{const i=argv.indexOf(name);return i<0?null:argv[i+1]};
function refuse(message){console.error('REFUSED: '+message);process.exit(2)}
if(argv.includes('--help')){console.log('bun scripts/bonfyre-lineage-reconcile.mjs --primary /absolute/Bonfyre --secondary /absolute/Bonfyre-converge --assert-root /absolute/Bonfyre [--output /absolute/report.json]');process.exit(0)}
function git(root,args){
 const r=spawnSync('git',args,{cwd:root,maxBuffer:32*1024*1024,env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_TERMINAL_PROMPT:'0'}});
 if(r.error||r.status!==0)refuse('git '+args.join(' ')+' failed in '+root);
 return r.stdout;
}
function inspect(path){
 if(!path||!isAbsolute(path))refuse('absolute checkout path required');
 const root=realpathSync(path);
 if(git(root,['rev-parse','--show-toplevel']).toString().trim()!==root)refuse('not a Git worktree root');
 const head=git(root,['rev-parse','HEAD']).toString().trim();
 const branch=git(root,['branch','--show-current']).toString().trim();
 const dirty=git(root,['status','--porcelain=v1','-z','--untracked-files=all']).length!==0;
 const tracked=git(root,['ls-files','-z']).toString().split('\0').filter(Boolean);
 const untracked=git(root,['ls-files','--others','--exclude-standard','-z']).toString().split('\0').filter(Boolean);
 const names=[...new Set([...tracked,...untracked])].sort();
 const inventory=new Map();
 for(const name of names){
  if(name.split('/').includes('..'))refuse('unsafe source path');
  const file=resolve(root,name);
  if(relative(root,file).startsWith('..'))refuse('source path escapes checkout');
  if(!existsSync(file)){inventory.set(name,{state:'deleted'});continue}
  const stat=lstatSync(file);
  if(!stat.isFile()||stat.isSymbolicLink()){inventory.set(name,{state:'unsupported'});continue}
  inventory.set(name,{state:'file',sha256:createHash('sha256').update(readFileSync(file)).digest('hex'),bytes:stat.size,tracked:!untracked.includes(name)});
 }
 return {root,head,branch,dirty,tracked_count:tracked.length,untracked_count:untracked.length,inventory};
}
const primary=inspect(option('--primary')),secondary=inspect(option('--secondary'));
if(primary.root===secondary.root)refuse('checkout roots must differ');
const asserted=option('--assert-root');
if(asserted&&primary.root!==realpathSync(asserted))refuse('configured MCP root does not match canonical worktree');
const names=[...new Set([...primary.inventory.keys(),...secondary.inventory.keys()])].sort();
const differences=[];
for(const path of names){
 const a=primary.inventory.get(path)||{state:'absent'},b=secondary.inventory.get(path)||{state:'absent'};
 if(a.state===b.state&&a.sha256===b.sha256)continue;
 differences.push({path,primary:a,secondary:b,action:'review-required'});
}
const describe=({inventory,...fields})=>fields;
const report={schema:'bonfyre.source-convergence.v1',primary:describe(primary),secondary:describe(secondary),identical_count:names.length-differences.length,difference_count:differences.length,differences,auto_copy:false,auto_merge:false,publication_authorized:false,live_verified:false};
const output=option('--output');
if(output){if(!isAbsolute(output)||existsSync(output))refuse('report must have a new absolute path');writeFileSync(output,JSON.stringify(report,null,2)+'\n',{flag:'wx',mode:0o600})}
console.log(JSON.stringify({primary:report.primary,secondary:report.secondary,identical_count:report.identical_count,difference_count:report.difference_count,output,live_verified:false}));
if(differences.length)process.exitCode=1;
