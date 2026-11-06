#!/usr/bin/env node
// Read-only salvage census for retained Fabric worktrees.
// Never checkout, add, reset, clean, merge, remove or replay an agent task.
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,readFileSync,realpathSync} from 'node:fs';
import {basename,join,resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';

const CAP_FILES=200;
const MAX_PATH_BYTES=768*1024;
const MAX_PATCH_BYTES=2*1024*1024;
function git(root,args,{allowExit1=false,input,maxBuffer=MAX_PATCH_BYTES}={}){
 const argv=['-c','core.fsmonitor=false','-C',root,...args];
 const result=spawnSync('git',argv,{env:{...process.env,GIT_OPTIONAL_LOCKS:'0'},
   timeout:20000,maxBuffer,input,encoding:'buffer'});
 if(result.error||result.signal||!Number.isInteger(result.status)||
    (result.status!==0&&!(allowExit1&&result.status===1))){
   const reason=result.error?.message??result.stderr?.toString('utf8').slice(0,220)??'';
   throw Error('read-only Git inspection failed: '+args[0]+' '+reason);
 }
 return {status:result.status,stdout:result.stdout??Buffer.alloc(0)};
}
const safeRelative=path=>typeof path==='string'&&path.length>0&&
 !path.startsWith('/')&&!path.includes('\\')&&!path.includes('\0')&&
 !path.split('/').some(part=>!part||part==='.'||part==='..');
const linesFromNull=(buffer)=>{
 if(buffer.length>MAX_PATH_BYTES)throw Error('Git file list exceeds audit bounds');
 const values=buffer.toString('utf8').split('\0').filter(Boolean);
 if(values.some(v=>!safeRelative(v)))throw Error('Unsafe Git path in worktree');
 return values;
};
function paths(root){
 const tracked=linesFromNull(git(root,['diff','--name-only','-z','--no-ext-diff','HEAD','--'],{maxBuffer:MAX_PATH_BYTES}).stdout);
 const untracked=linesFromNull(git(root,['ls-files','--others','--exclude-standard','-z'],{maxBuffer:MAX_PATH_BYTES}).stdout);
 const deleted=linesFromNull(git(root,['ls-files','--deleted','-z'],{maxBuffer:MAX_PATH_BYTES}).stdout);
 return {tracked,untracked,deleted,all:new Set([...tracked,...untracked])};
}
function fingerprint(root,path){
 const full=join(root,path);
 if(!existsSync(full))return {state:'absent'};
 const st=lstatSync(full);
 if(st.isSymbolicLink())return {state:'symlink_refused'};
 if(!st.isFile())return {state:'non_file'};
 if(st.size>1024*1024)return {state:'oversize',bytes:st.size};
 const digest=createHash('sha256').update(readFileSync(full)).digest('hex');
 return {state:'file',bytes:st.size,sha256:digest};
}
function repoRoot(path){
 const physical=realpathSync(resolve(path));
 const resolved=git(physical,['rev-parse','--show-toplevel']).stdout.toString().trim();
 if(realpathSync(resolved)!==physical)throw Error('Refusing a nested or aliased Git repository');
 return physical;
}
export function auditRecovery({candidate,target,includeCleanSource=false}={}){
 if(typeof candidate!=='string'||typeof target!=='string'||!candidate||!target)
   throw Error('Specify separate --candidate and --target Git worktree roots.');
 const from=repoRoot(candidate),into=repoRoot(target);
 if(from===into)throw Error('Cannot compare a worktree with itself');
 const candidateHead=git(from,['rev-parse','--verify','HEAD']).stdout.toString().trim();
 const targetHead=git(into,['rev-parse','--verify','HEAD']).stdout.toString().trim();
 const source=paths(from),destination=paths(into);
 const candidates=[...source.all].sort();
 if(candidates.length>CAP_FILES)throw Error('Too many changed worktree files for a bounded recovery audit');
 const files=candidates.map(path=>{
  const proposed=fingerprint(from,path),existing=fingerprint(into,path);
  const targetDirty=destination.all.has(path);
  const identical=proposed.state==='file'&&existing.state==='file'&&
    proposed.sha256===existing.sha256;
  const status=source.deleted.includes(path)?'deleted_requires_review':
    proposed.state!=='file'?'unsafe_or_missing':
    identical?'already_present':
    targetDirty?'dirty_target_conflict':
    existing.state==='absent'?'new_file_candidate':
    existing.state==='file'?'tracked_review_candidate':'unsafe_target';
  return {path,status,source:proposed,existing,targetDirty,
    sourceUntracked:source.untracked.includes(path)};
 });
 const divergent=git(into,['merge-base','--is-ancestor',candidateHead,targetHead],{allowExit1:true}).status!==0;
 let trackedPatch={state:'not_checked'};
 if(source.tracked.length){
  try{
   const payload=git(from,['diff','--binary','--no-ext-diff','--no-textconv','HEAD','--'],
     {maxBuffer:MAX_PATCH_BYTES}).stdout;
   if(payload.length>MAX_PATCH_BYTES)throw Error('Tracked patch exceeds audit bound');
   const check=git(into,['apply','--check','--'],{input:payload,allowExit1:true,maxBuffer:MAX_PATH_BYTES});
   trackedPatch={state:check.status===0?'check_passed':'check_failed',
     bytes:payload.length,sha256:createHash('sha256').update(payload).digest('hex')};
  }catch(error){
   trackedPatch={state:'not_verified',reason:String(error.message).slice(0,220)};
  }
 }
 const counts={};
 for(const f of files)counts[f.status]=(counts[f.status]||0)+1;
 const cleanSource=[];
 if(includeCleanSource){
  // Explicit, bounded C source recovery, not an arbitrary file-reading endpoint.
  for(const file of files){
   if(file.status!=='new_file_candidate'||
      !/^cmd\/BonfyreWebAddress\/(?:src|tests)\/stitch_[a-z0-9_]+\.[ch]$/.test(file.path)||
      file.source.bytes>65536)continue;
   const body=readFileSync(join(from,file.path));
   const text=new TextDecoder('utf-8',{fatal:true}).decode(body);
   if(text.includes('\0'))throw Error('Binary candidate source refused');
   const digest=createHash('sha256').update(body).digest('hex');
   if(digest!==file.source.sha256)throw Error('Candidate changed during recovery read');
   cleanSource.push({path:file.path,sha256:digest,content:text});
  }
 }
 return Object.freeze({schema:'bonfyre.stitch.worktree-recovery.v1',
   candidate:from,target:into,candidateHead,targetHead,taskId:basename(from),
   baseIsTargetAncestor:!divergent,trackedPatch,changedFiles:files.length,counts,
   files,...(includeCleanSource?{cleanSource}:{}),readOnly:true,applied:false,deleted:false,agentRestarted:false,
   acceptance:'Candidate inventory and Git apply --check only; real visual parity and governed merge are separate'});
}
export function parseArgs(argv){
 const includeCleanSource=argv.includes('--include-clean-source');
 argv=argv.filter(arg=>arg!=='--include-clean-source');
 const v={};for(let i=0;i<argv.length;i+=2){
  if(!['--candidate','--target'].includes(argv[i])||!argv[i+1]||v[argv[i]])throw Error('usage: recovery-audit --candidate PATH --target PATH');
  v[argv[i]]=argv[i+1];
 }
 if(!v['--candidate']||!v['--target'])throw Error('usage: recovery-audit --candidate PATH --target PATH');
 return {candidate:v['--candidate'],target:v['--target'],includeCleanSource};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{const v=auditRecovery(parseArgs(process.argv.slice(2)));
  console.log(JSON.stringify(v));
 }catch(error){console.error('Stitch salvage audit refused: '+error.message);process.exitCode=1;}
}
