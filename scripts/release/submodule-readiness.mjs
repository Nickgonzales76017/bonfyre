#!/usr/bin/env bun
// Read-only reconciliation of declared submodules, committed gitlinks and the
// working index. Never fetch, init, reset, clean, stage or update anything.
import {realpathSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {resolve} from 'node:path';

const git=(root,...args)=>{
 const result=spawnSync('git',['-C',root,...args],{encoding:'utf8',maxBuffer:1024*1024,timeout:15000});
 if(result.status!==0)throw Error('git inspection failed: '+args.slice(0,2).join(' '));
 return result.stdout;
};
const safe=path=>typeof path==='string'&&path.length>0&&path.length<=240&&
 /^[A-Za-z0-9._/-]+$/.test(path)&&!path.startsWith('/')&&!path.split('/').some(p=>!p||p==='.'||p==='..');
const parseGitlinks=(text,commit)=>{
 const map=new Map();
 for(const entry of text.split('\0').filter(Boolean)){
  const m=entry.match(/^160000 (?:commit )?([a-f0-9]{40})(?: 0)?\t(.+)$/);
  if(!m)continue;
  if(!safe(m[2])||map.has(m[2]))throw Error('unsafe or repeated gitlink');
  map.set(m[2],m[1]);
 }
 return map;
};
export function auditSubmodules(root){
 root=realpathSync(resolve(root));
 const base=git(root,'rev-parse','--show-toplevel').trim();
 if(base!==root)throw Error('audit requires the exact repository root');
 const committed=parseGitlinks(git(root,'ls-tree','-rz','HEAD'),true);
 const index=parseGitlinks(git(root,'ls-files','-sz'),false);
 const config=git(root,'config','--null','-f','.gitmodules','--get-regexp','^submodule\\..*\\.path$');
 const declared=new Map();
 for(const item of config.split('\0').filter(Boolean)){
  const split=item.indexOf('\n'),name=item.slice(0,split),path=item.slice(split+1);
  if(split<0||!name.endsWith('.path')||!safe(path)||declared.has(path))throw Error('unsafe or repeated submodule declaration');
  declared.set(path,name);
 }
 const held=[];
 if(git(root,'status','--porcelain','--','.gitmodules').trim())held.push('submodule declaration changed in working tree or index');
 for(const path of declared.keys())if(!committed.has(path))held.push('declaration has no committed gitlink: '+path);
 for(const path of committed.keys()){
  if(!declared.has(path))held.push('gitlink has no declaration: '+path);
  if(index.get(path)!==committed.get(path))held.push('gitlink differs from the audited HEAD: '+path);
 }
 let status='';try{status=git(root,'submodule','status','--recursive');}
 catch{held.push('submodule worktree status could not be read');}
 for(const line of status.split('\n').filter(Boolean)){
  const prefix=line[0],match=line.slice(1).match(/^[a-f0-9]{40}\s+(\S+)/);
  if(!match)throw Error('unexpected submodule status output');
  const path=match[1];
  if(prefix!==' ')held.push('submodule not at a ready checkout: '+path+' ('+prefix+')');
 }
 return {state:held.length?'HELD':'REVIEWABLE',head:git(root,'rev-parse','HEAD').trim(),
   committed:[...committed].map(([path,sha])=>({path,sha})),
   declarations:[...declared.keys()],held};
}
export function auditText(result){
 return ['submodule-audit '+result.state,'  head '+result.head,
  '  gitlinks '+result.committed.length,'  declarations '+result.declarations.length,
  ...result.committed.map(r=>'  gitlink '+r.path+' '+r.sha),
  ...result.held.map(reason=>'  hold '+reason),
  '  side_effects none','  release_authorized false'].join('\n')+'\n';
}
if(import.meta.main){
 try{
  if(process.argv.length!==3)throw Error('usage: bun submodule-readiness.mjs REPO_ROOT');
  const result=auditSubmodules(process.argv[2]);process.stdout.write(auditText(result));
  if(result.state!=='REVIEWABLE')process.exitCode=3;
 }catch(error){console.error('submodule-audit: '+error.message);process.exitCode=2;}
}
