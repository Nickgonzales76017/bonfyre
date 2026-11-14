#!/usr/bin/env bun
// Source-pinned admission of genuinely missing files, through Project MCP only.
// Overlapping edits, deletions, publication and release remain separately governed.
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,readFileSync,readdirSync,realpathSync} from 'node:fs';
import {homedir} from 'node:os';
import {join,resolve,relative,isAbsolute,dirname,normalize} from 'node:path';
import {spawnSync} from 'node:child_process';
const args=process.argv.slice(2);
const opt=k=>{const i=args.indexOf(k);return i<0?null:args[i+1]};
const fail=m=>{throw new Error(m)};
const sha=b=>createHash('sha256').update(b).digest('hex');
const hex=/^[a-f0-9]{64}$/;
const gitObjectId=/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/;
const allowedPrefix=/^(cmd\/|components\/|integrations\/|scripts\/|lib\/|architecture\/)/;
const allowedSuffix=/\.(?:c|h|zig|mjs|js|ts|tsx|jsx|cue|yaff|json|jsonl|sql|css|html|md|sh|toml|yaml|yml|xml|svg)$/;
const forbidden=/(?:^|\/)(?:\.git|node_modules|build|dist|output|tmp|temp|secrets?|credentials?|tokens?|private|cache)(?:\/|$)|(?:^|\/)(?:\.env[^/]*|id_rsa|[^/]*\.(?:key|pem|p12|pfx|sqlite3?|db))$/i;
function safePath(path){
 if(typeof path!=='string'||!path||path.includes('\0')||path.includes('\\')||isAbsolute(path)||path.split('/').some(p=>!p||p==='.'||p==='..'))return false;
 return /^[a-zA-Z0-9_./+\-]+$/.test(path)&&allowedPrefix.test(path)&&allowedSuffix.test(path)&&!forbidden.test(path);
}
function within(root,path){
 const joined=resolve(root,path);
 if(relative(root,joined).startsWith('..'))fail('path escaped worktree');
 const parts=path.split('/'),step=[];
 for(const part of parts){step.push(part);const p=join(root,...step);if(existsSync(p)&&lstatSync(p).isSymbolicLink())fail('symlinked source component: '+path)}
 return joined;
}
function git(root,argv){const p=spawnSync('git',argv,{cwd:root,encoding:'utf8',env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_TERMINAL_PROMPT:'0'}});if(p.status!==0)fail('git '+argv[0]+' failed in '+root);return p.stdout.trim()}
function chooseReport(){
 const explicit=opt('--report');if(explicit)return explicit;
 const found=readdirSync(homedir()).filter(x=>x.startsWith('bonfyre-lineage-review')&&x.endsWith('.json'));
 if(found.length!==1)fail('specify --report /absolute/report.json ('+found.length+' matches)');
 return join(homedir(),found[0]);
}
function readReport(path){
 if(!isAbsolute(path)||lstatSync(path).isSymbolicLink())fail('report must be a local non-symlink absolute file');
 const report=JSON.parse(readFileSync(path,'utf8'));
 if(report.schema!=='bonfyre.source-convergence.v1'||!Array.isArray(report.differences)||report.difference_count!==report.differences.length)fail('invalid report schema/count');
 for(const side of ['primary','secondary']){
  if(!isAbsolute(report[side]?.root)||!gitObjectId.test(report[side]?.head||''))fail('invalid source identity');
  report[side].root=realpathSync(report[side].root);
  if(git(report[side].root,['rev-parse','--show-toplevel'])!==report[side].root)fail('unexpected checkout root');
  if(git(report[side].root,['rev-parse','HEAD'])!==report[side].head)fail(side+' HEAD changed since report: regenerate');
 }
 if(report.primary.root===report.secondary.root)fail('same checkout');
 if(opt('--assert-root')&&realpathSync(opt('--assert-root'))!==report.primary.root)fail('MCP root mismatch');
 return report;
}
function fullyReconciled(report){
 const base=report.primary.root,donor=report.secondary.root;
 if(git(base,['rev-parse','HEAD'])!==git(donor,['rev-parse','HEAD']))return false;
 if(git(base,['status','--porcelain','--untracked-files=all'])||
    git(donor,['status','--porcelain','--untracked-files=all']))return false;
 return report.difference_count===0;
}
function patchFile(path,text,executable){
 const lines=text.slice(0,-1).split('\n'),mode=executable?'100755':'100644';
 return 'diff --git a/'+path+' b/'+path+'\nnew file mode '+mode+'\n--- /dev/null\n+++ b/'+path+'\n@@ -0,0 +1,'+lines.length+' @@\n'+lines.map(x=>'+'+x).join('\n')+'\n';
}
function plan(report,maxFiles=8,maxLines=950){
 let candidateCount=0,already=0,overlap=0,kept=0,unsafe=0,skipped=0;
 const candidates=[];
 for(const item of report.differences){
  const {path,primary:a,secondary:b}=item;
  if(!safePath(path)){unsafe++;continue}
  if(a?.state==='absent'&&b?.state==='file'&&hex.test(b.sha256||'')){
   const source=within(report.secondary.root,path);
   const destination=within(report.primary.root,path);
   if(!existsSync(source)||!lstatSync(source).isFile())fail('secondary source disappeared: '+path);
   const bytes=readFileSync(source);
   if(sha(bytes)!==b.sha256)fail('secondary source changed: '+path);
   if(existsSync(destination)){
    if(lstatSync(destination).isFile()&&sha(readFileSync(destination))===b.sha256){already++;continue}
    fail('destination was created/changed since report: '+path);
   }
   candidateCount++;
   if(bytes.length>128*1024||bytes.includes(0)||bytes.length===0){skipped++;continue}
   let text;try{text=new TextDecoder('utf-8',{fatal:true}).decode(bytes)}catch{skipped++;continue}
   if(!text.endsWith('\n')||text.includes('\r')){skipped++;continue}
   const lineCount=text.split('\n').length-1;
   if(lineCount>maxLines){skipped++;continue}
   const priority=/(?:profile|companion|affinity|dating|interest|authoring|publication|journal|frappe|form_tree)/i.test(path)?0:1;
   candidates.push({path,text,sha256:b.sha256,mode:lstatSync(source).mode&0o111?100755:100644,lines:lineCount,priority});
  }else if(b?.state==='absent'&&a?.state==='file'){kept++}else{overlap++}
 }
 // Complete dependencies of previously admitted C sources before unrelated imports.
 const required=new Set();
 for(const entry of report.differences){
  if(entry.primary?.state!=='absent'||entry.secondary?.state!=='file'||!safePath(entry.path)||!/\.(?:c|h)$/.test(entry.path))continue;
  const path=within(report.primary.root,entry.path);
  if(!existsSync(path)||!lstatSync(path).isFile())continue;
  const bytes=readFileSync(path);
  if(sha(bytes)!==entry.secondary.sha256)fail('already-admitted source changed: '+entry.path);
  const body=bytes.toString('utf8');
  for(const m of body.matchAll(/^\s*#\s*include\s+"([^"]+)"/gm)){
   const dependency=normalize(join(dirname(entry.path),m[1])).replaceAll('\\','/');
   if(dependency!=='..'&&!dependency.startsWith('../')&&!existsSync(join(report.primary.root,dependency)))required.add(dependency);
  }
 }
 candidates.sort((a,b)=>Number(required.has(b.path))-Number(required.has(a.path))||a.priority-b.priority||a.path.localeCompare(b.path));
 const selected=[];let lines=0;const dependencyBlockers=[];
 for(const row of candidates){
  if(selected.length>=maxFiles)break;
  if(lines+row.lines>maxLines)continue;
  // Do not install disconnected C components: every quoted include must
  // resolve in the active checkout or an earlier file in this same patch.
  const missing=[];
  if(/\.(?:c|h)$/.test(row.path))for(const match of row.text.matchAll(/^\s*#\s*include\s+"([^"]+)"/gm)){
   const dependency=normalize(join(dirname(row.path),match[1])).replaceAll('\\','/');
   if(dependency==='..'||dependency.startsWith('../')||
      (!existsSync(join(report.primary.root,dependency))&&!selected.some(item=>item.path===dependency)))missing.push(dependency);
  }
  if(missing.length){dependencyBlockers.push({path:row.path,missing});continue}
  selected.push(row);lines+=row.lines;
 }
 const patch=selected.map(c=>patchFile(c.path,c.text,c.mode===100755)).join('');
 return {summary:{difference_count:report.difference_count,eligible_missing:candidateCount,already_admitted:already,selected:selected.length,remaining:candidates.length-selected.length,overlap_requires_review:overlap,primary_kept:kept,unsafe,unsupported:skipped,selected_lines:lines,required_dependencies:required.size,dependencies_missing_from_donor:[...required].filter(path=>!candidates.some(row=>row.path===path)).slice(0,24),dependency_blockers:dependencyBlockers.slice(0,24)},selected,patch};
}
async function main(){
 if(args.includes('--help')){console.log('bun scripts/bonfyre-lineage-mcp.mjs [--report /abs/report.json] [--assert-root /abs/Bonfyre] [--max-files 8] [--apply]');return}
 const report=readReport(chooseReport());
 const active=realpathSync(process.cwd());
 if(active!==report.primary.root)fail('run from the canonical Bonfyre checkout');
 if(!opt('--assert-root')||realpathSync(opt('--assert-root'))!==active)fail('explicit canonical --assert-root required');
 const count=opt('--max-files')?Number(opt('--max-files')):8;
 if(!Number.isInteger(count)||count<1||count>12)fail('--max-files must be 1..12');
 const result=plan(report,count),{selected,patch,summary}=result;
 console.log(JSON.stringify({phase:args.includes('--apply')?'mcp-preflight':'plan-only',...summary,paths:selected.map(x=>({path:x.path,sha256:x.sha256})),live_verified:false},null,2));
 if(args.includes('--strict-drift')&&!fullyReconciled(report))fail('unreconciled source lineages: release gate closed');
 if(!args.includes('--apply')||!selected.length)return;
 const tokenPath=join(homedir(),'.bonfyre/mcp/token');
 if(!existsSync(tokenPath)||lstatSync(tokenPath).isSymbolicLink())fail('trusted local MCP token file required');
 const token=readFileSync(tokenPath,'utf8').trim();
 if(!token)fail('empty MCP token');
 const port=Number(process.env.BONFYRE_PROJECT_MCP_PORT||3051);
 if(!Number.isInteger(port)||port<1||port>65535)fail('invalid MCP port');
 const {createOwnedMcpCall}=await import('../integrations/mcp/owned_mcp_client.mjs');
 const call=createOwnedMcpCall({endpoint:'http://127.0.0.1:'+port+'/mcp',token,maxBytes:2000000,tools:['project_status','project_patch_preview','project_apply_low_risk_patch','project_read','project_test_profile']});
 const status=await call('project_status');
 if(status.root!==report.primary.root||status.head!==report.primary.head)fail('live MCP root or HEAD differs from source-pinned report');
 const pre=await call('project_patch_preview',{patch});
 if(pre.valid!==true||pre.low_risk_eligible!==true||pre.policy_errors?.length||pre.metadata?.dirty_conflicts?.length)fail('MCP preview refused; no changes made');
 if(pre.metadata?.file_count!==selected.length||pre.metadata.files.some(x=>x.exists))fail('MCP preimage mismatch; no changes made');
 // One effect. On uncertain outcome, stop and reconcile the receipt; never retry.
 const applied=await call('project_apply_low_risk_patch',{patch});
 if(applied.applied!==true||!applied.receipt_id)fail('MCP patch outcome unverified; reconcile receipt before retry');
 for(const item of selected)if(applied.post_hashes?.[item.path]!==item.sha256)fail('postimage mismatch; inspect '+applied.receipt_id);
 const readback=await call('project_read',{files:selected.map(item=>({path:item.path,if_sha256:item.sha256}))});
 if(!Array.isArray(readback.files)||readback.files.length!==selected.length||readback.files.some(item=>!selected.some(source=>source.path===item.path&&source.sha256===item.sha256)))fail('MCP readback mismatch; inspect '+applied.receipt_id);
 const checks=await call('project_test_profile',{profile:'git-diff-check',timeout_seconds:120});
 if(checks.result?.exit_code!==0)fail('git diff check failed after receipt '+applied.receipt_id);
 console.log(JSON.stringify({mcp_applied:true,receipt_id:applied.receipt_id,files:selected.length,verified_post_hashes:true,git_diff_check:true,live_verified:false}));
}
if(import.meta.main)main().catch(e=>{console.error('STOP: '+e.message);process.exitCode=2});
export {safePath,patchFile,plan,readReport};
