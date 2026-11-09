// Decide whether a Pages run still represents the latest authored source.
// A bot's verified release-output commit is not new authored source. Other
// source commits still supersede an old run. Fail closed on rewritten history.
import {spawnSync} from 'node:child_process';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

function git(cwd,args,optional=false) {
  const p=spawnSync('git',args,{cwd,encoding:'utf8',timeout:15000});
  if (p.error) throw p.error;
  if (!optional && p.status!==0) throw Error('git '+args[0]+' failed: '+(p.stderr||p.stdout));
  return p;
}
const generated = path =>
  path === 'site/index.html' ||
  path.startsWith('site/spaces/') ||
  path.startsWith('site/instruments/') ||
  path.startsWith('vendor/spaces-helpers-20261015/');
export function pagesSourceFreshness({cwd='.',source,upstream}){
  if(!/^[a-f0-9]{40}$/.test(source??'') || !/^[a-f0-9]{40}$/.test(upstream??'')) throw Error('Exact source and upstream Git SHA-1 references required');
  if(source===upstream) return {latest:true,generatedCommits:0,reason:'exact source'};
  if(git(cwd,['merge-base','--is-ancestor',source,upstream],true).status!==0) throw Error('Current main no longer contains the checkout source');
  const commits=git(cwd,['rev-list','--reverse',source+'..'+upstream]).stdout.trim().split('\n').filter(Boolean);
  for(const sha of commits){
    const [author,subject]=git(cwd,['show','-s','--format=%an%x09%s',sha]).stdout.trim().split('\t');
    const files=git(cwd,['diff-tree','--no-commit-id','--name-only','-r',sha]).stdout.split('\n').filter(Boolean);
    if(author!=='github-actions[bot]' ||
       subject!=='release(spaces): retain verified studio and instrument projections' ||
       files.length===0 || !files.every(generated)){
      return {latest:false,generatedCommits:0,reason:'new authored or unrecognized changes'};
    }
  }
  return {latest:true,generatedCommits:commits.length,reason:'only verified generated-release commits after source'};
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  try{
    const source=process.argv[2],upstream=process.argv[3];
    const r=pagesSourceFreshness({source,upstream});
    process.stdout.write('latest='+(r.latest?'true':'false')+'\n');
    process.stderr.write('PAGES_SOURCE_FRESHNESS '+r.reason+'; retained commits='+r.generatedCommits+'\n');
  } catch(error){console.error(error?.message??error);process.exitCode=1;}
}
