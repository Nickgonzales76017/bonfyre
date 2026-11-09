// Release reuse is permitted only for exact editorial source edits since an
// existing bot-authored, hash-verified studio/instrument release.
import {spawnSync} from 'node:child_process';
import {readFileSync,lstatSync,realpathSync} from 'node:fs';
import {join,resolve,sep} from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';

const RELEASE_TITLE='release(spaces): retain verified studio and instrument projections';
const EDITORIAL_SOURCES=new Set([
  'scripts/render-editorial.sh','scripts/editorial_publication.py',
  '.github/workflows/journal.yml'
]);
export function articleOnlyChanges(paths){
  return Array.isArray(paths) && paths.length>0 && paths.every(p=>
    typeof p==='string' && !p.includes('..') && (
      p.startsWith('editorial/') ||
      (p.startsWith('site/blog/') && p!=='site/blog/index.html') ||
      p.startsWith('site/series/') ||
      EDITORIAL_SOURCES.has(p) ||
      /^scripts\/tests\/(?:editorial-|journal-|public-editorial-language|editorial-publication-bridge)[a-z0-9-]*\.py$/.test(p)
    ));
}
function git(args,cwd){
  const p=spawnSync('git',args,{cwd,encoding:'utf8',timeout:30000,maxBuffer:2*1024*1024});
  if(p.error || p.status!==0)throw Error('git '+args[0]+' failed: '+(p.stderr||p.error||p.status));
  return p.stdout.trim();
}
function verifyManifest(root,manifest,relativeDir,fields){
  const rows=readFileSync(join(root,manifest),'utf8').trimEnd().split('\n');
  if(rows.length<2||rows[0]!==fields.join('\t'))throw Error('Invalid retained release receipt: '+manifest);
  const at=fields.includes('path')?fields.indexOf('path'):fields.indexOf('projection');
  const base=resolve(root,relativeDir);
  let checked=0;
  for(const row of rows.slice(1)){
    const cells=row.split('\t');
    if(cells.length!==fields.length)throw Error('Invalid receipt columns');
    const p=cells[at],size=Number(cells[fields.indexOf('bytes')]);
    const digest=cells[fields.indexOf('sha256')];
    if(!p||p.startsWith('/')||p.split('/').some(x=>x==='..'||x==='.')||
       !/^[a-f0-9]{64}$/.test(digest)||!Number.isSafeInteger(size)||size<0)
       throw Error('Unsafe retained artifact: '+p);
    const filename=resolve(base,p);
    if(!filename.startsWith(base+sep)||!lstatSync(filename).isFile()||
       !realpathSync(filename).startsWith(base+sep))throw Error('Invalid retained artifact: '+p);
    const body=readFileSync(filename);
    if(body.length!==size||createHash('sha256').update(body).digest('hex')!==digest)
      throw Error('Retained artifact has changed: '+p);
    checked++;
  }
  return checked;
}
export function verifyRetainedAssets(root){
  for(const p of ['site/index.html','site/spaces/catalogue/index.html']){
    if(!lstatSync(join(root,p)).isFile())throw Error('Missing retained publication: '+p);
  }
  const spaces=verifyManifest(root,'site/spaces/RELEASE.tsv','site/spaces',['path','sha256','bytes']);
  const instruments=verifyManifest(root,'site/instruments/SOURCE.tsv','site/instruments',['source','projection','sha256','bytes']);
  if(spaces<5||instruments<5)throw Error('Incomplete studio/instrument receipts');
  return {spaces,instruments};
}
export function classify({event='push',changed=[],releaseFound=false}={}){
  return event==='push'&&releaseFound&&articleOnlyChanges(changed);
}
export function currentReleaseScope({cwd=process.cwd(),event=process.env.GITHUB_EVENT_NAME}={}){
  if(event!=='push')return {editorial_only:false,reason:'manual release: full build'};
  const lines=git(['log','-n','1000','--format=%H%x09%an%x09%s'],cwd).split('\n');
  const retained=lines.map(x=>x.split('\t')).find(x=>x[1]==='github-actions[bot]'&&x[2]===RELEASE_TITLE);
  if(!retained)return {editorial_only:false,reason:'no retained studio release'};
  const diff=git(['diff','--name-only',retained[0]+'..HEAD'],cwd);
  const changed=diff?diff.split('\n'):[];
  if(!classify({event,changed,releaseFound:true}))
    return {editorial_only:false,reason:'non-editorial or empty change set',changed:changed.length};
  try{
    return {editorial_only:true,reason:'source-only change and verified artifact hashes',
      changed:changed.length,receipts:verifyRetainedAssets(cwd)};
  }catch(err){
    return {editorial_only:false,reason:'retained artifact verification refused: '+err.message};
  }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  try{
    const scope=currentReleaseScope();
    process.stdout.write('editorial_only='+(scope.editorial_only?'true':'false')+'\n');
    process.stderr.write('PAGES_EDITORIAL_SCOPE '+JSON.stringify(scope)+'\n');
  }catch(error){console.error('PAGES_EDITORIAL_SCOPE_REFUSED '+error.message);process.exitCode=1}
}
