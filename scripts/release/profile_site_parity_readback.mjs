/* Read-only, exact public profile parity against an independently published
 * Aurekai authored release. Never deploys or marks LIVE_VERIFIED.
 */
import {readFileSync,lstatSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const sha=body=>createHash('sha256').update(body).digest('hex');
const hex=s=>/^[a-f0-9]{64}$/.test(s||'');
const fail=s=>{throw Error('Profile site readback: '+s);};
const fileOK=s=>/^site\/[a-z0-9][a-z0-9._/-]*$/.test(s)&&!s.split('/').some(x=>!x||x==='.'||x==='..');
const routeOK=s=>/^\/(?:[a-z0-9-]+\/)*$/.test(s||'')&&s.length<=127;
function member(root,path,cap=16*1024*1024){
  if(!fileOK(path))fail('unsafe release path '+path);
  const base=resolve(root),parts=path.split('/');let current=base;
  for(let i=0;i<parts.length;i++){
    current=join(current,parts[i]);
    if(!current.startsWith(base+sep))fail('release traversal');
    const st=lstatSync(current);
    if(st.isSymbolicLink()||(i<parts.length-1?!st.isDirectory():!st.isFile()||st.size>cap))
      fail('untrusted release member '+path);
  }
  return readFileSync(current);
}
function metadata(root,file,columns){
  const path=join(resolve(root),file),st=lstatSync(path);
  if(!st.isFile()||st.isSymbolicLink()||st.size>512*1024)fail('invalid '+file);
  const rows=readFileSync(path,'utf8').trimEnd().split('\n').map(l=>l.replace(/\r$/,'').split('\t'));
  if(!rows.length||rows.some(r=>r.length!==columns))fail('malformed '+file);
  return rows;
}
export function publicationFiles(root,{namespace,publication,revision}){
  if(!/^person-[a-z0-9-]{2,80}-(?:work|life|live|shop)$/.test(namespace||'')||
     !hex(publication)||!hex(revision))fail('native publisher identity required');
  const meta=new Map(metadata(root,'RELEASE.tsv',2));
  if(meta.get('schema')!=='bonfyre.authored-site-release.v1'||
     meta.get('publication')!=='published'||meta.get('account_namespace')!==namespace||
     meta.get('release_id')!==publication||meta.get('cms_revision')!==revision)
    fail('publication identity differs from reviewed native owner');
  const files=metadata(root,'FILES.tsv',2);
  if(files.shift().join('\t')!=='path\tsha256'||files.length>12000||!files.length)
    fail('invalid file manifest');
  const paths=new Map();
  for(const [path,digest] of files){
    if(!fileOK(path)||!hex(digest)||paths.has(path))fail('unsafe or duplicate file identity');
    const actual=sha(member(root,path,path.endsWith('index.html')?1048576:16*1024*1024));
    if(actual!==digest)fail('published bytes changed '+path);
    paths.set(path,digest);
  }
  const routes=metadata(root,'SITE-PAGES.tsv',4);
  if(routes.shift().join('\t')!=='route\ttitle\tnative_revision\thtml_sha256'||
     !routes.length||routes.length>512||+meta.get('pages')!==routes.length)
    fail('invalid route manifest');
  const seen=new Set();
  for(const [route,title,rev,digest] of routes){
    const path='site/'+(route==='/'?'index.html':route.slice(1)+'index.html');
    if(!routeOK(route)||seen.has(route)||!title||!hex(rev)||!hex(digest)||
       paths.get(path)!==digest)fail('unverified published route '+route);
    seen.add(route);
  }
  if(!seen.has('/')||!paths.has('site/assets/site.css'))fail('incomplete authored publication');
  return [...paths].map(([path,digest])=>({path:path.slice(5),digest}));
}
function baseUrl(raw,kind){
  const url=new URL(raw),expected=kind==='pages'?'nickgonzales76017.github.io':'nick.aurekai.work';
  if(url.protocol!=='https:'||url.hostname!==expected||url.username||url.password||
      url.port||url.search||url.hash||!url.pathname.endsWith('/'))
    fail('only the explicit public '+kind+' site is admitted');
  return url;
}
async function observe(url,cap){
  const r=await fetch(url,{redirect:'manual',cache:'no-store',
    signal:AbortSignal.timeout(18000)});
  if(r.status!==200)return {http:r.status,sha256:null,bytes:0};
  if(Number(r.headers.get('content-length')||0)>cap)fail('oversized remote body');
  const reader=r.body.getReader(),chunks=[];let n=0;
  try{for(;;){const {done,value}=await reader.read();if(done)break;n+=value.length;
    if(n>cap)fail('oversized remote response');chunks.push(value);}}
  finally{reader.releaseLock();}
  return {http:200,sha256:sha(Buffer.concat(chunks,n)),bytes:n};
}
export async function compareProfileSite({root,namespace,publication,revision,pages,profile}){
  const files=publicationFiles(root,{namespace,publication,revision});
  const source=baseUrl(pages,'pages'),destination=baseUrl(profile,'profile');
  const mismatches=[];let matched=0,next=0;
  async function worker(){while(next<files.length){
    const {path,digest}=files[next++],cap=path.endsWith('index.html')?1048576:16*1024*1024;
    try{
      const [pagesCopy,profileCopy]=await Promise.all([
        observe(new URL(path,source),cap),observe(new URL(path,destination),cap)]);
      if(pagesCopy.http===200&&profileCopy.http===200&&
         pagesCopy.sha256===digest&&profileCopy.sha256===digest)matched++;
      else mismatches.push({path,expected:digest,pages:pagesCopy,profile:profileCopy});
    }catch(e){mismatches.push({path,error:String(e.message||e)});}
  }}
  await Promise.all(Array.from({length:4},worker));
  return {schema:'bonfyre.profile-site-parity.readonly.v1',namespace,publication,revision,
    pages:source.href,profile:destination.href,total:files.length,matched,mismatches,
    byte_parity:mismatches.length===0,live_verified:false,
    remaining_gates:['native activation receipt','account authoring',
      'published post reconciliation','narration interaction','independent browser test']};
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===resolve(process.argv[1])){
  const args=process.argv.slice(2),keys=['root','namespace','publication','revision','pages','profile'];
  const value=k=>{const i=args.indexOf('--'+k);return i<0?undefined:args[i+1];};
  if(keys.some(k=>!value(k)))fail('required: --root --namespace --publication --revision --pages --profile');
  try{
    const result=await compareProfileSite(Object.fromEntries(keys.map(k=>[k,value(k)])));
    console.log(JSON.stringify(result));if(!result.byte_parity)process.exitCode=1;
  }catch(e){console.error(e.message);process.exitCode=1;}
}
