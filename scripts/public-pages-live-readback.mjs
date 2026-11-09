// Compare public HTML to the exact rendered site files from this release.
// A successful GitHub Pages action alone is not proof that the content is live.
// This is a read-only check; it does not republish, change source, or force-push.
import {createHash} from 'node:crypto';
import {readFileSync, mkdirSync, writeFileSync, appendFileSync} from 'node:fs';
import {resolve, join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {setTimeout as sleep} from 'node:timers/promises';

export const PUBLIC_PAGES = Object.freeze([
  ['index.html', ''],
  ['explore.html', 'explore.html'],
  ['about/index.html', 'about/'],
  ['market.html', 'market.html'],
  ['fieldwork/index.html', 'fieldwork/'],
  ['matrix/index.html', 'matrix/'],
  ['discover/index.html', 'discover/'],
  ['blog/index.html', 'blog/'],
  ['series/index.html', 'series/'],
  ['blog/connections/index.html', 'blog/connections/'],
  ['docs.html', 'docs.html']
]);
export const JOURNAL_STATIC = Object.freeze([
  ['blog/blog.css','blog/blog.css'],
  ['blog/article-editions.css','blog/article-editions.css'],
  ['blog/reader.css','blog/reader.css'],
  ['blog/reader.js','blog/reader.js'],
  ['blog/complexity.css','blog/complexity.css'],
  ['blog/complexity/index.html','blog/complexity/'],
  ['blog/feed.xml','blog/feed.xml'],
  ['blog/sitemap.xml','blog/sitemap.xml'],
  ['blog/post-index.tsv','blog/post-index.tsv'],
  ['blog/authors/index.html','blog/authors/'],
  ['blog/authors/aurekai-editorial/index.html','blog/authors/aurekai-editorial/'],
  ['discover/article-index.tsv','discover/article-index.tsv'],
  ['sitemap.xml','sitemap.xml']
]);
export function publicationTargets(siteDir){
  const index=readFileSync(resolve(siteDir,'blog/post-index.tsv'),'utf8');
  const rows=index.trimEnd().split('\n').map(row=>row.split('\t'));
  if(!rows.length||rows[0].slice(0,2).join('\t')!=='subject_id\tslug')
    throw Error('Invalid Journal publication index');
  const seen=new Set(),articles=[];
  for(const row of rows.slice(1)){
    const slug=row[1];
    if(row[0]!=='post:'+slug||!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(slug)||seen.has(slug))
      throw Error('Invalid or duplicate Journal source identity');
    seen.add(slug);
    articles.push(['blog/'+slug+'/index.html','blog/'+slug+'/']);
  }
  if(articles.length<46)throw Error('Incomplete Journal edition set: '+articles.length);
  const targets=[...PUBLIC_PAGES,...JOURNAL_STATIC,...articles];
  if(new Set(targets.map(([f])=>f)).size!==targets.length)
    throw Error('Duplicate publication target');
  return {targets,articleCount:articles.length};
}
const sha256 = data => createHash('sha256').update(data).digest('hex');
const quote = value => String(value).replace(/[\t\n\r]/g, ' ');
export async function checkPublishedPages({
  origin = 'https://nickgonzales76017.github.io/bonfyre/',
  siteDir = 'site',
  fetchPage = globalThis.fetch,
  attempts = 8,
  delayMs = 5000,
  evidenceDir = null
} = {}) {
  if (!origin.startsWith('https://') || !origin.endsWith('/')) throw Error('Expected exact HTTPS Pages origin ending in /');
  if (!Number.isInteger(attempts) || attempts < 1 || attempts > 12) throw Error('Invalid bounded retry count');
  const {targets,articleCount}=publicationTargets(siteDir);
  const expected = targets.map(([file, route]) => {
    const body = readFileSync(resolve(siteDir,file));
    return {file,route,digest:sha256(body),size:body.byteLength};
  });
  const remaining = new Map(expected.map(p => [p.file,p]));
  const receipts = new Map(), errors = new Map();
  for (let attempt=1;attempt<=attempts && remaining.size;attempt++) {
    const pending=[...remaining.values()];
    // Limit parallel cache-bypass reads; 66 articles should never fan out as
    // unbounded simultaneous CDN requests.
    for(let start=0;start<pending.length;start+=12)
    await Promise.all(pending.slice(start,start+12).map(async p => {
      const url = new URL(p.route,origin);
      // Avoid a CDN returning an earlier HTML response under the same URL.
      url.searchParams.set('check', p.digest.slice(0,16));
      try {
        const response = await fetchPage(url.toString(),{
          redirect:'follow',
          headers:{'cache-control':'no-cache','pragma':'no-cache'},
          signal:AbortSignal.timeout(16000)
        });
        if (!response.ok) {
          errors.set(p.file,'HTTP '+response.status);
          return;
        }
        const actual=sha256(Buffer.from(await response.arrayBuffer()));
        if (actual !== p.digest) {
          errors.set(p.file,'outdated HTML (live '+actual.slice(0,16)+', built '+p.digest.slice(0,16)+')');
          return;
        }
        receipts.set(p.file,{...p,url:url.origin+url.pathname,attempt});
        remaining.delete(p.file);
        errors.delete(p.file);
      } catch (error) {
        errors.set(p.file,quote(error?.message ?? error).slice(0,160));
      }
    }));
    if (remaining.size && attempt < attempts) await sleep(delayMs);
  }
  if (evidenceDir) {
    mkdirSync(evidenceDir,{recursive:true});
    const rows=['file\tbuilt_sha256\tbytes\tlive_url\tattempt\tstatus'];
    for (const p of expected) {
      const r=receipts.get(p.file);
      rows.push([p.file,p.digest,p.size,r?.url??'',r?.attempt??'',
        r?'verified':'not_verified: '+quote(errors.get(p.file)??'unknown')].join('\t'));
    }
    writeFileSync(join(evidenceDir,'public-pages-readback.tsv'),rows.join('\n')+'\n');
  }
  if (remaining.size) {
    throw Error('PUBLIC_PAGES_READBACK_FAIL: '+[...remaining.keys()].map(k=>k+': '+(errors.get(k)??'unverified')).join(' | '));
  }
  return {verified:receipts.size,articleCount,receipts:[...receipts.values()]};
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const result = await checkPublishedPages({origin:process.env.PAGES_ORIGIN || undefined,
      evidenceDir:process.env.PUBLIC_PAGES_EVIDENCE || 'spaces-live-evidence'});
    const line='PUBLIC_PAGES_READBACK_PASS '+result.verified+' exact public files / '+result.articleCount+' individual articles';
    console.log(line);
    if(process.env.GITHUB_STEP_SUMMARY) {
      appendFileSync(process.env.GITHUB_STEP_SUMMARY,
        '\n### Public editorial release verified\n'+
        result.receipts.map(r=>'- '+r.url+' — '+r.digest.slice(0,12)+' (attempt '+r.attempt+')').join('\n')+'\n');
    }
  } catch (error) {
    console.error(error?.stack??String(error));
    process.exitCode=1;
  }
}
