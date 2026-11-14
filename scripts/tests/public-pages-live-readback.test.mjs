import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {checkPublishedPages,PUBLIC_PAGES,JOURNAL_STATIC,publicationTargets} from '../public-pages-live-readback.mjs';

function fixture(){
 const siteDir=mkdtempSync(join(tmpdir(),'bonfyre-public-readback-'));
 const records=new Map();
 const articles=Array.from({length:46},(_,i)=>'test-article-'+String(i+1).padStart(2,'0'));
 const index=['subject_id\tslug\tcategory\tpublished'];
 for(const slug of articles)index.push('post:'+slug+'\t'+slug+'\tResearch\t2026-01-01');
 const write=(file,route,text)=>{
  const bytes=Buffer.from(text);
  mkdirSync(dirname(join(siteDir,file)),{recursive:true});
  writeFileSync(join(siteDir,file),bytes);
  records.set(route,bytes);
 };
 for(const [file,route] of [...PUBLIC_PAGES,...JOURNAL_STATIC])
   write(file,route,file==='blog/post-index.tsv'?index.join('\n')+'\n':'<!doctype html><h1>'+file+'</h1>');
 for(const slug of articles)
   write('blog/'+slug+'/index.html','blog/'+slug+'/','<!doctype html><h1>'+slug+'</h1>');
 const response=async body=>({ok:true,status:200,arrayBuffer:async()=>body});
 const cleanup=()=>rmSync(siteDir,{recursive:true,force:true});
 return {siteDir,records,response,cleanup,expected:PUBLIC_PAGES.length+JOURNAL_STATIC.length+articles.length};
}
test('all article editions, CSS, JS, feeds and eleven primary site pages have exact public witnesses',async()=>{
 const f=fixture();
 try{
  const calls=[];
  const fetchPage=async url=>{
   const route=new URL(url).pathname.replace('/bonfyre/','');
   calls.push(route);return f.response(f.records.get(route));
  };
  const result=await checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',fetchPage,attempts:1,delayMs:0,evidenceDir:join(f.siteDir,'evidence')});
  assert.equal(result.verified,f.expected);
  assert.equal(result.articleCount,46);
  assert.equal(new Set(calls).size,f.expected);
  const receipts=readFileSync(join(f.siteDir,'evidence','public-pages-readback.tsv'),'utf8');
  assert.match(receipts,/blog\/article-editions.css/);
  assert.match(receipts,/blog\/test-article-46\/index.html/);
 }finally{f.cleanup();}
});
test('stale article bytes are retried until the exact edition appears',async()=>{
 const f=fixture();
 try{
  let requests=0;
  const result=await checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',
   fetchPage:async url=>{
    const route=new URL(url).pathname.replace('/bonfyre/','');
    if(route==='blog/test-article-01/' && ++requests===1)return f.response(Buffer.from('old article'));
    return f.response(f.records.get(route));
   },attempts:2,delayMs:0});
  assert.equal(result.verified,f.expected);
  assert.equal(requests,2);
 }finally{f.cleanup();}
});
test('stale live stylesheet refuses green publication and retains its failure receipt',async()=>{
 const f=fixture();
 try{
  const dir=join(f.siteDir,'evidence');
  await assert.rejects(checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',
   fetchPage:async url=>{
    const route=new URL(url).pathname.replace('/bonfyre/','');
    return f.response(route==='blog/article-editions.css'?Buffer.from('outdated styles'):f.records.get(route));
   },attempts:2,delayMs:0,evidenceDir:dir}),/PUBLIC_PAGES_READBACK_FAIL.*blog\/article-editions.css/);
  assert.match(readFileSync(join(dir,'public-pages-readback.tsv'),'utf8'),/not_verified: outdated HTML/);
 }finally{f.cleanup();}
});
test('missing or duplicate post records cannot become public targets',()=>{
 const f=fixture();
 try{
  writeFileSync(join(f.siteDir,'blog/post-index.tsv'),'subject_id\tslug\npost:x\tx\npost:x\tx\n');
  assert.throws(()=>publicationTargets(f.siteDir),/duplicate Journal/);
  writeFileSync(join(f.siteDir,'blog/post-index.tsv'),'subject_id\tslug\npost:x\tx\n');
  assert.throws(()=>publicationTargets(f.siteDir),/Incomplete Journal/);
 }finally{f.cleanup();}
});
test('origin and bounded retry gate remain enforced',async()=>{
 await assert.rejects(checkPublishedPages({origin:'http://unsafe.test/'}),/HTTPS/);
 await assert.rejects(checkPublishedPages({origin:'https://example.test/',attempts:999}),/bounded/);
});
