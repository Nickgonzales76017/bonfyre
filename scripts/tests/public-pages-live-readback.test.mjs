import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {checkPublishedPages,PUBLIC_PAGES} from '../public-pages-live-readback.mjs';

function fixture(){
 const siteDir=mkdtempSync(join(tmpdir(),'bonfyre-public-readback-'));
 const records=new Map();
 for(const [file,route] of PUBLIC_PAGES){
  const bytes=Buffer.from('<!doctype html><h1>'+file+'</h1>');
  mkdirSync(dirname(join(siteDir,file)),{recursive:true});
  writeFileSync(join(siteDir,file),bytes);
  records.set(route,bytes);
 }
 const response=async body=>({ok:true,status:200,arrayBuffer:async()=>body});
 const cleanup=()=>rmSync(siteDir,{recursive:true,force:true});
 return {siteDir,records,response,cleanup};
}
test('exact live bytes across seven pages produce receipts without publication',async()=>{
 const f=fixture();
 try{
  const calls=[];
  const fetchPage=async url=>{
   const route=new URL(url).pathname.replace('/bonfyre/','');
   calls.push(route);
   return f.response(f.records.get(route));
  };
  const result=await checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',fetchPage,attempts:1,delayMs:0,evidenceDir:join(f.siteDir,'evidence')});
  assert.equal(result.verified,PUBLIC_PAGES.length);
  assert.equal(new Set(calls).size,PUBLIC_PAGES.length);
  assert.match(readFileSync(join(f.siteDir,'evidence','public-pages-readback.tsv'),'utf8'),/verified/);
 }finally{f.cleanup();}
});
test('old cached HTML is rejected until the actual new bytes appear',async()=>{
 const f=fixture();
 try{
  let attempts=0;
  const result=await checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',
    fetchPage:async url=>{
      const route=new URL(url).pathname.replace('/bonfyre/','');
      if(route==='discover/' && ++attempts===1)return f.response(Buffer.from('old landing page'));
      return f.response(f.records.get(route));
    },attempts:2,delayMs:0});
  assert.equal(result.verified,PUBLIC_PAGES.length);
  assert.equal(attempts,2);
 }finally{f.cleanup();}
});
test('permanent mismatch fails and leaves a failure receipt, never approval',async()=>{
 const f=fixture();
 try{
  const dir=join(f.siteDir,'evidence');
  await assert.rejects(checkPublishedPages({siteDir:f.siteDir,origin:'https://example.test/bonfyre/',
    fetchPage:async url=>{
      const route=new URL(url).pathname.replace('/bonfyre/','');
      return f.response(route==='discover/'?Buffer.from('stale'):f.records.get(route));
    },attempts:2,delayMs:0,evidenceDir:dir}),/PUBLIC_PAGES_READBACK_FAIL.*discover\/index.html/);
  assert.match(readFileSync(join(dir,'public-pages-readback.tsv'),'utf8'),/not_verified: outdated HTML/);
 }finally{f.cleanup();}
});
test('bad publication origin and unbounded retry requests are rejected',async()=>{
 await assert.rejects(checkPublishedPages({origin:'http://unsafe.test/'}),/HTTPS/);
 await assert.rejects(checkPublishedPages({origin:'https://example.test/',attempts:999}),/bounded/);
});
