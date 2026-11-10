import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {prepareLink,checkSource,feedSourceContinuation} from '../tools/source_material.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const text=readFileSync(join(here,'antithesis_cohort.tsv'),'utf8');
const work=readFileSync(join(here,'antithesis_reliability.yaff'),'utf8');
const rows=text.split(/\r?\n/).filter(x=>x&&!x.startsWith('#'));
const head=rows.shift().split('\t');
const data=rows.map(row=>Object.fromEntries(row.split('\t').map((value,index)=>[head[index],value])));
test('the source cohort is exactly 64 distinct canonical public GitHub URLs',()=>{
 assert.deepEqual(head,['repository','url','capability_family','review_priority','source_state','observation_receipt_sha256','rights_gate']);
 assert.equal(data.length,64);
 assert.equal(new Set(data.map(x=>x.repository)).size,64);
 assert.equal(new Set(data.map(x=>x.url)).size,64);
 for(const d of data){
  assert.match(d.repository,/^antithesishq\/[A-Za-z0-9_.-]+$/);
  assert.equal(d.url,'https://github.com/'+d.repository);
  assert.match(d.capability_family,/^[a-z-]+$/);
  assert.match(d.review_priority,/^P[012]$/);
  assert.match(d.rights_gate,/^(license-review-required|theme-commercial-review)$/);
 }
 const names=data.map(x=>x.repository.toLowerCase());
 assert.deepEqual(names,[...names].sort((a,b)=>a.localeCompare(b)));
});
test('observations are a subset of listings with exact 64-character receipt identities',()=>{
 const observed=data.filter(x=>x.source_state==='observed');
 assert.equal(observed.length,6);
 assert.equal(data.filter(x=>x.source_state==='listed').length,58);
 for(const d of data){
  if(d.source_state==='observed') assert.match(d.observation_receipt_sha256,/^[a-f0-9]{64}$/);
  else assert.equal(d.observation_receipt_sha256,'-');
 }
 for(const required of ['bombadil','sqlancer','snouty','antithesis-skills','examples','S3-Compatible-Workload'])
  assert.equal(data.find(x=>x.repository==='antithesishq/'+required)?.source_state,'observed');
});
test('work graph has source-backed typed continuations, not unsupported execution or licenses',()=>{
 assert.match(work,/^pack antithesis-reliability-lab\n/);
 const blocks=work.split(/\n(?=work )/).slice(1);
 assert.equal(blocks.length,7);
 const ids=new Set();
 for(const block of blocks){
  const id=/^work ([a-z0-9_]+)/.exec(block)?.[1];
  assert.ok(id);assert.ok(!ids.has(id));ids.add(id);
  assert.match(block,/\n  target_plane [A-Za-z0-9]+/);
  assert.match(block,/\n  priority \d+\.\d+/);
  const sources=[...block.matchAll(/^  source (https:\/\/github\.com\/antithesishq\/[^\s]+)$/gm)].map(m=>m[1]);
  assert.ok(sources.length>0);
  for(const url of sources)assert.ok(data.some(r=>r.url===url),'unlisted source '+url);
 }
 assert.match(work,/No tests have been run on the Antithesis platform/);
 assert.match(work,/never copy the separate commercially restricted Antithesis theme/);
});

test('all 64 GitHub entries prepare through the real Source Library owner with stable unique identities',async()=>{
 const identities=new Set();
 for(const record of data){
  const result=await prepareLink(record.url,{title:record.repository,collection:'Antithesis Reliability Lab',tags:['git','reliability','candidate'],notes:'Source listing only; requires rights review.'});
  assert.equal(result.expectedRevision,0);
  assert.equal(result.value.kind,'link');
  assert.equal(result.value.state,'active');
  assert.equal(result.value.url,record.url);
  assert.equal(result.value.title,record.repository);
  assert.equal(result.value.collection,'Antithesis Reliability Lab');
  assert.equal(result.key,'source-material:'+result.value.id);
  assert.equal(result.value.id,result.value.contentSHA256);
  assert.equal(result.value.observed,undefined);
  assert.equal(feedSourceContinuation(result.value),null);
  checkSource(result.value);
  assert.ok(!identities.has(result.key),'duplicate source material '+record.repository);
  identities.add(result.key);
  const retitled=await prepareLink(record.url,{title:'Independent review'});
  assert.equal(retitled.key,result.key,'title edits must not fork one source');
 }
 assert.equal(identities.size,64);
});
