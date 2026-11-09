import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync,chmodSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
import {fileURLToPath} from 'node:url';
import {validateBundle,createPlan,Held,delimited} from '../editorial-native-reconcile.mjs';
const script=resolve(dirname(fileURLToPath(import.meta.url)),'../editorial-native-reconcile.mjs');
const sha=s=>createHash('sha256').update(s).digest('hex');
const PAGE=['namespace','route','title','heading','lede','editorial_kind','author_credit','author_subject_ref','status'];
const FRAG=['namespace','coordinate','page','slot','audience','kind','position','body','projection_contract','source_ref','source_digest','status'];
const RECEIPT=['subject_id','canonical_url','author_id','series','category','source_ref','source_sha256','status'];
function csv(fields,rows,separator=','){
  const quote=x=>{const text=String(x??'');return text.includes('"')||text.includes(separator)||text.includes('\n')?'"'+text.replaceAll('"','""')+'"':text;};
  return [fields.join(separator),...rows.map(r=>fields.map(f=>quote(r[f])).join(separator))].join('\n')+'\n';
}
function fixture(t){
  const root=mkdtempSync(join(tmpdir(),'journal-native-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
  const bundle=join(root,'build','publication'),posts=join(root,'editorial','posts'),authors=join(root,'editorial','authors');
  mkdirSync(bundle,{recursive:true});mkdirSync(posts,{recursive:true});mkdirSync(authors,{recursive:true});
  const original='---\ntitle: Fixture\n---\nText, with a quoted "word".\n\n## Chapter\n',author='---\nslug: desk\n---\nA credited desk.\n';
  writeFileSync(join(posts,'fixture.md'),original);writeFileSync(join(authors,'desk.md'),author);
  const digest=sha(original),authorDigest=sha(author),page='/blog/fixture/',profile='/blog/authors/desk/';
  const pages=[{namespace:'aurekai-journal',route:page,title:'Fixture',heading:'Fixture',lede:'',editorial_kind:'article',author_credit:'Desk',author_subject_ref:'author:desk',status:'draft'},
    {namespace:'aurekai-journal',route:profile,title:'Desk',heading:'Desk',lede:'',editorial_kind:'author-profile',author_credit:'Desk',author_subject_ref:'author:desk',status:'draft'}];
  const fragment=(coordinate,p,kind,source_ref,source_digest,body,projection_contract='')=>({namespace:'aurekai-journal',coordinate,page:p,slot:'main',audience:'public',kind,position:'1',body,projection_contract,source_ref,source_digest,status:'draft'});
  const ref='editorial/posts/fixture.md';
  const fragments=[fragment('journal:post:fixture:body',page,'article-body',ref,digest,'Paragraph.\n\n## Chapter'),
    fragment('journal:post:fixture:attribution',page,'article-credit',ref,digest,'By Desk'),
    fragment('journal:post:fixture:presentation',page,'article-presentation',ref,digest,'format: dossier',JSON.stringify({review_required:true,subject_id:'post:fixture',authored_source_sha256:digest})),
    fragment('journal:author:desk:bio',profile,'author-biography','editorial/authors/desk.md',authorDigest,'Desk')];
  const receipts=[{subject_id:'post:fixture',canonical_url:'https://example.test/blog/fixture/',author_id:'desk',series:'',category:'Test',source_ref:ref,source_sha256:digest,status:'draft'},
    {subject_id:'author:desk',canonical_url:'https://example.test/blog/authors/desk/',author_id:'desk',series:'',category:'Authors',source_ref:'editorial/authors/desk.md',source_sha256:authorDigest,status:'draft'}];
  const save=()=>{writeFileSync(join(bundle,'cms-site-page.csv'),csv(PAGE,pages));writeFileSync(join(bundle,'cms-site-fragment.csv'),csv(FRAG,fragments));writeFileSync(join(bundle,'source-receipts.tsv'),csv(RECEIPT,receipts,'\t'));};
  const db=join(root,'cms.db'),sqlite=new DatabaseSync(db);
  sqlite.exec('CREATE TABLE site_page (id INTEGER PRIMARY KEY,'+PAGE.map(f=>'"'+f+'" TEXT').join(',')+',UNIQUE(namespace,route))');
  sqlite.exec('CREATE TABLE site_fragment (id INTEGER PRIMARY KEY,'+FRAG.map(f=>'"'+f+'" TEXT').join(',')+',UNIQUE(namespace,coordinate))');
  sqlite.close();
  const schemas=join(root,'schemas');mkdirSync(schemas);
  const cms=join(root,'fake-cms.mjs');
  writeFileSync(cms,[
    '#!/usr/bin/env node',
    "import {DatabaseSync} from 'node:sqlite';",
    "const name=process.argv[4],row=JSON.parse(process.argv[5]),ns=process.argv[process.argv.indexOf('--ns')+1],db=process.argv[process.argv.indexOf('--db')+1];",
    'row.namespace=ns;',
    "for(const [key,value]of Object.entries(row))if(value&&typeof value==='object')row[key]=JSON.stringify(value);",
    'const cols=Object.keys(row).map(x=>String.fromCharCode(34)+x+String.fromCharCode(34)).join(\",\"),marks=Object.keys(row).map(()=>\"?\").join(\",\");',
    "const conn=new DatabaseSync(db);conn.prepare('INSERT INTO '+name+' ('+cols+') VALUES ('+marks+')').run(...Object.values(row));conn.close();"
  ].join('\n')+'\n');
  chmodSync(cms,0o755);
  const args=['--root',root,'--bundle',bundle,'--db',db,'--cms',cms,'--schemas',schemas,'--minimum-free-mib','0'];
  const cli=(...extra)=>spawnSync('node',[script,...args,...extra],{encoding:'utf8',timeout:30000});
  save();return {root,bundle,db,pages,fragments,receipts,save,cli,groups:()=>validateBundle(root,bundle)};
}
test('quoted multiline CSV and source digests retain exact content',t=>{
  const f=fixture(t);assert.deepEqual(f.groups().map(x=>x[2].length),[2,4]);
  assert.deepEqual(delimited(csv(['a','b'],[{a:'a,b',b:'multi\n"quoted"'}])),[{a:'a,b',b:'multi\n"quoted"'}]);
  assert.match(f.cli().stdout,/MISSING=6/);
});
test('review-gated admission resumes missing records without overwrites',async t=>{
  const f=fixture(t);let {plan,digest}=await createPlan(f.db,f.groups());assert.equal(plan.filter(x=>x.action==='MISSING').length,6);
  const first=f.cli('--apply-drafts','--expected-plan',digest,'--batch-size','2');assert.equal(first.status,0,first.stderr);
  ({plan,digest}=await createPlan(f.db,f.groups()));assert.equal(plan.filter(x=>x.action==='IDENTICAL').length,2);
  const next=f.cli('--apply-drafts','--expected-plan',digest,'--batch-size','4');assert.equal(next.status,0,next.stderr);
  ({plan}=await createPlan(f.db,f.groups()));assert.equal(plan.filter(x=>x.action==='IDENTICAL').length,6);
  assert.equal(f.cli().status,0);
});
test('published record is protected and blocks all subsequent admission',async t=>{
  const f=fixture(t),db=new DatabaseSync(f.db),p=f.pages[0];
  db.prepare('INSERT INTO site_page (namespace,route,title,heading,lede,editorial_kind,author_credit,author_subject_ref,status) VALUES (?,?,?,?,?,?,?,?,?)').run(...PAGE.map(k=>k==='status'?'published':p[k]));db.close();
  const {plan,digest}=await createPlan(f.db,f.groups());assert.ok(plan.some(x=>x.action==='PROTECTED'));
  const result=f.cli('--apply-drafts','--expected-plan',digest);assert.equal(result.status,2);assert.match(result.stderr,/owner review/);
  const check=new DatabaseSync(f.db,{readOnly:true});assert.equal(check.prepare('SELECT count(*) AS n FROM site_fragment').get().n,0);check.close();
});
test('divergent existing draft is held rather than overwritten',async t=>{
  const f=fixture(t),db=new DatabaseSync(f.db);
  db.prepare('INSERT INTO site_page (namespace,route,title,heading,editorial_kind,status) VALUES (?,?,?,?,?,?)').run('aurekai-journal','/blog/fixture/','Old title','Fixture','article','draft');db.close();
  const {plan,digest}=await createPlan(f.db,f.groups());assert.ok(plan.some(x=>x.action==='DRIFT'));
  assert.equal(f.cli('--apply-drafts','--expected-plan',digest).status,2);
});
test('stale source, private fragment, or published bundle is refused',t=>{
  const f=fixture(t),item=f.fragments[0];item.source_digest='0'.repeat(64);f.save();assert.throws(f.groups,Held);
  item.source_digest=f.receipts[0].source_sha256;item.audience='self';f.save();assert.throws(f.groups,Held);
  item.audience='public';f.pages[0].status='published';f.save();assert.throws(f.groups,Held);
});
test('admission requires exact current plan digest',t=>{
  const f=fixture(t);assert.equal(f.cli('--apply-drafts').status,2);
  assert.equal(f.cli('--apply-drafts','--expected-plan','0'.repeat(64)).status,2);
});
