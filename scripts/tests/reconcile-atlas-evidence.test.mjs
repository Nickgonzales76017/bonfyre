import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,mkdirSync,copyFileSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {mergeEvidence,reconcile,nativeAtlas} from '../reconcile-atlas-evidence.mjs';
const root=resolve(import.meta.dir,'../..'),atlas=nativeAtlas(root),schema=atlas.fieldSchema(),parseFile=atlas.parseFile;
const source=id=>`architecture ${id}\n  canonical_name ${id}\n  family command\n  domain Test\n  semantic_question What is verified?\n  maturity declared\n  forbidden_inference fixture -> deployed\n`;
function fixture(fn){const dir=mkdtempSync(join(tmpdir(),'atlas-reconcile-unit-'));try{return fn(dir)}finally{rmSync(dir,{recursive:true,force:true})}}
function parsed(dir,docs){return new Map([...docs].map(([p,t])=>{const f=join(dir,p);mkdirSync(resolve(f,'..'),{recursive:true});writeFileSync(f,t);return[p,parseFile(f,schema)]}))}
function evidence(dir,t){const p=join(dir,'incoming.yaff');writeFileSync(p,t);return parseFile(p,schema)}
test('new lines, reordered blocks and later OCR evidence are retained',()=>fixture(dir=>{
 const text='# later edits\n'+source('b')+'\n'+source('a')+'  witness later OCR witness\n  known_gap changed codec evidence\n';
 const docs=new Map([['moved.yaff',text]]),incoming=evidence(dir,'architecture a\n  invariant exact nutrition binding\n');
 const result=mergeEvidence(docs,parsed(dir,docs),incoming,schema).documents;
 expect(result.get('moved.yaff')).toContain('later OCR witness');expect(result.get('moved.yaff')).toContain('changed codec evidence');
 expect(result.get('moved.yaff')).toContain('exact nutrition binding');expect(docs.get('moved.yaff')).toBe(text);
 expect(mergeEvidence(result,parsed(dir,result),incoming,schema).documents).toEqual(result);
}));
test('known-gap replacements never replace a later revised statement',()=>fixture(dir=>{
 const docs=new Map([['a.yaff',source('a')+'  known_gap later revision\n']]),ops=[{kind:'architecture',id:'a',from:'known_gap earlier',to:'known_gap replacement'}];
 expect(mergeEvidence(docs,parsed(dir,docs),[],schema,ops).documents).toEqual(docs);
 const old=new Map([['a.yaff',source('a')+'  known_gap earlier\n']]);
 expect(mergeEvidence(old,parsed(dir,old),[],schema,ops).documents.get('a.yaff')).toContain('known_gap replacement');
}));
test('duplicate identities and unknown destinations are rejected',()=>fixture(dir=>{
 const docs=new Map([['a.yaff',source('a')],['b.yaff',source('a')]]);
 expect(()=>mergeEvidence(docs,parsed(dir,docs),[],schema)).toThrow('Duplicate');
 const one=new Map([['a.yaff',source('a')]]),incoming=evidence(dir,'interaction a-to-b\n  source a\n  destination b\n  relation projects_as\n');
 expect(()=>mergeEvidence(one,parsed(dir,one),incoming,schema)).toThrow('destination');
}));
test('interaction identity and scalar meaning are not fuzzy-merged',()=>fixture(dir=>{
 const text=source('a')+source('b')+'interaction bridge\n  source a\n  destination b\n  relation projects_as\n';
 const docs=new Map([['a.yaff',text]]),incoming=evidence(dir,'interaction bridge\n  source b\n  destination a\n  relation projects_as\n');
 expect(()=>mergeEvidence(docs,parsed(dir,docs),incoming,schema)).toThrow('Conflicting source');expect(docs.get('a.yaff')).toBe(text);
}));
test('the complete apply and export command is repeatable in a disposable repository',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'atlas-reconcile-integration-'));try{
  mkdirSync(join(dir,'architecture'));for(const f of readdirSync(join(root,'architecture')))if(/\.(mjs|cue)$/.test(f))copyFileSync(join(root,'architecture',f),join(dir,'architecture',f));
  expect(spawnSync('git',['init','-q'],{cwd:dir}).status).toBe(0);
  const names=['cmd-web-address','cmd-source-frontier','program-fabric','fleming-site'];writeFileSync(join(dir,'architecture/test.yaff'),names.map(source).join('\n'));
  const input=join(dir,'incoming.yaff');writeFileSync(input,'architecture cmd-web-address\n  invariant one native consumer\n\ninteraction witness-bridge\n  source program-fabric\n  destination cmd-web-address\n  relation projects_as\n  preserves meanings\n  forbidden_inference test -> deployed\n');
  const one=await reconcile({repo:dir,evidence:input,apply:true,output:join(dir,'receipt')});expect(one.applied).toBe(true);expect(one.exported).toBe(true);
  const before=readFileSync(join(dir,'architecture/test.yaff'),'utf8'),two=await reconcile({repo:dir,evidence:input,apply:true});expect(two.state).toBe('already_reconciled');expect(two.changed_files).toEqual([]);expect(readFileSync(join(dir,'architecture/test.yaff'),'utf8')).toBe(before);
 }finally{rmSync(dir,{recursive:true,force:true})}
},30000);
