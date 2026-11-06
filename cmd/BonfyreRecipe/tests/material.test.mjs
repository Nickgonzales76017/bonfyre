import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, writeFileSync, copyFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
const cli=resolve(process.argv[2]),driver=resolve(process.argv[3]);
const tmp=mkdtempSync(join(tmpdir(),'bonfyre-recipe-material-'));
const example=JSON.parse(readFileSync(new URL('../examples/preparation-family.jsonl',import.meta.url),'utf8'));
let serial=0,passed=0;
const run=(binary,args)=>spawnSync(binary,args,{encoding:'utf8',timeout:15000,maxBuffer:2000000});
const ok=(binary,args)=>{const r=run(binary,args);assert.equal(r.status,0,r.stderr+' '+r.stdout);return r.stdout};
const reject=(binary,args)=>{const r=run(binary,args);assert.notEqual(r.status,0,'unexpected success: '+args.join(' '));};
const check=(name,fn)=>{fn();passed++;console.log('PASS '+name);};
const build=(data)=>{const base=join(tmp,String(++serial)),source=base+'.jsonl',pack=base+'.db';writeFileSync(source,typeof data==='string'?data:JSON.stringify(data)+'\n');ok(cli,['corpus','factor',source,pack]);return{source,pack};};
const edit=(pack,sql)=>{const d=new Database(pack);try{d.exec(sql)}finally{d.close()}};
const rows=text=>Object.fromEntries(text.trim().split('\n').slice(1).map(line=>{const a=line.split('\t');return[a[0]+':'+a[4],[Number(a[2]),Number(a[3])]]}));
try {
 const raw='{ "id":"one", "name":"Café, \\\"quoted\\\"", "same":{"steps":["mix","rest"]}, "n":1.2300 }\r\n'+
 '{ "id":"two", "name":"Second", "same":{"steps":["mix","rest"]}, "n":1.2300 }';
 const exact=build(raw);
 check('original numeric spelling, escapes, Unicode and line endings reconstruct exactly',()=>assert.equal(ok(cli,['corpus','export',exact.pack]),raw));
 check('all source bytes verify, including final record without newline',()=>assert.match(ok(cli,['corpus','verify',exact.pack,exact.source]),/identical_records\t2/));
 check('equal root fields are physically shared',()=>{const d=new Database(exact.pack,{readonly:true});try{assert(d.query('SELECT count(*) n FROM record_part').get().n>d.query('SELECT count(*) n FROM record_atom').get().n)}finally{d.close()}});
 check('existing output cannot be overwritten',()=>{const before=readFileSync(exact.pack);reject(cli,['corpus','factor',exact.source,exact.pack]);assert.deepEqual(readFileSync(exact.pack),before)});
 check('extra source record cannot verify as complete',()=>{const p=join(tmp,'extra.jsonl');writeFileSync(p,raw+'\n{"id":"three","name":"Three"}\n');reject(cli,['corpus','verify',exact.pack,p])});
 for(const [name,bad] of [['invalid JSON','not json\n'],['duplicate identity','{"id":"x","name":"X"}\n{"id":"x","name":"Y"}\n'],['NUL input','{"id":"x","name":"X"}\0\n'],['oversized record','{"id":"x","name":"'+'x'.repeat(1048576)+'"}\n']])
 check(name+' fails without publishing output',()=>{const p=join(tmp,'bad-'+(++serial)),out=p+'.db';writeFileSync(p,bad);reject(cli,['corpus','factor',p,out]);assert(!existsSync(out))});
 check('damaged fragment refuses export and source verification',()=>{const p=join(tmp,'damage.db');copyFileSync(exact.pack,p);edit(p,"UPDATE record_atom SET payload=zeroblob(length(payload)) WHERE id=(SELECT min(id) FROM record_atom)");reject(cli,['corpus','export',p]);reject(cli,['corpus','verify',p,exact.source])});
 check('altered display metadata is not trusted',()=>{const p=join(tmp,'metadata.db');copyFileSync(exact.pack,p);edit(p,"UPDATE record SET title='invented' WHERE seq=1");reject(driver,['card',p,'0'])});
 const family=build(example),key=example.recipe_id;
 const one=rows(ok(cli,['corpus','list',family.pack,key,'1']));
 check('one binder batch supplies ten explicit variations',()=>{assert.deepEqual(one['peanut-butter:cup'],[5,4]);assert.deepEqual(one['crushed-oats:cup'],[10,3]);assert.deepEqual(one['mexican-vanilla:drop'],[20,1]);assert.deepEqual(one['honey:drop'],[11,1]);assert.deepEqual(one['cold-brew-extract:drop'],[22,1]);assert.deepEqual(one['brown-sugar:tsp'],[10,1])});
 check('same ingredient in cup and drop stays dimensionally separate',()=>{assert.deepEqual(one['syrup:cup'],[3,4]);assert.deepEqual(one['syrup:drop'],[2,1])});
 check('two household sets share two base batches without rounding fractions',()=>{const two=rows(ok(cli,['corpus','list',family.pack,key,'2']));assert.deepEqual(two['peanut-butter:cup'],[5,2]);assert.deepEqual(two['crushed-oats:cup'],[20,3]);assert.deepEqual(two['mexican-vanilla:drop'],[40,1])});
 check('native page exposes the residual two tablespoons',()=>{const html=ok(driver,['view',family.pack,key,'1']);assert.match(html,/arithmetic remainder: 2 tbsp/);assert.match(html,/1 declared-capacity base batch/);assert.match(html,/Save the grocery \/ helper ingredient list/)});
 check('the existing feed fragment leads to the same native subject',()=>assert.match(ok(driver,['card',family.pack,'0']),/\/recipes\?id=prep%3Aoat-bite-variations/));
 check('bounded render refuses truncation instead of emitting partial instructions',()=>ok(driver,['tiny',family.pack,key]));
 check('quantities never create a provider quote',()=>assert(ok(cli,['corpus','list',family.pack,key,'1']).split('\n').filter(Boolean).slice(1).every(l=>l.endsWith('\tnot_quoted'))));
 for(const [name,mutate] of [['boolean ingredient',x=>x.preparation.per_variant[0].num=true],['boolean yield',x=>x.preparation.shared_batch.yield.num=true],['string denominator',x=>x.preparation.per_variant[0].den='3']])
 check(name+' cannot be coerced into a number',()=>{const x=structuredClone(example);mutate(x);const f=build(x);reject(cli,['corpus','list',f.pack,key,'1'])});
 for(const [name,mutate] of [['mixed yield units',x=>x.preparation.shared_batch.portion.unit='ml'],['fractional numerator',x=>x.preparation.per_variant[0].num=1.5],['zero denominator',x=>x.preparation.per_variant[0].den=0],['missing additions',x=>delete x.preparation.variants[0].additions],['duplicate variant identity',x=>x.preparation.variants[1].id=x.preparation.variants[0].id]])
 check(name+' refuses planning while retaining original source',()=>{const x=structuredClone(example);mutate(x);const f=build(x);reject(cli,['corpus','list',f.pack,key,'1']);ok(cli,['corpus','verify',f.pack,f.source])});
 check('negative and unbounded household counts refuse',()=>{for(const n of ['0','-1','101','1oops'])reject(cli,['corpus','list',family.pack,key,n])});
 check('HTML from a retained recipe is text rather than execution',()=>{const x=structuredClone(example);x.title='<script>alert(1)</script>';const f=build(x);const html=ok(driver,['view',f.pack,key,'1']);assert(!html.includes('<script>'));assert(html.includes('&lt;script&gt;'))});
 check('source search remains available on the factored representation',()=>assert.match(ok(driver,['view',family.pack,'','1','oat']),/Ten oat-bite/));
 const training=Array.from({length:160},(_,i)=>JSON.stringify({id:'dict-'+i,name:'Variant '+i,instructions:'Arrange the common material in distinct lots and retain the exact input identity. '.repeat(25)+i})).join('\n')+'\n';
 const trained=build(training);
 check('the shared dictionary is actually consumed and every original byte survives',()=>{const d=new Database(trained.pack,{readonly:true});try{assert(d.query('SELECT length(payload) n FROM record_dictionary').get().n>0);assert(d.query('SELECT count(*) n FROM record_atom WHERE codec=2').get().n>0);assert.equal(d.query("SELECT count(*) n FROM sqlite_master WHERE name='build_atom_digest'").get().n,0)}finally{d.close()}assert.equal(ok(cli,['corpus','export',trained.pack]),training)});
 check('a damaged shared dictionary fails before a record is returned',()=>{edit(trained.pack,'UPDATE record_dictionary SET payload=zeroblob(length(payload))');reject(cli,['corpus','verify',trained.pack,trained.source])});
 check('prior nondictionary schema remains readable',()=>{const f=build(raw);edit(f.pack,'PRAGMA user_version=1;DROP TABLE record_dictionary');assert.equal(ok(cli,['corpus','export',f.pack]),raw)});
 check('source files are never edited by the native reader or factor',()=>assert.equal(readFileSync(exact.source,'utf8'),raw));
 console.log(`${passed} native material checks passed; source record, batch quantities and frontend fragments share the same compiled implementation.`);
} finally {rmSync(tmp,{recursive:true,force:true})}
