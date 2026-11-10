import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {spawn} from 'node:child_process';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,readdirSync,existsSync,rmSync,copyFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=process.env.BONFYRE_CMS_TEST_BINARY||resolve(import.meta.dir,'../bonfyre-cms');
const schemasSource=resolve(import.meta.dir,'../content-types');
const page=(title,route='/guide')=>({route,title,heading:title,status:'published'});
const fragments=(generation,route='/guide')=>[0,1].map(position=>({coordinate:`${route}:${generation}:${position}`,page:route,slot:'body',audience:'anonymous',kind:'copy',position,body:`${generation}-${position}`,status:'published'}));
const request=(operation,expected_id,title='Original',route='/guide')=>({operation,expected_id,route,page:operation==='remove'?null:page(title,route),fragments:operation==='remove'?[]:fragments(title,route)});
async function fixture(fn){
  const home=mkdtempSync(join(tmpdir(),'cms-page-write-')),path=join(home,'cms.db'),schemas=join(home,'schemas'),deltas=join(home,'deltas'),marker=join(home,'hooks.txt');
  mkdirSync(schemas);mkdirSync(deltas);
  for(const name of ['site_page','site_fragment'])copyFileSync(join(schemasSource,name+'.json'),join(schemas,name+'.json'));
  const env={...process.env,BONFYRE_FACT_DELTAS:deltas};
  const args=a=>[...a,'--db',path,'--schemas',schemas];
  const raw=(a,overrides={})=>{const p=Bun.spawnSync([binary,...args(a)],{env:{...env,...overrides},timeout:20000});return {code:p.exitCode,stdout:p.stdout.toString(),stderr:p.stderr.toString()};};
  let serial=0;
  const read=(route='/guide',ns='sample')=>{const result=raw(['entry','page-read',route,'--ns',ns]);if(result.code)throw Error(result.stdout+result.stderr);return JSON.parse(result.stdout);};
  const prepare=body=>{if(typeof body==='object'&&body.operation!=='create'&&!Object.hasOwn(body,'expected_digest'))body={...body,expected_digest:read(body.route).digest};const file=join(home,`request-${serial++}.json`);writeFileSync(file,typeof body==='string'?body:JSON.stringify(body));return ['entry','page-write','@'+file,'--ns','sample'];};
  const write=(body,ns='sample',overrides={})=>{const a=prepare(body);a[a.length-1]=ns;return raw(a,overrides);};
  expect(raw(['schema','migrate']).code).toBe(0);
  const db=new Database(path);
  const snapshot=()=>Object.fromEntries(['site_page','site_fragment','_ops','_relations'].map(t=>[t,db.query(`SELECT * FROM ${t} ORDER BY rowid`).all()]));
  const effects=()=>({deltas:readdirSync(deltas).sort().map(n=>[n,readFileSync(join(deltas,n),'utf8')]),hooks:existsSync(marker)?readFileSync(marker,'utf8'):''});
  try{await fn({home,path,db,raw,write,prepare,args,env,snapshot,effects,marker,deltas,read});}finally{db.close();rmSync(home,{recursive:true,force:true});}
}

test('page and fragment writes roll back together with history and deferred effects',()=>fixture(async f=>{
  expect(f.write(request('create',0)).code).toBe(0);
  const id=f.db.query('SELECT id FROM site_page').get().id;
  f.db.query('INSERT INTO _hooks(content_type,event,command) VALUES(?,?,?)').run('site_page','after_update',`printf hook >> '${f.marker}'`);
  f.db.exec("CREATE TRIGGER refuse_fragment BEFORE INSERT ON site_fragment WHEN new.body='Refused-1' BEGIN SELECT RAISE(ABORT,'injected failure'); END");
  const before=f.snapshot(),effects=f.effects();
  for(const body of [request('replace',id,'Refused'),request('create',0,'Refused','/new')]){
    const result=f.write(body);expect(result.code).toBe(1);expect(JSON.parse(result.stdout).committed).toBe(false);
    expect(f.snapshot()).toEqual(before);expect(f.effects()).toEqual(effects);
  }
  f.db.exec("CREATE TRIGGER refuse_page_delete BEFORE DELETE ON site_page BEGIN SELECT RAISE(ABORT,'injected failure'); END");
  expect(f.write(request('remove',id)).code).toBe(1);expect(f.snapshot()).toEqual(before);expect(f.effects()).toEqual(effects);
  f.db.exec('DROP TRIGGER refuse_page_delete');
  // Operation history is part of the same write, including the last deletion.
  f.db.exec("CREATE TRIGGER refuse_history BEFORE INSERT ON _ops WHEN new.target='site_page' BEGIN SELECT RAISE(ABORT,'injected history failure'); END");
  for(const body of [request('replace',id,'History'),request('remove',id)]){
    expect(f.write(body).code).toBe(1);expect(f.snapshot()).toEqual(before);expect(f.effects()).toEqual(effects);
  }
  f.db.exec('DROP TRIGGER refuse_history');
  expect(f.write(request('replace',id,'Accepted')).code).toBe(0);
  expect(f.db.query('SELECT id,title FROM site_page').get()).toEqual({id,title:'Accepted'});
  expect(f.db.query('SELECT body FROM site_fragment ORDER BY position').all().map(r=>r.body)).toEqual(['Accepted-0','Accepted-1']);
  expect(f.effects().hooks).toBe('hook');expect(f.effects().deltas.length).toBeGreaterThan(effects.deltas.length);
  expect(f.write(request('remove',id)).code).toBe(0);
  expect(f.db.query('SELECT count(*) AS n FROM site_page').get().n).toBe(0);
  expect(f.db.query('SELECT count(*) AS n FROM site_fragment').get().n).toBe(0);
}));

test('namespace, stale identity and malformed aggregates are refused without writes',()=>fixture(async f=>{
  expect(f.write(request('create',0)).code).toBe(0);
  const id=f.db.query('SELECT id FROM site_page').get().id,before=f.snapshot(),effects=f.effects();
  const wrongParent=request('replace',id,'Wrong');wrongParent.fragments[1].page='/elsewhere';
  const oversized=request('replace',id,'Large');oversized.fragments[1].body='x'.repeat(70000);
  const malformed=[request('create',0,'Duplicate'),request('replace',id+1,'Stale'),wrongParent,oversized,
    {...request('replace',id),page:page('Wrong','/other')},
    {...request('replace',id),fragments:[null]},
    {...request('replace',id),fragments:Array.from({length:513},()=>fragments('Too many')[0])},
    JSON.stringify(request('replace',id)).replace('"operation":','"operation":"create","operation":'),
    JSON.stringify(request('replace',id)).replace('Original','Bad\\u0000text'),'{invalid'];
  for(const body of malformed){expect(f.write(body).code, typeof body === 'string' ? body.slice(0,100) : JSON.stringify({...body,fragments:body.fragments?.length})).not.toBe(0);expect(f.snapshot()).toEqual(before);expect(f.effects()).toEqual(effects);}
  expect(f.write(request('replace',id,'Wrong namespace'),'other').code).not.toBe(0);expect(f.snapshot()).toEqual(before);
  expect(f.write(request('create',0,'Other'),'other').code).toBe(0);
  expect(f.db.query("SELECT title FROM site_page WHERE namespace='sample'").get().title).toBe('Original');
}));

test('concurrent replacements admit one reviewed generation and run its hook after commit',()=>fixture(async f=>{
  expect(f.write(request('create',0)).code).toBe(0);
  const id=f.db.query('SELECT id FROM site_page').get().id;
  // A separate connection in the hook must see both committed fragments.
  const hook=`sqlite3 '${f.path}' 'SELECT count(*) FROM site_fragment;' >> '${f.marker}'`;
  f.db.query('INSERT INTO _hooks(content_type,event,command) VALUES(?,?,?)').run('site_page','after_update',hook);
  const digest=f.read().digest;
  const codes=await Promise.all(['A','B'].map(title=>new Promise((resolve,reject)=>{
    const p=spawn(binary,f.args(f.prepare({...request('replace',id,title),expected_digest:digest})),{env:f.env,stdio:'ignore'});
    p.on('error',reject);p.on('exit',resolve);
  })));
  expect(codes.sort()).toEqual([0,1]);
  const title=f.db.query('SELECT title FROM site_page').get().title;expect(['A','B']).toContain(title);
  expect(f.db.query('SELECT body FROM site_fragment ORDER BY position').all().map(r=>r.body)).toEqual([title+'-0',title+'-1']);
  expect(readFileSync(f.marker,'utf8')).toBe('2\n');
}));

test('notification failure reports an already committed page explicitly',()=>fixture(async f=>{
  const result=f.write(request('create',0),'sample',{BONFYRE_FACT_DELTAS:join(f.home,'missing')});
  expect(result.code).toBe(2);expect(JSON.parse(result.stdout)).toMatchObject({committed:true,fragments:2,fact_delta:'DELIVERY_FAILED'});
  expect(f.db.query('SELECT title FROM site_page').get().title).toBe('Original');
  expect(f.db.query('SELECT count(*) AS n FROM site_fragment').get().n).toBe(2);
}));


test('snapshot covers every page fragment and namespace while unrelated edits remain independent',()=>fixture(async f=>{
  const missing=f.read();expect(missing.id).toBe(0);expect(missing.snapshot).toEqual({namespace:'sample',route:'/guide',page:null,fragments:[]});
  expect(f.write(request('create',0,'A quoted “review” 🧭')).code).toBe(0);
  const original=f.read(),again=f.read();expect(again).toEqual(original);expect(original.snapshot.fragments).toHaveLength(2);
  expect(original.snapshot.page.title).toBe('A quoted “review” 🧭');expect(original.digest).toMatch(/^[a-f0-9]{64}$/);
  const changed={...request('replace',original.id,'Reviewed'),expected_digest:original.digest};
  expect(f.write(request('create',0,'Unrelated','/other')).code).toBe(0);expect(f.read().digest).toBe(original.digest);
  expect(f.write(request('create',0,'Other namespace'),'other').code).toBe(0);expect(f.read().digest).toBe(original.digest);expect(f.read('/guide','other').digest).not.toBe(original.digest);
  const fragment=original.snapshot.fragments[0];
  expect(f.raw(['entry','update','site_fragment',String(fragment.id),JSON.stringify({body:'A colleague changed this section.'}),'--ns','sample']).code).toBe(0);
  const prior=f.snapshot(),effects=f.effects();expect(f.write(changed).code).toBe(1);expect(f.snapshot()).toEqual(prior);expect(f.effects()).toEqual(effects);
  expect(f.write({...request('remove',original.id),expected_digest:original.digest}).code).toBe(1);expect(f.snapshot()).toEqual(prior);
  const current=f.read(),accepted=f.write({...changed,expected_digest:current.digest});expect(accepted.code).toBe(0);expect(JSON.parse(accepted.stdout).digest).toBe(f.read().digest);
}));

test('replacement requires an exact digest and cannot hide a same-identity page edit',()=>fixture(async f=>{
  expect(f.write(request('create',0)).code).toBe(0);const prior=f.read();
  for(const expected_digest of [null,'','a'.repeat(63),prior.digest+'a','A'.repeat(64),'0'.repeat(64),1]){
    const before=f.snapshot();expect(f.write({...request('replace',prior.id,'Bad'),expected_digest}).code).toBe(1);expect(f.snapshot()).toEqual(before);
  }
  const raw=JSON.stringify(request('replace',prior.id,'Missing digest'));expect(f.write(raw).code).toBe(1);
  expect(f.raw(['entry','update','site_page',String(prior.id),JSON.stringify({title:'Another author'}),'--ns','sample']).code).toBe(0);
  expect(f.read().id).toBe(prior.id);expect(f.write({...request('replace',prior.id,'Stale'),expected_digest:prior.digest}).code).toBe(1);expect(f.read().snapshot.page.title).toBe('Another author');
}));


test('oversized retained aggregates refuse a snapshot instead of returning a partial page',()=>fixture(async f=>{
  expect(f.write(request('create',0)).code).toBe(0);
  f.db.exec("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<511) INSERT INTO site_fragment(namespace,coordinate,page,slot,audience,kind,position,body,status,created_at,updated_at) SELECT 'sample','extra-'||x,'/guide','body','anonymous','copy',x+2,'Retained section','draft','2026-10-20','2026-10-20' FROM n");
  const before=f.snapshot(),result=f.raw(['entry','page-read','/guide','--ns','sample']);
  expect(result.code).toBe(1);expect(JSON.parse(result.stdout)).toEqual({error:'page snapshot unavailable'});expect(f.snapshot()).toEqual(before);
}));
