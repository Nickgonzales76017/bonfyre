import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {packSetupFixture,root,decode as decodeHTML} from './pack_setup_fixture.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';
let f,owner,reader,other,home;
beforeAll(async()=>{
  f=await packSetupFixture();copyFileSync(join(root,'cmd/BonfyreCMS/content-types/household.json'),join(f.home,'schemas/household.json'));
  f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
  owner=await f.login('dest-owner@example.test');reader=await f.login('dest-reader@example.test');other=await f.login('dest-other@example.test');
  f.grant(owner.id,'destination-team','admin');f.grant(reader.id,'destination-team','read');f.grant(owner.id,'expired-team','write');
  f.authsql.run("UPDATE institution_grants SET expires_at='2000-01-01T00:00:00Z' WHERE institution='expired-team'");
  f.authsql.run("INSERT INTO institution_origins(institution,creator,request_digest,label,created_at) VALUES(?,?,?,?,?)",['destination-team',owner.id,'destination-fixture','Field team & partners','2026-10-20T00:00:00Z']);
  f.add('household',{household_id:'destination-home',label:'Shared home',steward:'user:'+owner.id,opened_at:'2026-10-20',cannot_infer:'Isolated fixture.',status:'published'});
  home=f.sql.query("SELECT id FROM household WHERE household_id='destination-home'").get().id;
  f.sql.exec('CREATE TABLE _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER NOT NULL);CREATE TABLE _household_membership(household_row INTEGER NOT NULL,member TEXT NOT NULL,role TEXT NOT NULL,revision INTEGER NOT NULL,authority_epoch INTEGER NOT NULL,expires_at INTEGER NOT NULL,granted_by TEXT NOT NULL,PRIMARY KEY(household_row,member));');
  f.sql.run('INSERT INTO _household_authority VALUES(?,1)',[home]);
  f.sql.run('INSERT INTO _household_membership VALUES(?,?,?,1,1,0,?)',[home,'user:'+reader.id,'read','user:'+owner.id]);
},60000);
afterAll(async()=>{await f?.close();});
async function request(user=owner,path='/apps/_destinations',method='GET'){
  const response=await fetch(f.base+path,{method,headers:{Host:'aurekai.life',...(user?{Cookie:user.cookie}:{})},redirect:'manual',signal:AbortSignal.timeout(20000)}),bytes=new Uint8Array(await response.arrayBuffer());
  return {status:response.status,headers:response.headers,value:response.headers.get('content-type')==='application/msgpack'?decode(bytes):new TextDecoder().decode(bytes)};
}
const row=(result,context)=>result.value[1].find(r=>r[0]===context);
test('authenticated destination discovery matches the human picker and preserves bindings schema',async()=>{
  const result=await request();expect(result.status).toBe(200);expect(result.value[0]).toBe(1);expect(result.value[1]).toEqual([['self','My personal workspace',true],['institution:destination-team','Field team & partners',true],['household:destination-home','Household: Shared home',true]]);
  const html=await f.request('/packs/setup?pack=creator-work',{cookie:owner.cookie}),select=html.body.match(/<select id="pack-context"[^>]*>([\s\S]*?)<\/select>/)?.[1];
  expect(select).toBeDefined();expect([...select.matchAll(/<option value="([^"]*)"[^>]*>([^<]*)<\/option>/g)].map(m=>[decodeHTML(m[1]),decodeHTML(m[2])])).toEqual(result.value[1].map(r=>r.slice(0,2)));
  expect(result.headers.get('cache-control')).toBe('private, no-store');expect(result.headers.has('access-control-allow-origin')).toBe(false);
  const bindings=await request(owner,'/apps/_bindings?context=self');expect(bindings.status).toBe(200);expect(bindings.value).toEqual([1,'user:'+owner.id,'self',[]]);
});
test('current read, write and revoked grants are projected without inheriting another destination',async()=>{
  let result=await request(reader);expect(row(result,'institution:destination-team')[2]).toBe(false);expect(row(result,'household:destination-home')[2]).toBe(false);
  f.grant(reader.id,'destination-team','write');f.sql.run("UPDATE _household_membership SET role='write' WHERE member=?",['user:'+reader.id]);
  result=await request(reader);expect(row(result,'institution:destination-team')[2]).toBe(true);expect(row(result,'household:destination-home')[2]).toBe(true);
  f.run(f.auth,['--db',f.authdb,'institution-revoke','--user-id',String(reader.id),'--institution','destination-team']);f.sql.run("UPDATE _household_membership SET role='none' WHERE member=?",['user:'+reader.id]);
  result=await request(reader);expect(result.value[1]).toEqual([['self','My personal workspace',true]]);expect((await request(other)).value[1]).toEqual([['self','My personal workspace',true]]);
});
test('household epoch and expiry are checked by the current household owner',async()=>{
  f.sql.run("UPDATE _household_membership SET role='write',authority_epoch=1,expires_at=0 WHERE member=?",['user:'+reader.id]);f.sql.run('UPDATE _household_authority SET epoch=2 WHERE household_row=?',[home]);
  expect(row(await request(reader),'household:destination-home')).toBeUndefined();
  f.sql.run('UPDATE _household_membership SET authority_epoch=2,expires_at=1 WHERE member=?',['user:'+reader.id]);expect(row(await request(reader),'household:destination-home')).toBeUndefined();
  f.sql.run('UPDATE _household_membership SET expires_at=0 WHERE member=?',['user:'+reader.id]);expect(row(await request(reader),'household:destination-home')[2]).toBe(true);
});
test('anonymous, caller-selected identities and mutation methods receive no destination projection',async()=>{
  expect((await request(null)).status).toBe(403);
  for(const query of ['?actor=user:1','?context=self','?context=institution:destination-team','?limit=1'])expect((await request(owner,'/apps/_destinations'+query)).status).toBe(400);
  expect((await request(owner,'/apps/_destinations','POST')).status).toBe(405);
});
test('an oversized destination set refuses the entire machine and human selection',async()=>{
  const insert=f.authsql.query("INSERT INTO institution_grants(user_id,institution,role,granted_at,active) VALUES(?,?,'read','2026-10-20T00:00:00Z',1)");
  for(let i=0;i<101;i++)insert.run(other.id,'overflow-'+i);
  try{const result=await request(other);expect(result.status).toBe(503);expect(typeof result.value).toBe('string');const html=await f.request('/packs/setup?pack=creator-work',{cookie:other.cookie});expect(html.body).toContain('complete destination list is unavailable');expect(html.body).not.toContain('id="pack-context"');}
  finally{f.authsql.run("DELETE FROM institution_grants WHERE user_id=? AND institution LIKE 'overflow-%'",[other.id]);}
  expect((await request(other)).value[1]).toEqual([['self','My personal workspace',true]]);
});
test('broken membership state refuses rather than returning a plausible partial list',async()=>{
  f.sql.run('ALTER TABLE _household_authority RENAME TO unavailable_household_authority');
  try{expect((await request()).status).toBe(503);}finally{f.sql.run('ALTER TABLE unavailable_household_authority RENAME TO _household_authority');}
  expect((await request()).status).toBe(200);
});
