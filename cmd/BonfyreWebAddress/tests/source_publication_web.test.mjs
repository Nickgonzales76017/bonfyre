import {test,expect,beforeAll,afterAll} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
let f,owner;const prior=process.env.BONFYRE_WORKSPACE_RELEASE;
beforeAll(async()=>{process.env.BONFYRE_WORKSPACE_RELEASE??='a5b379ab187e523c3d1eb090a33d014287491c4d37594520bbb7b4c151183e5a';f=await workspaceFixture();const schemas=join(f.home,'schemas');for(const type of ['source_collection','world','internet_source'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);f.add('internet_source',{source:'web-workshop',family:'news',label:'Workshop',says:'Organizer notices',url:'https://example.test/workshop/feed',state:'observed',status:'published'});f.run(f.auth,['update','--id',String(f.owner.id),'--tier','pro','--db',f.authdb]);owner=await f.login('workspace-owner@example.test',{signup:false});},30000);
afterAll(async()=>{await f?.close();if(prior===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=prior;});
const packet=(id='b'.repeat(64))=>[1,id,'foreign:web-workshop','web-workshop','Repair workshop','The retained organizer notices, selected for this publication.','atlas'];
const write=(p,user=owner,headers={})=>f.raw('/cms/source-publication',user,{method:'POST',headers:{'Content-Type':'application/msgpack',...headers},body:encode(p)});
const read=(id,user=owner)=>f.raw('/cms/source-publication?request='+id,user);
test('real HTTP publication and readback retain the exact binary tuple and owner pair',async()=>{
 const p=packet(),empty=await read(p[1]);expect(empty.status).toBe(200);expect(decode(new Uint8Array(await empty.arrayBuffer()))[1]).toBe('absent');
 const saved=await write(p);expect(saved.status,await saved.clone().text()).toBe(200);expect(saved.headers.get('content-type')).toBe('application/msgpack');expect(saved.headers.get('cache-control')).toContain('private');const result=decode(new Uint8Array(await saved.arrayBuffer()));expect(result[7]).toEqual(p);
 const before=f.sql.query('SELECT * FROM world ORDER BY id').all();const retry=await write(p);expect(retry.status).toBe(200);expect(decode(new Uint8Array(await retry.arrayBuffer()))[8]).toBe('UNCHANGED');expect(f.sql.query('SELECT * FROM world ORDER BY id').all()).toEqual(before);
 const reopened=await read(p[1]);expect(decode(new Uint8Array(await reopened.arrayBuffer()))[7]).toEqual(p);
});
test('anonymous, read-only and foreign-Origin requests cannot create or inspect the owner publication',async()=>{
 const before=f.sql.query('SELECT count(*) AS n FROM world').get().n;
 expect((await write(packet('c'.repeat(64)),null)).status).toBe(403);expect((await read('b'.repeat(64),null)).status).toBe(403);expect((await write(packet('c'.repeat(64)),f.reader)).status).toBe(403);
 expect((await write(packet('c'.repeat(64)),owner,{Origin:'https://foreign.example.test'})).status).toBe(403);expect(f.sql.query('SELECT count(*) AS n FROM world').get().n).toBe(before);
});
test('changed replays, malformed queries and unsupported carriers are refused',async()=>{
 const p=packet();p[4]='Overwrite someone else’s changes';expect((await write(p)).status).toBe(409);
 expect((await f.raw('/cms/source-publication?request='+'b'.repeat(64)+'&request='+'b'.repeat(64),owner)).status).toBe(400);
 expect((await write(packet('d'.repeat(64)),owner,{'Content-Type':'application/json'})).status).toBe(400);
 expect((await write(packet('e'.repeat(64)).concat('unrecognized'))).status).toBe(400);
});
