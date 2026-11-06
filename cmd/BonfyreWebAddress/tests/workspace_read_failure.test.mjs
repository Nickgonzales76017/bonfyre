// Isolated origin and accounts: a database failure must not impersonate a
// missing installation, and recovery must preserve the original continuation.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {workspaceFixture} from './workspace_fixture.mjs';
import {decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const release=process.env.BONFYRE_READ_FAILURE_RELEASE||'d8b2d86be17aed532bf84cffb9fb6c75efe1b8c4e3ca1375e4c2730a72b0c25a';
const prior=process.env.BONFYRE_WORKSPACE_RELEASE,pack='fixture.workspace-read-failure',key='draft:instruments:research:read-failure';
let f,binding,openPath;
beforeAll(async()=>{
 process.env.BONFYRE_WORKSPACE_RELEASE=release;f=await workspaceFixture();
 f.pack(pack,'Isolated workspace read check');
 f.add('pack_demand',{pack,demand_kind:'workspace',needs:'connected-instruments',preferred_sources:release,fields:'instruments/',status:'published'});
 f.add('pack_demand',{pack,demand_kind:'workspace-record',needs:'connected-instruments',fields:'draft:instruments:research:',status:'published'});
 binding=await f.install(pack);
 await f.store(binding).saveLocal(key,{kind:'research',title:'Storage failure regression',sources:[],findings:[],selectedSource:null,selectedFinding:null,tab:'sources'},0);
 const response=await f.raw(binding.endpoint+'?operation=get&prefix='+encodeURIComponent(key));expect(response.status).toBe(200);
 const wire=decode(new Uint8Array(await response.arrayBuffer())),row=wire[4][0],digest=createHash('sha256').update(row[2]).digest('hex');
 openPath='/apps/'+binding.id+'/_open?'+new URLSearchParams({view:'research',key,revision:String(row[1]),release,digest});
},60000);
afterAll(async()=>{await f?.close();if(prior===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=prior;});
test('the exact saved-work continuation reaches the installed editor',async()=>{
 const r=await f.raw(openPath);expect(r.status).toBe(303);const dest=r.headers.get('location');expect(dest).toContain(binding.id+'/instruments/');expect(dest).toContain('record=');
 const page=await f.raw(dest);expect(page.status).toBe(200);expect(await page.text()).toContain('bonfyre-workspace-store');
});
test('a database lookup error is unavailable, not inactive, and recovery needs no reinstallation',async()=>{
 const before=await f.store(binding).readVersion(key);
 f.sql.exec('ALTER TABLE _application_workspace RENAME TO _fixture_unavailable_workspace');
 try{for(const path of [openPath,binding.path]){const r=await f.raw(path);expect(r.status).toBe(503);expect(r.headers.get('cache-control')).toContain('no-store');const body=await r.text();expect(body).toContain('storage could not be read');expect(body).not.toContain('inactive');}}
 finally{f.sql.exec('ALTER TABLE _fixture_unavailable_workspace RENAME TO _application_workspace');}
 expect((await f.raw(openPath)).status).toBe(303);expect(await f.store(binding).readVersion(key)).toEqual(before);
});
test('genuinely inactive and absent bindings remain refused',async()=>{
 const row=f.sql.query('SELECT cms_id FROM _application_workspace WHERE binding_id=?').get(binding.id);
 f.sql.query("UPDATE application_binding SET status='inactive' WHERE id=?").run(row.cms_id);
 try{expect((await f.raw(openPath)).status).toBe(404);}finally{f.sql.query("UPDATE application_binding SET status='active' WHERE id=?").run(row.cms_id);}
 expect((await f.raw('/apps/ws-'+'0'.repeat(40)+'/instruments/')).status).toBe(404);expect((await f.raw(openPath)).status).toBe(303);
});
test('signed-out and foreign-account requests retain the original access boundary',async()=>{
 const r=await f.raw(openPath,null);expect(r.status).toBe(303);expect(r.headers.get('location')).toContain('/login?next=');expect(r.headers.get('location')).toContain(encodeURIComponent(openPath));
 expect((await f.raw(openPath,f.other)).status).toBe(403);await expect(f.store(binding,f.other).readVersion(key)).rejects.toMatchObject({status:403});
});
