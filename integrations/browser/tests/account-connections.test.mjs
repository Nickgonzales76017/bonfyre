import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import {AccountConnections} from '../onboarding/account-connections.mjs';
import {loadConnectionCatalog} from '../onboarding/connection-catalog.mjs';
import * as en from '../onboarding/enrollment.mjs';
import {encode} from '../workspace-msgpack.mjs';
import {providerReview,validateProviderAdmission} from '../provider-contract.mjs';
import {readConnectionPlan,readConnectionSetup,createConnectionState} from '../connection-source.mjs';
const binding={actor:'user:19',subject:'user:19',scope:'self'};
function fixture(work){const home=mkdtempSync(join(tmpdir(),'account-connections-'));mkdirSync(join(home,'queue'));const providers=join(home,'providers.json');writeFileSync(providers,'[]');const c=en.config({...process.env,HOME:home,BONFYRE_ENROLLMENT_HOME:join(home,'queue'),BONFYRE_ENROLLMENT_DB:join(home,'queue/enrollment.db'),BONFYRE_CREDENTIAL_PROVIDERS:providers,BONFYRE_CREDENTIALS_PROVIDERS:providers});en.seed(c);const assignments=[{binding,identities:['gmail','proton']}],owner=new AccountConnections(binding,{config:c,assignments});const db=new Database(c.db);const rows=()=>db.query('SELECT * FROM workgraph_nodes ORDER BY mission_id,node_id').all();return Promise.resolve().then(()=>work({owner,c,assignments,db,rows})).finally(()=>{db.close();rmSync(home,{recursive:true,force:true});});}
test('original identities, empty trailing fields, independent access findings and recovery targets survive',()=>{
 const rows=loadConnectionCatalog();expect(rows).toHaveLength(225);expect(new Set(rows.map(r=>r.id)).size).toBe(225);expect(rows.filter(r=>r.origin==='retained-200-provider-list')).toHaveLength(200);expect(rows.find(r=>r.id==='060').gate).toBe('retired');expect(rows.find(r=>r.id==='037').gate).toBe('existing-only');expect(rows.find(r=>r.id==='008').gate).toBe('public');expect(rows.find(r=>r.id==='009').provider).toBe('github');expect(rows.find(r=>r.id==='201').recoveryScopes.length).toBeGreaterThan(1);
});
test('both account lanes read all original items without acquiring work',()=>fixture(({owner,rows})=>{
 const before=rows();for(const identity of ['gmail','proton']){const list=owner.list(identity);expect(list.prepared).toBe(200);expect(list.missing).toHaveLength(25);for(const row of list.items){const view=owner.inspect(identity,row.workId);expect(view.item.work_id).toBe(row.workId);expect(view.revision).toBe(0);expect(providerReview(view.review)).toBe(true);expect(view.connectionState).toBe('setup_incomplete');}}expect(rows()).toEqual(before);
}),60000); // 400 queue reads; allow time for a loaded host without relaxing assertions.
test('supplement preparation is native idempotent and leaves the four hundred original nodes intact',()=>fixture(({owner,rows})=>{
 const before=rows();owner.prepare();const first=rows();expect(first).toHaveLength(450);for(const old of before)expect(first.find(r=>r.mission_id===old.mission_id)).toEqual(old);owner.prepare();expect(rows()).toEqual(first);for(const identity of ['gmail','proton']){expect(owner.list(identity).prepared).toBe(225);expect(owner.list(identity).missing).toEqual([]);}
}));
test('unassigned actors, destinations and identity lanes are refused',()=>fixture(({c,assignments,owner})=>{
 for(const b of [{actor:'user:20',subject:'user:20',scope:'self'},{actor:'user:19',subject:'institution:studio',scope:'institution'}])expect(()=>new AccountConnections(b,{config:c,assignments})).toThrow(/assigned/);
 expect(()=>owner.inspect('gmail','011-proton')).toThrow();expect(()=>owner.list('other')).toThrow();
}));
test('provider handoff is exact, fresh, bounded and separate from connection completion',()=>fixture(({owner,rows})=>{
 owner.prepare();const before=rows(),view=owner.inspect('gmail','201-gmail'),challenge='a'.repeat(64),origin='https://aurekai.work';
 const packet=owner.provider('gmail','201-gmail',view.review.revision,challenge,origin);expect(validateProviderAdmission([1,true,packet],{origin,review:view.review,challenge}).work.owner).toBe('BonfyreEnrollment');expect(packet.provider.url).toContain('dash.cloudflare.com');expect(rows()).toEqual(before);
 expect(()=>owner.provider('gmail','060-gmail',1,challenge,origin)).toThrow();expect(()=>owner.provider('gmail','008-gmail',1,challenge,origin)).toThrow();expect(()=>owner.provider('gmail','201-gmail',2,challenge,origin)).toThrow();expect(()=>validateProviderAdmission([1,true,{...packet,expiresAt:0}],{origin,review:view.review,challenge})).toThrow();
}));
test('explicit continuation, human progress and wait keep the same item; human verification is refused',()=>fixture(({owner})=>{
 let view=owner.inspect('gmail','014-gmail');view=owner.change('gmail',{action:'continue',review:view.review});expect(view.status).toBe('running');const old=view.review;view=owner.change('gmail',{action:'record',review:view.review,stage:'account_bound',note:'Fixture observation only'});expect(view.revision).toBe(1);expect(view.missing).not.toContain('account_bound');expect(()=>owner.change('gmail',{action:'record',review:old,stage:'scope_confirmed',note:''})).toThrow(/changed/);expect(()=>owner.change('gmail',{action:'record',review:view.review,stage:'credential_verified',note:''})).toThrow(/native owner/);view=owner.change('gmail',{action:'wait',review:view.review,why:'mfa',note:'Fixture wait'});expect(view.status).toBe('retry_wait');expect(view.item.work_id).toBe('014-gmail');expect(view.missing).toContain('credential_verified');
}));
test('actual native replies are accepted by the browser decoder without inventing connections',()=>fixture(async({owner})=>{
 const request=async url=>{const q=new URL(url,'https://aurekai.work').searchParams;const value=q.get('operation')==='list'?owner.list(q.get('identity')):owner.inspect(q.get('identity'),q.get('work'));return new Response(encode([1,true,value]),{headers:{'Content-Type':'application/msgpack'}});};
 const plan=await readConnectionPlan('self','gmail',{request});expect(plan.prepared).toBe(200);expect((await readConnectionSetup('self','gmail','014-gmail',{request,binding:plan.binding})).review.workId).toBe('014-gmail');
 const state=createConnectionState({request});await state.load();await state.inspect('014-gmail');expect(state.snapshot.detail.item.work_id).toBe('014-gmail');await state.identity('proton');expect(state.snapshot.detail).toBeNull();expect(state.snapshot.plan.identity).toBe('proton');state.destroy();
}));
test('a denied current account clears private state and delayed old responses cannot restore it',()=>fixture(async({owner})=>{
 let deny=false;const state=createConnectionState({request:async url=>deny?new Response('No',{status:403}):new Response(encode([1,true,owner.list('gmail')]),{headers:{'Content-Type':'application/msgpack'}})});await state.load();expect(state.snapshot.plan).not.toBeNull();deny=true;await state.load();expect(state.snapshot.locked).toBe(true);expect(state.snapshot.plan).toBeNull();expect(state.snapshot.detail).toBeNull();state.destroy();
}));
