import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes,createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..'),binary=process.env.BONFYRE_TEST_AUTH_BINARY??join(root,'cmd/BonfyreAuth/bonfyre-auth'),secret=()=>randomBytes(32).toString('hex'),hash=s=>createHash('sha256').update(s).digest('hex');
function fixture(run){const dir=mkdtempSync(join(tmpdir(),'org-auth-')),db=join(dir,'auth.db'),password='isolated-organization-fixture';const cli=(args,input)=>Bun.spawnSync([binary,'--db',db,...args],{stdout:'pipe',stderr:'pipe',stdin:input?Buffer.from(input):undefined,timeout:10000});
 const signup=email=>{const r=cli(['http-signup'],JSON.stringify({name:'Fixture person',email,password}));if(r.exitCode)throw Error(r.stderr.toString());const id=JSON.parse(r.stdout.toString()).user.id;const login=cli(['http-login'],JSON.stringify({email,password}));if(login.exitCode)throw Error(login.stderr.toString());return {id,email,token:JSON.parse(login.stdout.toString()).session.token};};
 const call=(user,op,args=[])=>{const body=encode([1,user?.token??'',op,args]),r=cli(['organization-stdin'],body);expect(r.stderr.toString()).not.toContain(user?.token??'never-a-token');const [version,status,message,rows,cursor]=decode(r.stdout);expect(version).toBe(1);expect(r.exitCode===0).toBe(status<400);return {status,message,rows,cursor};};
 const owner=signup('owner@example.test'),user=signup('member@example.test'),stranger=signup('other@example.test'),sql=new Database(db);
 const create=(name='Example collective',nonce=secret())=>{const result=call(owner,'create',[name,nonce]);expect(result.status).toBe(201);return {id:result.rows[0][0],nonce};};
 try{return run({dir,db,sql,cli,signup,call,owner,user,stranger,create,grant:(id,who,role,expiry=null)=>{sql.query('UPDATE institution_grants SET role=?,expires_at=? WHERE institution=? AND user_id=?').run(role,expiry,id,who.id);}});}finally{sql.close();rmSync(dir,{recursive:true,force:true});}}
test('explicit native creation gives only the new destination to its authenticated creator and replays exactly',()=>fixture(({call,owner,stranger,create,sql})=>{
 const {id,nonce}=create('A collective <not an external claim>');expect(id).toMatch(/^org-[a-f0-9]{32}$/);expect(sql.query('SELECT institution,role FROM institution_grants').all()).toEqual([{institution:id,role:'admin'}]);expect(call(owner,'create',['A collective <not an external claim>',nonce]).status).toBe(200);expect(call(owner,'create',['Different details',nonce]).status).toBe(409);
 expect(call(stranger,'create',['A collective <not an external claim>',nonce]).rows[0][0]).not.toBe(id);expect(call(owner,'create',['bonfyre',secret(),'bonfyre']).status).toBe(400);expect(sql.query("SELECT count(*) n FROM institution_grants WHERE institution='bonfyre'").get().n).toBe(0);
 sql.query('UPDATE institution_grants SET active=0 WHERE institution=? AND user_id=?').run(id,owner.id);expect(call(owner,'create',['A collective <not an external claim>',nonce]).status).toBe(403);
}));
test('invalid session, read-only session, bad fields and injected grant failure leave no organization',()=>fixture(({call,owner,sql,cli})=>{
 expect(call(null,'create',['No access',secret()]).status).toBe(401);for(const name of ['', ' '.repeat(4),' padded','x'.repeat(161),'bad\nlabel'])expect(call(owner,'create',[name,secret()]).status).toBe(400);
 sql.query("UPDATE sessions SET scope='read' WHERE token=?").run(owner.token);expect(call(owner,'create',['Read-only',secret()]).status).toBe(403);sql.query("UPDATE sessions SET scope='self' WHERE token=?").run(owner.token);
 sql.exec("CREATE TRIGGER fail_grant BEFORE INSERT ON institution_grants BEGIN SELECT RAISE(ABORT,'injected'); END");expect(call(owner,'create',['Atomic refusal',secret()]).status).toBe(503);expect(sql.query('SELECT count(*) n FROM institution_origins').get().n).toBe(0);sql.exec('DROP TRIGGER fail_grant');
 for(const input of [encode([1,owner.token,'list',[]]),Buffer.concat([encode([1,owner.token,'list',['']]),Buffer.from([0])]),Buffer.alloc(8193)]){const r=cli(['organization-stdin'],input);expect(r.exitCode).not.toBe(0);expect(decode(r.stdout)[1]).toBeGreaterThanOrEqual(400);}
}));
test('invitation creation grants nothing, keeps only its digest, and requires intended-account acceptance',()=>fixture(({call,owner,user,stranger,create,sql})=>{
 const {id}=create(),token=secret();const invite=call(owner,'invite',[id,user.email,'write','24',token]);expect(invite.status).toBe(201);expect(sql.query('SELECT count(*) n FROM institution_grants').get().n).toBe(1);expect(call(owner,'invite',[id,user.email,'write','24',token]).status).toBe(200);expect(call(owner,'invite',[id,user.email,'write','48',token]).status).toBe(409);
 expect(JSON.stringify(sql.query('SELECT * FROM institution_invitations').all())).not.toContain(token);expect(call(stranger,'review',[token]).status).toBe(403);expect(call(user,'review',[token]).rows[0][0]).toBe(id);expect(sql.query('SELECT count(*) n FROM institution_grants').get().n).toBe(1);
 expect(call(user,'accept',[token]).status).toBe(200);expect(call(user,'accept',[token]).status).toBe(200);expect(sql.query('SELECT count(*) n FROM institution_grants').get().n).toBe(2);expect(call(stranger,'list',['']).rows).toEqual([]);expect(call(user,'list',['']).rows[0][2]).toBe('write');
 sql.query('UPDATE institution_grants SET active=0 WHERE user_id=?').run(user.id);expect(call(user,'accept',[token]).status).toBe(409);expect(sql.query('SELECT active FROM institution_grants WHERE user_id=?').get(user.id).active).toBe(0);
}));
test('invited new account signs up after the invite and still must possess and accept its exact secret',()=>fixture(({call,owner,create,signup,sql})=>{
 const {id}=create(),token=secret();expect(call(owner,'invite',[id,'new@example.test','read','24',token]).status).toBe(201);const newcomer=signup('new@example.test');expect(call(newcomer,'review',[secret()]).status).toBe(403);expect(call(newcomer,'list',['']).rows).toEqual([]);expect(call(newcomer,'accept',[token]).status).toBe(200);expect(call(newcomer,'list',['']).rows[0][2]).toBe('read');
}));
test('invitation revocation uses its reviewed revision and cannot cancel an accepted membership',()=>fixture(({call,owner,user,create,sql})=>{
 const {id}=create(),token=secret();call(owner,'invite',[id,user.email,'read','24',token]);const page=call(owner,'invitations',[id,'']);expect(page.rows[0][0]).toBe(hash(token));expect(JSON.stringify(page)).not.toContain(token);expect(call(owner,'revoke',[id,hash(token),'2']).status).toBe(409);expect(call(owner,'revoke',[id,hash(token),'1']).status).toBe(200);expect(call(user,'accept',[token]).status).toBe(403);
 const next=secret();call(owner,'invite',[id,user.email,'read','24',next]);call(user,'accept',[next]);expect(call(owner,'revoke',[id,hash(next),'2']).status).toBe(409);expect(sql.query('SELECT active FROM institution_grants WHERE user_id=?').get(user.id).active).toBe(1);
}));
test('expired invitations and disabled or revoked issuers cannot issue membership',()=>fixture(({call,owner,user,create,sql})=>{
 const {id}=create();for(const fault of ['expired','revoked','inactive']){const token=secret();sql.query('UPDATE users SET active=1 WHERE id=?').run(owner.id);sql.query('UPDATE institution_grants SET active=1 WHERE institution=?').run(id);call(owner,'invite',[id,user.email,'admin','24',token]);
 if(fault==='expired')sql.query("UPDATE institution_invitations SET expires_at='2000-01-01T00:00:00Z' WHERE digest=?").run(hash(token));if(fault==='revoked')sql.query('UPDATE institution_grants SET active=0 WHERE institution=?').run(id);if(fault==='inactive')sql.query('UPDATE users SET active=0 WHERE id=?').run(owner.id);
 expect(call(user,'accept',[token]).status).toBe(403);expect(sql.query('SELECT count(*) n FROM institution_grants WHERE user_id=?').get(user.id).n).toBe(0);}
}));
test('delegated memberships cannot outlive either the original or current issuer grant',()=>fixture(({call,owner,user,create,sql,grant})=>{
 const {id}=create(),tomorrow=new Date(Date.now()+86400000).toISOString().slice(0,19)+'Z',earlier=new Date(Date.now()+3600000).toISOString().slice(0,19)+'Z';grant(id,owner,'admin',tomorrow);const token=secret();expect(call(owner,'invite',[id,user.email,'write','168',token]).rows[0][3]).toBe(tomorrow);grant(id,owner,'admin',earlier);expect(call(user,'accept',[token]).status).toBe(200);expect(sql.query('SELECT expires_at FROM institution_grants WHERE user_id=?').get(user.id).expires_at).toBe(earlier);
}));
test('new organization operations do not bootstrap permissions for existing provisioned institutions',()=>fixture(({call,owner,user,sql,create})=>{
 expect(call(owner,'invite',['bonfyre',user.email,'admin','24',secret()]).status).toBe(403);const {id}=create();call(owner,'invite',[id,user.email,'read','24',secret()]);expect(call(user,'invitations',[id,'']).status).toBe(403);expect(call(user,'invite',[id,owner.email,'admin','24',secret()]).status).toBe(403);
}));
