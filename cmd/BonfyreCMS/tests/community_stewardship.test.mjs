import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes,createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..'),binary=process.env.BONFYRE_TEST_CMS_BINARY??join(root,'cmd/BonfyreCMS/bonfyre-cms'),schemas=join(root,'cmd/BonfyreCMS/content-types'),nonce=()=>randomBytes(16).toString('hex');
const digest=(id,parts)=>createHash('sha256').update(id+';'+parts.map(p=>Buffer.byteLength(p)+':'+p).join('')).digest('hex');
function fixture(run){const dir=mkdtempSync(join(tmpdir(),'community-steward-')),db=join(dir,'cms.db');
 const work=(actor,operation,{community='',target='',expected='',request='',title='',text=''}={})=>{const r=Bun.spawnSync([binary,'entry','community-work','--actor',actor,'--operation',operation,'--db',db,'--schemas',schemas],{stdout:'pipe',stderr:'pipe',stdin:Buffer.from(encode([1,community,target,expected,request,title,text])),timeout:10000});const [version,state,communityOut,targetOut,outDigest,message]=decode(r.stdout);expect(version).toBe(1);return {code:r.exitCode,state,community:communityOut,target:targetOut,digest:outDigest,message};};
 const sql=new Database(db);
 const memberDigest=(community,person)=>{const m=sql.query('SELECT id,membership,member_subject,role,state,status,COALESCE(expires_at,\'\') e FROM community_membership WHERE namespace=\'root\' AND community=? AND member_subject=?').get(community,person);return digest(m.id,[m.membership,m.member_subject,m.role,m.state,m.status,m.e]);};
 const communityDigest=community=>{const c=sql.query('SELECT id,community,title,COALESCE(description,\'\') d,COALESCE(created_by_subject,\'\') o,status,visibility,COALESCE(moderation_policy,\'\') p FROM community_body WHERE namespace=\'root\' AND community=?').get(community);return digest(c.id,[c.community,c.title,c.d,c.o,c.status,c.visibility,c.p]);};
 try{return run({dir,db,sql,work,memberDigest,communityDigest});}finally{sql.close();rmSync(dir,{recursive:true,force:true});}}
function setup(work,steward,member){
 const created=work(steward,'create',{request:nonce(),title:'Harbor Guild',text:'A guild for testing stewardship transfer end to end.'});
 expect(created.code).toBe(0);expect(created.state).toBe('committed');
 const key=created.community;expect(key).toMatch(/^community-[a-f0-9]{64}$/);
 expect(work(member,'request',{community:key,expected:'absent'}).state).toBe('committed');
 return key;
}
test('stewardship passes to an active member and the previous steward stays a member',()=>fixture(({work,sql,memberDigest,communityDigest})=>{
 const steward='user:11',member='user:12',key=setup(work,steward,member);
 const join=work(steward,'accept',{community:key,target:member,expected:memberDigest(key,member)});
 expect(join.state).toBe('committed');
 const before=communityDigest(key);
 const done=work(steward,'transfer',{community:key,target:member,expected:before});
 expect(done.code).toBe(0);expect(done.state).toBe('committed');
 expect(done.target).toBe(member);expect(done.digest).not.toBe(before);
 expect(sql.query('SELECT created_by_subject o FROM community_body WHERE community=?').get(key).o).toBe(member);
 expect(sql.query('SELECT role,state FROM community_membership WHERE community=? AND member_subject=?').get(key,member)).toEqual({role:'steward',state:'active'});
 expect(sql.query('SELECT role,state FROM community_membership WHERE community=? AND member_subject=?').get(key,steward)).toEqual({role:'member',state:'active'});
 const again=work(member,'transfer',{community:key,target:steward,expected:done.digest});
 expect(again.state).toBe('committed');
 expect(sql.query('SELECT created_by_subject o FROM community_body WHERE community=?').get(key).o).toBe(steward);
}));
test('transfer refuses non-stewards, non-members, stale digests and self transfer',()=>fixture(({work,sql,memberDigest,communityDigest})=>{
 const steward='user:11',member='user:12',outsider='user:13',key=setup(work,steward,member);
 work(steward,'accept',{community:key,target:member,expected:memberDigest(key,member)});
 const d=communityDigest(key);
 expect(work(member,'transfer',{community:key,target:member,expected:d}).state).toBe('forbidden');
 expect(work(outsider,'transfer',{community:key,target:member,expected:d}).state).toBe('forbidden');
 expect(work(steward,'transfer',{community:key,target:outsider,expected:d}).state).toBe('forbidden');
 expect(work(steward,'transfer',{community:key,target:steward,expected:d}).state).toBe('invalid');
 expect(work(steward,'transfer',{community:key,target:member,expected:'0'.repeat(64)}).state).toBe('conflict');
 expect(sql.query('SELECT created_by_subject o FROM community_body WHERE community=?').get(key).o).toBe(steward);
}));
