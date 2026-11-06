import assert from 'node:assert/strict';
import {workspaceFixture} from './workspace_fixture.mjs';
const f=await workspaceFixture(),checks=[];
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const path='/households/access',household='workspace-home',context='household:'+household;
const query=user=>path+'?'+new URLSearchParams({household,member:'user:'+user.id});
async function review(user=f.writer,caller=f.owner){
 const response=await f.raw(query(user),caller),text=await response.text();
 assert.equal(response.status,200,text);
 const form=text.match(/<form[^>]*data-household-access-form[^>]*>([\s\S]*?)<\/form>/)?.[1];assert.ok(form,text);
 return new URLSearchParams([...form.matchAll(/name="([^"]+)" value="([^"]*)"/g)].map(m=>[m[1],m[2].replaceAll('&amp;','&')]));
}
async function apply(form,role,caller=f.owner,headers={}){
 const input=new URLSearchParams(form);input.set('role',role);
 return f.raw(path,caller,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded',...headers},body:input.toString()});
}
async function grant(user,role){const response=await apply(await review(user),role);assert.equal(response.status,303,await response.text());}
try{
 const binding=await f.install('catalogue',context),writer=f.store(binding,f.writer),owner=f.store(binding);
 check('organization access alone never opens the household',(await f.raw(binding.path,f.writer)).status===403);
 check('nonmembers cannot enumerate household membership or accounts',(await f.raw(query(f.reader),f.other)).status===403);
 const email=await f.raw(path+'?'+new URLSearchParams({household,email:'workspace-writer@example.test'}));
 check('steward reviews a resolved existing account before changing it',(await email.text()).includes('data-household-access-form'));
 const initial=await review();await grant(f.writer,'read');
 check('a household reader opens its actual native editor',(await f.raw(binding.path,f.writer)).status===200);
 await assert.rejects(()=>writer.saveLocal('fixture-plan:permission',{id:'permission'},0),/access|Sign in/);
 check('reader access does not enable draft writes',(await owner.readLocal()).length===0);
 const readerPage=await f.page('rooms',context,f.writer);
 check('a household reader cannot install another configuration',!readerPage.body.includes('value="install"'));
 check('stale initial membership review is refused',(await apply(initial,'admin')).status===409);
 await grant(f.writer,'write');await writer.saveLocal('fixture-plan:permission',{id:'permission',title:'Shared household plan'},0);
 check('the authorized household writer saves a destination-owned plan',(await owner.readLocal())[0].value.title==='Shared household plan');
 const picker=await f.page('catalogue','self',f.writer);
 check('the granted household appears in the member destination picker',picker.body.includes('household:workspace-home'));
 const fresh=await review();
 check('an organization administrator cannot grant household membership',(await apply(fresh,'admin',f.other)).status===403);
 check('a cross-origin access form is rejected',(await apply(fresh,'admin',f.owner,{Origin:'https://other.example'})).status===403);
 await grant(f.writer,'admin');const rooms=await f.install('rooms',context,f.writer);
 check('a delegated household admin installs the next specialized product',(await f.store(rooms,f.writer).readLocal())[0].value.id==='permission');
 check('delegated configuration administration cannot grant more members',(await apply(await review(f.reader),'write',f.writer)).status===403);
 await grant(f.writer,'none');
 check('revocation closes both previously opened product bindings',(await f.raw(binding.endpoint,f.writer)).status===403&&(await f.raw(rooms.endpoint,f.writer)).status===403);
 check('revocation leaves saved household work intact',(await owner.readLocal())[0].value.id==='permission');
 await grant(f.writer,'write');
 f.sql.run("UPDATE _household_membership SET expires_at=1 WHERE member=?",'user:'+f.writer.id);
 check('expired membership is refused on the next editor request',(await f.raw(binding.path,f.writer)).status===403);
 await grant(f.writer,'none');check('expired access can still be revoked',f.sql.query('SELECT role FROM _household_membership WHERE member=?').get('user:'+f.writer.id).role==='none');
 await grant(f.reader,'read');const staleAuthority=await review(f.reader);
 f.sql.run("UPDATE household SET steward=? WHERE household_id=?",'user:'+f.other.id,household);
 check('changing the steward invalidates prior delegated access',(await f.raw(binding.path,f.reader)).status===403);
 f.sql.run("UPDATE household SET steward=? WHERE household_id=?",'user:'+f.owner.id,household);
 check('restoring an earlier steward cannot revive old grants',(await f.raw(binding.path,f.reader)).status===403);
 check('a review from the former authority generation cannot grant access',(await apply(staleAuthority,'write')).status===409);
 await grant(f.reader,'read');check('the current steward can deliberately re-admit a member',(await f.raw(binding.path,f.reader)).status===200);
 f.sql.run("UPDATE household SET status='draft' WHERE household_id=?",household);f.sql.run("UPDATE household SET status='published' WHERE household_id=?",household);
 check('withdrawing then republishing the household cannot resurrect membership',(await f.raw(binding.path,f.reader)).status===403);
 check('duplicate household fields are not silently selected',(await f.raw(path+'?household=workspace-home&household=other')).status===400);
 check('forged actor fields cannot select the acting principal',(await f.raw(query(f.reader)+'&actor=user:1')).status===400);
 const counts=f.authsql.query('SELECT count(*) n FROM users').get().n;
 const missing=await f.raw(path+'?'+new URLSearchParams({household,email:'absent@example.test'}));
 check('unknown accounts stay a refusal rather than implicit provisioning',(await missing.text()).includes('No existing account')&&f.authsql.query('SELECT count(*) n FROM users').get().n===counts);
 console.log('HOUSEHOLD_MEMBERSHIP_PASS\t'+checks.length);
}finally{await f.close();}
