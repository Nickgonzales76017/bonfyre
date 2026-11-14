import {beforeAll,afterAll,test,expect} from 'bun:test';
import {packSetupFixture,decode} from './pack_setup_fixture.mjs';

let f,owner,outsider,reader,organization;
const secure={Origin:'https://aurekai.life'};
const inputs=(html,marker)=>{
 const form=[...html.matchAll(/<form\b([^>]*)>([\s\S]*?)<\/form>/g)].find(m=>m[1].includes(marker));
 if(!form)throw Error('Missing reviewed form '+marker);
 return Object.fromEntries([...form[2].matchAll(/<input\b[^>]*name="([^"]+)"[^>]*value="([^"]*)"/g)].map(m=>[decode(m[1]),decode(m[2])]));
};
beforeAll(async()=>{
 f=await packSetupFixture();
 owner=await f.login('subaccount-founder@example.test');
 outsider=await f.login('subaccount-outsider@example.test');
 reader=await f.login('subaccount-reader@example.test');
 const home=await f.request('/organizations',{cookie:owner.cookie});
 const create=await f.request('/organizations',{cookie:owner.cookie,headers:secure,form:{...inputs(home.body,'data-org-create'),name:'Publication collective'}});
 expect(create.status).toBe(200);
 organization=f.authsql.query("SELECT institution FROM institution_origins WHERE label='Publication collective'").get().institution;
 f.grant(reader.id,organization,'read');
},60000);
afterAll(async()=>{await f?.close()},30000);

test('the live native organization view shows its own reviewed subaccount form',async()=>{
 const page=await f.request('/organizations?organization='+organization,{cookie:owner.cookie});
 expect(page.status).toBe(200);
 expect(page.body).toContain('data-org-subaccount-creation');
 expect(page.body).toContain('data-org-subaccounts');
 expect(page.body).toContain('data-org-subaccount-create');
 expect(page.body).not.toContain('data-org-subaccount="sub-');
});
test('creating a child uses Auth, retains parent and does not mint independent membership',async()=>{
 const page=await f.request('/organizations?organization='+organization,{cookie:owner.cookie});
 const form={...inputs(page.body,'data-org-subaccount-create'),name:'Editorial journal'};
 const created=await f.request('/organizations',{cookie:owner.cookie,headers:secure,form});
 expect(created.status).toBe(200);
 expect(created.body).toContain('Child workspace recorded without inherited permissions or publication authority.');
 const row=f.authsql.query('SELECT subaccount,institution,label FROM institution_subaccounts WHERE institution=?').get(organization);
 expect(row.label).toBe('Editorial journal');
 expect(row.institution).toBe(organization);
 expect(row.subaccount).toMatch(/^sub-[a-f0-9]{32}$/);
 expect(created.body).toContain('data-org-subaccount="'+row.subaccount+'"');
 expect(f.authsql.query('SELECT count(*) n FROM institution_grants WHERE institution=?').get(row.subaccount).n).toBe(0);
 const replay=await f.request('/organizations',{cookie:owner.cookie,headers:secure,form});
 expect(replay.status).toBe(200);
 expect(f.authsql.query('SELECT count(*) n FROM institution_subaccounts WHERE institution=?').get(organization).n).toBe(1);
 const mismatch=await f.request('/organizations',{cookie:owner.cookie,headers:secure,form:{...form,name:'Different publication'}});
 expect(mismatch.status).toBe(409);
});
test('reader can see child scope but not create it; outsider cannot see or create it',async()=>{
 const view=await f.request('/organizations?organization='+organization,{cookie:reader.cookie});
 expect(view.status).toBe(200);
 expect(view.body).toContain('data-org-subaccounts');
 expect(view.body).toContain('Editorial journal');
 expect(view.body).not.toContain('data-org-subaccount-create');
 const outsiderView=await f.request('/organizations?organization='+organization,{cookie:outsider.cookie});
 expect(outsiderView.status).toBe(403);
 const submitted=await f.request('/organizations',{cookie:reader.cookie,headers:secure,form:{operation:'subaccount-create',organization,name:'Unauthorized',nonce:'b'.repeat(64)}});
 expect(submitted.status).toBe(403);
 expect(f.authsql.query("SELECT count(*) n FROM institution_subaccounts WHERE label='Unauthorized'").get().n).toBe(0);
});
test('revoking parent role immediately removes child access',async()=>{
 f.authsql.query("UPDATE institution_grants SET active=0 WHERE institution=? AND user_id=?").run(organization,reader.id);
 expect((await f.request('/organizations?organization='+organization,{cookie:reader.cookie})).status).toBe(403);
});
