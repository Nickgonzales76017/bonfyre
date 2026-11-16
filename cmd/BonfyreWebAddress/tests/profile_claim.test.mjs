// A new account claims its own address once: one handle binds user:N to
// person:handle, bootstraps Matrix subject/institution/domains/presences and
// files one site draft per territory for the existing publish review.
import {test as testCase,beforeAll,afterAll,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {fixture,root,password} from './auth_continuation_fixture.mjs';
const test=(name,run)=>testCase(name,run,45000);
let f,owner,other,sql,authsql,matrix;
async function request(path,{user=owner,host='aurekai.life',form,headers={}}={}){
 return f.request(path,{host,cookie:user?.cookie||'',form,headers:{Accept:'text/html',...(form!==undefined?{Origin:'https://'+host}:{}),...headers}});
}
async function login(email){
 await f.request('/signup',{form:{name:'Claim user',email,password}});
 const r=await f.request('/login',{form:{email,password,next:'/profile'}});
 const cookie=r.headers.get('set-cookie')?.split(';')[0];
 if(!cookie)throw Error('Native login failed');
 return {cookie,id:authsql.query('SELECT id FROM users WHERE email=?').get(email).id};
}
beforeAll(async()=>{
 f=await fixture();
 const schemas=join(f.home,'schemas');
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_presentation.json'),join(schemas,'site_presentation.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 sql=new Database(f.db);authsql=new Database(f.authdb);matrix=new Database(join(f.home,'matrix.db'));
 owner=await login('claim-owner@example.test');other=await login('claim-other@example.test');
},60000);
afterAll(async()=>{sql?.close();authsql?.close();matrix?.close();await f?.close();},30000);
test('a siteless account is offered the claim form, not the setup loop',async()=>{
 const r=await request('/profile?tab=sites');
 expect(r.status).toBe(200);expect(r.body).toContain('data-profile-claim="form"');
 expect(r.body).toContain('Claim your address');
 expect(r.body).not.toContain('/account#presence');
});
test('claim validates, binds, bootstraps and files drafts, then waits for review',async()=>{
 expect((await request('/profile/claim',{form:{handle:'Bad Handle!'}})).status).toBe(400);
 const claimed=await request('/profile/claim',{form:{handle:'riverstone'}});
 expect(claimed.status).toBe(303);
 expect(claimed.headers.get('location')).toBe('/profile?tab=sites');
 expect(authsql.query("SELECT actor_subject AS a FROM actor_closure_bindings WHERE user_id=?").get(owner.id).a).toBe('person:riverstone');
 expect(matrix.query("SELECT count(*) AS n FROM matrix_domains WHERE hostname LIKE 'riverstone.aurekai.%'").get().n).toBe(4);
 expect(matrix.query("SELECT count(*) AS n FROM matrix_presences WHERE subject_id='person:riverstone' AND active=1").get().n).toBe(4);
 expect(sql.query("SELECT count(*) AS n FROM site_presentation WHERE subject_address='person:riverstone' AND status='draft'").get().n).toBe(4);
 const pending=await request('/profile?tab=sites');
 expect(pending.body).toContain('waiting for review');
 expect(pending.body).not.toContain('Set up your profile');
 const hero=await request('/profile');
 expect(hero.body).toContain('Your address is waiting for review');
});
test('claim is one-time per account and one-winner per handle',async()=>{
 expect((await request('/profile/claim',{form:{handle:'riverstone'}})).status).toBe(303);
 expect((await request('/profile/claim',{form:{handle:'otherplace'}})).status).toBe(409);
 expect((await request('/profile/claim',{user:other,form:{handle:'riverstone'}})).status).toBe(409);
 expect((await request('/profile/claim',{user:null,form:{handle:'anonclaim'}})).status).toBe(401);
 const rival=await request('/profile/claim',{user:other,form:{handle:'nightmarket'}});
 expect(rival.status).toBe(303);
});
test('publishing the drafts opens the claimed profile',async()=>{
 sql.exec("UPDATE site_presentation SET status='published' WHERE subject_address='person:riverstone'");
 sql.exec("UPDATE site_page SET status='published' WHERE namespace LIKE 'riverstone-%'");
 const r=await request('/profile');
 expect(r.body).toContain('Write an article');
 expect(r.body).toContain('riverstone.aurekai.life');
 const pub=await request('/',{host:'riverstone.aurekai.life',user:null});
 expect(pub.status).toBe(200);
});
