// Isolated native CMS/Auth/WebAddress. No customer sessions or publication writes.
import {beforeAll,afterAll,test,expect} from 'bun:test';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {packSetupFixture,root} from './pack_setup_fixture.mjs';
let f,owner,other;
const carrier=(title,subject_address,status)=>({
 title,subject_address,carrier:'web',payload_profile:'profile-reference',
 purpose:'fixture-only',temporal_policy:'current',audience:'self',
 authority_boundary:'self read only',proximity_policy:'none',
 security_policy:'No publication is admitted by this fixture',status
});
beforeAll(async()=>{
 f=await packSetupFixture();
 const schemas=join(f.home,'schemas');
 copyFileSync(join(root,'cmd/BonfyreCMS/content-types/physical_carrier.json'),join(schemas,'physical_carrier.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 owner=await f.login('review-owner@example.test');
 other=await f.login('review-other@example.test');
 f.add('physical_carrier',carrier('Owner draft for review','subject:review-owner-1','draft'),'actor-'+owner.id);
 f.add('physical_carrier',carrier('Another owner private draft','subject:review-other-1','held'),'actor-'+other.id);
 f.add('physical_carrier',carrier('Published owner record','subject:review-owner-published','published'),'actor-'+owner.id);
},60000);
afterAll(async()=>{await f?.close();});
test('review is a visible first-class signed-in navigation destination',async()=>{
 const page=await f.request('/account/navigation',{cookie:owner.cookie});
 expect(page.status).toBe(200);
 expect(page.body).toContain('data-account-review');
 expect(page.body).toContain('href="/account/review"');
});
test('the review desk sees only current account draft states',async()=>{
 const r=await f.request('/account/review',{cookie:owner.cookie});
 expect(r.status).toBe(200);
 expect(r.body).toContain('data-account-review');
 expect(r.body).toContain('Owner draft for review');
 expect(r.body).toContain('1 drafts');
 expect(r.body).not.toContain('Another owner private draft');
 expect(r.body).not.toContain('Published owner record');
 expect(r.headers.get('cache-control')).toContain('private');
});
test('another account sees only its own review holds',async()=>{
 const r=await f.request('/account/review',{cookie:other.cookie});
 expect(r.status).toBe(200);
 expect(r.body).toContain('Another owner private draft');
 expect(r.body).not.toContain('Owner draft for review');
});
test('no public review body or review write path is exposed',async()=>{
 expect((await f.request('/account/review')).status).toBe(403);
 expect((await f.request('/account/review?namespace=actor-'+owner.id,{cookie:owner.cookie})).status).toBe(400);
 expect((await f.request('/account/review',{cookie:owner.cookie,method:'POST'})).status).toBe(400);
 expect(f.sql.query("SELECT count(*) AS n FROM physical_carrier WHERE status='draft' AND namespace=?").get('actor-'+owner.id).n).toBe(1);
});
