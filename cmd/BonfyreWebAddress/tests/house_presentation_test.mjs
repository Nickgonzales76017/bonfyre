import { test, beforeAll, afterAll, expect } from 'bun:test';
import { copyFileSync } from 'node:fs';
import { join } from 'node:path';
import { fixture, root } from './auth_continuation_fixture.mjs';
let f;
beforeAll(async () => {
  f = await fixture();
  copyFileSync(join(root, 'cmd/BonfyreCMS/content-types/presentation_template.json'), join(f.home, 'schemas/presentation_template.json'));
  f.run(f.cms, ['schema', 'migrate', '--db', f.db, '--schemas', join(f.home, 'schemas')]);
  for (const territory of ['life', 'live', 'work', 'shop']) {
    const profile = {
      template: 'house-' + territory + '-scope-test', territory_affinity: territory,
      reference_source: 'native-test', reference_name: 'Isolated house scope', reference_url: 'https://example.test/house',
      reference_categories: 'test', family: 'editorial', renderer_mix: 'publication', density: 'comfortable',
      hero_mode: 'lead', navigation_mode: 'header', media_mode: 'source', typography_mode: 'editorial', card_mode: 'source', chrome_mode: 'quiet',
      semantic_roles: {}, layout_tokens: {ground:'#f9f0e1',ink:'#18251e',display:'#152019',accent:'#126044',muted:'#47594a',rule:'#ccd6ce',panel:'#ffffff',field:'#144e42',field_ink:'#ffffff',night:'#10221d',night_ink:'#ffffff',face_body:'Georgia,serif'},
      acceptance_contract: 'Test-only presentation; source identity and source material are unchanged.',
      cannot_infer: 'A house profile neither changes the subject nor creates content or authority.', status: 'published'
    };
    f.run(f.cms, ['entry','create','presentation_template',JSON.stringify(profile),'--ns','root','--db',f.db,'--schemas',join(f.home,'schemas')]);
    if(territory==='life') {
      const individual={...profile,template:'individual-token-scope-test',territory_affinity:'independent',layout_tokens:{metadata:{ground:'#ce0000',face_body:'Wrong family'},...profile.layout_tokens,face_body:'"Source Serif",serif',display_weight:600}};
      f.run(f.cms,['entry','create','presentation_template',JSON.stringify(individual),'--ns','root','--db',f.db,'--schemas',join(f.home,'schemas')]);
    }
    f.run(f.web, ['domain','--db',join(f.home,'matrix.db'),'--host','reader.aurekai.'+territory,'--territory',territory,'--scope','public','--world','live','--repr','HTML','--evidence','fixture:house-scope']);
  }
}, 30000);
afterAll(async () => { await f?.close(); });
for (const territory of ['life', 'live', 'work', 'shop']) {
  test(territory+' apex uses its house without changing the retained story', async () => {
    const page = await f.request('/discover', {host:'aurekai.'+territory});
    expect(page.status).toBe(200);
    expect(page.body).toContain('data-bf-profile="house-'+territory+'-scope-test"');
    expect(page.body).toContain('data-bf-band="paper"');
    expect(page.body).toContain('--w-ground:#f9f0e1');
    expect(page.body).toContain('City opens a new library');
    expect(page.body).toContain(f.path);
  });
  test(territory+' personal subdomain does not inherit apex house bands', async () => {
    const page = await f.request('/discover', {host:'reader.aurekai.'+territory});
    expect(page.status).toBe(200);
    expect(page.body).not.toContain('data-bf-band=');
    expect(page.body).not.toContain('data-bf-profile="house-');
    expect(page.body).toContain('City opens a new library');
    expect(page.body).toContain(f.path);
  });
}
test('different host presentations are not mixed by the anonymous cache', async () => {
  const a = await f.request('/discover',{host:'aurekai.life'});
  const b = await f.request('/discover',{host:'reader.aurekai.life'});
  const c = await f.request('/discover',{host:'aurekai.live'});
  expect(a.body).toContain('house-life-scope-test');
  expect(b.body).not.toContain('data-bf-band=');
  expect(c.body).toContain('house-live-scope-test');
  expect(c.body).not.toContain('data-bf-profile="house-life-scope-test"');
});

test('nested metadata never overrides the personal profile and quoted fonts survive native projection',async()=>{
  const page=await f.request('/discover',{host:'reader.aurekai.life'});
  expect(page.body).toContain('data-bf-profile="individual-token-scope-test"');
  expect(page.body).toContain('--w-face-body:"Source Serif",serif;');
  expect(page.body).toContain('--w-display-weight:600;');
  expect(page.body).not.toContain('--w-ground:#ce0000');expect(page.body).not.toContain('Wrong family');
});
