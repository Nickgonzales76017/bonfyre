import { test, expect } from 'bun:test';
import {
  loadConfig, reconcileAuthHosts, reconcileMemberRoutes,
  reconcilePublicRoots, reconcileTunnel,
  memberReadRedirectPlan,
  reconcileMemberReadRedirects,
} from '../cloudflare-reconcile.mjs';

const config = loadConfig();
const target = `${config.tunnelID}.cfargotunnel.com`;
const roots = new Map(config.territories.map((t, i) => [t.root, `z${i}`]));

function fixture({ dns = {}, routes = {}, tunnel = [] } = {}) {
  const writes = [];
  const api = async (method, path, body) => {
    if (method !== 'GET') { writes.push({ method, path, body }); return {}; }
    if (path === '/zones?per_page=50') return [...roots].map(([name, id]) => ({ name, id }));
    if (path.includes('/cfd_tunnel/')) return { config: { ingress: structuredClone(tunnel) } };
    const id = /^\/zones\/([^/]+)/.exec(path)?.[1];
    if (!id) throw new Error(`unhandled path ${path}`);
    const name = new URL(`https://fixture${path}`).searchParams.get('name');
    if (name) return structuredClone(dns[name] ?? []);
    if (path.endsWith('/workers/routes')) return structuredClone(routes[id] ?? []);
    throw new Error(`unhandled path ${path}`);
  };
  return { api, writes };
}

test('CUE owns four territory/member bindings and distinct work OAuth sibling', () => {
  expect(config.territories).toHaveLength(4);
  expect(config.territories.find(t => t.root === 'aurekai.work').member).toBe('signin.aurekai.work');
});

test('member read plan preserves login redirect semantics and excludes native auth and machine hosts', () => {
  const plans=memberReadRedirectPlan(config);
  expect(plans).toHaveLength(1);expect(plans[0].root).toBe('aurekai.life');
  const rule=plans[0].rule;
  expect(rule.expression).toContain('ends_with(http.host, ".aurekai.life")');
  expect(rule.expression).toContain('not http.host in {"auth.aurekai.life"}');
  expect(rule.expression).toContain('http.request.method in {"GET" "HEAD"}');
  expect(rule.expression).toContain('http.request.uri.path eq "/login" or starts_with(http.request.uri.path, "/login/")');
  expect(rule.expression).not.toContain('"/login*"');
  expect(rule.action_parameters.from_value).toEqual({target_url:{expression:'concat("https://auth.aurekai.life", http.request.uri.path)'},status_code:302,preserve_query_string:true});
  const work=memberReadRedirectPlan({...config,memberReadRedirects:[{root:'aurekai.work',paths:['/login']}]})[0].rule;
  expect(work.expression).toContain('"signin.aurekai.work" "mcp.aurekai.work" "auth.aurekai.work"');
  expect(()=>memberReadRedirectPlan({...config,memberReadRedirects:[{root:'other.example',paths:['/login']}]})).toThrow('territory');
  expect(()=>memberReadRedirectPlan({...config,memberReadRedirects:[{root:'aurekai.life',paths:['/']}]})).toThrow('paths');
  expect(()=>memberReadRedirectPlan({...config,memberReadRedirects:[...config.memberReadRedirects,...config.memberReadRedirects]})).toThrow('duplicate');
});

test('redirect reconciliation is idempotent, preserves unrelated rules and refuses owned drift', async () => {
  let current=null;const writes=[];
  const api=async(method,path,body)=>{
    if(method!=='GET'){writes.push({method,path,body});return {};}
    if(path==='/zones?per_page=50')return [...roots].map(([name,id])=>({name,id}));
    if(path==='/zones/z0/rulesets')return current?[{id:'existing',kind:'zone',phase:'http_request_dynamic_redirect'}]:[];
    if(path==='/zones/z0/rulesets/existing')return current;
    throw Error('unexpected request '+path);
  };
  const plan=await reconcileMemberReadRedirects(api,config);
  expect(plan.planned).toBe(1);expect(writes).toHaveLength(0);
  current={id:'existing',rules:[{ref:'unrelated',action:'redirect',expression:'true'}]};
  await reconcileMemberReadRedirects(api,config,true);
  expect(writes).toHaveLength(1);expect(writes[0].path).toBe('/zones/z0/rulesets/existing/rules');
  expect(writes[0].body.ref).toBe('bonfyre_member_read_aurekai_life');
  current.rules.push(memberReadRedirectPlan(config)[0].rule);
  expect((await reconcileMemberReadRedirects(api,config,true)).present).toBe(1);expect(writes).toHaveLength(1);
  current.rules[1].enabled=false;
  await expect(reconcileMemberReadRedirects(api,config,true)).rejects.toThrow('drift');expect(writes).toHaveLength(1);
});

test('auth hosts and wildcard profiles dry run does not mutate; apply creates absent and updates wrong CNAME', async () => {
  const first = config.territories[0].member;
  const f = fixture({ dns: { [first]: [{ id: 'r1', type: 'CNAME', content: 'old', proxied: true }] } });
  expect(await reconcileAuthHosts(f.api, config)).toEqual({ apply: false, created: 9, updated: 1, present: 0, target });
  expect(f.writes).toHaveLength(0);
  expect((await reconcileAuthHosts(f.api, config, true)).updated).toBe(1);
  expect(f.writes).toHaveLength(10);
  const wildcards=f.writes.filter(row=>row.body?.name?.startsWith('*.aurekai.'));
  expect(wildcards).toHaveLength(4);
  expect(new Set(wildcards.map(row=>row.body.name))).toEqual(new Set(config.territories.map(t=>'*.'.concat(t.root))));
  for(const row of wildcards)expect(row.body).toEqual({type:'CNAME',name:row.body.name,content:target,proxied:true,ttl:1});
});

test('wildcard DNS refuses an unexpected A record rather than replacing the host', async()=>{
  const root=config.territories[0].root,name='*.'+root;
  const f=fixture({dns:{[name]:[{id:'occupied',type:'A',content:'192.0.2.1',proxied:false}]}});
  await expect(reconcileAuthHosts(f.api,config,true)).rejects.toThrow('non-CNAME');
  expect(f.writes).toHaveLength(0);
});

test('member routes remove only legacy null-script routes, never foreign worker', async () => {
  const t = config.territories[0];
  const f = fixture({ routes: { z0: [
    { id: 'legacy', pattern: `${t.root}/login*`, script: null },
    { id: 'foreign', pattern: `${t.root}/signup*`, script: 'other' },
    { id: 'member', pattern: `${t.member}/*`, script: null },
  ] } });
  expect(await reconcileMemberRoutes(f.api, config)).toEqual({ apply: false, created: 5, present: 1, removed: 1, patterns: 6 });
  expect(f.writes).toHaveLength(0);
  await reconcileMemberRoutes(f.api, config, true);
  expect(f.writes.filter(w => w.method === 'DELETE').map(w => w.path)).toEqual(['/zones/z0/workers/routes/legacy']);
});

test('public apex refuses to replace Worker without explicit override', async () => {
  const dns = Object.fromEntries(config.territories.map(t => [t.root, [{ id: `r-${t.root}`, type: 'CNAME', content: target, proxied: true }]]));
  const f = fixture({ dns, routes: { z0: [{ id: 'worker', pattern: 'aurekai.life/*', script: 'aurekai-estate' }] } });
  await expect(reconcilePublicRoots(f.api, config)).rejects.toThrow('refusing to replace');
  expect(f.writes).toHaveLength(0);
  expect((await reconcilePublicRoots(f.api, config, false, true)).routes_created).toBe(4);
  expect(f.writes).toHaveLength(0);
});

test('public apex ignores coexisting mail records but rejects competing address records', async () => {
  const dns = Object.fromEntries(config.territories.map(t => [t.root, [
    {id:'cname-'+t.root,type:'CNAME',content:target,proxied:true},
    {id:'mx-'+t.root,type:'MX',content:'mail.example',proxied:false},
    {id:'txt-'+t.root,type:'TXT',content:'v=spf1 -all',proxied:false},
  ]]));
  const f = fixture({dns});
  expect(await reconcilePublicRoots(f.api, config)).toEqual({
    apply:false,dns_changed:0,routes_created:4,routes_present:0,roots:4,
  });
  expect(f.writes).toHaveLength(0);
  dns[config.territories[0].root].push({id:'other',type:'A',content:'192.0.2.1'});
  const conflict = fixture({dns});
  await expect(reconcilePublicRoots(conflict.api, config)).rejects.toThrow('requires exactly one CNAME');
  expect(conflict.writes).toHaveLength(0);
});

test('tunnel keeps unrelated ingress and only changes configured public hosts', async () => {
  const f = fixture({ tunnel: [
    { hostname: 'aurekai.life', service: 'http://old' },
    { hostname: 'control.example', service: 'http://control' },
    { hostname: config.gateway.host, path: config.gateway.path, service: config.gateway.service },
    { hostname: config.gateway.host, service: config.nativeService },
  ] });
  expect(await reconcileTunnel(f.api, config, 'account')).toEqual({ apply: false, changed: ['aurekai.life'], ingress_rules: 4 });
  expect(f.writes).toHaveLength(0);
  await reconcileTunnel(f.api, config, 'account', true);
  expect(f.writes).toHaveLength(1);
  expect(f.writes[0].body.config.ingress[1].service).toBe('http://control');
  expect(f.writes[0].body.config.ingress[2].service).toBe(config.gateway.service);
});

test('tunnel splits the gateway paths off the apex ahead of its catch-all', async () => {
  const f = fixture({ tunnel: [
    { hostname: 'mcp.aurekai.work', service: 'http://127.0.0.1:8791' },
    { hostname: config.gateway.host, service: config.nativeService },
    { service: 'http_status:404' },
  ] });
  expect((await reconcileTunnel(f.api, config, 'account')).changed).toEqual([`${config.gateway.host} gateway paths`]);
  expect(f.writes).toHaveLength(0);
  await reconcileTunnel(f.api, config, 'account', true);
  const ingress = f.writes[0].body.config.ingress;
  expect(ingress).toHaveLength(4);
  expect(ingress[1]).toEqual({ hostname: config.gateway.host, path: config.gateway.path, service: config.gateway.service });
  expect(ingress[2]).toEqual({ hostname: config.gateway.host, service: config.nativeService });
  for (const allowed of ['/oauth/authorize', '/oauth/token', '/mcp', '/auth.md', '/.well-known/oauth-authorization-server', '/.well-known/oauth-protected-resource/mcp'])
    expect(new RegExp(config.gateway.path).test(allowed)).toBe(true);
  for (const refused of ['/', '/mcp/resources/', '/oauth/authorize/extra', '/account'])
    expect(new RegExp(config.gateway.path).test(refused)).toBe(false);
});
