#!/usr/bin/env bun
import { spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const configPath = resolve(dirname(fileURLToPath(import.meta.url)), 'cloudflare-reconcile.cue');
const apiBase = 'https://api.cloudflare.com/client/v4';

function hasControlCharacters(value) {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 32 || code === 127) return true;
  }
  return false;
}

export function loadConfig() {
  const cue = process.env.BONFYRE_CUE || Bun.which('cue') || '/opt/homebrew/bin/cue';
  const result = spawnSync(cue, ['export', configPath], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(result.stderr?.trim() || 'CUE export failed');
  return JSON.parse(result.stdout);
}

export function zoneToken(env = process.env) {
  return /Bearer\s+([A-Za-z0-9_-]+)/.exec(env.CLOUDFLARE_info_zones || '')?.[1] || env.CLOUDFLARE_API_TOKEN || '';
}

export function client(token, fetcher = fetch) {
  if (!token) throw new Error('Cloudflare credential is required');
  return async (method, path, body) => {
    let response;
    try { response = await fetcher(apiBase + path, {
      method,
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(30000),
    }); } catch { throw new Error('Cloudflare transport failed'); }
    // Do not print arbitrary response bodies: token endpoints and upstream
    // error envelopes can contain secrets. Check transport and API acceptance.
    if (!response.ok) throw new Error(`Cloudflare HTTP ${response.status}`);
    let answer;
    try { answer = await response.json(); } catch { throw new Error('Cloudflare returned invalid JSON'); }
    if (answer?.success !== true || !Object.hasOwn(answer, 'result'))
      throw new Error('Cloudflare rejected the operation');
    return answer.result;
  };
}

// The historical CLI is a mutating purge command. Exact paths remain default;
// full-zone invalidation requires the sole explicit --everything argument.
export async function purgeEstate(api, config, args, onPurged = () => {}) {
  const everything = args.length === 1 && args[0] === '--everything';
  if (!args.length || (!everything && args.some(path => typeof path !== 'string' ||
      !path || path.startsWith('--') || hasControlCharacters(path))))
    throw new Error('name paths to purge, or use --everything alone');
  const roots = config.territories.map(row => row.root);
  if (!roots.length || new Set(roots).size !== roots.length ||
      roots.some(root => !/^[a-z0-9]+(?:[.-][a-z0-9]+)*\.[a-z]{2,}$/.test(root)))
    throw new Error('invalid authored territory set');
  // Resolve ALL exact targets before the first write. An absent/ambiguous zone
  // must not produce a partial purge or a misleading successful exit.
  const targets = [];
  for (const root of roots) {
    const rows = await api('GET', `/zones?name=${encodeURIComponent(root)}`);
    if (!Array.isArray(rows) || rows.length !== 1 || rows[0].name !== root ||
        !/^[a-f0-9]{32}$/i.test(rows[0].id || ''))
      throw new Error(`expected one exact Cloudflare zone for ${root}`);
    targets.push({ root, id: rows[0].id });
  }
  if (new Set(targets.map(row => row.id)).size !== targets.length)
    throw new Error('Cloudflare zone identities are not distinct');
  for (const { root, id } of targets) {
    const body = everything ? { purge_everything: true }
      : { files: [...new Set(args.map(path => `https://${root}${path.startsWith('/') ? path : `/${path}`}`))] };
    await api('POST', `/zones/${id}/purge_cache`, body);
    onPurged(root);
  }
  return { purged: targets.length, everything };
}

export async function runConnector(api, config, env = process.env, spawn = Bun.spawn) {
  const account = env.CLOUDFLARE_ACCOUNT_ID;
  const tunnel = env.BONFYRE_CLOUDFLARE_TUNNEL_ID || config.tunnelID;
  if (!/^[a-f0-9]{32}$/i.test(account || '') ||
      !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(tunnel || ''))
    throw new Error('valid Cloudflare account and tunnel identities are required');
  const token = await api('GET', `/accounts/${account}/cfd_tunnel/${tunnel}/token`);
  if (typeof token !== 'string' || !token || token.length > 65536 || /\s/.test(token) || hasControlCharacters(token))
    throw new Error('Cloudflare returned an invalid connector token');
  const childEnv = { ...env, TUNNEL_TOKEN: token };
  // The connector needs its scoped run token, not the administrative API key.
  delete childEnv.CLOUDFLARE_API_TOKEN;
  delete childEnv.CLOUDFLARE_info_zones;
  let child;
  try {
    child = spawn([env.CLOUDFLARED_BIN || '/opt/homebrew/bin/cloudflared',
      '--no-autoupdate', '--loglevel', 'info', 'tunnel', 'run'],
    { env: childEnv, stdin: 'inherit', stdout: 'inherit', stderr: 'inherit' });
  } catch { throw new Error('could not start cloudflared'); }
  const interrupt = () => child.kill('SIGINT'), terminate = () => child.kill('SIGTERM');
  process.on('SIGINT', interrupt); process.on('SIGTERM', terminate);
  try { return await child.exited; }
  finally { process.off('SIGINT', interrupt); process.off('SIGTERM', terminate); }
}

async function zones(api, territories) {
  const rows = await api('GET', '/zones?per_page=50');
  const ids = new Map(rows.map(row => [row.name, row.id]));
  for (const { root } of territories) if (!ids.has(root)) throw new Error(`missing zone ${root}`);
  return ids;
}

const dnsPath = (id, name) => `/zones/${id}/dns_records?name=${encodeURIComponent(name)}`;
const routesPath = id => `/zones/${id}/workers/routes`;
const tunnelTarget = config => `${config.tunnelID}.cfargotunnel.com`;
const desiredDns = (name, config) => ({ type: 'CNAME', name, content: tunnelTarget(config), proxied: true, ttl: 1 });

export function memberReadRedirectPlan(config) {
  const seen = new Set();
  return (config.memberReadRedirects ?? []).map(({root, paths}) => {
    const territory = config.territories.find(t => t.root === root);
    if (!territory || seen.has(root) || !/^[a-z0-9.-]+$/.test(root) ||
        !territory.member.endsWith('.' + root) || !/^[a-z0-9.-]+$/.test(territory.member))
      throw new Error('invalid or duplicate member redirect territory');
    seen.add(root);
    if (!Array.isArray(paths) || !paths.length || paths.some(p => !/^\/[a-z]+$/.test(p)))
      throw new Error('invalid member redirect paths');
    const excluded = [...new Set([territory.member, ...(config.controlHosts ?? []).filter(t => t.root === root).map(t => t.member)])];
    if (excluded.some(h => !/^[a-z0-9.-]+$/.test(h) || !h.endsWith('.' + root))) throw new Error('invalid excluded host');
    const quote = JSON.stringify;
    const pathExpression = paths.map(p => `(http.request.uri.path eq ${quote(p)} or starts_with(http.request.uri.path, ${quote(p + '/')}))`).join(' or ');
    return {root, phase:'http_request_dynamic_redirect', rule:{
      ref:'bonfyre_member_read_' + root.replaceAll('.', '_'),
      description:'Send publisher login reads directly to the native session host', enabled:true, action:'redirect',
      expression:`ends_with(http.host, ${quote('.' + root)}) and not http.host in {${excluded.map(quote).join(' ')}} and http.request.method in {"GET" "HEAD"} and (${pathExpression})`,
      action_parameters:{from_value:{target_url:{expression:`concat(${quote('https://' + territory.member)}, http.request.uri.path)`},status_code:302,preserve_query_string:true}},
    }};
  });
}

export async function reconcileMemberReadRedirects(api, config, apply = false) {
  const plans = memberReadRedirectPlan(config), ids = await zones(api, config.territories);
  const operations = []; let present = 0;
  // Read every target before writing. Append only our missing rule; never PUT
  // a whole existing ruleset and accidentally replace someone else's redirects.
  for (const plan of plans) {
    const base = `/zones/${ids.get(plan.root)}/rulesets`;
    const entrypoints = (await api('GET', base)).filter(r => r.kind === 'zone' && r.phase === plan.phase);
    if (entrypoints.length > 1) throw new Error('ambiguous redirect entrypoint');
    if (!entrypoints.length) {
      operations.push({root:plan.root, method:'POST', path:base,
        body:{name:'Bonfyre member read redirects', kind:'zone', phase:plan.phase, rules:[plan.rule]}});
      continue;
    }
    const current = await api('GET', base + '/' + entrypoints[0].id);
    const matches = (current.rules ?? []).filter(r => r.ref === plan.rule.ref);
    if (matches.length > 1) throw new Error('duplicate owned redirect rule');
    if (matches.length) {
      const actual = matches[0], desired = plan.rule;
      const a = actual.action_parameters?.from_value, d = desired.action_parameters.from_value;
      if (actual.enabled !== true || actual.action !== desired.action || actual.expression !== desired.expression ||
          a?.status_code !== d.status_code || a?.preserve_query_string !== d.preserve_query_string ||
          a?.target_url?.expression !== d.target_url.expression)
        throw new Error('owned redirect rule drift; inspect before replacing');
      present++;
    } else operations.push({root:plan.root, method:'POST', path:base + '/' + current.id + '/rules', body:plan.rule});
  }
  if (apply) for (const op of operations) await api(op.method, op.path, op.body);
  return {apply, present, planned:operations.length, operations};
}

export async function reconcileAuthHosts(api, config, apply = false) {
  const ids = await zones(api, config.territories);
  const result = { apply, created: 0, updated: 0, present: 0, target: tunnelTarget(config) }, planned = [];
  // Every published member profile needs a wildcard DNS entrance, not only ingress.
  // These are still tunnel CNAMEs; a conflicting non-CNAME fails closed.
  for (const { root, member } of [...config.territories, ...(config.controlHosts ?? []), ...config.territories.map(({ root }) => ({ root, member: `*.${root}` }))]) {
    const id = ids.get(root);
    const rows = await api('GET', dnsPath(id, member));
    if (rows.length > 1) throw new Error(`more than one record exists for ${member}`);
    const desired = desiredDns(member, config);
    const row = rows[0];
    if (!row) {
      planned.push({ method: 'POST', path: `/zones/${id}/dns_records`, body: desired });
      result.created++;
    } else if (row.type === 'CNAME' && row.content === desired.content && row.proxied) {
      result.present++;
    } else {
      if (row.type !== 'CNAME') throw new Error(`${member} has non-CNAME record`);
      planned.push({ method: 'PUT', path: `/zones/${id}/dns_records/${row.id}`, body: desired });
      result.updated++;
    }
  }
  // Read every owned name and refuse conflicting ownership before first mutation.
  if (apply) for (const operation of planned) await api(operation.method, operation.path, operation.body);
  return result;
}

export async function reconcileMemberRoutes(api, config, apply = false) {
  const ids = await zones(api, config.territories);
  const control = config.controlHosts ?? [];
  const result = { apply, created: 0, present: 0, removed: 0, patterns: config.territories.length + control.length };
  for (const { root, member } of [...config.territories, ...control]) {
    const id = ids.get(root);
    const rows = await api('GET', routesPath(id));
    const byPattern = new Map(rows.map(row => [row.pattern, row]));
    // Legacy member paths belong to a territory; a control host has none to retire.
    if (!control.some(item => item.member === member)) for (const prefix of [root, `*.${root}`]) for (const suffix of config.legacyMemberPaths) {
      const row = byPattern.get(prefix + suffix);
      if (row && row.script == null) {
        if (apply) await api('DELETE', `${routesPath(id)}/${row.id}`);
        result.removed++;
      }
    }
    const pattern = `${member}/*`;
    const exact = byPattern.get(pattern);
    if (exact?.script == null && exact) result.present++;
    else if (exact) throw new Error(`${pattern} is bound to ${exact.script}; refusing to replace it`);
    else {
      if (apply) await api('POST', routesPath(id), { pattern, script: null });
      result.created++;
    }
  }
  return result;
}

export async function reconcilePublicRoots(api, config, apply = false, replaceEstateWorker = false) {
  const ids = await zones(api, config.territories);
  const result = { apply, dns_changed: 0, routes_created: 0, routes_present: 0, roots: config.territories.length };
  for (const { root } of config.territories) {
    const id = ids.get(root);
    const records = (await api('GET', dnsPath(id, root)))
      .filter(record => ['CNAME', 'A', 'AAAA'].includes(record.type));
    if (records.length !== 1 || records[0].type !== 'CNAME') throw new Error(`${root} requires exactly one CNAME apex record`);
    const row = records[0];
    const desired = desiredDns(root, config);
    if (row.content !== desired.content || !row.proxied) {
      if (apply) await api('PUT', `/zones/${id}/dns_records/${row.id}`, desired);
      result.dns_changed++;
    }
    const pattern = `${root}/*`;
    const exact = (await api('GET', routesPath(id))).find(item => item.pattern === pattern);
    if (exact?.script != null) {
      if (exact.script !== 'aurekai-estate' || !replaceEstateWorker) throw new Error(`${pattern} is bound to ${exact.script}; refusing to replace it`);
      if (apply) {
        await api('DELETE', `${routesPath(id)}/${exact.id}`);
        await api('POST', routesPath(id), { pattern, script: null });
      }
      result.routes_created++;
    } else if (exact) result.routes_present++;
    else {
      if (apply) await api('POST', routesPath(id), { pattern, script: null });
      result.routes_created++;
    }
  }
  return result;
}

export async function reconcileTunnel(api, config, account, apply = false) {
  if (!account) throw new Error('CLOUDFLARE_ACCOUNT_ID is required');
  const path = `/accounts/${account}/cfd_tunnel/${config.tunnelID}/configurations`;
  const current = (await api('GET', path)).config;
  const hosts = new Set([...config.territories.map(t => t.root), ...config.additionalTunnelHosts].flatMap(root => [root, `*.${root}`]));
  const changed = [];
  current.ingress ??= [];
  for (const row of current.ingress) {
    // A path-scoped rule is a deliberate split of a host; only the host's
    // catch-all belongs to the native service.
    if (hosts.has(row.hostname) && !row.path && row.service !== config.nativeService) {
      changed.push(row.hostname);
      row.service = config.nativeService;
    }
  }
  const gateway = config.gateway;
  if (gateway) {
    const label = `${gateway.host} gateway paths`;
    const matches = current.ingress.filter(row => row.hostname === gateway.host && row.path === gateway.path);
    if (matches.length > 1) throw new Error('duplicate gateway path ingress');
    const rule = matches[0];
    const catchAll = current.ingress.findIndex(row => row.hostname === gateway.host && !row.path);
    if (catchAll < 0) throw new Error(`${gateway.host} has no tunnel catch-all`);
    if (rule) {
      const currentIndex = current.ingress.indexOf(rule);
      if (currentIndex > catchAll) { current.ingress.splice(currentIndex,1); current.ingress.splice(catchAll,0,rule); changed.push(label); }
      if (rule.service !== gateway.service) { rule.service = gateway.service; if (!changed.includes(label)) changed.push(label); }
    } else {
      // Ingress is first-match, so the split must sit ahead of the host's catch-all.
      const before = catchAll;
      const entry = { hostname: gateway.host, path: gateway.path, service: gateway.service };
      if (before < 0) throw new Error(`${gateway.host} has no tunnel rule to split`);
      current.ingress.splice(before, 0, entry);
      changed.push(label);
    }
  }
  if (apply && changed.length) await api('PUT', path, { config: current });
  return { apply, changed, ingress_rules: (current.ingress ?? []).length };
}

const modes = {
  'auth-hosts': reconcileAuthHosts,
  'member-read-redirects': reconcileMemberReadRedirects,
  'member-routes': reconcileMemberRoutes,
  'public-roots': reconcilePublicRoots,
  tunnel: reconcileTunnel,
};

if (import.meta.main) {
  const [mode, ...args] = process.argv.slice(2);
  if (mode === 'member-redirect-plan') {
    if (args.length) { console.error('member-redirect-plan takes no arguments'); process.exit(2); }
    try { console.log(JSON.stringify(memberReadRedirectPlan(loadConfig()), null, 2)); }
    catch (error) { console.error(error.message); process.exit(2); }
    process.exit(0);
  }
  if ((!Object.hasOwn(modes, mode) && !['purge', 'connector'].includes(mode)) ||
      (mode === 'connector' && args.length) ||
      (!['purge', 'connector'].includes(mode) && args.some(arg => !['--apply', '--replace-estate-worker'].includes(arg)))) {
    console.error('usage: cloudflare-reconcile <auth-hosts|member-routes|member-read-redirects|public-roots|tunnel> [--apply] [--replace-estate-worker]; member-redirect-plan; purge <paths...|--everything>; connector');
    process.exit(2);
  }
  const apply = args.includes('--apply');
  const env = process.env;
  try {
    const config = loadConfig();
    const token = ['tunnel', 'connector'].includes(mode) ? env.CLOUDFLARE_API_TOKEN : zoneToken(env);
    const api = client(token);
    if (mode === 'connector') process.exit(await runConnector(api, config, env));
    if (mode === 'purge') {
      await purgeEstate(api, config, args, root => console.log(`${root} purged`));
      process.exit(0);
    }
    const result = mode === 'tunnel'
      ? await reconcileTunnel(api, config, env.CLOUDFLARE_ACCOUNT_ID, apply)
      : mode === 'public-roots'
        ? await reconcilePublicRoots(api, config, apply, args.includes('--replace-estate-worker'))
        : await modes[mode](api, config, apply);
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error(`cloudflare-reconcile: ${error.message}`);
    process.exit(2);
  }
}
