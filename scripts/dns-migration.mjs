// Registrar -> Cloudflare lowering. Acceptance is not propagation/DNSSEC proof.
import { isIP } from 'node:net';
import { fileURLToPath } from 'node:url';
import { client, loadConfig, zoneToken } from './cloudflare-reconcile.mjs';

const xmlBinary = fileURLToPath(new URL('./foreign-xml', import.meta.url));
const hostName = value => typeof value === 'string' && value.length <= 253 &&
  value.split('.').every(label => /^[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?$/i.test(label));
const canonicalHost = value => value.toLowerCase().replace(/\.$/, '');
const integer = (value, min, max) => {
  if (!/^\d+$/.test(value) || Number(value) < min || Number(value) > max) throw new Error('invalid DNS integer field');
  return Number(value);
};

export function decodeRegistrar(raw, method, domain, binary = xmlBinary) {
  if (!Buffer.isBuffer(raw) || raw.length > 4 * 1024 * 1024) throw new Error('registrar response exceeds byte limit');
  const flag = method === 'getHosts' ? '--namecheap-hosts' : method === 'setCustom' ? '--namecheap-updated' : null;
  if (!flag) throw new Error('unsupported registrar operation');
  const result = Bun.spawnSync([binary, flag], { stdin: raw, stdout: 'pipe', stderr: 'pipe' });
  if (result.exitCode !== 0) throw new Error('registrar XML refused');
  const document = JSON.parse(result.stdout.toString());
  if (document.Domain !== domain) throw new Error('registrar returned another domain');
  return document;
}

export function desiredRecords(domain, document) {
  if (!document || document.Domain !== domain || !Array.isArray(document.hosts) || !document.hosts.length)
    throw new Error('refusing an empty or mismatched registrar record set');
  const records = [];
  for (const row of document.hosts) {
    if (!row || typeof row.Name !== 'string' || !row.Name) throw new Error('invalid DNS record name');
    const type = row.Type === 'ALIAS' ? 'CNAME' : row.Type;
    if (!['A', 'AAAA', 'CNAME', 'TXT', 'MX', 'NS'].includes(type))
      throw new Error(`record type ${String(row.Type)} has no lossless migration; registrar unchanged`);
    const name = row.Name === '@' ? domain : `${row.Name}.${domain}`;
    if (!(hostName(name) || (name.startsWith('*.') && hostName(name.slice(2))))) throw new Error('invalid DNS record name');
    let content = row.Address;
    if (typeof content !== 'string' || !content || content.includes('\0')) throw new Error('invalid DNS record content');
    if ((type === 'A' && isIP(content) !== 4) || (type === 'AAAA' && isIP(content) !== 6)) throw new Error('invalid DNS IP address');
    if (['CNAME', 'MX', 'NS'].includes(type)) {
      content = canonicalHost(content);
      if (!hostName(content)) throw new Error('invalid DNS target');
    }
    const record = { type, name: name.toLowerCase(), content,
      ttl: integer(row.TTL || '300', 60, 86400), proxied: false };
    if (type === 'MX') record.priority = integer(row.MXPref, 0, 65535);
    records.push(record);
  }
  const unique = new Map(records.map(row => [recordKey(row), row]));
  for (const row of unique.values()) if (row.type === 'CNAME' &&
    [...unique.values()].some(other => other.name === row.name && recordKey(other) !== recordKey(row)))
    throw new Error('conflicting CNAME in registrar records');
  return [...unique.values()];
}

function recordKey(row) {
  return JSON.stringify([row.type, canonicalHost(row.name),
    ['CNAME', 'MX', 'NS'].includes(row.type) ? canonicalHost(row.content) : row.content,
    row.ttl, row.proxied === true, row.type === 'MX' ? row.priority : null]);
}

async function recordsFor(cf, id) {
  const rows = [], ids = new Set();
  for (let page = 1; page <= 100; page++) {
    const batch = await cf('GET', `/zones/${id}/dns_records?per_page=100&page=${page}`);
    if (!Array.isArray(batch)) throw new Error('invalid Cloudflare record list');
    for (const row of batch) {
      if (!row || typeof row.id !== 'string' || ids.has(row.id) || typeof row.name !== 'string' || typeof row.content !== 'string')
        throw new Error('ambiguous Cloudflare record list');
      ids.add(row.id); rows.push(row);
    }
    if (batch.length < 100) return rows;
  }
  throw new Error('Cloudflare record pagination limit exceeded');
}

function validateZone(zone, domain, account) {
  if (!zone || zone.name !== domain || zone.account?.id !== account || !/^[a-f0-9]{32}$/i.test(zone.id || '') ||
      zone.type !== 'full' || !Array.isArray(zone.name_servers) || zone.name_servers.length < 2 ||
      new Set(zone.name_servers).size !== zone.name_servers.length || !zone.name_servers.every(hostName))
    throw new Error('Cloudflare zone identity or nameservers do not match the requested migration');
  return zone;
}

export async function migrateDns({ cf, registrar, account, domains, apply = false, report = () => {} }) {
  if (typeof apply !== 'boolean' || !/^[a-f0-9]{32}$/i.test(account || '') || !Array.isArray(domains) || !domains.length ||
      new Set(domains).size !== domains.length || domains.some(domain => !hostName(domain) || !domain.includes('.')))
    throw new Error('invalid migration scope');
  // Entire registrar source preflight precedes even zone creation.
  const plans = [];
  for (const domain of domains) plans.push({ domain, records: desiredRecords(domain, await registrar('getHosts', domain)) });
  if (!apply) return { apply, domains: plans.map(p => ({ domain: p.domain, records: p.records.length })), changed: 0 };
  let changed = 0;
  for (const { domain, records } of plans) {
    const found = await cf('GET', `/zones?name=${encodeURIComponent(domain)}&account.id=${account}`);
    if (!Array.isArray(found) || found.length > 1) throw new Error('ambiguous Cloudflare zone lookup');
    const zone = validateZone(found.length ? found[0] : await cf('POST', '/zones', { name: domain, account: { id: account }, type: 'full' }), domain, account);
    const before = await recordsFor(cf, zone.id);
    const desired = new Set(records.map(recordKey));
    for (const old of before) if (records.some(row => row.name === canonicalHost(old.name)) && !desired.has(recordKey(old)))
      throw new Error('existing Cloudflare records conflict with the registrar; no records replaced');
    const present = new Set(before.map(recordKey));
    for (const row of records) if (!present.has(recordKey(row))) await cf('POST', `/zones/${zone.id}/dns_records`, row);
    const after = await recordsFor(cf, zone.id);
    const observed = new Set(after.map(recordKey));
    if (records.some(row => !observed.has(recordKey(row))) ||
        after.some(row => records.some(wanted => wanted.name === canonicalHost(row.name)) && !desired.has(recordKey(row))))
      throw new Error('DNS copy readback differs; registrar unchanged');
    // Source freshness is checked again immediately before the registrar boundary.
    const fresh = desiredRecords(domain, await registrar('getHosts', domain));
    if (fresh.length !== desired.size || fresh.some(row => !desired.has(recordKey(row))))
      throw new Error('registrar records changed during migration; nameservers unchanged');
    const accepted = await registrar('setCustom', domain, zone.name_servers);
    if (accepted.Domain !== domain || accepted.updated !== true) throw new Error('registrar nameserver change not acknowledged');
    changed++;
    report({ domain, records: records.length, state: 'registrar_acknowledged', propagation_verified: false });
  }
  return { apply, changed, propagation_verified: false };
}

async function boundedResponse(response, limit) {
  if (!response.ok) throw new Error(`registrar HTTP ${response.status}`);
  const reader = response.body?.getReader();
  if (!reader) throw new Error('empty registrar response');
  const chunks = []; let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > limit) throw new Error('registrar response exceeds byte limit');
      chunks.push(value);
    }
  } finally { await reader.cancel(); reader.releaseLock(); }
  return Buffer.concat(chunks, size);
}

export function registrarClient(env, fetcher = fetch) {
  if (!env.NAMECHEAP_API_USER || !env.NAMECHEAP_API_KEY || isIP(env.NAMECHEAP_CLIENT_IP || '') !== 4)
    throw new Error('NAMECHEAP_API_USER, NAMECHEAP_API_KEY and explicit IPv4 NAMECHEAP_CLIENT_IP are required');
  return async (method, domain, nameservers) => {
    if (!['getHosts', 'setCustom'].includes(method) || !hostName(domain) || !domain.includes('.'))
      throw new Error('invalid registrar operation scope');
    const [SLD, ...rest] = domain.split('.');
    const params = new URLSearchParams({ ApiUser: env.NAMECHEAP_API_USER, ApiKey: env.NAMECHEAP_API_KEY,
      UserName: env.NAMECHEAP_API_USER, Command: `namecheap.domains.dns.${method}`, ClientIp: env.NAMECHEAP_CLIENT_IP,
      SLD, TLD: rest.join('.') });
    if (method === 'setCustom') params.set('Nameservers', nameservers.join(','));
    // Never log credential-bearing URLs or upstream bodies; refuse redirects.
    let response;
    try { response = await fetcher(`https://api.namecheap.com/xml.response?${params}`, { redirect: 'error', signal: AbortSignal.timeout(40000) }); }
    catch { throw new Error('registrar transport failed'); }
    return decodeRegistrar(await boundedResponse(response, 4 * 1024 * 1024), method, domain);
  };
}

if (import.meta.main) {
  try {
    let domain, apply = false;
    const args = process.argv.slice(2);
    for (let i = 0; i < args.length; i++) {
      if (args[i] === '--apply' && !apply) apply = true;
      else if (args[i] === '--domain' && !domain && args[i + 1]) domain = args[++i];
      else throw new Error('usage: move_dns_to_cloudflare.sh [--apply] [--domain authored-root]');
    }
    const config = loadConfig(), env = process.env;
    const roots = config.territories.map(row => row.root);
    if (domain && !roots.includes(domain)) throw new Error('domain is outside the authored territory set');
    const rawZonesToken = (env.CLOUDFLARE_info_zones || '').trim();
    const token = (/^[A-Za-z0-9_-]{40,}$/.test(rawZonesToken) ? rawZonesToken : zoneToken(env)) ||
      /Bearer\s+([A-Za-z0-9_-]+)/.exec(env.CLOUDFLARE_info || '')?.[1];
    const account = env.CLOUDFLARE_ACCOUNT_ID || /\/accounts\/([a-f0-9]{32})\//i.exec(env.CLOUDFLARE_info || '')?.[1];
    const result = await migrateDns({ cf: client(token), registrar: registrarClient(env), account,
      domains: domain ? [domain] : roots, apply, report: row => console.log(JSON.stringify(row)) });
    console.log(JSON.stringify(result));
  } catch (error) { console.error(`DNS migration: ${error.message}`); process.exitCode = 2; }
}
