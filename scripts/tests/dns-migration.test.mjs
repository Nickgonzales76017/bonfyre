// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { decodeRegistrar, desiredRecords, migrateDns, registrarClient } from '../dns-migration.mjs';

const domain = 'aurekai.life', account = 'a'.repeat(32), id = 'b'.repeat(32);
const hosts = [
  { Name: '@', Type: 'A', Address: '192.0.2.1', TTL: '1800', MXPref: '0' },
  { Name: '@', Type: 'MX', Address: 'mail.example.', TTL: '300', MXPref: '10' },
  { Name: '_acme-challenge', Type: 'TXT', Address: '"quoted" & café', TTL: '120', MXPref: '0' },
];
const escape = text => String(text).replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll('<', '&lt;');
const xml = (method = 'getHosts', rows = hosts, name = domain) => {
  const command = `namecheap.domains.dns.${method}`;
  const body = method === 'getHosts'
    ? `<DomainDNSGetHostsResult Domain="${name}" IsUsingOurDNS="true">${rows.map(row =>
      `<Host ${Object.entries(row).map(([k, v]) => `${k}="${escape(v)}"`).join(' ')}/>`).join('')}</DomainDNSGetHostsResult>`
    : `<DomainDNSSetCustomResult Domain="${name}" Updated="true"/>`;
  return `<ApiResponse xmlns="http://api.namecheap.com/xml.response" Status="OK"><Errors/><RequestedCommand>${command}</RequestedCommand><CommandResponse Type="${command}">${body}</CommandResponse></ApiResponse>`;
};

test('native registrar XML handles namespaces, entity text, uppercase Host, MX priority and TTL', () => {
  const doc = decodeRegistrar(Buffer.from(xml()), 'getHosts', domain);
  expect(doc.hosts).toEqual(hosts);
  expect(desiredRecords(domain, doc)).toEqual([
    { type: 'A', name: domain, content: '192.0.2.1', ttl: 1800, proxied: false },
    { type: 'MX', name: domain, content: 'mail.example', priority: 10, ttl: 300, proxied: false },
    { type: 'TXT', name: `_acme-challenge.${domain}`, content: '"quoted" & café', ttl: 120, proxied: false },
  ]);
  expect(decodeRegistrar(Buffer.from(xml('setCustom')), 'setCustom', domain)).toEqual({ Domain: domain, updated: true });
  for (const bad of [
    xml().replace('Status="OK"', 'Status="ERROR"'),
    xml().replace('<Errors/>', '<Errors><Error Number="1">no</Error></Errors>'),
    xml().replace('IsUsingOurDNS="true"', 'IsUsingOurDNS="false"'),
    xml().replace('</CommandResponse>', '<DomainDNSGetHostsResult Domain="aurekai.life" IsUsingOurDNS="true"/></CommandResponse>'),
    xml().replace('http://api.namecheap.com/xml.response', 'urn:spoof'),
    '<!DOCTYPE ApiResponse [<!ENTITY injected "x">]>' + xml(),
    xml().replace('Domain="aurekai.life"', 'Domain="other.test"'),
    xml().slice(0, -12),
  ]) expect(() => decodeRegistrar(Buffer.from(bad), 'getHosts', domain)).toThrow();
  expect(() => decodeRegistrar(Buffer.from(xml('setCustom').replace('Updated="true"', 'Updated="false"')), 'setCustom', domain)).toThrow();
  expect(() => decodeRegistrar(Buffer.alloc(4 * 1024 * 1024 + 1), 'getHosts', domain)).toThrow('byte limit');
});

function fixture(options = {}) {
  const calls = [], reports = [], current = structuredClone(options.existing || []);
  const zone = { id, name: domain, type: 'full', account: { id: account }, name_servers: ['one.ns.cloudflare.com', 'two.ns.cloudflare.com'] };
  let read = 0;
  const registrar = async (method, name, nameservers) => {
    calls.push({ owner: 'registrar', method, name, nameservers });
    if (method === 'setCustom') return { Domain: name, updated: !options.rejectRegistrar };
    read++;
    return { Domain: name, hosts: options.changeSource && read > 1 ? [...hosts, { ...hosts[0], Name: 'new' }] : options.hosts || hosts };
  };
  const cf = async (method, path, body) => {
    calls.push({ owner: 'cf', method, path, body });
    if (path.startsWith('/zones?')) return options.wrongAccount ? [{ ...zone, account: { id: 'c'.repeat(32) } }] : options.create ? [] : [zone];
    if (path === '/zones' && method === 'POST') return zone;
    if (method === 'GET' && path.includes('/dns_records?')) return structuredClone(current);
    if (method === 'POST' && path.endsWith('/dns_records')) {
      if (options.failCopy) throw new Error('copy failed');
      const row = { ...body, id: `record-${current.length}` };
      if (!options.loseCopy) current.push(row);
      return row;
    }
    throw new Error('unexpected API request');
  };
  const run = (apply = true) => migrateDns({ cf, registrar, account, domains: [domain], apply, report: row => reports.push(row) });
  return { run, calls, reports, current };
}

test('default migration is read-only; apply reads all copied records back before registrar change', async () => {
  const dry = fixture();
  expect((await dry.run(false)).changed).toBe(0);
  expect(dry.calls.every(c => c.owner === 'registrar' && c.method === 'getHosts')).toBe(true);
  const f = fixture({ create: true });
  expect(await f.run()).toEqual({ apply: true, changed: 1, propagation_verified: false });
  expect(f.calls.at(-1).method).toBe('setCustom');
  expect(f.calls.at(-2).method).toBe('getHosts');
  expect(f.calls.at(-3).path).toContain('/dns_records?');
  expect(f.current).toHaveLength(3);
  expect(f.reports[0].state).toBe('registrar_acknowledged');
  const reuse = fixture({ existing: f.current });
  await reuse.run();
  expect(reuse.calls.filter(c => c.owner === 'cf' && c.method === 'POST')).toHaveLength(0);
});

test('unsupported, missing, conflicting, failed or drifting copies cannot reach registrar mutation', async () => {
  for (const options of [
    { hosts: [] }, { hosts: [{ ...hosts[0], Type: 'URL301' }] },
    { hosts: [{ ...hosts[1], MXPref: '' }] }, { hosts: [{ ...hosts[0], TTL: 'zero' }] },
    { wrongAccount: true }, { failCopy: true }, { loseCopy: true }, { changeSource: true },
    { existing: [{ id: 'conflict', type: 'A', name: domain, content: '192.0.2.2', ttl: 1800, proxied: false }] },
  ]) {
    const f = fixture(options);
    await expect(f.run()).rejects.toThrow();
    expect(f.calls.filter(c => c.method === 'setCustom')).toHaveLength(0);
    expect(f.reports).toHaveLength(0);
  }
  const refused = fixture({ rejectRegistrar: true });
  await expect(refused.run()).rejects.toThrow('not acknowledged');
  expect(refused.reports).toHaveLength(0);
});

test('registrar client refuses redirection and never exposes keys in transport errors', async () => {
  const env = { NAMECHEAP_API_USER: 'fixture', NAMECHEAP_API_KEY: 'secret-key', NAMECHEAP_CLIENT_IP: '192.0.2.10' };
  await expect(registrarClient(env, async (_url, options) => {
    expect(options.redirect).toBe('error'); throw new Error('https://secret-key');
  })('getHosts', domain)).rejects.toThrow('registrar transport failed');
  expect(() => registrarClient({ ...env, NAMECHEAP_CLIENT_IP: '' })).toThrow('explicit IPv4');
});

test('real shell entrypoint uses native XML and Bun with all network calls mocked', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-dns-migration-'));
  try {
    const log = join(dir, 'calls.jsonl'), preload = join(dir, 'preload.mjs');
    writeFileSync(preload, `import {appendFileSync} from 'node:fs';
const records=[];
globalThis.fetch=async(url,options={})=>{
  const u=new URL(url);appendFileSync(${JSON.stringify(log)},JSON.stringify({host:u.host,path:u.pathname,method:options.method||'GET',command:u.searchParams.get('Command')})+'\\n');
  if(u.host==='api.namecheap.com') {
    const method=u.searchParams.get('Command');
    if(method==='namecheap.domains.dns.getHosts')return new Response(${JSON.stringify(xml())});
    if(method==='namecheap.domains.dns.setCustom')return new Response(${JSON.stringify(xml('setCustom'))});
  }
  if(u.host==='api.cloudflare.com') {
    let result;
    if(u.pathname==='/client/v4/zones')result=[${JSON.stringify({id,name:domain,type:'full',account:{id:account},name_servers:['one.ns.cloudflare.com','two.ns.cloudflare.com']})}];
    else if(u.pathname==='/client/v4/zones/${id}/dns_records' && options.method==='GET') result=records;
    else if(u.pathname==='/client/v4/zones/${id}/dns_records' && options.method==='POST') {result={...JSON.parse(options.body),id:'r'+records.length};records.push(result);}
    else throw new Error('unexpected Cloudflare request');
    return Response.json({success:true,result});
  }
  throw new Error('unexpected network request');
};`);
    const launcher = join(dir, 'bun');
    writeFileSync(launcher, `#!/bin/sh\nexec '${process.execPath}' --preload '${preload}' "$@"\n`, { mode: 0o700 });
    for (const name of ['python', 'python3', 'curl']) writeFileSync(join(dir, name), '#!/bin/sh\nexit 99\n', { mode: 0o700 });
    const refs = join(dir, 'refs.env');
    writeFileSync(refs, `CLOUDFLARE_API_TOKEN=mock-secret\nCLOUDFLARE_ACCOUNT_ID=${account}\nNAMECHEAP_API_USER=fixture\nNAMECHEAP_API_KEY=mock-secret\nNAMECHEAP_CLIENT_IP=192.0.2.10\n`);
    const run = args => Bun.spawnSync(['/bin/sh', join(import.meta.dir, '../move_dns_to_cloudflare.sh'), '--domain', domain, ...args], {
      env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, BONFYRE_BUN: launcher, BONFYRE_CLOUD_REFS: refs }, stdout: 'pipe', stderr: 'pipe',
    });
    const dry = run([]);
    expect(dry.exitCode).toBe(0);
    expect(JSON.parse(dry.stdout.toString()).apply).toBe(false);
    expect(readFileSync(log, 'utf8').trim().split('\n')).toHaveLength(1);
    const applied = run(['--apply']);
    expect(applied.exitCode).toBe(0);
    expect(applied.stderr.toString()).toBe('');
    expect(applied.stdout.toString()).toContain('registrar_acknowledged');
    expect(applied.stdout.toString()).not.toContain('mock-secret');
    const calls = readFileSync(log, 'utf8').trim().split('\n').map(JSON.parse);
    expect(calls.at(-1).command).toBe('namecheap.domains.dns.setCustom');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
