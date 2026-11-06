import { test, expect } from 'bun:test';
import { law, prove, formatProof } from '../roundtrip_proof.mjs';

test('runtime contract is the CUE export', () => {
  const command = Bun.spawnSync(['cue', 'export', 'scripts/roundtrip-law.cue', '-e', 'roundtripLaw'], { cwd: new URL('../../', import.meta.url).pathname });
  expect(command.exitCode).toBe(0);
  expect(law).toEqual(JSON.parse(new TextDecoder().decode(command.stdout)));
});

test('each carrier is fetched once and absent agreement is explicit', async () => {
  const calls = new Map();
  const documents = new Map([
    ['/ap/actor', { id: 'https://aurekai.life/ap/actor', type: 'Person', inbox: 'i', preferredUsername: 'b' }],
    ['/mcp/resources/', { resources: [] }],
    ['/beckn/search', { context: {} }],
    ['/.well-known/bonfyre', { version: 1, host: 'aurekai.life' }],
  ]);
  const result = await prove({ fetcher: async url => {
    const address = `${url.pathname}${url.search}`;
    calls.set(address, (calls.get(address) || 0) + 1);
    return new Response(documents.has(address) ? JSON.stringify(documents.get(address)) : '', { status: documents.has(address) ? 200 : 404 });
  } });
  expect(result.total).toBe(law.projections.length + law.agreement.length);
  expect(result.projections.find(row => row.address === 'actor id vs webfinger').verdict).toBe('unreachable');
  expect([...calls.values()]).toEqual(Array(law.projections.length).fill(1));
  expect(result.faithful).toBeLessThan(result.total);
});

test('carrier presence does not substitute for cross-carrier agreement', async () => {
  const documents = new Map(law.projections.map(({ address, required }) => [address, Object.fromEntries(required.map(field => [field, 'present']))]));
  documents.set('/ap/actor', { id: 'https://aurekai.life/ap/actor', type: 'Person', inbox: 'i', preferredUsername: 'b' });
  documents.set('/.well-known/webfinger?resource=acct:bonfyre@aurekai.life', { subject: 'acct:bonfyre@aurekai.life', links: [{ href: 'https://different.example/ap/actor' }] });
  documents.set('/.well-known/agent.json', { name: 'Bonfyre', url: 'https://different.example', skills: [] });
  const result = await prove({ fetcher: async url => new Response(JSON.stringify(documents.get(`${url.pathname}${url.search}`)), { status: 200 }) });
  expect(result.projections.filter(row => row.verdict === 'drifted')).toHaveLength(2);
  expect(result.faithful).toBe(law.projections.length);
});

test('one identity across the declared carriers closes all checks', async () => {
  const actor = 'https://aurekai.life/ap/actor';
  const documents = new Map(law.projections.map(({ address, required }) => [address, Object.fromEntries(required.map(field => [field, 'present']))]));
  documents.set('/ap/actor', { id: actor, type: 'Person', inbox: 'i', preferredUsername: 'b' });
  documents.set('/.well-known/webfinger?resource=acct:bonfyre@aurekai.life', { subject: 'acct:bonfyre@aurekai.life', links: [{ href: actor }] });
  documents.set('/.well-known/agent.json', { name: 'Bonfyre', url: 'https://aurekai.life', skills: [] });
  const result = await prove({ fetcher: async url => new Response(JSON.stringify(documents.get(`${url.pathname}${url.search}`)), { status: 200 }) });
  expect(result.faithful).toBe(result.total);
  expect(formatProof(result)).toContain('actor id vs webfinger');
  expect(formatProof(result)).toContain(`round trip: ${result.total}/${result.total}`);
});
