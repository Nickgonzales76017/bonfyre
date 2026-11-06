import { test, expect } from 'bun:test';
import { providerProbes, providerVerdict, refreshProviders } from '../tools/refresh_providers.mjs';

const clock = () => new Date('2026-09-29T12:00:00.000Z');

test('catalogue is complete even with no credential, without making requests', async () => {
  const probes = providerProbes();
  expect(probes.length).toBe(9 + 32 + 26);
  expect(new Set(probes.map(probe => probe.provider)).size).toBe(probes.length);
  const result = await refreshProviders({ env: {}, clock, fetcher: () => { throw new Error('must not fetch'); } });
  expect(result.rows).toHaveLength(probes.length);
  expect(result.summary.by_verdict).toEqual({ no_credential: 37, not_probeable: 30 });
  expect(result.rows.filter(row => row.provider.startsWith('cloudflare-'))).toHaveLength(9);
  expect(result.rows.find(row => row.provider === 'cloudflare-workers-ai')).toMatchObject({
    verdict: 'no_credential', wants: 'CLOUDFLARE_info', asked_at: '2026-09-29T12:00:00Z' });
  expect(result.rows.find(row => row.provider === 'supabase')).toMatchObject({
    verdict: 'not_probeable', why: 'no independently probeable endpoint has been declared' });
  expect(result.rows.find(row => row.provider === 'yaff').verdict).toBe('not_probeable');
});

test('scoped authenticated probe emits a bounded reachability receipt, not secrets', async () => {
  const seen = [];
  const result = await refreshProviders({ only: 'cloudflare-r2',
    env: { CF_TOKEN: 'secret-token', CF_ACCT: 'private-account' }, clock,
    fetcher: async (url, init) => {
      seen.push([url, init.headers.Authorization]);
      return { status: 404, body: { cancel: async () => {} } };
    } });
  expect(seen).toEqual([['https://api.cloudflare.com/client/v4/accounts/private-account/r2/buckets',
    'Bearer secret-token']]);
  expect(result.rows).toEqual([{ provider: 'cloudflare-r2', plane: 'artifact-store',
    verdict: 'reachable', http_status: 404, asked_at: '2026-09-29T12:00:00Z' }]);
  expect(JSON.stringify(result)).not.toContain('secret-token');
  expect(JSON.stringify(result)).not.toContain('private-account');
});

test('auth modes, refusal, transport failure and status classification stay distinct', async () => {
  const seen = [];
  const credentials = { ANTHROPIC_API_KEY: 'anthropic-secret', GEMINI_API_KEY: 'gemini-secret',
    GITHUB_PAT: 'github-secret', OPENAI_API_KEY: 'openai-secret' };
  const fetcher = async (url, init) => {
    seen.push([url, init.headers]);
    if (url.includes('openai.com')) throw new Error('leaked openai-secret');
    return { status: url.includes('anthropic.com') ? 401 : 503 };
  };
  for (const name of ['anthropic', 'gemini', 'github', 'openai']) {
    const { rows } = await refreshProviders({ only: name, env: credentials, clock, fetcher });
    expect(rows.find(row => row.provider === name)?.verdict)
      .toBe({ anthropic: 'refused', gemini: 'answered_503', github: 'answered_503',
        openai: 'unreachable' }[name]);
    expect(JSON.stringify(rows)).not.toMatch(/secret/);
  }
  expect(seen.find(([url]) => url.includes('anthropic.com'))[1]).toMatchObject({
    'x-api-key': 'anthropic-secret', 'anthropic-version': '2023-06-01' });
  expect(seen.find(([url]) => url.includes('googleapis.com'))[0]).toContain('key=gemini-secret');
  expect(seen.find(([url]) => url.includes('github.com'))[1].Authorization).toBe('token github-secret');
  expect([0, 200, 204, 401, 403, 404, 429, 503].map(status => providerVerdict(status)))
    .toEqual(['unreachable', 'reachable', 'reachable', 'refused', 'refused', 'reachable',
      'answered_429', 'answered_503']);
});
