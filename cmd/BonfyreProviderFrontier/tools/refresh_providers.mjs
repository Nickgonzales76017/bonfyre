#!/usr/bin/env bun
// Reachability receipts only. No provider response body, key, URL, or error is persisted.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function providerRefreshPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../provider-refresh.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'providerRefresh']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

export function providerProbes(policy = providerRefreshPolicy()) {
  const cloudflare = policy.cloudflare.probes.map(probe => ({ ...probe,
    key_env: policy.cloudflare.token_env, account_env: policy.cloudflare.account_env,
    credential_label: policy.cloudflare.credential_label, auth: 'bearer',
    cloudflare_base: policy.cloudflare.base }));
  const local = Object.entries(policy.local).sort(([a], [b]) => a.localeCompare(b))
    .map(([provider, plane]) => ({ provider, plane, local: true }));
  return [...cloudflare, ...policy.endpoints, ...local];
}

export function providerVerdict(status, policy = providerRefreshPolicy()) {
  if (status === 0) return 'unreachable';
  if (policy.refused_statuses.includes(status)) return 'refused';
  if ((status >= 200 && status < 300) || status === 404) return 'reachable';
  return `answered_${status}`;
}

function requestFor(probe, key, account, policy) {
  const url = probe.cloudflare_base
    ? new URL(`${encodeURIComponent(account)}${probe.path}`, probe.cloudflare_base)
    : new URL(probe.url);
  const headers = { 'User-Agent': policy.agent };
  const auth = policy.auth[probe.auth];
  if (!auth) throw new Error(`unknown provider authentication scheme: ${probe.auth}`);
  if (auth.parameter) url.searchParams.set(auth.parameter, key);
  else {
    headers[auth.header] = `${auth.prefix}${key}`;
    Object.assign(headers, auth.additional ?? {});
  }
  return { url, headers };
}

export async function refreshProviders(options = {}, policy = providerRefreshPolicy()) {
  const environment = options.env ?? process.env;
  const fetcher = options.fetcher ?? fetch;
  const clock = options.clock ?? (() => new Date());
  const rows = [], counts = {};
  const began = performance.now();
  for (const probe of providerProbes(policy)) {
    if (options.only && !probe.provider.includes(options.only)) continue;
    const row = { provider: probe.provider, plane: probe.plane };
    const key = String(environment[probe.key_env] ?? '').trim();
    const account = probe.account_env ? String(environment[probe.account_env] ?? '').trim() : '';
    if (probe.local) {
      row.verdict = 'not_probeable';
      row.why = policy.local_reason;
    } else if (!probe.url && !probe.cloudflare_base) {
      row.verdict = 'not_probeable';
      row.why = policy.unconfigured_reason;
    } else if (!key || (probe.account_env && !account)) {
      row.verdict = 'no_credential';
      row.wants = probe.credential_label ?? probe.key_env;
    } else {
      let status = 0;
      try {
        const request = requestFor(probe, key, account, policy);
        const answer = await fetcher(request.url.toString(), {
          method: 'GET', headers: request.headers,
          signal: AbortSignal.timeout(policy.timeout_ms),
        });
        status = answer.status;
        await answer.body?.cancel?.();
      } catch { /* Never persist a thrown error: it may contain a query credential. */ }
      row.verdict = providerVerdict(status, policy);
      row.http_status = status;
      if (status === 0) row.why = 'request_failed';
    }
    row.asked_at = clock().toISOString().replace(/\.\d{3}Z$/, 'Z');
    rows.push(row);
    counts[row.verdict] = (counts[row.verdict] ?? 0) + 1;
  }
  return { rows, summary: { providers: rows.length, by_verdict: counts,
    seconds: Math.round((performance.now() - began) / 100) / 10,
    cannot_infer: policy.cannot_infer } };
}

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index++) {
    if (argv[index] === '--out') args.out = argv[++index];
    else if (argv[index] === '--only') args.only = argv[++index];
    else throw new Error(`unknown argument: ${argv[index]}`);
  }
  if (!args.out) throw new Error('usage: refresh_providers.mjs --out receipts.jsonl [--only provider]');
  return args;
}

if (import.meta.main) {
  try {
    const args = parseArgs(process.argv.slice(2));
    const result = await refreshProviders({ only: args.only });
    writeFileSync(args.out, `${result.rows.map(row => JSON.stringify(row)).join('\n')}\n`);
    process.stdout.write(`${JSON.stringify(result.summary)}\n`);
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
