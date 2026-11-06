#!/usr/bin/env bun
// Only observer-returned endpoint shapes become eligible channels, never products.
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { classifyProbe } from '../../BonfyreExternalEstate/tools/compile_commerce_family.mjs';
import { readObservationReceipt, successfulPublicObservation } from './foreign_observation.mjs';

const ROOT = resolve(fileURLToPath(import.meta.url), '../../../..');
const providerSpec = JSON.parse(readFileSync(resolve(ROOT, 'schemas/control/commerce-observation-providers.json'), 'utf8'));
export const activeProbes = spec => (spec.phenotypes || []).flatMap(({ discovery }) =>
  (discovery?.probes || []).filter(rule => rule.active && rule.request_path && rule.response_key));

export function hostOf(value) {
  try {
    const url = new URL(value.includes('://') ? value : `https://${value}`);
    return url.hostname.toLowerCase().replace(/^www\./, '');
  } catch { return ''; }
}

export function frontierRows(host, observations, spec = providerSpec) {
  const responses = Object.fromEntries(Object.entries(observations).flatMap(([key, observation]) =>
    observation && successfulPublicObservation(observation.receipt) && Object.hasOwn(observation, 'body')
      ? [[key, observation.body]] : []));
  return classifyProbe(spec, { root: host, responses }).map(found => {
    return { address: `https://${host}${found.endpoint}`, protocol: found.phenotype, material: 'commerce',
      state: 'eligible', truth_level: found.truth_level, title: `${found.phenotype} at ${host}`,
      discovered_from: 'SourceFrontier observer received declared storefront endpoint shape',
      cannot_infer: 'a storefront endpoint answer -> a product observation, whole catalogue, account relationship, merchant inventory, or a right to buy' };
  });
}

async function ask(project, host, rule) {
  const address = `https://${host}${rule.request_path}`;
  const stem = createHash('sha256').update(address).digest('hex').slice(0, 16);
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-storefront-probe.'));
  try {
    const body = join(dir, `${stem}.body`), receipt = join(dir, `${stem}.receipt`);
    const child = Bun.spawn([project, 'foreign', 'observe', `storefront-${stem}`, address,
      '--observe-right', 'public', '--body', body, '--receipt', receipt],
    { stdout: 'ignore', stderr: 'ignore' });
    const timeout = setTimeout(() => child.kill(), 60000);
    const code = await child.exited;
    clearTimeout(timeout);
    if (code !== 0) return [rule.response_key, null];
    // The native observer owns both retained bytes and receipt. An unreceipted
    // result cannot enter the classifier even when it resembles a product API.
    const proof = readObservationReceipt(receipt);
    if (!successfulPublicObservation(proof)) return [rule.response_key, null];
    return [rule.response_key, { body: JSON.parse(readFileSync(body, 'utf8')), receipt: proof }];
  } catch { return [rule.response_key, null]; }
  finally { rmSync(dir, { recursive: true, force: true }); }
}

export async function probeHosts(hosts, project, workers = 8, spec = providerSpec) {
  const results = Array.from({ length: hosts.length });
  const probes = activeProbes(spec);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(Math.max(workers, 1), hosts.length) }, async () => {
    for (;;) {
      const index = next++;
      if (index >= hosts.length) return;
      // Each worker advances only after its bounded pair of observer calls.
      // eslint-disable-next-line no-await-in-loop
      const responses = Object.fromEntries(await Promise.all(probes.map(rule => ask(project, hosts[index], rule))));
      results[index] = frontierRows(hosts[index], responses, spec);
    }
  }));
  return results.flat();
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args.hosts || !args.out) throw new Error('missing --hosts or --out');
    const limit = Number(args.limit ?? 400), workers = Number(args.workers ?? 8);
    if (!Number.isInteger(limit) || limit < 1 || !Number.isInteger(workers) || workers < 1)
      throw new Error('limit and workers must be positive integers');
    const hosts = [...new Set(readFileSync(args.hosts, 'utf8').split('\n').map(value => hostOf(value.trim())).filter(Boolean))].slice(0, limit);
    const rows = await probeHosts(hosts, args.project || resolve(ROOT, 'components/BonfyreProject/bonfyre-project'), workers);
    writeFileSync(args.out, rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''));
    console.log(JSON.stringify({ hosts_asked: hosts.length, eligible_storefront_channels: rows.length,
      cannot_infer: 'an endpoint answer is not materialized commerce or permission to buy' }));
  } catch (error) { console.error(`storefront probe: ${error.message}`); process.exitCode = 2; }
}
