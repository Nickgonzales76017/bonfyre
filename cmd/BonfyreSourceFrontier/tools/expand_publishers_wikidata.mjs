#!/usr/bin/env bun
// Official-site discovery only: no channel, fetch, or admission inference.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function publisherClassesPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../publisher-classes.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'publisherClasses']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

export function publisherHost(address) {
  let host;
  try { host = new URL(address).hostname.toLowerCase().replace(/\.$/, ''); } catch { return ''; }
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(host)) return '';
  return host.replace(/^www\./, '');
}

export async function expandPublishers(options = {}, policy = publisherClassesPolicy(), fetcher = fetch) {
  const wanted = options.classes?.length ? options.classes : Object.keys(policy.classes);
  const perClass = Number(options.perClass || policy.default_per_class);
  const pause = options.pauseMs ?? policy.default_pause_ms;
  if (!Number.isInteger(perClass) || perClass < 1) throw new Error('--per-class must be positive');
  if (!Number.isFinite(pause) || pause < 0) throw new Error('--pause must be nonnegative');
  const seen = new Set(), rows = [], refused = {};
  for (const kind of wanted) {
    const identity = policy.classes[kind];
    if (!identity) { refused[kind] = 'not a class this tool knows'; continue; }
    let answer;
    try {
      const query = `${policy.query_prefix}${identity}${policy.query_suffix}${perClass}`;
      const url = `${policy.endpoint}?${new URLSearchParams({ query, format: 'json' })}`;
      const response = await fetcher(url, { headers: { 'User-Agent': policy.agent, Accept: policy.accept },
        signal: AbortSignal.timeout(policy.timeout_ms) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      answer = await response.json();
    } catch (error) {
      refused[kind] = String(error.message || error).slice(0, 80);
      if (pause) await Bun.sleep(pause);
      continue;
    }
    let found = 0;
    for (const row of answer?.results?.bindings || []) {
      const site = row.site?.value || '', host = publisherHost(site);
      if (!host || seen.has(host)) continue;
      seen.add(host);
      const item = row.item?.value || '';
      rows.push({ root: host, name: (row.itemLabel?.value || host).slice(0, 160),
        identity: item ? item.split('/').at(-1) : '',
        discovered_from: `Wikidata records ${site} as the official website of this ${kind}` });
      found++;
    }
    if (options.progress) options.progress(kind, found);
    if (pause) await Bun.sleep(pause);
  }
  return { rows, summary: { publishers: rows.length, classes_asked: wanted.length,
    classes_refused: refused, cannot_infer: policy.cannot_infer } };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index += 2) {
      if (!argv[index]?.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[argv[index].slice(2)] = argv[index + 1];
    }
    if (!args.out) throw new Error('--out is required');
    const result = await expandPublishers({ classes: args.classes?.split(',').map(value => value.trim()).filter(Boolean),
      perClass: args['per-class'] ? Number(args['per-class']) : undefined,
      pauseMs: args.pause ? Number(args.pause) * 1000 : undefined,
      progress: (kind, found) => console.error(`${kind.padEnd(20)} ${found} publishers`) });
    writeFileSync(args.out, result.rows.map(row => JSON.stringify(row)).join('\n') + (result.rows.length ? '\n' : ''));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`publisher expansion: ${error.message}`); process.exitCode = 2; }
}
