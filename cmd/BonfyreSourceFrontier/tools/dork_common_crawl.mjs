#!/usr/bin/env bun
// Historical index discovery only; exact sources must qualify separately.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function commonCrawlPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../common-crawl-queries.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'commonCrawlQueries']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const hostOf = address => {
  let host;
  try { host = new URL(address).hostname.toLowerCase().replace(/\.$/, ''); } catch { return ''; }
  return /^[a-z0-9.-]+\.[a-z]{2,}$/.test(host) ? host.replace(/^www\./, '') : '';
};
const ask = async (url, timeout, agent, fetcher) => {
  try {
    const response = await fetcher(url, { headers: { 'User-Agent': agent }, signal: AbortSignal.timeout(timeout) });
    return { status: response.status, body: response.status === 200 ? await response.text() : '' };
  } catch { return { status: 0, body: '' }; }
};

export async function discoverCommonCrawl(options = {}, policy = commonCrawlPolicy(), fetcher = fetch) {
  const perDork = Number(options.perDork || policy.default_per_dork);
  const pause = options.pauseMs ?? policy.default_pause_ms;
  if (!Number.isInteger(perDork) || perDork < 1) throw new Error('--per-dork must be positive');
  if (!Number.isFinite(pause) || pause < 0) throw new Error('--pause must be nonnegative');
  let collection = options.collection;
  if (!collection) {
    const result = await ask(policy.collection_index, policy.collection_timeout_ms, policy.agent, fetcher);
    if (result.status !== 200) return { rows: [], summary: { asked: 0,
      why: 'the index did not say what collections it has' }, failed: true };
    collection = JSON.parse(result.body)[0]?.id;
    if (!collection) throw new Error('collection index has no first collection id');
  }
  const feedish = new RegExp(policy.feedish, 'i'), excluded = new RegExp(policy.excluded, 'i');
  const seen = new Set(), rows = [];
  let asked = 0;
  for (const { domain, wants } of policy.dorks) for (const want of wants) {
    asked++;
    const query = `https://index.commoncrawl.org/${collection}-index?url=` +
      `${encodeURIComponent(domain + want + '*')}&output=json&limit=${perDork}`;
    const answer = await ask(query, policy.query_timeout_ms, policy.agent, fetcher);
    let found = 0;
    if (answer.status === 200) for (const line of answer.body.split(/\r?\n/)) {
      if (!line.trimStart().startsWith('{')) continue;
      let row;
      try { row = JSON.parse(line); } catch { continue; }
      const address = row.url || '';
      if (!/^https?:\/\//i.test(address) || excluded.test(address) || !feedish.test(address) ||
        !policy.observed_statuses.includes(String(row.status)) || seen.has(address)) continue;
      const host = hostOf(address);
      if (!host) continue;
      seen.add(address);
      rows.push({ root: host, address, protocol: address.toLowerCase().endsWith('.ics') ? 'ics' : 'rss',
        title: '', truth_level: policy.candidate_truth,
        discovered_from: `listed in the ${collection} web index as ${row['mime-detected'] || 'a document'}` });
      found++;
    }
    if (options.progress) options.progress(domain, want, answer.status, found);
    if (pause) await Bun.sleep(pause);
  }
  return { rows, summary: { dorks_asked: asked, collection, addresses_listed: rows.length,
    publishers: new Set(rows.map(row => row.root)).size, cannot_infer: policy.cannot_infer }, failed: false };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index += 2) {
      if (!argv[index]?.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[argv[index].slice(2)] = argv[index + 1];
    }
    if (!args.out) throw new Error('--out is required');
    const result = await discoverCommonCrawl({ collection: args.collection,
      perDork: args['per-dork'] ? Number(args['per-dork']) : undefined,
      pauseMs: args.pause ? Number(args.pause) * 1000 : undefined,
      progress: (domain, want, status, found) =>
        console.error(`${domain.padEnd(22)} ${want.padEnd(10)} ${status === 200 ? '' : `(${status}) `}${found}`) });
    if (!result.failed) writeFileSync(args.out, result.rows.map(row => JSON.stringify(row)).join('\n') +
      (result.rows.length ? '\n' : ''));
    console.log(JSON.stringify(result.summary));
    if (result.failed) process.exitCode = 1;
  } catch (error) { console.error(`common crawl discovery: ${error.message}`); process.exitCode = 2; }
}
