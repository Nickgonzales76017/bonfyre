#!/usr/bin/env bun
// Sample retained, proven-readable routes. A sample never proves the whole table.
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { atomicReport, observationReader } from './economic-reports.mjs';

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const safeName = value => value.replace(/[^A-Za-z0-9._-]/g, '_');
const estateDir = () => process.env.ESTATE_DIR || join(homedir(), '.bonfyre/estate');
const sweepDir = () => process.env.SWEEP_DIR || join(homedir(), '.bonfyre/foreign-sweep');

function lines(bytes) { return bytes.toString('utf8').split(/\r?\n/).filter(Boolean); }
export function planLiveness({ estate = estateDir(), sweep = sweepDir(), sample = 60 } = {}) {
  if (!Number.isSafeInteger(sample) || sample < 0 || sample > 100000)
    throw new Error('sample must be an integer from 0 to 100000');
  const read = observationReader();
  const free = new Set();
  for (const line of lines(read(join(estate, 'enactment-evidence.jsonl'), { limit: 64 * 1024 * 1024 }))) {
    const row = JSON.parse(line);
    if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error('invalid enactment evidence row');
    if (row.state === 'obligations_closed') {
      if (typeof row.occurrence_id !== 'string' || !row.occurrence_id) throw new Error('closed enactment has no identity');
      free.add(row.occurrence_id);
    }
  }
  const seenHosts = new Set(), candidates = [];
  let invalidReceipts = 0, missingReceipts = 0;
  for (const id of [...free].sort()) {
    const bytes = read(join(sweep, 'receipts', `${safeName(id)}.json`), { optional: true });
    if (!bytes.length) { missingReceipts++; continue; }
    const newest = lines(bytes).filter(line => /^(?:\{|\[|null(?:\s|$))/.test(line)).at(-1);
    if (!newest) { invalidReceipts++; continue; }
    let row;
    try { row = JSON.parse(newest); }
    catch { invalidReceipts++; continue; }
    if (!row || typeof row !== 'object' || Array.isArray(row) ||
        typeof row.source_url !== 'string' || typeof row.content_sha256 !== 'string' ||
        !/^[a-f0-9]{64}$/i.test(row.content_sha256)) { invalidReceipts++; continue; }
    let url;
    try { url = new URL(row.source_url); }
    catch { invalidReceipts++; continue; }
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || !url.host) {
      invalidReceipts++; continue;
    }
    if (seenHosts.has(url.host)) continue;
    seenHosts.add(url.host);
    candidates.push({ occurrence_id: id, url: url.href, digest: row.content_sha256.toLowerCase(),
      rank: sha256(`20260913\x1f${id}`) });
  }
  // Reproducible host-balanced sample; unlike the historical PRNG shuffle,
  // rank is stable across Python/Bun runtimes and source-order changes.
  candidates.sort((a, b) => a.rank.localeCompare(b.rank));
  return { rows: candidates.slice(0, sample), invalid_receipts: invalidReceipts,
    missing_receipts: missingReceipts };
}

function shareRoundedHalfEven(numerator, denominator) {
  if (!denominator) return 0;
  const scaled = numerator * 10000;
  const lower = Math.floor(scaled / denominator), rest = scaled % denominator;
  const rounded = rest * 2 > denominator || (rest * 2 === denominator && lower % 2 === 1) ? lower + 1 : lower;
  return rounded / 10000;
}

export async function measureLiveness({ estate = estateDir(), sweep = sweepDir(), sample = 60, delay = 0.4,
  curl = process.env.BONFYRE_CURL_BINARY || 'curl' } = {}) {
  if (typeof delay !== 'number' || !Number.isFinite(delay) || delay < 0 || delay > 60)
    throw new Error('delay must be a finite number of seconds from 0 to 60');
  const plan = planLiveness({ estate, sweep, sample });
  const read = observationReader();
  const temp = mkdtempSync(join(tmpdir(), 'bonfyre-liveness-'));
  const rows = [];
  let still = 0, moved = 0, gone = 0, probeFailures = 0;
  try {
    for (const item of plan.rows) {
      const body = join(temp, 'body');
      let code = '000';
      try {
        const probe = Bun.spawnSync([curl, '-sS', '--max-time', '20', '--max-filesize', '16777216',
          '--user-agent', 'BonfyreProject/1.0', '-o', body, '-w', '%{http_code}', item.url]);
        const status = probe.stdout.toString().trim();
        if (probe.exitCode === 0 && /^\d{3}$/.test(status)) code = status;
        else probeFailures++;
      } catch { probeFailures++; }
      let state = 'gone';
      if (code === '200') {
        try {
          const digest = sha256(read(body));
          if (digest === item.digest) { state = 'still_answering'; still++; }
          else { state = 'moved'; moved++; }
        } catch { code = '000'; probeFailures++; }
      }
      if (state === 'gone') gone++;
      rows.push({ occurrence_id: item.occurrence_id, http_status: code, state });
      rmSync(body, { force: true });
      if (delay && rows.length < plan.rows.length) await Bun.sleep(delay * 1000);
    }
  } finally { rmSync(temp, { recursive: true, force: true }); }
  const total = still + moved + gone, routable = still + moved;
  const summary = { schema: 'bonfyre-route-liveness.v1', sampled: total, still_answering: still,
    moved, gone, routable, routable_share: shareRoundedHalfEven(routable, total),
    invalid_receipts: plan.invalid_receipts, missing_receipts: plan.missing_receipts,
    probe_failures: probeFailures,
    truth: 'one_host_per_sample_asked_again_and_compared_to_its_receipt',
    cannot_infer: [
      'a sample -> the whole table',
      'moved -> the source being wrong; a live source changes and that is what live means',
      'gone -> the source being dead; it may refuse this agent, this path, or this moment',
    ] };
  atomicReport(join(estate, 'liveness.jsonl'), write => {
    for (const row of rows) write(`${JSON.stringify(row)}\n`);
  });
  return summary;
}

if (import.meta.main) {
  try {
    const [sample = '60', delay = '0.4'] = process.argv.slice(2);
    const result = await measureLiveness({ sample: Number(sample), delay: Number(delay) });
    console.log(JSON.stringify(result));
  } catch (error) { console.error(`route liveness: ${error.message}`); process.exitCode = 1; }
}
