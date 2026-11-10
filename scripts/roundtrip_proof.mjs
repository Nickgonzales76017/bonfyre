#!/usr/bin/env bun
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const lawPath = fileURLToPath(new URL('./roundtrip-law.generated.json', import.meta.url));
export const law = JSON.parse(readFileSync(lawPath, 'utf8'));

async function observe(origin, address, fetcher) {
  try {
    const response = await fetcher(new URL(address, origin), {
      headers: { Accept: 'application/json', 'User-Agent': 'bonfyre-roundtrip/1' },
      signal: AbortSignal.timeout(20_000),
    });
    if (response.status !== 200) return { status: response.status, document: null, note: 'http error' };
    try {
      const document = await response.json();
      const object = document && typeof document === 'object' && !Array.isArray(document);
      return { status: response.status, document: object ? document : null, note: object ? '' : 'not object' };
    } catch {
      return { status: response.status, document: null, note: 'not json' };
    }
  } catch (error) {
    return { status: 0, document: null, note: String(error.message || error).slice(0, 60) };
  }
}

function host(value) {
  try { return new URL(value).host; } catch { return ''; }
}

export async function prove({ contract = law, origin = contract.origin, fetcher = fetch } = {}) {
  const observations = new Map();
  await Promise.all(contract.projections.map(async ({ address }) => {
    if (!observations.has(address)) observations.set(address, observe(origin, address, fetcher));
  }));
  const rows = [];
  for (const { carrier, address, required } of contract.projections) {
    const { status, document, note } = await observations.get(address);
    if (!document) {
      rows.push({ carrier, address, verdict: 'unreachable', detail: `${status} ${note}`.trim() });
      continue;
    }
    const missing = required.filter(field => !Object.hasOwn(document, field));
    rows.push({ carrier, address, verdict: missing.length ? 'drifted' : 'faithful', detail: missing.length ? `missing ${missing.join(',')}` : `${Object.keys(document).length} fields` });
  }
  for (const { name, kind, left, right } of contract.agreement) {
    const a = (await observations.get(left))?.document;
    const b = (await observations.get(right))?.document;
    if (!a || !b) {
      rows.push({ carrier: 'cross-carrier', address: name, verdict: 'unreachable', detail: 'required carrier missing' });
      continue;
    }
    if (kind === 'actor-webfinger') {
      const hrefs = Array.isArray(b.links) ? b.links.filter(link => link && typeof link === 'object').map(link => link.href) : [];
      const agree = typeof a.id === 'string' && hrefs.includes(a.id);
      rows.push({ carrier: 'cross-carrier', address: name, verdict: agree ? 'agree' : 'drifted', detail: agree ? a.id : `${a.id} not among ${JSON.stringify(hrefs)}` });
    } else if (kind === 'host') {
      const leftHost = host(a.id);
      const rightHost = host(b.url);
      rows.push({ carrier: 'cross-carrier', address: name, verdict: leftHost && leftHost === rightHost ? 'agree' : 'drifted', detail: leftHost && leftHost === rightHost ? leftHost : `${leftHost} vs ${rightHost}` });
    } else {
      throw new Error(`unknown agreement kind: ${kind}`);
    }
  }
  const faithful = rows.filter(row => row.verdict === 'faithful' || row.verdict === 'agree').length;
  return { schema: 'bonfyre-roundtrip.v1', origin, projections: rows, faithful, total: rows.length, forbidden_inference: contract.forbidden_inference };
}

export function formatProof(result, includeJson = false) {
  const width = Math.max(...result.projections.map(row => row.address.length));
  const marks = { faithful: ' ', agree: ' ', drifted: '!', unreachable: 'x' };
  const lines = result.projections.map(({ carrier, address, verdict, detail }) => ` ${marks[verdict]} ${carrier.padEnd(18)} ${address.padEnd(width)}  ${verdict.padEnd(11)} ${detail}`);
  lines.push('', `round trip: ${result.faithful}/${result.total} projections observed as published`);
  if (includeJson) lines.push(JSON.stringify(result, null, 2));
  return `${lines.join('\n')}\n`;
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const originAt = args.indexOf('--origin');
  if (originAt >= 0 && !args[originAt + 1]) throw new Error('--origin requires a URL');
  const result = await prove({ origin: originAt >= 0 ? args[originAt + 1] : law.origin });
  process.stdout.write(formatProof(result, args.includes('--json')));
  if (result.faithful !== result.total) process.exitCode = 1;
}
