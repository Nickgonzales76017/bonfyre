import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, symlinkSync, readdirSync, truncateSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { captureQuotes } from '../tools/economic-reports.mjs';

const cleanups = [];
afterEach(() => { for (const path of cleanups.splice(0)) rmSync(path, { recursive: true, force: true }); });
const digest = text => createHash('sha256').update(text).digest('hex');
function fixture() {
  const path = mkdtempSync(join(tmpdir(), 'bonfyre-economic-'));
  cleanups.push(path);
  const bin = join(path, 'bin'), estate = join(path, 'estate'), grants = join(path, 'grants'), quotes = join(path, 'quotes');
  for (const dir of [bin, estate, grants, quotes]) mkdirSync(dir);
  symlinkSync(process.execPath, join(bin, 'bun'));
  symlinkSync('/usr/bin/dirname', join(bin, 'dirname'));
  const trap = join(path, 'forbidden-command');
  for (const cmd of ['python', 'python3', 'curl', 'wget', 'ssh', 'bonfyre-project'])
    writeFileSync(join(bin, cmd), '#!/bin/sh\nprintf forbidden > "$FORBIDDEN_LOG"\nexit 93\n', { mode: 0o755 });
  return { path, estate, grants, quotes, trap, run(name, args = []) {
    const script = fileURLToPath(new URL(`../tools/${name}.sh`, import.meta.url));
    const result = Bun.spawnSync(['/bin/sh', script, ...args], {
      cwd: path, env: { ...process.env, PATH: bin, ESTATE_DIR: estate, FORBIDDEN_LOG: trap },
    });
    expect(readdirSync(path)).not.toContain('forbidden-command');
    return { code: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString() };
  } };
}
function quote(f, name, body, status = 'HTTP/1.1 402 Payment Required', extra = '') {
  writeFileSync(join(f.quotes, `${name}.body`), body);
  writeFileSync(join(f.quotes, `${name}.headers`), `${status}\r\n${extra}\r\n`);
}
const rows = path => readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));

test('grant shell retains legacy residual counts and discloses unverified heuristic inputs', () => {
  const f = fixture(), out = join(f.path, 'grant report.json');
  writeFileSync(join(f.estate, 'enactment-evidence.jsonl'), [
    { occurrence_id: 'a', state: 'obligations_closed' }, { occurrence_id: 'a', state: 'obligations_closed' },
    { occurrence_id: 'b', unrelated: 'obligations_closed' }, { occurrence_id: 'c', state: 'read' },
  ].map(row => JSON.stringify(row)).join('\n'));
  // Mere file presence deliberately remains the legacy signal, never verified.
  writeFileSync(join(f.estate, 'attestation.json'), 'not a valid attestation');
  writeFileSync(join(f.grants, 'opp-a.json'), JSON.stringify({ data: { synopsis: {
    opportunityTitle: '😀'.repeat(61), agencyName: 'agency', applicantTypes: [{ description: 'university' }],
    costSharing: true, responseDate: '2030-01-01', fundingActivityCategories: [{}],
  } } }));
  writeFileSync(join(f.grants, 'opp-b.json'), JSON.stringify({ data: { synopsis: { opportunityTitle: 'No listed residuals' } } }));
  writeFileSync(join(f.grants, 'opp-empty.json'), '{}');
  const result = f.run('grant-closure', [f.grants, out]);
  expect(result.code).toBe(0);
  const report = JSON.parse(readFileSync(out, 'utf8'));
  expect(report.opportunities_assessed).toBe(2);
  expect(report.covered_with_no_gaps).toBe(1);
  expect(report.evidence_class_covered).toBe(1);
  expect(report.blocked_by_class).toEqual({ PartnerResidual: 1, CapitalResidual: 1, TemporalResidual: 1 });
  expect(report.estate_capabilities_applied).toEqual({ sources_proven_readable: 2, signed_attestation: true, provenance_chain_depth: 5 });
  expect(report.assessment_basis.signed_attestation).toContain('not verified');
  expect(report.assessment_basis.evidence).toContain('not grant-specific');
  expect(report.opportunities[0].title).toBe('😀'.repeat(60));
  expect(report.opportunities[1].evidence_class_covered).toBe(false);
  expect(JSON.parse(result.stdout).opportunities).toBeUndefined();
});

test('grant shell preserves prior output on malformed input and refuses symlink observations', () => {
  const f = fixture(), out = join(f.path, 'report.json');
  writeFileSync(join(f.estate, 'enactment-evidence.jsonl'), '');
  writeFileSync(out, 'prior');
  writeFileSync(join(f.grants, 'opp-bad.json'), '{');
  expect(f.run('grant-closure', [f.grants, out]).code).toBe(1);
  expect(readFileSync(out, 'utf8')).toBe('prior');
  rmSync(join(f.grants, 'opp-bad.json'));
  symlinkSync(out, join(f.grants, 'opp-link.json'));
  expect(f.run('grant-closure', [f.grants, out]).code).toBe(1);
  expect(readFileSync(out, 'utf8')).toBe('prior');
});

test('grant shell refuses structured dates instead of manufacturing a temporal description', () => {
  const f = fixture(), out = join(f.path, 'report.json');
  writeFileSync(join(f.estate, 'enactment-evidence.jsonl'), '');
  writeFileSync(out, 'prior');
  writeFileSync(join(f.grants, 'opp-date.json'), JSON.stringify({ data: { synopsis: { responseDate: { arbitrary: 'date' } } } }));
  const result = f.run('grant-closure', [f.grants, out]);
  expect(result.code).toBe(1);
  expect(result.stderr).toContain('invalid responseDate');
  expect(readFileSync(out, 'utf8')).toBe('prior');
});

test('quote shell records v1/v2 prices and recovers header envelopes with prose or malformed JSON body', () => {
  const f = fixture(), out = join(f.path, 'quote evidence.jsonl');
  const body = JSON.stringify({ accepts: [{ maxAmountRequired: '100000000000000000001', asset: 'token', network: 'chain', extra: { humanAmount: '0.1' } }] });
  quote(f, 'a-v1', body);
  quote(f, 'b-v2', JSON.stringify({ accepts: [{ amount: '200', asset: '0xabc', extra: { assetSymbol: 'USDC' }, network: 'chain:1' }] }));
  const encoded = Buffer.from(JSON.stringify({ accepts: [{ amount: '300', asset: 'token', network: 'chain:2' }] })).toString('base64').replace(/=+$/, '');
  quote(f, 'c-prose', 'Payment needed', 'HTTP/2 402', `PaYmEnT-ReQuIrEd: ${encoded}\r\n`);
  quote(f, 'd-json', '{', 'HTTP/1.1 402', `payment-required: ${encoded}\r\n`);
  const result = f.run('quote-capture', [f.quotes, out]);
  expect(result.code).toBe(0);
  expect(JSON.parse(result.stdout).quotes_recorded).toBe(4);
  const records = rows(out);
  expect(records.map(row => row.envelope_source)).toEqual(['body', 'body', 'payment-required', 'payment-required']);
  expect(records[0].amount).toBe('100000000000000000001');
  expect(records[0].basis_sha256).toBe(digest(body));
  expect(records[0].evidence_sha256).toBe(digest(['external:a-v1', '100000000000000000001', 'token', 'chain', 'quoted'].join('\x1f')));
  expect(records[1].asset).toBe('USDC');
  expect(records[2].headers_sha256).toBe(digest(readFileSync(join(f.quotes, 'c-prose.headers'))));
  for (const record of records) {
    expect(record.state).toBe('quoted'); expect(record.spend).toBe(0);
    expect(record.cannot_infer.join(' ')).toContain('those move money and this did not');
  }
});

test('quote shell excludes non-402 responses, malformed envelopes and inexact numeric prices', () => {
  const f = fixture(), out = join(f.path, 'quotes.jsonl');
  const valid = JSON.stringify({ accepts: [{ amount: '42' }] });
  quote(f, 'wrong-status', valid, 'HTTP/1.1 200 reason includes 402');
  quote(f, 'inexact', '{"accepts":[{"amount":9007199254740993}]}');
  quote(f, 'object', '{"accepts":[{"amount":{"nested":"price"}}]}');
  quote(f, 'missing', '{"accepts":[{}]}');
  quote(f, 'badbase64', 'prose', 'HTTP/2 402', 'payment-required: not base64!');
  quote(f, 'array', '[]');
  const result = f.run('quote-capture', [f.quotes, out]);
  expect(result.code).toBe(0);
  expect(JSON.parse(result.stdout).quotes_recorded).toBe(0);
  expect(readFileSync(out, 'utf8')).toBe('');
});

test('quote shell fails atomically if a later observation is not a regular file', () => {
  const f = fixture(), out = join(f.path, 'quotes.jsonl');
  writeFileSync(out, 'prior');
  quote(f, 'a-valid', '{"accepts":[{"amount":"42"}]}');
  mkdirSync(join(f.quotes, 'z-directory.body'));
  expect(f.run('quote-capture', [f.quotes, out]).code).toBe(1);
  expect(readFileSync(out, 'utf8')).toBe('prior');
  expect(readdirSync(f.path).some(name => name.endsWith('.tmp'))).toBe(false);
});

test('quote receipts are deterministic given the same observation time and bytes', () => {
  const f = fixture(), out = join(f.path, 'quotes.jsonl');
  quote(f, 'one', '{"accepts":[{"amount":"42"}]}');
  const now = new Date('2026-10-02T00:00:00Z');
  captureQuotes(f.quotes, out, now);
  const before = readFileSync(out, 'utf8');
  captureQuotes(f.quotes, out, now);
  expect(readFileSync(out, 'utf8')).toBe(before);
});

test('entrypoints reject oversized observations before allocation and retain previous output', () => {
  const f = fixture(), out = join(f.path, 'report');
  writeFileSync(out, 'prior');
  const body = join(f.quotes, 'oversized.body');
  writeFileSync(body, ''); truncateSync(body, 16 * 1024 * 1024 + 1);
  const quoteResult = f.run('quote-capture', [f.quotes, out]);
  expect(quoteResult.code).toBe(1);
  expect(quoteResult.stderr).toContain('regular-file budget');
  expect(readFileSync(out, 'utf8')).toBe('prior');
  const evidence = join(f.estate, 'enactment-evidence.jsonl');
  writeFileSync(evidence, ''); truncateSync(evidence, 64 * 1024 * 1024 + 1);
  const grantResult = f.run('grant-closure', [f.grants, out]);
  expect(grantResult.code).toBe(1);
  expect(grantResult.stderr).toContain('regular-file budget');
  expect(readFileSync(out, 'utf8')).toBe('prior');
});
