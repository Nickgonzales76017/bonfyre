// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const temporary = [];
afterEach(() => { for (const path of temporary.splice(0)) rmSync(path, { recursive: true, force: true }); });
const sha = value => createHash('sha256').update(value).digest('hex');
function fixture() {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-liveness-test-'));
  temporary.push(base);
  const estate = join(base, 'estate'), sweep = join(base, 'sweep'), receipts = join(sweep, 'receipts');
  mkdirSync(estate); mkdirSync(sweep); mkdirSync(receipts);
  return { base, estate, sweep, receipts };
}
function run(env, args = ['60', '0']) {
  return Bun.spawnSync([join(root, 'tools/route-liveness.sh'), ...args], { env: { ...process.env, ...env } });
}

test('actual route-liveness entrypoint measures one per host with offline curl mock', () => {
  const { base, estate, sweep, receipts } = fixture();
  const observations = [
    ['still', 'https://a.example/one', sha('same')],
    ['duplicate', 'https://a.example/two', sha('same')],
    ['moved', 'https://b.example/item', sha('before')],
    ['gone', 'https://c.example/item', sha('before')],
  ];
  writeFileSync(join(estate, 'enactment-evidence.jsonl'), observations.map(([id]) =>
    JSON.stringify({ occurrence_id: id, state: 'obligations_closed' })).join('\n') + '\n');
  for (const [id, url, digest] of observations)
    writeFileSync(join(receipts, `${id}.json`), JSON.stringify({ source_url: url, content_sha256: digest }) + '\n');
  const curl = join(base, 'curl-mock'), log = join(base, 'calls.log');
  writeFileSync(curl, `#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) body=$2; shift 2 ;;
    https:*) url=$1; shift ;;
    *) shift ;;
  esac
done
printf '%s\\n' "$url" >> "$MOCK_CURL_LOG"
case "$url" in
  https://a.example/*) printf same > "$body"; printf 200 ;;
  https://b.example/*) printf changed > "$body"; printf 200 ;;
  *) exit 7 ;;
esac
`);
  chmodSync(curl, 0o755);
  const result = run({ ESTATE_DIR: estate, SWEEP_DIR: sweep, BONFYRE_CURL_BINARY: curl, MOCK_CURL_LOG: log });
  expect(result.exitCode).toBe(0);
  const summary = JSON.parse(result.stdout.toString());
  expect(summary).toMatchObject({ sampled: 3, still_answering: 1, moved: 1, gone: 1,
    routable: 2, routable_share: 0.6667, invalid_receipts: 0, missing_receipts: 0,
    probe_failures: 1 });
  const rows = readFileSync(join(estate, 'liveness.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  expect(rows).toHaveLength(3);
  expect(readFileSync(log, 'utf8').trim().split('\n')).toHaveLength(3);
  expect(rows.filter(row => ['still', 'duplicate'].includes(row.occurrence_id))).toHaveLength(1);
  expect(summary.cannot_infer).toContain('a sample -> the whole table');
});

test('malformed retained enactment refuses without replacing prior liveness report', () => {
  const { estate, sweep } = fixture();
  writeFileSync(join(estate, 'enactment-evidence.jsonl'), '{bad json\n');
  writeFileSync(join(estate, 'liveness.jsonl'), 'prior-valid-report\n');
  const result = run({ ESTATE_DIR: estate, SWEEP_DIR: sweep }, ['1', '0']);
  expect(result.exitCode).not.toBe(0);
  expect(readFileSync(join(estate, 'liveness.jsonl'), 'utf8')).toBe('prior-valid-report\n');
});

test('null and array receipt rows are counted invalid without probing', () => {
  const { estate, sweep, receipts } = fixture();
  writeFileSync(join(estate, 'enactment-evidence.jsonl'),
    '{"occurrence_id":"null-row","state":"obligations_closed"}\n' +
    '{"occurrence_id":"array-row","state":"obligations_closed"}\n');
  writeFileSync(join(receipts, 'null-row.json'), 'null\n');
  writeFileSync(join(receipts, 'array-row.json'), '[]\n');
  const result = run({ ESTATE_DIR: estate, SWEEP_DIR: sweep }, ['2', '0']);
  expect(result.exitCode).toBe(0);
  expect(JSON.parse(result.stdout.toString())).toMatchObject({ sampled: 0, invalid_receipts: 2 });
});
