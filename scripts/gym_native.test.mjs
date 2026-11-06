import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, readFile, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { digestCohort, selectTwins } from './gym_contract.mjs';
import { sourceCeiling, marketCeiling, buildGymReceipt } from './gym_run.mjs';
import { executeRoute, runTrial } from './provider_trial.mjs';

function fixture() {
  const db = new Database(':memory:');
  db.exec("CREATE TABLE live_gym(gym TEXT, title TEXT, subject_kind TEXT, generator TEXT, validity_window TEXT, cohort_size INTEGER, cannot_infer TEXT, status TEXT); INSERT INTO live_gym VALUES ('g','Gym','source','foreign_twins','days',2,'a;b','published'); CREATE TABLE provider_route(route TEXT, capability TEXT, kind TEXT, endpoint TEXT, credential_env TEXT, unit_cost_usd TEXT, status TEXT); INSERT INTO provider_route VALUES ('direct','discovery.direct_source','direct_source','','','','published'); INSERT INTO provider_route VALUES ('completion','completion.chat','external','','','','published');");
  return db;
}

test('cohort identity ignores order and capability; selection is stable', () => {
  expect(digestCohort(['b', 'a'], 'g')).toBe(digestCohort(['a', 'b'], 'g'));
  expect(digestCohort(['a', 'b'], 'g')).not.toBe(digestCohort(['a', 'b'], 'h'));
  expect(selectTwins({ twins: [{ url: 'https://b', last_seen: '2026-09-02' }, { url: 'https://a', last_seen: '2026-09-01' }] }, 1)[0].url).toBe('https://a');
});

test('ceilings require scoped executed evidence, not declared routes or credentials', () => {
  expect(marketCeiling([{ route: 'completion', capability: 'completion.chat', kind: 'external' }], []).rung).toBe('admissible');
  expect(marketCeiling([{ route: 'search', capability: 'retrieval.search', kind: 'external' }], []).rung).toBe('executed');
  expect(sourceCeiling([]).measured).toBe(false);
  expect(sourceCeiling([{ capability: 'discovery.direct_source', outcome: 'retrieved', evidence_digest: 'same' }, { capability: 'discovery.direct_source', outcome: 'retrieved', evidence_digest: 'same' }]).value).toBe(1);
});

test('gym receipt counts only same-cohort trials and keeps action boundary', async () => {
  const db = fixture(), contract = db.query('SELECT * FROM live_gym').get(), now = Date.parse('2026-09-30T00:00:00Z');
  const twins = [{ url: 'https://a', last_seen: '2026-09-29T23:00:00Z', ever_retained: true }];
  const digest = digestCohort(['https://a'], 'g');
  const receipt = await buildGymReceipt(contract, twins, [], [{ gym: 'g', cohort_digest: digest, capability: 'discovery.direct_source', outcome: 'retrieved', evidence_digest: 'x' }, { gym: 'g', cohort_digest: 'old', capability: 'discovery.direct_source', outcome: 'retrieved', evidence_digest: 'y' }], async (_name, args) => ({ code: 0, stdout: args[1] === 'duplicates' ? '{"duplicates":[]}' : '{"admissible":1,"observations":1}', stderr: '' }), now);
  expect(receipt.ceilings.DirectSourceCeiling.value).toBe(1);
  expect(receipt.ceilings.DirectSourceCeiling.of).toBe(1);
  expect(receipt.ceilings.ActionCeiling.measured).toBe(false);
  db.close();
});

test('trial defaults to no calls/writes; apply writes scoped receipt', async () => {
  const db = fixture(), dir = await mkdtemp(join(tmpdir(), 'bonfyre-trial-'));
  const cohort = [{ url: 'https://a' }];
  let calls = 0;
  const runner = async () => { calls++; return { code: 0, stdout: '{"http_status":200,"content_sha256":"abc"}' }; };
  const dry = await runTrial({ capability: 'discovery.direct_source', gym: 'g', cohort, db, dir, runner });
  expect(dry.receipts_written).toBe(0);
  expect(calls).toBe(0);
  expect(await readdir(dir)).toEqual([]);
  const applied = await runTrial({ capability: 'discovery.direct_source', gym: 'g', cohort, db, dir, runner, apply: true });
  expect(applied.receipts_written).toBe(1);
  expect(calls).toBe(1);
  const file = (await readdir(dir))[0];
  const receipt = JSON.parse(await readFile(join(dir, file), 'utf8'));
  expect(receipt.cohort_digest).toBe(dry.cohort_digest);
  expect(receipt.evidence_digest).toBe('abc');
  expect(receipt.outcome).toBe('retrieved');
  await expect(runTrial({ capability: 'discovery.direct_source', gym: 'g', cohort, db, dir, runner, apply: true })).rejects.toThrow('already exists');
  expect(calls).toBe(1);
  db.close();
});

test('direct source missing evidence is not invented', async () => {
  const result = await executeRoute({ kind: 'direct_source' }, 'https://a', async () => ({ code: 0, stdout: '{"http_status":200}' }));
  expect(result.outcome).toBe('retrieved');
  expect(result.evidence_digest).toBe('');
});
