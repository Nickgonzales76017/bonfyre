// Exercise the repository-owned native WorkGraph checkpoint interface in isolation.
// No suppliers, instruments, external effects, permissions or live state are changed.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';

const repo = resolve(fileURLToPath(new URL('../..', import.meta.url)));
const bin = join(repo, 'cmd/BonfyreQueue/bonfyre-queue');
const digest = bytes => createHash('sha256').update(bytes).digest('hex');

function call(argv, ok = true) {
  const result = spawnSync(bin, argv, { encoding: 'utf8', timeout: 15000, maxBuffer: 131072 });
  assert.ifError(result.error);
  if (!ok) return result;
  assert.equal(result.status, 0, String(result.stderr || result.stdout));
  const lines = result.stdout.trim().split(/\r?\n/);
  assert.equal(lines.length, 1, 'one native JSON response per call');
  return JSON.parse(lines[0]);
}

test('simulation checkpoint binds one source and one reviewed result to native WorkGraph', () => {
  const help = spawnSync(bin, ['--help'], { encoding: 'utf8', timeout: 8000 });
  assert.ifError(help.error);
  assert.match((help.stdout || '') + (help.stderr || ''), /checkpoint-save\s+MISSION/);
  assert.match((help.stdout || '') + (help.stderr || ''), /--checkpoint-revision/);

  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-research-checkpoint-'));
  try {
    const db = join(dir, 'workgraph.sqlite');
    const source = join(dir, 'synthetic-source.json');
    const result = join(dir, 'synthetic-result.json');
    const scenario = Buffer.from(JSON.stringify({
      schema: 'bonfyre.research.synthetic.v1',
      simulation: true,
      name: 'source-to-result lifecycle only',
      supplier_effects: 0,
      hardware_effects: 0
    }) + '\n');
    writeFileSync(source, scenario, { flag: 'wx', mode: 0o600 });
    const resultBytes = Buffer.from(JSON.stringify({
      schema: 'bonfyre.research.result.v1',
      simulation: true,
      source_sha256: digest(scenario),
      observed: 'fixture checked, no external effects'
    }) + '\n');
    writeFileSync(result, resultBytes, { flag: 'wx', mode: 0o600 });
    const mission = 'research-native-checkpoint';
    const worker = 'research-fixture-worker';
    const inp = pathToFileURL(source).href, out = pathToFileURL(result).href;

    const enq = call(['enqueue', 'research-simulation', inp, '--mission', mission,
      '--family', 'queue', '--retries', '0', '--db', db]);
    assert.equal(enq.mission_id, mission);
    assert.equal(enq.node_id, 'job');
    const claim = call(['claim', mission, 'job', worker, '--family', 'queue', '--lease-ms', '120000', '--db', db]);
    assert.match(claim.claim_token, /^[a-f0-9]{64}$/i);

    const checkpoint = call(['checkpoint-save', mission, 'job', worker, claim.claim_token,
      '--expected-revision', '0', '--artifact-uri', out, '--db', db]);
    assert.equal(checkpoint.revision, 1);
    assert.equal(checkpoint.artifact_uri, out);

    const stale = call(['ack', mission, 'job', worker, claim.claim_token,
      '--checkpoint-revision', '0', '--output-uri', out, '--db', db], false);
    assert.notEqual(stale.status, 0, 'stale revision must be refused');

    const ack = call(['ack', mission, 'job', worker, claim.claim_token,
      '--checkpoint-revision', '1', '--output-uri', out, '--db', db]);
    assert.equal(ack.result, 'ok');
    // Completed work stays completed: a replay claim is not another effect.
    const savedAgain = call(['checkpoint-read', mission, 'job', '--db', db]);
    assert.equal(savedAgain.revision, 1, 'checkpoint revision is reusable');
    assert.equal(savedAgain.artifact_uri, out, 'the reviewed artifact identity survives completion');
    const replay = call(['claim', mission, 'job', worker, '--family', 'queue', '--db', db], false);
    assert.notEqual(replay.status, 0, 'completed work cannot be claimed again');

    const final = call(['mission', mission, '--db', db]);
    assert.equal(final.status, 'complete');
    assert.equal(final.node_id, 'job');
    assert.equal(final.output_uri, out);
    assert.ok(final.receipt_id, 'durable native completion receipt');
    assert.equal(digest(readFileSync(result)), digest(resultBytes));
    assert.equal(JSON.parse(readFileSync(result, 'utf8')).source_sha256, digest(readFileSync(source)));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
