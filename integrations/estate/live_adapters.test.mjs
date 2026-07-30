import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { createLiveAdapterRuntime, createReceiptJournal, probeLiveAdapters } from './live_adapters.mjs';

function fakeSpawn(_command, _args) {
  const child = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = () => true;
  setImmediate(() => { child.stdout.emit('data', 'healthy'); child.emit('close', 0, null); });
  return child;
}

const probes = await probeLiveAdapters({ adapters: [
  { id: 'cavemem', command_path: '/mock/cavemem', args: ['status'], healthyExitCodes: [0] },
  { id: 'lance', command_path: null, args: ['--version'], healthyExitCodes: [0] },
], spawnImpl: fakeSpawn });
assert.deepEqual(probes.map((probe) => probe.status), ['passed', 'skipped']);
assert.ok(probes.every((probe) => probe.receipt_sha256));
assert.equal(probes[0].ledger.event, 'adapter.probe.completed');
assert.equal(probes[0].analytics.measures.success, 1);
const temp = await mkdtemp('/tmp/bonfyre-journal-');
try {
  const journal = createReceiptJournal({ file: join(temp, 'receipts.jsonl') });
  const record = await journal.append(probes[0]);
  assert.equal(record.schema_version, 'bonfyre.receipt.journal.v1');
  assert.equal((await journal.read()).length, 1);
  const runtime = createLiveAdapterRuntime({ pathValue: '/not-installed', spawnImpl: fakeSpawn, receiptJournal: journal });
  const all = await runtime.probe();
  assert.equal(all[0].status, 'skipped');
  assert.equal(runtime.observations().length, all.length);
  assert.equal((await runtime.probe({ ids: ['cavemem'] })).length, 1);
  assert.equal((await journal.read()).length, 1 + all.length + 1);
} finally { await rm(temp, { recursive: true, force: true }); }
console.log('live adapter tests passed');
