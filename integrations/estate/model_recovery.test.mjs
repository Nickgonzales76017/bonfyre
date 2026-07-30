import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { verifyModelPack } from './generation_fabric.mjs';

const directory = await mkdtemp('/tmp/bonfyre-model-recovery-');
try {
  const part = join(directory, 'layer-000.fpq');
  const bytes = 'verified model bytes';
  await writeFile(part, bytes);
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  const pack = join(directory, 'model.fpq-pack.json');
  await writeFile(pack, JSON.stringify({ model_repo: 'bonfyre/test', parts_dir: '.', parts: [{ path: 'layer-000.fpq', sha256 }] }));
  const verified = await verifyModelPack({ modelPack: pack });
  assert.equal(verified.ready, true);
  assert.equal(verified.missing_parts, 0);
  assert.equal(verified.unverified_parts, 0);
  await writeFile(part, 'corrupted model bytes');
  const corrupt = await verifyModelPack({ modelPack: pack });
  assert.equal(corrupt.ready, false);
  assert.equal(corrupt.hash_mismatches, 1);
  await writeFile(pack, JSON.stringify({ model_repo: 'bonfyre/test', parts_dir: '.', parts: [{ path: 'layer-000.fpq' }] }));
  const unsigned = await verifyModelPack({ modelPack: pack });
  assert.equal(unsigned.ready, false);
  assert.equal(unsigned.unverified_parts, 1);
  assert.equal((await verifyModelPack({ modelPack: pack, verifyHashes: false })).ready, true);
} finally {
  await rm(directory, { recursive: true, force: true });
}
console.log('model recovery tests passed');
