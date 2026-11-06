// Every local model load asks the shared memory ledger first and gives the
// grant back afterwards; a load that cannot fit is refused with the reason.
import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, truncateSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { acquireModelMemory, defaultModelAdmission, ModelNotAdmittedError } from './model_admission.mjs';

function fixture(bytes) {
  const dir = mkdtempSync(join(tmpdir(), 'model-admission-')), model = join(dir, 'model.gguf');
  writeFileSync(model, ''); truncateSync(model, bytes);
  return { dir, model, admission: { ...defaultModelAdmission(), db: join(dir, 'admission.db') } };
}

test('a model that fits takes a memory grant and releases it once', () => {
  const { dir, model, admission } = fixture(2 ** 30);
  try {
    const grant = acquireModelMemory({ modelPath: model, plane: 'test', admission, freeBytes: 8 * 2 ** 30 });
    const db = new Database(admission.db);
    expect(db.query('SELECT volume, estimated_bytes, released_at FROM resource_grants').get())
      .toEqual({ volume: 'memory', estimated_bytes: grant.bytes, released_at: null });
    grant.release(); grant.release();
    expect(db.query('SELECT count(*) AS open FROM resource_grants WHERE released_at IS NULL').get().open).toBe(0);
    db.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a model that does not fit is refused before anything loads', () => {
  const { dir, model, admission } = fixture(6 * 2 ** 30);
  try {
    let refused;
    try { acquireModelMemory({ modelPath: model, plane: 'test', admission, freeBytes: 4 * 2 ** 30 }); }
    catch (error) { refused = error; }
    expect(refused).toBeInstanceOf(ModelNotAdmittedError);
    expect(refused.code).toBe('MODEL_NOT_ADMITTED');
    expect(refused.message).toContain('memory admission said');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
