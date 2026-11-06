import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('target-host install stages the complete native membrane runtime without Python', () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-membrane-install.'));
  try {
    const root = join(directory, 'native-src');
    const built = Bun.spawnSync(['make', '-C', new URL('..', import.meta.url).pathname,
      `MEMBRANE_ROOT=${root}`, 'install-membrane']);
    expect(built.exitCode).toBe(0);
    const script = join(root, 'scripts', 'foreign-discover.mjs');
    expect(statSync(script).mode & 0o111).not.toBe(0);
    const invocation = Bun.spawnSync([script]);
    expect(invocation.exitCode).toBe(2);
    expect(new TextDecoder().decode(invocation.stderr)).toContain('usage: foreign-discover.mjs');
    const dbPath = join(directory, 'membrane.db');
    const stored = Bun.spawnSync(['bun', join(root, 'scripts', 'membrane-store.mjs'), 'summary'],
      { env: { ...process.env, BONFYRE_MEMBRANE_DB: dbPath } });
    expect(stored.exitCode).toBe(0);
    expect(JSON.parse(new TextDecoder().decode(stored.stdout)).inbound.sources).toBe(0);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
