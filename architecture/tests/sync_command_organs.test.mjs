import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const script = resolve(dirname(fileURLToPath(import.meta.url)), '../sync-command-organs.mjs');

test('sync appends missing command evidence while preserving authored Atlas law', () => {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-command-sync-'));
  try {
    mkdirSync(join(root, 'architecture/commands'), { recursive: true });
    mkdirSync(join(root, 'cmd/BonfyreOld/src'), { recursive: true });
    mkdirSync(join(root, 'cmd/BonfyreNew/src'), { recursive: true });
    mkdirSync(join(root, 'cmd/BonfyreNew/tests'));
    writeFileSync(join(root, 'cmd/BonfyreOld/src/main.c'), 'int main(void) { return 0; }\n');
    writeFileSync(join(root, 'cmd/BonfyreNew/src/main.c'), 'int main(void) { return 0; }\n');
    writeFileSync(join(root, 'cmd/command_smoke.json'), JSON.stringify({ results: [{ command: 'BonfyreNew', responds: true, binary: 'bonfyre-new' }] }));
    const target = join(root, 'architecture/commands/command-surface.yaff');
    const authored = 'atlas command\narchitecture cmd-old\n  canonical_name BonfyreOld\n  forbidden_inference authored-law-must-survive\n';
    writeFileSync(target, authored);
    const run = (...args) => Bun.spawnSync([process.execPath, script, ...args, `--root=${root}`]);
    expect(run('--check').exitCode).toBe(1);
    expect(run().exitCode).toBe(0);
    const first = readFileSync(target, 'utf8');
    expect(first).toContain(authored.trimEnd());
    expect(first).toContain('architecture cmd-new');
    expect(first).toContain('  maturity measured');
    expect(first).toContain('  source_path cmd/BonfyreNew/src/main.c');
    expect(first).toContain('the command exists -> its effect is authorized');
    expect(run('--check').exitCode).toBe(0);
    expect(run().exitCode).toBe(0);
    expect(readFileSync(target, 'utf8')).toBe(first);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
