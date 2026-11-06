import { test, expect } from 'bun:test';
import { decide as atlasDecision, touchesAtlas } from '../.claude/hooks/atlas-guard.mjs';
import { needsExport } from '../.claude/hooks/atlas-export-guard.mjs';
import { decide as treeDecision, rules } from '../.claude/hooks/live-tree-guard.mjs';
import { declared, named, recall } from '../.claude/hooks/atlas-recall.mjs';
import { resolve } from 'node:path';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('atlas edit guard applies only to YAFF entries and blocks invalid declarations', () => {
  expect(touchesAtlas({ tool_input: { file_path: '/repo/architecture/commons/x.yaff' } })).toBe(true);
  expect(touchesAtlas({ tool_input: { file_path: 'architecture/commons/x.yaff' } })).toBe(true);
  expect(touchesAtlas({ tool_input: { file_path: '/repo/architecture/readme.md' } })).toBe(false);
  expect(atlasDecision({ tool_input: { file_path: 'script.mjs' } }, () => { throw new Error('must not run'); })).toBe('');
  expect(atlasDecision({ tool_input: { file_path: 'architecture/a.yaff' } },
    () => ({ exitCode: 0, stdout: 'atlas valid\n' }))).toBe('atlas valid');
  const refusal = JSON.parse(atlasDecision({ tool_input: { file_path: 'architecture/a.yaff' } },
    () => ({ exitCode: 1, stdout: 'invalid claim', stderr: '' })));
  expect(refusal.decision).toBe('block');
  expect(refusal.reason).toContain('invalid claim');
});

test('authored destructive-command rules deny real hazards without denying reads', () => {
  expect(rules).toHaveLength(6);
  for (const command of [
    'umount -f /mnt/bonfyre', 'diskutil unmount force /mnt/bonfyre',
    'rm -rf /opt/bonfyre/releases/matrix-edge', 'git gc --prune=now',
    'git reset --hard HEAD', 'rm -rf /var/lib/bonfyre/auth',
    'sqlite3 fabric.db "UPDATE receipts SET ok=1"',
  ]) {
    expect(treeDecision({ tool_input: { command } })?.hookSpecificOutput.permissionDecision).toBe('deny');
  }
  for (const command of ['git status --short', 'sqlite3 fabric.db "SELECT * FROM receipts"', 'rm -rf /tmp/fixture'])
    expect(treeDecision({ tool_input: { command } })).toBeNull();
});

test('atlas recall is bounded to declared exact IDs and canonical names', () => {
  const names = new Map([['live-semantic-track', 'LiveSemanticTrack'], ['retained-body-service', 'RetainedBodyService'], ['x', 'Work'], ['publication-program', 'PublicationProgram'], ['protocol-library', 'ProtocolLibrary']]);
  expect(named('live-semantic-track, RetainedBodyService, PublicationProgram, ProtocolLibrary', names))
    .toEqual(['live-semantic-track', 'retained-body-service', 'publication-program']);
  expect(named('retained-body-service-other work', names)).toEqual([]);
  expect(recall('RetainedBodyService', names, id => ({ stdout: `entry:${id}` }))?.hookSpecificOutput.additionalContext)
    .toContain('entry:retained-body-service');
  expect(recall('unrelated sentence', names)).toBeNull();
  expect(declared().has('live-semantic-track')).toBe(true);
});

test('export guard can inspect the current native index without Python', () => {
  expect(typeof needsExport()).toBe('boolean');
});

test('configured entrypoints accept Claude JSON stdin and emit protocol-shaped decisions', () => {
  const repo = resolve(import.meta.dir, '..');
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-hook-witness-'));
  try {
    const path = join(dir, 'event.json');
    writeFileSync(path, JSON.stringify({ tool_input: { command: 'git reset --hard HEAD' } }));
    const run = Bun.spawnSync(['bun', '.claude/hooks/live-tree-guard.mjs'], { cwd: repo, stdin: Bun.file(path) });
    expect(run.exitCode).toBe(0);
    expect(JSON.parse(run.stdout.toString()).hookSpecificOutput.permissionDecision).toBe('deny');
    writeFileSync(path, '{}');
    const noOp = Bun.spawnSync(['bun', '.claude/hooks/atlas-guard.mjs'], { cwd: repo, stdin: Bun.file(path) });
    expect(noOp.exitCode).toBe(0);
    expect(noOp.stdout.toString()).toBe('');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
