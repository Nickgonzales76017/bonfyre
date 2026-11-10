#!/usr/bin/env bun
import { existsSync, statSync } from 'node:fs';
import { resolve, join } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const arch = join(repo, 'architecture');
const index = join(arch, 'atlas.index.json');

export function needsExport() {
  if (!existsSync(index)) return true;
  const indexTime = statSync(index).mtimeMs;
  const files = [...new Bun.Glob('**/*.yaff').scanSync({ cwd: arch })];
  return files.some(path => statSync(join(arch, path)).mtimeMs > indexTime);
}

export function maybeExport(run = args => Bun.spawnSync(['bun', 'architecture/atlas.mjs', ...args], { cwd: repo })) {
  try { if (!needsExport()) return ''; } catch { return ''; }
  const checked = run(['validate']);
  if (checked.exitCode !== 0) {
    const detail = `${checked.stdout?.toString() ?? ''}${checked.stderr?.toString() ?? ''}`.trim().slice(0, 800);
    return `atlas: entries changed and validation fails; index left alone.\n${detail}`;
  }
  const exported = run(['export']);
  const detail = `${exported.stdout?.toString() ?? ''}${exported.stderr?.toString() ?? ''}`.trim();
  return exported.exitCode === 0
    ? `atlas: entries changed, index re-exported. ${detail.split('\n').at(-1) || ''}`
    : `atlas: export failed, index left alone.\n${detail.slice(0, 800)}`;
}

if (import.meta.main) {
  await Bun.stdin.text();
  const output = maybeExport();
  if (output) process.stderr.write(`${output}\n`);
}
