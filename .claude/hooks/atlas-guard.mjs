#!/usr/bin/env bun
import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');

export function touchesAtlas(event) {
  const path = event?.tool_input?.file_path;
  return typeof path === 'string' && /(?:^|\/)architecture\/.*\.yaff$/.test(path);
}

export function decide(event, validate = () => Bun.spawnSync(['bun', 'architecture/atlas.mjs', 'validate'], { cwd: repo })) {
  if (!touchesAtlas(event)) return '';
  const result = validate();
  const stdout = result.stdout?.toString() ?? '';
  if (result.exitCode === 0) return stdout.trim().split('\n').at(-1) || 'atlas valid';
  const detail = `${stdout}${result.stderr?.toString() ?? ''}`.trim().slice(-3000);
  return JSON.stringify({ decision: 'block', reason: `The atlas rejects this edit. It is the canonical registry, so an invalid entry is a false claim about the system. Fix it now:\n\n${detail}` });
}

if (import.meta.main) {
  let event;
  try { event = JSON.parse(await Bun.stdin.text()); } catch { process.exit(0); }
  const output = decide(event);
  if (output) process.stdout.write(`${output}\n`);
}
