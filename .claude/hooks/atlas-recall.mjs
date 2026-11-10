#!/usr/bin/env bun
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const arch = join(repo, 'architecture');
const limit = 3;
const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export function declared() {
  const names = new Map();
  try {
    for (const file of new Bun.Glob('**/*.yaff').scanSync({ cwd: arch })) {
      let current = '';
      for (const line of readFileSync(join(arch, file), 'utf8').split(/\r?\n/)) {
        if (line.startsWith('architecture ')) {
          current = line.slice('architecture '.length).trim();
          if (!names.has(current)) names.set(current, '');
        } else if (current && line.trimStart().startsWith('canonical_name ')) {
          names.set(current, line.trim().slice('canonical_name '.length).trim());
          current = '';
        }
      }
    }
  } catch { return new Map(); }
  return names;
}

export function named(prompt, names = declared()) {
  const hits = [];
  const lowered = prompt.toLowerCase();
  for (const [id, canonical] of names) {
    if (id.length < 5) continue;
    const idRe = new RegExp(`(?<![\\w-])${escape(id)}(?![\\w-])`);
    const nameRe = canonical.length > 4 ? new RegExp(`\\b${escape(canonical)}\\b`) : null;
    if (idRe.test(lowered) || (nameRe && nameRe.test(prompt))) hits.push(id);
    if (hits.length >= limit) break;
  }
  return hits;
}

export function recall(prompt, names = declared(), get = id => Bun.spawnSync(['bun', 'architecture/atlas.mjs', 'get', id], { cwd: repo })) {
  if (typeof prompt !== 'string' || !prompt.trim()) return null;
  const blocks = named(prompt, names).flatMap(id => {
    try { const value = get(id).stdout?.toString().trim(); return value ? [value] : []; }
    catch { return []; }
  });
  return blocks.length ? { hookSpecificOutput: {
    hookEventName: 'UserPromptSubmit',
    additionalContext: `Atlas entries for organs named in this prompt (canonical; do not re-derive them from memory, and note each entry's known gaps and what it states it cannot infer):\n\n${blocks.join('\n\n')}`,
  } } : null;
}

if (import.meta.main) {
  let event;
  try { event = JSON.parse(await Bun.stdin.text()); } catch { process.exit(0); }
  const output = recall(event?.prompt);
  if (output) process.stdout.write(`${JSON.stringify(output)}\n`);
}
