#!/usr/bin/env bun
/* Reconcile the command Atlas with real command directories without erasing
 * authored witnesses or invariants in existing command blocks. A command
 * directory proves source presence, not execution or authority.
 */
import { readdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { resolve, dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const rootArg = process.argv.find((arg) => arg.startsWith('--root='));
const root = rootArg ? resolve(rootArg.slice(7)) : resolve(dirname(fileURLToPath(import.meta.url)), '..');
const commands = join(root, 'cmd');
const target = join(root, 'architecture/commands/command-surface.yaff');
const smokePath = join(commands, 'command_smoke.json');
const sourceExt = new Set(['.c', '.h', '.rs', '.zig', '.cpp', '.cc', '.go']);
const ext = (name) => name.slice(name.lastIndexOf('.'));
const entries = (dir) => existsSync(dir) ? readdirSync(dir, { withFileTypes: true }) : [];
// A Makefile that names its source outside the command directory is the build
// edge; the files beside it are not what the command is compiled from.
const builtFrom = (dir) => {
  const makefile = join(dir, 'Makefile');
  if (!existsSync(makefile)) return [];
  return [...readFileSync(makefile, 'utf8').matchAll(/^(?:SOURCE|SRC|SRCS|SOURCES)\s*:?=\s*(.+)$/gm)]
    .flatMap((match) => match[1].split(/\s+/)).filter((name) => name.startsWith('../') && sourceExt.has(ext(name)))
    .map((name) => relative(root, resolve(dir, name))).filter((name) => existsSync(join(root, name)));
};
const sources = (dir) => {
  const delegated = builtFrom(dir);
  if (delegated.length) return [...delegated.slice(0, 3), relative(root, join(dir, 'Makefile'))];
  for (const path of [join(dir, 'src'), dir]) {
    const names = entries(path).filter((entry) => entry.isFile() && sourceExt.has(ext(entry.name)))
      .map((entry) => join(path, entry.name)).sort();
    if (names.length) return names.slice(0, 4).map((name) => relative(root, name));
  }
  return [];
};
const slug = (name) => `cmd-${name.slice('Bonfyre'.length).replace(/(?!^)(?=[A-Z])/g, '-').toLowerCase()}`;

export function missingCommands() {
  const existing = readFileSync(target, 'utf8');
  const names = new Set([...existing.matchAll(/^  canonical_name (Bonfyre\S+)\s*$/gm)].map((match) => match[1]));
  return entries(commands).filter((entry) => entry.isDirectory() && entry.name.startsWith('Bonfyre') && entry.name !== 'BonfyreFS' && !names.has(entry.name))
    .map((entry) => entry.name).sort();
}

export function render(name, smoke) {
  const path = join(commands, name);
  const suffix = name.slice('Bonfyre'.length);
  const src = sources(path);
  const row = smoke[name] ?? {};
  const live = row.responds === true;
  const hasTest = existsSync(join(path, 'tests')) || entries(path).some((entry) => entry.name.toLowerCase().includes('selftest'));
  const maturity = live && hasTest ? 'measured' : live ? 'implemented' : src.length ? 'declared' : 'architectural';
  const capability = `${suffix}Capability`;
  const note = live ? 'responds to --help (live)' : src.length ? 'source-only, not built' : 'declared, no source found';
  return [`architecture ${slug(name)}`,
    `  canonical_name ${name}`,
    '  family command',
    `  domain Command / ${suffix}`,
    `  semantic_question What does the ${name} capability organ realize, and by what contract?`,
    `  purpose Command-surface organ realizing the ${capability} capability. Its product effects require separate scope and authority.`,
    `  maturity ${maturity}`,
    `  command ${name}`,
    `  internal_calculus ${row.binary || name} binary`,
    '  forbidden_inference the command exists -> its effect is authorized; Authority gates mutation and read scope still applies',
    ...src.map((source) => `  source_path ${source}`),
    ...(live ? ['  witness cmd/command_smoke.sh'] : []),
    '  source_lineage current command directory and command_smoke.json census',
    `  known_gap ${note}; deeper WiringSpec fact flow and authority admission are separate work`,
    ''].join('\n');
}

if (import.meta.main) {
  try {
    const missing = missingCommands();
    if (process.argv.includes('--check')) {
      if (missing.length) {
        console.error(`${missing.length} command organs absent from the Atlas: ${missing.join(', ')}`);
        process.exit(1);
      }
      console.log('command Atlas covers every current Bonfyre command directory');
    } else {
      const smoke = existsSync(smokePath) ? JSON.parse(readFileSync(smokePath, 'utf8')) : { results: [] };
      const byName = Object.fromEntries((smoke.results ?? []).map((row) => [row.command, row]));
      const existing = readFileSync(target, 'utf8');
      if (missing.length) writeFileSync(target, `${existing.trimEnd()}\n\n${missing.map((name) => render(name, byName)).join('\n')}\n`);
      console.log(`added ${missing.length} disk-backed command organs; retained all existing authored blocks`);
    }
  } catch (error) { console.error(error.message); process.exit(1); }
}
