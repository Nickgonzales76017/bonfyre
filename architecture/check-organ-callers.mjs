#!/usr/bin/env bun
// One-pass source-token index for generous native organ reachability auditing.
import { readFile, readdir } from 'node:fs/promises';
import { join, relative, resolve, extname } from 'node:path';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(process.env.BONFYRE_REPO_ROOT ?? dirname(fileURLToPath(import.meta.url)) + '/..');
const args = process.argv.slice(2);
if (args.some(arg => !['--json', '--strict'].includes(arg))) {
  process.stderr.write('usage: bun architecture/check-organ-callers.mjs [--json] [--strict]\n');
  process.exit(2);
}
const targets = ['cmd', 'engine', 'components', 'integrations', 'scripts', 'lib'];
const extensions = new Set(['.c', '.h', '.mjs', '.py', '.sh', '.ts']);
const listed = Bun.spawnSync(['rg', '--files', ...targets], {
  cwd: root, stdout: 'pipe', stderr: 'pipe', maxBuffer: 32 * 1024 * 1024,
});
if (listed.exitCode !== 0) {
  process.stderr.write(listed.stderr);
  process.exit(listed.exitCode ?? 1);
}
const paths = listed.stdout.toString().trim().split('\n').filter(path => extensions.has(extname(path)));
const mentions = new Map();
for (let offset = 0; offset < paths.length; offset += 64) {
  await Promise.all(paths.slice(offset, offset + 64).map(async path => {
    let source;
    try { source = await readFile(join(root, path), 'utf8'); } catch { return; }
    for (const symbol of new Set(source.match(/\bbf_[a-z0-9_]+\b/g) ?? [])) {
      if (!mentions.has(symbol)) mentions.set(symbol, new Set());
      mentions.get(symbol).add(path);
    }
  }));
}
const headers = (await readdir(join(root, 'engine/core/include'))).filter(name => /^bf_.*\.h$/.test(name)).sort();
const rows = [];
for (const header of headers) {
  const path = join(root, 'engine/core/include', header);
  const source = await readFile(path, 'utf8');
  const symbols = new Set([...source.matchAll(/\b(bf_[a-z0-9_]+)\s*\(/g)].map(match => match[1]));
  if (!symbols.size) continue;
  const organ = header.slice(0, -2);
  const base = organ.slice(3);
  const callers = new Set();
  for (const symbol of symbols) for (const candidate of mentions.get(symbol) ?? []) {
    if (candidate.startsWith(`engine/core/include/${organ}.h`) ||
        candidate.startsWith(`engine/core/src/${base}.c`) ||
        candidate.includes('/tests/') || /_test\.(c|sh|py)$/.test(candidate)) continue;
    callers.add(candidate);
  }
  rows.push({ organ, symbols: symbols.size, callers: [...callers].sort() });
}
const uncalled = rows.filter(row => row.callers.length === 0).map(row => row.organ);
rows.sort((a, b) => a.callers.length - b.callers.length);
if (args.includes('--json')) process.stdout.write(JSON.stringify({ organs: rows, uncalled }, null, 1) + '\n');
else {
  process.stdout.write(`native organs: ${rows.length}, of which ${uncalled.length} are called by nothing\n`);
  for (const row of rows) {
    const note = row.callers.length ? '' : '   <- nothing outside its own tests calls it';
    process.stdout.write(`  ${row.organ.padEnd(26)} ${String(row.symbols).padStart(3)} symbols  ${String(row.callers.length).padStart(2)} callers${note}\n`);
  }
}
if (args.includes('--strict') && uncalled.length) {
  process.stderr.write(`\n${uncalled.length} organs are proven and unused.\n`);
  process.exitCode = 1;
}
