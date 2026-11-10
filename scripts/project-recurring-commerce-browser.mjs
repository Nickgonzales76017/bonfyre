import { existsSync, lstatSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';

const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const pairs = [
  ['scripts/recurring-commerce/draft.mjs', 'integrations/browser/extension/recurring-commerce.mjs'],
  ['scripts/recurring-commerce/catalog.json', 'integrations/browser/extension/catalog.json'],
  ['scripts/recurring-commerce/contents.mjs', 'integrations/browser/extension/contents.mjs'],
];

// These are packaging outputs, not separately maintained implementations.
// Keep exact source bytes; importing the browser module never reaches outside
// the extension and never includes the obsolete standalone CLI wrapper.
function regularPath(root, relative, missing = false) {
  let path = root;
  for (const part of relative.split('/')) {
    path = join(path, part);
    try {
      if (lstatSync(path).isSymbolicLink()) throw new Error(`symlink refused: ${relative}`);
    } catch (error) {
      if (error.code !== 'ENOENT' || !missing) throw error;
    }
  }
  if (existsSync(path) && !lstatSync(path).isFile()) throw new Error(`not a file: ${relative}`);
  return path;
}

export function projectCommerce({ root = defaultRoot, write = false } = {}) {
  root = resolve(root);
  const rows = pairs.map(([source, target]) => {
    const from = regularPath(root, source), to = regularPath(root, target, true);
    const bytes = readFileSync(from);
    return { source, target, from, to, bytes,
      changed: !existsSync(to) || !readFileSync(to).equals(bytes) };
  });
  // Read both inputs before changing either output. Packaging only continues
  // when the subsequent check sees one coherent source/output generation.
  const staged = [];
  try {
    if (write) {
      for (const row of rows.filter(row => row.changed)) {
        const temporary = `${row.to}.projection-${randomBytes(8).toString('hex')}`;
        staged.push(temporary);
        writeFileSync(temporary, row.bytes, { flag: 'wx', mode: 0o644 });
      }
      if (rows.some(row => !readFileSync(row.from).equals(row.bytes)))
        throw new Error('canonical source changed during projection');
      let index = 0;
      for (const row of rows.filter(row => row.changed)) renameSync(staged[index++], row.to);
    }
    return rows.map(({ source, target, changed }) => ({ source, target, changed }));
  } finally {
    for (const temporary of staged) rmSync(temporary, { force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2), mode = args[0] ?? '--check';
    if (args.length > 1 || !['--check', '--write'].includes(mode))
      throw new Error('usage: project-recurring-commerce-browser.mjs --check|--write');
    const rows = projectCommerce({ write: mode === '--write' });
    for (const row of rows) console.log(`${row.changed ? mode === '--write' ? 'generated' : 'drift' : 'current'}\t${row.target}\t${row.source}`);
    if (mode === '--check' && rows.some(row => row.changed)) process.exitCode = 1;
  } catch (error) {
    console.error(`commerce projection: ${error.message}`);
    process.exitCode = 1;
  }
}
