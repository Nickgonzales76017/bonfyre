// Pin the transitive local import graph, plus explicitly declared resource
// sidecars. This does not assert that arbitrary dynamic loader expressions are
// statically knowable; the caller must declare those deployment inputs.
import { lstatSync, readFileSync, realpathSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { createHash } from 'node:crypto';

export function deploymentSourceClosure({ root, entries, sidecars = [] }) {
  root = realpathSync(root);
  if (!Array.isArray(entries) || !entries.length || !Array.isArray(sidecars)) throw new Error('deployment inputs required');
  const transpiler = new Bun.Transpiler({ loader: 'js' }), queue = [...entries, ...sidecars], found = new Map();
  let bytes = 0;
  for (let index = 0; index < queue.length; index++) {
    const path = resolve(root, queue[index]), name = relative(root, path);
    if (!name || name === '..' || name.startsWith(`..${sep}`) || isAbsolute(name)) throw new Error('deployment input escapes workspace');
    if (found.has(path)) continue;
    let parent = root;
    for (const part of name.split(sep)) { parent = resolve(parent, part); if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink deployment input'); }
    const stat = lstatSync(path);
    if (!stat.isFile() || stat.size > 8 * 1024 * 1024 || found.size >= 256) throw new Error('deployment source budget exceeded');
    const contents = readFileSync(path); bytes += contents.byteLength;
    if (contents.byteLength !== stat.size || bytes > 64 * 1024 * 1024) throw new Error('deployment source changed or exceeded budget');
    found.set(path, { path, name, bytes: contents.byteLength, sha256: createHash('sha256').update(contents).digest('hex') });
    if (!/\.[cm]?js$/.test(path)) continue;
    const source = new TextDecoder('utf-8', { fatal: true }).decode(contents).replace(/^#![^\n]*(?:\n|$)/, '\n');
    for (const item of transpiler.scanImports(source)) {
      if (item.path.startsWith('.')) queue.push(resolve(dirname(path), item.path));
      else if (!/^(?:node:|bun(?::|$))/.test(item.path)) throw new Error(`unowned package dependency: ${item.path}`);
    }
  }
  return [...found.values()].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
}
