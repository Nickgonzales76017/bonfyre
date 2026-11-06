#!/usr/bin/env bun
// Compare the old five-read snapshot algorithm with the shared four-read owner.
// Both use Bun + identical Git admission so this isolates the redundant walk,
// not Python-vs-Bun startup, live delivery, or correctness under concurrent edits.
import { strict as assert } from 'node:assert';
import { createHash } from 'node:crypto';
import { gitCommand, snapshot, summarizeChanges } from '../project_delta_watcher.mjs';

function legacySnapshot(root) {
  const status = gitCommand(['git', 'status', '--porcelain=v2', '--branch', '-z'], root);
  const head = gitCommand(['git', 'rev-parse', 'HEAD'], root).trim();
  const headers = gitCommand(['git', 'status', '--porcelain=v2', '--branch'], root);
  const working = summarizeChanges(gitCommand(['git', 'diff', '--name-status'], root));
  const staged = summarizeChanges(gitCommand(['git', 'diff', '--cached', '--name-status'], root));
  let branch = '', upstream = '';
  for (const line of headers.split('\n')) {
    if (line.startsWith('# branch.head ')) {
      const value = line.slice('# branch.head '.length).trim();
      branch = value === '(detached)' ? '' : value;
    } else if (line.startsWith('# branch.upstream ')) {
      upstream = line.slice('# branch.upstream '.length).trim();
    }
  }
  return { source: 'local-project', ts: 0, root, branch, upstream, head,
    status_hash: createHash('sha256').update(status).digest('hex'), working, staged };
}

const [mode, root] = process.argv.slice(2);
if (!root || !['legacy', 'current', 'verify'].includes(mode)) {
  throw new Error('usage: project_delta_benchmark.mjs legacy|current|verify <isolated-git-root>');
}
if (mode === 'verify') {
  assert.deepEqual(snapshot(root, { now: () => 0 }), legacySnapshot(root));
  console.log('identical snapshot fields and digest; four Git commands instead of five');
} else {
  const result = mode === 'legacy' ? legacySnapshot(root) : snapshot(root, { now: () => 0 });
  console.log(result.status_hash);
}
