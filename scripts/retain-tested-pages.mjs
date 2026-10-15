// Retain the tested Pages projection without overwriting concurrent source work.
import {spawnSync} from 'node:child_process';
import {existsSync} from 'node:fs';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

export function retainRelease({cwd, paths, verify, attempts = 3, beforePush = () => {}, pushResult = null}) {
  if (!cwd || !Array.isArray(paths) || !paths.length || typeof verify !== 'function' || !Number.isInteger(attempts) || attempts < 1 || attempts > 3) throw Error('Explicit repository, generated paths and verification are required.');
  if (paths.some(p => !/^[a-zA-Z0-9_./-]+$/.test(p) || p.startsWith('/') || p.split('/').some(s => !s || s === '.' || s === '..'))) throw Error('Unsafe generated path.');
  const allowed = path => paths.some(p => path === p || path.startsWith(p + '/'));
  const git = (args, optional = false) => {
    const result = spawnSync('git', args, {cwd, encoding: 'utf8', timeout: 60000, maxBuffer: 4 * 1024 * 1024});
    if (result.error) throw result.error;
    if (!optional && result.status !== 0) throw Error('git ' + args[0] + ' failed: ' + (result.stderr || result.stdout).slice(-6000));
    return result;
  };
  if (git(['symbolic-ref', '--short', 'HEAD']).stdout.trim() !== 'main') throw Error('Release retention requires the main checkout.');
  let basis = git(['rev-parse', 'HEAD']).stdout.trim();
  const guardIndex = () => {
    const staged = git(['diff', '--cached', '--name-only', '-z']).stdout.split('\0').filter(Boolean);
    if (staged.some(p => !allowed(p))) throw Error('Unrelated staged work is outside this release.');
  };
  const commitProjection = () => {
    guardIndex();
    const present = paths.filter(p => existsSync(resolve(cwd, p)));
    if (present.length) git(['add', '--', ...present]);
    guardIndex();
    const changed = git(['diff', '--cached', '--quiet'], true);
    if (changed.status === 1) git(['commit', '-m', 'release(spaces): retain verified studio projection and discovery links']);
    else if (changed.status !== 0) throw Error('Could not inspect the staged release.');
    if (git(['diff', '--name-only', '-z']).stdout) throw Error('Tracked changes remain outside the retained release.');
  };
  guardIndex();
  verify();
  commitProjection();
  for (let attempt = 1; attempt <= attempts; attempt++) {
    beforePush(attempt);
    const sent = pushResult ? pushResult(git, attempt) : git(['push', 'origin', 'HEAD:main'], true);
    const head = git(['rev-parse', 'HEAD']).stdout.trim();
    if (sent.status === 0) return {head, attempts: attempt, recovered: false};
    git(['fetch', '--no-tags', 'origin', 'main']);
    const upstream = git(['rev-parse', 'refs/remotes/origin/main']).stdout.trim();
    if (upstream === head) return {head, attempts: attempt, recovered: true};
    if (git(['merge-base', '--is-ancestor', basis, upstream], true).status !== 0) throw Error('Upstream history no longer contains the tested source basis.');
    if (git(['merge-base', '--is-ancestor', upstream, head], true).status === 0) throw Error('Push failed without a concurrent source change: ' + (sent.stderr || '').slice(-2000));
    if (attempt === attempts) throw Error('Concurrent source changes exhausted the bounded retention attempts.');
    const rebased = git(['rebase', 'refs/remotes/origin/main'], true);
    if (rebased.status !== 0) {
      git(['rebase', '--abort'], true);
      throw Error('Concurrent release conflict; upstream was not overwritten. ' + (rebased.stderr || '').slice(-2000));
    }
    basis = upstream;
    // The old browser result cannot certify newly reconciled source.
    verify();
    commitProjection();
  }
  throw Error('Release was not retained.');
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const cwd = process.cwd();
  const run = args => {
    const result = spawnSync(process.execPath, args, {cwd, stdio: 'inherit', timeout: 240000});
    if (result.error || result.status !== 0) throw result.error || Error('Release verification failed: ' + args.join(' '));
  };
  const verify = () => {
    run(['--check', 'components/BonfyreServiceSite/product-scene-tools.mjs']);
    run(['--check', 'scripts/upgrade-spaces.mjs']);
    run(['scripts/upgrade-spaces.mjs', 'site/spaces', 'studio-inputs']);
    run(['scripts/tests/spaces-live-browser.mjs']);
    run(['scripts/prepare-spaces-discovery.mjs']);
    run(['--test', 'scripts/tests/showcase-directory.test.mjs']);
  };
  const result = retainRelease({cwd, paths: ['site/spaces', 'site/index.html', 'vendor/spaces-helpers-20261015'], verify});
  console.log('PAGES_RETENTION_PASS\t' + result.head + '\tattempts\t' + result.attempts + '\trecovered\t' + result.recovered);
}
