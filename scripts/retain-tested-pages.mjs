// Retain the tested Pages projection without overwriting concurrent source work.
import {spawnSync} from 'node:child_process';
import {existsSync, appendFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

export function retainRelease({cwd, paths, verify, attempts = 3, beforePush = () => {}, pushResult = null, reconcileGeneratedPaths = []}) {
  if (!cwd || !Array.isArray(paths) || !paths.length || typeof verify !== 'function' || !Number.isInteger(attempts) || attempts < 1 || attempts > 3) throw Error('Explicit repository, generated paths and verification are required.');
  if (paths.some(p => !/^[a-zA-Z0-9_./-]+$/.test(p) || p.startsWith('/') || p.split('/').some(s => !s || s === '.' || s === '..'))) throw Error('Unsafe generated path.');
  if (!Array.isArray(reconcileGeneratedPaths) || reconcileGeneratedPaths.some(p => !paths.some(root => p === root || p.startsWith(root + '/')))) throw Error('Only explicit retained outputs may be reconciled.');
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
    if (changed.status === 1) git(['commit', '-m', 'release(spaces): retain verified studio and instrument projections']);
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
      const conflicted = git(['diff', '--name-only', '--diff-filter=U', '-z'], true).stdout.split('\0').filter(Boolean);
      const newPaths = git(['diff', '--name-only', '-z', basis, upstream]).stdout.split('\0').filter(Boolean);
      git(['rebase', '--abort'], true);
      // Never resolve an ambiguous release conflict by choosing ours or theirs.
      // Only regenerate an explicitly approved, source-derived public page from
      // the newer upstream copy. All studio/instrument output conflicts remain
      // hard stops; no force push, dropped source commits or blind merge.
      const canRegenerate = conflicted.length > 0 &&
        conflicted.every(p => reconcileGeneratedPaths.includes(p)) &&
        !newPaths.some(p => allowed(p) && !reconcileGeneratedPaths.includes(p));
      if (!canRegenerate) {
        throw Error('Concurrent release conflict; upstream was not overwritten. ' + (rebased.stderr || '').slice(-2000));
      }
      // The tested basis is already an ancestor of upstream (checked above).
      // Therefore every authored source commit is retained before rebuilding.
      // The verifier must replay the deterministic generator on the latest
      // authored HTML and recheck the release, rather than transplanting a
      // stale output from an older GitHub Actions run.
      git(['reset', '--hard', 'refs/remotes/origin/main']);
      basis = upstream;
      verify();
      commitProjection();
      continue;
    }
    // An editorial-only concurrent commit cannot change the tested studio or
    // instrument projections. The later Journal render consumes the rebased
    // editorial source. Do not rerun expensive unrelated studio tests here.
    const incoming = git(['diff', '--name-only', '-z', basis, upstream]).stdout.split('\0').filter(Boolean);
    const editorialOnly = incoming.length > 0 && incoming.every(p =>
      p.startsWith('editorial/') ||
      p.startsWith('site/blog/') ||
      p.startsWith('site/series/') ||
      p === 'scripts/render-editorial.sh' ||
      p === 'scripts/tests/journal-discoverability.py' ||
      p === '.github/workflows/journal.yml');
    basis = upstream;
    if (!editorialOnly) verify();
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
    run(['--test', 'scripts/tests/component-contracts.test.mjs', 'scripts/tests/instruments-core.test.mjs']);
    run(['scripts/upgrade-spaces.mjs', 'site/spaces', 'studio-inputs']);
    run(['scripts/component-release.mjs', 'site/spaces']);
    run(['scripts/tests/spaces-live-browser.mjs']);
    run(['scripts/prepare-spaces-discovery.mjs']);
    run(['scripts/build-instruments.mjs']);
    run(['scripts/tests/instruments-browser.mjs']);
    run(['scripts/prepare-instruments-discovery.mjs']);
    run(['--test', 'scripts/tests/showcase-directory.test.mjs']);
  };
  try {
    const result = retainRelease({cwd, paths: ['site/spaces', 'site/instruments', 'site/index.html', 'vendor/spaces-helpers-20261015'], verify, reconcileGeneratedPaths: ['site/index.html']});
    console.log('PAGES_RETENTION_PASS\t' + result.head + '\tattempts\t' + result.attempts + '\trecovered\t' + result.recovered);
  } catch (error) {
    // An obsolete build must not publish an older site or block the next
    // source revision. This is not approval to resolve a real generated-file
    // conflict: those remain errors and preserve every upstream commit.
    if (error?.message !== 'Concurrent source changes exhausted the bounded retention attempts.' || !process.env.GITHUB_OUTPUT) throw error;
    appendFileSync(process.env.GITHUB_OUTPUT, 'superseded=true\n');
    console.log('PAGES_RETENTION_SUPERSEDED\tNewer source work is queued; no artifact will be published from this run.');
  }
}
