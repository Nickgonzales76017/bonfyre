// What this does not prove:
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { compileApplianceManifest, releaseNotes } from '../appliance-manifest.mjs';

const script = resolve(import.meta.dir, '../appliance-manifest.mjs');
const makefile = resolve(import.meta.dir, '../../Makefile');
const roots = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
const fixture = () => { const root = mkdtempSync(join(tmpdir(), 'appliance-manifest-')); roots.push(root); return root; };
const sha = value => createHash('sha256').update(value).digest('hex');
function put(root, path, content, mode = 0o644) {
  const target = join(root, path); mkdirSync(resolve(target, '..'), { recursive: true });
  writeFileSync(target, content); chmodSync(target, mode);
}

test('manifest preserves archived executable, library and representation alias contracts', async () => {
  const root = fixture();
  put(root, 'cmd/B/bonfyre-zeta', 'zeta binary', 0o755);
  put(root, 'cmd/A/bonfyre', 'root binary', 0o755);
  put(root, 'cmd/A/bonfyre-alpha', Buffer.from([0, 255, 1, 8]), 0o755);
  put(root, 'cmd/A/bonfyre-not-executable', 'ignored');
  put(root, 'cmd/A/unrelated', 'ignored', 0o755);
  put(root, 'cmd/A/.bonfyre-hidden', 'ignored', 0o755);
  put(root, 'cmd/A/deep/bonfyre-nested', 'ignored', 0o755);
  put(root, 'lib/libbonfyre/libbonfyre.so', 'linux library');
  put(root, 'lib/libbonfyre/libbonfyre.dylib', 'mac library');
  for (const name of ['zeta.bfmodel', 'alpha.v2.bfmodel', 'alpha.akmodel', 'sae.bfsae', 'sae.aksae',
    'pair.v1.bffpqx', 'pair.v1.akfpqx', 'modern.akfpqx']) put(root, `model-memory/${name}`, '{}');
  const result = await compileApplianceManifest({ root, target: 'bun-darwin-arm64', release: '1.2.3',
    runtimeAbi: 'test-abi', builtAt: '2026-10-02T00:00:00Z' });
  expect(result).toEqual({ schema_version: 'bonfyre.deploy.v1', release: '1.2.3', revision: 'git:unknown',
    built_at: '2026-10-02T00:00:00Z', target: 'bun-darwin-arm64',
    hyper: { path: 'bin/bonfyre-hyper', bun_target: 'bun-darwin-arm64', version: '1.2.3' },
    runtime: { operator_count: 3, ffi_registry_zero_drift: 'unknown', capabilities_runnable: 'unknown' },
    libraries: [{ name: 'libbonfyre.so', sha256: sha('linux library'), abi: 'test-abi' },
      { name: 'libbonfyre.dylib', sha256: sha('mac library'), abi: 'test-abi' }],
    operators: [{ name: 'bonfyre', binary: 'bonfyre', sha256: sha('root binary'), required: true },
      { name: 'alpha', binary: 'bonfyre-alpha', sha256: sha(Buffer.from([0, 255, 1, 8])), required: true },
      { name: 'zeta', binary: 'bonfyre-zeta', sha256: sha('zeta binary'), required: true }],
    model_memory: [{ model_family: 'alpha', required: false, bfmodel: 'model-memory/alpha.v2.bfmodel', akmodel: 'model-memory/alpha.akmodel' },
      { model_family: 'zeta', required: false, bfmodel: 'model-memory/zeta.bfmodel' }],
    sae_dicts: [{ name: 'sae', bfsae: 'model-memory/sae.bfsae', aksae: 'model-memory/sae.aksae' }],
    fpqx_alignments: [{ stem: 'pair.v1', bffpqx: 'model-memory/pair.v1.bffpqx', akfpqx: 'model-memory/pair.v1.akfpqx' },
      { stem: 'modern', akfpqx: 'model-memory/modern.akfpqx' }], signature: { alg: 'none', value: 'TODO' } });
});

test('empty inventory keeps optional fallback while explicitly refusing residency and drift claims', async () => {
  const result = await compileApplianceManifest({ root: fixture(), defaultModelFamily: 'fixture-model' });
  expect(result.operators).toEqual([]);
  expect(result.libraries).toEqual([]);
  expect(result.model_memory).toEqual([{ model_family: 'fixture-model', required: false,
    bfmodel: 'model-memory/fixture-model.bfmodel', akmodel: 'model-memory/fixture-model.akmodel', presence: 'not_observed' }]);
  expect(result.runtime.ffi_registry_zero_drift).toBe('unknown');
  await expect(compileApplianceManifest({ root: fixture(), defaultModelFamily: '../escape' })).rejects.toThrow('invalid default model');
});

test('release notes preserve archived headings, aliases, ordering and unsigned marker', () => {
  const notes = releaseNotes({ release: 'test', target: 'fixture', revision: 'git:abc', built_at: 'fixed',
    schema_version: 'bonfyre.deploy.v1', operators: [{ name: 'read', binary: 'bonfyre-read', required: true }],
    libraries: [{ name: 'libbonfyre.so', abi: 'v1' }],
    model_memory: [{ model_family: 'qwen', bfmodel: 'qwen.bfmodel', akmodel: 'qwen.akmodel' }],
    sae_dicts: [{ name: 'plain', bfsae: 'plain.bfsae' }],
    fpqx_alignments: [{ stem: 'pair', bffpqx: 'pair.bffpqx', akfpqx: 'pair.akfpqx' }], signature: { alg: 'none', value: 'TODO' } });
  expect(notes).toBe('# Bonfyre Release test\n\n- Target: fixture\n- Revision: git:abc\n- Built at: fixed\n- Manifest schema: bonfyre.deploy.v1\n\n## Runtime\n\n- Operators: 1\n- Libraries: 1\n\n### Operators\n\n- read (bonfyre-read) required=true\n\n### Libraries\n\n- libbonfyre.so abi=v1\n\n## Model Memory\n\n- qwen: qwen.akmodel (legacy: qwen.bfmodel)\n\n## SAE Dictionaries\n\n- plain: plain.bfsae\n\n## FPQx Alignments\n\n- pair: pair.akfpqx (legacy: pair.bffpqx)\n\n## Signature\n\n- alg: none\n- value: TODO\n\n');
  expect(releaseNotes({ built_at: 'fixed' })).toContain('## Model Memory\n\n- none');
});

test('actual root Make targets generate both manifests and notes with no Python invocation', () => {
  const root = fixture();
  mkdirSync(join(root, 'scripts'));
  copyFileSync(script, join(root, 'scripts/appliance-manifest.mjs'));
  put(root, 'cmd/Test/bonfyre-fixture', 'fixture bytes', 0o755);
  const poison = join(root, 'poison'); mkdirSync(poison);
  for (const name of ['python', 'python3']) put(root, `poison/${name}`, '#!/bin/sh\nexit 99\n', 0o755);
  const result = Bun.spawnSync(['make', '-f', makefile, 'release-notes', `BUN_BIN=${process.execPath}`,
    'BONFYRE_VERSION=9.8.7', 'HYPER_TARGET=bun-darwin-arm64', 'RUNTIME_ABI=fixture-abi'], {
    cwd: root, env: { ...process.env, PATH: `${poison}:${process.env.PATH}` }, stdout: 'pipe', stderr: 'pipe' });
  expect(result.stderr.toString()).toBe(''); expect(result.exitCode).toBe(0);
  const bytes = readFileSync(join(root, 'bonfyre.manifest.json'), 'utf8');
  expect(readFileSync(join(root, 'dist/bonfyre.manifest.json'), 'utf8')).toBe(bytes);
  const manifest = JSON.parse(bytes);
  expect(manifest.release).toBe('9.8.7'); expect(manifest.target).toBe('bun-darwin-arm64');
  expect(manifest.operators[0].sha256).toBe(sha('fixture bytes'));
  expect(readFileSync(join(root, 'dist/release-notes.md'), 'utf8')).toBe(releaseNotes(manifest));
  expect(readdirSync(root).some(name => name.includes('.tmp-'))).toBe(false);
});

test('malformed notes refuse without replacing the previous report', () => {
  const root = fixture(), manifest = join(root, 'manifest.json'), out = join(root, 'notes.md');
  writeFileSync(out, 'previous report');
  for (const content of ['{', JSON.stringify({ operators: 'not-an-array' }),
    JSON.stringify({ release: 'fake\n# injected' }), JSON.stringify({ operators: [{ required: 'yes' }] })]) {
    writeFileSync(manifest, content);
    const result = Bun.spawnSync([process.execPath, script, 'notes', '--manifest', manifest, '--out', out]);
    expect(result.exitCode).toBe(2); expect(readFileSync(out, 'utf8')).toBe('previous report');
  }
  expect(readdirSync(root).some(name => name.includes('.tmp-'))).toBe(false);
});
