import { createHash } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, renameSync, unlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';

const own = dirname(fileURLToPath(import.meta.url));
const root = resolve(own, '../../..');
const lock = JSON.parse(readFileSync(join(own, 'spines_helix.lock.json'), 'utf8'));
const artifact = join(own, lock.artifact);
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
function checkedBytes(path, maximum) {
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > maximum) throw new Error(`unsafe source/artifact: ${path}`);
  return readFileSync(path);
}
function checkSources() {
  for (const [path, expected] of Object.entries(lock.sources)) {
    const actual = sha256(checkedBytes(join(root, path), 1024 * 1024));
    if (actual !== expected) throw new Error(`source drift: ${path}`);
  }
}
function checkArtifact(path = artifact) {
  const bytes = checkedBytes(path, 2 * 1024 * 1024);
  if (bytes.length !== lock.artifact_bytes || sha256(bytes) !== lock.artifact_sha256) throw new Error('Wasm artifact drift');
}
checkSources();
const mode = process.argv[2] ?? '--verify';
if (mode === '--verify') checkArtifact();
else if (mode === '--build') {
  const version = Bun.spawnSync({ cmd: ['zig', 'version'], stdout: 'pipe', stderr: 'pipe' });
  if (version.exitCode !== 0 || new TextDecoder().decode(version.stdout).trim() !== '0.13.0') throw new Error('pinned Zig 0.13.0 required');
  const temporary = `${artifact}.${process.pid}.tmp`;
  try {
    const command = ['zig', 'cc', '-target', 'wasm32-wasi', '-O3', '-g0', '-Iintegrations/mcp/native', '-Ilib/libbonfyre/include',
      'integrations/mcp/native/spines_helix_export.c', 'lib/libbonfyre/src/bf_sha256.c', '-lm', '-Wl,--no-entry',
      '-Wl,--export=bf_reduce', '-Wl,--export=malloc', '-Wl,--export=free', '-Wl,--export-memory', '-Wl,--strip-all',
      '-Wl,--max-memory=0x2000000', '-o', temporary];
    const result = Bun.spawnSync({ cmd: command, cwd: root, stdout: 'pipe', stderr: 'pipe' });
    if (result.exitCode !== 0) throw new Error(`Zig build failed: ${new TextDecoder().decode(result.stderr).slice(0, 1000)}`);
    checkArtifact(temporary);
    renameSync(temporary, artifact);
  } finally { if (existsSync(temporary)) unlinkSync(temporary); }
} else throw new Error('usage: build_spines_wasm.mjs [--verify|--build]');
console.log(JSON.stringify({ verified: true, artifact: lock.artifact, sha256: lock.artifact_sha256, bytes: lock.artifact_bytes }));
