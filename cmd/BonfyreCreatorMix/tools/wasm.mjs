import {readFileSync, mkdtempSync, rmSync, copyFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import {resolve, join} from 'node:path';
import {pathToFileURL} from 'node:url';

const root = resolve(import.meta.dirname, '../../..');
const directory = join(root, 'Aurekai/native/creator');
const sha = body => createHash('sha256').update(body).digest('hex');

export function verifyCreatorWasm() {
  const lock = JSON.parse(readFileSync(join(directory, 'wasm.lock.json')));
  for (const [file, digest] of Object.entries(lock.sources)) {
    if (sha(readFileSync(join(directory, file))) !== digest) throw new Error(`creator WASM source drift: ${file}`);
  }
  const bytes = readFileSync(join(directory, 'mix_kernel.wasm'));
  if (sha(bytes) !== lock.artifact_sha256) throw new Error('creator WASM artifact drift');
  return {lock, bytes};
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2);
  if (args.length > 1 || (args.length && args[0] !== '--build')) throw new Error('usage: wasm.mjs [--build]');
  const {lock, bytes} = verifyCreatorWasm();
  if (args[0] === '--build') {
    const zig = process.env.ZIG || 'zig';
    const version = spawnSync(zig, ['version'], {encoding: 'utf8'});
    if (version.status !== 0 || version.stdout.trim() !== lock.zig_version) throw new Error(`requires Zig ${lock.zig_version}`);
    const temp = mkdtempSync(join(tmpdir(), 'creator-wasm-'));
    try {
      const output = join(temp, 'kernel.wasm');
      const build = spawnSync(zig, ['cc', ...lock.flags, 'mix_kernel.c', '-o', output], {cwd: directory, encoding: 'utf8'});
      if (build.status !== 0) throw new Error(build.stderr || build.error?.message || 'WASM build failed');
      if (sha(readFileSync(output)) !== lock.artifact_sha256) throw new Error('rebuilt WASM differs from reviewed artifact');
      copyFileSync(output, join(directory, 'mix_kernel.wasm'));
    } finally { rmSync(temp, {recursive: true, force: true}); }
  }
  console.log(JSON.stringify({status: 'verified', rebuilt: args[0] === '--build', bytes: bytes.length, sha256: lock.artifact_sha256, zig_version: lock.zig_version}));
}
