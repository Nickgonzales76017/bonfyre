import {test, expect} from 'bun:test';
import {mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';

const binary = process.env.BONFYRE_COMPRESS_TEST_BINARY || resolve(import.meta.dir, '../bonfyre-compress');
const tar = Bun.which('tar'), zstd = Bun.which('zstd');
if (!tar || !zstd) throw new Error('Real tar and zstd are required for the stream oracle');

async function fixture(fn) {
  const root = mkdtempSync(join(tmpdir(), 'compress-stream-'));
  const source = join(root, "family; literal ' argument"), bin = join(root, 'bin');
  mkdirSync(source); mkdirSync(bin);
  const captured = join(root, 'archive.tar'), calls = join(root, 'tar-calls'), cli = join(root, 'zstd-calls');
  writeFileSync(join(bin, 'tar'), `#!${process.execPath}
import {appendFileSync, writeFileSync} from 'node:fs';
appendFileSync(${JSON.stringify(calls)}, 'tar\\n');
if (process.env.FAIL_PRODUCER) { writeFileSync(1, Buffer.alloc(65536, 120)); process.exit(42); }
const result = Bun.spawnSync([${JSON.stringify(tar)}, ...process.argv.slice(2)], {maxBuffer: 32*1024*1024});
writeFileSync(${JSON.stringify(captured)}, result.stdout);
writeFileSync(1, result.stdout);
process.exit(result.exitCode);
`, {mode: 0o700});
  writeFileSync(join(bin, 'zstd'), `#!${process.execPath}
import {appendFileSync} from 'node:fs';
appendFileSync(${JSON.stringify(cli)}, 'zstd\\n');
const child = Bun.spawn([${JSON.stringify(zstd)}, ...process.argv.slice(2)], {stdin:'inherit',stdout:'inherit',stderr:'inherit'});
process.exit(await child.exited);
`, {mode: 0o700});
  const run = (extra = {}) => Bun.spawnSync([binary, 'savings', source], {env: {...process.env, PATH: bin, ...extra}, timeout: 20000, maxBuffer: 1024*1024});
  try { await fn({root, source, bin, captured, calls, cli, run}); }
  finally { rmSync(root, {recursive: true, force: true}); }
}

test('savings counts one real archive stream and matches independent zstd output', () => fixture(({source, captured, calls, cli, run}) => {
  const text = Buffer.from('One retained source body, shared across representations.\n'.repeat(70000));
  writeFileSync(join(source, 'body.txt'), text);
  mkdirSync(join(source, 'nested'));
  writeFileSync(join(source, 'nested', '.hidden'), 'hidden');
  const result = run();
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  const measured = JSON.parse(result.stdout.toString()), archive = readFileSync(captured);
  const oracle = Bun.spawnSync([zstd, '-19', '-q', '-c'], {stdin: archive, maxBuffer: 16*1024*1024});
  expect(oracle.exitCode, oracle.stderr.toString()).toBe(0);
  expect(measured.directory).toBe(source);
  expect(measured.raw_bytes).toBe(text.length + 6);
  expect(measured.archive_stream_bytes).toBe(archive.length);
  expect(measured.compressed_bytes).toBe(oracle.stdout.length);
  expect(measured.stream_ratio).toBeCloseTo(oracle.stdout.length / archive.length, 4);
  expect(readFileSync(calls, 'utf8')).toBe('tar\n');
  expect(existsSync(cli)).toBe(false);
}), 30000);

test('an empty family still finalizes and counts the complete compressed frame', () => fixture(({captured, calls, run}) => {
  const result = run();
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  const measured = JSON.parse(result.stdout.toString()), archive = readFileSync(captured);
  const oracle = Bun.spawnSync([zstd, '-19', '-q', '-c'], {stdin: archive});
  expect(oracle.exitCode).toBe(0);
  expect(measured.raw_bytes).toBe(0);
  expect(measured.archive_stream_bytes).toBe(archive.length);
  expect(measured.compressed_bytes).toBe(oracle.stdout.length);
  expect(readFileSync(calls, 'utf8')).toBe('tar\n');
}));

test('multi-job frames match the CLI worker mode on a larger archive', () => fixture(({source, captured, run}) => {
  const block = Buffer.alloc(1024*1024);
  let state = 1729;
  for (let i = 0; i < block.length; i++) { state ^= state << 13; state ^= state >>> 17; state ^= state << 5; block[i] = state & 255; }
  writeFileSync(join(source, 'body.bin'), Buffer.concat(Array(17).fill(block)));
  const result = run();
  expect(result.exitCode, result.stderr.toString()).toBe(0);
  const measured = JSON.parse(result.stdout.toString()), archive = readFileSync(captured);
  const oracle = Bun.spawnSync([zstd, '-19', '-q', '-c'], {stdin: archive, maxBuffer: 32*1024*1024});
  expect(oracle.exitCode).toBe(0);
  expect(measured.archive_stream_bytes).toBe(archive.length);
  expect(measured.compressed_bytes).toBe(oracle.stdout.length);
}), 30000);

test('a failed or unavailable archive producer emits no successful measurement', () => fixture(({root, run}) => {
  for (const env of [{FAIL_PRODUCER: '1'}, {PATH: join(root, 'absent')}]) {
    const result = run(env);
    expect(result.exitCode).not.toBe(0);
    expect(result.stdout.length).toBe(0);
  }
}));
