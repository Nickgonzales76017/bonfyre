import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createV6Bfp, deltaApply, deltaEncode, packBfp, verifyBfp } from '../v6_bfp.mjs';

test('section identities reject filesystem aliases before packing', () => {
  const home = fixture();
  for (const name of ['.', 'a/./b', 'a//b', 'a/'])
    expect(() => packBfp(join(home, 'alias.bfp'), { [name]: Buffer.from('body') })).toThrow('unsafe section');
});

test('stream verification checks cooperative cancellation between bounded reads', () => {
  const home = fixture(), path = join(home, 'cancel.bfp');
  packBfp(path, { body: Buffer.alloc(12 * 1024 * 1024, 3) });
  let checks = 0;
  expect(() => verifyBfp(path, { signal: { throwIfAborted() { if (++checks === 4) throw new Error('cancelled'); } } })).toThrow('cancelled');
  expect(checks).toBe(4);
});
import { canonicalJson } from '../canonical_json.mjs';

const fixtures = [];
afterEach(() => { for (const home of fixtures.splice(0)) rmSync(home, { recursive: true, force: true }); });
function fixture() { const home = mkdtempSync(join(tmpdir(), 'bonfyre-v6-bfp-')); fixtures.push(home); return home; }
const bytes = text => Buffer.from(text, 'utf8');
const sha = value => createHash('sha256').update(value).digest('hex');

test('RAW, COPY_ADD and DEFLATE_DICT32 preserve exact target and digest identity', () => {
  const source = bytes('prefix-' + 'x'.repeat(34000) + '-suffix');
  const target = bytes('prefix-' + 'x'.repeat(34000) + '-changed-suffix');
  for (const codec of ['RAW', 'COPY_ADD', 'DEFLATE_DICT32']) {
    const doc = deltaEncode(source, target, codec);
    expect(doc.schema).toBe('bonfyre.dictionary-delta.v1');
    expect(doc.source_sha256).toBe(sha(source));
    expect(doc.target_sha256).toBe(sha(target));
    expect(deltaApply(source, doc)).toEqual(target);
  }
  const copy = deltaEncode(source, target, 'COPY_ADD');
  expect(Buffer.from(copy.payload_hex, 'hex').toString()).toBe(canonicalJson({ prefix: source.length - 6, suffix: 6, middle_hex: bytes('changed-').toString('hex') }, { ascii: false }));
  const dict = deltaEncode(source, target, 'DEFLATE_DICT32').metadata;
  expect(dict.dictionary_sha256).toBe(sha(source.subarray(source.length - 32768)));
  expect(dict.dictionary_hex).toBe(source.subarray(source.length - 32768).toString('hex'));
});

test('delta application refuses changed source, target, dictionary and malformed bounds', () => {
  const source = bytes('abcdef'), target = bytes('abXYef');
  const doc = deltaEncode(source, target);
  expect(() => deltaApply(bytes('else'), doc)).toThrow('source');
  expect(() => deltaApply(source, { ...doc, target_sha256: '0'.repeat(64) })).toThrow('target');
  expect(() => deltaApply(source, { ...doc, payload_hex: 'zz' })).toThrow('payload');
  const invalid = { prefix: 999, suffix: 1, middle_hex: '' };
  expect(() => deltaApply(source, { ...doc, payload_hex: Buffer.from(JSON.stringify(invalid)).toString('hex') })).toThrow('bounds');
  const compressed = deltaEncode(source, target, 'DEFLATE_DICT32');
  expect(() => deltaApply(source, { ...compressed, metadata: { ...compressed.metadata, dictionary_sha256: '0'.repeat(64) } })).toThrow('dictionary');
});

test('pack is deterministic across section insertion order and verifies sorted offsets and non-ASCII metadata', () => {
  const home = fixture(), first = join(home, 'first.bfp'), second = join(home, 'second.bfp');
  const sections = { 'z/data': bytes('three'), 'a.txt': bytes('one'), 'b.bin': bytes('two') };
  const one = packBfp(first, sections, { title: '雪' });
  const two = packBfp(second, { 'b.bin': sections['b.bin'], 'z/data': sections['z/data'], 'a.txt': sections['a.txt'] }, { title: '雪' });
  expect(readFileSync(first)).toEqual(readFileSync(second));
  expect(one.sha256).toBe(two.sha256);
  expect(one.manifest.sections.map(item => [item.name, item.offset, item.length])).toEqual([
    ['a.txt', 0, 3], ['b.bin', 3, 3], ['z/data', 6, 5],
  ]);
  const headerLength = Number(readFileSync(first).readBigUInt64BE(8));
  expect(readFileSync(first).subarray(16, 16 + headerLength).toString()).toContain('雪');
  const verified = verifyBfp(first);
  expect(verified.verified).toBe(true);
  expect(verified.sha256).toBe(one.sha256);
  expect(verified.reconstructed).toEqual(one.manifest.sections.map(item => ({ name: item.name, bytes: item.length, sha256: item.sha256 })));
});

test('pack streams file sections and extraction happens only after all section digests pass', () => {
  const home = fixture(), source = join(home, 'large.bin'), output = join(home, 'body.bfp'), extracted = join(home, 'out');
  const payload = Buffer.alloc(5 * 1024 * 1024, 73); writeFileSync(source, payload);
  const packed = packBfp(output, { 'data/large.bin': source, 'small.txt': bytes('ok') });
  expect(packed.bytes).toBeGreaterThan(payload.length);
  const verified = verifyBfp(output, { extract: extracted });
  expect(verified.verified).toBe(true);
  expect(readFileSync(join(extracted, 'data/large.bin'))).toEqual(payload);
  const altered = readFileSync(output); altered[altered.length - 1] ^= 1; writeFileSync(output, altered);
  const untouched = join(home, 'must-stay-empty');
  expect(() => verifyBfp(output, { extract: untouched })).toThrow('digest mismatch');
  expect(() => readFileSync(join(untouched, 'data/large.bin'))).toThrow();
});

test('format refuses bad magic, gap, trailing body, unsafe names and unsupported header', () => {
  const home = fixture(), path = join(home, 'body.bfp');
  expect(() => packBfp(path, { '../escape': bytes('x') })).toThrow('unsafe');
  packBfp(path, { 'a.txt': bytes('abc') });
  const original = readFileSync(path);
  const wrongMagic = Buffer.from(original); wrongMagic[0] = 0; writeFileSync(path, wrongMagic);
  expect(() => verifyBfp(path)).toThrow('magic');
  writeFileSync(path, Buffer.concat([original, bytes('extra')]));
  expect(() => verifyBfp(path)).toThrow('unmanifested');
  writeFileSync(path, original);
  expect(verifyBfp(path).verified).toBe(true);
});

test('factory confines path/source and keeps optional native witness separate from JSON authority', async () => {
  const home = fixture(), outside = fixture(), runCalls = [];
  const owner = createV6Bfp({ root: home, home,
    nativeVerifier: '/native/bfp-verify', run: async (argv, options) => {
      runCalls.push({ argv, options });
      return { exit_code: 1, stdout: '{"verified":false}', stderr: 'unsupported header', duration_ms: 1 };
    } });
  const path = join(home, 'body.bfp');
  expect(() => owner.pack(join(outside, 'escape.bfp'), { a: bytes('x') })).toThrow('escapes');
  expect(() => owner.pack(path, { a: join(outside, 'source') })).toThrow('escapes');
  owner.pack(path, { a: bytes('x') });
  const result = await owner.verify(path);
  expect(result.verified).toBe(true);
  expect(result.native_witness.state).toBe('failed');
  expect(runCalls[0].argv).toEqual(['/native/bfp-verify', join(realpathSync(home), 'body.bfp')]);
  expect(runCalls[0].options.timeoutMs).toBe(120000);
  const linked = join(home, 'linked'); symlinkSync(outside, linked);
  await expect(owner.verify(join(linked, 'outside.bfp'))).rejects.toThrow('escapes');
  const dangling = join(home, 'dangling.bfp'); symlinkSync(join(outside, 'missing.bfp'), dangling);
  await expect(owner.verify(dangling)).rejects.toThrow();
  expect(() => owner.pack(dangling, { a: bytes('x') })).toThrow();
});

test('extraction refuses symlink ancestors and dangling destination links', () => {
  const home = fixture(), outside = fixture(), path = join(home, 'body.bfp');
  packBfp(path, { 'nested/a.txt': bytes('safe') });
  const extraction = join(home, 'out');
  symlinkSync(outside, extraction);
  expect(() => verifyBfp(path, { extract: extraction })).toThrow('symlink');
  rmSync(extraction);
  const nested = join(extraction, 'nested');
  mkdirSync(nested, { recursive: true });
  symlinkSync(join(outside, 'missing.txt'), join(nested, 'a.txt'));
  expect(() => verifyBfp(path, { extract: extraction })).toThrow('symlink');
  expect(existsSync(join(outside, 'missing.txt'))).toBe(false);
});

const installedNative = join(homedir(), '.local/bin/bonfyre-bfp-verify');
test.skipIf(!existsSync(installedNative))('installed native verifier corroborates the Bun-packed temporary archive', () => {
  const home = fixture(), path = join(home, 'native.bfp');
  packBfp(path, { 'a.txt': bytes('a'), 'b/data': bytes('xyz') }, { test: true });
  const result = Bun.spawnSync({ cmd: [installedNative, path], stdout: 'pipe', stderr: 'pipe' });
  expect(result.exitCode).toBe(0);
  expect(JSON.parse(new TextDecoder().decode(result.stdout))).toEqual({ verified: true, format: 'BFP6',
    sections: 2, payload_bytes: 4, streaming: true });
  expect(verifyBfp(path).verified).toBe(true);
});
