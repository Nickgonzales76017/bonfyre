import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execPathFromUnitText, attest, sha256, sealedIndex } from '../release_attest.mjs';

test('last systemd ExecStart names the script, not env/interpreter or an argument binary', () => {
  const text = 'ExecStart=/usr/bin/python3 /opt/old.py\nExecStart=\nExecStart=/usr/bin/env bun /opt/new.mjs --auth-binary /usr/local/bin/bonfyre-auth\n';
  expect(execPathFromUnitText(text, path => ['/usr/bin/python3', '/opt/old.py', '/usr/bin/env', '/opt/new.mjs', '/usr/local/bin/bonfyre-auth'].includes(path))).toBe('/opt/new.mjs');
  expect(execPathFromUnitText('ExecStart=/usr/local/bin/bonfyre-web-address serve --auth-binary /usr/local/bin/bonfyre-auth', () => true)).toBe('/usr/local/bin/bonfyre-web-address');
});
test('sealed mismatch reports but running mismatch and changed script fail', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-attest-'));
  const manifest = join(dir, 'sha256-manifest.txt');
  writeFileSync(manifest, '0'.repeat(64) + '  binary\n' + '1'.repeat(64) + '  script.mjs\n');
  expect(sealedIndex(manifest).get('binary')).toBe('0'.repeat(64));
  const command = (...argv) => argv[1] === 'show' ? '42' : argv[1] === 'is-active' ? 'active' : '/proc/42/exe';
  const binary = attest(['binary'], manifest, { command, unitPath: () => join(dir, 'binary'), kind: () => 'elf', hash: path => path.startsWith('/proc') ? 'r' : 'd', newer: () => false });
  expect(binary.fatal).toBe(1);
  expect(binary.rows[0].verdict).toBe('RUNNING_DIFFERS_FROM_DEPLOYED');
  const script = attest(['script'], manifest, { command, unitPath: () => join(dir, 'script.mjs'), kind: () => 'script', hash: () => 'd', newer: () => true });
  expect(script.fatal).toBe(1);
  expect(script.rows[0].verdict).toBe('SOURCE_CHANGED_SINCE_START');
  const ordinary = attest(['binary'], manifest, { command, unitPath: () => join(dir, 'binary'), kind: () => 'elf', hash: path => path.startsWith('/proc') ? 'd' : 'd', newer: () => false });
  expect(ordinary.fatal).toBe(0);
  expect(ordinary.rows[0].verdict).toBe('deployed differs from sealed (expected during development)');
});
test('hash is of actual bytes', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-attest-hash-'));
  const path = join(dir, 'payload'); writeFileSync(path, 'abc');
  expect(sha256(path)).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
});
