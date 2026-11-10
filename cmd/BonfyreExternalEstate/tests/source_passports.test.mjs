import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compilePassports } from '../tools/compile_source_passports.mjs';

test('passport advertisements preserve capability and authority boundaries', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-passports.'));
  try {
    const passports = [
      { passport: 'public-music', state: 'ADMITTED', name: 'Música Publica',
        capabilities: ['identify-recording'], roles: ['reference'], access: ['dump'], topology: ['recording'],
        subject_kinds: ['track'], identifiers: ['isrc'], refresh: 'daily', temporal: 'historical', spatial: 'global',
        evidence: 'retained', mirror: 'allowed', reuse: { allows: ['commercial-use'], unknown: [],
          requires: ['attribution'], varies_per_item: true }, credential: 'music-key',
        carrier: { robots: 'allows' }, census: ['music-census'], cannot_infer: 'a result -> authority' },
      { passport: 'private-catalog', state: 'DECLARED', name: 'Private Catalog', capabilities: [],
        roles: [], access: [], topology: [], refresh: 'unknown', temporal: 'unknown', spatial: 'unknown',
        evidence: 'declared', mirror: 'unknown', reuse: {}, cannot_infer: 'declared -> admitted' },
    ];
    const result = compilePassports(passports);
    expect(result.summary).toEqual({ advertisements: 2, admitted: 1 });
    expect(result.advertisements[0]).toMatchObject({ candidate_kind: 'external_source',
      exact_verifier: 'ForeignTwin:receipt', lowerer_ref: 'InternetSource:census',
      dependencies: ['SourcePassport:public-music', 'InternetSource:music-census'] });
    expect(result.advertisements[0].terms).toContain('reuse:varies-per-item');
    expect(result.advertisements[0].terms).toContain('authority:credential-music-key');
    expect(result.advertisements[1]).toMatchObject({ exact_verifier: 'SourcePassport:declared',
      lowerer_ref: 'SourcePassport:unlowered' });
    const input = join(dir, 'passports.json'), out = join(dir, 'advertisements.jsonl');
    writeFileSync(input, JSON.stringify(passports));
    const child = Bun.spawnSync(['bun', new URL('../tools/compile_source_passports.mjs', import.meta.url).pathname,
      '--json', input, '--out', out]);
    expect(child.exitCode).toBe(0);
    expect(readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse)).toEqual(result.advertisements);
    const authored = join(dir, 'authored.jsonl');
    const native = Bun.spawnSync(['bun', new URL('../tools/compile_source_passports.mjs', import.meta.url).pathname,
      '--out', authored]);
    expect(native.exitCode).toBe(0);
    expect(readFileSync(authored, 'utf8').trim().split('\n').map(JSON.parse).length).toBeGreaterThan(0);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
