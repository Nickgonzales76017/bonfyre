import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { compilePhenotypes, compileCarrier, compileConnection, ROOTS } from '../tools/compile_coordination_family.mjs';

test('declared incumbent phenotypes retain distinct public and authority boundaries', () => {
  const spec = JSON.parse(readFileSync(resolve(import.meta.dir, '../../../schemas/control/incumbent-coordination-phenotypes.json'), 'utf8'));
  const rows = compilePhenotypes(spec);
  const wordpress = rows.find(row => row.phenotype === 'wordpress-estate');
  expect(wordpress.bridge_state).toBe('verified_installation');
  expect(wordpress.public_carriers).toContain('woocommerce');
  expect(rows.filter(row => row.phenotype !== 'wordpress-estate').every(row => row.bridge_state === 'deferred')).toBe(true);
  expect(() => compilePhenotypes({ phenotypes: [spec.phenotypes[0], spec.phenotypes[0]] })).toThrow();
});

test('public participation carriers retain exact candidate roots without becoming subjects', () => {
  for (const kind of Object.keys(ROOTS)) {
    const row = compileCarrier({ provider: 'test', carrier_kind: kind, source_ref: 'source:test',
      receipt_ref: 'receipt:test', observed_at: '2026',
      payload: { id: kind, title: 'Test', url: 'https://example.test/x' } });
    expect(row.body.candidate_roots).toBe(ROOTS[kind]);
    expect(row.body.cannot_infer).toContain('not a Bonfyre subject');
  }
  expect(() => compileCarrier({ provider: 'test' })).toThrow();
});

test('connection requires both control proof and explicit provider grant', () => {
  const carrier = { carrier_id: 'carrier:github:org/repo', provider: 'github' };
  const base = { subject_ref: 'project:repo', carrier, at: '2026-09-28T00:00:00Z' };
  const proof = { state: 'bridge_control_verified', receipt_ref: 'receipt:proof' };
  expect(compileConnection({ ...base, phase: 'observe' }).body.state).toBe('observed');
  expect(compileConnection({ ...base, phase: 'verify', proof }).body.state).toBe('verified');
  const connected = compileConnection({ ...base, phase: 'connect', proof,
    grant: { receipt_ref: 'receipt:grant', capabilities: ['read.public'] } }).body;
  expect(connected.state).toBe('connected');
  expect(connected.granted_capabilities).toEqual(['read.public']);
  expect(() => compileConnection({ ...base, phase: 'verify' })).toThrow();
  expect(() => compileConnection({ ...base, phase: 'connect', proof })).toThrow();
  expect(compileConnection({ ...base, phase: 'revoke' }).body.state).toBe('revoked');
});
