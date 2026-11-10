import { test, expect } from 'bun:test';
import { materialize } from '../tools/materialize_incumbent_records.mjs';
import { promote } from '../tools/promote_external_resource.mjs';

const base = { provider: 'github', kind: 'github_repository', source_ref: 'source:github',
  receipt_ref: 'foreign:github', observed_at: '2026-09-28T00:00:00Z',
  payload: { id: '42', title: 'org/repo', url: 'https://github.test/org/repo' } };

test('public repository becomes a stable observed Project, never carrier authority', () => {
  const first = materialize(base)[0];
  expect(first.type).toBe('project');
  expect(first.body.stage).toBe('observed');
  expect(first.body.subject_ref).toBe('external:github:42');
  expect(materialize(base)[0].body.project_id).toBe(first.body.project_id);
  expect(first.body.cannot_infer).toContain('authority');
  expect(() => materialize({ ...base, receipt_ref: '' })).toThrow();
  expect(() => materialize({ ...base, payload: { ...base.payload, url: 'http://unsafe.test' } })).toThrow();
});

test('211 classification promotes only to an observed saveable opportunity', () => {
  const record = { ...base, provider: '211', kind: '211_resource', payload: {
    id: 'food', title: 'Food', url: 'https://211.test/food', functions: 'food' } };
  const resource = materialize(record)[0];
  expect(resource.type).toBe('external_resource');
  const promoted = promote(record);
  const opportunity = materialize(promoted)[0];
  expect(opportunity.type).toBe('household_opportunity');
  expect(opportunity.body.resource_ref).toBe(resource.body.ext_id);
  expect(opportunity.body.cannot_infer).toContain('eligibility');
  expect(() => promote({ ...record, provider: 'clever' })).toThrow();
  expect(() => promote({ ...record, payload: { ...record.payload, functions: '' } })).toThrow();
});
