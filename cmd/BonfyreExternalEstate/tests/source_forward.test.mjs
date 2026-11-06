import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileForward, sourceForwardPolicy } from '../tools/compile_source_forward.mjs';

const input = { roots: [{ root_id: 'r1', address: 'https://one.test/' }],
  gaps: [{ gap_id: 'g1', root_id: 'r1', kind: 'access', reason: 'private data',
    resolution_classes: ['PUBLIC_BACKFILL', 'OUTREACH', 'PURCHASE', 'OUTREACH'] },
  { gap_id: 'g2', root_id: 'r1', kind: 'identity', resolution_classes: ['DISCOVER'] }],
  demand: [{ root_address: 'https://one.test/', subject_reuse: 2, rights_risk: 1,
    candidate_branches_before: 8, candidate_branches_after: 2, queries_avoided: 3 }] };

test('SourceGap compiler retains measured dividend and non-effecting routes', () => {
  const { summary, opportunities } = compileForward(input);
  expect(summary.gap_plans).toBe(2);
  expect(summary.measured_plans).toBe(2);
  expect(summary.resolution_classes).toEqual({ DISCOVER: 1, OUTREACH: 1, PUBLIC_BACKFILL: 1, PURCHASE: 1 });
  expect(summary.plans[0].closure_dividend).toMatchObject({ state: 'MEASURED', score: 0.5,
    calculation_reduction: { branches_eliminated: 6, reduction_ratio: 0.75, queries_avoided: 3 } });
  expect(summary.plans[0].resolution_plan.map(row => row.class)).toEqual([
    'PUBLIC_BACKFILL', 'OUTREACH', 'PURCHASE']);
  expect(opportunities).toMatchObject([{ possible_relationships: ['OUTREACH'],
    identity_disclosure_required: true, effect: 'candidate_only' }]);
  expect(summary.plans.every(row => row.cannot_infer.includes('does not authorize'))).toBe(true);
});

test('absent demand stays unmeasured, not zero value', () => {
  const { summary, opportunities } = compileForward({ roots: input.roots, gaps: input.gaps });
  expect(summary.measured_plans).toBe(0);
  expect(summary.plans[0].closure_dividend.score).toBeNull();
  expect(opportunities[0].closure_dividend_state).toBe('UNMEASURED');
});

test('authored policy changes score while the gap owner controls route order', () => {
  const base = sourceForwardPolicy();
  const policy = { ...base, benefit_weights: { ...base.benefit_weights, subject_reuse: 3 } };
  const changed = compileForward({ ...input, policy });
  expect(changed.summary.plans[0].closure_dividend.score).toBe(4.5);
  expect(changed.summary.plans[0].resolution_plan.map(row => row.class)).toEqual([
    'PUBLIC_BACKFILL', 'OUTREACH', 'PURCHASE']);
  expect(changed.summary.plans[0].resolution_plan.every(row => !('next' in row))).toBe(true);
});

test('Bun CLI writes summary and relationship artifacts', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-source-forward-'));
  const roots = join(dir, 'roots.json'), gaps = join(dir, 'gaps.json'), out = join(dir, 'forward.json');
  const opportunities = join(dir, 'opportunities.jsonl');
  writeFileSync(roots, JSON.stringify({ roots: input.roots }));
  writeFileSync(gaps, JSON.stringify({ gaps: input.gaps }));
  const run = Bun.spawnSync(['bun', new URL('../tools/compile_source_forward.mjs', import.meta.url).pathname,
    '--roots', roots, '--gaps', gaps, '--out', out, '--opportunities', opportunities]);
  expect(run.exitCode).toBe(0);
  expect(JSON.parse(readFileSync(out, 'utf8')).gap_plans).toBe(2);
  expect(JSON.parse(readFileSync(opportunities, 'utf8')).effect).toBe('candidate_only');
});
