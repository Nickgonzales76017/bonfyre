import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileAttention } from '../tools/compile_source_attention.mjs';

const roots = ['impact', 'pressure', 'warm', 'gap', 'cold'].map(id =>
  ({ root_id: id, root_address: `https://${id}.test/` }));

test('attention compiles distinct scheduling signals without authority', () => {
  const result = compileAttention({ roots, gaps: [{ root_id: 'gap', kind: 'identity' }],
    pressure: [{ root_id: 'pressure', candidate_occurrences: 32 },
      { root_id: 'warm', candidate_closure_reuse: 1 }],
    impact: [{ root_id: 'impact', affected_closures: 4 },
      { root_id: 'impact', affected_scenes: 1 }] });
  expect(result.plans.map(row => [row.root_id, row.temperature])).toEqual([
    ['impact', 'hot'], ['pressure', 'hot'], ['warm', 'warm'], ['gap', 'cool'], ['cold', 'cold']]);
  expect(result.plans[0].basis).toBe('measured_reverse_dependency_impact');
  expect(result.plans.find(row => row.root_id === 'gap').reheat_on).toBe('source-demand:gap');
  expect(result.plans.find(row => row.root_id === 'cold').reheat_on).toBe('');
  expect(result.summary.temperatures).toEqual({ cold: 1, cool: 1, hot: 2, warm: 1 });
  expect(result.plans.every(row => row.truth ===
    'attention_projection_only_not_admission_fetch_authority_or_value')).toBe(true);
});

test('legacy reuse fallback and impact maxima remain stable', () => {
  const { plans } = compileAttention({ roots: [{ root_id: 'reuse' }], gaps: [],
    impact: [{ root_id: 'reuse', subject_reuse: 2, scene_reuse: 1 },
      { root_id: 'reuse', subject_reuse: 1, scene_reuse: 3 }] });
  expect(plans[0].temperature).toBe('warm');
  expect(plans[0].evidence).toMatchObject({ affected_closures: 2, affected_scenes: 3 });
  expect(() => compileAttention({ roots: [], policy: { hot_impact_scenes: 0 } })).toThrow();
  const lowered = compileAttention({ roots: [{ root_id: 'policy' }], gaps: [],
    pressure: [{ root_id: 'policy', candidate_occurrences: 2 }],
    policy: { hot_pressure_occurrences: 2 } });
  expect(lowered.plans[0].temperature).toBe('hot');
});

test('Bun CLI writes the Source Fabric attention artifact', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-source-attention-'));
  const root = join(dir, 'roots.jsonl'), gaps = join(dir, 'gaps.jsonl'), out = join(dir, 'attention.jsonl');
  writeFileSync(root, `${JSON.stringify(roots[0])}\n`);
  writeFileSync(gaps, '');
  const run = Bun.spawnSync(['bun', new URL('../tools/compile_source_attention.mjs', import.meta.url).pathname,
    '--roots', root, '--gaps', gaps, '--out', out]);
  expect(run.exitCode).toBe(0);
  expect(JSON.parse(readFileSync(out, 'utf8')).temperature).toBe('cold');
});
