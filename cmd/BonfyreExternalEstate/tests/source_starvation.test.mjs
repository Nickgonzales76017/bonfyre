import { test, expect } from 'bun:test';
import { compileStarvation, sourceStarvationPolicy } from '../tools/compile_source_starvation.mjs';

test('diagnostic laws distinguish pressure, missing Scene edges, and latent active roots', () => {
  const result = compileStarvation({
    roots: [
      { root_id: 'root:pressure', root_address: 'https://pressure.test/', admitted_channels: 2, active_channels: 0 },
      { root_id: 'root:scene', root_address: 'https://scene.test/', admitted_channels: 1, active_channels: 0 },
      { root_id: 'root:latent', root_address: 'https://latent.test/', admitted_channels: 1, active_channels: 1 },
      { root_id: 'root:closure', root_address: 'https://closure.test/', admitted_channels: 1, active_channels: 1 },
    ],
    gaps: [{ root_id: 'root:scene', kind: 'identity' }],
    pressure: [{ root_id: 'root:pressure', candidate_occurrences: 5, candidate_closure_reuse: 2 }],
    impact: [{ root_id: 'root:scene', affected_closures: 1, affected_subjects: 1 },
      { root_id: 'root:closure', affected_closures: 2, affected_subjects: 2 }],
    attention: [{ root_id: 'root:pressure', temperature: 'hot', basis: 'candidate pressure' }],
    sceneReceipts: [
      { schema: 'bonfyre.scene-source-receipt.v1', scene_key: 'scene:one', source_roots: [{ root_id: 'root:scene' }] },
      { schema: 'bonfyre.scene-source-receipt.v1', scene_key: 'scene:empty', subject_address: '/s/empty',
        actor_scope: 'public', source_roots: [], external_candidate_dependencies: [{}], action_apertures: ['read'] },
    ],
  });
  expect(result.diagnostics[0].root_id).toBe('root:pressure');
  expect(result.diagnostics.find(row => row.root_id === 'root:pressure').issues.map(row => row.code))
    .toEqual(['ADMITTED_NO_CLOSURE_IMPACT', 'PRESSURED_WITHOUT_OBSERVED_IMPACT', 'HOT_FROM_PRESSURE_ONLY']);
  expect(result.diagnostics.find(row => row.root_id === 'root:scene').issues.map(row => row.code))
    .toEqual(['SCENE_DEPENDS_ON_NONACTIVE_ROOT', 'IMPACTFUL_ROOT_HAS_OPEN_GAPS']);
  expect(result.diagnostics.find(row => row.root_id === 'root:latent').issues.map(row => row.code))
    .toEqual(['ADMITTED_NO_CLOSURE_IMPACT', 'ACTIVE_BUT_UNREFERENCED']);
  expect(result.diagnostics.find(row => row.root_id === 'root:closure').issues.map(row => row.code))
    .toEqual(['CLOSURE_IMPACT_WITHOUT_SCENE_EDGE']);
  expect(result.emptyScenes).toMatchObject([{ scene_key: 'scene:empty', candidate_dependencies: 1,
    truth: 'scene_dependency_diagnostic_not_material_failure_verdict' }]);
  expect(result.summary).toMatchObject({ roots: 4, roots_with_diagnostics: 4,
    scenes_without_source_dependency: 1 });
  expect(result.diagnostics.every(row => row.truth === 'engineering_diagnostic_not_semantic_failure_verdict')).toBe(true);
});

test('authored diagnostic severity propagates without changing evidence', () => {
  const base = sourceStarvationPolicy();
  const policy = structuredClone(base);
  policy.rules.find(rule => rule.code === 'ACTIVE_BUT_UNREFERENCED').severity = 'high';
  const result = compileStarvation({ roots: [{ root_id: 'latent', active_channels: 1 }], policy });
  expect(result.diagnostics[0].issues[0].severity).toBe('high');
  expect(result.diagnostics[0].issues[0].code).toBe('ACTIVE_BUT_UNREFERENCED');
});
