import { test, expect } from 'bun:test';
import { compileTaskPlan, affectedTaskPlan, diffTaskPlans, missionPlanContext } from '../mission_plan.mjs';

const task = (id, dependencies = [], extra = {}) => ({ id, dependencies, capability: 'pure.test', ...extra });
const diamond = () => [task('a'), task('b', ['a']), task('c', ['a']), task('d', ['b', 'c']), task('unrelated')];

test('stable content identity, exact waves and input snapshots survive input order', () => {
  const tasks = diamond(), plan = compileTaskPlan(tasks, { context: { policy: 'candidate' } });
  expect(plan.waves).toEqual([['a', 'unrelated'], ['b', 'c'], ['d']]);
  expect(plan.stats).toEqual({ tasks: 5, edges: 4, waves: 3, maximum_wave_width: 2 });
  expect(compileTaskPlan([...tasks].reverse(), { context: { policy: 'candidate' } }).digest).toBe(plan.digest);
  tasks[0].capability = 'changed'; tasks[1].dependencies.length = 0;
  expect(plan.nodes.find(node => node.id === 'b').dependencies).toEqual(['a']);
  expect(Object.isFrozen(plan.nodes[0])).toBe(true);
});
test('one shared producer invalidates its exact downstream cut, no unrelated branch', () => {
  const plan = compileTaskPlan(diamond());
  expect(affectedTaskPlan(plan, ['b', 'b'])).toMatchObject({ affected: ['b', 'd'], stats: { visited_nodes: 2, visited_edges: 1 } });
  expect(affectedTaskPlan(plan, ['a'])).toMatchObject({ affected: ['a', 'b', 'c', 'd'], stats: { visited_nodes: 4, visited_edges: 4 } });
  expect(() => affectedTaskPlan(plan, ['missing'])).toThrow('unknown changed');
  expect(() => affectedTaskPlan(JSON.parse(JSON.stringify(plan)), ['a'])).toThrow('compiled locally');
});
test('semantic delta propagates transitively; runtime status does not change definition identity', () => {
  const before = compileTaskPlan(diamond());
  const tasks = diamond(); tasks[1].acceptance = ['proof'];
  const result = diffTaskPlans(before, compileTaskPlan(tasks));
  expect(result.changed).toEqual(['b']); expect(result.affected).toEqual(['b', 'd']);
  expect(result.reusable_candidates).toEqual(['a', 'c', 'unrelated']);
  expect(result.authority).toBe('candidate-invalidation-only');
  const operational = diamond(); operational[0].state = 'failed'; operational[0].evidence = ['receipt'];
  expect(compileTaskPlan(operational).digest).toBe(before.digest);
  const binding = diamond(); binding[0].provider_id = 'different-owner';
  expect(diffTaskPlans(before, compileTaskPlan(binding)).affected).toEqual(['a', 'b', 'c', 'd']);
});
test('policy changes invalidate all candidate reuse, even disconnected tasks', () => {
  const before = compileTaskPlan(diamond(), { context: { authority: 'old' } });
  const result = diffTaskPlans(before, compileTaskPlan(diamond(), { context: { authority: 'new' } }));
  expect(result.context_changed).toBe(true); expect(result.affected).toHaveLength(5); expect(result.reusable_candidates).toEqual([]);
});
test('new mission semantic fields and verification obligations participate conservatively in identity', () => {
  const mission = { id: 'old', tasks: diamond(), goal: 'same', verification: ['check'], created_at: 10 };
  const plan = value => compileTaskPlan(value.tasks, { context: missionPlanContext(value) });
  expect(plan({ ...mission, id: 'new', created_at: 20 }).digest).toBe(plan(mission).digest);
  expect(diffTaskPlans(plan(mission), plan({ ...mission, verification: ['different check'] })).affected).toHaveLength(5);
  expect(diffTaskPlans(plan(mission), plan({ ...mission, future_policy: 'new constraint' })).affected).toHaveLength(5);
});
test('removal and rewiring cannot preserve stale descendant keys', () => {
  const before = compileTaskPlan(diamond()), tasks = diamond().filter(item => item.id !== 'b');
  tasks.find(item => item.id === 'd').dependencies = ['c']; tasks.push(task('new', ['d']));
  const diff = diffTaskPlans(before, compileTaskPlan(tasks));
  expect(diff.removed).toEqual(['b']); expect(diff.added).toEqual(['new']);
  expect(diff.affected).toEqual(['d', 'new']);
});
test('duplicate ids, duplicate edges, missing parents, cycles and non-JSON definitions refuse', () => {
  for (const tasks of [[task('x'), task('x')], [task('x', ['y'])], [task('x', ['x'])],
    [task('a', ['b']), task('b', ['a'])], [task('a'), task('b', ['a', 'a'])], [task('../escape')], [task('a', [], { run: () => {} })]])
    expect(() => compileTaskPlan(tasks)).toThrow();
  expect(() => compileTaskPlan(Array.from({ length: 10001 }, (_, i) => task(`n${i}`)))).toThrow('count budget');
});
test('indexed invalidation visits only two nodes in a 10000-task forest', () => {
  const tasks = Array.from({ length: 10000 }, (_, i) => task(`n${i}`, i % 2 ? [`n${i - 1}`] : []));
  const plan = compileTaskPlan(tasks), cut = affectedTaskPlan(plan, ['n8888']);
  expect(cut.affected).toEqual(['n8888', 'n8889']);
  expect(cut.stats).toEqual({ visited_nodes: 2, visited_edges: 1 });
});
