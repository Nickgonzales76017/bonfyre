// Pure, content-addressed dependency plans. These identities select work that
// needs reconsideration; they never authorize effects or attest cached results.
import { canonicalJson, digestValue } from './canonical_json.mjs';

const MAX_TASKS = 10000, MAX_EDGES = 100000, MAX_BYTES = 16 * 1024 * 1024;
const RUNTIME_FIELDS = new Set(['state', 'evidence', 'attempts', 'error', 'receipt_id', 'created_at', 'updated_at']);
const indexes = new WeakMap();
// Both authoring and representation owners bind these mission-wide semantics.
// Keep wall-clock IDs and mutable execution evidence out of reusable input keys.
const MISSION_NON_INPUT_FIELDS = new Set(['id', 'tasks', 'execution_plan', 'created_at', 'updated_at', 'bugs', 'state']);
export const missionPlanContext = mission => Object.fromEntries(Object.entries(mission)
  .filter(([key]) => !MISSION_NON_INPUT_FIELDS.has(key)));
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$/.test(value);
function freeze(value) {
  if (value && typeof value === 'object') { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}
function indexOf(plan) {
  const index = indexes.get(plan);
  if (!index) throw new Error('plan must be compiled locally, not an unverified serialized descriptor');
  return index;
}

export function compileTaskPlan(tasks, { context = {} } = {}) {
  if (!Array.isArray(tasks) || tasks.length > MAX_TASKS) throw new Error('task count budget exceeded');
  // Canonicalization rejects cycles/non-JSON/deep values. Snapshot definitions so
  // caller mutation cannot change a compiled plan after identity was calculated.
  let bytes = 0, edges = 0;
  const definitions = new Map();
  for (const source of tasks) {
    const encoded = canonicalJson(source);
    bytes += Buffer.byteLength(encoded);
    if (bytes > MAX_BYTES) throw new Error('task definition byte budget exceeded');
    const task = JSON.parse(encoded);
    if (!task || typeof task !== 'object' || Array.isArray(task) || !identifier(task.id)) throw new Error('invalid task identity');
    if (definitions.has(task.id)) throw new Error(`duplicate task: ${task.id}`);
    const dependencies = task.dependencies ?? [];
    if (!Array.isArray(dependencies) || dependencies.some(id => !identifier(id)) || new Set(dependencies).size !== dependencies.length)
      throw new Error(`invalid dependencies: ${task.id}`);
    edges += dependencies.length;
    if (edges > MAX_EDGES) throw new Error('task edge budget exceeded');
    task.dependencies = [...dependencies].sort();
    definitions.set(task.id, task);
  }
  const ids = [...definitions.keys()].sort(), children = new Map(ids.map(id => [id, []]));
  const indegrees = new Map();
  for (const id of ids) {
    const deps = definitions.get(id).dependencies;
    indegrees.set(id, deps.length);
    for (const parent of deps) {
      if (!children.has(parent)) throw new Error(`missing dependency ${parent} for ${id}`);
      children.get(parent).push(id);
    }
  }
  const contextDigest = digestValue(context), nodes = new Map(), waves = [];
  let frontier = ids.filter(id => indegrees.get(id) === 0), visited = 0;
  while (frontier.length) {
    waves.push(frontier);
    const next = [];
    for (const id of frontier) {
      const task = definitions.get(id);
      const semantics = Object.fromEntries(Object.entries(task).filter(([key]) => !RUNTIME_FIELDS.has(key)));
      const definitionDigest = digestValue(semantics);
      const inputDigest = digestValue({ schema: 'bonfyre.task-input.v1', context_digest: contextDigest,
        definition_digest: definitionDigest, dependencies: task.dependencies.map(parent => [parent, nodes.get(parent).input_digest]) });
      nodes.set(id, { id, dependencies: task.dependencies, definition_digest: definitionDigest, input_digest: inputDigest });
      visited++;
      for (const child of children.get(id)) {
        const count = indegrees.get(child) - 1;
        indegrees.set(child, count);
        if (count === 0) next.push(child);
      }
    }
    frontier = next.sort();
  }
  if (visited !== tasks.length) throw new Error('task dependency cycle');
  const body = { schema: 'bonfyre.task-plan.v1', context_digest: contextDigest, nodes: ids.map(id => nodes.get(id)), waves,
    stats: { tasks: tasks.length, edges, waves: waves.length, maximum_wave_width: Math.max(0, ...waves.map(wave => wave.length)) },
    authority: 'candidate-plan-only' };
  const plan = freeze({ ...body, digest: digestValue(body) });
  indexes.set(plan, { nodes, children });
  return plan;
}

// Traversal visits only the changed cut and its outgoing dependency edges, not
// every task once per change. Unknown roots refuse rather than silently omit work.
export function affectedTaskPlan(plan, changedIds) {
  const { nodes, children } = indexOf(plan);
  if (!Array.isArray(changedIds) || changedIds.length > MAX_TASKS) throw new Error('invalid changed task set');
  const queue = [...new Set(changedIds)], seen = new Set(queue);
  for (const id of queue) if (!nodes.has(id)) throw new Error(`unknown changed task: ${id}`);
  let visitedEdges = 0;
  for (let cursor = 0; cursor < queue.length; cursor++) {
    for (const child of children.get(queue[cursor])) {
      visitedEdges++;
      if (!seen.has(child)) { seen.add(child); queue.push(child); }
    }
  }
  return { plan_digest: plan.digest, affected: queue.sort(), stats: { visited_nodes: queue.length, visited_edges: visitedEdges },
    authority: 'candidate-invalidation-only' };
}

export function diffTaskPlans(before, after) {
  const oldIndex = indexOf(before), newIndex = indexOf(after);
  const added = [], removed = [], changed = [], affected = [], reusable = [];
  for (const [id, node] of newIndex.nodes) {
    const previous = oldIndex.nodes.get(id);
    if (!previous) added.push(id);
    else if (node.definition_digest !== previous.definition_digest) changed.push(id);
    if (!previous || node.input_digest !== previous.input_digest) affected.push(id);
    else reusable.push(id);
  }
  for (const id of oldIndex.nodes.keys()) if (!newIndex.nodes.has(id)) removed.push(id);
  return { before_digest: before.digest, after_digest: after.digest, added: added.sort(), removed: removed.sort(), changed: changed.sort(),
    affected: affected.sort(), reusable_candidates: reusable.sort(), context_changed: before.context_digest !== after.context_digest,
    authority: 'candidate-invalidation-only',
    reuse_requires: ['verified-output-identity', 'current-authority', 'current-input-residency', 'effect-specific-replay-policy'] };
}
