import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { compareUnicode, jsonFloat, parsedDocument } from './canonical_json.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const json = value => JSON.stringify(value);
const parsed = value => JSON.parse(value);
const LIMIT = 8 * 1024 * 1024;
const ROW_LIMIT = 100000, ROW_BYTES = 32 * 1024 * 1024;

function bounded(value, label, max = 1024) {
  if (typeof value !== 'string' || !value || Buffer.byteLength(value) > max || value.includes('\0')) throw new Error(`invalid ${label}`);
  return value;
}
function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Buffer.byteLength(json(value)) > LIMIT) throw new Error(`invalid ${label}`);
  return value;
}
function rows(db, sql, ...args) {
  const query = db.query(sql), values = [];
  let bytes = 0;
  try {
    for (const value of query.iterate(...args)) {
      bytes += Buffer.byteLength(json(value));
      if (values.length >= ROW_LIMIT || bytes > ROW_BYTES) throw new Error('temporal row budget exceeded');
      values.push(value);
    }
  } finally { query.finalize(); }
  return values;
}
function row(db, sql, ...args) { return db.query(sql).get(...args); }
function run(db, sql, ...args) { return db.query(sql).run(...args); }
function digestParts(...parts) { return sha(parts.map(value => value === null ? 'None' : value).join('\0')); }
function regularJson(path) {
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.size > LIMIT) throw new Error('native manifest is not a bounded regular file');
  return parsed(readFileSync(path, 'utf8'));
}
function probeEvidence(value) {
  return { exit_code: Number.isInteger(value?.exit_code) ? value.exit_code : 127,
    ...(value?.timeout || value?.timed_out ? { timed_out: true } : {}),
    ...(value?.overflow ? { overflow: true } : {}),
    ...(value?.spawn_error ? { spawn_error: true } : {}),
    ...(typeof value?.stderr === 'string' ? { stderr: value.stderr.slice(0, 2000) } : {}) };
}
function pythonBool(value) {
  if (value === null || value === undefined || value === false || value === 0 || value === '') return false;
  if (Array.isArray(value)) return value.length !== 0;
  if (typeof value === 'object') return Object.keys(value).length !== 0;
  return true;
}
function pythonStr(value) {
  if (value === null) return 'None';
  if (value === true) return 'True';
  if (value === false) return 'False';
  return String(value);
}

export function createTemporalEngine(store) {
  if (!store?.db || !store.digest || !store.canonical || !store.writeReceipt) throw new Error('temporal store interface unavailable');
  const db = store.db;
  const digest = value => store.digest(value);
  const now = () => store.now();
  const id = () => store.id();
  const write = (path, value) => store.writeJson(path, value);
  const receipt = (kind, subject_kind, subject_id, state, payload) => store.writeReceipt({ release: 'R5', kind, subject_kind, subject_id, state, payload });

  function registerDependency({ source_kind, source_id, target_kind, target_id, edge_kind = 'depends_on', metadata = {} }) {
    for (const [label, value] of Object.entries({ source_kind, source_id, target_kind, target_id, edge_kind })) bounded(value, label);
    object(metadata, 'dependency metadata');
    run(db, `INSERT INTO dependency_edges(source_kind,source_id,target_kind,target_id,edge_kind,metadata_json,created_at)
      VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_kind,source_id,target_kind,target_id,edge_kind)
      DO UPDATE SET metadata_json=excluded.metadata_json`, source_kind, source_id, target_kind, target_id, edge_kind, json(metadata), now());
    return { source_kind, source_id, target_kind, target_id, edge_kind, metadata };
  }

  function closure({ root_kind, root_id, max_depth = 16 }) {
    bounded(root_kind, 'root kind'); bounded(root_id, 'root id');
    if (!Number.isSafeInteger(max_depth) || max_depth < 0 || max_depth > 64) throw new Error('invalid closure depth');
    const queue = [[root_kind, root_id, 0]], seen = new Set([json([root_kind, root_id])]);
    const nodes = [{ kind: root_kind, id: root_id, depth: 0 }], edges = [];
    for (let index = 0; index < queue.length; index++) {
      const [kind, objectId, depth] = queue[index];
      if (depth >= max_depth) continue;
      for (const record of rows(db, `SELECT * FROM dependency_edges WHERE source_kind=? AND source_id=?
        ORDER BY target_kind,target_id,edge_kind`, kind, objectId)) {
        if (edges.length >= 100000) throw new Error('dependency closure exceeds bound');
        const { metadata_json, ...edge } = record;
        edges.push({ ...edge, metadata: parsed(metadata_json) });
        const target = [edge.target_kind, edge.target_id], key = json(target);
        if (!seen.has(key)) {
          seen.add(key); nodes.push({ kind: target[0], id: target[1], depth: depth + 1 });
          queue.push([...target, depth + 1]);
        }
      }
    }
    const body = { root_kind, root_id, max_depth, nodes, edges };
    const bodyDigest = digest({ ...body, edges: edges.map(edge => ({ ...edge, created_at: jsonFloat(edge.created_at) })) });
    const closure_id = digestParts(root_kind, root_id, bodyDigest);
    run(db, `INSERT OR REPLACE INTO closures(id,root_kind,root_id,max_depth,nodes_json,edges_json,digest,created_at)
      VALUES(?,?,?,?,?,?,?,?)`, closure_id, root_kind, root_id, max_depth, json(nodes), json(edges), bodyDigest, now());
    return { closure_id, digest: bodyDigest, ...body };
  }

  function stateRows() {
    return rows(db, 'SELECT * FROM object_state ORDER BY subject_kind,subject_id').map(({ state_json, ...record }) => ({ ...record, state: parsed(state_json) }));
  }
  function viewDocuments(state) {
    const kind_counts = Object.create(null), lifecycle_counts = Object.create(null), ready_objects = [], invalid_objects = [];
    for (const record of state) {
      if (record.deleted) continue;
      kind_counts[record.subject_kind] = (kind_counts[record.subject_kind] || 0) + 1;
      const value = record.state;
      const lifecycle = pythonStr(Object.hasOwn(value, 'state') ? value.state : Object.hasOwn(value, 'status') ? value.status : 'unknown');
      lifecycle_counts[lifecycle] = (lifecycle_counts[lifecycle] || 0) + 1;
      if (pythonBool(value.ready) || ['ready', 'passed', 'healthy'].includes(lifecycle)) ready_objects.push({ kind: record.subject_kind, id: record.subject_id });
      if (pythonBool(value._invalid)) invalid_objects.push({ kind: record.subject_kind, id: record.subject_id });
    }
    const sorted = value => Object.fromEntries(Object.entries(value).sort(([a], [b]) => compareUnicode(a, b)));
    return { kind_counts: sorted(kind_counts), lifecycle_counts: sorted(lifecycle_counts), ready_objects, invalid_objects };
  }

  function refreshViews({ affected = [] } = {}) {
    const documents = viewDocuments(stateRows()), outputs = {};
    if (!Array.isArray(affected) || affected.length > 10000) throw new Error('invalid affected set');
    const affectedPairs = affected.map(item => {
      if (!Array.isArray(item) || item.length !== 2) throw new Error('invalid affected pair');
      return [bounded(item[0], 'affected kind'), bounded(item[1], 'affected id')];
    }).sort((a, b) => compareUnicode(a[0], b[0]) || compareUnicode(a[1], b[1]));
    for (const [name, state] of Object.entries(documents)) {
      const bodyDigest = digest({ name, state, affected: affectedPairs });
      run(db, `INSERT INTO materialized_views(name,schema_name,state_json,digest,backend,updated_at)
        VALUES(?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET state_json=excluded.state_json,
        digest=excluded.digest,backend=excluded.backend,updated_at=excluded.updated_at`,
      name, 'bonfyre.standing-view.v1', json(state), bodyDigest, 'bonfyre-incremental-r5', now());
      outputs[name] = { state, digest: bodyDigest };
    }
    return { backend: 'bonfyre-incremental-r5', feldera_adapter: store.backendStatus?.().feldera ?? { available: false, binary: null, mode: 'contract-only' }, views: outputs };
  }

  function graphPartition({ graph_kind, root_kind, root_id, max_depth = 16 }) {
    bounded(graph_kind, 'graph kind');
    const found = closure({ root_kind, root_id, max_depth });
    const body = { schema: 'bonfyre.graf-partition.v1', graph_kind, root_kind, root_id,
      nodes: found.nodes, edges: found.edges, closure_digest: found.digest };
    const encodedEdges = found.edges.map(edge => ({ ...edge, created_at: jsonFloat(edge.created_at) }));
    const bodyDigest = digest({ ...body, edges: encodedEdges });
    const partition_id = digestParts(graph_kind, root_kind, root_id, bodyDigest);
    const path = join(store.graphRoot, `${partition_id}.graf.json`);
    write(path, { ...body, edges: encodedEdges, id: partition_id, digest: bodyDigest });
    run(db, `INSERT OR REPLACE INTO graph_partitions(id,graph_kind,root_kind,root_id,nodes_json,edges_json,digest,path,created_at)
      VALUES(?,?,?,?,?,?,?,?,?)`, partition_id, graph_kind, root_kind, root_id, json(found.nodes), json(found.edges), bodyDigest, path, now());
    return { partition_id, path, digest: bodyDigest, ...body };
  }

  async function recordDelta({ subject_kind, subject_id, source_state, target_state, operation }) {
    const oldKeys = Object.keys(source_state), newKeys = Object.keys(target_state), logical_ops = [];
    const oldSet = new Set(oldKeys), newSet = new Set(newKeys);
    for (const key of oldKeys.filter(key => !newSet.has(key)).sort(compareUnicode)) logical_ops.push({ op: 'DELETE', path: key, old: source_state[key] });
    for (const key of newKeys.filter(key => !oldSet.has(key)).sort(compareUnicode)) logical_ops.push({ op: 'INSERT', path: key, new: target_state[key] });
    for (const key of oldKeys.filter(key => newSet.has(key)).sort(compareUnicode)) if (digest(source_state[key]) !== digest(target_state[key]))
      logical_ops.push({ op: 'REPLACE', path: key, old: source_state[key], new: target_state[key] });
    if (!logical_ops.length && operation !== 'UPSERT') logical_ops.push({ op: operation, path: '$' });
    const source_digest = oldKeys.length ? digest(source_state) : null, target_digest = digest(target_state);
    const delta_id = digestParts(subject_kind, subject_id, source_digest, target_digest, operation);
    const native_manifest_path = join(store.deltaRoot, `${delta_id}.native.json`);
    let native_manifest;
    const temp = mkdtempSync(join(store.deltaRoot, '.native-delta-'));
    try {
      const oldPath = join(temp, 'old.json'), newPath = join(temp, 'new.json'), outPath = join(temp, 'out.json');
      const oldBytes = store.canonical(source_state), newBytes = store.canonical(target_state);
      writeFileSync(oldPath, oldBytes); writeFileSync(newPath, newBytes);
      let probe;
      try { probe = await store.run([store.deltaKernel, 'delta', '--old', oldPath, '--new', newPath, '--out', outPath], { timeout: 60 }); }
      catch (error) { probe = { exit_code: 127, spawn_error: true, stderr: String(error?.message || error).slice(0, 2000) }; }
      if (probe?.exit_code === 0 && existsSync(outPath)) {
        try {
          const candidate = regularJson(outPath);
          if (candidate?.schema !== 'bonfyre.native-chunk-delta.v1' ||
              candidate.old_bytes !== Buffer.byteLength(oldBytes) || candidate.new_bytes !== Buffer.byteLength(newBytes) ||
              candidate.source_sha256 !== sha(oldBytes) || candidate.target_sha256 !== sha(newBytes) ||
              !Array.isArray(candidate.operations) || candidate.operations.length > 100000)
            throw new Error('native manifest content mismatch');
          renameSync(outPath, native_manifest_path);
          native_manifest = candidate;
        } catch { native_manifest = { state: 'failed', probe: probeEvidence(probe), reason: 'invalid_native_manifest' }; }
      } else native_manifest = { state: 'failed', probe: probeEvidence(probe) };
    } finally { rmSync(temp, { recursive: true, force: true }); }
    const invalidations = logical_ops.map(op => ({ kind: 'dependent', root_kind: subject_kind, root_id: subject_id, path: op.path }));
    const body = { schema: 'bonfyre.delta.v1', id: delta_id, subject_kind, subject_id, source_digest, target_digest,
      logical_operation: operation, logical_ops,
      physical_candidates: [{ codec: 'CHUNK_MANIFEST', eligible: true }, { codec: 'COPY_RANGE', eligible: true }, { codec: 'RAW', eligible: true }],
      native_manifest, invalidations };
    const bodyDigest = digest(body);
    run(db, `INSERT OR REPLACE INTO deltas(id,source_digest,target_digest,subject_kind,subject_id,logical_ops_json,
      native_manifest_path,invalidations_json,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
    delta_id, source_digest, target_digest, subject_kind, subject_id, json(logical_ops), native_manifest_path, json(invalidations), bodyDigest, now());
    return { ...body, digest: bodyDigest, delta_id, native_manifest_path };
  }

  async function append({ stream, kind, subject_kind, subject_id, operation, payload, dependencies = [] }) {
    for (const [label, value] of Object.entries({ stream, kind, subject_kind, subject_id })) bounded(value, label);
    if (!/^[A-Za-z0-9_.-]+$/.test(stream) || stream === '.' || stream === '..') throw new Error('invalid flux stream path');
    operation = bounded(operation, 'operation').toUpperCase();
    if (!['UPSERT', 'DELETE', 'INVALIDATE', 'RETRACT'].includes(operation)) throw new Error(`unsupported operation: ${operation}`);
    object(payload, 'flux payload');
    if (!Array.isArray(dependencies) || dependencies.length > 1000) throw new Error('invalid flux dependencies');
    for (const dep of dependencies) {
      object(dep, 'dependency'); bounded(dep.target_kind, 'target kind'); bounded(dep.target_id, 'target id');
      bounded(dep.edge_kind ?? 'depends_on', 'edge kind'); object(dep.metadata ?? {}, 'dependency metadata');
    }
    const created_at = now(), eventId = id();
    const streamRow = row(db, 'SELECT * FROM flux_streams WHERE name=?', stream);
    const previous_digest = streamRow?.head_digest ?? null;
    const oldRow = row(db, 'SELECT * FROM object_state WHERE subject_kind=? AND subject_id=?', subject_kind, subject_id);
    const oldState = oldRow ? parsed(oldRow.state_json) : {};
    let newState = oldState, deleted = oldRow?.deleted ?? 0;
    if (operation === 'UPSERT') { newState = { ...oldState, ...payload }; deleted = 0; }
    else if (operation === 'DELETE') deleted = 1;
    else if (operation === 'INVALIDATE') newState = { ...oldState, _invalid: true, _invalidation: payload };
    else newState = { ...oldState, _retracted: true, _retraction: payload };
    object(newState, 'resulting object state');
    const body = { schema: 'bonfyre.flux-event.v1', id: eventId, stream, kind, subject_kind, subject_id,
      operation, payload, dependencies, previous_digest, created_at };
    const bodyDigest = digest({ ...body, created_at: jsonFloat(created_at) });
    const segment_path = join(store.fluxRoot, stream, `${Math.trunc(created_at * 1000)}-${eventId}.flux`);
    write(segment_path, { ...body, created_at: jsonFloat(created_at), digest: bodyDigest });
    const stateDigest = digest({ subject_kind, subject_id, state: newState, deleted });
    db.transaction(() => {
      run(db, `INSERT INTO flux_events(id,stream,kind,subject_kind,subject_id,operation,payload_json,dependencies_json,
        previous_digest,digest,segment_path,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
      eventId, stream, kind, subject_kind, subject_id, operation, json(payload), json(dependencies), previous_digest, bodyDigest, segment_path, created_at);
      run(db, `INSERT INTO flux_streams(name,head_digest,event_count,created_at,updated_at) VALUES(?,?,1,?,?)
        ON CONFLICT(name) DO UPDATE SET head_digest=excluded.head_digest,event_count=flux_streams.event_count+1,
        updated_at=excluded.updated_at`, stream, bodyDigest, created_at, created_at);
      run(db, `INSERT INTO object_state(subject_kind,subject_id,state_json,deleted,last_event_id,last_digest,updated_at)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(subject_kind,subject_id) DO UPDATE SET state_json=excluded.state_json,
        deleted=excluded.deleted,last_event_id=excluded.last_event_id,last_digest=excluded.last_digest,updated_at=excluded.updated_at`,
      subject_kind, subject_id, json(newState), deleted, eventId, stateDigest, created_at);
    }).immediate();
    const delta = await recordDelta({ subject_kind, subject_id, source_state: oldState, target_state: newState, operation });
    for (const dep of dependencies) registerDependency({ source_kind: subject_kind, source_id: subject_id,
      target_kind: String(dep.target_kind), target_id: String(dep.target_id), edge_kind: String(dep.edge_kind ?? 'depends_on'), metadata: dep.metadata ?? {} });
    const affected_closure = closure({ root_kind: subject_kind, root_id: subject_id, max_depth: 16 });
    const views = refreshViews({ affected: [[subject_kind, subject_id]] });
    const graph_partition = graphPartition({ graph_kind: 'AffectedGraph', root_kind: subject_kind, root_id: subject_id, max_depth: 16 });
    const savedReceipt = receipt('flux.append', subject_kind, subject_id, delta.native_manifest.state === 'failed' ? 'failed' : 'passed',
      { event_id: eventId, stream, digest: bodyDigest,
      segment_path, delta_id: delta.delta_id, closure_id: affected_closure.closure_id, graph_partition_id: graph_partition.partition_id });
    return { event: { ...body, digest: bodyDigest, segment_path }, delta, affected_closure, views, graph_partition, receipt: savedReceipt };
  }

  function parity() {
    const state = stateRows();
    const storedViews = Object.create(null), viewFailures = [];
    for (const view of rows(db, 'SELECT name,state_json FROM materialized_views ORDER BY name')) {
      try { storedViews[view.name] = parsed(view.state_json); }
      catch { storedViews[view.name] = null; viewFailures.push({ name: view.name, reason: 'invalid_state_json' }); }
    }
    const expectedNames = Object.keys(viewDocuments(state)).sort(compareUnicode);
    for (const name of expectedNames) if (!Object.hasOwn(storedViews, name)) viewFailures.push({ name, reason: 'missing_view' });
    for (const name of Object.keys(storedViews)) if (!expectedNames.includes(name)) viewFailures.push({ name, reason: 'unexpected_view' });
    const incremental = { object_state: state.map(item => ({ kind: item.subject_kind, id: item.subject_id, state: item.state, deleted: item.deleted })), views: storedViews };
    const events = rows(db, 'SELECT * FROM flux_events ORDER BY seq');
    const rebuilt = new Map(), deleted = new Map();
    for (const event of events) {
      const key = json([event.subject_kind, event.subject_id]), current = rebuilt.get(key) ?? {}, payload = parsed(event.payload_json);
      if (event.operation === 'UPSERT') { rebuilt.set(key, { ...current, ...payload }); deleted.set(key, 0); }
      else if (event.operation === 'DELETE') { if (!rebuilt.has(key)) rebuilt.set(key, current); deleted.set(key, 1); }
      else if (event.operation === 'INVALIDATE') rebuilt.set(key, { ...current, _invalid: true, _invalidation: payload });
      else if (event.operation === 'RETRACT') rebuilt.set(key, { ...current, _retracted: true, _retraction: payload });
    }
    const rebuildRows = [...rebuilt].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, value]) => {
      const [subject_kind, subject_id] = parsed(key); return { subject_kind, subject_id, state: value, deleted: deleted.get(key) ?? 0 };
    });
    const rebuild = { object_state: rebuildRows.map(item => ({ kind: item.subject_kind, id: item.subject_id, state: item.state, deleted: item.deleted })), views: viewDocuments(rebuildRows) };
    const incremental_digest = digest(incremental), rebuild_digest = digest(rebuild), equal = !viewFailures.length && incremental_digest === rebuild_digest;
    const proofId = digestParts(incremental_digest, rebuild_digest);
    const proof = { schema: 'bonfyre.incremental-parity.v1', id: proofId, equal, incremental_digest, rebuild_digest,
      events: events.length, objects: rebuildRows.length, incremental, rebuild,
      ...(viewFailures.length ? { view_failures: viewFailures } : {}) };
    const proof_path = join(store.deltaRoot, `${proofId}.parity.json`); write(proof_path, proof);
    run(db, `INSERT OR REPLACE INTO parity_runs(id,incremental_digest,rebuild_digest,equal,details_json,proof_path,created_at)
      VALUES(?,?,?,?,?,?,?)`, proofId, incremental_digest, rebuild_digest, equal ? 1 : 0,
    json({ events: events.length, objects: rebuildRows.length }), proof_path, now());
    return { ...proof, proof_path, receipt: receipt('incremental.parity', 'parity', proofId, equal ? 'passed' : 'failed', { proof_path, ...proof }) };
  }

  function streamVerify() {
    const streams = rows(db, 'SELECT name FROM flux_streams UNION SELECT stream AS name FROM flux_events ORDER BY name')
      .map(value => value.name), failures = [], heads = Object.create(null);
    for (const stream of streams) {
      let previous = null, count = 0;
      for (const event of rows(db, 'SELECT * FROM flux_events WHERE stream=? ORDER BY seq', stream)) {
        count++;
        const body = { schema: 'bonfyre.flux-event.v1', id: event.id, stream: event.stream, kind: event.kind,
          subject_kind: event.subject_kind, subject_id: event.subject_id, operation: event.operation,
          payload: parsed(event.payload_json), dependencies: parsed(event.dependencies_json),
          previous_digest: event.previous_digest, created_at: event.created_at };
        if (digest({ ...body, created_at: jsonFloat(event.created_at) }) !== event.digest) failures.push({ stream, id: event.id, reason: 'digest' });
        if (event.previous_digest !== previous) failures.push({ stream, id: event.id, reason: 'chain' });
        try {
          const { value, calculated } = parsedDocument(store.readText(event.segment_path));
          const fields = ['schema', 'id', 'stream', 'kind', 'subject_kind', 'subject_id', 'operation', 'previous_digest', 'created_at'];
          if (value.digest !== calculated || value.digest !== event.digest ||
              fields.some(key => value[key] !== body[key]) ||
              store.canonical(value.payload) !== store.canonical(body.payload) ||
              store.canonical(value.dependencies) !== store.canonical(body.dependencies))
            failures.push({ stream, id: event.id, reason: 'segment_mismatch' });
        } catch { failures.push({ stream, id: event.id, reason: 'missing_or_unreadable_segment' }); }
        previous = event.digest;
      }
      const streamRow = row(db, 'SELECT * FROM flux_streams WHERE name=?', stream);
      if (!streamRow) failures.push({ stream, reason: 'missing_stream' });
      else {
        if (streamRow.head_digest !== previous) failures.push({ stream, reason: 'head_mismatch' });
        if (streamRow.event_count !== count) failures.push({ stream, reason: 'count_mismatch' });
      }
      heads[stream] = previous;
    }
    return { verified: failures.length === 0, streams: heads, failures };
  }
  return { append, recordDelta, registerDependency, closure, refreshViews, parity, graphPartition, streamVerify };
}
