import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { constants, closeSync, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, mkdtempSync,
  openSync, readFileSync, readSync, realpathSync, renameSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { homedir } from 'node:os';
import { join, relative, resolve, sep } from 'node:path';
import { canonicalJson, digestValue, parsedDocument, compareUnicode, jsonFloat } from './canonical_json.mjs';
import { projectRun } from './project_runtime.mjs';
import { compileTaskPlan, missionPlanContext } from './mission_plan.mjs';

export const VERSION = '5.1.0-recursive-r1';
const TOKENIZERS = Object.freeze(['byte-utf8-v1', 'unicode-scalar-v1', 'lexeme-v1']);
const IR_TYPES = Object.freeze(['ArtifactIR', 'DeltaIR', 'EquivalenceIR', 'DistributedIR', 'ExecutionPhenotypeIR', 'AuthorityIR', 'ProofIR']);
const TABLES = Object.freeze(['artifacts', 'token_artifacts', 'token_chunks', 'flux_events', 'deltas', 'dependency_edges',
  'affected_closures', 'ir_objects', 'lambda_motifs', 'provider_token_bids', 'graph_projections', 'runtime_images', 'bfp_successor_plans', 'receipts']);
const MAX_BYTES = 64 * 1024 * 1024, MAX_ROWS = 100000;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const uuid = () => randomUUID().replaceAll('-', '');
const clone = value => JSON.parse(canonicalJson(value));
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function str(value, name, nullable = false) {
  if (nullable && value === null) return value;
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > 16384) throw new Error(`invalid ${name}`);
  return value;
}
function num(value, min = 0, max = Number.MAX_SAFE_INTEGER, integer = false) {
  if (!Number.isFinite(value) || value < min || value > max || (integer && !Number.isSafeInteger(value))) throw new Error('number outside bounded contract');
  return value;
}
function list(value, name) {
  if (!Array.isArray(value) || value.length > 10000) throw new Error(`invalid ${name}`);
  return clone(value);
}
function map(value, name) { if (!object(value)) throw new Error(`invalid ${name}`); return clone(value); }
function location(path) {
  const tail = []; let parent = resolve(path);
  while (!existsSync(parent)) { tail.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); }
  if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink store root refused');
  return join(realpathSync(parent), ...tail);
}
function safeOwned(root, path) {
  const full = resolve(path), rel = relative(root, full);
  if (rel === '..' || rel.startsWith(`..${sep}`)) throw new Error('owned path escapes store');
  let current = root;
  for (const part of rel.split(sep).filter(Boolean)) {
    current = join(current, part);
    if (existsSync(current) && lstatSync(current).isSymbolicLink()) throw new Error('owned path is a symlink');
  }
  return full;
}
function readBounded(path, maximum = MAX_BYTES) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > BigInt(maximum)) throw new Error('input is not a bounded regular file');
    const bytes = Buffer.alloc(Number(before.size) + 1); let used = 0;
    while (used < bytes.length) { const n = readSync(fd, bytes, used, bytes.length - used, null); if (!n) break; used += n; }
    const after = fstatSync(fd, { bigint: true }), named = lstatSync(path, { bigint: true });
    const identity = s => [s.dev, s.ino, s.size, s.mtimeNs, s.ctimeNs].join(':');
    if (used !== Number(before.size) || identity(before) !== identity(after) || identity(after) !== identity(named) || !named.isFile()) throw new Error('input changed while reading');
    return bytes.subarray(0, used);
  } finally { closeSync(fd); }
}
const readJson = path => JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(readBounded(path, 8 * 1024 * 1024)));

export function createRecursiveStore({ home, tokenBinary, now = () => Date.now() / 1000, run = projectRun } = {}) {
  str(home, 'home'); str(tokenBinary, 'token binary');
  if (typeof now !== 'function' || typeof run !== 'function') throw new Error('invalid store dependencies');
  home = location(home); tokenBinary = resolve(tokenBinary);
  const dbPath = join(home, 'substrate.sqlite3'), artifactRoot = join(home, 'artifacts'), tokenRoot = join(home, 'tokens');
  const deltaRoot = join(home, 'deltas'), irRoot = join(home, 'ir'), receiptRoot = join(home, 'receipts'), runtimeRoot = join(home, 'runtime-images');
  let connection, closed = false;
  const clock = () => num(now(), 0, 8640000000000);
  function db() {
    if (closed) throw new Error('recursive store is closed');
    if (!connection) {
      for (const path of [home, artifactRoot, tokenRoot, deltaRoot, irRoot, receiptRoot, runtimeRoot]) {
        safeOwned(home, path); mkdirSync(path, { recursive: true, mode: 0o700 });
      }
      safeOwned(home, dbPath);
      const opened = new Database(dbPath, { create: true, strict: true });
      try {
        opened.exec('PRAGMA busy_timeout=30000;');
        opened.exec(readFileSync(new URL('./recursive_schema.sql', import.meta.url), 'utf8'));
        connection = opened; bootstrap();
      } catch (error) { connection = null; opened.close(); throw error; }
    }
    return connection;
  }
  const get = (sql, ...args) => db().query(sql).get(...args);
  const write = (sql, ...args) => db().query(sql).run(...args);
  const atomic = fn => db().transaction(fn).immediate();
  function insert(table, record, replace = true) {
    if (!TABLES.includes(table)) throw new Error('invalid internal table');
    const keys = Object.keys(record);
    if (keys.some(key => !/^[a-z_]+$/.test(key))) throw new Error('invalid internal column');
    return write(`INSERT ${replace ? 'OR REPLACE ' : ''}INTO ${table}(${keys.join(',')}) VALUES(${keys.map(() => '?').join(',')})`, ...keys.map(key => record[key]));
  }
  function output(path, bytes) {
    db(); safeOwned(home, path); mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const temp = `${path}.${uuid()}.tmp`; let fd;
    try {
      fd = openSync(temp, 'wx', 0o600); writeFileSync(fd, bytes); fsyncSync(fd); closeSync(fd); fd = undefined; renameSync(temp, path);
    } finally { if (fd !== undefined) closeSync(fd); if (existsSync(temp)) unlinkSync(temp); }
  }
  function bootstrap() {
    return atomic(() => {
      const at = clock();
      for (const tokenizer of TOKENIZERS) write(`INSERT INTO tokenizer_descriptors VALUES(?,'1','bonfyre-token-fabric-rust',1,0,?,?,?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at`, tokenizer,
      canonicalJson({ authority: 'native-rust', scope: 'canonical-fallback', warning: 'Not interchangeable with a model vendor tokenizer.' }), at, at);
      write("INSERT INTO metadata VALUES('schema_version',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", VERSION, at);
    });
  }
  function writeReceipt({ kind, subject_kind = null, subject_id = null, state, payload }) {
    str(kind, 'kind'); str(subject_kind, 'subject kind', true); str(subject_id, 'subject id', true); str(state, 'state'); payload = clone(payload);
    return atomic(() => {
      const previous = get('SELECT digest FROM receipts ORDER BY rowid DESC LIMIT 1')?.digest ?? null;
      const id = uuid(), at = clock();
      const body = { id, kind, subject_kind, subject_id, state, payload, previous_digest: previous, created_at: at };
      const digest = digestValue(body), path = join(receiptRoot, `${Math.floor(at)}-${id}.json`);
      output(path, canonicalJson({ ...body, digest }));
      insert('receipts', { id, kind, subject_kind, subject_id, state, payload_json: canonicalJson(payload), previous_digest: previous, digest, path, created_at: at }, false);
      return { receipt_id: id, digest, previous_digest: previous, path, state };
    });
  }
  function verifyReceipts() {
    let previous = null, count = 0, complete = true; const failures = [];
    for (const row of db().query('SELECT rowid AS receipt_seq,* FROM receipts ORDER BY rowid LIMIT 100001').iterate()) {
      if (++count > MAX_ROWS) { count--; complete = false; break; }
      const fail = reason => { if (failures.length < 1000) failures.push({ receipt_id: row.id, reason }); };
      try {
        const path = safeOwned(receiptRoot, row.path);
        if (!existsSync(path)) fail('missing_file');
        else {
          const { value, calculated } = parsedDocument(new TextDecoder('utf-8', { fatal: true }).decode(readBounded(path, 8 * 1024 * 1024)));
          if (value.digest !== calculated || row.digest !== calculated) fail('digest_mismatch');
          if (['id', 'kind', 'subject_kind', 'subject_id', 'state', 'previous_digest', 'created_at'].some(key => value[key] !== row[key]) ||
              canonicalJson(value.payload) !== canonicalJson(JSON.parse(row.payload_json))) fail('row_document_mismatch');
        }
      } catch { fail('unreadable_document'); }
      if (row.previous_digest !== previous) fail('chain_mismatch'); previous = row.digest;
    }
    return { verified: complete && !failures.length, receipts: count, head: previous, failures,
      ...(complete ? {} : { incomplete: true, reason: 'verification_budget' }) };
  }
  function appendFlux({ stream, kind, subject_kind = null, subject_id = null, payload }) {
    str(stream, 'stream'); str(kind, 'kind'); str(subject_kind, 'subject kind', true); str(subject_id, 'subject id', true); payload = map(payload, 'payload');
    return atomic(() => {
      const body = { id: uuid(), stream, kind, subject_kind, subject_id, payload,
        previous_digest: get('SELECT digest FROM flux_events WHERE stream=? ORDER BY seq DESC LIMIT 1', stream)?.digest ?? null, created_at: clock() };
      const digest = digestValue({ ...body, created_at: jsonFloat(body.created_at) });
      const result = insert('flux_events', { id: body.id, stream, kind, subject_kind, subject_id, payload_json: canonicalJson(payload),
        previous_digest: body.previous_digest, digest, created_at: body.created_at }, false);
      return { ...body, seq: Number(result.lastInsertRowid), digest };
    });
  }
  function verifyFlux() {
    const streams = Object.create(null), failures = []; let visited = 0, complete = true;
    for (const row of db().query('SELECT * FROM flux_events ORDER BY stream,seq LIMIT 100001').iterate()) {
      if (++visited > MAX_ROWS) { complete = false; break; }
      const fail = reason => { if (failures.length < 1000) failures.push({ stream: row.stream, seq: row.seq, reason }); };
      try {
        // REAL timestamps lose the Python int/float lexical distinction. Preserve payload
        // number lexemes, and accept its historical float timestamp form as well.
        const parts = Object.fromEntries(['id', 'stream', 'kind', 'subject_kind', 'subject_id', 'previous_digest', 'created_at']
          .map(key => [key, canonicalJson(row[key])]));
        parts.payload = row.payload_json;
        const check = () => parsedDocument(`{${Object.entries(parts).map(([key, value]) => `${JSON.stringify(key)}:${value}`).join(',')}}`).calculated;
        let calculated = check();
        if (calculated !== row.digest && Number.isSafeInteger(row.created_at)) { parts.created_at = `${row.created_at}.0`; calculated = check(); }
        if (calculated !== row.digest) fail('digest_mismatch');
      } catch { fail('invalid_payload'); }
      if (row.previous_digest !== (streams[row.stream] ?? null)) fail('chain_mismatch');
      streams[row.stream] = row.digest;
    }
    return { verified: complete && !failures.length, streams, failures, ...(complete ? {} : { incomplete: true, reason: 'verification_budget' }) };
  }
  const fluxHeads = () => verifyFlux().streams;
  function ingestArtifact({ path = null, text = null, kind = 'text', classification = 'internal', schema_name = null, provenance = {} }) {
    if ((path === null) === (text === null)) throw new Error('provide exactly one of path or text');
    str(kind, 'kind'); str(classification, 'classification'); str(schema_name, 'schema name', true); provenance = map(provenance, 'provenance');
    let sourcePath = null, raw;
    if (path !== null) { str(path, 'source path'); sourcePath = realpathSync(path.startsWith('~/') ? join(homedir(), path.slice(2)) : resolve(path)); raw = readBounded(sourcePath); }
    else { if (typeof text !== 'string' || Buffer.byteLength(text) > MAX_BYTES) throw new Error('invalid artifact text'); raw = Buffer.from(text); }
    const sourceDigest = hash(raw), start = raw.subarray(0, 3).equals(Buffer.from([239, 187, 191])) ? 3 : 0;
    // Byte-level canonicalization preserves non-UTF8 bodies rather than silently decoding/replacing them.
    const buffer = Buffer.alloc(raw.length - start); let length = 0;
    for (let index = start; index < raw.length; index++) {
      if (raw[index] === 13) { buffer[length++] = 10; if (raw[index + 1] === 10) index++; }
      else buffer[length++] = raw[index];
    }
    const canonical = buffer.subarray(0, length), canonicalDigest = hash(canonical);
    const id = hash(`${kind}\0${canonicalDigest}\0${schema_name || ''}`), canonicalPath = join(artifactRoot, `${id}.canonical`);
    return atomic(() => {
      if (existsSync(canonicalPath)) { if (hash(readBounded(safeOwned(artifactRoot, canonicalPath))) !== canonicalDigest) throw new Error('canonical body corrupted'); }
      else output(canonicalPath, canonical);
      write(`INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
        source_path=COALESCE(excluded.source_path,artifacts.source_path),provenance_json=excluded.provenance_json`,
      id, kind, sourcePath, sourceDigest, canonicalDigest, canonicalPath, canonical.length, classification, schema_name, canonicalJson(provenance), clock());
      const event = appendFlux({ stream: 'artifact', kind: 'artifact.canonicalized', subject_kind: 'artifact', subject_id: id,
        payload: { kind, source_digest: sourceDigest, canonical_digest: canonicalDigest, bytes: canonical.length } });
      const receipt = writeReceipt({ kind: 'artifact.ingest', subject_kind: 'artifact', subject_id: id, state: 'passed', payload: { artifact_id: id, event } });
      return { artifact_id: id, kind, source_path: sourcePath, source_digest: sourceDigest, canonical_digest: canonicalDigest,
        canonical_path: canonicalPath, byte_count: canonical.length, flux_event: event, receipt };
    });
  }
  function artifact(id) {
    str(id, 'artifact id'); const row = get('SELECT * FROM artifacts WHERE id=?', id);
    if (!row) throw new Error('artifact not found'); const { provenance_json, ...value } = row;
    return { ...value, provenance: JSON.parse(provenance_json) };
  }
  function tokenArtifact(id) {
    str(id, 'token artifact id'); const row = get('SELECT * FROM token_artifacts WHERE id=?', id);
    if (!row) throw new Error('token artifact not found');
    const chunks = db().query('SELECT * FROM token_chunks WHERE token_artifact_id=? ORDER BY chunk_index LIMIT 10001').all(id);
    if (chunks.length > 10000) throw new Error('token chunk budget exceeded');
    return { ...row, chunks };
  }
  async function native(args, timeoutMs = 1800000) {
    // No interpreter fallback: the explicitly configured owner must be a native executable.
    const fd = openSync(tokenBinary, constants.O_RDONLY | constants.O_NOFOLLOW); const magic = Buffer.alloc(4);
    try { readSync(fd, magic, 0, 4, 0); } finally { closeSync(fd); }
    if (!['7f454c46', 'feedface', 'feedfacf', 'cefaedfe', 'cffaedfe', 'cafebabe', 'bebafeca'].includes(magic.toString('hex')))
      throw new Error('token owner is not a native executable');
    const result = await run([tokenBinary, ...args], { timeoutMs, maxOutputBytes: 100000 });
    if (!result || result.exit_code !== 0 || result.timeout || result.overflow) {
      const error = new Error('native token command failed or exceeded bounds'); error.receipt = result; throw error;
    }
    return result;
  }
  function verifyTokenFiles(value) {
    const bytes = readBounded(safeOwned(tokenRoot, value.canonical_path));
    if (hash(bytes) !== value.canonical_sha256 || bytes.length !== value.canonical_bytes) throw new Error('token canonical body mismatch');
    const spans = new TextDecoder('utf-8', { fatal: true }).decode(readBounded(safeOwned(tokenRoot, value.spans_path)));
    let count = 0, lastEnd = 0; const starts = [], ends = [];
    for (const line of spans.split('\n')) {
      if (!line) continue; const span = JSON.parse(line);
      if (span.index !== count || !Number.isSafeInteger(span.byte_start) || !Number.isSafeInteger(span.byte_end) ||
          span.byte_start < lastEnd || span.byte_end <= span.byte_start || span.byte_end > bytes.length ||
          span.id !== hash(bytes.subarray(span.byte_start, span.byte_end)).slice(0, 16)) throw new Error('invalid token span');
      starts.push(span.byte_start); ends.push(span.byte_end);
      lastEnd = span.byte_end; if (++count > 1000000) throw new Error('token span budget exceeded');
    }
    if (count !== value.token_count) throw new Error('token count mismatch');
    let byteEnd = 0, tokenStart = 0, tokenEnd = 0;
    for (const [index, chunk] of value.chunks.entries()) {
      // Native chunks are byte-addressed: a split lexeme legitimately overlaps two chunks.
      while (tokenStart < count && ends[tokenStart] <= chunk.byte_start) tokenStart++;
      while (tokenEnd < count && starts[tokenEnd] < chunk.byte_end) tokenEnd++;
      if (chunk.index !== index || chunk.byte_start !== byteEnd || chunk.token_start !== tokenStart || chunk.token_end !== tokenEnd ||
          !Number.isSafeInteger(chunk.byte_end) || !Number.isSafeInteger(chunk.token_end) || chunk.byte_end < byteEnd ||
          chunk.byte_end > bytes.length || chunk.token_end < tokenEnd || chunk.token_end > count ||
          chunk.digest !== hash(bytes.subarray(chunk.byte_start, chunk.byte_end))) throw new Error('invalid token chunk');
      byteEnd = chunk.byte_end;
    }
    if (byteEnd !== bytes.length || tokenEnd !== count) throw new Error('incomplete token chunk coverage');
  }
  async function compileTokenArtifact(id, tokenizer_id = 'lexeme-v1') {
    const input = artifact(id);
    if (!TOKENIZERS.includes(tokenizer_id)) throw new Error('tokenizer not found');
    const cached = () => get(`SELECT t.id FROM token_artifacts t WHERE t.tokenizer_id=? AND t.tokenizer_version='1' AND
      (t.artifact_id=? OR EXISTS(SELECT 1 FROM dependency_edges e WHERE e.source_kind='artifact' AND e.source_id=?
      AND e.target_kind='token_artifact' AND e.target_id=t.id AND e.edge_kind='tokenized_as')) LIMIT 1`, tokenizer_id, id, id);
    const reuse = tokenId => {
      const row = tokenArtifact(tokenId), manifestBytes = readBounded(safeOwned(tokenRoot, row.manifest_path));
      if (hash(manifestBytes) !== row.manifest_digest) throw new Error('cached token manifest changed');
      const manifest = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(manifestBytes));
      if (manifest.artifact_id !== tokenId || manifest.tokenizer?.id !== tokenizer_id || manifest.tokenizer?.version !== '1' ||
          manifest.canonical_sha256 !== input.canonical_digest || manifest.token_count !== row.token_count ||
          manifest.canonical_path !== row.canonical_path || manifest.spans_path !== row.spans_path) throw new Error('cached token identity mismatch');
      verifyTokenFiles(manifest);
      return { ...row, source_artifact_id: id };
    };
    const existing = cached();
    if (existing) return reuse(existing.id);
    const original = readBounded(safeOwned(artifactRoot, input.canonical_path));
    if (hash(original) !== input.canonical_digest) throw new Error('artifact body changed');
    const stage = mkdtempSync(join(tokenRoot, '.compile-')), manifestPath = join(stage, 'manifest.json');
    let retained = false;
    try {
      await native(['tokenize', '--input', input.canonical_path, '--tokenizer', tokenizer_id, '--out', manifestPath]);
      const manifest = readJson(manifestPath);
      if (manifest.schema !== 'bonfyre.token-artifact.v1' || !/^[a-f0-9]{64}$/.test(manifest.artifact_id) ||
          manifest.tokenizer?.id !== tokenizer_id || manifest.tokenizer?.version !== '1' || manifest.tokenizer?.exact !== true ||
          manifest.canonical_sha256 !== input.canonical_digest || manifest.canonical_bytes !== original.length ||
          manifest.canonical_path !== `${manifestPath}.canonical` || manifest.spans_path !== `${manifestPath}.spans.jsonl` ||
          !Array.isArray(manifest.chunks) || manifest.chunks.length > 10000 || manifest.chunk_count !== manifest.chunks.length)
        throw new Error('invalid native token manifest');
      num(manifest.token_count, 0, 1000000, true); verifyTokenFiles(manifest);
      if (hash(readBounded(input.canonical_path)) !== input.canonical_digest) throw new Error('artifact changed during compilation');
      return atomic(() => {
        const winner = cached();
        if (winner) return reuse(winner.id);
        if (get('SELECT id FROM token_artifacts WHERE id=?', manifest.artifact_id)) {
          const shared = reuse(manifest.artifact_id);
          registerDependency({ source_kind: 'artifact', source_id: id, target_kind: 'token_artifact', target_id: manifest.artifact_id, edge_kind: 'tokenized_as' });
          return shared;
        }
        const digest = hash(readBounded(manifestPath)), prefix = hash(`${manifest.canonical_sha256}\0${tokenizer_id}\0${manifest.token_count}`);
        insert('token_artifacts', { id: manifest.artifact_id, artifact_id: id, tokenizer_id, tokenizer_version: '1',
          token_count: manifest.token_count, chunk_count: manifest.chunk_count, manifest_path: manifestPath,
          spans_path: manifest.spans_path, canonical_path: manifest.canonical_path, manifest_digest: digest, prefix_identity: prefix, created_at: clock() }, false);
        for (const chunk of manifest.chunks) insert('token_chunks', { token_artifact_id: manifest.artifact_id, chunk_index: chunk.index,
          byte_start: chunk.byte_start, byte_end: chunk.byte_end, token_start: chunk.token_start, token_end: chunk.token_end, digest: chunk.digest }, false);
        registerDependency({ source_kind: 'artifact', source_id: id, target_kind: 'token_artifact', target_id: manifest.artifact_id, edge_kind: 'tokenized_as' });
        const event = appendFlux({ stream: 'token', kind: 'token.artifact.compiled', subject_kind: 'token_artifact', subject_id: manifest.artifact_id,
          payload: { artifact_id: id, tokenizer: tokenizer_id, token_count: manifest.token_count, chunk_count: manifest.chunk_count, prefix_identity: prefix } });
        const receipt = writeReceipt({ kind: 'token.artifact.compile', subject_kind: 'token_artifact', subject_id: manifest.artifact_id, state: 'passed', payload: { manifest, event } });
        retained = true; return { ...manifest, manifest_digest: digest, prefix_identity: prefix, receipt };
      });
    } finally { if (!retained) rmSync(stage, { recursive: true, force: true }); }
  }
  function recordDelta({ kind, source_id = null, target_id = null, logical_ops, physical_alternatives = [], invalidations = [], manifest_path = null }) {
    str(kind, 'delta kind'); str(source_id, 'source id', true); str(target_id, 'target id', true); str(manifest_path, 'manifest path', true);
    const body = { kind, source_id, target_id, logical_ops: list(logical_ops, 'logical ops'),
      physical_alternatives: list(physical_alternatives, 'physical alternatives'), invalidations: list(invalidations, 'invalidations'), manifest_path };
    const digest = digestValue(body), id = hash(`${kind}\0${digest}`);
    return atomic(() => {
      insert('deltas', { id, kind, source_id, target_id, logical_ops_json: canonicalJson(body.logical_ops),
        physical_alternatives_json: canonicalJson(body.physical_alternatives), invalidations_json: canonicalJson(body.invalidations), manifest_path, digest, created_at: clock() });
      const event = appendFlux({ stream: 'delta', kind: 'delta.recorded', subject_kind: 'delta', subject_id: id, payload: { kind, source_id, target_id } });
      const receipt = writeReceipt({ kind: 'delta.record', subject_kind: 'delta', subject_id: id, state: 'passed', payload: { body, event } });
      return { delta_id: id, digest, event, receipt };
    });
  }
  async function deriveTokenDelta(sourceId, targetId) {
    const source = tokenArtifact(sourceId), target = tokenArtifact(targetId);
    if (source.tokenizer_id !== target.tokenizer_id || source.tokenizer_version !== target.tokenizer_version) throw new Error('tokenizer mismatch');
    for (const item of [source, target]) {
      if (hash(readBounded(safeOwned(tokenRoot, item.manifest_path))) !== item.manifest_digest) throw new Error('token manifest changed');
      verifyTokenFiles(readJson(item.manifest_path));
    }
    const outputPath = join(deltaRoot, `${uuid()}.token-delta.json`);
    await native(['delta', '--old', source.manifest_path, '--new', target.manifest_path, '--out', outputPath]);
    const manifest = readJson(safeOwned(deltaRoot, outputPath));
    const operations = list(manifest.operations, 'native delta operations');
    if (manifest.schema !== 'bonfyre.token-delta.v1' || manifest.source_artifact_id !== sourceId || manifest.target_artifact_id !== targetId ||
        manifest.old_chunks !== source.chunks.length || manifest.new_chunks !== target.chunks.length) throw new Error('native delta identity mismatch');
    if (operations.some(op => !object(op) || !['COPY', 'INSERT', 'DELETE', 'REPLACE'].includes(op.op))) throw new Error('invalid native delta operation');
    const seenOld = new Set(), seenNew = new Set(); let copies = 0;
    for (const op of operations) {
      const old = op.op !== 'INSERT', next = op.op !== 'DELETE';
      if (old) {
        num(op.old_index, 0, source.chunks.length - 1, true);
        if (seenOld.has(op.old_index) || source.chunks[op.old_index].digest !== (op.op === 'COPY' ? op.digest : op.old_digest)) throw new Error('native delta source chunk mismatch');
        seenOld.add(op.old_index);
      }
      if (next) {
        num(op.new_index, 0, target.chunks.length - 1, true);
        if (seenNew.has(op.new_index) || target.chunks[op.new_index].digest !== (op.op === 'COPY' ? op.digest : op.new_digest)) throw new Error('native delta target chunk mismatch');
        seenNew.add(op.new_index);
      }
      if (op.op === 'COPY') copies++;
    }
    if (seenOld.size !== source.chunks.length || seenNew.size !== target.chunks.length || manifest.unchanged_chunks !== copies ||
        manifest.changed_chunks !== operations.length - copies) throw new Error('native delta coverage mismatch');
    const invalidations = operations.filter(op => op.op !== 'COPY').map(op => ({ kind: 'token_chunk', target_artifact_id: targetId, operation: op.op, new_index: op.new_index ?? null }));
    const delta = recordDelta({ kind: 'token', source_id: sourceId, target_id: targetId, logical_ops: operations,
      physical_alternatives: ['CHUNK_MANIFEST', 'COPY_RANGE', 'RAW'].map(encoding => ({ encoding, eligible: true })), invalidations, manifest_path: outputPath });
    return { ...manifest, delta_record: delta };
  }
  function registerDependency({ source_kind, source_id, target_kind, target_id, edge_kind, metadata = {} }) {
    for (const value of [source_kind, source_id, target_kind, target_id, edge_kind]) str(value, 'dependency identity'); metadata = map(metadata, 'metadata');
    write(`INSERT INTO dependency_edges VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_kind,source_id,target_kind,target_id,edge_kind)
      DO UPDATE SET metadata_json=excluded.metadata_json`, source_kind, source_id, target_kind, target_id, edge_kind, canonicalJson(metadata), clock());
    return { source_kind, source_id, target_kind, target_id, edge_kind, metadata };
  }
  function affectedClosure({ root_kind, root_id, max_depth = 12 }) {
    str(root_kind, 'root kind'); str(root_id, 'root id'); num(max_depth, 0, 128, true);
    return atomic(() => {
      const nodes = [{ kind: root_kind, id: root_id, depth: 0 }], visited = new Set([canonicalJson([root_kind, root_id])]), edges = [];
      for (let index = 0; index < nodes.length; index++) {
        const node = nodes[index]; if (node.depth >= max_depth) continue;
        const rows = db().query('SELECT * FROM dependency_edges WHERE source_kind=? AND source_id=? ORDER BY target_kind,target_id,edge_kind LIMIT 10001').all(node.kind, node.id);
        if (rows.length + edges.length > 10000) throw new Error('dependency edge budget exceeded');
        for (const { metadata_json, ...row } of rows) {
          edges.push({ ...row, metadata: JSON.parse(metadata_json) }); const key = canonicalJson([row.target_kind, row.target_id]);
          if (!visited.has(key)) { if (nodes.length >= 10000) throw new Error('dependency node budget exceeded');
            visited.add(key); nodes.push({ kind: row.target_kind, id: row.target_id, depth: node.depth + 1 }); }
        }
      }
      const body = { root_kind, root_id, max_depth, nodes, edges }, digest = digestValue(body), id = hash(`${root_kind}\0${root_id}\0${digest}`);
      insert('affected_closures', { id, root_kind, root_id, max_depth, nodes_json: canonicalJson(nodes), edges_json: canonicalJson(edges), digest, created_at: clock() });
      return { closure_id: id, digest, ...body };
    });
  }
  function compileIr({ ir_type, subject_kind, subject_id, payload, dependencies = [], schema_version = '1' }) {
    if (!IR_TYPES.includes(ir_type)) throw new Error('unsupported IR');
    str(subject_kind, 'subject kind'); str(subject_id, 'subject id'); str(schema_version, 'schema version');
    const body = { ir_type, subject_kind, subject_id, schema_version, payload: map(payload, 'payload'), dependencies: list(dependencies, 'dependencies') };
    const digest = digestValue(body), id = hash(`${ir_type}\0${subject_id}\0${digest}`), path = join(irRoot, `${id}.${ir_type}.json`);
    const document = { schema: `bonfyre.${ir_type}.v${schema_version}`, id, ...body, digest };
    return atomic(() => {
      output(path, canonicalJson(document));
      insert('ir_objects', { id, ir_type, subject_kind, subject_id, schema_version, payload_json: canonicalJson(body.payload), dependencies_json: canonicalJson(body.dependencies), digest, created_at: clock() });
      registerDependency({ source_kind: subject_kind, source_id: subject_id, target_kind: 'ir', target_id: id, edge_kind: `compiled_as:${ir_type}` });
      const receipt = writeReceipt({ kind: 'ir.compile', subject_kind: 'ir', subject_id: id, state: 'passed', payload: { path, document } });
      return { ...document, path, receipt };
    });
  }
  function compileIrBundle({ subject_kind, subject_id, mission, artifact_ids = [], token_artifact_ids = [], delta_ids = [] }) {
    mission = map(mission, 'mission'); artifact_ids = list(artifact_ids, 'artifacts'); token_artifact_ids = list(token_artifact_ids, 'tokens'); delta_ids = list(delta_ids, 'deltas');
    // Never trust a supplied serialized plan: rederive keys from owned semantic
    // input, then bind the same plan identity into every representation.
    const taskPlan = Object.hasOwn(mission, 'tasks') ? compileTaskPlan(mission.tasks, { context: missionPlanContext(mission) }) : null;
    const common = { mission_id: mission.id ?? null, goal: mission.goal ?? null, artifact_ids, token_artifact_ids, delta_ids,
      ...(taskPlan ? { task_plan_digest: taskPlan.digest } : {}) };
    const payloads = {
      ArtifactIR: { canonical_sources: artifact_ids, token_spans: token_artifact_ids, classification: 'internal' },
      DeltaIR: { logical_changes: delta_ids, affected_closure_required: true, physical_encodings: ['RAW', 'COPY_RANGE', 'CHUNK_MANIFEST'],
        ...(taskPlan ? { task_input_roots: taskPlan.nodes.map(node => ({ id: node.id, input_digest: node.input_digest })) } : {}) },
      EquivalenceIR: { alternatives: [], guards: [], objectives: ['correctness', 'proof_strength', 'token_cost'] },
      DistributedIR: { nodes: ['local', 'oracle'], authority_zones: ['candidate', 'authoritative'], placement_status: 'uncompiled' },
      ExecutionPhenotypeIR: { tokenizer_requirements: ['exact'], context_plan: 'gigatoken-closure', fallback_lanes: ['local', 'oracle'],
        ...(taskPlan ? { task_plan: taskPlan } : {}) },
      AuthorityIR: { effect_class: 'CANDIDATE_WRITE', external_effects: false, requires_zig_authority: true },
      ProofIR: { obligations: mission.verification ?? [], receipt_chain_required: true, incremental_full_parity_required: true },
    };
    return atomic(() => ({ subject_kind, subject_id, objects: Object.fromEntries(Object.entries(payloads).map(([ir_type, payload]) =>
      [ir_type, compileIr({ ir_type, subject_kind, subject_id, payload: { ...common, ...payload } })])) }));
  }
  function compileRuntimeImage({ mission_id = null, active_bfp_root = null, graph_roots = {}, token_roots = {}, provider_routes = {},
    session_pointers = [], authority_state = {}, placement = {} }) {
    str(mission_id, 'mission id', true); str(active_bfp_root, 'BFP root', true);
    return atomic(() => {
      const receiptChain = verifyReceipts(), flux = verifyFlux();
      if (!receiptChain.verified || !flux.verified) throw new Error('runtime image requires verified receipt and flux chains');
      const payload = { mission_id, active_bfp_root, graph_roots: map(graph_roots, 'graph roots'), token_roots: map(token_roots, 'token roots'),
        provider_routes: map(provider_routes, 'provider routes'), session_pointers: list(session_pointers, 'sessions'), authority_state: map(authority_state, 'authority'),
        placement: map(placement, 'placement'), proof_head: receiptChain.head, flux_heads: flux.streams };
      const digest = digestValue(payload), id = hash(`runtime-image\0${digest}`), document = { schema: 'bonfyre.runtime-image.v1', id, created_at: clock(), ...payload, digest };
      const path = join(runtimeRoot, `${id}.json`); output(path, canonicalJson(document));
      insert('runtime_images', { id, mission_id, active_bfp_root, ...Object.fromEntries(['graph_roots', 'token_roots', 'provider_routes', 'session_pointers', 'authority_state', 'placement'].map(key => [`${key}_json`, canonicalJson(payload[key])])),
        proof_head: payload.proof_head, payload_json: canonicalJson(payload), digest, created_at: document.created_at });
      return { ...document, path, receipt: writeReceipt({ kind: 'runtime.image.compile', subject_kind: 'runtime_image', subject_id: id, state: 'passed', payload: { path, digest } }) };
    });
  }
  function compileProviderBid({ mission_id = null, provider_id, tokenizer_id, required_tokens, reusable_tokens = 0, expected_output_tokens = 0,
    latency_ms = null, cost_usd = null, privacy_class = 'internal', quality_score = null, proof_score = null, compatible = true, metadata = {} }) {
    str(mission_id, 'mission id', true); str(provider_id, 'provider id'); str(tokenizer_id, 'tokenizer id'); str(privacy_class, 'privacy class');
    num(required_tokens, 0, Number.MAX_SAFE_INTEGER, true); num(reusable_tokens, 0, Number.MAX_SAFE_INTEGER, true); num(expected_output_tokens, 0, Number.MAX_SAFE_INTEGER, true);
    for (const value of [latency_ms, cost_usd, quality_score, proof_score]) if (value !== null) num(value);
    if (typeof compatible !== 'boolean') throw new Error('invalid compatibility');
    const reusable = Math.min(required_tokens, reusable_tokens), uncached = required_tokens - reusable;
    const payload = { mission_id, provider_id, tokenizer_id, required_tokens, reusable_tokens: reusable, uncached_tokens: uncached,
      expected_output_tokens, latency_ms, cost_usd, privacy_class, quality_score, proof_score, compatible, metadata: map(metadata, 'metadata') };
    const id = hash(`${mission_id ?? 'None'}\0${provider_id}\0${digestValue(payload)}`);
    insert('provider_token_bids', { id, mission_id, provider_id, tokenizer_id, required_tokens, reusable_tokens: reusable,
      uncached_tokens: uncached, expected_output_tokens, latency_ms, cost_usd, privacy_class, quality_score, proof_score,
      compatible: compatible ? 1 : 0, payload_json: canonicalJson(payload), created_at: clock() });
    return { bid_id: id, ...payload };
  }
  function registerMotif({ name, structure, reconstruction_checks = 0 }) {
    str(name, 'motif name'); structure = map(structure, 'structure'); num(reconstruction_checks, 0, Number.MAX_SAFE_INTEGER, true);
    const digest = digestValue(structure), id = hash(`lambda-motif\0${digest}`), at = clock();
    return atomic(() => {
      write(`INSERT INTO lambda_motifs VALUES(?,?,?,?,0,?,?,?) ON CONFLICT(digest) DO UPDATE SET name=excluded.name,
        reconstruction_checks=MAX(lambda_motifs.reconstruction_checks,excluded.reconstruction_checks),updated_at=excluded.updated_at`,
      id, name, canonicalJson(structure), digest, reconstruction_checks, at, at);
      const { structure_json, ...row } = get('SELECT * FROM lambda_motifs WHERE digest=?', digest); return { ...row, structure: JSON.parse(structure_json) };
    });
  }
  function matchMotifs(structure) {
    structure = map(structure, 'structure'); const digest = digestValue(structure), exact = get('SELECT * FROM lambda_motifs WHERE digest=?', digest);
    const decode = ({ structure_json, ...row }) => ({ ...row, structure: JSON.parse(structure_json) });
    if (exact) return { match: 'exact', motif: decode(exact) };
    const target = new Set(Object.keys(structure));
    const candidates = db().query('SELECT * FROM lambda_motifs ORDER BY instance_count DESC,updated_at DESC LIMIT 50').all().map(row => {
      const motif = decode(row), keys = Object.keys(motif.structure), union = new Set([...keys, ...target]).size || 1;
      return { score: keys.filter(key => target.has(key)).length / union, motif };
    }).sort((a, b) => b.score - a.score || compareUnicode(a.motif.id, b.motif.id));
    return { match: 'approximate', candidates: candidates.slice(0, 10) };
  }
  function graphProjection({ graph_kind, root_kind, root_id, max_depth = 12 }) {
    str(graph_kind, 'graph kind');
    return atomic(() => {
      const closure = affectedClosure({ root_kind, root_id, max_depth }), body = { graph_kind, root_kind, root_id, nodes: closure.nodes, edges: closure.edges };
      const digest = digestValue(body), id = hash(`${graph_kind}\0${root_kind}\0${root_id}\0${digest}`);
      insert('graph_projections', { id, graph_kind, root_kind, root_id, nodes_json: canonicalJson(body.nodes), edges_json: canonicalJson(body.edges), digest, created_at: clock() });
      return { projection_id: id, digest, ...body };
    });
  }
  function bfpSuccessorPlan({ source_root = null, target_root = null, changed_sections, token_chunk_ids = [], logical_delta_ids = [], rollback_root = null, destination = {} }) {
    for (const value of [source_root, target_root, rollback_root]) str(value, 'root', true);
    changed_sections = list(changed_sections, 'sections'); for (const item of changed_sections) str(item, 'section');
    token_chunk_ids = list(token_chunk_ids, 'token chunks'); logical_delta_ids = list(logical_delta_ids, 'deltas');
    const physical = [{ encoding: 'CHUNK_MANIFEST', eligible: Boolean(token_chunk_ids.length), reason: 'content-addressed token chunks' },
      { encoding: 'COPY_RANGE', eligible: Boolean(source_root), reason: source_root ? 'stable predecessor root available' : 'thin-copy unavailable' },
      { encoding: 'RAW', eligible: true, reason: 'universal fallback' }];
    const payload = { source_root, target_root, changed_sections: [...new Set(changed_sections)].sort(compareUnicode), token_chunk_ids, logical_delta_ids,
      physical_candidates: physical, rollback_root: rollback_root || source_root, destination: map(destination, 'destination'),
      activation: { staged: true, atomic: true, requires_reconstruction_proof: true, requires_authority_gate: true } };
    const digest = digestValue(payload), id = hash(`bfp-successor\0${digest}`);
    return atomic(() => {
      insert('bfp_successor_plans', { id, source_root, target_root, changed_sections_json: canonicalJson(payload.changed_sections), token_chunks_json: canonicalJson(token_chunk_ids),
        logical_delta_ids_json: canonicalJson(logical_delta_ids), physical_candidates_json: canonicalJson(physical), rollback_root: payload.rollback_root,
        payload_json: canonicalJson(payload), digest, created_at: clock() });
      return { plan_id: id, digest, ...payload, receipt: writeReceipt({ kind: 'bfp.successor.plan', subject_kind: 'bfp_successor_plan', subject_id: id, state: 'candidate', payload }) };
    });
  }
  async function status() {
    const counts = Object.fromEntries(TABLES.map(table => [table, get(`SELECT COUNT(*) AS count FROM ${table}`).count]));
    const tokenizers = db().query('SELECT * FROM tokenizer_descriptors ORDER BY id LIMIT 1000').all();
    let tokenStatus;
    try { tokenStatus = await native(['status'], 10000); }
    catch (error) { tokenStatus = error.receipt ?? { argv: [tokenBinary, 'status'], exit_code: 127, stdout: '', stderr: 'Native token owner unavailable or refused', duration_ms: 0 }; }
    return { service: 'bonfyre-recursive-substrate-v5', version: VERSION, home, database: dbPath, token_binary: tokenBinary,
      token_binary_status: tokenStatus, counts, tokenizers, receipt_chain: verifyReceipts(), flux_chains: verifyFlux() };
  }
  function close() { if (connection) connection.close(); connection = null; closed = true; }
  return Object.freeze({ home, dbPath, tokenBinary, artifactRoot, tokenRoot, deltaRoot, irRoot, receiptRoot, runtimeRoot,
    bootstrap, ingestArtifact, artifact, compileTokenArtifact, tokenArtifact, deriveTokenDelta, appendFlux, recordDelta,
    registerDependency, affectedClosure, compileIr, compileIrBundle, compileRuntimeImage, compileProviderBid, registerMotif,
    matchMotifs, graphProjection, bfpSuccessorPlan, writeReceipt, verifyReceipts, verifyFlux, fluxHeads, status, close });
}
