import { createHash } from 'node:crypto';
import { lstatSync } from 'node:fs';
import { join } from 'node:path';
import { createSpinesWasm, SPINES_HELIX_WASM_PATH, SPINES_HELIX_WASM_SHA256 } from './spines_wasm.mjs';

const EFFECTS = new Set(['PURE', 'READ_ONLY', 'CANDIDATE_WRITE', 'REVERSIBLE_INTERNAL_WRITE',
  'AUTHORITATIVE_INTERNAL_WRITE', 'EXTERNAL_REVERSIBLE_EFFECT', 'EXTERNAL_IRREVERSIBLE_EFFECT']);
const MAX_TERM_BYTES = 64 * 1024;
const MAX_TERM_DEPTH = 64;
const MAX_TERM_NODES = 4096;
const MAX_JSON_BYTES = 1024 * 1024;
const MAX_RULES = 2048;
const hash = text => createHash('sha256').update(text).digest('hex');
const app = (name, args) => ({ kind: 'app', name, args });
const numeric = (value, floating) => ({ kind: 'number', value, floating });

function jsonBounded(value, name) {
  const text = JSON.stringify(value);
  if (text === undefined || Buffer.byteLength(text) > MAX_JSON_BYTES) throw new Error(`${name} exceeds JSON budget`);
  return text;
}
function textBounded(value, name, maximum = MAX_TERM_BYTES) {
  if (typeof value !== 'string' || Buffer.byteLength(value) > maximum || value.includes('\0')) throw new Error(`invalid ${name}`);
  return value;
}
function countBounded(value, name, minimum, maximum) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new Error(`invalid ${name}`);
  return value;
}
function pythonNumber(value) {
  if (!Number.isFinite(value)) throw new Error('nonfinite term');
  if (Object.is(value, -0)) return '-0';
  const scientific = value.toExponential(16);
  const [, mantissa, exponent] = /^([^e]+)e([+-]\d+)$/.exec(scientific);
  const power = Number(exponent);
  if (power < -4 || power >= 17) {
    return `${mantissa.replace(/\.?0+$/, '')}e${power >= 0 ? '+' : '-'}${String(Math.abs(power)).padStart(2, '0')}`;
  }
  return value.toPrecision(17).replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
}
function termText(term) {
  if (typeof term === 'string') return term;
  if (term.kind === 'number') return term.floating ? pythonNumber(term.value) : String(term.value);
  return `${term.name}(${term.args.map(termText).join(',')})`;
}
function termCost(term) { return typeof term === 'object' && term.kind === 'app' ? 1 + term.args.reduce((sum, arg) => sum + termCost(arg), 0) : 1; }
function termDepth(term) { return typeof term === 'object' && term.kind === 'app' ? 1 + Math.max(0, ...term.args.map(termDepth)) : 1; }
function checkTermShape(term) {
  const stack = [[term, 1]]; let nodes = 0;
  while (stack.length) {
    const [node, depth] = stack.pop();
    if (depth > MAX_TERM_DEPTH || ++nodes > MAX_TERM_NODES) throw new Error('rewritten term complexity budget exceeded');
    if (typeof node === 'object' && node.kind === 'app') for (const child of node.args) stack.push([child, depth + 1]);
  }
}

class TermParser {
  constructor(text) { this.text = textBounded(text, 'term'); this.position = 0; this.nodes = 0; }
  skip() { while (/\s/u.test(this.text[this.position] || '') && this.position < this.text.length) this.position++; }
  parse() { const term = this.read(0); this.skip(); if (this.position !== this.text.length) throw new Error(`trailing term input at ${this.position}`); return term; }
  read(depth) {
    if (depth > MAX_TERM_DEPTH || ++this.nodes > MAX_TERM_NODES) throw new Error('term complexity budget exceeded');
    this.skip();
    if (this.position >= this.text.length) throw new Error('unexpected end of term');
    const start = this.position;
    const rest = this.text.slice(start);
    if (/^[-.\d]/.test(rest)) {
      const match = /^-?(?:\d+(?:\.\d*)?|\.\d+)/.exec(rest);
      if (match) {
        this.position += match[0].length;
        const value = Number(match[0]);
        if (!Number.isFinite(value) || (!match[0].includes('.') && !Number.isSafeInteger(value))) throw new Error('unsafe numeric term');
        return numeric(value, match[0].includes('.'));
      }
    }
    if (rest[0] === '?') {
      const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(rest.slice(1));
      if (!match) throw new Error('invalid pattern variable');
      this.position += match[0].length + 1;
      return `?${match[0]}`;
    }
    const match = /^[A-Za-z_][A-Za-z0-9_.:-]*/.exec(rest);
    if (!match) throw new Error(`invalid term at ${start}`);
    const name = match[0]; this.position += name.length; this.skip();
    if (this.text[this.position] !== '(') return name;
    this.position++; this.skip();
    const args = [];
    if (this.text[this.position] === ')') { this.position++; return app(name, args); }
    for (;;) {
      args.push(this.read(depth + 1)); this.skip();
      if (this.position >= this.text.length) throw new Error('unterminated application');
      if (this.text[this.position] === ')') { this.position++; return app(name, args); }
      if (this.text[this.position] !== ',') throw new Error(`expected comma at ${this.position}`);
      this.position++;
    }
  }
}
export const parseSpinesTerm = text => new TermParser(text).parse();

function equalTerm(left, right) {
  if (typeof left !== typeof right) return false;
  if (typeof left === 'string') return left === right;
  if (left.kind !== right.kind) return false;
  if (left.kind === 'number') return left.floating === right.floating && left.value === right.value;
  return left.name === right.name && left.args.length === right.args.length && left.args.every((value, index) => equalTerm(value, right.args[index]));
}
function matchTerm(pattern, value, bindings = new Map()) {
  if (typeof pattern === 'string' && pattern.startsWith('?')) {
    if (bindings.has(pattern) && !equalTerm(bindings.get(pattern), value)) return null;
    bindings.set(pattern, value); return bindings;
  }
  if (typeof pattern !== typeof value || (typeof pattern === 'object' && pattern.kind !== value.kind)) return null;
  if (typeof pattern === 'string') return pattern === value ? bindings : null;
  if (pattern.kind === 'number') return equalTerm(pattern, value) ? bindings : null;
  if (pattern.name !== value.name || pattern.args.length !== value.args.length) return null;
  for (let i = 0; i < pattern.args.length; i++) if (!matchTerm(pattern.args[i], value.args[i], bindings)) return null;
  return bindings;
}
function substitute(term, bindings) {
  if (typeof term === 'string' && term.startsWith('?')) {
    if (!bindings.has(term)) throw new Error(`unbound rewrite variable ${term}`);
    return bindings.get(term);
  }
  return typeof term === 'object' && term.kind === 'app' ? app(term.name, term.args.map(arg => substitute(arg, bindings))) : term;
}
function* rewriteOnce(term, lhs, rhs, depth = 0) {
  if (depth > MAX_TERM_DEPTH) throw new Error('rewrite depth exceeded');
  const bindings = matchTerm(lhs, term);
  if (bindings) yield substitute(rhs, bindings);
  if (typeof term === 'object' && term.kind === 'app') {
    for (let i = 0; i < term.args.length; i++) {
      for (const replacement of rewriteOnce(term.args[i], lhs, rhs, depth + 1)) {
        const args = term.args.slice(); args[i] = replacement; yield app(term.name, args);
      }
    }
  }
}
function constantFold(term) {
  if (typeof term !== 'object' || term.kind !== 'app') return term;
  const args = term.args.map(constantFold);
  if (args.every(arg => typeof arg === 'object' && arg.kind === 'number')) {
    const a = args[0]?.value ?? 0, b = args[1]?.value ?? 0;
    let value;
    switch (term.name) {
      case 'add': value = a + b; break;
      case 'sub': value = a - b; break;
      case 'mul': value = a * b; break;
      case 'div': if (b !== 0) value = a / b; break;
      case 'min': if (args.length) value = Math.min(...args.map(arg => arg.value)); break;
      case 'max': if (args.length) value = Math.max(...args.map(arg => arg.value)); break;
      case 'eq': value = Math.abs(a - b) < 1e-12 ? 1 : 0; break;
      case 'lt': value = a < b ? 1 : 0; break;
      case 'gt': value = a > b ? 1 : 0; break;
      case 'neg': value = -a; break;
      case 'sqrt': if (a >= 0) value = Math.sqrt(a); break;
    }
    if (value !== undefined && Number.isFinite(value)) return numeric(value, false === (term.name === 'eq' || term.name === 'lt' || term.name === 'gt'));
  }
  return app(term.name, args);
}

export function createReasoningEngine(store, { wasmPath = SPINES_HELIX_WASM_PATH } = {}) {
  if (!store?.db || typeof store.digest !== 'function' || typeof store.writeReceipt !== 'function') throw new Error('reasoning store unavailable');
  const wasm = createSpinesWasm({ artifactPath: wasmPath });
  const receipt = (kind, subject_kind, subject_id, state, payload) => store.writeReceipt({ release: 'R6', kind, subject_kind, subject_id, state, payload });
  const time = () => store.now();
  const db = () => store.db;
  const wasmPresent = () => {
    try { lstatSync(wasmPath); return true; }
    catch (error) { if (error.code === 'ENOENT') return false; throw error; }
  };

  function rules() {
    return db().query('SELECT * FROM rewrite_rules ORDER BY name').all().map(({ guard_json, ...row }) => ({ ...row, guard: JSON.parse(guard_json) }));
  }
  function addRule({ name, lhs, rhs, guard = {}, enabled = true } = {}) {
    textBounded(name, 'rule name', 1024); textBounded(lhs, 'lhs'); textBounded(rhs, 'rhs');
    parseSpinesTerm(lhs); parseSpinesTerm(rhs); jsonBounded(guard, 'guard');
    const body = { name, lhs, rhs, guard };
    const ruleId = hash(`${name}\0${lhs}\0${rhs}\0${store.digest(guard)}`);
    db().query('INSERT OR REPLACE INTO rewrite_rules(id,name,lhs,rhs,guard_json,enabled,digest,created_at) VALUES(?,?,?,?,?,?,?,?)')
      .run(ruleId, name, lhs, rhs, JSON.stringify(guard), enabled ? 1 : 0, store.digest(body), time());
    return { rule_id: ruleId, ...body, receipt: receipt('rewrite.rule.register', 'rewrite_rule', ruleId, 'passed', body) };
  }
  function saturate({ term, rule_ids = null, max_iterations = 12, max_terms = 2048 } = {}) {
    const seed = parseSpinesTerm(term);
    countBounded(max_iterations, 'max_iterations', 1, 100);
    countBounded(max_terms, 'max_terms', 1, 10000);
    if (rule_ids !== null && (!Array.isArray(rule_ids) || rule_ids.length > MAX_RULES || rule_ids.some(id => typeof id !== 'string' || !/^[a-f0-9]{64}$/.test(id)))) throw new Error('invalid rule_ids');
    const rows = rule_ids?.length ? db().query(`SELECT * FROM rewrite_rules WHERE enabled=1 AND id IN (${rule_ids.map(() => '?').join(',')}) ORDER BY name`).all(...rule_ids)
      : db().query('SELECT * FROM rewrite_rules WHERE enabled=1 ORDER BY name').all();
    if (rows.length > MAX_RULES) throw new Error('rewrite rule budget exceeded');
    const active = rows.map(row => ({ id: row.id, name: row.name, lhs: parseSpinesTerm(row.lhs), rhs: parseSpinesTerm(row.rhs) }));
    const terms = new Map([[termText(seed), seed]]); let frontier = [seed], truncated = false, iterations = 0;
    for (let iteration = 0; iteration < max_iterations; iteration++) {
      iterations = iteration + 1; const next = [];
      for (const current of frontier) {
        const addCandidate = candidate => {
          checkTermShape(candidate);
          const text = termText(candidate);
          if (Buffer.byteLength(text) > MAX_TERM_BYTES) throw new Error('rewritten term budget exceeded');
          if (!terms.has(text)) { terms.set(text, candidate); next.push(candidate); if (terms.size >= max_terms) truncated = true; }
        };
        addCandidate(constantFold(current));
        if (!truncated) for (const rule of active) {
          for (const candidate of rewriteOnce(current, rule.lhs, rule.rhs)) {
            addCandidate(constantFold(candidate));
            if (truncated) break;
          }
          if (truncated) break;
        }
        if (truncated) break;
      }
      if (truncated || !next.length) break;
      frontier = next;
    }
    const ranked = [...terms].map(([text, value]) => ({ term: text, cost: termCost(value) }))
      .sort((a, b) => a.cost - b.cost || a.term.length - b.term.length || (a.term < b.term ? -1 : a.term > b.term ? 1 : 0));
    const selected = ranked[0].term;
    const body = { schema: 'bonfyre.equivalence-saturation.v1', input_term: term, selected_term: selected,
      terms: ranked, iterations, truncated, rules: active.map(({ id, name }) => ({ id, name })),
      backend: 'bonfyre-bounded-egraph-r6', egglog_adapter: store.backendStatus().egglog };
    jsonBounded(body, 'saturation body');
    const digest = store.digest(body), runId = hash(`${term}\0${selected}\0${digest}`);
    db().query('INSERT OR REPLACE INTO saturation_runs(id,input_term,selected_term,terms_json,iterations,truncated,rules_json,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
      .run(runId, term, selected, JSON.stringify(ranked), iterations, truncated ? 1 : 0, JSON.stringify(body.rules), digest, time());
    return { run_id: runId, digest, ...body, receipt: receipt('equivalence.saturate', 'saturation_run', runId, 'passed', body) };
  }
  function compileHelix({ term, source_run_id = null, fuel = 10000, max_depth = 256, labels = {} } = {}) {
    const parsed = parseSpinesTerm(term); jsonBounded(labels, 'labels');
    if (!Number.isSafeInteger(fuel) || fuel < 1 || !Number.isSafeInteger(max_depth) || max_depth < 1) throw new Error('invalid resource bounds');
    const bounds = { fuel: Math.max(1, Math.min(fuel, 1000000)), max_depth: Math.max(1, Math.min(max_depth, 4096)), effect_scope: 'PURE' };
    if (termDepth(parsed) > bounds.max_depth) throw new Error('helix max_depth exceeded');
    const body = { schema: 'bonfyre.helix.v1', source_run_id, term, duplication_labels: labels,
      superposition_labels: {}, guards: [], resource_bounds: bounds, backend_contract: 'HVM4-compatible-pure-term-boundary' };
    const digest = store.digest(body), helixId = hash(`${term}\0${digest}`), path = join(store.helixRoot, `${helixId}.helix.json`);
    store.writeJson(path, { ...body, id: helixId, digest });
    db().query('INSERT OR REPLACE INTO helix_artifacts(id,source_run_id,term,resource_bounds_json,path,digest,created_at) VALUES(?,?,?,?,?,?,?)')
      .run(helixId, source_run_id, term, JSON.stringify(bounds), path, digest, time());
    return { helix_id: helixId, path, digest, ...body,
      receipt: receipt('helix.compile', 'helix', helixId, 'passed', { path, ...body }) };
  }
  async function reduce({ helix_id } = {}) {
    if (typeof helix_id !== 'string' || !/^[a-f0-9]{64}$/.test(helix_id)) throw new Error('invalid helix_id');
    const row = db().query('SELECT * FROM helix_artifacts WHERE id=?').get(helix_id);
    if (!row) throw new Error(`unknown helix_id ${helix_id}`);
    if (row.path !== join(store.helixRoot, `${helix_id}.helix.json`)) throw new Error('helix artifact path mismatch');
    const artifact = store.readJson(row.path);
    if (!artifact || typeof artifact !== 'object' || Array.isArray(artifact) || artifact.id !== helix_id || artifact.digest !== row.digest)
      throw new Error('helix artifact identity mismatch');
    const { id: _artifactId, digest: _artifactDigest, ...artifactBody } = artifact;
    if (store.digest(artifactBody) !== row.digest || artifactBody.schema !== 'bonfyre.helix.v1' || artifactBody.term !== row.term ||
      artifactBody.source_run_id !== row.source_run_id || artifactBody.resource_bounds?.effect_scope !== 'PURE' ||
      artifactBody.backend_contract !== 'HVM4-compatible-pure-term-boundary') throw new Error('helix artifact body mismatch');
    const parsed = parseSpinesTerm(row.term);
    const bounds = JSON.parse(row.resource_bounds_json);
    if (store.digest(bounds) !== store.digest(artifactBody.resource_bounds) ||
      !Number.isSafeInteger(bounds.fuel) || bounds.fuel < 1 || bounds.fuel > 1000000 ||
      !Number.isSafeInteger(bounds.max_depth) || bounds.max_depth < 1 || bounds.max_depth > 4096)
      throw new Error('helix resource bounds mismatch');
    if (termDepth(parsed) > bounds.max_depth) throw new Error('helix max_depth exceeded');
    const useWasm = wasmPresent();
    const probe = useWasm ? await wasm.reduce(row.term, bounds.fuel)
      : await store.run([store.helixVm, 'reduce', '--term', row.term, '--fuel', String(bounds.fuel)], { timeout: 60 });
    let witness;
    try { witness = probe.stdout ? JSON.parse(probe.stdout) : {}; }
    catch { witness = { state: 'failed', raw: probe }; }
    const validWitness = witness && typeof witness === 'object' && !Array.isArray(witness) &&
      witness.schema === 'bonfyre.helix-witness.v1' && witness.state === 'passed' &&
      witness.term_sha256 === hash(row.term) && Number.isSafeInteger(witness.steps) &&
      witness.steps >= 1 && witness.steps <= bounds.fuel &&
      witness.fuel_remaining === bounds.fuel - witness.steps &&
      typeof witness.result === 'number' && Number.isFinite(witness.result);
    const state = probe.exit_code === 0 && validWitness ? 'passed' : 'failed';
    const body = { schema: 'bonfyre.reduction.v1', helix_id,
      backend: useWasm ? 'bonfyre-helix-wasm-r7' : 'bonfyre-helix-vm-r6',
      hvm4_adapter: store.backendStatus().hvm4, state, witness, probe,
      ...(useWasm ? { wasm_artifact_sha256: SPINES_HELIX_WASM_SHA256 } : {}),
      ...(probe.exit_code === 0 && !validWitness ? { witness_validation: 'native_witness_identity_or_bounds_mismatch' } : {}) };
    jsonBounded(body, 'reduction body');
    const digest = store.digest(body), reductionId = hash(`${helix_id}\0${digest}`), witnessPath = join(store.cruxRoot, `${reductionId}.witness.json`);
    store.writeJson(witnessPath, { ...body, id: reductionId, digest });
    db().query('INSERT OR REPLACE INTO reductions(id,helix_id,backend,state,witness_json,witness_path,digest,created_at) VALUES(?,?,?,?,?,?,?,?)')
      .run(reductionId, helix_id, body.backend, state, JSON.stringify(witness), witnessPath, digest, time());
    return { reduction_id: reductionId, digest, witness_path: witnessPath, ...body,
      receipt: receipt('helix.reduce', 'reduction', reductionId, state, body) };
  }
  function authority({ actor, effect_class, resource = null, proof_refs = [] } = {}) {
    textBounded(actor, 'actor', 4096);
    if (typeof effect_class !== 'string' || !EFFECTS.has(effect_class.toUpperCase())) throw new Error('invalid effect_class');
    effect_class = effect_class.toUpperCase();
    if (resource !== null) textBounded(resource, 'resource', 16384);
    if (!Array.isArray(proof_refs) || proof_refs.length > 2048 || proof_refs.some(ref => typeof ref !== 'string')) throw new Error('invalid proof_refs');
    let decision, reason, requires_zig;
    if (effect_class === 'PURE') [decision, reason, requires_zig] = ['allowed_pure', 'Pure deterministic computation may run inside bounded R6.', false];
    else if (effect_class === 'READ_ONLY') [decision, reason, requires_zig] = ['allowed_read', 'Read-only observation may run under the current capability scope.', false];
    else if (effect_class === 'CANDIDATE_WRITE') [decision, reason, requires_zig] = ['candidate_only', 'Candidate state may be constructed but cannot become authoritative.', true];
    else if (effect_class === 'REVERSIBLE_INTERNAL_WRITE') [decision, reason, requires_zig] = ['review_required', 'Reversible internal mutation requires explicit Zig authority and rollback proof.', true];
    else [decision, reason, requires_zig] = ['denied', 'Authoritative or external effects are outside R6 automatic authority.', true];
    const body = { schema: 'bonfyre.authority-decision.v1', actor, effect_class, resource, decision, reason,
      proof_refs, requires_zig, zig_executed: false };
    const digest = store.digest(body), decisionId = hash(`${actor}\0${effect_class}\0${resource || ''}\0${digest}`);
    db().query('INSERT OR REPLACE INTO authority_decisions(id,actor,effect_class,resource,decision,reason,proof_refs_json,requires_zig,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)')
      .run(decisionId, actor, effect_class, resource, decision, reason, JSON.stringify(proof_refs), requires_zig ? 1 : 0, digest, time());
    return { decision_id: decisionId, digest, ...body,
      receipt: receipt('authority.preflight', 'authority_decision', decisionId,
        ['allowed_pure', 'allowed_read', 'candidate_only'].includes(decision) ? 'passed' : 'blocked', body) };
  }
  function crux({ claim, inputs, witnesses, authority_decision_id = null, classification = 'candidate' } = {}) {
    textBounded(claim, 'claim', 16384); jsonBounded(inputs, 'inputs'); jsonBounded(witnesses, 'witnesses');
    if (authority_decision_id !== null && (typeof authority_decision_id !== 'string' || !/^[a-f0-9]{64}$/.test(authority_decision_id))) throw new Error('invalid authority_decision_id');
    textBounded(classification, 'classification', 1024);
    const body = { schema: 'bonfyre.crux.v1', claim, inputs, witnesses, authority_decision_id, classification, authoritative: false };
    const digest = store.digest(body), proofId = hash(`${claim}\0${digest}`), path = join(store.cruxRoot, `${proofId}.crux.json`);
    store.writeJson(path, { ...body, id: proofId, digest });
    db().query('INSERT OR REPLACE INTO crux_proofs(id,claim,inputs_json,witnesses_json,authority_decision_id,classification,path,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
      .run(proofId, claim, JSON.stringify(inputs), JSON.stringify(witnesses), authority_decision_id, classification, path, digest, time());
    return { proof_id: proofId, path, digest, ...body,
      receipt: receipt('crux.seal', 'crux', proofId, 'passed', { path, ...body }) };
  }
  return { rules, addRule, saturate, compileHelix, reduce, authority, crux };
}
