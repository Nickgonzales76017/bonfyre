// Preservation helpers for the existing handoff build. No work or credential store.
import * as fs from 'node:fs';
import { dirname, basename, join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';

export function parseTSV(input) {
  if (typeof input !== 'string' || Buffer.byteLength(input) > 32 * 1024 * 1024 || input.includes('\0')) throw Error('Invalid or oversized TSV input.');
  const text = input.replace(/^\uFEFF/, ''), matrix = [];
  let row = [], field = '', quoted = false, closed = false;
  const cell = () => { row.push(field); field = ''; closed = false; if (row.length > 128) throw Error('Too many TSV columns.'); };
  const line = () => { cell(); if (row.some(v => v !== '')) matrix.push(row); row = []; if (matrix.length > 100001) throw Error('Too many TSV rows.'); };
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else { quoted = false; closed = true; } } else field += c; continue; }
    if (c === '\t') { cell(); continue; }
    if (c === '\n' || c === '\r') { if (c === '\r' && text[i + 1] === '\n') i++; line(); continue; }
    if (closed) throw Error('Unexpected text after a quoted TSV field.');
    if (c === '"' && field === '') quoted = true; else field += c;
  }
  if (quoted) throw Error('Unclosed quoted TSV field.');
  if (field || row.length || closed) line();
  const columns = matrix.shift();
  if (!columns?.length || columns.some(c => !c || c.trim() !== c) || new Set(columns).size !== columns.length) throw Error('TSV header names must be nonempty and unique.');
  const rows = matrix.map((values, i) => { if (values.length !== columns.length) throw Error(`TSV record ${i + 2} has ${values.length} fields; expected ${columns.length}.`); return Object.fromEntries(columns.map((c, j) => [c, values[j]])); });
  return { columns, rows };
}
export const tsv = input => parseTSV(input).rows;
const cell = value => { const s = String(value ?? ''); return /[\t\r\n"]/.test(s) ? '"' + s.replaceAll('"', '""') + '"' : s; };
export function writeTSV(rows, preferredColumns) {
  if (!Array.isArray(preferredColumns) || new Set(preferredColumns).size !== preferredColumns.length) throw Error('Invalid output columns.');
  // Preserve supplemental fields instead of silently dropping future progress/requirements.
  const columns = [...preferredColumns];
  for (const row of rows) for (const key of Object.keys(row)) if (!columns.includes(key)) columns.push(key);
  const result = columns.map(cell).join('\t') + '\n' + rows.map(row => columns.map(c => cell(row[c])).join('\t')).join('\n') + (rows.length ? '\n' : '');
  parseTSV(result); return result;
}
function identity(row, keys) {
  return keys.map(k => { if (!Object.hasOwn(row, k) || String(row[k]) === '') throw Error('Missing identity field: ' + k); const v = String(row[k]); return v.length + ':' + v; }).join('|');
}
export function indexRows(rows, keys, name = 'ledger') {
  const fields = Array.isArray(keys) ? keys : [keys], map = new Map();
  for (const row of rows) { const id = identity(row, fields); if (map.has(id)) throw Error(name + ': repeated identity ' + id); map.set(id, row); }
  return map;
}
export function preserveLedger(seed, current, { keys, immutable = [], name = 'ledger' }) {
  const fields = Array.isArray(keys) ? keys : [keys], incoming = indexRows(seed, fields, name + ' source');
  if (current === null) return seed.map(row => ({ ...row }));
  const prior = indexRows(current, fields, name);
  if (prior.size !== incoming.size || [...prior.keys()].some(k => !incoming.has(k))) throw Error(name + ': scope membership changed; reconcile explicitly before rebuilding.');
  return seed.map(row => {
    const old = prior.get(identity(row, fields));
    for (const key of [...fields, ...immutable]) {
      if (Object.hasOwn(old, key) && Object.hasOwn(row, key) && String(old[key] ?? '') !== String(row[key] ?? '')) throw Error(`${name}: source field ${key} differs for ${identity(row, fields)}; existing work was not overwritten.`);
    }
    // Current authored values, including an intentional blank, win over bootstrap defaults.
    // Source-owned identity fields were checked above; new fields are retained from the seed.
    return { ...row, ...old };
  });
}
export function sameTable(left, right) {
  const a = parseTSV(left), b = parseTSV(right);
  return a.columns.length === b.columns.length && a.columns.every(c => b.columns.includes(c)) && a.rows.length === b.rows.length && a.rows.every((row, i) => a.columns.every(c => row[c] === b.rows[i][c]));
}
export function observedReader() {
  const observations = new Map();
  const read = (path, encoding = null) => {
    const stat = fs.lstatSync(path); if (!stat.isFile() || stat.size > 32 * 1024 * 1024) throw Error('Not a bounded regular source file: ' + path);
    const bytes = fs.readFileSync(path);
    if (observations.has(path) && !bytes.equals(observations.get(path))) throw Error('Source changed during this read: ' + path);
    observations.set(path, bytes); return encoding ? bytes.toString(encoding) : bytes;
  };
  return { read, observations };
}
function matches(path, expected) {
  try { const stat = fs.lstatSync(path); return expected !== null && stat.isFile() && fs.readFileSync(path).equals(expected); }
  catch (error) { if (error.code === 'ENOENT') return expected === null; throw error; }
}
export function commitOutputs(outputs, observations, { check = false, problems = [] } = {}) {
  // Validate ALL outputs and input failures before writing even the first ledger.
  const prepared = outputs.map(({ path, text }) => { parseTSV(text); const before = observations.has(path) ? observations.get(path) : null; if (!observations.has(path) && fs.existsSync(path)) throw Error('Output preimage was not read: ' + path); return { path, text, before }; });
  if (new Set(prepared.map(p => p.path)).size !== prepared.length) throw Error('Repeated output path.');
  if (problems.length) throw Error(problems.join('\n'));
  const checkInputs = () => { for (const [path, bytes] of observations) if (!matches(path, bytes)) throw Error('Concurrent source change: ' + path); for (const p of prepared) if (!matches(p.path, p.before)) throw Error('Concurrent ledger change: ' + p.path); };
  checkInputs();
  if (check) { const changed = prepared.filter(p => p.before === null || !sameTable(p.before.toString('utf8'), p.text)); if (changed.length) throw Error(changed.map(p => p.path + ': full fields differ from rebuild').join('\n')); return { changed: [], check: true }; }
  const pending = prepared.filter(p => p.before === null || !p.before.equals(Buffer.from(p.text))), staged = [], changed = [];
  try {
    for (const p of pending) { const temporary = join(dirname(p.path), '.' + basename(p.path) + '.' + randomUUID() + '.tmp'); const fd = fs.openSync(temporary, 'wx', p.before === null ? 0o600 : fs.statSync(p.path).mode & 0o777); staged.push({ ...p, temporary }); try { fs.writeFileSync(fd, p.text); fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
    checkInputs();
    for (const p of staged) {
      if (!matches(p.path, p.before)) throw Error('Concurrent ledger change: ' + p.path);
      fs.renameSync(p.temporary, p.path); changed.push(p.path);
    }
    return { changed, check: false };
  } catch (error) { error.message += '\nReplaced before refusal: ' + (changed.join(', ') || 'none') + '. Per-file replacement is atomic; this is not a cross-file transaction.'; throw error; }
  finally { for (const p of staged) try { fs.unlinkSync(p.temporary); } catch (error) { if (error.code !== 'ENOENT') throw error; } }
}

export function suppliedPlatforms(markdown) {
  const sha256 = createHash('sha256').update(markdown).digest('hex'), rows = []; let tier = '', category = '';
  markdown.split('\n').forEach((line, i) => {
    const t = /^## Tier ([1-5]):/.exec(line); if (t) tier = t[1];
    const c = /^### (.+)/.exec(line); if (c) category = c[1].trim();
    const item = /^(\d+)\. \*\*(.+?)\*\* — (.+)/.exec(line);
    if (item) rows.push({ source_number: item[1], source_name: item[2], supplied_tier: tier, source_category: category, supplied_claim: item[3].trim(), source_line: String(i + 1), source_sha256: sha256 });
  });
  if (rows.length !== 100 || rows.some((r, i) => r.source_number !== String(i + 1) || !r.supplied_tier)) throw Error('The supplied platform attachment is not the complete numbered 100-entry source.');
  return rows;
}
