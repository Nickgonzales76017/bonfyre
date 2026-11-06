#!/usr/bin/env bun
import { Database } from 'bun:sqlite';

const ident = value => typeof value === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
const opt = (row, key) => String(row[key] ?? '').trim();
const tableExists = (db, name) => !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);

function known(db, kind, targetType, targetKey) {
  if (kind === 'table') return new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));
  if (kind === 'column') return new Set(db.query(`PRAGMA table_info(${targetType})`).all().map(row => row.name));
  if (!ident(targetKey)) return null;
  return new Set(db.query(`SELECT ${targetKey} AS value FROM ${targetType}`).all().map(row => row.value).filter(Boolean));
}

export function check(db) {
  const out = { schema: 'bonfyre-reference-check.v1', references: [], checked: 0, dangling: 0, undeclarable: 0 };
  if (!tableExists(db, 'reference_contract')) return { ...out, error: 'no reference contracts are declared' };
  const contracts = db.query("SELECT * FROM reference_contract WHERE status = 'published' ORDER BY reference").all();
  for (const c of contracts) {
    const rec = {
      reference: c.reference, holder: `${c.holder_type}.${c.holder_field}`,
      target: `${c.target_type}.${c.target_key}`, cardinality: c.cardinality,
      on_dangling: c.on_dangling, checked: 0, dangling: [],
      target_kind: (opt(c, 'target_kind') || 'row').toLowerCase(),
    };
    const kind = rec.target_kind;
    const typeField = opt(c, 'target_type_field');
    const skipField = opt(c, 'skip_when_empty');
    const fields = [c.holder_type, c.holder_key, c.holder_field];
    if (kind !== 'table') fields.push(...(typeField ? [typeField] : [c.target_type, c.target_key]));
    const undeclarable = message => {
      rec.undeclarable = message;
      out.undeclarable++;
      out.references.push(rec);
    };
    if (!fields.every(ident)) { undeclarable('a declaration names a table or column that is not an identifier'); continue; }
    if (!tableExists(db, c.holder_type)) { undeclarable('the holder type is not a table here'); continue; }
    if (kind !== 'table' && !typeField && !tableExists(db, c.target_type)) { undeclarable('the target type is not a table here'); continue; }
    const fixed = typeField ? null : known(db, kind, c.target_type, c.target_key);
    if (!typeField && fixed === null) { undeclarable('the target names no rows and no columns here'); continue; }
    const sep = c.separator || '';
    const required = ['yes', 'true', '1'].includes(String(c.required_ref || '').toLowerCase());
    const cols = [`${c.holder_key} AS k`, `COALESCE(${c.holder_field}, '') AS v`];
    if (typeField) cols.push(`COALESCE(${typeField}, '') AS t`);
    if (ident(skipField)) cols.push(`COALESCE(${skipField}, '') AS s`);
    for (const row of db.query(`SELECT ${cols.join(', ')} FROM ${c.holder_type}`).all()) {
      if (String(row.s ?? '').trim()) continue;
      let targets = fixed;
      if (typeField) {
        const targetType = String(row.t ?? '').trim();
        if (!targetType) continue;
        if (!ident(targetType) || !tableExists(db, targetType)) {
          rec.dangling.push({ holder: row.k, names: targetType, why: 'target type is not a table' });
          out.dangling++;
          continue;
        }
        targets = known(db, kind, targetType, c.target_key);
        if (targets === null) continue;
      }
      const names = (sep ? String(row.v ?? '').split(sep) : [String(row.v ?? '')]).map(name => name.trim()).filter(Boolean);
      if (!names.length && required) {
        rec.dangling.push({ holder: row.k, names: null, why: 'required and empty' });
        out.dangling++;
      }
      for (const name of names) {
        rec.checked++;
        out.checked++;
        if (!targets.has(name)) {
          rec.dangling.push({ holder: row.k, names: name, why: kind === 'column' ? 'no such column' : kind === 'table' ? 'no such table' : 'no such row' });
          out.dangling++;
        }
      }
    }
    out.references.push(rec);
  }
  return out;
}

export function report(result, json = false) {
  if (json) return JSON.stringify(result, null, 2);
  if (result.error) return `reference check: ${result.error}`;
  const lines = [];
  for (const ref of result.references) {
    const state = ref.undeclarable ? 'UNDECLARABLE' : ref.dangling.length ? `${ref.dangling.length} DANGLING` : 'ok';
    lines.push(`${ref.reference.padEnd(32)} ${ref.holder.padEnd(38)} -> ${ref.target.padEnd(34)} ${String(ref.checked).padStart(4)} ${state}`);
    if (ref.undeclarable) lines.push(`    ${ref.undeclarable}`);
    for (const item of ref.dangling.slice(0, 8)) lines.push(`    ${item.holder} -> ${JSON.stringify(item.names)}  (${item.why})`);
  }
  lines.push('', `${result.checked} references resolved, ${result.dangling} dangling, ${result.undeclarable} undeclarable`);
  return lines.join('\n');
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const path = args.includes('--db') ? args[args.indexOf('--db') + 1] : process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db';
  let db;
  try {
    db = new Database(path, { readonly: true });
    const result = check(db);
    console.log(report(result, args.includes('--json')));
    if (result.error || result.dangling || result.undeclarable) process.exitCode = 1;
  } catch (error) { console.error(`reference-check: ${error.message}`); process.exitCode = 1; }
  finally { db?.close(); }
}
