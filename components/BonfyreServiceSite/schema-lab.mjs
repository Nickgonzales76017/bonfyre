// Changing a SQLite database's structure without losing its rows. A database
// is read into a model (tables, columns, keys, indexes, row counts); changes
// are staged as a list; the list becomes a migration that follows SQLite's own
// procedure for changes ALTER TABLE cannot make (build the new table, copy,
// drop, rename, check foreign keys); and the migration is tried on a copy,
// reporting rows before and after, broken references and rows a change would
// reject, before anybody runs it on the real file.
//
// Works over any handle with q(sql, params) -> rows and run(sql): sql.js in
// the browser, bun:sqlite in tests.

export const TYPES = ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC'];
const ident = name => '"' + String(name).replace(/"/g, '""') + '"';
const lit = v => v == null ? 'NULL' : typeof v === 'number' ? String(v) : "'" + String(v).replace(/'/g, "''") + "'";

// ------------------------------------------------------------ reading a database

// CHECK clauses are kept from the table's own definition so a rebuild does not drop them.
export function checksOf(sql) {
  const out = [], text = String(sql ?? '');
  for (let at = text.search(/\bCHECK\s*\(/i); at >= 0; ) {
    let i = text.indexOf('(', at), depth = 0, j = i;
    for (; j < text.length; j++) { if (text[j] === '(') depth++; else if (text[j] === ')' && --depth === 0) break; }
    out.push(text.slice(at, j + 1));
    const rest = text.slice(j + 1).search(/\bCHECK\s*\(/i); at = rest < 0 ? -1 : j + 1 + rest;
  }
  return out;
}

export function inspect(db) {
  const tables = db.q("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
  return { tables: tables.map(t => {
    const columns = db.q(`PRAGMA table_xinfo(${ident(t.name)})`).filter(c => !c.hidden).map(c => ({ name: c.name, type: (c.type || '').toUpperCase(), notnull: !!c.notnull, default: c.dflt_value, pk: c.pk }));
    const fks = []; for (const f of db.q(`PRAGMA foreign_key_list(${ident(t.name)})`)) { const g = fks.find(x => x.id === f.id); if (g) { g.from.push(f.from); g.to.push(f.to); } else fks.push({ id: f.id, from: [f.from], table: f.table, to: [f.to ?? null], onDelete: f.on_delete, onUpdate: f.on_update }); }
    const indexes = db.q(`PRAGMA index_list(${ident(t.name)})`).filter(i => i.origin !== 'pk').map(i => ({ name: i.name, unique: !!i.unique, origin: i.origin, columns: db.q(`PRAGMA index_info(${ident(i.name)})`).map(x => x.name),
      sql: db.q("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", [i.name])[0]?.sql ?? null }));
    return { name: t.name, sql: t.sql, columns, fks: fks.map(({ id, ...f }) => f), indexes, checks: checksOf(t.sql), autoincrement: /AUTOINCREMENT/i.test(t.sql), withoutRowid: /WITHOUT\s+ROWID/i.test(t.sql),
      rows: db.q(`SELECT COUNT(*) AS n FROM ${ident(t.name)}`)[0].n };
  }) };
}

// ------------------------------------------------------------ staged changes

// Each change is applied to a copy of the model; a change that cannot apply says why.
export function applyChange(model, ch) {
  const m = structuredClone(model), t = m.tables.find(x => x.name === ch.table);
  const need = (ok, why) => { if (!ok) throw Error(why); };
  if (ch.kind !== 'addTable') need(t, `There is no table ${ch.table}.`);
  const col = n => t.columns.find(c => c.name === n);
  switch (ch.kind) {
    case 'addTable': need(!m.tables.some(x => x.name === ch.name), `${ch.name} already exists.`); need(ch.columns?.length, 'A new table needs a column.');
      m.tables.push({ name: ch.name, columns: ch.columns.map(c => ({ notnull: false, default: null, pk: 0, ...c })), fks: [], indexes: [], checks: [], rows: 0, created: true }); break;
    case 'renameTable': need(!m.tables.some(x => x.name === ch.to), `${ch.to} already exists.`);
      // Tables that refer to it name it in their own definition, so they are rebuilt too.
      for (const o of m.tables) for (const f of o.fks) if (f.table === t.name) { f.table = ch.to; if (o !== t) o.rebuild = true; } t.renamedFrom ??= t.name; t.name = ch.to; break;
    case 'dropTable': need(!m.tables.some(o => o !== t && o.fks.some(f => f.table === t.name)), `Other tables refer to ${t.name}; remove those references first.`); m.tables = m.tables.filter(x => x !== t); m.dropped = [...(m.dropped ?? []), t.renamedFrom ?? t.name]; break;
    case 'addColumn': need(!col(ch.name), `${t.name} already has ${ch.name}.`); need(!ch.notnull || ch.default != null, 'A required new column needs a value for the rows already there.');
      t.columns.push({ name: ch.name, type: ch.type ?? 'TEXT', notnull: !!ch.notnull, default: ch.default == null ? null : lit(ch.default), pk: 0, added: true }); break;
    case 'dropColumn': need(col(ch.column), `${t.name} has no ${ch.column}.`); need(!col(ch.column).pk, 'A primary key column cannot be removed here.');
      need(!m.tables.some(o => o.fks.some(f => f.table === t.name && f.to.includes(ch.column))), `Other tables refer to ${t.name}.${ch.column}.`);
      t.columns = t.columns.filter(c => c.name !== ch.column); t.fks = t.fks.filter(f => !f.from.includes(ch.column)); t.indexes = t.indexes.filter(i => !i.columns.includes(ch.column));
      t.checks = t.checks.filter(k => !new RegExp(`\\b${ch.column.replace(/\W/g, '\\$&')}\\b`).test(k)); break;
    case 'renameColumn': need(col(ch.column), `${t.name} has no ${ch.column}.`); need(!col(ch.to), `${t.name} already has ${ch.to}.`);
      Object.assign(col(ch.column), { name: ch.to, renamedFrom: col(ch.column).renamedFrom ?? ch.column });
      for (const f of t.fks) f.from = f.from.map(x => x === ch.column ? ch.to : x); for (const i of t.indexes) i.columns = i.columns.map(x => x === ch.column ? ch.to : x);
      for (const o of m.tables) for (const f of o.fks) if (f.table === t.name && f.to.includes(ch.column)) { f.to = f.to.map(x => x === ch.column ? ch.to : x); if (o !== t) o.rebuild = true; }
      t.checks = t.checks.map(k => k.replace(new RegExp(`\\b${ch.column.replace(/\W/g, '\\$&')}\\b`, 'g'), ch.to)); break;
    case 'changeType': need(col(ch.column), `${t.name} has no ${ch.column}.`); need(TYPES.includes(ch.type), `A type is one of ${TYPES.join(', ')}.`); col(ch.column).type = ch.type; col(ch.column).retyped = true; break;
    case 'setRequired': need(col(ch.column), `${t.name} has no ${ch.column}.`); col(ch.column).notnull = !!ch.required; if (ch.required) col(ch.column).fill = ch.fill ?? null; break;
    case 'addForeignKey': { need(col(ch.column), `${t.name} has no ${ch.column}.`); const r = m.tables.find(x => x.name === ch.refTable); need(r, `There is no table ${ch.refTable}.`);
      need(r.columns.some(c => c.name === ch.refColumn), `${ch.refTable} has no ${ch.refColumn}.`);
      need((r.columns.find(c => c.name === ch.refColumn).pk && r.columns.filter(c => c.pk).length === 1) || r.indexes.some(i => i.unique && i.columns.length === 1 && i.columns[0] === ch.refColumn), `${ch.refTable}.${ch.refColumn} must be its key or unique to be referred to.`);
      t.fks.push({ from: [ch.column], table: ch.refTable, to: [ch.refColumn], onDelete: ch.onDelete ?? 'NO ACTION', onUpdate: 'NO ACTION', added: true }); break; }
    case 'dropForeignKey': t.fks = t.fks.filter(f => !(f.from.join() === ch.column && f.table === ch.refTable)); break;
    case 'addIndex': need(ch.columns?.every(c => col(c)), 'Index only columns the table has.');
      t.indexes.push({ name: ch.name ?? `idx_${t.name}_${ch.columns.join('_')}`.replace(/\W/g, '_'), unique: !!ch.unique, origin: 'c', columns: ch.columns, sql: null, added: true }); break;
    case 'dropIndex': t.indexes = t.indexes.filter(i => i.name !== ch.name); break;
    default: throw Error(`Unknown change ${ch.kind}.`);
  }
  if (t && ch.kind !== 'addTable' && ch.kind !== 'dropTable' && ch.kind !== 'addIndex' && ch.kind !== 'dropIndex') t.rebuild = true;
  if (t && (ch.kind === 'addIndex' || ch.kind === 'dropIndex')) t.reindex = true;
  return m;
}
export function stage(model, changes) { let m = model; for (const ch of changes) m = applyChange(m, ch); return m; }

// ------------------------------------------------------------ the migration

function createSql(t, name) {
  const single = t.columns.filter(c => c.pk).length === 1;
  const cols = t.columns.map(c => [ident(c.name), c.type, single && c.pk ? 'PRIMARY KEY' + (t.autoincrement && c.type === 'INTEGER' ? ' AUTOINCREMENT' : '') : '', c.notnull ? 'NOT NULL' : '', c.default != null ? `DEFAULT ${c.default}` : ''].filter(Boolean).join(' '));
  const pk = t.columns.filter(c => c.pk).sort((a, b) => a.pk - b.pk);
  if (pk.length > 1) cols.push(`PRIMARY KEY (${pk.map(c => ident(c.name)).join(', ')})`);
  for (const i of t.indexes.filter(i => i.origin === 'u')) cols.push(`UNIQUE (${i.columns.map(ident).join(', ')})`);
  for (const f of t.fks) cols.push(`FOREIGN KEY (${f.from.map(ident).join(', ')}) REFERENCES ${ident(f.table)}${f.to[0] == null ? '' : ` (${f.to.map(ident).join(', ')})`}${f.onDelete && f.onDelete !== 'NO ACTION' ? ' ON DELETE ' + f.onDelete : ''}${f.onUpdate && f.onUpdate !== 'NO ACTION' ? ' ON UPDATE ' + f.onUpdate : ''}`);
  cols.push(...t.checks);
  return `CREATE TABLE ${ident(name)} (\n  ${cols.join(',\n  ')}\n)${t.withoutRowid ? ' WITHOUT ROWID' : ''};`;
}
const castExpr = (c, from) => { const src = ident(from); const filled = c.fill != null ? `COALESCE(${src}, ${lit(c.fill)})` : src;
  return c.retyped ? `CAST(${filled} AS ${c.type})` : filled; };

// The statements, in order. Simple changes use ALTER TABLE; anything else rebuilds the table the way SQLite documents.
export function migration(before, after) {
  const out = ['PRAGMA foreign_keys = OFF;', 'BEGIN;'], old = new Map(before.tables.map(t => [t.name, t]));
  for (const t of after.tables.filter(t => t.created)) { out.push(createSql(t, t.name)); for (const i of t.indexes) out.push(indexSql(t.name, i)); }
  for (const name of after.dropped ?? []) out.push(`DROP TABLE ${ident(name)};`);
  for (const t of after.tables.filter(t => !t.created)) {
    const was = old.get(t.renamedFrom ?? t.name);
    if (t.rebuild) {
      const temp = `${t.name}__new`, kept = t.columns.filter(c => !c.added);
      out.push(createSql(t, temp), `INSERT INTO ${ident(temp)} (${kept.map(c => ident(c.name)).join(', ')})\n  SELECT ${kept.map(c => castExpr(c, c.renamedFrom ?? c.name)).join(', ')} FROM ${ident(was.name)};`,
        `DROP TABLE ${ident(was.name)};`, `ALTER TABLE ${ident(temp)} RENAME TO ${ident(t.name)};`);
      for (const i of t.indexes.filter(i => i.origin !== 'u')) out.push(indexSql(t.name, i));
    } else if (t.reindex) {
      for (const i of was.indexes.filter(i => i.origin === 'c' && !t.indexes.some(j => j.name === i.name))) out.push(`DROP INDEX ${ident(i.name)};`);
      for (const i of t.indexes.filter(i => i.added)) out.push(indexSql(t.name, i));
    }
  }
  out.push('PRAGMA foreign_key_check;', 'COMMIT;', 'PRAGMA foreign_keys = ON;');
  return out;
}
const indexSql = (table, i) => i.sql && !i.added ? i.sql.replace(/;?$/, ';') : `CREATE ${i.unique ? 'UNIQUE ' : ''}INDEX ${ident(i.name)} ON ${ident(table)} (${i.columns.map(ident).join(', ')});`;

// ------------------------------------------------------------ before running it

// What the data says about each change: rows a required column would reject, values a new type would change,
// references with nothing to point at, duplicates a unique index would refuse. Each with a few examples.
export function precheck(db, model, changes) {
  const issues = []; let m = model;
  for (const ch of changes) {
    const t = m.tables.find(x => x.name === ch.table), from = t ? ident(t.renamedFrom && !t.created ? t.renamedFrom : t.name) : null;
    const colName = n => t?.columns.find(c => c.name === n)?.renamedFrom ?? n;
    try {
      if (ch.kind === 'setRequired' && ch.required) { const n = db.q(`SELECT COUNT(*) n FROM ${from} WHERE ${ident(colName(ch.column))} IS NULL`)[0].n;
        if (n) issues.push({ change: ch, rows: n, blocking: ch.fill == null, words: ch.fill == null ? `${n.toLocaleString()} rows have no ${ch.column}; give a value to fill them or the migration stops.` : `${n.toLocaleString()} empty ${ch.column} values will become ${lit(ch.fill)}.` }); }
      if (ch.kind === 'changeType' && (ch.type === 'INTEGER' || ch.type === 'REAL')) {
        const c = ident(colName(ch.column)), bad = `${c} IS NOT NULL AND (CAST(${c} AS ${ch.type}) || '') <> (${c} || '') AND NOT (typeof(${c}) IN ('integer','real') AND ${ch.type === 'REAL' ? 1 : `${c} = CAST(${c} AS INTEGER)`})`;
        const n = db.q(`SELECT COUNT(*) n FROM ${from} WHERE ${bad}`)[0].n;
        if (n) issues.push({ change: ch, rows: n, blocking: false, examples: db.q(`SELECT ${c} AS v FROM ${from} WHERE ${bad} LIMIT 5`).map(r => r.v), words: `${n.toLocaleString()} values of ${ch.column} would change as ${ch.type}, for example ${db.q(`SELECT ${c} AS v, CAST(${c} AS ${ch.type}) AS w FROM ${from} WHERE ${bad} LIMIT 1`).map(r => `"${r.v}" becoming ${r.w}`)[0]}.` }); }
      if (ch.kind === 'addForeignKey') { const r = m.tables.find(x => x.name === ch.refTable), rf = ident(r.renamedFrom && !r.created ? r.renamedFrom : r.name), c = ident(colName(ch.column));
        const orphan = `${c} IS NOT NULL AND ${c} NOT IN (SELECT ${ident(r.columns.find(x => x.name === ch.refColumn).renamedFrom ?? ch.refColumn)} FROM ${rf})`;
        const n = db.q(`SELECT COUNT(*) n FROM ${from} WHERE ${orphan}`)[0].n;
        if (n) issues.push({ change: ch, rows: n, blocking: true, examples: db.q(`SELECT ${c} AS v FROM ${from} WHERE ${orphan} LIMIT 5`).map(r => r.v), words: `${n.toLocaleString()} rows refer to a ${ch.refTable} that does not exist; the foreign key check will list them.` }); }
      if (ch.kind === 'addIndex' && ch.unique) { const cols = ch.columns.map(x => ident(colName(x))).join(', ');
        const n = db.q(`SELECT COUNT(*) n FROM (SELECT 1 FROM ${from} GROUP BY ${cols} HAVING COUNT(*) > 1)`)[0].n;
        if (n) issues.push({ change: ch, rows: n, blocking: true, words: `${n.toLocaleString()} values of ${ch.columns.join(' + ')} appear more than once; a unique index refuses them.` }); }
      if (ch.kind === 'dropColumn') { const n = db.q(`SELECT COUNT(*) n FROM ${from} WHERE ${ident(colName(ch.column))} IS NOT NULL`)[0].n; if (n) issues.push({ change: ch, rows: n, blocking: false, words: `${n.toLocaleString()} values in ${ch.column} will be gone.` }); }
    } catch (e) { issues.push({ change: ch, rows: 0, blocking: true, words: e.message }); }
    try { m = applyChange(m, ch); } catch (e) { issues.push({ change: ch, rows: 0, blocking: true, words: e.message }); break; }
  }
  return issues;
}

// Run the migration on a copy. Counts are compared table by table; nothing is kept if a step fails.
export function trial(copy, before, after, sql) {
  const rowsBefore = Object.fromEntries(before.tables.map(t => [t.name, t.rows]));
  const statements = sql.filter(s => !/^PRAGMA foreign_key_check/.test(s));
  let step = null;
  try { for (const s of statements) { step = s; copy.run(s); } }
  catch (e) { try { copy.run('ROLLBACK;'); } catch {} copy.run('PRAGMA foreign_keys = ON;'); return { ok: false, error: e.message, step }; }
  const broken = copy.q('PRAGMA foreign_key_check'), integrity = copy.q('PRAGMA integrity_check').map(r => Object.values(r)[0]);
  const now = inspect(copy);
  const tables = now.tables.map(t => { const was = after.tables.find(x => x.name === t.name), from = was?.renamedFrom ?? t.name; return { name: t.name, before: was?.created ? 0 : rowsBefore[from] ?? 0, after: t.rows }; });
  return { ok: true, tables, lost: tables.filter(t => t.after < t.before), broken: broken.slice(0, 50), brokenCount: broken.length, integrity, model: now };
}

// ------------------------------------------------------------ handles

export const sqlJsHandle = db => ({
  q(sql, params = []) { const st = db.prepare(sql); try { st.bind(params); const out = []; while (st.step()) out.push(st.getAsObject()); return out; } finally { st.free(); } },
  run(sql) { db.exec(sql); },
});
export const bunHandle = db => ({ q: (sql, params = []) => db.query(sql).all(...params), run: sql => db.exec(sql) });
