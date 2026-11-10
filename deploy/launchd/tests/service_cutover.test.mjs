import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { Database } from 'bun:sqlite';

// Derived by executing only the SCHEMA string from installed legacy
// recursive_store.py in an in-memory SQLite database (never Python or the
// production DB). Source SHA256:
// e76a165f5df61560409cdbee0d4a40aeaf79ed4fce8df4ad3efac6dc31465bf8
const LEGACY_TABLE_SHAPE_SHA256 = 'c00506aab3a6281e2fbd1997c503bc23adb756ed25b2104a852fb7fca9becee9';

test('recursive schema retains every legacy table, column, foreign key and unique constraint for configuration rollback', () => {
  const db = new Database(':memory:');
  try {
    db.exec(readFileSync(new URL('../../../integrations/mcp/recursive_schema.sql', import.meta.url), 'utf8'));
    const tables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
      .all().map(row => row.name);
    expect(tables).toHaveLength(16);
    const shape = tables.map(name => ({
      name,
      columns: db.query(`PRAGMA table_info(${name})`).all()
        .map(({ name, type, notnull, dflt_value, pk }) => ({ name, type, notnull, dflt_value, pk })),
      foreign: db.query(`PRAGMA foreign_key_list(${name})`).all()
        .map(({ table, from, to, on_update, on_delete }) => ({ table, from, to, on_update, on_delete })),
      unique: db.query(`PRAGMA index_list(${name})`).all().filter(row => row.origin !== 'c')
        .map(row => ({ unique: row.unique, origin: row.origin, partial: row.partial,
          columns: db.query(`PRAGMA index_info(${row.name})`).all().map(index => index.name) }))
        .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
    }));
    expect(createHash('sha256').update(JSON.stringify(shape)).digest('hex')).toBe(LEGACY_TABLE_SHAPE_SHA256);
    expect(db.query("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name")
      .all().map(row => row.name)).toEqual(['recursive_dependency_source', 'recursive_flux_stream_seq']);
  } finally { db.close(); }
});
