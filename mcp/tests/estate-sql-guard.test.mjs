import { expect, test } from 'bun:test';
import { admitEstateSql } from '../estate-sql-guard.mjs';

test('estate read gate admits owner views and derived CTEs', () => {
  expect(admitEstateSql('SELECT count(*) FROM observation;').sql).toBe('SELECT count(*) FROM observation');
  expect(admitEstateSql("WITH p AS (SELECT src_id FROM observation) SELECT count(*) FROM p JOIN page ON p.src_id=page.src_id").sql).toContain('JOIN page');
  expect(admitEstateSql("SELECT json_extract_string(observed,'$.City') FROM observation").sql).toContain('City');
});

test('estate read gate rejects writes, external readers, comments and side channels', () => {
  for (const sql of [
    'UPDATE observation SET src_id=1', 'SELECT 1; DROP TABLE observation',
    'SELECT * FROM read_csv_auto("/etc/passwd")', "SELECT read_text('/etc/passwd')",
    "SELECT * FROM '/etc/passwd'", 'SELECT * FROM observation, sqlite_master',
    'SELECT * FROM information_schema.tables', 'SELECT getenv(\'TOKEN\')',
    'SELECT 1 -- hidden', 'SELECT 1 /* hidden */',
  ]) expect(admitEstateSql(sql).error).toBeTruthy();
});
