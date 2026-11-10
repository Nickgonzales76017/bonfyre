import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { OBSERVED, DERIVED, EFFECT, ensureSchema, record, originOf, inputsOf, causedByUs, byOrigin, provenance } from '../occurrence-origin.mjs';

const subject = 'live:neon-june-2026-09-04';
const db = () => { const connection = new Database(':memory:'); ensureSchema(connection); return connection; };
function night(connection) {
  record(connection, { occurrenceId: 'utt:18m', origin: OBSERVED, subject, at: '2026-09-04T21:18:00Z', source: 'audio:reno-s-mic' });
  record(connection, { occurrenceId: 'foreign:trees-calendar', origin: OBSERVED, subject, source: 'https://treesdallas.com/calendar' });
  record(connection, { occurrenceId: 'claim:trees-friday', origin: DERIVED, subject, derivedFrom: ['utt:18m', 'foreign:trees-calendar'] });
  record(connection, { occurrenceId: 'publish:clip-1', origin: EFFECT, subject, authority: 'publish:clip' });
}

test('the same spine distinguishes watching, derivation and authorized action', () => {
  const connection = db(); night(connection);
  expect(originOf(connection, 'utt:18m')).toBe(OBSERVED);
  expect(originOf(connection, 'claim:trees-friday')).toBe(DERIVED);
  expect(originOf(connection, 'publish:clip-1')).toBe(EFFECT);
  expect(causedByUs(connection, 'utt:18m')).toBe(false);
  expect(causedByUs(connection, 'claim:trees-friday')).toBe(false);
  expect(causedByUs(connection, 'publish:clip-1')).toBe(true);
  expect(byOrigin(connection, subject)).toEqual({ observed: ['foreign:trees-calendar', 'utt:18m'], derived: ['claim:trees-friday'], effect: ['publish:clip-1'] });
  expect(connection.query("SELECT authority FROM occurrence_origin WHERE occurrence_id='publish:clip-1'").get().authority).toBe('publish:clip');
  connection.close();
});

test('origin refusals prevent observations and derivations from laundering authority', () => {
  const connection = db();
  expect(() => record(connection, { occurrenceId: 'o', origin: OBSERVED, subject })).toThrow('no provenance');
  expect(() => record(connection, { occurrenceId: 'o', origin: OBSERVED, subject, source: 'mic', authority: 'observe' })).toThrow('nobody exercised any');
  expect(() => record(connection, { occurrenceId: 'd', origin: DERIVED, subject })).toThrow("occurrence's clothes");
  expect(() => record(connection, { occurrenceId: 'd', origin: DERIVED, subject, derivedFrom: ['o'], authority: 'derive' })).toThrow('deriving is not acting');
  expect(() => record(connection, { occurrenceId: 'e', origin: EFFECT, subject })).toThrow('answerable');
  expect(() => record(connection, { occurrenceId: 'raw', origin: OBSERVED, subject: '', source: 'mic' })).toThrow('not addressed');
  expect(() => record(connection, { occurrenceId: 'unknown', origin: 'happened', subject })).toThrow('unknown origin');
  connection.close();
});

test('provenance bottoms out at observed roots, not effects, and duplicate origin cannot replace one', () => {
  const connection = db(); night(connection);
  expect(provenance(connection, 'claim:trees-friday')).toEqual(['foreign:trees-calendar', 'utt:18m']);
  expect(provenance(connection, 'utt:18m')).toEqual(['utt:18m']);
  record(connection, { occurrenceId: 'd2', origin: DERIVED, subject, derivedFrom: ['claim:trees-friday'] });
  expect(provenance(connection, 'd2')).toEqual(['foreign:trees-calendar', 'utt:18m']);
  record(connection, { occurrenceId: 'from-effect', origin: DERIVED, subject, derivedFrom: ['publish:clip-1'] });
  expect(provenance(connection, 'from-effect')).toEqual([]);
  expect(inputsOf(connection, 'from-effect')).toEqual(['publish:clip-1']);
  expect(() => record(connection, { occurrenceId: 'utt:18m', origin: EFFECT, subject, authority: 'publish' })).toThrow('already on the spine');
  expect(originOf(connection, 'utt:18m')).toBe(OBSERVED);
  connection.close();
});
