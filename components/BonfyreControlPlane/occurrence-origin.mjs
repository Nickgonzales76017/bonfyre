// In-process binding to the occurrence_origin table owned by bf_occurrence_origin.
// The native C owner and this Bun facade share the same schema and refusal laws.
export const OBSERVED = 'observed';
export const DERIVED = 'derived';
export const EFFECT = 'effect';
export const ORIGINS = Object.freeze([OBSERVED, DERIVED, EFFECT]);
const kinds = new Set(ORIGINS);

export class OriginError extends Error {}

export function ensureSchema(db) {
  db.exec("CREATE TABLE IF NOT EXISTS occurrence_origin(occurrence_id TEXT PRIMARY KEY,origin TEXT NOT NULL,subject TEXT NOT NULL,at TEXT NOT NULL DEFAULT '',source TEXT NOT NULL DEFAULT '',authority TEXT NOT NULL DEFAULT '',derived_from TEXT NOT NULL DEFAULT '')");
}

export function record(db, { occurrenceId, origin, subject, at = '', source = '', authority = '', derivedFrom = [] }) {
  if (!kinds.has(origin)) throw new OriginError(`unknown origin ${origin}; expected one of ${ORIGINS.join(', ')}`);
  if (!subject) throw new OriginError('an occurrence addresses a subject or it is not addressed');
  if (!occurrenceId || String(occurrenceId).length >= 256) throw new OriginError('invalid occurrence identifier');
  if (origin === OBSERVED) {
    if (!source) throw new OriginError('an observed occurrence names the source it was observed from; without one it is an assertion with no provenance');
    if (authority) throw new OriginError('an observed occurrence carries no authority: nobody exercised any, and recording one would make watching look like acting');
  } else if (origin === DERIVED) {
    if (!Array.isArray(derivedFrom) || !derivedFrom.length || derivedFrom.length > 64 || derivedFrom.some(id => typeof id !== 'string' || !id || id.includes(',') || id.length >= 256)) throw new OriginError("a derived occurrence names valid inputs; without inputs it is a claim wearing an occurrence's clothes");
    if (authority) throw new OriginError('a derived occurrence carries no authority: deriving is not acting');
  } else if (!authority) throw new OriginError('an effect occurrence names the authority it happened under; an act nobody is answerable for is what this estate refuses');
  ensureSchema(db);
  try { db.query('INSERT INTO occurrence_origin(occurrence_id,origin,subject,at,source,authority,derived_from) VALUES(?,?,?,?,?,?,?)').run(occurrenceId, origin, subject, at, source, authority, origin === DERIVED ? derivedFrom.join(',') : ''); }
  catch (error) { if (error.code === 'SQLITE_CONSTRAINT_PRIMARYKEY' || error.code === 'SQLITE_CONSTRAINT') throw new OriginError(`occurrence ${occurrenceId} already on the spine`); throw error; }
}

export function originOf(db, id) { ensureSchema(db); return db.query('SELECT origin FROM occurrence_origin WHERE occurrence_id=?').get(id)?.origin ?? null; }
export function inputsOf(db, id) {
  ensureSchema(db);
  const row = db.query('SELECT derived_from FROM occurrence_origin WHERE occurrence_id=?').get(id);
  if (!row) throw new OriginError(`unknown occurrence ${id}`);
  return row.derived_from.split(',').filter(Boolean);
}
export function causedByUs(db, id) { return originOf(db, id) === EFFECT; }
export function byOrigin(db, subject) {
  ensureSchema(db);
  const out = { [OBSERVED]: [], [DERIVED]: [], [EFFECT]: [] };
  for (const row of db.query('SELECT occurrence_id,origin FROM occurrence_origin WHERE subject=? ORDER BY occurrence_id').all(subject)) {
    if (!out[row.origin]) throw new OriginError(`unknown stored origin ${row.origin}`);
    out[row.origin].push(row.occurrence_id);
  }
  return out;
}
export function provenance(db, id) {
  ensureSchema(db);
  const roots = new Set(), seen = new Set(), frontier = [id];
  for (let depth = 0; frontier.length && depth < 64; depth++) {
    const next = [];
    for (const current of frontier) {
      if (seen.has(current)) continue;
      seen.add(current);
      const kind = originOf(db, current);
      if (kind === OBSERVED) roots.add(current);
      else if (kind === DERIVED) next.push(...inputsOf(db, current));
    }
    frontier.splice(0, frontier.length, ...next);
  }
  return [...roots].sort();
}
