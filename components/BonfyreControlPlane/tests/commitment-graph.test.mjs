import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { CommitmentGraph, CommitmentError, OPEN, DISCHARGED, DELEGATED,
  VIOLATED, COMPENSATED } from '../commitment-graph.mjs';

function graph() { return new CommitmentGraph(); }
function opened(g, id = 'c1', extra = {}) {
  g.record({ commitment_id:id, committer:'venue', beneficiary:'organizer',
    resource:'room-on-friday', ...extra });
  return id;
}
function states(g, id) { return g.history(id).map(event => event[0]); }
function withGraph(run) { const g = graph(); try { run(g); } finally { g.close(); } }

test('open commitment is not delivery', () => withGraph(g => {
  const id = opened(g); expect(g.status(id)).toBe(OPEN); expect(g.delivered(id)).toBe(false);
}));
test('unknown commitment is null, not open', () => withGraph(g => {
  expect(g.status('never-recorded')).toBeNull(); expect(g.delivered('never-recorded')).toBe(false);
}));
test('delegation retains original obligation', () => withGraph(g => {
  const id = opened(g), child = g.delegate(id, {to_committer:'sub-venue', delegated_id:'c1-d'});
  expect(g.status(id)).toBe(DELEGATED); expect(g.delivered(id)).toBe(false);
  expect(g.status(child)).toBe(OPEN);
  expect(Object.fromEntries(g.outstanding('organizer'))).toEqual({c1:DELEGATED, 'c1-d':OPEN});
}));
test('delegated commitment remains violable', () => withGraph(g => {
  const id = opened(g); g.delegate(id,{to_committer:'sub-venue',delegated_id:'c1-d'});
  g.violate(id,{detail:'sub-venue never showed'}); expect(g.status(id)).toBe(VIOLATED);
}));
test('delegation chain retains original promiser', () => withGraph(g => {
  const id = opened(g); g.delegate(id,{to_committer:'sub-venue',delegated_id:'c1-d'});
  expect(g.delegationChain(id)).toEqual(['venue']);
  expect(g.delegationChain('c1-d')).toEqual(['venue','sub-venue']);
}));
test('violated commitment is still outstanding', () => withGraph(g => {
  const id = opened(g); g.violate(id,{detail:'room double-booked'});
  expect(Object.fromEntries(g.outstanding('organizer'))).toEqual({c1:VIOLATED});
}));
test('late delivery discharges and retains violation', () => withGraph(g => {
  const id = opened(g); g.violate(id,{detail:'missed the date'});
  g.discharge(id,{detail:'delivered a week late'});
  expect(g.status(id)).toBe(DISCHARGED); expect(g.wasEver(id,VIOLATED)).toBe(true);
  expect(g.outstanding('organizer')).toEqual([]);
}));
test('compensation requires a violation', () => withGraph(g => {
  const id = opened(g);
  expect(() => g.compensate(id,{detail:'refund'})).toThrow(CommitmentError);
  expect(() => g.compensate(id,{detail:'refund'})).toThrow('open');
}));
test('compensation retains visible violation', () => withGraph(g => {
  const id = opened(g); g.violate(id,{detail:'room double-booked'});
  g.compensate(id,{detail:'full refund plus a free date'});
  expect(g.status(id)).toBe(COMPENSATED); expect(g.wasEver(id,VIOLATED)).toBe(true);
  expect(states(g,id)).toEqual([OPEN,VIOLATED,COMPENSATED]);
}));
test('compensated and discharged remain distinguishable', () => withGraph(g => {
  g.record({commitment_id:'clean',committer:'venue',beneficiary:'alice',resource:'room'});
  g.discharge('clean');
  g.record({commitment_id:'messy',committer:'venue',beneficiary:'alice',resource:'room'});
  g.violate('messy'); g.compensate('messy');
  expect(g.outstanding('alice')).toEqual([]);
  expect(g.wasEver('clean',VIOLATED)).toBe(false);
  expect(g.wasEver('messy',VIOLATED)).toBe(true);
}));
test('discharged is terminal', () => withGraph(g => {
  const id = opened(g); g.discharge(id);
  for (const move of [g.violate,g.compensate])
    expect(() => move.call(g,id)).toThrow('terminal');
}));
test('compensated is terminal', () => withGraph(g => {
  const id = opened(g); g.violate(id); g.compensate(id);
  expect(() => g.discharge(id)).toThrow('terminal');
}));
test('re-recording cannot reopen obligation', () => withGraph(g => {
  const id = opened(g); g.violate(id);
  expect(() => opened(g,id)).toThrow(CommitmentError);
  expect(g.status(id)).toBe(VIOLATED); expect(g.wasEver(id,VIOLATED)).toBe(true);
}));
test('expiry is not automatic violation', () => withGraph(g => {
  const id = opened(g,'c1',{expires_at:'2026-01-01T00:00:00Z'});
  expect(g.violable('2026-06-01T00:00:00Z')).toEqual([id]);
  expect(g.status(id)).toBe(OPEN); expect(g.wasEver(id,VIOLATED)).toBe(false);
}));
test('violable excludes settled and unexpiring', () => withGraph(g => {
  const settled = opened(g,'settled',{expires_at:'2026-01-01T00:00:00Z'});
  g.discharge(settled); opened(g,'no-expiry');
  const future = opened(g,'future',{expires_at:'2099-01-01T00:00:00Z'});
  expect(g.violable('2026-06-01T00:00:00Z')).toEqual([]); expect(g.status(future)).toBe(OPEN);
}));
test('event log is append-only and complete', () => withGraph(g => {
  const id = opened(g); g.delegate(id,{to_committer:'sub',delegated_id:'d'});
  g.violate(id); g.compensate(id);
  expect(states(g,id)).toEqual([OPEN,DELEGATED,VIOLATED,COMPENSATED]);
  expect(g.history(id)).toHaveLength(4);
}));
test('enactment binds obligation to originating exchange', () => withGraph(g => {
  opened(g,'c1',{enactment:'quote-accept:2026-09-04:venue-organizer'});
  const db = new Database(g.dbPath,{readonly:true});
  try { expect(db.query('SELECT enactment FROM commitments WHERE commitment_id=?').get('c1').enactment)
    .toBe('quote-accept:2026-09-04:venue-organizer'); }
  finally { db.close(); }
}));
