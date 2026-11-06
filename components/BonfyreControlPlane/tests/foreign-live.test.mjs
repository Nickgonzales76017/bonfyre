import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { ClaimLedger, KIND_TIER } from '../live-semantic-track.mjs';
import { OBSERVED, EFFECT, originOf, causedByUs, byOrigin, record } from '../occurrence-origin.mjs';
import { ForeignLiveSource, RightsError, IngestError, RIGHTS, OBSERVABLE, RETAINABLE,
  DERIVABLE, REPUBLISHABLE, ingest, rightsGap } from '../foreign-live.mjs';

const MIN = 60_000;
const SUBJECT = 'foreign:twitch:neon-june';
const source = (...granted) => new ForeignLiveSource({ twinId: 'twin:twitch-neon-june',
  platform: 'twitch', url: 'https://twitch.tv/neonjune', granted,
  basis: 'platform terms, observed 2026-09-04' });

test('four rights are individually granted; no forward inference', () => {
  const visible = source(OBSERVABLE);
  const opened = ingest(visible);
  expect(() => opened.retain(0, 'caption', 'hey Deep Ellum')).toThrow("'retainable'");
  expect(() => opened.retain(0, 'chat', 'hello')).toThrow("['observable']");
  expect(opened.track.events).toEqual([]);
  const kept = ingest(source(OBSERVABLE, RETAINABLE));
  kept.retain(18 * MIN, 'caption', "we're playing Trees again Friday");
  expect(() => kept.deriveClaim(new ClaimLedger(), 18 * MIN,
    { claimId: 'c1', asserter: 'Neon June', subject: 'event:trees-friday', assertion: 'plays Trees Friday' })).toThrow("'derivable'");
  expect(kept.retained).toHaveLength(1);
  const derived = ingest(source(OBSERVABLE, RETAINABLE, DERIVABLE));
  derived.retain(0, 'caption', 'hey Deep Ellum');
  derived.deriveClaim(new ClaimLedger(), 0,
    { claimId: 'c1', asserter: 'Neon June', subject: 's', assertion: 'a' });
  expect(() => derived.republishableEvents()).toThrow("'republishable'");
  expect(() => ingest(source())).toThrow('open an ingest');
  expect(() => ingest(source())).toThrow('nothing');
  expect(rightsGap(visible, REPUBLISHABLE)).toEqual([RETAINABLE, DERIVABLE, REPUBLISHABLE]);
  expect(rightsGap(derived.source, DERIVABLE)).toEqual([]);
  expect(rightsGap(derived.source, REPUBLISHABLE)).toEqual([REPUBLISHABLE]);
  expect(() => derived.source.holds('scraping')).toThrow(RightsError);
  expect(() => rightsGap(derived.source, 'scraping')).toThrow(RightsError);
  expect(RIGHTS).toEqual([OBSERVABLE, RETAINABLE, DERIVABLE, REPUBLISHABLE]);
});

test('foreign material shares the native semantic grammar and retention lattice', () => {
  const stream = ingest(source(OBSERVABLE, RETAINABLE, DERIVABLE));
  stream.retain(0, 'caption', 'hey Deep Ellum', { speaker: 'Neon June' });
  stream.retain(2 * MIN, 'chat', 'where is everybody');
  stream.retain(3 * MIN, 'title', "Neon June live from Reno's");
  expect(stream.track.events.map(event => event.kind)).toEqual(['utterance', 'chat', 'topic']);
  expect(stream.track.events[0].tier()).toBe(KIND_TIER.utterance);
  expect(stream.track.events[2].tier()).toBe(KIND_TIER.topic);
  expect(stream.track.events[0].source).toBe('foreign:twitch');
  expect(() => stream.retain(0, 'superchat', '£5')).toThrow(IngestError);
  const chat = ingest(source(OBSERVABLE, RETAINABLE));
  chat.retain(0, 'chat', 'hello').retain(1, 'caption', "we're playing Trees Friday");
  const [kept, expired] = chat.track.cool();
  expect(kept).toEqual([]);
  expect(expired).toHaveLength(2);
});

test('ingested claims belong to the foreign speaker and still need corroboration', () => {
  const stream = ingest(source(OBSERVABLE, RETAINABLE, DERIVABLE));
  const ledger = new ClaimLedger();
  stream.retain(18 * MIN, 'caption', "we're playing Trees again Friday");
  const claim = stream.deriveClaim(ledger, 18 * MIN,
    { claimId: 'c1', asserter: 'Neon June', subject: 'event:trees-friday', assertion: 'plays Friday' });
  expect(claim.asserter).toBe('Neon June');
  expect(() => stream.deriveClaim(new ClaimLedger(), 0,
    { claimId: 'bad', asserter: stream.source.twinId, subject: 's', assertion: 'a' })).toThrow('watching look like claiming');
  expect(ledger.crystallizable()).toEqual([]);
  ledger.corroborate('c1', { source: 'foreign:trees-calendar', kind: 'EventListingObservation' });
  ledger.corroborate('c1', { source: 'foreign:dallas-observer', kind: 'ForeignOccurrence' });
  expect(ledger.crystallizable()).toEqual(['c1']);
  expect(ledger.proven('c1')).toBe(false);
});

test('republishing requires an explicit fourth grant and grant basis remains visible', () => {
  const stream = ingest(source(...RIGHTS));
  stream.retain(0, 'caption', 'hey');
  expect(stream.republishableEvents()).toHaveLength(1);
  expect(stream.source.basis).toBe('platform terms, observed 2026-09-04');
});

test('retained foreign material is filed as observed, never as an effect', () => {
  const db = new Database(':memory:');
  try {
    const stream = ingest(source(OBSERVABLE, RETAINABLE, DERIVABLE));
    stream.retain(0, 'caption', 'hey').retain(2 * MIN, 'chat', 'hello');
    const filed = stream.fileObservations(db, { subject: SUBJECT });
    expect(filed).toHaveLength(2);
    for (const id of filed) {
      expect(originOf(db, id)).toBe(OBSERVED);
      expect(causedByUs(db, id)).toBe(false);
    }
    expect(db.query('SELECT source FROM occurrence_origin WHERE occurrence_id=?').get(filed[0]).source)
      .toBe('twitch:https://twitch.tv/neonjune');
    record(db, { occurrenceId: 'publish:our-clip', origin: EFFECT, subject: SUBJECT,
      authority: 'publish:clip' });
    expect(byOrigin(db, SUBJECT)[OBSERVED]).toEqual([...filed].sort());
    expect(byOrigin(db, SUBJECT)[EFFECT]).toEqual(['publish:our-clip']);
  } finally { db.close(); }
});
