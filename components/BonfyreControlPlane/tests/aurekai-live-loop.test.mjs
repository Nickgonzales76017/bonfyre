import { test, expect } from 'bun:test';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { LiveSemanticTrack, SemanticEvent, ClaimLedger } from '../live-semantic-track.mjs';
import { AuthorityGraph, AuthorityRefused } from '../protocol-authority.mjs';
import { CommitmentGraph, OPEN, DISCHARGED, VIOLATED } from '../commitment-graph.mjs';
import { get, closed, outstandingAfter } from '../protocol-library.mjs';
import { ObligationWork, queue } from '../obligation-work.mjs';

const MIN = 60_000;
const SUBJECT = 'live:neon-june-2026-09-04';
const formationActors = { asserter: 'creator:neon-june', corroborator: 'twin:foreign-observer', admitter: 'institution:aurekai' };
const sponsorActors = { creator: 'creator:neon-june', publisher: 'creator:neon-june', observer: 'twin:foreign-observer', settler: 'sponsor:red-bull' };

function stream() {
  const track = new LiveSemanticTrack(SUBJECT);
  track.say(0, "hey Deep Ellum, we're Neon June", 'Neon June');
  track.append(new SemanticEvent(2 * MIN, 'chat', 'where is everybody'));
  track.say(18 * MIN, "we're playing Trees again Friday", 'Neon June');
  track.append(new SemanticEvent(18 * MIN, 'place', 'Trees'));
  track.append(new SemanticEvent(25 * MIN, 'sponsor_mention', 'Red Bull', 'Neon June'));
  track.append(new SemanticEvent(39 * MIN, 'cta', 'tickets are in our profile', 'Neon June'));
  track.append(new SemanticEvent(41 * MIN, 'chat', 'Trees line is insane'));
  track.append(new SemanticEvent(52 * MIN, 'sponsor_mention', 'Red Bull', 'Neon June'));
  track.append(new SemanticEvent(58 * MIN, 'clip_candidate', 'friday-announcement'));
  return track;
}
function grantRoles(authority, contract, actors) {
  for (const role of contract.roles) authority.grant({
    edge_id: `${contract.name}:${role.name}`, actor: actors[role.name],
    permission: role.authority, subject: SUBJECT,
  });
}
function withGraphs(run) {
  const commitments = new CommitmentGraph();
  const authority = new AuthorityGraph();
  try { return run(commitments, authority); }
  finally { authority.close(); commitments.close(); }
}

test('attributed live claim, independent observations, native authority and durable event obligation', () => withGraphs((commitments, authority) => {
  const track = stream(), ledger = new ClaimLedger();
  expect(track.byKind('claim')).toEqual([]);
  ledger.fromUtterance(track, 18 * MIN, { claim_id: 'c1', asserter: 'Neon June', subject: 'event:trees-friday', assertion: 'Neon June plays Trees on Friday' });
  expect(ledger.crystallizable()).toEqual([]);
  ledger.corroborate('c1', { source: 'foreign:trees-calendar', kind: 'EventListingObservation' });
  ledger.corroborate('c1', { source: 'foreign:dallas-observer-listings', kind: 'ForeignOccurrence' });
  expect(ledger.crystallizable()).toEqual(['c1']);
  expect(ledger.proven('c1')).toBe(false);
  const contract = get('social', 'event-formation');
  const enact = () => authority.enactAuthorized(commitments, contract, { enactment_id: 'ef1', actors: formationActors, subject: SUBJECT });
  expect(enact).toThrow(AuthorityRefused);
  expect(commitments.status('ef1:perform_as_announced')).toBeNull();
  grantRoles(authority, contract, formationActors);
  expect(enact()).toEqual(['ef1:perform_as_announced']);
  expect(commitments.status('ef1:perform_as_announced')).toBe(OPEN);
  expect(closed(contract, commitments, 'ef1')).toBe(false);
  const db = new Database(commitments.dbPath, { readonly: true });
  try {
    expect(db.query("SELECT committer,beneficiary,enactment FROM commitments WHERE commitment_id='ef1:perform_as_announced'").get())
      .toMatchObject({ committer: 'creator:neon-june', beneficiary: 'institution:aurekai', enactment: 'ef1' });
  } finally { db.close(); }
  const work = new ObligationWork(commitments);
  const queueDb = join(commitments.tempDir, 'work.db');
  const [mission, node] = work.lower('ef1:perform_as_announced', { queue_db: queueDb });
  expect(work.workStatus('ef1:perform_as_announced')).toBe('ready');
  expect(closed(contract, commitments, 'ef1')).toBe(false);
  const claim = queue(['claim-next', 'crew', '--lease-ms', '60000'], queueDb);
  expect([claim.mission_id, claim.node_id]).toEqual([mission, node]);
  queue(['ack', mission, node, 'crew', claim.claim_token], queueDb);
  expect(work.settle('ef1:perform_as_announced')).toBe(DISCHARGED);
  expect(commitments.status('ef1:perform_as_announced')).toBe(DISCHARGED);
  expect(closed(contract, commitments, 'ef1')).toBe(true);
}));

test('a listed show not performed remains a breach after compensation', () => withGraphs((commitments, authority) => {
  const contract = get('social', 'event-formation');
  grantRoles(authority, contract, formationActors);
  authority.enactAuthorized(commitments, contract, { enactment_id: 'ef1', actors: formationActors, subject: SUBJECT });
  commitments.violate('ef1:perform_as_announced', { actor: 'institution:aurekai', detail: 'venue confirms no set' });
  expect(closed(contract, commitments, 'ef1')).toBe(false);
  commitments.compensate('ef1:perform_as_announced', { detail: 'refunds issued' });
  expect(closed(contract, commitments, 'ef1')).toBe(true);
  expect(commitments.wasEver('ef1:perform_as_announced', VIOLATED)).toBe(true);
}));

test('spoken sponsorship, externally observed clip and settlement close distinct obligations', () => withGraphs((commitments, authority) => {
  const contract = get('economic', 'sponsor-obligation');
  grantRoles(authority, contract, sponsorActors);
  const opened = authority.enactAuthorized(commitments, contract, { enactment_id: 'sp1', actors: sponsorActors, subject: SUBJECT });
  expect(opened).toEqual(['sp1:speak_the_acknowledgements', 'sp1:publish_the_clip', 'sp1:pay_on_evidence']);
  const mentions = stream().sponsorMentions('Red Bull');
  expect(mentions.map(event => event.at_ms)).toEqual([25 * MIN, 52 * MIN]);
  commitments.discharge(opened[0], { actor: 'live-semantic-track', detail: `acknowledged at ${mentions.map(event => event.at_ms)}` });
  expect(closed(contract, commitments, 'sp1')).toBe(false);
  expect(new Set(outstandingAfter(contract, commitments, 'sp1').map(([id]) => id))).toEqual(new Set(opened.slice(1)));
  commitments.discharge(opened[1], { actor: 'twin:foreign-observer', detail: 'clip observed on platform' });
  expect(outstandingAfter(contract, commitments, 'sp1')).toEqual([[opened[2], OPEN]]);
  commitments.discharge(opened[2], { actor: 'sponsor:red-bull' });
  expect(closed(contract, commitments, 'sp1')).toBe(true);
}));

test('one sponsor acknowledgement cannot close a two-acknowledgement contract', () => withGraphs((commitments, authority) => {
  const thin = new LiveSemanticTrack(SUBJECT);
  thin.append(new SemanticEvent(25 * MIN, 'sponsor_mention', 'Red Bull'));
  const contract = get('economic', 'sponsor-obligation');
  grantRoles(authority, contract, sponsorActors);
  authority.enactAuthorized(commitments, contract, { enactment_id: 'sp1', actors: sponsorActors, subject: SUBJECT });
  expect(thin.sponsorMentions('Red Bull')).toHaveLength(1);
  expect(commitments.status('sp1:speak_the_acknowledgements')).toBe(OPEN);
}));

test('cooling drops chatter but preserves promoted claims and institutional commitments', () => withGraphs((commitments, authority) => {
  const track = stream(), ledger = new ClaimLedger();
  ledger.fromUtterance(track, 18 * MIN, { claim_id: 'c1', asserter: 'Neon June', subject: 'event:trees-friday', assertion: 'plays Trees Friday' });
  const contract = get('economic', 'sponsor-obligation');
  grantRoles(authority, contract, sponsorActors);
  authority.enactAuthorized(commitments, contract, { enactment_id: 'sp1', actors: sponsorActors, subject: SUBJECT });
  const [kept, expired] = track.cool();
  const expiredKinds = new Set(expired.map(event => event.kind));
  for (const kind of ['chat', 'utterance', 'clip_candidate', 'place', 'cta']) expect(expiredKinds.has(kind)).toBe(true);
  expect(new Set(kept.map(event => event.kind))).toEqual(new Set(['claim', 'sponsor_mention']));
  expect(kept.filter(event => event.kind === 'sponsor_mention')).toHaveLength(2);
  expect(outstandingAfter(contract, commitments, 'sp1')).toHaveLength(3);
  expect(ledger.claims.get('c1').asserter).toBe('Neon June');
}));

test('whole-occurrence compression only increases durability after explicit claim promotion', () => {
  const track = stream();
  expect(track.compression()).toEqual({ utterances: 2, semantic_events: 9, durable_events: 2, claims: 0 });
  new ClaimLedger().fromUtterance(track, 18 * MIN, { claim_id: 'c1', asserter: 'Neon June', subject: 'event:trees-friday', assertion: 'plays Trees Friday' });
  expect(track.compression()).toEqual({ utterances: 2, semantic_events: 10, durable_events: 3, claims: 1 });
});

test('a mid-stream surface sees place but no future sponsor acknowledgement', () => {
  const snapshot = stream().atOrBefore(20 * MIN);
  expect(snapshot.filter(event => event.kind === 'sponsor_mention')).toEqual([]);
  expect(snapshot.filter(event => event.kind === 'place' && event.value === 'Trees')).toHaveLength(1);
});
