import { test, expect } from "bun:test";
import { ClaimLedger, ClaimObservation, LiveSemanticTrack, SemanticEvent, survivesCooling, TIERS } from "../live-semantic-track.mjs";
import { liveProfile, momentRecord } from "../domain-profile.mjs";
import { InformationPolicy } from "../information-policy.mjs";
import { CommitmentGraph, OPEN, DISCHARGED } from "../commitment-graph.mjs";

const minute = 60_000;
function scenario() {
  const track = new LiveSemanticTrack("live:neon-june-2026-09-04");
  track.say(0, "hey Deep Ellum", "Neon June");
  track.say(18 * minute, "we're playing Trees again Friday", "Neon June");
  track.append(new SemanticEvent(18 * minute, "place", "Trees"));
  track.append(new SemanticEvent(25 * minute, "sponsor_mention", "Red Bull", "Neon June"));
  track.append(new SemanticEvent(39 * minute, "cta", "tickets are in our profile", "Neon June"));
  track.append(new SemanticEvent(40 * minute, "clip_candidate", "friday-announcement"));
  track.append(new SemanticEvent(41 * minute, "chat", "line at Trees is insane"));
  return track;
}

test("speech is not a claim; attributed promotion is explicit", () => {
  const track = scenario();
  const ledger = new ClaimLedger();
  expect(track.byKind("utterance")).toHaveLength(2);
  expect(track.byKind("claim")).toHaveLength(0);
  expect(() => ledger.assertClaim(new ClaimObservation("bad", 0, "", "event", "x"))).toThrow("names no asserter");
  const claim = ledger.fromUtterance(track, 18 * minute, { claim_id: "c1", asserter: "Neon June", subject: "event:trees-friday", assertion: "plays Friday" });
  expect(claim.asserter).toBe("Neon June");
  expect(track.byKind("claim")).toHaveLength(1);
  expect(ledger.proven("c1")).toBe(false);
  expect(() => ledger.assertClaim(claim)).toThrow("already asserted");
});

test("repeated, dependent, and self corroboration never manufacture proof", () => {
  const ledger = new ClaimLedger();
  ledger.assertClaim(new ClaimObservation("c1", 0, "Neon June", "event", "plays Friday"));
  ledger.corroborate("c1", { source: "foreign:calendar", kind: "EventListingObservation" });
  ledger.corroborate("c1", { source: "foreign:calendar", kind: "EventListingObservation" });
  ledger.corroborate("c1", { source: "asserter:Neon June", kind: "Post" });
  ledger.corroborate("c1", { source: "foreign:dependent", kind: "Post", independent: false });
  expect(ledger.independentSources("c1")).toEqual(["foreign:calendar"]);
  expect(ledger.crystallizable()).toEqual([]);
  ledger.corroborate("c1", { source: "foreign:venue", kind: "EventListingObservation" });
  expect(ledger.crystallizable()).toEqual(["c1"]);
  expect(ledger.proven("c1")).toBe(false);
  expect(() => ledger.corroborate("missing", { source: "x", kind: "x" })).toThrow("unknown claim");
});

test("media clock windows partition and the track never reads a future moment", () => {
  const track = scenario();
  expect(track.window(0, 18 * minute)).toHaveLength(1);
  expect(track.window(18 * minute, 19 * minute)).toHaveLength(2);
  expect(track.atOrBefore(18 * minute)).toHaveLength(3);
  expect(() => track.append(new SemanticEvent(-1, "chat", "bad"))).toThrow("media clock");
  expect(() => track.append(new SemanticEvent(0, "unknown", "bad"))).toThrow("unknown semantic kind");
});

test("retention tiers and cooling are CUE-authored, promotion is cause-bound and upward only", () => {
  expect(TIERS).toEqual(["flash", "chat", "moment", "social", "saved", "institutional", "evidentiary", "economic"]);
  expect(survivesCooling("saved")).toBe(true);
  expect(survivesCooling("social")).toBe(false);
  const track = scenario();
  expect(track.byTier().chat).toBe(3);
  expect(track.byTier().evidentiary).toBe(1);
  expect(track.cool()[0].map(event => event.kind)).toEqual(["sponsor_mention"]);
  for (const reason of ["popular", "referenced", "survived", "engagement"]) expect(() => track.promote(6, { to: "saved", reason })).toThrow("does not justify promotion");
  const saved = track.promote(6, { to: "saved", reason: "explicit_action" });
  expect(saved.tier()).toBe("saved");
  expect(track.cool()[0]).toContain(saved);
  expect(() => track.promote(3, { to: "chat", reason: "explicit_action" })).toThrow("cannot demote");
  expect(() => track.promote(1, { to: "forever", reason: "evidence" })).toThrow("unknown retention tier");
  expect(() => track.append(new SemanticEvent(0, "chat", "x", "", "audio", 1, "forever"))).toThrow("unknown retention tier");
});

test("sponsor matching is case-insensitive but exact; candidates cool", () => {
  const track = scenario();
  expect(track.sponsorMentions("red bull")).toHaveLength(1);
  expect(track.sponsorMentions("Red")).toHaveLength(0);
  expect(track.byKind("clip_candidate")[0].tier()).toBe("moment");
  expect(track.cool()[1]).toContain(track.byKind("clip_candidate")[0]);
  const ledger = new ClaimLedger();
  ledger.fromUtterance(track, 18 * minute, { claim_id: "c1", asserter: "Neon June", subject: "event", assertion: "plays Friday" });
  expect(track.compression()).toEqual({ utterances: 2, semantic_events: 8, durable_events: 2, claims: 1 });
  expect(track.cool()[1].length).toBeGreaterThan(track.cool()[0].length);
});

test("one live moment projects across carrier grammars without inventing meaning or leaking a withheld speaker", () => {
  const event = new SemanticEvent(18 * minute, "claim", "plays Trees Friday", "Neon June");
  const record = momentRecord("live:neon-june", event);
  const carriers = ["cloudevents", "activitypub", "atproto", "mcp"];
  const profile = liveProfile();
  for (const carrier of carriers) expect(profile.render(record, carrier).fields).toBeTruthy();
  expect(profile.disagreements(record, carriers)).toEqual([]);
  expect(profile.render(record, "cloudevents").fields.type).toBe("claim");
  const activitypub = profile.render(record, "activitypub").fields;
  expect(activitypub.time).toBeUndefined();
  expect(activitypub.createdAt).toBeUndefined();

  const policy = new InformationPolicy("LiveMoment");
  for (const field of ["occurrence", "at_ms", "kind", "value", "source"]) policy.disclose(field, { to: ["public", "institution"] });
  policy.disclose("speaker", { to: ["institution"], reason: "speaker identity is not released to the public feed" });
  const guarded = liveProfile(policy);
  for (const carrier of ["activitypub", "atproto", "mcp"]) expect(JSON.stringify(guarded.render(record, carrier, "public"))).not.toContain("Neon June");
  expect(guarded.render(record, "mcp", "institution").fields.speaker).toBe("Neon June");
});

test("spoken sponsorship is evidence toward a native durable obligation, not its discharge", () => {
  const track = scenario();
  const graph = new CommitmentGraph();
  try {
    graph.record({ commitment_id: "s1", committer: "Neon June", beneficiary: "Red Bull", resource: "two spoken acknowledgements on air", enactment: "sponsor:2026-09" });
    expect(track.sponsorMentions("Red Bull")).toHaveLength(1);
    expect(graph.status("s1")).toBe(OPEN);
    track.append(new SemanticEvent(52 * minute, "sponsor_mention", "Red Bull", "Neon June"));
    const mentions = track.sponsorMentions("Red Bull");
    expect(mentions.map(event => event.at_ms)).toEqual([25 * minute, 52 * minute]);
    graph.discharge("s1", { actor: "institution", detail: `two acknowledgements at ${mentions.map(event => event.at_ms).join(",")}` });
    expect(graph.status("s1")).toBe(DISCHARGED);
    expect(graph.delivered("s1")).toBe(true);
  } finally { graph.close(); }
});
