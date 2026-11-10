import { fileURLToPath } from "node:url";

const policyPath = fileURLToPath(new URL("../../schemas/control/live-semantic-track.cue", import.meta.url));
const exported = Bun.spawnSync(["cue", "export", policyPath, "--out", "json"]);
if (exported.exitCode !== 0) throw new Error(`live retention CUE invalid: ${exported.stderr.toString()}`);
export const retention = Object.freeze(JSON.parse(exported.stdout.toString()).liveRetention);
export const TIERS = Object.freeze([...retention.tiers]);
export const KIND_TIER = Object.freeze(retention.kindTier);
export const COOLING_FLOOR = retention.coolingFloor;
const tierRank = new Map(TIERS.map((tier, index) => [tier, index]));
const promotionReasons = new Set(retention.promotionReasons);
export const survivesCooling = tier => tierRank.get(tier) >= tierRank.get(COOLING_FLOOR);

export class TrackError extends Error {}

export class SemanticEvent {
  constructor(at_ms, kind, value, speaker = "", source = "audio", confidence = 1, retentionTier = "") {
    Object.assign(this, { at_ms, kind, value, speaker, source, confidence, retention: retentionTier });
    Object.freeze(this);
  }
  tier() { return this.retention || KIND_TIER[this.kind]; }
  durability() { return survivesCooling(this.tier()) ? "durable" : "ephemeral"; }
}

export class ClaimObservation {
  constructor(claim_id, at_ms, asserter, subject, assertion, source = "audio") {
    Object.assign(this, { claim_id, at_ms, asserter, subject, assertion, source });
    Object.freeze(this);
  }
}

export class Corroboration {
  constructor(claim_id, source, kind, independent = true) {
    Object.assign(this, { claim_id, source, kind, independent });
    Object.freeze(this);
  }
}

export class LiveSemanticTrack {
  constructor(occurrence_id) { this.occurrence_id = occurrence_id; this.events = []; this.utterance_count = 0; }
  append(event) {
    if (!(event.kind in KIND_TIER)) throw new TrackError(`unknown semantic kind '${event.kind}'; declaring a retention tier for a kind is part of adding it, not a later decision`);
    if (event.retention && !tierRank.has(event.retention)) throw new TrackError(`unknown retention tier '${event.retention}'`);
    if (!Number.isSafeInteger(event.at_ms) || event.at_ms < 0) throw new TrackError("media clock runs from stream start; at_ms cannot be negative or fractional");
    this.events.push(event);
    if (event.kind === "utterance") this.utterance_count++;
    return this;
  }
  say(at_ms, text, speaker = "") { return this.append(new SemanticEvent(at_ms, "utterance", text, speaker)); }
  byKind(kind) { return this.events.filter(event => event.kind === kind); }
  window(start_ms, end_ms) { return this.events.filter(event => start_ms <= event.at_ms && event.at_ms < end_ms); }
  atOrBefore(at_ms) { return this.events.filter(event => event.at_ms <= at_ms); }
  sponsorMentions(sponsor) { return this.byKind("sponsor_mention").filter(event => event.value.toLowerCase() === sponsor.toLowerCase()); }
  compression() { return { utterances: this.utterance_count, semantic_events: this.events.length, durable_events: this.events.filter(event => event.durability() === "durable").length, claims: this.byKind("claim").length }; }
  byTier() { const counts = Object.fromEntries(TIERS.map(tier => [tier, 0])); for (const event of this.events) counts[event.tier()]++; return counts; }
  promote(index, { to, reason }) {
    if (!tierRank.has(to)) throw new TrackError(`unknown retention tier '${to}'`);
    if (!promotionReasons.has(reason)) throw new TrackError(`'${reason}' does not justify promotion; only ${JSON.stringify([...promotionReasons].sort())} move a record up the lattice`);
    const current = this.events[index];
    if (!current) throw new RangeError(`unknown event index ${index}`);
    if (tierRank.get(to) < tierRank.get(current.tier())) throw new TrackError(`cannot demote '${current.kind}' from ${current.tier()} to ${to}; retention moves upward`);
    const promoted = new SemanticEvent(current.at_ms, current.kind, current.value, current.speaker, current.source, current.confidence, to);
    this.events[index] = promoted;
    return promoted;
  }
  cool() { return [this.events.filter(event => survivesCooling(event.tier())), this.events.filter(event => !survivesCooling(event.tier()))]; }
}

export class ClaimLedger {
  constructor() { this.claims = new Map(); this.corroborations = new Map(); }
  assertClaim(claim) {
    if (!claim.asserter) throw new TrackError(`claim '${claim.claim_id}' names no asserter; an unattributed assertion is a rumour and is not recorded as a claim`);
    if (this.claims.has(claim.claim_id)) throw new TrackError(`claim '${claim.claim_id}' already asserted`);
    this.claims.set(claim.claim_id, claim);
    this.corroborations.set(claim.claim_id, []);
    return this;
  }
  fromUtterance(track, at_ms, { claim_id, asserter, subject, assertion }) {
    const claim = new ClaimObservation(claim_id, at_ms, asserter, subject, assertion);
    this.assertClaim(claim);
    track.append(new SemanticEvent(at_ms, "claim", assertion, asserter));
    return claim;
  }
  corroborate(claim_id, { source, kind, independent = true }) {
    if (!this.claims.has(claim_id)) throw new TrackError(`unknown claim '${claim_id}'`);
    const existing = this.corroborations.get(claim_id);
    if (!existing.some(record => record.source === source)) existing.push(new Corroboration(claim_id, source, kind, independent));
    return this;
  }
  independentSources(claim_id) {
    const claim = this.claims.get(claim_id);
    if (!claim) throw new TrackError(`unknown claim '${claim_id}'`);
    return [...new Set(this.corroborations.get(claim_id).filter(record => record.independent && record.source !== `asserter:${claim.asserter}`).map(record => record.source))].sort();
  }
  crystallizable({ threshold = 2 } = {}) { return [...this.claims.keys()].filter(id => this.independentSources(id).length >= threshold).sort(); }
  proven(_claim_id) { return false; }
}
