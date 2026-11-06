import { resolve } from 'node:path';
import { ClaimLedger, LiveSemanticTrack, SemanticEvent } from './live-semantic-track.mjs';
import { OBSERVED, record } from './occurrence-origin.mjs';

const repo = resolve(import.meta.dir, '../..');
const exported = Bun.spawnSync(['cue', 'export', 'schemas/control/foreign-live.cue',
  '-e', 'foreignLiveBinding', '--out', 'json'], { cwd: repo });
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
const binding = JSON.parse(exported.stdout.toString());

export const RIGHTS = Object.freeze(binding.rights);
export const [OBSERVABLE, RETAINABLE, DERIVABLE, REPUBLISHABLE] = RIGHTS;
export const FOREIGN_KIND_MAP = Object.freeze(binding.kindMap);
const rank = new Map(RIGHTS.map((right, index) => [right, index]));

export class RightsError extends Error {}
export class IngestError extends Error {}

function pyQuote(value) { return `'${String(value).replaceAll("'", "\\'")}'`; }

export class ForeignLiveSource {
  constructor({ twinId, platform, url, granted = [OBSERVABLE], basis = '' }) {
    Object.assign(this, { twinId, platform, url, granted: new Set(granted), basis });
  }
  holds(right) {
    if (!rank.has(right)) throw new RightsError(`unknown right ${pyQuote(right)}`);
    return this.granted.has(right);
  }
  require(right, doing) {
    if (this.holds(right)) return;
    const held = [...this.granted].sort((a, b) => rank.get(a) - rank.get(b));
    const heldText = held.length ? `[${held.map(pyQuote).join(', ')}]` : `['nothing']`;
    throw new RightsError(`cannot ${doing}: ${pyQuote(this.twinId)} grants ${heldText} and not ${pyQuote(right)}. Each right is granted separately and holding one never implies the next`);
  }
}

export class ForeignLiveIngest {
  constructor(source, track) { Object.assign(this, { source, track, retained: [] }); }
  retain(atMs, foreignKind, value, { speaker = '' } = {}) {
    this.source.require(RETAINABLE, `retain ${pyQuote(foreignKind)}`);
    if (!Object.hasOwn(FOREIGN_KIND_MAP, foreignKind)) {
      throw new IngestError(`unknown foreign kind ${pyQuote(foreignKind)}; a foreign source's material is mapped onto the native vocabulary rather than given a parallel one of its own`);
    }
    this.track.append(new SemanticEvent(atMs, FOREIGN_KIND_MAP[foreignKind], value,
      speaker, `foreign:${this.source.platform}`));
    this.retained.push([atMs, foreignKind, value]);
    return this;
  }
  deriveClaim(ledger, atMs, { claimId, asserter, subject, assertion }) {
    this.source.require(DERIVABLE, 'derive a claim from retained material');
    if (asserter === this.source.twinId) {
      throw new IngestError('the asserter is whoever spoke on the foreign stream, not the twin that observed it; a twin asserting its own observation would make watching look like claiming');
    }
    return ledger.fromUtterance(this.track, atMs, { claim_id: claimId, asserter, subject, assertion });
  }
  republishableEvents() {
    this.source.require(REPUBLISHABLE, 'republish foreign material');
    return [...this.track.events];
  }
  fileObservations(db, { subject, at = '' }) {
    return this.retained.map(([atMs], index) => {
      const occurrenceId = `${this.source.twinId}:${atMs}:${index}`;
      record(db, { occurrenceId, origin: OBSERVED, subject, at,
        source: `${this.source.platform}:${this.source.url}` });
      return occurrenceId;
    });
  }
}

export function ingest(source, { occurrenceId } = {}) {
  source.require(OBSERVABLE, 'open an ingest');
  return new ForeignLiveIngest(source, new LiveSemanticTrack(occurrenceId || `foreign:${source.twinId}`));
}

export function rightsGap(source, wanted) {
  if (!rank.has(wanted)) throw new RightsError(`unknown right ${pyQuote(wanted)}`);
  return RIGHTS.slice(0, rank.get(wanted) + 1).filter(right => !source.granted.has(right));
}

export { ClaimLedger };
