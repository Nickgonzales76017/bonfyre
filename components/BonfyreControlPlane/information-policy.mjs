import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const exported = Bun.spawnSync(['cue', 'export', 'schemas/control/information-policy.cue',
  '-e', 'informationPolicyBinding', '--out', 'json'], { cwd: repo });
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
const binding = JSON.parse(exported.stdout.toString());

export const AUDIENCES = Object.freeze(binding.audiences);
export const WITHHELD = '__withheld__';
const knownAudiences = new Set(AUDIENCES);
const defaultAggregateAudiences = Object.freeze(binding.aggregate_default_audiences);
const undeclaredField = 'no disclosure rule declared for this field';
const undeclaredAggregate = 'no aggregation floor declared for this statistic';

export class PolicyError extends Error {}

function audienceSet(to, subject) {
  const audiences = new Set(to);
  const unknown = [...audiences].filter(audience => !knownAudiences.has(audience)).sort();
  if (unknown.length) throw new PolicyError(`unknown audience(s) ${JSON.stringify(unknown)} on ${subject}`);
  return audiences;
}

function requireAudience(audience) {
  if (!knownAudiences.has(audience)) throw new PolicyError(`unknown audience ${JSON.stringify(audience)}`);
}

export class InformationPolicy {
  constructor(subjectKind) {
    this.subjectKind = subjectKind;
    this.rules = new Map();
    this.floors = new Map();
    this.evidenceRelease = new Set();
  }

  disclose(fieldName, { to, reason = 'not disclosed to this audience' }) {
    this.rules.set(fieldName, { fieldName, audiences: audienceSet(to, `field ${JSON.stringify(fieldName)}`), reason });
    return this;
  }

  floor(statistic, { minimumPopulation, to = defaultAggregateAudiences }) {
    if (!Number.isInteger(minimumPopulation) || minimumPopulation < 2) {
      throw new PolicyError(`aggregation floor for ${JSON.stringify(statistic)} must be at least 2; a floor of ${minimumPopulation} discloses the individual`);
    }
    this.floors.set(statistic, { statistic, minimumPopulation,
      audiences: audienceSet(to, `statistic ${JSON.stringify(statistic)}`) });
    return this;
  }

  fieldDecision(fieldName, audience) {
    requireAudience(audience);
    const rule = this.rules.get(fieldName);
    if (!rule) return [false, undeclaredField];
    return rule.audiences.has(audience) ? [true, ''] : [false, rule.reason];
  }

  project(record, audience) {
    requireAudience(audience);
    return Object.fromEntries(Object.entries(record).map(([field, value]) => {
      const [allowed, reason] = this.fieldDecision(field, audience);
      return [field, allowed ? value : { [WITHHELD]: true, reason }];
    }));
  }

  withheldFields(projected) {
    return Object.keys(projected).filter(field => projected[field]?.[WITHHELD]).sort();
  }

  aggregateDecision(statistic, population, audience) {
    requireAudience(audience);
    const floor = this.floors.get(statistic);
    if (!floor) return [false, undeclaredAggregate];
    if (!floor.audiences.has(audience)) return [false, 'this audience may not receive this aggregate'];
    if (population < floor.minimumPopulation) {
      return [false, `population ${population} is below the disclosure floor of ${floor.minimumPopulation}`];
    }
    return [true, ''];
  }

  releaseAggregate(statistic, value, population, audience) {
    const [allowed, reason] = this.aggregateDecision(statistic, population, audience);
    if (allowed) return { statistic, value, population };
    const floor = this.floors.get(statistic);
    return { statistic, [WITHHELD]: true,
      reason: floor && population < floor.minimumPopulation
        ? `below the disclosure floor of ${floor.minimumPopulation}` : reason };
  }
}

export function evidenceVisible(policy, witnessKind) {
  return policy.evidenceRelease.has(witnessKind);
}
