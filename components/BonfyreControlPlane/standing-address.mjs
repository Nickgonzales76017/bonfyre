import { territoryActions, territoryAudience, attentionSignals } from './authored-territory.mjs';

export { territoryActions, territoryAudience, attentionSignals } from './authored-territory.mjs';
export const ACTIONS = Object.freeze([...new Set(Object.values(territoryActions).flat())]);
const actions = new Set(ACTIONS), signals = new Set(attentionSignals);
export class StandingError extends Error {}

export class Opportunity {
  constructor(subject, action, territory, { relevance = 0, reason = '', available = true } = {}) {
    if (!subject || typeof subject !== 'string' || subject.length >= 256) throw new StandingError('an opportunity needs a bounded addressed subject');
    if (!actions.has(action)) throw new StandingError(`unknown action ${action}`);
    if (!territoryActions[territory]) throw new StandingError(`unknown territory ${territory}`);
    if (!territoryActions[territory].includes(action)) throw new StandingError(`${action} is not offered in .${territory}; a territory is a disclosure context and an action outside it is a category error rather than a low-ranking candidate`);
    if (!Number.isFinite(relevance)) throw new StandingError('relevance must be finite');
    Object.assign(this, { subject, action, territory, relevance, reason, available: Boolean(available) });
    Object.freeze(this);
  }
}

export class StandingAddress {
  constructor({ actor, territory, policy = null }) {
    if (!actor || typeof actor !== 'string' || actor.length >= 256) throw new StandingError('standing address requires a bounded actor');
    if (!territoryActions[territory]) throw new StandingError(`unknown territory ${territory}`);
    this.actor = actor;
    this.territory = territory;
    this.policy = policy;
    this.attention = new Map();
  }

  observeAttention(subject, signal, weight = 1) {
    if (!signals.has(signal)) throw new StandingError(`${signal} is not an attention signal; attention may reorder what is offered and may never open an obligation, so an Action cannot be recorded here`);
    if (!subject || typeof subject !== 'string' || subject.length >= 256) throw new StandingError('attention needs a bounded subject');
    if (!Number.isFinite(weight)) throw new StandingError('attention weight must be finite');
    if (!this.attention.has(subject) && this.attention.size >= 256) throw new StandingError('attention subject capacity exceeded');
    this.attention.set(subject, (this.attention.get(subject) ?? 0) + weight);
    return this;
  }

  offerable(candidates) {
    if (candidates.length > 256) throw new StandingError('opportunity capacity exceeded');
    if (this.policy && !this.policy.fieldDecision('subject', territoryAudience[this.territory])[0]) return [];
    return candidates.filter(candidate => candidate instanceof Opportunity && candidate.territory === this.territory);
  }

  compile(candidates) {
    return this.offerable(candidates).map((opportunity, index) => ({ opportunity, index, score: opportunity.relevance + (this.attention.get(opportunity.subject) ?? 0) * 0.1 }))
      .sort((a, b) => Number(b.opportunity.available) - Number(a.opportunity.available) || b.score - a.score || a.index - b.index)
      .map(item => item.opportunity);
  }

  top(candidates, n = 5) {
    if (!Number.isInteger(n) || n < 0) throw new StandingError('top count must be nonnegative');
    return this.compile(candidates).slice(0, n);
  }
}

export function attentionDelta(address, subject, signal, weight = 1) {
  address.observeAttention(subject, signal, weight);
  return { loop: 'attention', subject, signal, invalidates: [`standing:${address.actor}:${address.territory}`], opens_obligation: false };
}

export function institutionalDelta(subject, action, { enactment, occurrence }) {
  if (!actions.has(action)) throw new StandingError(`unknown action ${action}`);
  if (!enactment || !occurrence) throw new StandingError("the institutional loop is entered by an enactment that produced an occurrence; without both this is an attention signal wearing an Action's name");
  return { loop: 'institutional', subject, action, enactment, occurrence, invalidates: [`subject:${subject}`, `enactment:${enactment}`], opens_obligation: true };
}
