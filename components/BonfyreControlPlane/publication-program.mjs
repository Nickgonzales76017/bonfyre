import { resolve } from 'node:path';
import { socialClipProfile } from './domain-profile.mjs';

const repo = resolve(import.meta.dir, '../..');
const exported = Bun.spawnSync(['cue', 'export', 'schemas/control/publication-program.cue',
  '-e', 'publicationBinding', '--out', 'json'], { cwd: repo });
if (exported.exitCode !== 0) throw new Error(exported.stderr.toString());
const binding = JSON.parse(exported.stdout.toString());
export const PUBLISH = binding.defaultOperation;

export class PublicationRefused extends Error {
  constructor(stage, detail) { super(`${stage}: ${detail}`); this.stage = stage; }
}

export class PublicationProgram {
  constructor({ subject, principal, agent, purpose, profile,
    targets = [], expiresAt = null, roundTripRequired = true }) {
    Object.assign(this, { subject, principal, agent, purpose, profile, targets: [...targets],
      expiresAt, roundTripRequired });
  }

  to(platform, { carrier, audience = binding.defaultAudience, operation = PUBLISH }) {
    if (!this.profile.projections.has(carrier)) {
      throw new PublicationRefused('projection',
        `profile '${this.profile.name}' has no '${carrier}' projection, so '${platform}' has no grammar to be rendered into`);
    }
    this.targets.push({ platform, carrier, audience, operation });
    return this;
  }

  consentGaps(authority, { atIso = null } = {}) {
    const gaps = {};
    for (const target of this.targets) {
      const permission = `${target.operation}:${target.platform}`;
      if (!authority.hasAuthority(this.agent, permission, this.subject,
        { at_iso: atIso, purpose: this.purpose })) {
        gaps[target.platform] = `no live grant for '${permission}' on '${this.subject}' for purpose '${this.purpose}'`;
      }
    }
    return gaps;
  }

  consented(authority, options = {}) { return !Object.keys(this.consentGaps(authority, options)).length; }

  render(record, target) { return this.profile.render(record, target.carrier, target.audience); }

  publish(authority, record, { adapters, atIso = null }) {
    const gaps = this.consentGaps(authority, { atIso });
    if (Object.keys(gaps).length) {
      throw new PublicationRefused('delegation', Object.keys(gaps).sort()
        .map(platform => `${platform}: ${gaps[platform]}`).join('; '));
    }
    const missing = this.targets.map(target => target.platform)
      .filter(platform => !Object.hasOwn(adapters, platform)).sort();
    if (missing.length) throw new PublicationRefused('adapter', `no adapter supplied for ${JSON.stringify(missing)}`);
    const results = {};
    for (const target of this.targets) {
      const sent = this.render(record, target);
      results[target.platform] = { sent, adapter_reported: adapters[target.platform](sent),
        observed: false, withheld: this.profile.withheldIn(sent) };
    }
    return results;
  }

  roundTripPending(results) {
    return this.roundTripRequired ? Object.keys(results).filter(platform => !results[platform].observed).sort() : [];
  }

  observe(results, platform, { twinReceipt }) {
    if (!Object.hasOwn(results, platform)) {
      throw new PublicationRefused('observation', `'${platform}' was not published by this program`);
    }
    if (!twinReceipt) {
      throw new PublicationRefused('observation',
        'a round-trip observation names its receipt; without one it is the estate confirming its own publication');
    }
    results[platform].observed = true;
    results[platform].twin_receipt = twinReceipt;
    return results;
  }
}

export { socialClipProfile };
