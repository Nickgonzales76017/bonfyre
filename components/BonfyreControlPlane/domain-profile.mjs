import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const cueFile = 'schemas/control/domain-profile.cue';
function authored(expression) {
  const result = Bun.spawnSync(['cue', 'export', cueFile, '-e', expression, '--out', 'json'], { cwd: repo });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return JSON.parse(result.stdout.toString());
}
export const nativeDomainProfiles = authored('nativeDomainProfiles');
export const parcelLiveSurface = authored('parcelLiveSurface');
export const parcelLiveRoutes = authored('parcelLiveRoutes');
export const WITHHELD = '__withheld__';

export class ProfileError extends Error {}

export class DomainProfile {
  constructor(name, semanticFields, evidenceRequirements = [], policy = null) {
    this.name = name;
    this.semanticFields = new Set(semanticFields);
    this.evidenceRequirements = new Set(evidenceRequirements);
    this.projections = new Map();
    this.policy = policy;
  }
  projectInto(carrier, standard, mapping, container = '') {
    const undeclared = Object.keys(mapping).filter(field => !this.semanticFields.has(field)).sort();
    if (undeclared.length) throw new ProfileError(`carrier ${carrier} projects field(s) ${undeclared} that profile ${this.name} does not declare -- a grammar may rename the shared meaning and may not add to it`);
    this.projections.set(carrier, { carrier, standard, mapping: { ...mapping }, container });
    return this;
  }
  render(record, carrier, audience = 'public') {
    const projection = this.projections.get(carrier);
    if (!projection) throw new ProfileError(`profile ${this.name} has no ${carrier} projection`);
    const disclosed = this.policy ? this.policy.project(record, audience) : { ...record };
    const fields = {};
    for (const [semantic, target] of Object.entries(projection.mapping)) {
      if (!Object.hasOwn(disclosed, semantic)) continue;
      const value = disclosed[semantic];
      fields[target] = value && typeof value === 'object' && value[WITHHELD]
        ? { [WITHHELD]: true, reason: value.reason } : value;
    }
    return projection.container
      ? { standard: projection.standard, container: projection.container, fields }
      : { standard: projection.standard, fields };
  }
  withheldIn(rendered) {
    return Object.entries(rendered.fields).filter(([, value]) => value && typeof value === 'object' && value[WITHHELD])
      .map(([field]) => field).sort();
  }
  semanticAgreement(record, carriers, audience = 'public') {
    const out = new Map();
    for (const carrier of carriers) {
      const projection = this.projections.get(carrier);
      if (!projection) throw new ProfileError(`profile ${this.name} has no ${carrier} projection`);
      const rendered = this.render(record, carrier, audience);
      for (const [semantic, field] of Object.entries(projection.mapping)) {
        if (!Object.hasOwn(rendered.fields, field)) continue;
        const value = rendered.fields[field];
        if (!out.has(semantic)) out.set(semantic, new Set());
        out.get(semantic).add(value && typeof value === 'object' && value[WITHHELD]
          ? WITHHELD : value);
      }
    }
    return out;
  }
  disagreements(record, carriers, audience = 'public') {
    return [...this.semanticAgreement(record, carriers, audience)].filter(([, values]) => values.size > 1)
      .map(([name]) => name).sort();
  }
}

export function namedProfile(name, policy = null) {
  const declaration = nativeDomainProfiles.find(profile => profile.name === name);
  if (!declaration) throw new ProfileError(`unknown domain profile ${name}`);
  const profile = new DomainProfile(name, declaration.fields.map(field => field.name),
    declaration.evidence_requirements, policy);
  for (const projection of declaration.projections) profile.projectInto(
    projection.carrier, projection.standard,
    Object.fromEntries(projection.fields.map(field => [field.semantic, field.carrier])),
    projection.container || '');
  return profile;
}
export const parcelProfile = policy => namedProfile('parcel', policy);
export const liveProfile = policy => namedProfile('live-moment', policy);
export const socialClipProfile = policy => namedProfile('social-clip', policy);
export function dig(payload, path) { return path.reduce((node, step) => node[step], payload); }
export function momentRecord(occurrence, event) {
  return { occurrence, at_ms:event.at_ms, kind:event.kind, value:event.value,
    speaker:event.speaker, source:event.source };
}
