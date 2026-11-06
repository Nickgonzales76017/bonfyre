import { resolve } from 'node:path';
import { territoryActions } from './authored-territory.mjs';
export { territoryActions } from './authored-territory.mjs';

const repo = resolve(import.meta.dir, '../..');
const cueFiles = ['schemas/control/surface-pattern.cue', 'schemas/control/standing-address.cue'];
function authored(expression) {
  const result = Bun.spawnSync(['cue', 'export', ...cueFiles, '-e', expression, '--out', 'json'], { cwd: repo });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return JSON.parse(result.stdout.toString());
}

export const extents = Object.freeze(['chip', 'card', 'section', 'page', 'workspace']);
export const actionExtents = new Set(['page', 'workspace']);
export const surfacePatterns = authored('surfacePatterns');
export const referenceSources = authored('referenceSources');
export const permissiveLicenses = new Set(authored('permissiveLicenses'));
export const restrictedLicenses = authored('restrictedLicenses');

export class PatternError extends Error {}

export function referenceSource(project, license, use, solves) {
  if (!['donor', 'implementation', 'reference'].includes(use)) throw new PatternError(`unknown use class ${use}`);
  if (use !== 'reference' && !permissiveLicenses.has(license)) {
    const reason = restrictedLicenses[license] || 'license is not on the permissive list';
    throw new PatternError(`${project} is ${license} and cannot be a ${use}: ${reason}. Study its behaviour as a reference instead`);
  }
  return Object.freeze({ project, license, use, solves });
}

export function pattern(row) {
  const { name, reads, source, extent, authority = '', lineage = '', territory = 'live' } = row;
  const actions = new Set(row.actions || []);
  if (!territoryActions[territory]) throw new PatternError(`pattern ${name} declares territory ${territory}; a pattern belongs to one of ${Object.keys(territoryActions)}`);
  const outside = [...actions].filter(action => !territoryActions[territory].includes(action));
  if (outside.length) throw new PatternError(`pattern ${name} is in .${territory} and offers ${outside}, which that territory does not carry`);
  if (!['cms_type', 'native_organ'].includes(reads)) throw new PatternError(`pattern ${name} declares reads=${reads}; a pattern reads a CMS type or a native organ, because a pattern that read both would be two patterns sharing a name`);
  if (!source) throw new PatternError(`pattern ${name} names no source to read`);
  if (!extents.includes(extent)) throw new PatternError(`unknown extent ${extent}`);
  if (actions.size && !actionExtents.has(extent)) throw new PatternError(`pattern ${name} carries actions at extent ${extent}; only page and workspace carry an action layer, because a chip, card or section is a reference to a surface and there is no room to act inside a reference`);
  if (actions.size && !authority) throw new PatternError(`pattern ${name} offers actions and names no authority; an action layer states what it requires or nothing can refuse it`);
  return Object.freeze({ name, reads, source, extent, actions, authority, lineage, territory });
}

export function atExtent(surface, extent) {
  if (!extents.includes(extent)) throw new PatternError(`unknown extent ${extent}`);
  const actions = actionExtents.has(extent) ? [...surface.actions] : [];
  return pattern({ ...surface, extent, actions, authority: actions.length ? surface.authority : '' });
}

export class PatternCatalogue {
  constructor(rows = []) {
    this.patterns = new Map();
    for (const row of rows) this.add(row);
  }
  add(value) {
    const entry = value.actions instanceof Set ? value : pattern(value);
    if (this.patterns.has(entry.name)) throw new PatternError(`pattern ${entry.name} already declared`);
    this.patterns.set(entry.name, entry);
    return this;
  }
  get(name) {
    if (!this.patterns.has(name)) throw new PatternError(`unknown pattern ${name}; declared: ${[...this.patterns.keys()].sort()}`);
    return this.patterns.get(name);
  }
  offering(action) {
    return [...this.patterns.values()].filter(p => p.actions.has(action)).map(p => p.name).sort();
  }
  byLineage() {
    const out = {};
    for (const p of [...this.patterns.values()].sort((a, b) => a.name.localeCompare(b.name))) {
      const key = p.lineage || 'native';
      (out[key] ||= []).push(p.name);
    }
    return out;
  }
  byTerritory() {
    const out = Object.fromEntries(Object.keys(territoryActions).map(t => [t, []]));
    for (const p of [...this.patterns.values()].sort((a, b) => a.name.localeCompare(b.name))) out[p.territory].push(p.name);
    return out;
  }
}

export function referenceEstate() { return new PatternCatalogue(surfacePatterns); }
export function referencesByUse() {
  const out = { donor: [], implementation: [], reference: [] };
  for (const r of referenceSources) out[r.use].push(r.project);
  return Object.fromEntries(Object.entries(out).map(([key, values]) => [key, values.sort()]));
}
