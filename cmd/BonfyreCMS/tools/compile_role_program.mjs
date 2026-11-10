#!/usr/bin/env bun
// A RoleProgram arranges shared subjects and exact actions; it grants nothing.
import { readFileSync, writeFileSync } from 'node:fs';

const fields = ['subject_kinds', 'relationship_kinds', 'search_scopes', 'lenses', 'work_queues', 'capability_programs', 'authority_requirements', 'requested_operations', 'metric_views', 'handoff_contracts'];
export class SpecError extends Error {}
const requireSpec = (okay, message) => { if (!okay) throw new SpecError(message); };
function strings(value, name) { requireSpec(Array.isArray(value) && value.every(x => typeof x === 'string' && x), `${name} must be a list of nonempty strings`); return [...value]; }
export function compileRoles(spec, catalog) {
  requireSpec(spec?.schema === 'bonfyre-role-program-spec.v1', 'unsupported role program schema');
  requireSpec(Array.isArray(spec.roles) && Array.isArray(catalog?.contracts), 'roles and exact Foundry catalog are required');
  const exact = new Set(catalog.contracts.filter(x => x && typeof x === 'object').map(x => String(x.birth_operation)));
  const seen = new Set(), programs = [];
  for (const role of spec.roles) {
    requireSpec(role && typeof role === 'object' && !Array.isArray(role), 'role must be an object');
    const id = role.role_id, label = role.label;
    requireSpec(typeof id === 'string' && /^[a-z][a-z0-9-]*$/.test(id) && !seen.has(id), 'role_id must be a unique lowercase identifier');
    requireSpec(typeof label === 'string' && label, `${id}: label required`);
    seen.add(id);
    const values = Object.fromEntries(fields.map(field => [field, strings(role[field], `${id}.${field}`)]));
    const scene = role.surface_scene;
    requireSpec(scene && typeof scene === 'object' && ['focus', 'presentation', 'workspace'].every(k => typeof scene[k] === 'string' && scene[k]), `${id}: surface_scene requires focus, presentation, workspace`);
    const requested = values.requested_operations; delete values.requested_operations;
    programs.push({ role_id: id, label, ...values, exact_operations: requested.filter(x => exact.has(x)), unresolved_operations: requested.filter(x => !exact.has(x)),
      surface_scene: scene, cannot_infer: 'a role program selects lawful lenses and exact existing operations; it does not create an Actor kind, grant authority or provider access, or complete an effect', status: 'published' });
  }
  programs.sort((a, b) => a.role_id.localeCompare(b.role_id));
  return { schema: 'bonfyre-role-program-catalog.v1', programs,
    cannot_infer: 'compiled role reach is globally portable public cognition; effect authority remains local and is checked by the owner at action time' };
}
if (import.meta.main) {
  const args = {};
  for (let i = 2; i < process.argv.length; i++) { const key = process.argv[i].slice(2); if (!['spec', 'foundry-catalog', 'out'].includes(key)) throw Error(`unknown option ${process.argv[i]}`); args[key] = process.argv[++i]; }
  try {
    if (!args.spec || !args['foundry-catalog'] || !args.out) throw new SpecError('--spec, --foundry-catalog and --out are required');
    const result = compileRoles(JSON.parse(readFileSync(args.spec, 'utf8')), JSON.parse(readFileSync(args['foundry-catalog'], 'utf8')));
    writeFileSync(args.out, JSON.stringify(result, null, 2) + '\n');
    console.log(JSON.stringify({ programs: result.programs.length, schema: result.schema }));
  } catch (error) { console.error(`role program compiler: ${error.message}`); process.exit(2); }
}
