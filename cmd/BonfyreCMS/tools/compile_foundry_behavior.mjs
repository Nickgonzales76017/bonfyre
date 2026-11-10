#!/usr/bin/env bun
// Build-time family expansion; Address and Surface still use exact kind equality.
import { readFileSync, writeFileSync } from 'node:fs';

export const expectedFoundries = new Set(['person', 'household', 'school', 'program', 'community', 'project', 'property', 'creator', 'business', 'event', 'learning_opportunity', 'tutor', 'library', 'club']);
const modes = new Set(['DECLARE', 'PROPOSE', 'ATTACH', 'DERIVE', 'INVOKE']);
const ident = /^[A-Za-z][A-Za-z0-9_]*$/;
export class SpecError extends Error {}
const requireSpec = (okay, message) => { if (!okay) throw new SpecError(message); };
const textList = (value, field) => { requireSpec(Array.isArray(value) && value.every(x => typeof x === 'string' && x), `${field} must be a list of nonempty strings`); return value.join(','); };
export function exactContract(target, behavior) {
  const operation = behavior.operation, mode = behavior.birth_mode, ownerPath = behavior.owner_path, ownerParam = behavior.owner_subject_param;
  const ownerValue = behavior.owner_subject_value, returnTemplate = behavior.return_template ?? '';
  requireSpec(typeof target === 'string' && ident.test(target), `invalid exact target kind: ${JSON.stringify(target)}`);
  requireSpec(typeof operation === 'string' && operation.includes('.'), `${target}: behavior needs a qualified operation`);
  requireSpec(modes.has(mode), `${operation}: unknown birth mode ${JSON.stringify(mode)}`);
  requireSpec(typeof ownerPath === 'string' && ownerPath.startsWith('/') && !ownerPath.startsWith('//'), `${operation}: verified owner_path must be a local absolute path`);
  requireSpec(typeof ownerParam === 'string' && ident.test(ownerParam), `${operation}: owner_subject_param must be an identifier`);
  requireSpec(['subject', 'subject_identifier'].includes(ownerValue), `${operation}: owner_subject_value must be one of ['subject', 'subject_identifier']`);
  requireSpec(typeof returnTemplate === 'string', `${operation}: return_template must be a string`);
  if (returnTemplate) {
    requireSpec(returnTemplate.startsWith('/') && !returnTemplate.startsWith('//'), `${operation}: return_template must be a local path`);
    requireSpec(ownerValue !== 'subject_identifier' || returnTemplate.includes('{subject_identifier}'), `${operation}: return_template must preserve the subject identifier`);
  }
  requireSpec(['object_name', 'collection_lens', 'action_label', 'owner_note', 'cannot_infer'].every(k => typeof behavior[k] === 'string' && behavior[k]), `${operation}: missing required behavior contract text`);
  requireSpec(mode === 'INVOKE', `${operation}: external owner lowering must be INVOKE`);
  return {
    object_name: behavior.object_name, applies_to_kind: target, applies_to_capability: '', birth_operation: operation, birth_mode: mode,
    requires_authority: behavior.authority_requirement ?? '', initial_state: '', identity_allocation: 'none', body_content_type: '',
    collection_lens: behavior.collection_lens, slug_field: '', closed_fields: textList(behavior.closed_fields ?? [], `${operation}.closed_fields`),
    human_fields: textList(behavior.human_fields ?? [], `${operation}.human_fields`), owner_note: behavior.owner_note,
    submit_to: ownerPath, submit_method: 'POST', owner_subject_param: ownerParam, owner_subject_value: ownerValue,
    return_template: returnTemplate, action_label: behavior.action_label, status: 'published',
  };
}
export function compileSpec(spec) {
  requireSpec(spec?.schema === 'bonfyre-foundry-behavior-spec.v1', "expected schema 'bonfyre-foundry-behavior-spec.v1'");
  requireSpec(Array.isArray(spec.families), 'families must be a list');
  requireSpec(Array.isArray(spec.foundries), 'foundries must be a list');
  const foundries = [], names = new Set();
  for (const row of spec.foundries) {
    requireSpec(row && typeof row === 'object' && !Array.isArray(row), 'foundry must be an object');
    const name = row.foundry;
    requireSpec(typeof name === 'string' && expectedFoundries.has(name), `unknown foundry ${JSON.stringify(name)}`);
    requireSpec(!names.has(name), `duplicate foundry ${name}`);
    requireSpec(Array.isArray(row.roots) && row.roots.length > 0 && row.roots.every(x => typeof x === 'string' && x) && ['owner_ready', 'materialization_needed', 'projection', 'composition'].includes(row.closure_posture), `${name}: roots and closure_posture are required`);
    names.add(name);
    foundries.push({ foundry: name, roots: row.roots, facets: row.facets ?? [], closure_posture: row.closure_posture, reason: String(row.reason ?? '') });
  }
  requireSpec(names.size === expectedFoundries.size && [...expectedFoundries].every(x => names.has(x)), 'foundries must declare all fourteen canonical Foundries');
  const contracts = [], unresolved = [], projections = [], seen = new Set();
  for (const family of spec.families) {
    requireSpec(family && typeof family === 'object' && !Array.isArray(family), 'family must be an object');
    const { family: name, targets, behaviors } = family;
    requireSpec(typeof name === 'string' && name && Array.isArray(targets) && Array.isArray(behaviors), 'family needs family, targets, and behaviors');
    for (const behavior of behaviors) {
      requireSpec(behavior && typeof behavior === 'object' && typeof behavior.operation === 'string', `${name}: every behavior needs an operation`);
      if (behavior.status === 'verified') {
        for (const target of targets) {
          const contract = exactContract(target, behavior), key = `${contract.applies_to_kind}\0${contract.birth_operation}`;
          requireSpec(!seen.has(key), `duplicate exact contract: ${target} -> ${contract.birth_operation}`);
          seen.add(key); contracts.push(contract);
        }
      } else if (['unresolved', 'projection'].includes(behavior.status)) {
        (behavior.status === 'unresolved' ? unresolved : projections).push({ family: name, operation: behavior.operation, reason: String(behavior.reason ?? 'reason not stated') });
      } else throw new SpecError(`${name}.${behavior.operation}: status must be verified, unresolved, or projection`);
    }
  }
  contracts.sort((a, b) => a.applies_to_kind.localeCompare(b.applies_to_kind) || a.birth_operation.localeCompare(b.birth_operation));
  foundries.sort((a, b) => a.foundry.localeCompare(b.foundry));
  return { schema: 'bonfyre-foundry-behavior-catalog.v1', foundries, contracts, unresolved, projections,
    cannot_infer: 'a behavior family is compile-time authoring only; it neither creates runtime inheritance nor proves an owner completed an effect' };
}
if (import.meta.main) {
  const args = {};
  for (let i = 2; i < process.argv.length; i++) { const key = process.argv[i].slice(2); if (!['spec', 'out'].includes(key)) throw Error(`unknown option ${process.argv[i]}`); args[key] = process.argv[++i]; }
  try {
    if (!args.spec || !args.out) throw new SpecError('--spec and --out are required');
    const result = compileSpec(JSON.parse(readFileSync(args.spec, 'utf8')));
    writeFileSync(args.out, JSON.stringify(result, null, 2) + '\n');
    console.log(JSON.stringify({ foundries: result.foundries.length, contracts: result.contracts.length, unresolved: result.unresolved.length, projections: result.projections.length, schema: result.schema }));
  } catch (error) { console.error(`foundry behavior compiler: ${error.message}`); process.exit(2); }
}
