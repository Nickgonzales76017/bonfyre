#!/usr/bin/env bun
// Lower CUE passports into candidate AddressPlane advertisements only.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const cue = (path, expression, cwd) => {
  const result = Bun.spawnSync(['cue', 'export', path, '-e', expression], cwd ? { cwd } : undefined);
  if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
  return JSON.parse(new TextDecoder().decode(result.stdout));
};
let cachedPolicy;
export function passportAdvertisementPolicy() {
  return cachedPolicy ||= cue(fileURLToPath(new URL('../passport-advertisement.cue', import.meta.url)), 'passportAdvertisement');
}
const token = value => String(value).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '').slice(0, 150);
const at = (row, path) => path.split('.').reduce((part, key) => part?.[key], row);
const pyString = value => JSON.stringify(String(value)).replace(/[\u007f-\uffff]/g, char =>
  `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`);
const canonical = value => Array.isArray(value) ? `[${value.map(canonical).join(', ')}]`
  : value && typeof value === 'object' ? `{${Object.keys(value).toSorted().map(key => `${pyString(key)}: ${canonical(value[key])}`).join(', ')}}`
    : typeof value === 'string' ? pyString(value) : JSON.stringify(value);

export function advertisePassport(passport, policy = passportAdvertisementPolicy(), options = {}) {
  const terms = [];
  for (const term of policy.term_arrays) for (const value of at(passport, term.path) || [])
    terms.push(term.prefix + token(value));
  for (const term of policy.term_scalars) {
    const value = at(passport, term.path) ?? term.fallback;
    if (value === undefined) throw new Error(`passport missing ${term.path}`);
    terms.push(term.prefix + token(value));
  }
  if (passport.reuse?.varies_per_item) terms.push(policy.varies_per_item_term);
  if (passport.credential) for (const prefix of policy.credential_prefixes)
    terms.push(prefix + token(passport.credential));
  const admitted = passport.state === policy.admitted_state, id = passport.passport;
  return { identity: policy.identity_prefix + id, candidate_kind: policy.candidate_kind,
    owner_ref: policy.owner_prefix + id,
    exact_verifier: options.verifier || (admitted ? policy.admitted_verifier : policy.declared_verifier),
    lowerer_ref: options.lowerer || (admitted && passport.census?.length ? policy.admitted_lowerer : policy.declared_lowerer),
    validity_frontier: `sha256:${createHash('sha256').update(canonical(passport)).digest('hex')}`,
    terms: [...new Set(terms.filter(term => term.split(':', 2)[1]))].toSorted(),
    dependencies: [policy.owner_prefix + id, ...(passport.census || []).map(item => policy.census_prefix + item)],
    name: passport.name, cannot_infer: passport.cannot_infer };
}

export function compilePassports(passports, policy = passportAdvertisementPolicy()) {
  const advertisements = passports.map(passport => advertisePassport(passport, policy));
  return { advertisements, summary: { advertisements: advertisements.length,
    admitted: advertisements.filter(row => row.terms.includes('admission:' + token(policy.admitted_state))).length } };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index += 2) {
      if (!argv[index]?.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[argv[index].slice(2)] = argv[index + 1];
    }
    if (!args.out) throw new Error('--out is required');
    const passports = args.json ? JSON.parse(readFileSync(args.json, 'utf8'))
      : cue('.', 'nativeSourcePassports', fileURLToPath(new URL('../../../schemas/control/', import.meta.url)));
    const result = compilePassports(passports);
    writeFileSync(args.out, result.advertisements.map(row => JSON.stringify(row)).join('\n') +
      (result.advertisements.length ? '\n' : ''));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`source passports: ${error.message}`); process.exitCode = 2; }
}
