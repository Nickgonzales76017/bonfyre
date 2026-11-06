#!/usr/bin/env bun
// Rebuilds the backfilled handoff ledgers from what this machine actually
// holds: the retained claude-skills tree and index (ForeignTwin bodies), the
// scope bundle a ChatGPT session handed over on 2026-10-19, and the onboarding
// target lists in integrations/browser/onboarding. Nothing here is fetched.
// The ledgers carry separate progress columns so a later session records how
// far each row got instead of rereading a chat.
//
//   bun docs/engineering/handoff-backfill-20261019/build.mjs [--bundle DIR] [--check]
//
// --check rebuilds in memory and fails if a committed ledger differs, or if
// the retained bodies no longer match the digests the handoff recorded.
import { existsSync } from 'node:fs';
import { tsv, writeTSV, preserveLedger, indexRows, observedReader, commitOutputs, suppliedPlatforms } from './ledger-preservation.mjs';
import { createHash } from 'node:crypto';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir } from 'node:os';

const here = dirname(fileURLToPath(import.meta.url)), repo = resolve(here, '../../..');
const { read: readFileSync, observations } = observedReader();
const arg = name => { const i = process.argv.indexOf(name); return i > 0 ? process.argv[i + 1] : null; };
const check = process.argv.includes('--check');
const bundle = arg('--bundle') ?? join(homedir(), 'Downloads/aurekai-skills-to-native-demos');
const bodies = join(homedir(), '.bonfyre/foreign/bodies');
const sha = b => createHash('sha256').update(b).digest('hex');

const PIN = '19392f7a08264ed00486a251f5b2098321771f94';
const RETAINED = {
  tree: ['claude-skills-tree-19392f7a-20261019T033726Z-688d28f31a.body', '783c7d578f48bd6e94719c0953614e166a8d488538d6b16577ccacb29857bc09'],
  index: ['claude-skills-index-19392f7a-20261019T033810Z-f07e92c353.body', '6de1179228ce9358b4b2f27a3d85ed685119fc158770b9f4c2cfd38da4b17588'],
};
const PATH_DIGEST = '063a11c3599ae8ba19753fdd47d7f0ece6c4566904c582199255fb261dbb7cb8';

// Progress is recorded per row and per stage; one stage never implies the next.
const STAGES = ['source_reviewed', 'operation_implemented', 'ui_complete', 'native_continuation', 'downstream_used', 'public_deployed', 'live_verified'];

const write = writeTSV;

const problems = [];
const need = (ok, why) => { if (!ok) problems.push(why); };

// ------------------------------------------------------------ skills
const treeBody = readFileSync(join(bodies, RETAINED.tree[0])), indexBody = readFileSync(join(bodies, RETAINED.index[0]));
need(sha(treeBody) === RETAINED.tree[1], 'retained tree body changed');
need(sha(indexBody) === RETAINED.index[1], 'retained index body changed');
const tree = JSON.parse(treeBody), index = JSON.parse(indexBody);
need(tree.sha === PIN && tree.truncated === false, 'tree is not the pinned, untruncated tree');
const skillBlobs = tree.tree.filter(e => e.type === 'blob' && e.path.endsWith('/SKILL.md') && !e.path.split('/').some(p => p.startsWith('.')));
const paths = skillBlobs.map(e => e.path).sort();
need(paths.length === 388, `expected 388 skill files, tree has ${paths.length}`);
need(sha(paths.join('\n') + '\n') === PATH_DIGEST, 'skill path list digest differs from the native census');
const indexed = new Set(index.skills.map(s => s.source.replace(/^(\.\.\/)+/, '') + '/SKILL.md'));
const blobOf = new Map(skillBlobs.map(e => [e.path, e]));

const scopeFile = join(bundle, 'all-388-native-demo-scope.tsv');
const committedScope = join(here, 'skills-native-demo-scope.tsv');
const planRows = tsv(readFileSync(existsSync(scopeFile) ? scopeFile : committedScope, 'utf8'));
indexRows(planRows, 'source_path', 'skill planning source');
need(planRows.every(r => !r.source_commit || r.source_commit === PIN), 'planning rows name a different source commit');
const planned = new Map(planRows.map(r => [r.source_path, r]));
need(planned.size === 388, 'the planning rows do not cover 388 skills');
// Historical bootstrap observations only. Current ledger values, even blank stages, win at reconciliation.
const PROGRESS = {
  'business-operations/skills/capacity-planner/SKILL.md': { source_reviewed: '2026-10-19 chatgpt selected range', operation_implemented: '2026-10-19 components/BonfyreServiceSite/process-sim.mjs (Erlang C, staffing by hour, event simulation)' },
  'business-growth/skills/sales-engineer/SKILL.md': { source_reviewed: '2026-10-19 chatgpt selected range' },
  'product-team/skills/product-manager-toolkit/SKILL.md': { source_reviewed: '2026-10-19 chatgpt selected range' },
  'marketing-skill/skills/social-media-manager/SKILL.md': { source_reviewed: '2026-10-19 chatgpt selected range' },
};
const skillCols = ['source_path', 'source_domain', 'source_name', 'git_blob_sha', 'bytes', 'in_codex_index', 'same_skill_body_group', 'classification', 'candidate_demo_title', 'proposed_workbench', 'proposed_existing_owners', 'proposed_interactions', 'proposed_output', 'proposed_source_and_operation_connections', 'acceptance', ...STAGES, 'source_url'];
// Stage columns are filled by hand after a build, so they always come from the committed ledger.
const committedRows = new Map(existsSync(committedScope) ? tsv(readFileSync(committedScope, 'utf8')).map(r => [r.source_path, r]) : []);
const skills = paths.map(p => { const plan = planned.get(p) ?? {}, blob = blobOf.get(p), prev = committedRows.get(p) ?? {};
  return { ...plan, source_path: p, source_domain: p.split('/')[0], source_name: plan.source_name || p.split('/').at(-2), git_blob_sha: blob.sha, bytes: blob.size, in_codex_index: indexed.has(p),
    source_url: `https://github.com/alirezarezvani/claude-skills/blob/${PIN}/${p}`, ...Object.fromEntries(STAGES.map(s => [s, prev[s] ?? ''])), ...PROGRESS[p] }; });
need(skills.filter(s => s.in_codex_index).length === 364, 'index coverage is not 364');

// ------------------------------------------------------------ platforms
const platformFile = join(bundle, 'all-100-platforms-as-supplied.tsv'), committedPlatforms = join(here, 'platforms-100.tsv');
const platformDefaults = tsv(readFileSync(existsSync(platformFile) ? platformFile : committedPlatforms, 'utf8'));
const supplied = readFileSync(join(here, 'platforms-100-as-supplied.md'), 'utf8');
need(sha(supplied) === '090e7c8e82506c07e484425eddbafca89c258c6126e70945a18126506f8e4352', 'original platform attachment changed');
const originalPlatforms = suppliedPlatforms(supplied);
const platformIdentity = Object.keys(originalPlatforms[0]);
const platforms = preserveLedger(originalPlatforms, platformDefaults, { keys: 'source_number', immutable: platformIdentity, name: 'platform source' });
need(platforms.length === 100, `expected 100 platforms, have ${platforms.length}`);
const OPERATIONS = ['public_discovery', 'account_read', 'embed_or_link', 'import_export', 'author_or_publish', 'membership_or_commerce', 'distribution_or_settlement'];
const platformCols = ['source_number', 'source_name', 'supplied_tier', 'source_category', 'supplied_claim', 'source_line', 'source_sha256', 'verification_status', 'verification_note', ...OPERATIONS, 'integration_status', 'runtime_authority'];

// ------------------------------------------------------------ onboarding
// The unit of work is a capability for one identity at one provider scope, not a tab.
const onboard = join(repo, 'integrations/browser/onboarding');
const targets = tsv(readFileSync(join(onboard, 'targets.tsv'), 'utf8')), p0 = tsv(readFileSync(join(onboard, 'p0-proton.tsv'), 'utf8'));
indexRows(targets, 'source_id', 'onboarding targets');
indexRows(p0, ['provider', 'scope'], 'P0 references');
need(targets.length === 200, `expected 200 onboarding targets, have ${targets.length}`);
const ENROLL = ['existing_access_checked', 'account_bound', 'credential_sealed', 'credential_verified', 'scope_confirmed', 'consumer_bound'];
// Where each target is entered, from the list the retired launcher carried.
const launcher = readFileSync(join(onboard, 'open-accounts.command'), 'utf8'), listed = /cat <<'TARGETS'\r?\n([\s\S]*?)\r?\nTARGETS/.exec(launcher)?.[1] ?? '';
const signupRows = listed.split('\n').filter(Boolean).map(l => { const [id, , url] = l.split('|'); return { id, url }; });
indexRows(signupRows, 'id', 'historical signup addresses');
const signup = new Map(signupRows.map(r => [r.id, r.url]));
need(signup.size === 200 && targets.every(t => /^https:\/\//.test(signup.get(t.source_id) ?? '')), 'every target needs a signup address from the launcher list');
const enrollCols = ['work_id', 'source_id', 'platform', 'signup_url', 'identity', 'account_group', 'source_tier', 'supplied_state', 'api_gate', 'credential_account', ...ENROLL, 'interruption', 'note'];
const IDENTITY = { gmail: 'nickgonzales76017@gmail.com', proton: 'nickgonzales76017@proton.me' };
const enrollment = targets.flatMap(t => Object.entries(IDENTITY).map(([lane, account]) => ({ work_id: `${t.source_id}-${lane}`, source_id: t.source_id, platform: t.platform, signup_url: signup.get(t.source_id), identity: lane,
  account_group: t.account_group, source_tier: t.source_tier, supplied_state: t[`${lane}_state`], api_gate: t.api_gate, credential_account: account,
  ...Object.fromEntries(ENROLL.map(s => [s, ''])), interruption: '', note: '' })));
const recoveryCols = ['provider', 'scope', 'candidate_binding_names', 'reference_locations', 'evidence_basis', 'proton_action', 'existing_access_checked', 'credential_sealed', 'note'];
const recovery = p0.map(r => ({ ...r, existing_access_checked: '', credential_sealed: '', note: '' }));

// ------------------------------------------------------------ instruments
// What the sessions built or promised as working demos, and where each one really is.
const existingInstruments = new Map(existsSync(join(here, 'demo-instruments.tsv')) ? tsv(readFileSync(join(here, 'demo-instruments.tsv'), 'utf8')).map(r => [r.id, r]) : []);
const instruments = [
  ['process-simulation', 'Queued-work simulation and staffing', 'business-operations/skills/capacity-planner', 'components/BonfyreServiceSite/process-sim.mjs', 'built 2026-10-19 in this repo; earlier ChatGPT build was sandbox-only and called fake by Nick'],
  ['research-synthesis', 'Literature screening and synthesis with exact excerpts', 'research/*', 'components/BonfyreServiceSite/synthesis.mjs', 'built 2026-10-19 in this repo; earlier ChatGPT build was sandbox-only and called fake by Nick'],
  ['schema-editing', 'Schema and migration workspace preserving SQLite rows and foreign keys', 'engineering/*', '', 'ChatGPT sandbox only (51 tests reported there); not in repo'],
  ['dependency-planning', 'Dependency and critical-path planner', 'project-management/*', '', 'ChatGPT sandbox only; not in repo'],
  ['pricing', 'Pricing instrument', 'commercial/*', '', 'ChatGPT sandbox only; not in repo'],
  ['staffing', 'Hiring sequence with ramp and attrition', 'business-operations/skills/capacity-planner', '', 'ChatGPT sandbox only; staffing by hour now lives in process-sim.mjs'],
  ['campaign-composer', 'Linked artboards, per-format overrides, layered scenes, PNG/WebM export', 'marketing-skill/skills/social-media-manager', '', 'ChatGPT sandbox only; not in repo'],
  ['audio-assembly', 'Reordered passages with retimed captions, WAV/VTT', 'marketing-skill/*', 'components/BonfyreServiceSite/creator-*.mjs', 'live Audio workspace exists; ChatGPT caption retiming is sandbox only'],
  ['sales-poc-desk', 'Requirement grid, competitor matrix, POC results, proposal', 'business-growth/skills/sales-engineer', '', 'specified only'],
  ['product-discovery', 'Source-linked passages, RICE, sensitivity, roadmap', 'product-team/skills/product-manager-toolkit', '', 'specified only'],
].map(([id, title, donor, source, state]) => { const prior = existingInstruments.get(id) ?? {}; return { id, title, donor, source, ...Object.fromEntries(STAGES.map(s => [s, prior[s] ?? ''])), state: prior.state || state }; });
const instrumentCols = ['id', 'title', 'donor', 'source', 'state', ...STAGES];

// Rebuild source fields without replacing current targets, observations or progress.
const retained = (name, rows, columns, keys, immutable = []) => {
  const at = join(here, name), prior = existsSync(at) ? tsv(readFileSync(at, 'utf8')) : null;
  return write(preserveLedger(rows, prior, { keys, immutable, name }), columns);
};
const out = {
  'skills-native-demo-scope.tsv': retained('skills-native-demo-scope.tsv', skills, skillCols, 'source_path', ['source_domain', 'git_blob_sha', 'bytes', 'in_codex_index', 'source_url', 'source_commit']),
  'platforms-100.tsv': retained('platforms-100.tsv', platforms.map(p => ({ ...Object.fromEntries(OPERATIONS.map(o => [o, ''])), ...p })), platformCols, 'source_number', platformIdentity),
  'onboarding-enrollment.tsv': retained('onboarding-enrollment.tsv', enrollment, enrollCols, 'work_id', ['source_id', 'identity', 'credential_account']),
  'onboarding-recovery-p0.tsv': retained('onboarding-recovery-p0.tsv', recovery, recoveryCols, ['provider', 'scope']),
  'demo-instruments.tsv': retained('demo-instruments.tsv', instruments, instrumentCols, 'id'),
};
// Domains stay independently enumerable; their counts derive from the pinned tree.
const domainFile = existsSync(join(bundle, 'domain-allocation.tsv')) ? join(bundle, 'domain-allocation.tsv') : join(here, 'skills-domain-allocation.tsv');
const domains = tsv(readFileSync(domainFile, 'utf8'));
indexRows(domains, 'source_domain', 'domain allocation');
const counts = new Map(); for (const path of paths) { const domain = path.split('/')[0]; counts.set(domain, (counts.get(domain) ?? 0) + 1); }
need(domains.length === 20 && counts.size === 20 && domains.every(d => String(d.source_skill_files) === String(counts.get(d.source_domain))), 'domain membership/counts differ from the pinned tree');
out['skills-domain-allocation.tsv'] = retained('skills-domain-allocation.tsv', domains, ['source_domain', 'source_skill_files', 'proposed_workbench', 'proposed_owner_families', 'proposed_interactions', 'proposed_output', 'proposed_connections'], 'source_domain', ['source_skill_files']);
need(enrollment.length === 400 && recovery.length === 34, 'onboarding scope differs from 400 enrollment / 34 P0 rows');
// Validation and read preimages precede any writes. --check compares every field.
// Unchanged files are untouched. Replacements are per-file, not a cross-file transaction.
const result = commitOutputs(Object.entries(out).map(([name, text]) => ({ path: join(here, name), text })), observations, { check, problems });
console.log(JSON.stringify({ check, skills: skills.length, indexed: skills.filter(s => s.in_codex_index).length, platforms: platforms.length, enrollment: enrollment.length, recovery: recovery.length, instruments: instruments.length, domains: domains.length, changed_files: result.changed.map(p => p.slice(here.length + 1)) }));
