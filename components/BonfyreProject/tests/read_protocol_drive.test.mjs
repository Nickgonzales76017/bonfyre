// What this does not prove:
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, copyFileSync, symlinkSync, existsSync, truncateSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { selectReadFacet } from '../tools/read-protocol-contract.mjs';

const roots = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
const hash = text => createHash('sha256').update(text).digest('hex');
const shape = relation => ({ phenotype_derivation: 'shape', relation_kind: relation, protocol_occurrence: 'protocol:read' });
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-read-protocol-')); roots.push(root);
  const tools = join(root, 'components/BonfyreProject/tools'), bin = join(root, 'bin');
  const sweep = join(root, 'sweep'), receipts = join(sweep, 'receipts'), log = join(root, 'calls.jsonl');
  for (const dir of [tools, bin, receipts, join(root, 'cmd/BonfyreFacet'), join(root, 'cmd/BonfyreLex')]) mkdirSync(dir, { recursive: true });
  for (const name of ['contract-drive.sh', 'enact-drive.sh', 'read-protocol-contract.mjs'])
    copyFileSync(fileURLToPath(new URL(`../tools/${name}`, import.meta.url)), join(tools, name));
  symlinkSync(process.execPath, join(bin, 'bun'));
  for (const [name, binary] of Object.entries({ dirname: '/usr/bin/dirname', mktemp: '/usr/bin/mktemp', rm: '/bin/rm', rmdir: '/bin/rmdir', shasum: '/usr/bin/shasum', cut: '/usr/bin/cut' }))
    symlinkSync(binary, join(bin, name));
  for (const cmd of ['python', 'python3', 'curl', 'wget', 'ssh'])
    writeFileSync(join(bin, cmd), '#!/bin/sh\nprintf forbidden > "$FORBIDDEN_LOG"\nexit 91\n', { mode: 0o755 });
  symlinkSync(fileURLToPath(new URL('../../../cmd/BonfyreLex/bonfyre-lex', import.meta.url)), join(root, 'cmd/BonfyreLex/bonfyre-lex'));
  writeFileSync(join(root, 'cmd/BonfyreFacet/bonfyre-facet'), `#!/usr/bin/env bun
import {readFileSync, appendFileSync} from 'node:fs';
const args=process.argv.slice(2), mode=args[0];
const value=key=>args[args.indexOf(key)+1];
appendFileSync(process.env.CALL_LOG, JSON.stringify(args)+'\\n');
const plans=JSON.parse(readFileSync(process.env.MOCK_PLAN,'utf8'));
const plan=plans[value('--occurrence')] || {};
if(mode==='compile') {
  process.stdout.write(plan.raw ?? JSON.stringify(plan.doc ?? {facets:[]}));
  process.exit(plan.compileCode ?? 0);
}
if(args.includes('--grant-authority')) process.exit(97);
if(plan.ownerCode) process.exit(plan.ownerCode);
appendFileSync(value('--out'),JSON.stringify({occurrence_id:value('--occurrence'),mode,relation:value('--relation')})+'\\n');
console.log('{}');
`, { mode: 0o755 });
  const plan = join(root, 'plans.json');
  writeFileSync(plan, '{}');
  return { root, tools, sweep, log, plans(data) { writeFileSync(plan, JSON.stringify(data)); },
    receipt(id, extra = {}) {
      const body = join(root, `${id}.body`); writeFileSync(body, `body:${id}`);
      writeFileSync(join(receipts, `${id}.json`), JSON.stringify({ twin_id: `external:${id}`, body_path: body,
        content_sha256: hash(`body:${id}`), policy_decision: 'allow', http_status: 200,
        fetch_attempted: true, transport_exit: 0, observed_at: '2026-10-02T01:00:00Z', ...extra }));
    }, calls() { return existsSync(log) ? readFileSync(log, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)) : []; },
    run(mode, env = {}) {
      const out = join(root, `${mode}.jsonl`);
      const result = Bun.spawnSync(['/bin/sh', join(tools, `${mode}-drive.sh`), sweep, out], { cwd: root,
        env: { ...process.env, PATH: bin, TMPDIR: root, CORPUS: join(root, 'corpus.jsonl'), ENACT_DB: join(root, 'local.db'),
          CALL_LOG: log, MOCK_PLAN: plan, FORBIDDEN_LOG: join(root, 'forbidden'), ...env } });
      expect(existsSync(join(root, 'forbidden'))).toBe(false);
      return { code: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString(), out };
    } };
}

test('read selector prefers DESCRIBE then QUERY then DISCOVER and never domain or non-read facets', () => {
  expect(selectReadFacet({ facets: [shape('PUBLISH'), shape('DISCOVER'), shape('QUERY'),
    { ...shape('DESCRIBE'), phenotype_derivation: 'domain' }] })).toEqual({ protocol: 'protocol:read', relation: 'QUERY' });
  expect(selectReadFacet({ facets: [shape('QUERY'), shape('DESCRIBE')] }).relation).toBe('DESCRIBE');
  expect(selectReadFacet({ facets: [shape('PUBLISH')] })).toBeNull();
  expect(() => selectReadFacet({ facets: [null] })).toThrow();
  expect(() => selectReadFacet({ facets: [{ ...shape('QUERY'), protocol_occurrence: '--grant-authority x' }] })).toThrow();
});

test('contract entrypoint delegates only selected read facets to native owner and reports compiler failures separately', () => {
  const f = fixture();
  for (const id of ['a', 'b', 'c', 'd', 'e']) f.receipt(id);
  f.plans({
    'external:a': { doc: { facets: [shape('QUERY'), shape('DESCRIBE')] } },
    'external:b': { doc: { facets: [shape('DESCRIBE')] }, compileCode: 7 },
    'external:c': { raw: '{' },
    'external:d': { doc: { facets: [shape('PUBLISH')] } },
    'external:e': { doc: { facets: [shape('DISCOVER')] }, ownerCode: 4 },
  });
  const result = f.run('contract');
  expect(result.code).toBe(0);
  const summary = JSON.parse(result.stdout);
  expect(summary).toMatchObject({ considered: 5, contracts_compiled: 1, refused: 4, compiler_failed: 1, invalid_compiler_output: 1 });
  const owners = f.calls().filter(args => args[0] === 'contract');
  expect(owners).toHaveLength(2);
  expect(owners[0][owners[0].indexOf('--relation') + 1]).toBe('DESCRIBE');
  expect(owners.every(args => !args.includes('--grant-authority'))).toBe(true);
  expect(readFileSync(result.out, 'utf8').trim().split('\n')).toHaveLength(1);
});

test('enact entrypoint gates transport, digest, permission and timestamp without granting authority', () => {
  const f = fixture();
  f.receipt('a'); f.receipt('b', { transport_exit: 28 }); f.receipt('c', { fetch_attempted: false });
  f.receipt('d', { policy_decision: 'robots_disallow' }); f.receipt('e', { content_sha256: '0'.repeat(64) });
  f.receipt('f', { observed_at: '' });
  f.plans({ 'external:a': { doc: { facets: [shape('DESCRIBE')] } }, 'external:f': { doc: { facets: [shape('QUERY')] } } });
  const actor = 'caller asserted actor';
  const result = f.run('enact', { ACTOR: actor });
  expect(result.code).toBe(0);
  expect(JSON.parse(result.stdout)).toMatchObject({ considered: 2, enacted: 1, refused: 1,
    not_permitted_or_not_fetched: 3, body_drifted_from_receipt: 1, actor });
  expect(JSON.parse(result.stdout).actor_basis).toContain('caller assertion');
  const owners = f.calls().filter(args => args[0] === 'enact');
  expect(owners).toHaveLength(1);
  expect(owners[0]).toContain('--evidence-stands'); expect(owners[0]).toContain('--discharge');
  expect(owners[0]).not.toContain('--grant-authority');
  expect(owners[0][owners[0].indexOf('--actor') + 1]).toBe(actor);
});

test('both actual wrappers refuse malformed selected protocol instead of passing split arguments', () => {
  for (const mode of ['contract', 'enact']) {
    const f = fixture(); f.receipt('a');
    f.plans({ 'external:a': { doc: { facets: [{ ...shape('DESCRIBE'), protocol_occurrence: 'protocol:x --grant-authority' }] } } });
    const result = f.run(mode);
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout).invalid_compiler_output).toBe(1);
    expect(f.calls().every(args => args[0] === 'compile')).toBe(true);
  }
});

test('missing native dependency refuses before truncating prior output', () => {
  const f = fixture();
  const out = join(f.root, 'contract.jsonl'); writeFileSync(out, 'prior evidence');
  rmSync(join(f.root, 'cmd/BonfyreFacet/bonfyre-facet'));
  expect(f.run('contract').code).not.toBe(0);
  expect(readFileSync(out, 'utf8')).toBe('prior evidence');
});

test('Bun selection boundary refuses oversized and symlinked compiler files', () => {
  const f = fixture(), raw = join(f.root, 'huge.json'), link = join(f.root, 'link.json');
  writeFileSync(raw, ''); truncateSync(raw, 8 * 1024 * 1024 + 1); symlinkSync(raw, link);
  for (const path of [raw, link]) {
    const result = Bun.spawnSync([process.execPath, join(f.tools, 'read-protocol-contract.mjs'), 'pick', path]);
    expect(result.exitCode).toBe(1);
  }
});

test('native argv values cannot smuggle grant-authority flags or corrupt interpolated owner JSON', () => {
  for (const actor of ['--grant-authority', 'actor"quoted', 'actor\\escape', 'actor\ncontrol']) {
    const f = fixture(); f.receipt('a');
    f.plans({ 'external:a': { doc: { facets: [shape('DESCRIBE')] } } });
    const out = join(f.root, 'enact.jsonl'); writeFileSync(out, 'prior');
    expect(f.run('enact', { ACTOR: actor }).code).toBe(1);
    expect(readFileSync(out, 'utf8')).toBe('prior');
    expect(f.calls()).toHaveLength(0);
  }
  expect(() => selectReadFacet({ facets: [{ ...shape('DESCRIBE'), protocol_occurrence: '--grant-authority' }] })).toThrow();
  const f = fixture(); f.receipt('a', { observed_at: '--grant-authority' });
  f.plans({ 'external:a': { doc: { facets: [shape('DESCRIBE')] } } });
  const result = f.run('enact');
  expect(JSON.parse(result.stdout).refused).toBe(1);
  expect(f.calls().every(args => args[0] === 'compile')).toBe(true);
});
