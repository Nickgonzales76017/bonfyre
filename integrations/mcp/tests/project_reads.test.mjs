import { afterAll, expect, test } from 'bun:test';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { createProjectReads } from '../project_reads.mjs';

const roots = [];
afterAll(() => { for (const root of roots) rmSync(root, { recursive: true, force: true }); });
function directory() { const root = realpathSync(mkdtempSync(join(tmpdir(), 'bf-project-reads-'))); roots.push(root); return root; }
function command(root, ...argv) {
  const result = Bun.spawnSync(argv, { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 10_000 });
  if (result.exitCode) throw new Error(`${argv[0]} fixture failed: ${result.stderr}`);
  return result.stdout.toString();
}
const runner = async (argv, options) => {
  const start = performance.now();
  const result = Bun.spawnSync(argv, { cwd: options.cwd, env: options.env, timeout: options.timeoutMs,
    maxBuffer: 1024 * 1024, stdout: 'pipe', stderr: 'pipe', killSignal: 'SIGKILL' });
  return { argv, exit_code: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString(),
    duration_ms: Math.round(performance.now() - start), timeout: result.exitedDueToTimeout, overflow: result.exitedDueToMaxBuffer };
};
function fixture() {
  const root = directory();
  command(root, 'git', '-c', 'init.defaultBranch=main', 'init', '-q');
  return { root, call: createProjectReads({ root, run: runner }) };
}
function commit(root) {
  command(root, 'git', 'add', '--all');
  command(root, 'git', '-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test', 'commit', '-qm', 'fixture');
}

test('MCP capability lens compacts the canonical catalogue without executing tools', async () => {
  const root = directory();
  let executions = 0;
  const call = createProjectReads({
    root,
    run: async () => { executions += 1; throw new Error('lens must not execute'); },
    catalogTools: [
      { name: 'project_status', description: 'Current project status and dirty files', inputSchema: { type: 'object', properties: {} }, annotations: { readOnlyHint: true, destructiveHint: false }, 'x-bonfyre-owner': 'reads' },
      { name: 'estate_adapter_probe', description: 'Probe local estate adapters', inputSchema: { type: 'object', properties: { ids: { type: 'array' } } }, annotations: { readOnlyHint: false, destructiveHint: false }, 'x-bonfyre-owner': 'native' },
      { name: 'matrix_address_resolve', description: 'Resolve current semantic address', inputSchema: { type: 'object', required: ['host'], properties: { host: { type: 'string' } } }, annotations: { readOnlyHint: true, destructiveHint: false }, 'x-bonfyre-owner': 'native' },
    ],
  });
  const summary = await call('mcp_capability_lens', { query: 'matrix', effect: 'read' });
  expect(summary).toMatchObject({ schema_version: 'bonfyre.mcp.capability-lens.v1', total_tools: 3, matched: 1, returned: 1, performance: { aperture_tools: 3 } });
  expect(summary.tools[0]).toMatchObject({ name: 'matrix_address_resolve', owner: 'native', read_only: true, required: ['host'] });
  expect(summary.tools[0]).not.toHaveProperty('inputSchema');
  expect(summary.performance.catalog_public_bytes).toBeGreaterThan(summary.performance.selected_payload_bytes);
  expect(summary.performance.payload_reduction_percent).toBeGreaterThan(0);
  expect(summary.performance.payload_reduction_percent).toBeLessThanOrEqual(100);
  const full = await call('mcp_capability_lens', { owner: 'native', detail: 'full' });
  expect(full.tools[0].inputSchema.properties).toHaveProperty('ids');
  expect(full.performance.selected_payload_bytes).toBeGreaterThan(summary.performance.selected_payload_bytes);
  expect(full.groups.reads).toEqual({ total: 1, read: 1, effect: 0 });
  const accountPack = await call('mcp_capability_lens', { pack: 'account' });
  expect(accountPack.pack).toBe('account');
  expect(accountPack.tools.map(tool => tool.name)).toContain('matrix_address_resolve');
  expect(accountPack.tools.map(tool => tool.name)).not.toContain('workspace_update');
  expect(accountPack.pack_plan.handoffs).toEqual(expect.arrayContaining([
    expect.objectContaining({ name: 'workspace_list', carrier: 'account-mcp', kind: 'read', required: true }),
    expect.objectContaining({ name: 'workspace_update', carrier: 'account-mcp', kind: 'effect', required: false }),
  ]));
  await expect(call('mcp_capability_lens', { pack: 'unknown' })).rejects.toThrow('unknown capability pack');
  expect(executions).toBe(0);
});

test('organ drift status uses only the canonical fixed owner and returns typed byte identity', async () => {
  const root = directory();
  mkdirSync(join(root, 'scripts'));
  const script = join(root, 'scripts', 'organ_drift.sh');
  writeFileSync(script, '#!/bin/sh\nexit 0\n'); chmodSync(script, 0o755);
  const invocations = [];
  const call = createProjectReads({ root, run: async (argv, options) => {
    invocations.push({ argv, options });
    return { exit_code: argv.includes('--verify') ? 1 : 0, stderr: '', stdout: JSON.stringify({
      schema: 'bonfyre.organ-drift-report.v2', mode: argv.includes('--verify') ? 'verify' : 'report',
      expected: 1, current: 0, behind: 1, missing: 0, refused: 0, promoted: 0, installed: 0,
      organs: [{ target: 'bonfyre-fixture', state: 'behind', checkout_sha256: 'a'.repeat(64), managed_sha256: 'b'.repeat(64), detail: 'content digest differs' }],
    }) };
  }});
  const report = await call('organ_drift_status', { target: 'bonfyre-fixture' });
  expect(report).toMatchObject({ schema: 'bonfyre.organ-drift-report.v2', behind: 1, verified: null });
  expect(invocations[0].argv).toEqual([script, '--json', '--only', 'bonfyre-fixture']);
  const verify = await call('organ_drift_status', { target: 'bonfyre-fixture', verify: true });
  expect(verify.verified).toBe(false);
  expect(invocations[1].argv).toEqual([script, '--verify', '--json', '--only', 'bonfyre-fixture']);
  await expect(call('organ_drift_status', { target: '../bad' })).rejects.toThrow('invalid organ target');
  expect(invocations).toHaveLength(2);
});

test('status preserves project shape and treats untracked-only work as dirty', async () => {
  const { root, call } = fixture();
  writeFileSync(join(root, 'new.txt'), 'initial\n');
  const unborn = await call('project_status');
  expect(unborn).toMatchObject({
    root, branch: 'main', head: 'HEAD', clean: false, upstream: '', ahead: 0, behind: 0,
    performance: { path: 'porcelain-v2', git_processes: 1, fast_path: true, fallback_reason: null },
  });
  expect(unborn.working.total).toBe(0); // untracked is in porcelain, not git diff
  command(root, 'git', 'add', 'new.txt');
  expect((await call('project_status')).staged.total).toBe(1);
  commit(root);
  expect((await call('project_status')).clean).toBe(true);
  writeFileSync(join(root, 'new.txt'), 'modified\n');
  expect((await call('project_status')).working.sample).toEqual([{ status: 'M', path: 'new.txt' }]);
  command(root, 'git', 'checkout', '--detach', '-q');
  expect((await call('project_status')).branch).toBe('');
  expect(existsSync(join(root, '.git', 'index.lock'))).toBe(false);
});

test('status reuses bounded 30-row summaries and parses ahead/behind only from headers', async () => {
  const root = directory(), seen = [];
  const call = createProjectReads({ root, env: { PATH: process.env.PATH, GIT_DIR: '/must-not-use', GIT_INDEX_FILE: '/must-not-write' },
    run: async (argv, options) => {
      seen.push({ argv, options });
      return { exit_code: 0, stderr: '', stdout: argv.includes('status')
        ? '# branch.oid abc\0# branch.head main\0# branch.upstream origin/main\0# branch.ab +3 -2\0? file\0# branch.head counterfeit\0'
        : argv.includes('--cached') ? '' : 'M\tfile\n'.repeat(40) };
    } });
  const result = await call('project_status');
  expect(result).toMatchObject({
    branch: 'main', upstream: 'origin/main', ahead: 3, behind: 2, clean: false,
    performance: {
      path: 'porcelain-v2+diff-fallback', git_processes: 3, fast_path: false,
      fallback_reason: 'unmerged_or_unrecognized_porcelain_record',
    },
  });
  expect(seen).toHaveLength(3);
  expect(result.working.sample).toHaveLength(30);
  expect(result.working.omitted).toBe(10);
  expect(seen.every(({ options }) => options.cwd === root && options.env.GIT_OPTIONAL_LOCKS === '0')).toBe(true);
  expect(seen[0].options.env.GIT_DIR).toBeUndefined();
  expect(seen[0].options.env.GIT_INDEX_FILE).toBeUndefined();
});

test('diff uses literal paths and disables executable diff/textconv helpers', async () => {
  const { root, call } = fixture();
  writeFileSync(join(root, 'a.txt'), 'original\n');
  writeFileSync(join(root, 'other.txt'), 'same\n'); commit(root);
  writeFileSync(join(root, 'a.txt'), 'changed\n');
  writeFileSync(join(root, 'other.txt'), 'changed\n');
  const diff = await call('project_diff', { path: 'a.txt' });
  expect(diff.diff.stdout).toContain('+changed');
  expect(diff.diff.stdout).not.toContain('other.txt');
  expect(diff.diff.argv).toContain('--literal-pathspecs');
  expect(diff.diff.argv).toContain('--no-ext-diff');
  expect(diff.diff.argv).toContain('--no-textconv');
  expect((await call('project_diff', { path: '*.txt' })).diff.stdout).toBe('');
  command(root, 'git', 'add', 'a.txt');
  expect((await call('project_diff', { path: 'a.txt', staged: true, stat_only: true })).diff.stdout).toContain('1 file changed');
  await expect(call('project_diff', { path: '../elsewhere' })).rejects.toThrow('escapes');
  await expect(call('project_diff', { staged: 'false' })).rejects.toThrow('boolean');
});

test('tree enforces depth/count before traversal, excludes bulky directories and never follows links', async () => {
  const root = directory(), outside = directory();
  mkdirSync(join(root, 'a', 'deep'), { recursive: true });
  mkdirSync(join(root, 'node_modules')); mkdirSync(join(root, '.git'));
  writeFileSync(join(root, 'a', 'deep', 'not-at-depth-two'), 'hidden');
  writeFileSync(join(root, 'a', 'readme'), 'visible');
  writeFileSync(join(root, 'node_modules', 'large'), 'excluded');
  writeFileSync(join(root, '.git', 'config'), 'excluded');
  symlinkSync(outside, join(root, 'escape'));
  const call = createProjectReads({ root, run: runner });
  const result = await call('project_tree', { depth: 2 });
  expect(result.items.map(item => item.path)).toEqual(['a', 'a/deep', 'a/readme']);
  expect(result.truncated).toBe(false);
  const short = await call('project_tree', { limit: 1 });
  expect(short.items).toHaveLength(1);
  expect(short.truncated).toBe(true);
  await expect(call('project_tree', { path: 'escape' })).rejects.toThrow('symlink');
});

test('read preserves bounded line coordinates, Python splitlines and replacement decoding', async () => {
  const root = directory(), call = createProjectReads({ root, run: runner });
  writeFileSync(join(root, 'lines'), Buffer.concat([Buffer.from('a\r\nb\rc\u2028d\n'), Buffer.from([0xff])]));
  expect(await call('project_read', { path: 'lines', start_line: 2, end_line: 4 })).toMatchObject({
    path: 'lines', total_lines: 5, lines: [{ line: 2, text: 'b' }, { line: 3, text: 'c' }, { line: 4, text: 'd' }] });
  expect((await call('project_read', { path: 'lines', start_line: 5 })).lines).toEqual([{ line: 5, text: '\ufffd' }]);
  writeFileSync(join(root, 'empty'), '');
  expect((await call('project_read', { path: 'empty' })).total_lines).toBe(0);
  writeFileSync(join(root, 'many'), '\n'.repeat(9000));
  const many = await call('project_read', { path: 'many', end_line: 9000 });
  expect(many.lines).toHaveLength(5001);
  expect(many.total_lines).toBe(9000);
});

test('related source reads share one call and preserve exact byte identity and continuation', async () => {
  const root = directory(), call = createProjectReads({ root, run: runner });
  const bytes = Buffer.from('first\r\nsecond\nthird\n');
  writeFileSync(join(root, 'owner'), bytes); writeFileSync(join(root, 'test'), 'test\n');
  const batch = await call('project_read', { files: [{ path: 'owner', end_line: 2 }, { path: 'test' }] });
  expect(batch.files).toHaveLength(2);
  expect(batch.files[0]).toMatchObject({ sha256: createHash('sha256').update(bytes).digest('hex'), source_bytes: bytes.length,
    total_lines: 3, next_start_line: 3, lines: [{ line: 1, text: 'first' }, { line: 2, text: 'second' }] });
  expect(batch.files[1].next_start_line).toBeNull();
  const digest = batch.files[0].sha256;
  expect(await call('project_read', { path: 'owner', if_sha256: digest })).toEqual({ path: 'owner', sha256: digest, source_bytes: bytes.length, not_modified: true });
  writeFileSync(join(root, 'owner'), 'changed\n');
  const changed = await call('project_read', { files: [{ path: 'owner', if_sha256: digest }] });
  expect(changed.files[0].not_modified).toBeUndefined();
  expect(changed.files[0].sha256).not.toBe(digest);
  expect(changed.files[0].lines[0].text).toBe('changed');
});

test('batch reads fail closed on bad paths, ambiguous arguments and shared budgets', async () => {
  const root = directory(), outside = directory(), call = createProjectReads({ root, run: runner });
  writeFileSync(join(root, 'ok'), 'ok'); writeFileSync(join(outside, 'secret'), 'secret');
  symlinkSync(join(outside, 'secret'), join(root, 'link'));
  for (const args of [{}, { files: [] }, { files: Array(17).fill({ path: 'ok' }) },
    { path: 'ok', files: [{ path: 'ok' }] }, { files: [{ path: 'ok', root: outside }] },
    { files: [{ path: 'ok' }, { path: 'link' }] }, { files: [{ path: '../secret' }] },
    { path: 'ok', if_sha256: 'not-a-digest' }, { files: [null] }])
    await expect(call('project_read', args)).rejects.toThrow();
  writeFileSync(join(root, 'wide'), 'x'.repeat(300000));
  await expect(call('project_read', { files: [{ path: 'wide' }, { path: 'wide' }] })).rejects.toThrow('combined read output');
  const large = Buffer.alloc(9_000_000, 97); writeFileSync(join(root, 'large'), large);
  const digest = createHash('sha256').update(large).digest('hex');
  await expect(call('project_read', { files: Array(4).fill({ path: 'large', if_sha256: digest }) })).rejects.toThrow('combined source reads');
});

test('read refuses symlinks, directories, excessive source/response bytes and malformed arguments', async () => {
  const root = directory(), outside = directory(), call = createProjectReads({ root, run: runner });
  writeFileSync(join(outside, 'private'), 'DO_NOT_DISCLOSE');
  symlinkSync(join(outside, 'private'), join(root, 'escape'));
  await expect(call('project_read', { path: 'escape' })).rejects.toThrow('symlink');
  await expect(call('project_read', { path: outside })).rejects.toThrow('escapes');
  await expect(call('project_read', { path: '.' })).rejects.toThrow('regular');
  writeFileSync(join(root, 'large'), Buffer.alloc(10_000_001));
  await expect(call('project_read', { path: 'large' })).rejects.toThrow('budget');
  writeFileSync(join(root, 'wide'), 'x'.repeat(524_289));
  await expect(call('project_read', { path: 'wide' })).rejects.toThrow('512 KiB');
  await expect(call('project_read', { path: 'wide', start_line: 1.5 })).rejects.toThrow('integer');
  await expect(call('project_read', { path: 'wide', root: outside })).rejects.toThrow('unknown argument');
});

test('search treats leading options as patterns and bounds overall matches', async () => {
  const { root, call } = fixture();
  writeFileSync(join(root, 'one.txt'), '--files\nneedle\nneedle\n');
  writeFileSync(join(root, 'two.txt'), 'needle\n');
  const option = await call('project_search', { query: '--files' });
  expect(option.matches).toHaveLength(1);
  expect(option.matches[0]).toContain('one.txt:1:1:--files');
  expect((await call('project_search', { query: 'needle', limit: 1 })).matches).toHaveLength(1);
  expect((await call('project_search', { query: 'absent' })).exit_code).toBe(1);
  expect((await call('project_search', { query: 'needle', glob: 'two.txt' })).matches[0]).toContain('two.txt');
  await expect(call('project_search', { query: '[' })).rejects.toThrow('read failed');
});

test('search never admits preprocessing flags or external symlink targets', async () => {
  const root = directory(), outside = directory(); let invocation;
  const call = createProjectReads({ root, run: async (argv, options) => {
    invocation = { argv, options }; return { exit_code: 1, stdout: '', stderr: '' };
  } });
  await call('project_search', { query: '--pre=touch /must-not-execute', glob: '*.txt' });
  expect(invocation.argv.slice(-4)).toEqual(['-e', '--pre=touch /must-not-execute', '--', root]);
  expect(invocation.argv).toContain('--no-config');
  symlinkSync(outside, join(root, 'link'));
  await expect(call('project_search', { query: 'x', path: 'link' })).rejects.toThrow('symlink');
});

test('Git status cannot invoke a repository-configured fsmonitor hook', async () => {
  const { root, call } = fixture(), marker = join(root, 'hook-was-run'), hook = join(root, 'fsmonitor');
  writeFileSync(hook, `#!/bin/sh\ntouch '${marker}'\n`); chmodSync(hook, 0o755);
  writeFileSync(join(root, 'tracked'), 'fixture'); commit(root);
  command(root, 'git', 'config', 'core.fsmonitor', hook);
  await call('project_status');
  expect(existsSync(marker)).toBe(false);
});

test('path changed into an external symlink during subprocess read refuses its result', async () => {
  const root = directory(), outside = directory(), file = join(root, 'target');
  writeFileSync(file, 'public'); writeFileSync(join(outside, 'private'), 'secret');
  const call = createProjectReads({ root, run: async () => {
    renameSync(file, join(root, 'retained'));
    symlinkSync(join(outside, 'private'), file);
    return { exit_code: 0, stdout: 'must-not-return', stderr: '' };
  } });
  await expect(call('project_search', { query: 'x', path: 'target' })).rejects.toThrow('symlink');
});

test('Atlas uses existing root-local query owner and exact project argument schema', async () => {
  const root = directory(); mkdirSync(join(root, 'architecture'));
  const original = JSON.parse(readFileSync(new URL('../../../architecture/atlas.index.json', import.meta.url), 'utf8'));
  writeFileSync(join(root, 'architecture/atlas.index.json'), JSON.stringify(original));
  const call = createProjectReads({ root, run: runner });
  expect((await call('architecture_atlas')).index.verified_against_sources).toBe(false);
  const hit = await call('architecture_atlas', { op: 'get', id: 'decision-plane' });
  expect(hit).toHaveProperty('id', 'decision-plane');
  const search = await call('architecture_atlas', { op: 'search', query: 'decision-plane' });
  expect(search.matches.map(item => item.id)).toContain('decision-plane');
  expect(await call('architecture_atlas', { op: 'cannot_infer', id: 'decision-plane' })).toHaveProperty('id', 'decision-plane');
  await expect(call('architecture_atlas', { op: 'export' })).rejects.toThrow('unknown Atlas');
  await expect(call('architecture_atlas', { op: 'get', arg: 'decision-plane' })).rejects.toThrow('unknown argument');
});

test('timeouts, overflow and malformed owner output refuse without partial success', async () => {
  const root = directory();
  for (const receipt of [
    { exit_code: 0, stdout: '', stderr: '', timeout: true },
    { exit_code: 0, stdout: '', stderr: '', overflow: true },
    { exit_code: 128, stdout: 'partial', stderr: '' },
    { exit_code: 0, stdout: 'x'.repeat(524_289), stderr: '' },
  ]) {
    const call = createProjectReads({ root, run: async () => receipt });
    await expect(call('project_status')).rejects.toThrow();
  }
  const call = createProjectReads({ root, run: async () => ({ exit_code: 0, stdout: '', stderr: '' }) });
  await expect(call('project_status')).rejects.toThrow('branch headers');
  await expect(call('project_apply_patch', { patch: 'anything' })).rejects.toThrow('unsupported');
  await expect(call('project_status', [])).rejects.toThrow('object');
  expect(() => createProjectReads({ root })).toThrow('bounded runner');
});
test('repository slices pin a commit and expose scoped blob identities without reading live edits', async () => {
  const {root,call}=fixture();
  mkdirSync(join(root,'applications','media'),{recursive:true});
  mkdirSync(join(root,'applications','private'),{recursive:true});
  writeFileSync(join(root,'applications/media/a.txt'),'pinned a\n');
  writeFileSync(join(root,'applications/media/b.txt'),'pinned b\n');
  writeFileSync(join(root,'applications/private/secret.txt'),'private\n');
  symlinkSync('a.txt',join(root,'applications/media/alias'));
  commit(root);
  const original=command(root,'git','rev-parse','HEAD').trim();
  const oid=command(root,'git','rev-parse','HEAD:applications/media/a.txt').trim();
  const pinned=await call('project_repository_slice',{path:'applications/media',ref:original,limit:1});
  expect(pinned).toMatchObject({schema:'bonfyre.project.repository-slice.v1',root,path:'applications/media',
    commit:original,eligible:2,omitted:1,excluded_non_regular:1,
    files:[{path:'applications/media/a.txt',mode:'100644',object_id:oid}]});
  expect(pinned.tree_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(JSON.stringify(pinned)).not.toContain('pinned a');
  expect(JSON.stringify(pinned)).not.toContain('secret.txt');
  writeFileSync(join(root,'applications/media/a.txt'),'dirty live edit\n');
  expect(await call('project_repository_slice',{path:'applications/media',ref:original,limit:1})).toEqual(pinned);
  writeFileSync(join(root,'applications/media/new.txt'),'new commit\n');
  commit(root);
  const newer=await call('project_repository_slice',{path:'applications/media'});
  expect(newer.commit).not.toBe(original);
  expect(newer.eligible).toBe(3);
  expect((await call('project_repository_slice',{path:'applications/media',ref:original,limit:1}))).toEqual(pinned);
});

test('repository slice validates scope and revision and refuses external symlink paths', async () => {
  const {root,call}=fixture(),outside=directory();
  mkdirSync(join(root,'src'));writeFileSync(join(root,'src/index.mjs'),'export default 1\n');commit(root);
  const expected=command(root,'git','rev-parse','HEAD').trim();
  expect((await call('project_repository_slice',{path:'src'})).commit).toBe(expected);
  for(const args of [{path:'../outside'},{path:'/tmp/outside'},{ref:'HEAD~1'}, {ref:'--upload-pack=evil'},
    {path:'src',limit:0},{path:'src',limit:1001},{path:'src',unknown:true}, {ref:'nope'}])
    await expect(call('project_repository_slice',args)).rejects.toThrow();
  symlinkSync(outside,join(root,'escape'));
  await expect(call('project_repository_slice',{path:'escape'})).rejects.toThrow('symlink');
  expect((await call('project_repository_slice',{path:'missing-owner',ref:expected})).eligible).toBe(0);
});

test('repository slice refuses malformed Git tree entries from its bounded reader', async () => {
  const root=directory(),sha='a'.repeat(40),seen=[];
  const call=createProjectReads({root,run:async (argv,options)=>{
    seen.push({argv,options});
    return {exit_code:0,stderr:'',stdout:argv.includes('rev-parse')?sha:'100644 blob '+sha+'\toutside/file\0'};
  }});
  await expect(call('project_repository_slice',{path:'selected',ref:sha})).rejects.toThrow('escaped');
  expect(seen.map(r=>r.argv)).toEqual([
    ['git','--no-pager','--literal-pathspecs','-c','core.fsmonitor=false','rev-parse','--verify','--end-of-options',sha+'^{commit}'],
    ['git','--no-pager','--literal-pathspecs','-c','core.fsmonitor=false','ls-tree','-r','-z','--full-tree',sha,'--','selected']]);
  expect(seen.every(r=>r.options.env.GIT_OPTIONAL_LOCKS==='0')).toBe(true);
});
test('project_tree exposes pinned Git subtree reads without changing the existing filesystem view', async () => {
  const {root,call}=fixture();
  mkdirSync(join(root,'cmd'),{recursive:true});
  writeFileSync(join(root,'cmd/owner.c'),'compiled source\n');
  commit(root);
  const commitId=command(root,'git','rev-parse','HEAD').trim();
  const local=await call('project_tree',{path:'cmd'});
  expect(local).toMatchObject({base:'cmd',items:[{path:'cmd/owner.c',type:'file'}]});
  const slice=await call('project_tree',{path:'cmd',ref:'HEAD',limit:1});
  expect(slice).toMatchObject({schema:'bonfyre.project.repository-slice.v1',path:'cmd',commit:commitId,eligible:1,omitted:0});
  expect(slice.files[0].path).toBe('cmd/owner.c');
  writeFileSync(join(root,'cmd/owner.c'),'edited live source\n');
  expect((await call('project_tree',{path:'cmd',ref:commitId,limit:1})).files).toEqual(slice.files);
  for(const args of [{path:'cmd',ref:'HEAD',depth:2},{path:'cmd',ref:'HEAD~1'}, {ref:'HEAD',limit:1001}])
    await expect(call('project_tree',args)).rejects.toThrow();
});
