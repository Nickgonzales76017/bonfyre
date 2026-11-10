import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync,
  symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { edgeCatalogManifest, publishEdgeCatalogManifest } from '../edge-catalog-manifest.mjs';
import { nightgridPersonaRows, nightgridSignupId } from '../nightgrid-persona-contract.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url)), temporary = [];
const contract = () => ({ worlds: { nightgrid: 'DEMO' }, role_ranks: { read: 0, write: 1 },
  persona_lattice: { nightgrid: [
    { persona: 'venue', rank: 'write', capabilities: 'merchant,offers,capacity' },
    { persona: 'member', rank: 'read', capabilities: 'participation and review' },
  ] } });
afterEach(() => { for (const dir of temporary.splice(0)) rmSync(dir, { recursive: true, force: true }); });
function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-catalog-persona-')); temporary.push(dir);
  const bin = join(dir, 'bin'); mkdirSync(bin);
  // Tests cannot invoke a real publisher, remote connection, or Python runtime.
  for (const name of ['python', 'python3', 'ssh', 'curl', 'aws'])
    writeFileSync(join(bin, name), '#!/bin/sh\nprintf "forbidden external dependency\\n" >&2\nexit 97\n', { mode: 0o755 });
  return { dir, bin, env: { ...process.env, PATH: `${bin}:${process.env.PATH}` } };
}
function nativeFixture(bin, name, source) {
  writeFileSync(join(bin, name), `#!${process.execPath}\n${source}`, { mode: 0o755 });
}

test('catalogue metadata handles real filenames and publishes one complete JSON manifest', () => {
  const { dir } = fixture();
  for (const [name, content] of [['a space.parquet', 'abc'], ['quote"name.parquet', '12345'],
    ['line\nbreak.parquet', 'x'], ['.private.parquet', 'private'], ['not-a-table.txt', 'none']])
    writeFileSync(join(dir, name), content);
  mkdirSync(join(dir, 'directory.parquet'));
  symlinkSync(join(dir, 'a space.parquet'), join(dir, 'link.parquet'));
  const manifest = edgeCatalogManifest(dir, new Date('2026-10-02T03:04:05.123Z'));
  expect(manifest.schema).toBe('bonfyre-edge-catalog.v1');
  expect(manifest.published).toBe('2026-10-02T03:04:05Z');
  expect(manifest.tables).toEqual([
    { name: 'a space', file: 'a space.parquet', bytes: 3 },
    { name: 'line\nbreak', file: 'line\nbreak.parquet', bytes: 1 },
    { name: 'quote"name', file: 'quote"name.parquet', bytes: 5 },
  ]);
  writeFileSync(join(dir, 'catalog.json'), 'previous');
  publishEdgeCatalogManifest(dir);
  expect(JSON.parse(readFileSync(join(dir, 'catalog.json'), 'utf8')).tables).toEqual(manifest.tables);
  expect(readdirSync(dir).filter(name => name.startsWith('.catalog-'))).toEqual([]);
});

test('actual publish_catalog.sh invokes Bun manifest without Python or an upload', () => {
  const { dir, bin, env } = fixture();
  const estate = join(dir, 'estate with spaces'), out = join(dir, 'catalog with spaces');
  mkdirSync(estate); mkdirSync(out);
  writeFileSync(join(estate, 'items.db'), 'isolated fixture');
  writeFileSync(join(out, 'an existing table.parquet'), 'other');
  const calls = join(dir, 'duckdb-calls.jsonl');
  nativeFixture(bin, 'duckdb', `
    import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
    const args = process.argv.slice(2); appendFileSync(process.env.FIXTURE_CALLS, JSON.stringify(args)+'\\n');
    if (args[0] === '-f') {
      const sql = readFileSync(args[1], 'utf8');
      for (const match of sql.matchAll(/TO '([^']+\\.parquet)'/g)) writeFileSync(match[1], 'fixture-columnar-body');
    } else console.log('1');
  `);
  const result = Bun.spawnSync(['sh', join(root, 'scripts/publish_catalog.sh'), '--out', out], {
    cwd: dir, env: { ...env, BONFYRE_ESTATE: estate, BONFYRE_WEB: join(dir, 'absent-web'), FIXTURE_CALLS: calls },
  });
  expect(result.exitCode).toBe(0);
  expect(result.stderr.toString()).toBe('');
  const manifest = JSON.parse(readFileSync(join(out, 'catalog.json'), 'utf8'));
  expect(manifest.tables.map(row => row.name)).toEqual(['an existing table', 'kin', 'material', 'neighbour']);
  expect(manifest.tables.every(row => row.bytes === statSync(join(out, row.file)).size)).toBe(true);
  expect(readFileSync(calls, 'utf8').trim().split('\n').length).toBe(7);
});

test('persona lattice is fully validated before emitting account-producing rows', () => {
  expect(nightgridPersonaRows(contract())).toBe('venue\twrite\tmerchant,offers,capacity\nmember\tread\tparticipation and review\n');
  for (const mutate of [
    c => { c.worlds.nightgrid = 'LIVE'; }, c => { c.persona_lattice.nightgrid = []; },
    c => { c.persona_lattice.nightgrid[1].persona = 'venue'; },
    c => { c.persona_lattice.nightgrid[1].persona = 'escape; touch /tmp/no'; },
    c => { c.persona_lattice.nightgrid[1].rank = 'undeclared'; },
    c => { c.persona_lattice.nightgrid[1].rank = 'read; nope'; },
    c => { c.persona_lattice.nightgrid[1].capabilities = 'read\nextra'; },
    c => { c.role_ranks.read = '0'; },
  ]) { const c = contract(); mutate(c); expect(() => nightgridPersonaRows(c)).toThrow(); }
  for (const id of ['1', '1; touch /tmp/no', 0, -1, 1.5, null, true, 2147483648])
    expect(() => nightgridSignupId({ user: { id } })).toThrow();
  expect(nightgridSignupId({ user: { id: 2147483647 } })).toBe('2147483647');
});

function provisionFixture() {
  const f = fixture(), contractPath = join(f.dir, 'institution contract.json'), calls = join(f.dir, 'ssh-calls.jsonl');
  const temp = join(f.dir, 'tmp'); mkdirSync(temp);
  writeFileSync(contractPath, JSON.stringify(contract()));
  nativeFixture(f.bin, 'ssh', `
    import { appendFileSync } from 'node:fs';
    const args = process.argv.slice(2), command = args.at(-1);
    const input = command.endsWith('http-signup') ? JSON.parse(await Bun.stdin.text()) : null;
    appendFileSync(process.env.FIXTURE_CALLS, JSON.stringify({ args, input })+'\\n');
    if (input) {
      if (process.env.FIXTURE_BAD_ID === '1') console.log(JSON.stringify({ user: { id: '1; do-not-execute' } }));
      else console.log(JSON.stringify({ user: { id: input.email.startsWith('venue@') ? 11 : 12 } }));
    }
  `);
  const env = { ...f.env, TMPDIR: temp, BONFYRE_INSTITUTION_CONTRACT: contractPath,
    BONFYRE_SSH_KEY: join(f.dir, 'fixture key'), BONFYRE_SSH_HOST: 'fixture.invalid', FIXTURE_CALLS: calls };
  const output = join(f.dir, 'private credentials.md');
  return { ...f, contractPath, calls, output, env,
    run: (overrides = {}) => Bun.spawnSync(['bash', join(root, 'scripts/provision_nightgrid_personas.sh'), output],
      { cwd: f.dir, env: { ...env, ...overrides } }) };
}

test('actual persona entrypoint keeps password stdin-only and grants declared roles without Python', () => {
  const f = provisionFixture(), result = f.run();
  expect(result.exitCode).toBe(0); expect(result.stderr.toString()).toBe('');
  const calls = readFileSync(f.calls, 'utf8').trim().split('\n').map(JSON.parse);
  expect(calls).toHaveLength(4);
  for (const [index, persona, id, role] of [[0, 'venue', 11, 'write'], [2, 'member', 12, 'read']]) {
    const signup = calls[index], grant = calls[index + 1];
    expect(signup.input.email).toBe(`${persona}@nightgrid.demo.invalid`);
    expect(signup.input.name).toBe(`Night Grid ${persona} (DEMO)`);
    expect(signup.input.password.length).toBeGreaterThan(12);
    expect(JSON.stringify(signup.args)).not.toContain(signup.input.password);
    expect(result.stdout.toString()).not.toContain(signup.input.password);
    expect(grant.args.at(-1)).toBe(`sudo /usr/local/bin/bonfyre-auth --db /var/lib/bonfyre/auth.db institution-grant --user-id ${id} --institution nightgrid --role ${role}`);
    expect(grant.input).toBeNull();
  }
  expect(statSync(f.output).mode & 0o777).toBe(0o600);
  const credentials = readFileSync(f.output, 'utf8');
  expect(credentials).toContain('participation and review');
  expect(credentials).toContain(calls[0].input.password);
});

test('bad contract cannot create any account; bad signup IDs cannot grant or overwrite credentials', () => {
  const f = provisionFixture(), c = contract(); c.persona_lattice.nightgrid[1].rank = 'read; inject';
  writeFileSync(f.contractPath, JSON.stringify(c));
  expect(f.run().exitCode).toBe(1);
  expect(readdirSync(f.dir)).not.toContain('ssh-calls.jsonl');
  writeFileSync(f.contractPath, JSON.stringify(contract()));
  writeFileSync(f.output, 'retain existing credentials', { mode: 0o600 });
  expect(f.run({ FIXTURE_BAD_ID: '1' }).exitCode).toBe(1);
  expect(readFileSync(f.output, 'utf8')).toBe('retain existing credentials');
  const calls = readFileSync(f.calls, 'utf8').trim().split('\n').map(JSON.parse);
  expect(calls).toHaveLength(2);
  expect(calls.every(call => call.args.at(-1).endsWith('http-signup'))).toBe(true);
});
