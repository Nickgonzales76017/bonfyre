import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';

const root = resolve(import.meta.dir, '../../..');
const binary = process.env.BONFYRE_REFERENCE_BINARY || join(root, 'programs/bonfyre/bonfyre');
const digest = value => createHash('sha256').update(value).digest('hex');

test('operator reference uses a verified run from the selected compiled generation', () => {
  const temp = mkdtempSync(join(tmpdir(), 'bonfyre-reference-'));
  let db;
  const run = (...args) => {
    const result = spawnSync(binary, args, { cwd: root, env: { ...process.env, BONFYRE_STATE_DIR: temp }, encoding: 'utf8', timeout: 20000, maxBuffer: 2000000 });
    expect(result.status, result.stderr).toBe(0);
    return result.stdout;
  };
  const reference = () => run('operator', 'reference', 'command.hash');
  const compile = profile => run('fabric', 'compile', join(root, 'bonfyre.workspace.yaff'), join(root, 'bonfyre.lock.yaff'), join(root, 'estate/catalog.yaff'), join(root, 'estate/compositions.yaff'), profile, join(root, 'estate/legacy-operators.tsv'));
  const execute = (mission, uri) => {
    run('mission', 'create', mission);
    run('work', 'add', mission, 'hash', 'command.hash', uri);
    run('work', 'run', mission);
    return db.query("SELECT e.id,e.receipt_id,r.payload,r.content_hash FROM events e JOIN receipts r ON r.id=e.receipt_id WHERE e.mission_id=? AND e.status='complete'").get(mission);
  };
  try {
    compile(join(root, 'estate/profiles.yaff'));
    db = new Database(join(temp, 'fabric.db'));
    expect(run('operator', 'reference', 'command.tel')).not.toContain('In the same family'); expect(reference()).toContain('In the same family'); // service_dependent is a status, not a family
    const input = join(temp, 'input.txt');
    writeFileSync(input, 'A retained native reference fixture.\n');
    const uri = run('artifact', 'ingest', input, 'text/plain').match(/^uri=(.+)$/m)[1];
    const first = execute('first-reference', uri);
    expect(reference()).toContain(first.receipt_id);

    const profile = join(temp, 'profiles.yaff');
    writeFileSync(profile, readFileSync(join(root, 'estate/profiles.yaff'), 'utf8') + '\n# New reference generation\n');
    compile(profile);
    const afterCompile = reference();
    expect(afterCompile).not.toContain(first.receipt_id);
    expect(afterCompile).toContain('No verified run recorded for this contract generation');
    const second = execute('second-reference', uri);
    const generation = JSON.parse(second.payload).contract_generation;
    expect(reference()).toContain(second.receipt_id);
    expect(reference()).toContain(generation);

    for (const [field, value] of [['contract', 'unrelated'], ['contract_generation', 'old'], ['operator', 'command.other'], ['output', 'bonfyre://artifact/other'], ['node', 'other'], ['mission', 'other'], ['workload_result', 'failed'], ['quality_result', 'failed']]) {
      const payload = JSON.stringify({ ...JSON.parse(second.payload), [field]: value });
      db.query('UPDATE receipts SET payload=?,content_hash=? WHERE id=?').run(payload, digest(payload), second.receipt_id);
      expect(reference(), field).not.toContain(second.receipt_id);
    }
    db.query('UPDATE receipts SET payload=?,content_hash=? WHERE id=?').run(second.payload, 'wrong-digest', second.receipt_id);
    expect(reference()).not.toContain(second.receipt_id);
    db.query('UPDATE receipts SET payload=?,content_hash=? WHERE id=?').run('{invalid', digest('{invalid'), second.receipt_id);
    expect(reference()).not.toContain(second.receipt_id);
    db.query('UPDATE receipts SET payload=?,content_hash=? WHERE id=?').run(second.payload, second.content_hash, second.receipt_id);
    db.query('UPDATE execution_metrics SET catalog_generation=? WHERE event_id=?').run('old', second.id);
    expect(reference()).not.toContain(second.receipt_id);
    db.query('UPDATE execution_metrics SET catalog_generation=? WHERE event_id=?').run(generation, second.id);
    expect(reference()).toContain(second.receipt_id);
    db.query('UPDATE execution_metrics SET event_id=? WHERE event_id=?').run('missing-metric', second.id);
    expect(reference()).not.toContain(second.receipt_id);
    db.query('UPDATE execution_metrics SET event_id=? WHERE event_id=?').run(second.id, 'missing-metric');
    db.query("UPDATE receipts SET subject_kind='unrelated' WHERE id=?").run(second.receipt_id);
    expect(reference()).not.toContain(second.receipt_id);
    db.query("UPDATE receipts SET subject_kind='operator-execution' WHERE id=?").run(second.receipt_id);

    for (const [table, column, predicate] of [
      ['operator_contract_bindings', 'generation', "operator_id='command.hash'"],
      ['catalog_bindings', 'catalog_generation', "operator_id='command.hash'"],
      ['fabric_meta', 'value', "key='catalog_generation'"],
    ]) {
      db.run(`UPDATE ${table} SET ${column}='stale' WHERE ${predicate}`);
      const refused = spawnSync(binary, ['operator', 'reference', 'command.hash'], { cwd: root, env: { ...process.env, BONFYRE_STATE_DIR: temp }, encoding: 'utf8', timeout: 20000 });
      expect(refused.status).not.toBe(0);
      expect(refused.stdout).toBe('');
      expect(refused.stderr).toContain('no current compiled contract binding');
      db.query(`UPDATE ${table} SET ${column}=? WHERE ${predicate}`).run(generation);
    }

    const third = execute('third-reference', uri);
    db.query('UPDATE receipts SET content_hash=? WHERE id=?').run('wrong-digest', third.receipt_id);
    db.query("UPDATE events SET end_at='9999-01-01' WHERE id=?").run(third.id);
    const fallback = reference();
    expect(fallback).toContain(second.receipt_id);
    expect(fallback).not.toContain(third.receipt_id);
  } finally {
    db?.close();
    rmSync(temp, { recursive: true, force: true });
  }
}, 60000);

test('reference excerpts require exact public disclosure and unchanged retained bytes', () => {
  const temp = mkdtempSync(join(tmpdir(), 'bonfyre-reference-disclosure-'));
  let db;
  const result = (...args) => spawnSync(binary, args, { cwd: root, env: { ...process.env, BONFYRE_STATE_DIR: temp }, encoding: 'utf8', timeout: 20000, maxBuffer: 2000000 });
  const run = (...args) => { const r = result(...args); expect(r.status, r.stderr).toBe(0); return r.stdout; };
  const policyPath = join(temp, 'disclosure.json');
  const reference = policy => {
    if (policy !== undefined) writeFileSync(policyPath, typeof policy === 'string' ? policy : JSON.stringify(policy));
    return result('operator', 'reference', 'command.hash', ...(policy === undefined ? [] : ['--disclosure', policyPath]));
  };
  try {
    run('fabric', 'compile', join(root, 'bonfyre.workspace.yaff'));
    db = new Database(join(temp, 'fabric.db'));
    const input = join(temp, 'source.txt'), text = 'PRIVATE FIXTURE CONTENT\n```\n# This stays inside the excerpt\n';
    writeFileSync(input, text);
    const uri = run('artifact', 'ingest', input, 'text/plain').match(/^uri=(.+)$/m)[1];
    run('mission', 'create', 'disclosure');
    run('work', 'add', 'disclosure', 'hash', 'command.hash', uri);
    run('work', 'run', 'disclosure');
    const generation = db.query("SELECT value FROM fabric_meta WHERE key='catalog_generation'").get().value;
    const policy = { schema: 'bonfyre.reference-disclosure.v1', operator: 'command.hash', contract_generation: generation, fields: [{ field_name: uri + '#excerpt', audiences: ['public'] }] };
    const plain = reference();
    expect(plain.status).toBe(0);
    expect(plain.stdout).not.toContain('PRIVATE FIXTURE CONTENT');
    expect(plain.stdout).toContain('In excerpt withheld');
    // A local publication has no implied public audience.
    const effect = run('effect', 'request', 'disclosure', 'publish-local', uri).match(/^effect=bonfyre:\/\/effect\/(.+)$/m)[1];
    run('effect', 'approve', effect);
    run('effect', 'commit', effect);
    expect(reference().stdout).not.toContain('PRIVATE FIXTURE CONTENT');
    const publicRead = reference(policy);
    expect(publicRead.status, publicRead.stderr).toBe(0);
    expect(publicRead.stdout).toContain('````text In\n' + text + '````');
    expect(publicRead.stdout).toContain('Out excerpt withheld');
    expect(publicRead.stdout).toContain(digest(JSON.stringify(policy)));
    for (const audiences of [[], ['self'], ['institution'], ['market']]) {
      const r = reference({ ...policy, fields: [{ ...policy.fields[0], audiences }] });
      expect(r.status).toBe(0);
      expect(r.stdout).not.toContain('PRIVATE FIXTURE CONTENT');
    }
    const wrongArtifact = reference({ ...policy, fields: [{ field_name: 'bonfyre://artifact/' + '0'.repeat(64) + '#excerpt', audiences: ['public'] }] });
    expect(wrongArtifact.status).toBe(0);
    expect(wrongArtifact.stdout).not.toContain('PRIVATE FIXTURE CONTENT');
    for (const invalid of [
      { ...policy, operator: 'command.other' },
      { ...policy, contract_generation: '0'.repeat(64) },
      { ...policy, fields: [policy.fields[0], policy.fields[0]] },
      { ...policy, fields: [{ ...policy.fields[0], audiences: ['everyone'] }] },
      { ...policy, fields: [{ ...policy.fields[0], audiences: ['public', 'public'] }] },
      { ...policy, fields: [{ ...policy.fields[0], field_name: '*' }] },
      { ...policy, unexpected: true },
      JSON.stringify(policy).replace('"operator":', '"operator":"command.other","operator":'),
      '{invalid', JSON.stringify(policy) + ' trailing', '\0' + JSON.stringify(policy),
    ]) {
      const refused = reference(invalid);
      expect(refused.status).not.toBe(0);
      expect(refused.stdout).toBe('');
      expect(refused.stderr).toContain('disclosure');
    }
    const locator = db.query('SELECT locator FROM artifacts WHERE uri=?').get(uri).locator;
    const retained = readFileSync(locator);
    writeFileSync(locator, 'CHANGED PRIVATE CONTENT\n');
    const changed = reference(policy);
    expect(changed.status).toBe(0);
    expect(changed.stdout).not.toContain('CHANGED PRIVATE CONTENT');
    expect(changed.stdout).toContain('In excerpt unavailable');
    writeFileSync(locator, retained);
    expect(reference(policy).stdout).toContain('PRIVATE FIXTURE CONTENT');
    let sampleOrder = 0;
    for (const [name, bytes, valid] of [
      ['unicode', Buffer.from('Café 🌱\n'), true],
      ['surrogate', Buffer.from([0xed, 0xa0, 0x80]), false],
      ['overlong', Buffer.from([0xe0, 0x80, 0xaf]), false],
      ['out-of-range', Buffer.from([0xf4, 0x90, 0x80, 0x80]), false],
    ]) {
      writeFileSync(input, bytes);
      const sampleUri = run('artifact', 'ingest', input, 'text/plain').match(/^uri=(.+)$/m)[1];
      run('mission', 'create', name);
      run('work', 'add', name, 'hash', 'command.hash', sampleUri);
      run('work', 'run', name);
      // Native event timestamps have second precision; order these fixtures explicitly.
      db.query('UPDATE events SET end_at=? WHERE mission_id=?').run(`2030-01-01T00:00:0${++sampleOrder}Z`, name);
      const rendered = reference({ ...policy, fields: [{ field_name: sampleUri + '#excerpt', audiences: ['public'] }] });
      expect(rendered.status, rendered.stderr).toBe(0);
      expect(rendered.stdout.includes('In excerpt unavailable')).toBe(!valid);
      if (valid) expect(rendered.stdout).toContain('Café 🌱');
    }
  } finally {
    db?.close();
    rmSync(temp, { recursive: true, force: true });
  }
}, 60000);
