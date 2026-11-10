import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRecursiveStore, VERSION } from '../recursive_store.mjs';
import { canonicalJson, digestValue, jsonFloat, parsedDocument } from '../canonical_json.mjs';
import { projectRun } from '../project_runtime.mjs';
import { compileTaskPlan, missionPlanContext } from '../mission_plan.mjs';

test('all seven IRs bind the shared semantic task plan instead of trusting supplied plan metadata', () => {
  const { store } = fixture();
  const mission = { id: 'plan-fixture', goal: 'Compile governed work', constraints: ['candidate only'],
    tasks: [{ id: 'input', capability: 'source.read' }, { id: 'result', dependencies: ['input'], capability: 'pure.transform' }],
    execution_plan: { digest: 'forged' } };
  const expected = compileTaskPlan(mission.tasks, { context: missionPlanContext(mission) });
  const bundle = store.compileIrBundle({ subject_kind: 'mission', subject_id: mission.id, mission });
  expect(Object.keys(bundle.objects)).toHaveLength(7);
  for (const ir of Object.values(bundle.objects)) expect(ir.payload.task_plan_digest).toBe(expected.digest);
  expect(bundle.objects.ExecutionPhenotypeIR.payload.task_plan).toEqual(expected);
  expect(bundle.objects.DeltaIR.payload.task_input_roots).toEqual(expected.nodes.map(node => ({ id: node.id, input_digest: node.input_digest })));
  expect(bundle.objects.AuthorityIR.payload.external_effects).toBe(false);
  expect(store.verifyReceipts().verified).toBe(true);
  const before = store.verifyReceipts().receipts;
  expect(() => store.compileIrBundle({ subject_kind: 'mission', subject_id: mission.id,
    mission: { ...mission, tasks: [{ id: 'bad', dependencies: ['missing'] }] } })).toThrow('missing dependency');
  expect(store.verifyReceipts().receipts).toBe(before);
});

const roots = [], stores = [];
const binary = process.env.BONFYRE_TOKEN_FABRIC_BIN ?? join(homedir(), '.local/bin/bonfyre-token-fabric'); const nativeTest = test.skipIf(!existsSync(binary)); // installed organ; no source in this repository
function fixture(extra = {}) {
  const root = mkdtempSync(join(tmpdir(), 'recursive-store-test-')); roots.push(root);
  const store = createRecursiveStore({ home: join(root, 'owner'), tokenBinary: binary, now: () => 1800000000, ...extra });
  stores.push(store); return { root, store };
}
afterEach(() => { for (const store of stores.splice(0)) store.close(); for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

test('construction is lazy; original schema and tokenizer distinctions survive reopen', async () => {
  const { store } = fixture({ run: async () => ({ exit_code: 0, stdout: '{}', stderr: '', duration_ms: 0 }) });
  expect(existsSync(store.home)).toBe(false);
  store.bootstrap();
  const db = new Database(store.dbPath, { readonly: true });
  expect(db.query('SELECT value FROM metadata WHERE key=?').get('schema_version').value).toBe(VERSION);
  expect(db.query('SELECT model_compatible FROM tokenizer_descriptors').all().map(x => x.model_compatible)).toEqual([0, 0, 0]);
  db.close();
  const status = await store.status(); expect(status.receipt_chain.verified).toBe(true); expect(status.flux_chains.verified).toBe(true);
});

test('arbitrary local source paths canonicalize BOM/newlines without corrupting binary bytes', () => {
  const { root, store } = fixture(); const source = join(root, 'outside-owner');
  writeFileSync(source, Buffer.from([239,187,191,255,13,10,65,13]));
  const item = store.ingestArtifact({ path: source });
  expect([...readFileSync(item.canonical_path)]).toEqual([255,10,65,10]);
  expect(store.artifact(item.artifact_id).source_path).toBe(realpathSync(source));
  expect(store.verifyReceipts().verified).toBe(true); expect(store.verifyFlux().verified).toBe(true);
  expect(() => store.ingestArtifact({ text: 'x', path: source })).toThrow('exactly one');
});

test('owned output symlink escape refuses without external mutation', () => {
  const { root, store } = fixture(); mkdirSync(store.home); const external = join(root, 'external'); mkdirSync(external);
  symlinkSync(external, store.artifactRoot);
  expect(() => store.ingestArtifact({ text: 'no' })).toThrow('symlink');
  expect(existsSync(store.dbPath)).toBe(false);
});

test('receipt corruption and database payload substitution refuse runtime image proof', () => {
  const { store } = fixture();
  const receipt = store.writeReceipt({ kind: 'fixture', state: 'passed', payload: { evidence: 'ÿ' } });
  expect(store.verifyReceipts().verified).toBe(true);
  const raw = readFileSync(receipt.path, 'utf8'); writeFileSync(receipt.path, raw.replace('\\u00ff', 'x'));
  expect(store.verifyReceipts().verified).toBe(false);
  expect(() => store.compileRuntimeImage({})).toThrow('verified');
  writeFileSync(receipt.path, raw);
  const db = new Database(store.dbPath); db.query('UPDATE receipts SET payload_json=?').run('{"evidence":"substituted"}'); db.close();
  expect(store.verifyReceipts().failures.some(x => x.reason === 'row_document_mismatch')).toBe(true);
});

test('historical Python float lexemes verify in receipt and flux chains', () => {
  const { store } = fixture(); store.bootstrap();
  const db = new Database(store.dbPath);
  const receiptBody = '{"id":"old","kind":"old","subject_kind":null,"subject_id":null,"state":"passed","payload":{"integral_float":1.0,"unicode":"ÿ"},"previous_digest":null,"created_at":1800000000.0}';
  const digest = parsedDocument(receiptBody).calculated, path = join(store.receiptRoot, 'old.json');
  writeFileSync(path, receiptBody.slice(0, -1) + ',"digest":"' + digest + '"}');
  db.query('INSERT INTO receipts VALUES(?,?,?,?,?,?,?,?,?,?)').run('old','old',null,null,'passed','{"integral_float":1.0,"unicode":"ÿ"}',null,digest,path,1800000000);
  const fluxBody = receiptBody.replace('"state":"passed",', '"stream":"legacy",');
  db.query('INSERT INTO flux_events(id,stream,kind,subject_kind,subject_id,payload_json,previous_digest,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
    .run('old','legacy','old',null,null,'{"integral_float":1.0,"unicode":"ÿ"}',null,parsedDocument(fluxBody).calculated,1800000000);
  db.close();
  expect(store.verifyReceipts().verified).toBe(true); expect(store.verifyFlux().verified).toBe(true);
  store.appendFlux({ stream: 'legacy', kind: 'new', payload: {} });
  store.writeReceipt({ kind: 'new', state: 'passed', payload: {} });
  expect(store.verifyReceipts().receipts).toBe(2); expect(store.verifyFlux().verified).toBe(true);
});

test('new flux REAL timestamps preserve integral float semantics for legacy verification', () => {
  const { store } = fixture({now:()=>1}); const event = store.appendFlux({stream:'real',kind:'test',payload:{}});
  const {seq, digest, ...body} = event; expect(seq).toBe(1);
  expect(digest).toBe(digestValue({...body,created_at:jsonFloat(1)}));
  expect(store.verifyFlux().verified).toBe(true);
});

test('dependency closure is bounded, cycle safe and feeds graph and seven IR views', () => {
  const { store } = fixture();
  for (const [from,to] of [['a','b'],['b','c'],['c','a']]) store.registerDependency({ source_kind:'artifact',source_id:from,target_kind:'artifact',target_id:to,edge_kind:'depends' });
  expect(store.affectedClosure({root_kind:'artifact',root_id:'a',max_depth:1}).nodes.map(x=>x.id)).toEqual(['a','b']);
  expect(store.graphProjection({graph_kind:'fixture',root_kind:'artifact',root_id:'a'}).nodes).toHaveLength(3);
  expect(() => store.affectedClosure({root_kind:'artifact',root_id:'a',max_depth:129})).toThrow();
  const bundle = store.compileIrBundle({subject_kind:'mission',subject_id:'test',mission:{verification:['must-pass']}});
  expect(Object.keys(bundle.objects)).toHaveLength(7);
  expect(() => store.compileIr({ir_type:'../../outside',subject_kind:'x',subject_id:'x',payload:{}})).toThrow();
  expect(store.compileRuntimeImage({mission_id:'test'}).schema).toBe('bonfyre.runtime-image.v1');
});

test('bids default optional counts and cannot claim negative uncached usage', () => {
  const { store } = fixture();
  const args = {provider_id:'local',tokenizer_id:'lexeme-v1',required_tokens:5};
  expect(store.compileProviderBid(args).uncached_tokens).toBe(5);
  expect(store.compileProviderBid({...args,reusable_tokens:20}).reusable_tokens).toBe(5);
  expect(() => store.compileProviderBid({...args,required_tokens:-1})).toThrow();
});

test('motifs distinguish structural heuristic from exact match; BFP remains gated candidate', () => {
  const { store } = fixture(); store.registerMotif({name:'fixture',structure:{a:1,b:2},reconstruction_checks:1});
  expect(store.matchMotifs({a:1,b:2}).match).toBe('exact');
  expect(store.matchMotifs({a:999}).candidates[0].score).toBe(0.5);
  const plan = store.bfpSuccessorPlan({changed_sections:['z','a','a']});
  expect(plan.changed_sections).toEqual(['a','z']); expect(plan.receipt.state).toBe('candidate');
  expect(plan.physical_candidates.find(x=>x.encoding==='COPY_RANGE').eligible).toBe(false);
  expect(plan.activation.requires_authority_gate).toBe(true);
});

nativeTest('real native tokens share body across distinct semantic owners and refuse corrupted cache', async () => {
  const { store } = fixture();
  const a = store.ingestArtifact({text:'hello ÿ\n',kind:'text'}), b = store.ingestArtifact({text:'hello ÿ\n',kind:'mission'});
  const first = await store.compileTokenArtifact(a.artifact_id);
  const second = await store.compileTokenArtifact(b.artifact_id);
  expect(a.artifact_id).not.toBe(b.artifact_id); expect(second.id).toBe(first.artifact_id); expect(second.source_artifact_id).toBe(b.artifact_id);
  expect(store.affectedClosure({root_kind:'artifact',root_id:b.artifact_id}).nodes.some(x=>x.id===first.artifact_id)).toBe(true);
  expect((await store.compileTokenArtifact(b.artifact_id)).id).toBe(first.artifact_id);
  writeFileSync(second.spans_path, '{}\n');
  await expect(store.compileTokenArtifact(b.artifact_id)).rejects.toThrow('invalid token span');
});

nativeTest('concurrent native compilation keeps one valid body; empty tokens retain exact byte coverage', async () => {
  const { store } = fixture(); const item = store.ingestArtifact({text:'same body'});
  const pair = await Promise.all([store.compileTokenArtifact(item.artifact_id),store.compileTokenArtifact(item.artifact_id)]);
  expect(pair[0].id ?? pair[0].artifact_id).toBe(pair[1].id ?? pair[1].artifact_id);
  for (const text of ['', '   \n']) {
    const body = store.ingestArtifact({text}); const token = await store.compileTokenArtifact(body.artifact_id);
    expect(token.token_count).toBe(0);
  }
  expect(store.verifyReceipts().verified).toBe(true);
});

nativeTest('native output injection is refused and never gets a token row', async () => {
  const { store } = fixture({run:async argv => {
    const out = argv[argv.indexOf('--out')+1];
    writeFileSync(out, canonicalJson({schema:'bonfyre.token-artifact.v1',artifact_id:sha('x'),tokenizer:{id:'lexeme-v1',version:'1',exact:true},canonical_path:'/etc/passwd',chunks:[]}));
    return {exit_code:0,stdout:'',stderr:'',duration_ms:0};
  }});
  const body = store.ingestArtifact({text:'abc'});
  await expect(store.compileTokenArtifact(body.artifact_id)).rejects.toThrow('invalid native token manifest');
  const db = new Database(store.dbPath,{readonly:true}); expect(db.query('SELECT count(*) AS n FROM token_artifacts').get().n).toBe(0); db.close();
});

nativeTest('native multichunk spans overlap at byte boundaries; deltas bind all chunk digests', async () => {
  const { store } = fixture();
  const a = await store.compileTokenArtifact(store.ingestArtifact({text:'hello'}).artifact_id);
  const b = await store.compileTokenArtifact(store.ingestArtifact({text:Array.from({length:6000},(_,i)=>`word${i}`).join(' ')}).artifact_id);
  expect(b.chunk_count).toBeGreaterThan(1);
  expect(b.chunks.some((chunk,i)=>i>0 && chunk.token_start<b.chunks[i-1].token_end)).toBe(true);
  const insert = await store.deriveTokenDelta(a.artifact_id,b.artifact_id);
  const remove = await store.deriveTokenDelta(b.artifact_id,a.artifact_id);
  const copy = await store.deriveTokenDelta(b.artifact_id,b.artifact_id);
  expect(insert.operations.some(op=>op.op==='INSERT')).toBe(true);
  expect(remove.operations.some(op=>op.op==='DELETE')).toBe(true);
  expect(copy.operations.every(op=>op.op==='COPY')).toBe(true);
  expect(store.verifyFlux().verified).toBe(true);
}, 10000);

nativeTest('forged delta chunk source cannot commit a delta receipt', async () => {
  const { store } = fixture({run:async (argv, options) => {
    const result = await projectRun(argv, options);
    if (argv[1] === 'delta') {
      const out = argv[argv.indexOf('--out')+1], body = JSON.parse(readFileSync(out,'utf8'));
      body.operations[0].old_digest = '0'.repeat(64); writeFileSync(out,JSON.stringify(body));
    }
    return result;
  }});
  const a = await store.compileTokenArtifact(store.ingestArtifact({text:'before'}).artifact_id);
  const b = await store.compileTokenArtifact(store.ingestArtifact({text:'after'}).artifact_id);
  const head = store.verifyReceipts().head;
  await expect(store.deriveTokenDelta(a.artifact_id,b.artifact_id)).rejects.toThrow('source chunk mismatch');
  expect(store.verifyReceipts().head).toBe(head);
  const db = new Database(store.dbPath,{readonly:true}); expect(db.query('SELECT count(*) AS n FROM deltas').get().n).toBe(0); db.close();
});

nativeTest('native status preserves actual failure and timeout receipt instead of inventing missing binary', async () => {
  const receipt = {exit_code:124,stdout:'',stderr:'deadline',duration_ms:10,timeout:true};
  const {store} = fixture({run:async()=>receipt}); expect((await store.status()).token_binary_status).toEqual(receipt);
});
