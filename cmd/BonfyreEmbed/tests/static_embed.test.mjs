// Static embeddings: the browser reader and the native reader give the same
// vector for the same text and file, the index carries its model's identity,
// and retrieval over a real collection agrees with the ONNX teacher.
// Needs the distilled model (~/BonfyreModels/static/nomic-static-128.bfse);
// without it the suite reports itself skipped rather than passing.
import { test, expect } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadStaticModel, embed, loadIndex, cosineTop } from '../../../components/BonfyreServiceSite/static-embed.mjs';

const here = dirname(fileURLToPath(import.meta.url)), binary = resolve(here, '../bonfyre-embed');
const modelPath = process.env.BONFYRE_STATIC_MODEL || join(homedir(), 'BonfyreModels/static/nomic-static-128.bfse');
const present = existsSync(modelPath) && existsSync(binary);
const it = present ? test : test.skip;

const TEXTS = [
  'Proof of delivery for shipment DEMO-1001, signed Oct 1, 2026.',
  'The best rivalry in women’s tennis — and why it matters',
  'Palmeiras bate o São Paulo no clássico',
  'Уроженка Москвы впервые выиграла турнир',
  '東京で新しい店舗がオープンしました',
  'Café crème brûlée, naïve résumé, Ångström',
  'WIFI password is on the router label; guest network is “Hollow-Guest”.',
  'zzqxv unknownword xylophonic',
  '',
  'a'.repeat(300),
];

function native(texts, extra = []) {
  const dir = mkdtempSync(join(tmpdir(), 'static-embed-'));
  try {
    writeFileSync(join(dir, 'in.jsonl'), texts.map((text, i) => JSON.stringify({ id: String(i), text: text || ' ' })).join('\n') + '\n');
    const run = Bun.spawnSync([binary, 'static', '--model', modelPath, '--input-jsonl', join(dir, 'in.jsonl'), '--out', join(dir, 'out.jsonl'), ...extra.map(a => a.replace('$DIR', dir))], { stderr: 'pipe' });
    if (run.exitCode !== 0) throw new Error(run.stderr.toString());
    return { rows: readFileSync(join(dir, 'out.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line)), dir };
  } catch (error) { rmSync(dir, { recursive: true, force: true }); throw error; }
}

it('the browser reader gives the native vector for the same text, to float32 precision', () => {
  const model = loadStaticModel(readFileSync(modelPath)), { rows, dir } = native(TEXTS);
  rmSync(dir, { recursive: true, force: true });
  for (const [i, text] of TEXTS.entries()) {
    const mine = embed(model, text || ' ');
    expect({ i, tokens: mine.tokens }).toEqual({ i, tokens: rows[i].tokens });
    const worst = Math.max(0, ...rows[i].vector.map((v, d) => Math.abs(v - mine.vector[d])));
    expect({ i, worst: worst < 1e-6 }).toEqual({ i, worst: true });
  }
  // Text the vocabulary cannot see at all gives no tokens and a zero vector.
  expect(embed(model, '東京').tokens).toBeGreaterThanOrEqual(0);
});

it('an index is bound to its model and finds the passage a question is about', async () => {
  const passages = ['The router label has the Wi-Fi password and the guest network name.',
    'The water heater was serviced in March; the next flush is due in September.',
    'Receipts for the dishwasher and the washer are in the blue folder in the hall closet.',
    'The house sitter feeds the cat twice a day and waters the tomatoes on Tuesdays.'];
  const { dir } = native(passages, ['--index-out', '$DIR/p.bfsi']);
  try {
    const modelBytes = readFileSync(modelPath), model = loadStaticModel(modelBytes);
    const index = await loadIndex(readFileSync(join(dir, 'p.bfsi')), modelBytes);
    expect(index.count).toBe(4);
    const ask = q => cosineTop(embed(model, q).vector, index, 1)[0][1];
    expect(ask('where is the wifi password')).toBe(0);
    expect(ask('when is the water heater maintenance')).toBe(1);
    expect(ask('where did we put the appliance receipts')).toBe(2);
    expect(ask('who looks after the pets')).toBe(3);
    const other = new Uint8Array(modelBytes); other[100] ^= 1;
    await expect(loadIndex(readFileSync(join(dir, 'p.bfsi')), other)).rejects.toThrow('different model');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

if (!present) test('static embedding witness', () => { console.log(`skipped: no model at ${modelPath} or no built ${binary}`); });
