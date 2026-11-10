// Reader for a .bfse static embedding (bonfyre-embed distill). Served as
// /assets/static-embed.mjs; the components/ path re-exports it. A text becomes
// a vector with no model and no runtime: the native tokenizer, reproduced
// here from the vocabulary and Unicode fold table the file carries, then the
// average of the tokens' int8 rows, normalised. Arithmetic follows the native
// reader step by step in float32, so a vector here equals `bonfyre-embed
// static` for the same file and text (tests/static-embed.test.mjs).

const ASCII_PUNCT = new Set('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~');

export function loadStaticModel(buffer) {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 44 || String.fromCharCode(...bytes.subarray(0, 4)) !== 'BFSE' || view.getUint32(4, true) !== 1) throw new Error('not a version 1 .bfse file');
  const u32 = at => view.getUint32(at, true);
  const count = u32(8), dims = u32(12), cls = u32(16), sep = u32(20), unk = u32(24), vocabBytes = u32(28), foldCount = u32(32), foldBytes = u32(36);
  const decoder = new TextDecoder();
  const tokens = decoder.decode(bytes.subarray(44, 44 + vocabBytes)).split('\n');
  if (tokens.length !== count) throw new Error('vocabulary size does not match the header');
  // The native trie walks ASCII only: a token with any other character can
  // never be matched, and a later duplicate replaces an earlier one.
  const main = new Map(), sub = new Map();
  tokens.forEach((token, id) => {
    if (!/^[\x01-\x7f]+$/.test(token)) return;
    main.set(token, id);
    if (token.length > 2 && token.startsWith('##')) sub.set(token.slice(2), id);
  });
  const fold = new Map();
  for (let at = 44 + vocabBytes, n = 0; n < foldCount; n++) {
    const cp = u32(at), len = bytes[at + 4];
    fold.set(cp, decoder.decode(bytes.subarray(at + 5, at + 5 + len)));
    at += 5 + len;
  }
  const tables = (44 + vocabBytes + foldBytes + 3) & ~3;
  const scales = new Float32Array(count);
  for (let i = 0; i < count; i++) scales[i] = view.getFloat32(tables + 4 * i, true);
  const rows = new Int8Array(bytes.buffer, bytes.byteOffset + tables + 4 * count, count * dims);
  return { count, dims, cls, sep, unk, tokens, main, sub, fold, scales, rows };
}

const isSpace = cp => cp === 0x20 || cp === 0x09 || cp === 0x0a || cp === 0x0d || cp === 0x0b || cp === 0x0c || cp === 0xa0 ||
  (cp >= 0x2000 && cp <= 0x200b) || cp === 0x202f || cp === 0x205f || cp === 0x3000 || cp === 0xfeff;
const isControl = cp => cp < 0x20 || (cp >= 0x7f && cp <= 0x9f) || cp === 0xfffd;
function isPunct(cp) {
  if (cp < 0x80) return ASCII_PUNCT.has(String.fromCharCode(cp));
  if (cp >= 0xa1 && cp <= 0xbf && cp !== 0xaa && cp !== 0xb5 && cp !== 0xba) return true;
  if (cp === 0xd7 || cp === 0xf7) return true;
  if ((cp >= 0x2010 && cp <= 0x2027) || (cp >= 0x2030 && cp <= 0x205e) || (cp >= 0x3001 && cp <= 0x303f)) return true;
  if ((cp >= 0xff01 && cp <= 0xff0f) || (cp >= 0xff1a && cp <= 0xff20)) return true;
  return [0x060c, 0x061b, 0x061f, 0x06d4, 0x0964, 0x0965, 0x0589, 0x05be].includes(cp);
}
const isCjk = cp => (cp >= 0x4e00 && cp <= 0x9fff) || (cp >= 0x3400 && cp <= 0x4dbf) || (cp >= 0xf900 && cp <= 0xfaff) ||
  (cp >= 0x20000 && cp <= 0x2a6df) || (cp >= 0x2f800 && cp <= 0x2fa1f);

// BERT uncased pre-tokenization, as bert_pre_tokenize in BonfyreEmbed.
export function preTokenize(model, text) {
  const words = [];
  let word = '';
  for (const ch of String(text)) {
    const cp = ch.codePointAt(0);
    if (isSpace(cp) || isControl(cp)) { if (word) { words.push(word); word = ''; } continue; }
    if (isPunct(cp) || isCjk(cp)) { if (word) { words.push(word); word = ''; } words.push(ch); continue; }
    if (cp < 0x80) { word += cp >= 65 && cp <= 90 ? String.fromCharCode(cp + 32) : ch; continue; }
    const folded = model.fold.get(cp);
    word += folded === undefined ? ch : folded;
  }
  if (word) words.push(word);
  return words;
}

// Longest prefix of s from `start` that the map holds, walking ASCII only.
function longest(map, s, start) {
  let best = -1, bestLen = 0;
  for (let end = start + 1; end <= s.length; end++) {
    if (s.charCodeAt(end - 1) >= 0x80) break;
    const id = map.get(s.slice(start, end));
    if (id !== undefined) { best = id; bestLen = end - start; }
  }
  return [best, bestLen];
}

// WordPiece as wordpiece_encode: pieces already matched stay, and a word
// that cannot be finished adds [UNK] after them.
export function tokenIds(model, text, cap = 8192) {
  const ids = [model.cls];
  for (const word of preTokenize(model, text)) {
    if (ids.length >= cap - 1) break;
    let start = 0, bad = false;
    while (start < word.length && ids.length < cap - 1) {
      const [id, len] = longest(start === 0 ? model.main : model.sub, word, start);
      if (id < 0 || len === 0) { bad = true; break; }
      ids.push(id); start += len;
    }
    if (bad && ids.length < cap - 1) ids.push(model.unk);
  }
  ids.push(model.sep);
  return ids;
}

export function embed(model, text) {
  const out = new Float32Array(model.dims);
  let used = 0;
  for (const id of tokenIds(model, text)) {
    if (id === model.cls || id === model.sep || id === model.unk || model.scales[id] === 0) continue;
    const scale = model.scales[id], base = id * model.dims;
    for (let d = 0; d < model.dims; d++) out[d] = Math.fround(out[d] + Math.fround(model.rows[base + d] * scale));
    used++;
  }
  let norm = 0;
  for (let d = 0; d < model.dims; d++) norm += out[d] * out[d];
  norm = Math.sqrt(norm);
  if (norm > 0) for (let d = 0; d < model.dims; d++) out[d] = Math.fround(out[d] / norm);
  return { vector: out, tokens: used };
}

// A search index: unit vectors stored as int8 with one scale each, plus the
// passages they stand for. Built by the CMS at publish time.
export function cosineTop(query, index, k = 10) {
  const { dims, count, scales, rows } = index, scored = [];
  for (let i = 0; i < count; i++) {
    let dot = 0;
    const base = i * dims;
    for (let d = 0; d < dims; d++) dot += rows[base + d] * query[d];
    scored.push([dot * scales[i], i]);
  }
  return scored.sort((a, b) => b[0] - a[0]).slice(0, k);
}

// A .bfsi index, as bf_static_index_write leaves it. `model` (optional) must
// be the .bfse that wrote it: an index is refused under another model.
export async function loadIndex(buffer, modelBytes) {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 48 || String.fromCharCode(...bytes.subarray(0, 4)) !== 'BFSI' || view.getUint32(4, true) !== 1) throw new Error('not a version 1 .bfsi index');
  const dims = view.getUint32(8, true), count = view.getUint32(12, true), model_sha256 = [...bytes.subarray(16, 48)].map(b => b.toString(16).padStart(2, '0')).join('');
  if (bytes.length !== 48 + count * 4 + count * dims) throw new Error('truncated .bfsi index');
  if (modelBytes) {
    const digest = [...new Uint8Array(await crypto.subtle.digest('SHA-256', modelBytes))].map(b => b.toString(16).padStart(2, '0')).join('');
    if (digest !== model_sha256) throw new Error('this index was built with a different model');
  }
  const scales = new Float32Array(count);
  for (let i = 0; i < count; i++) scales[i] = view.getFloat32(48 + 4 * i, true);
  return { dims, count, model_sha256, scales, rows: new Int8Array(bytes.buffer, bytes.byteOffset + 48 + 4 * count, count * dims) };
}

// Keyword ranking (BM25) over the passages' own words, built once per index.
// Static vectors blur rare, exact words ("manifest", "Wix", a product name);
// keyword ranking is strongest exactly there, so search blends the two.
const STOP = new Set('a an and are as at be by can do for from has have how i in is it its my of on or our so that the their them this to up was we what when where which who why will with you your'.split(' '));
const words = text => (String(text).toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g, '').match(/[a-z0-9]+/g) || []).filter(w => w.length > 1 && !STOP.has(w));
const stem = w => w.length > 4 ? w.replace(/(ies|es|s|ing|ed)$/, '') : w;
export function keywordIndex(passages) {
  const docs = passages.map(p => words(`${p.page} ${p.heading} ${p.heading} ${p.excerpt}`).map(stem)), df = new Map();
  for (const doc of docs) for (const w of new Set(doc)) df.set(w, (df.get(w) || 0) + 1);
  const avg = docs.reduce((n, d) => n + d.length, 0) / Math.max(1, docs.length);
  return { docs, df, avg, n: docs.length };
}
export function keywordScores(kw, query, k1 = 1.2, b = 0.75) {
  const terms = [...new Set(words(query).map(stem))];
  return kw.docs.map(doc => {
    let score = 0;
    for (const t of terms) {
      const f = doc.filter(w => w === t).length;
      if (!f) continue;
      const idf = Math.log(1 + (kw.n - kw.df.get(t) + 0.5) / (kw.df.get(t) + 0.5));
      score += idf * (f * (k1 + 1)) / (f + k1 * (1 - b + b * doc.length / kw.avg));
    }
    return score;
  });
}
// Each signal scaled to its best passage for this question, then blended.
export function hybridTop(query, model, index, kw, k = 8, weight = 0.5) {
  const vector = embed(model, query).vector, cosine = cosineTop(vector, index, index.count), lexical = keywordScores(kw, query);
  const bestCos = Math.max(1e-9, cosine[0]?.[0] ?? 0), bestLex = Math.max(1e-9, ...lexical);
  const byPassage = new Float64Array(index.count);
  for (const [score, i] of cosine) byPassage[i] = (1 - weight) * Math.max(0, score) / bestCos;
  lexical.forEach((score, i) => { byPassage[i] += weight * score / bestLex; });
  return [...byPassage].map((score, i) => [score, i]).sort((a, b) => b[0] - a[0]).slice(0, k);
}
