#!/usr/bin/env bun
// Extract candidate copy from Clang JSON-sequence AST; never publishes CMS state.
import { readFileSync, writeFileSync, realpathSync } from 'node:fs';
import { createHash } from 'node:crypto';

const sha = value => createHash('sha256').update(value).digest('hex');
const words = /[A-Za-z][A-Za-z0-9'’_-]*/g;
const formats = /%(?:[-+ #0]*\d*(?:\.\d+)?[hljztL]*[diuoxXfFeEgGaAcspn%])/g;
const media = /^[A-Za-z0-9.+_-]+\/[A-Za-z0-9.+_-]+(?:\s*;|$)/;
const sql = /^(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|PRAGMA|ATTACH|DETACH|WITH|REPLACE|BEGIN|COMMIT|ROLLBACK|VACUUM|ANALYZE)\b/i;

export function* jsonSequence(text) {
  let start = -1, depth = 0, quoted = false, escaped = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (start < 0) { if (ch === '{' || ch === '[') { start = i; depth = 1; } continue; }
    if (quoted) { if (escaped) escaped = false; else if (ch === '\\') escaped = true; else if (ch === '"') quoted = false; continue; }
    if (ch === '"') quoted = true;
    else if (ch === '{' || ch === '[') depth++;
    else if (ch === '}' || ch === ']') {
      if (--depth === 0) {
        const value = JSON.parse(text.slice(start, i + 1));
        if (value && !Array.isArray(value) && typeof value === 'object') yield value;
        start = -1;
      }
    }
  }
  if (start >= 0) throw new Error('incomplete Clang JSON sequence');
}

export function literalValue(spelling) {
  if (typeof spelling !== 'string') return '';
  try { return JSON.parse(spelling); }
  catch {
    if (spelling.length >= 2 && spelling[0] === '"' && spelling.at(-1) === '"') return spelling.slice(1, -1);
    return spelling;
  }
}

function repairMojibake(value) {
  if (!/[ÃÂÅâ]/u.test(value)) return value;
  const fixed = Buffer.from(value, 'latin1').toString('utf8');
  return fixed.includes('\uFFFD') ? value : fixed;
}

export function classify(value) {
  const text = value.trim();
  if (!text) return 'empty';
  if (text.startsWith('HTTP/') || text.slice(0, 160).includes('\r\n') || media.test(text)) return 'protocol';
  if (sql.test(text)) return 'sql';
  if (text.startsWith('/') && !/\s/u.test(text)) return 'route';
  if ((text.includes('<') && text.includes('>')) || text.startsWith('<!')) return 'markup';
  if ('[{'.includes(text[0]) && (text.includes('":') || text.includes('"jsonrpc"'))) return 'protocol';
  if (text.includes('{') && text.includes('}') && text.includes(':')) return 'style';
  const stripped = text.replace(formats, ' ');
  const wordCount = [...stripped.matchAll(words)].length;
  const visible = [...text].filter(ch => !/\s/u.test(ch)).length;
  const alpha = [...text].filter(ch => /\p{L}/u.test(ch)).length;
  if (wordCount >= 3 && visible && alpha / visible >= 0.35) return 'copy';
  if (formats.test(text)) return 'format';
  return 'control';
}

const named = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: '\u00a0', ndash: '–', mdash: '—', hellip: '…', rsquo: '’', lsquo: '‘', rdquo: '”', ldquo: '“', copy: '©', reg: '®' };
function entities(text) {
  return text.replace(/&(#(?:x[0-9a-f]+|[0-9]+)|[A-Za-z][A-Za-z0-9]+);/gi, (whole, key) => {
    if (key[0] !== '#') return named[key] ?? whole;
    const point = key[1]?.toLowerCase() === 'x' ? parseInt(key.slice(2), 16) : parseInt(key.slice(1), 10);
    return point > 0 && point <= 0x10ffff ? String.fromCodePoint(point) : whole;
  });
}

export async function textNodes(value, role) {
  if (role === 'copy') { const body = value.replace(formats, ' ').trim().replace(/\s+/gu, ' '); return body ? [body] : []; }
  if (role !== 'markup') return [];
  const nodes = [];
  let skip = 0, current = '';
  await new HTMLRewriter()
    .on('script,style', { element(element) { skip++; element.onEndTag(() => skip--); } })
    .onDocument({ text(node) {
      if (!skip) current += node.text;
      if (node.lastInTextNode) {
        const value = entities(current).trim().replace(/\s+/gu, ' ');
        if ([...value.matchAll(words)].length >= 2) nodes.push(value);
        current = '';
      }
    } })
    .transform(new Response(value)).text();
  return nodes;
}

function offset(node, side) {
  const range = node.range?.[side] || {};
  const loc = range.spellingLoc || range;
  return Number.isInteger(loc.offset) ? loc.offset : null;
}
function endOffset(node) {
  const range = node.range?.end || {};
  const loc = range.spellingLoc || range;
  return Number.isInteger(loc.offset) ? loc.offset + (loc.tokLen || 0) : null;
}
function starts(source) {
  const result = [0];
  for (let i = 0; i < source.length; i++) if (source[i] === 10) result.push(i + 1);
  return result;
}
function lineColumn(lines, at) {
  if (at === null) return [null, null];
  let low = 0, high = lines.length;
  while (low + 1 < high) { const mid = (low + high) >> 1; if (lines[mid] <= at) low = mid; else high = mid; }
  return [low + 1, at - lines[low] + 1];
}

async function walk(node, fn, source, lines, path, out) {
  if (node.kind === 'FunctionDecl' && node.name) fn = String(node.name);
  if (node.kind === 'StringLiteral') {
    const value = repairMojibake(literalValue(node.value));
    const begin = offset(node, 'begin'), end = endOffset(node), [line, column] = lineColumn(lines, begin);
    const raw = begin !== null && end !== null && begin >= 0 && end <= source.length && begin <= end ? source.subarray(begin, end) : null;
    const digest = sha(raw?.length ? raw : Buffer.from(value));
    const role = classify(value), ref = `source:${path}#${fn || 'global'}@L${line || 0}:C${column || 0}`;
    out.push({ candidate_id: sha(`${ref}:${digest}`).slice(0, 24), function: fn || null, line, column, role, value, text_nodes: await textNodes(value, role), source_ref: ref, source_digest: digest });
  }
  for (const child of node.inner || []) if (child && typeof child === 'object' && !Array.isArray(child)) await walk(child, fn, source, lines, path, out);
}

export async function compileCandidates(sequence, source, path) {
  const candidates = [], lines = starts(source);
  for (const root of jsonSequence(sequence)) await walk(root, '', source, lines, path, candidates);
  const roles = {}, functions = {};
  for (const candidate of candidates) {
    roles[candidate.role] = (roles[candidate.role] || 0) + 1;
    const fn = candidate.function || 'global';
    functions[fn] = (functions[fn] || 0) + 1;
  }
  return { schema_version: 'bonfyre.surface.literal-candidates.v2', source: path, source_sha256: sha(source), candidate_count: candidates.length, human_text_node_count: candidates.reduce((sum, candidate) => sum + candidate.text_nodes.length, 0), roles, functions, candidates, forbidden_inference: 'a source literal candidate is not authored CMS state, publication, an exact SurfaceIR coordinate, or evidence that a rendered phrase is still wanted' };
}

export async function main(args = process.argv.slice(2)) {
  if (args.length !== 3) throw new Error('usage: ast_literal_candidates.mjs CLANG_AST.json SOURCE.c OUTPUT.json');
  const path = realpathSync(args[1]), source = readFileSync(path);
  const payload = await compileCandidates(readFileSync(args[0], 'utf8'), source, path);
  writeFileSync(args[2], JSON.stringify(payload, null, 2) + '\n');
  console.log(JSON.stringify(Object.fromEntries(['schema_version', 'candidate_count', 'human_text_node_count', 'roles'].map(key => [key, payload[key]]))));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
