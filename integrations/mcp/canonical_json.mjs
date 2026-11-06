import { createHash } from 'node:crypto';

const MAX_DOCUMENT = 8 * 1024 * 1024;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
export const compareUnicode = (a, b) => {
  const left = Array.from(a, c => c.codePointAt(0)), right = Array.from(b, c => c.codePointAt(0));
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
};
function quote(value, ascii) {
  if (!ascii && Array.from(value).some(c => { const point = c.codePointAt(0); return point >= 0xd800 && point <= 0xdfff; }))
    throw new Error('unpaired surrogate cannot encode as UTF-8');
  const quoted = JSON.stringify(value);
  return ascii ? quoted.replace(/[\u007f-\uffff]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`) : quoted;
}
class NumericSource {
  constructor(source) { this.source = source; }
  // Lexical type is needed for historical hashes, not for the public wire ABI.
  toJSON() { return Number(this.source); }
}
function floatText(value) {
  if (Object.is(value, -0)) return '-0.0';
  const magnitude = Math.abs(value);
  if (magnitude !== 0 && (magnitude < 0.0001 || magnitude >= 1e16))
    return value.toExponential().replace(/e([+-])(\d)$/, 'e$10$2');
  return String(value);
}
// SQLite REAL fields are floats when read by the legacy owner, even when their
// numeric value is integral. Bind that type explicitly when hashing new rows.
export function jsonFloat(value) {
  if (!Number.isFinite(value)) throw new Error('non-finite JSON float');
  const source = floatText(value);
  return new NumericSource(source.includes('.') || source.includes('e') ? source : `${source}.0`);
}
// Existing Python documents encode ASCII escapes and distinguish 1 from 1.0.
// Raw JSON number sources retain that distinction during historical verification.
export function canonicalJson(value, { ascii = true, spaced = false } = {}) {
  if (typeof ascii !== 'boolean' || typeof spaced !== 'boolean') throw new Error('invalid canonical JSON mode');
  const comma = spaced ? ', ' : ',', colon = spaced ? ': ' : ':';
  let nodes = 0;
  function encode(item, depth) {
    if (++nodes > 200000 || depth > 64) throw new Error('JSON structure budget exceeded');
    if (item instanceof NumericSource) return item.source;
    if (item === null || typeof item === 'boolean') return String(item);
    if (typeof item === 'string') return quote(item, ascii);
    if (typeof item === 'number') {
      if (!Number.isFinite(item)) throw new Error('non-finite JSON number');
      if (Number.isSafeInteger(item) && !Object.is(item, -0)) return String(item);
      return floatText(item);
    }
    if (Array.isArray(item)) return `[${item.map(child => encode(child, depth + 1)).join(comma)}]`;
    if (!object(item) || ![Object.prototype, null].includes(Object.getPrototypeOf(item))) throw new Error('not a JSON value');
    return `{${Object.keys(item).sort(compareUnicode).map(key => `${quote(key, ascii)}${colon}${encode(item[key], depth + 1)}`).join(comma)}}`;
  }
  const result = encode(value, 0);
  if (Buffer.byteLength(result) > MAX_DOCUMENT) throw new Error('JSON byte budget exceeded');
  return result;
}
export const digestValue = (value, options) => hash(canonicalJson(value, options));
export function parsedDocument(source, { ascii = true, spaced = false, omitKey = 'digest' } = {}) {
  if (typeof source !== 'string' || Buffer.byteLength(source) > MAX_DOCUMENT) throw new Error('JSON byte budget exceeded');
  const omitted = omitKey === null ? [] : Array.isArray(omitKey) ? omitKey : [omitKey];
  if (omitted.some(key => typeof key !== 'string') || omitted.length > 32) throw new Error('invalid omitted digest key');
  const raw = JSON.parse(source, (_key, value, context) => {
    if (typeof value !== 'number') return value;
    if (!Number.isFinite(value)) throw new Error('non-finite JSON number');
    return new NumericSource(context.source);
  });
  const plain = JSON.parse(source);
  if (!object(plain) || !object(raw)) throw new Error('document must be an object');
  for (const key of omitted) delete raw[key];
  return { value: plain, calculated: digestValue(raw, { ascii, spaced }) };
}
