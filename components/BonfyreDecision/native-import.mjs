// Offline import of Cloudflare's standalone joint_head.safetensors. No pickle,
// remote code, tensor-name rewriting, or backbone loading is permitted here.
// Release contract: clef-flash 17f0b0ad64efb65d273590632833508766b2aae6,
// joint_schema_model.py load_release_model() (strict standalone head state_dict).
import { closeSync, constants, fstatSync, openSync, readSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { compileNativeDecisionHead, nativeDecisionBinding } from './native-head.mjs';

const fail = message => { throw new Error(`Clef import: ${message}`); };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const configKeys = ['hidden_size', 'width', 'heads', 'feedforward', 'routing_layers', 'layers'];
const dtypeBytes = Object.freeze({ F32: 4, F16: 2, BF16: 2 });
const chunkBytes = 64 * 1024;

function exactKeys(value, keys, label) {
  if (!object(value) || Object.keys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key)))
    fail(`${label} has unexpected or missing fields`);
}

// JSON.parse supplies grammar validation; the token walk additionally refuses
// duplicate decoded keys (including escaped aliases), which JSON.parse erases.
function json(bytes, label) {
  let text, value;
  try { text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes); value = JSON.parse(text); }
  catch { fail(`${label} must be UTF-8 JSON`); }
  const tokens = /"(?:[^"\\]|\\[\s\S])*"|[{}[\],:]|[^\s{}[\],:"]+/g;
  const stack = [];
  for (const [token] of text.matchAll(tokens)) {
    const top = stack.at(-1);
    if (token === '{' || token === '[') {
      if (stack.length === 16) fail(`${label} nesting exceeds limit`);
      stack.push(token === '{' ? { keys: new Set(), key: true } : {});
    } else if (token === '}' || token === ']') stack.pop();
    else if (token === ':') top.key = false;
    else if (token === ',' && top.keys) top.key = true;
    else if (top?.keys && top.key) {
      const key = JSON.parse(token);
      if (top.keys.has(key)) fail(`${label} has duplicate key: ${key}`);
      top.keys.add(key);
    }
  }
  return value;
}

function openRegular(path, maxBytes, label) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = fstatSync(fd, { bigint: true });
    if (!stat.isFile() || stat.size < 1n || stat.size > BigInt(maxBytes)) fail(`${label} is not a bounded regular file`);
    return { fd, stat, size: Number(stat.size), label };
  } catch (error) { closeSync(fd); throw error; }
}
function unchanged(file) {
  const after = fstatSync(file.fd, { bigint: true });
  if (['size', 'mtimeNs', 'ctimeNs', 'dev', 'ino'].some(key => after[key] !== file.stat[key]))
    fail(`${file.label} changed during import`);
}
function read(file, buffer, position) {
  let offset = 0;
  while (offset < buffer.length) {
    const count = readSync(file.fd, buffer, offset, buffer.length - offset, position + offset);
    if (!count) fail(`${file.label} was truncated`);
    offset += count;
  }
  return buffer;
}
function verifyDigest(expected, actual, label) {
  if (expected !== undefined && expected !== actual) fail(`${label} digest mismatch`);
}

/** Import local released head files and snapshot their immutable F32 weights.
 * maxWeightBytes bounds decoded weights, NOT total RSS. maxImportBytes admits
 * three decoded copies (named tensors, packed JS, native snapshot), IO scratch,
 * and header/config parsing allowance; JS runtime/GC overhead is not a hard RSS
 * limit. For untrusted inputs requiring a hard RSS limit, use process isolation.
 * Optional expected digests pin exact source files before native compilation.
 */
export function importNativeClefHead({ configPath, weightsPath,
  binding = nativeDecisionBinding(), maxWeightBytes = 512 * 2 ** 20,
  maxHeaderBytes = 2 ** 20, maxImportBytes = 2 ** 31,
  expectedConfigDigest, expectedWeightDigest } = {}) {
  for (const [name, value] of Object.entries({ maxWeightBytes, maxHeaderBytes, maxImportBytes }))
    if (!integer(value) || value < 4) fail(`invalid ${name}`);
  for (const value of [expectedConfigDigest, expectedWeightDigest])
    if (value !== undefined && (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value))) fail('invalid expected digest');
  const configFile = openRegular(configPath, 16 * 1024, 'config');
  let config, configDigest;
  try {
    const bytes = read(configFile, Buffer.alloc(configFile.size), 0);
    configDigest = hash(bytes); verifyDigest(expectedConfigDigest, configDigest, 'config');
    config = json(bytes, 'config'); exactKeys(config, configKeys, 'config');
    if (configKeys.some(key => !integer(config[key]))) fail('config dimensions must be nonnegative safe integers');
    unchanged(configFile);
  } finally { closeSync(configFile.fd); }

  const layout = binding.layout(config), last = layout.at(-1);
  const weightBytes = (last.offset + last.rows * last.columns) * 4;
  if (!integer(weightBytes) || weightBytes > maxWeightBytes) fail('decoded head exceeds weight budget');
  // Header parsing creates strings, keys and descriptors; the additional factor
  // is an admission allowance, not a claim about an engine's object overhead.
  const admittedBytes = 3 * weightBytes + chunkBytes + 8 * maxHeaderBytes + 16 * 1024;
  if (!integer(admittedBytes) || admittedBytes > maxImportBytes) fail('head exceeds import allocation budget');
  const maxFileBytes = weightBytes + maxHeaderBytes + 8;
  if (!integer(maxFileBytes)) fail('file budget overflow');
  const file = openRegular(weightsPath, maxFileBytes, 'weights');
  let sourceDigest;
  const tensors = Object.create(null);
  try {
    if (file.size < 10) fail('weights lack safetensors header');
    const prefix = read(file, Buffer.alloc(8), 0), length = prefix.readBigUInt64LE();
    if (length < 2n || length > BigInt(maxHeaderBytes) || length > BigInt(file.size - 8)) fail('invalid header length');
    const bytes = read(file, Buffer.alloc(Number(length)), 8);
    if (bytes[0] !== 0x7b) fail('safetensors header must begin with an object');
    const header = json(bytes, 'header');
    if (!object(header)) fail('header must be an object');
    if (Object.hasOwn(header, '__metadata__')) {
      if (!object(header.__metadata__) || Object.values(header.__metadata__).some(value => typeof value !== 'string'))
        fail('metadata must contain only strings');
      delete header.__metadata__;
    }
    exactKeys(header, layout.map(item => item.name), 'tensor inventory');
    const records = layout.map(item => {
      const record = header[item.name], shape = [item.rows, item.columns].slice(0, item.rank);
      exactKeys(record, ['dtype', 'shape', 'data_offsets'], item.name);
      if (!Array.isArray(record.shape) || record.shape.length !== shape.length ||
          shape.some((value, index) => value !== record.shape[index])) fail(`shape mismatch: ${item.name}`);
      const width = Object.hasOwn(dtypeBytes, record.dtype) && dtypeBytes[record.dtype];
      if (!width) fail(`unsupported dtype: ${item.name}`);
      if (!Array.isArray(record.data_offsets) || record.data_offsets.length !== 2 || !record.data_offsets.every(integer))
        fail(`invalid offsets: ${item.name}`);
      const [begin, end] = record.data_offsets, count = item.rows * item.columns;
      if (end - begin !== count * width) fail(`tensor byte length mismatch: ${item.name}`);
      return { name: item.name, shape, dtype: record.dtype, begin, end, count, width };
    }).sort((a, b) => a.begin - b.begin);
    let cursor = 0;
    for (const record of records) {
      if (record.begin !== cursor) fail('tensor data has a gap or overlap');
      cursor = record.end;
    }
    const dataStart = 8 + Number(length);
    if (cursor !== file.size - dataStart) fail('tensor offsets do not cover the complete file');
    const hasher = createHash('sha256').update(prefix).update(bytes);
    const chunk = Buffer.alloc(chunkBytes), bf = new DataView(new ArrayBuffer(4));
    for (const record of records) {
      const data = new Float32Array(record.count);
      for (let offset = 0; offset < record.end - record.begin;) {
        const size = Math.min(chunk.length, record.end - record.begin - offset);
        const part = read(file, chunk.subarray(0, size), dataStart + record.begin + offset);
        hasher.update(part);
        for (let i = 0; i < size; i += record.width) {
          let value;
          if (record.dtype === 'F32') value = part.readFloatLE(i);
          else {
            const bits = part.readUInt16LE(i);
            if (record.dtype === 'BF16') { bf.setUint32(0, bits * 65536, true); value = bf.getFloat32(0, true); }
            else {
              const sign = bits & 0x8000 ? -1 : 1, exponent = (bits >>> 10) & 31, fraction = bits & 1023;
              value = exponent === 31 ? NaN : sign * (exponent === 0 ? fraction * 2 ** -24 : (1024 + fraction) * 2 ** (exponent - 25));
            }
          }
          if (!Number.isFinite(value)) fail(`nonfinite weight: ${record.name}`);
          data[(offset + i) / record.width] = value;
        }
        offset += size;
      }
      tensors[record.name] = { shape: record.shape, data };
    }
    sourceDigest = hasher.digest('hex');
    unchanged(file); verifyDigest(expectedWeightDigest, sourceDigest, 'weights');
  } finally { closeSync(file.fd); }
  const head = compileNativeDecisionHead(config, tensors, { binding, maxWeightBytes });
  return Object.freeze({ ...head, source: Object.freeze({ format: 'safetensors',
    config_digest: configDigest, weights_digest: sourceDigest, weight_bytes: file.size }) });
}
