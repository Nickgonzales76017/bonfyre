import { createHash, randomUUID } from 'node:crypto';
import { constants, closeSync, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync,
  readSync, realpathSync, renameSync, statSync, unlinkSync, writeSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, parse, relative, resolve, sep } from 'node:path';
import { deflateRawSync, inflateRawSync } from 'node:zlib';
import { canonicalJson } from './canonical_json.mjs';

const MAGIC = Buffer.from([0x42, 0x46, 0x50, 0x36, 0, 1, 0, 0]);
const CHUNK = 4 * 1024 * 1024;
const MAX_HEADER = 8 * 1024 * 1024;
const MAX_SECTIONS = 4096;
const MAX_DELTA = 128 * 1024 * 1024;
const SAFE_NAME = /^[A-Za-z0-9._/-]+$/;
const SHA = /^[a-f0-9]{64}$/;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const canonBytes = value => Buffer.from(canonicalJson(value, { ascii: false }), 'utf8');

function sectionName(name) {
  if (typeof name !== 'string' || !name || name.startsWith('/') || !SAFE_NAME.test(name) ||
    name.split('/').some(part => !part || part === '.' || part === '..') || Buffer.byteLength(name) > 4096) throw new Error(`unsafe section name: ${name}`);
  return name;
}
function byteSource(value, name) {
  if (!(value instanceof Uint8Array)) throw new Error(`${name} must be bytes`);
  if (value.byteLength > MAX_DELTA) throw new Error(`${name} exceeds delta budget`);
  return Buffer.from(value);
}
function hexBytes(value, name, maximum = MAX_DELTA) {
  if (typeof value !== 'string' || value.length % 2 || value.length > maximum * 2 || !/^[a-f0-9]*$/i.test(value))
    throw new Error(`invalid ${name}`);
  return Buffer.from(value, 'hex');
}
function safeInteger(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`invalid ${name}`);
  return value;
}
function existingStat(path) {
  try { return lstatSync(path); }
  catch (error) { if (error?.code === 'ENOENT') return null; throw error; }
}
function physicalPath(path) {
  const full = resolve(path);
  let ancestor = full;
  while (!existingStat(ancestor)) {
    const parent = dirname(ancestor);
    if (parent === ancestor) throw new Error('BFP path ancestor missing');
    ancestor = parent;
  }
  return resolve(realpathSync(ancestor), relative(ancestor, full));
}
function fileIdentity(fd) {
  const stat = fstatSync(fd, { bigint: true });
  return { dev: stat.dev, ino: stat.ino, size: stat.size, mtimeNs: stat.mtimeNs, ctimeNs: stat.ctimeNs };
}
function assertSameFile(fd, original) {
  const current = fileIdentity(fd);
  if (Object.keys(original).some(key => current[key] !== original[key])) throw new Error('BFP file changed during read');
}
function openRegular(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  const stat = fstatSync(fd);
  if (!stat.isFile() || !Number.isSafeInteger(stat.size)) { closeSync(fd); throw new Error('not a bounded regular file'); }
  return { fd, size: stat.size, identity: fileIdentity(fd) };
}
function writeAll(fd, bytes) {
  let used = 0;
  while (used < bytes.length) {
    const count = writeSync(fd, bytes, used, bytes.length - used);
    if (!count) throw new Error('short BFP write');
    used += count;
  }
}
function exactRead(fd, length, position) {
  const bytes = Buffer.alloc(length); let used = 0;
  while (used < length) {
    const count = readSync(fd, bytes, used, length - used, position + used);
    if (!count) throw new Error('truncated BFP region');
    used += count;
  }
  return bytes;
}
function hashRegion(fd, offset, length, targetFd = null, signal = null) {
  const digest = createHash('sha256'), buffer = Buffer.allocUnsafe(Math.min(CHUNK, Math.max(1, length)));
  let used = 0;
  while (used < length) {
    signal?.throwIfAborted();
    const take = Math.min(buffer.length, length - used);
    const count = readSync(fd, buffer, 0, take, offset + used);
    if (count !== take) throw new Error('truncated file region');
    digest.update(buffer.subarray(0, count));
    if (targetFd !== null) {
      let written = 0;
      while (written < count) written += writeSync(targetFd, buffer, written, count - written);
    }
    used += count;
  }
  return digest.digest('hex');
}
function digestFile(path) {
  const { fd, size, identity } = openRegular(path);
  try { const digest = hashRegion(fd, 0, size); assertSameFile(fd, identity); return digest; }
  finally { closeSync(fd); }
}
function sourceInfo(value) {
  if (typeof value === 'string') {
    const { fd, size, identity } = openRegular(value);
    try { const digest = hashRegion(fd, 0, size); assertSameFile(fd, identity); return { length: size, sha256: digest }; }
    finally { closeSync(fd); }
  }
  if (!(value instanceof Uint8Array)) throw new Error('BFP section source must be bytes or a file path');
  return { length: value.byteLength, sha256: sha(value) };
}
function sourceWrite(value, out, expected) {
  if (typeof value === 'string') {
    const { fd, size, identity } = openRegular(value);
    try {
      if (size !== expected.length || hashRegion(fd, 0, size, out) !== expected.sha256) throw new Error('section source changed during pack');
      assertSameFile(fd, identity);
    } finally { closeSync(fd); }
  } else {
    const bytes = Buffer.from(value);
    if (bytes.length !== expected.length || sha(bytes) !== expected.sha256) throw new Error('section source changed during pack');
    writeAll(out, bytes);
  }
}

export function deltaEncode(source, target, codec = 'COPY_ADD') {
  source = byteSource(source, 'source'); target = byteSource(target, 'target');
  let payload, metadata = {};
  if (codec === 'RAW') payload = target;
  else if (codec === 'DEFLATE_DICT32') {
    const dictionary = source.subarray(Math.max(0, source.length - 32768));
    payload = deflateRawSync(target, { level: 9, dictionary });
    metadata = { dictionary_sha256: sha(dictionary), dictionary_hex: dictionary.toString('hex') };
  } else if (codec === 'COPY_ADD') {
    let prefix = 0, suffix = 0, limit = Math.min(source.length, target.length);
    while (prefix < limit && source[prefix] === target[prefix]) prefix++;
    while (suffix < limit - prefix && source[source.length - 1 - suffix] === target[target.length - 1 - suffix]) suffix++;
    const middle = target.subarray(prefix, suffix ? target.length - suffix : target.length);
    payload = canonBytes({ prefix, suffix, middle_hex: middle.toString('hex') });
  } else throw new Error(`unknown delta codec: ${codec}`);
  return { schema: 'bonfyre.dictionary-delta.v1', codec, source_sha256: sha(source), target_sha256: sha(target),
    payload_hex: payload.toString('hex'), metadata };
}

export function deltaApply(source, doc) {
  source = byteSource(source, 'source');
  if (!doc || typeof doc !== 'object' || Array.isArray(doc) || doc.schema !== 'bonfyre.dictionary-delta.v1' ||
    !SHA.test(doc.source_sha256) || !SHA.test(doc.target_sha256) || sha(source) !== doc.source_sha256)
    throw new Error('source digest or delta schema mismatch');
  const payload = hexBytes(doc.payload_hex, 'payload'); let target;
  if (doc.codec === 'RAW') target = payload;
  else if (doc.codec === 'DEFLATE_DICT32') {
    const dictionary = hexBytes(doc.metadata?.dictionary_hex, 'dictionary', 32768);
    if (sha(dictionary) !== doc.metadata?.dictionary_sha256 ||
      !dictionary.equals(source.subarray(Math.max(0, source.length - 32768)))) throw new Error('delta dictionary mismatch');
    target = inflateRawSync(payload, { dictionary, maxOutputLength: MAX_DELTA });
  } else if (doc.codec === 'COPY_ADD') {
    let operation;
    try { operation = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(payload)); }
    catch { throw new Error('invalid COPY_ADD payload'); }
    const prefix = safeInteger(operation?.prefix, 'prefix'), suffix = safeInteger(operation?.suffix, 'suffix');
    if (prefix + suffix > source.length) throw new Error('COPY_ADD source bounds');
    const middle = hexBytes(operation.middle_hex, 'middle');
    if (prefix + suffix + middle.length > MAX_DELTA) throw new Error('COPY_ADD output budget exceeded');
    target = Buffer.concat([source.subarray(0, prefix), middle, suffix ? source.subarray(source.length - suffix) : Buffer.alloc(0)]);
  } else throw new Error(`unknown delta codec: ${doc.codec}`);
  if (sha(target) !== doc.target_sha256) throw new Error('target digest mismatch');
  return target;
}

export function packBfp(path, sections, metadata = {}) {
  if (!sections || typeof sections !== 'object' || Array.isArray(sections) ||
    !metadata || typeof metadata !== 'object' || Array.isArray(metadata)) throw new Error('invalid BFP sections or metadata');
  const names = Object.keys(sections).map(sectionName).sort();
  if (names.length > MAX_SECTIONS) throw new Error('too many BFP sections');
  let offset = 0;
  const entries = names.map(name => {
    const { length, sha256 } = sourceInfo(sections[name]);
    if (!Number.isSafeInteger(offset + length)) throw new Error('BFP payload size exceeded');
    const entry = { name, offset, length, sha256 }; offset += length; return entry;
  });
  const manifest = { schema: 'bonfyre.bfp-manifest.v1', version: 6, sections: entries, metadata };
  const header = canonBytes(manifest);
  if (header.length < 2 || header.length > MAX_HEADER) throw new Error('BFP header budget exceeded');
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = join(dirname(path), `${basename(path)}.${randomUUID()}.tmp`);
  let out;
  try {
    out = openSync(temporary, 'wx', 0o600);
    writeAll(out, MAGIC);
    const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(header.length)); writeAll(out, length); writeAll(out, header);
    for (const entry of entries) sourceWrite(sections[entry.name], out, entry);
    fsyncSync(out); closeSync(out); out = undefined;
    renameSync(temporary, path);
  } finally { if (out !== undefined) closeSync(out); if (existsSync(temporary)) unlinkSync(temporary); }
  return { path: String(path), sha256: digestFile(path), bytes: statSync(path).size, manifest, sections: entries.length };
}

export function verifyBfp(path, { extract = null, signal = null } = {}) {
  signal?.throwIfAborted();
  const { fd, size, identity } = openRegular(path);
  try {
    if (size < 18) throw new Error('short BFP');
    const prefix = exactRead(fd, 16, 0);
    if (!prefix.subarray(0, 8).equals(MAGIC)) throw new Error('bad BFP magic');
    const length = prefix.readBigUInt64BE(8);
    if (length < 2n || length > BigInt(MAX_HEADER) || length > BigInt(size - 16)) throw new Error('bad BFP header length');
    const header = exactRead(fd, Number(length), 16);
    let manifest;
    try { manifest = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(header)); }
    catch { throw new Error('invalid BFP manifest JSON'); }
    if (!manifest || typeof manifest !== 'object' || manifest.schema !== 'bonfyre.bfp-manifest.v1' || manifest.version !== 6 ||
      !Array.isArray(manifest.sections) || manifest.sections.length > MAX_SECTIONS) throw new Error('invalid BFP manifest');
    const payloadStart = 16 + header.length, payloadBytes = size - payloadStart;
    const seen = new Set(), reconstructed = []; let cursor = 0;
    for (const entry of manifest.sections) {
      signal?.throwIfAborted();
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new Error('invalid BFP section');
      const name = sectionName(entry.name), offset = safeInteger(entry.offset, 'section offset');
      const sectionLength = safeInteger(entry.length, 'section length');
      if (seen.has(name)) throw new Error('duplicate BFP section');
      seen.add(name);
      if (!SHA.test(entry.sha256) || offset !== cursor || sectionLength > payloadBytes - offset) throw new Error('BFP section gap, overlap or bounds');
      const actual = hashRegion(fd, payloadStart + offset, sectionLength, null, signal);
      if (actual !== entry.sha256) throw new Error(`BFP section digest mismatch: ${name}`);
      reconstructed.push({ name, bytes: sectionLength, sha256: actual }); cursor = offset + sectionLength;
    }
    if (cursor !== payloadBytes) throw new Error('unmanifested BFP payload bytes');
    if (extract !== null) for (const entry of manifest.sections) extractSection(fd, payloadStart, entry, extract, signal);
    const digest = hashRegion(fd, 0, size, null, signal);
    signal?.throwIfAborted();
    assertSameFile(fd, identity);
    return { verified: true, path: String(path), sha256: digest, manifest, reconstructed };
  } finally { closeSync(fd); }
}

function extractSection(fd, payloadStart, entry, root, signal) {
  signal?.throwIfAborted();
  const suppliedRoot = resolve(root);
  if (existingStat(suppliedRoot)?.isSymbolicLink()) throw new Error('BFP extraction symlink');
  root = physicalPath(suppliedRoot);
  const destination = resolve(root, entry.name), rel = relative(root, destination);
  if (isAbsolute(rel) || rel === '..' || rel.startsWith(`..${sep}`)) throw new Error('BFP extraction traversal');
  let current = parse(destination).root;
  for (const part of dirname(destination).slice(current.length).split(sep).filter(Boolean)) {
    current = join(current, part);
    const stat = existingStat(current);
    if (stat?.isSymbolicLink()) throw new Error('BFP extraction symlink');
    if (stat && !stat.isDirectory()) throw new Error('BFP extraction parent is not a directory');
    if (!stat) mkdirSync(current, { mode: 0o700 });
  }
  if (existingStat(destination)?.isSymbolicLink()) throw new Error('BFP extraction symlink');
  const temporary = `${destination}.${randomUUID()}.tmp`; let out;
  try {
    out = openSync(temporary, 'wx', 0o600);
    if (hashRegion(fd, payloadStart + entry.offset, entry.length, out, signal) !== entry.sha256) throw new Error('BFP extraction source changed');
    fsyncSync(out); closeSync(out); out = undefined;
    renameSync(temporary, destination);
  } finally { if (out !== undefined) closeSync(out); if (existsSync(temporary)) unlinkSync(temporary); }
}

function confined(path, roots) {
  if (typeof path !== 'string' || !path || path.includes('\0')) throw new Error('invalid BFP path');
  const full = resolve(path), actualRoots = roots.map(root => realpathSync(root));
  const actual = physicalPath(full);
  if (!actualRoots.some(root => { const rel = relative(root, actual); return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel)); }))
    throw new Error('BFP path escapes allowed roots');
  return actual;
}

export function createV6Bfp({ root, home, nativeVerifier = null, run = null } = {}) {
  if (!root || !home) throw new Error('V6 BFP roots required');
  const roots = [root, home];
  return {
    deltaEncode, deltaApply,
    pack(path, sections, metadata = {}) {
      const owned = Object.fromEntries(Object.entries(sections).map(([name, source]) =>
        [name, typeof source === 'string' ? confined(source, roots) : source]));
      return packBfp(confined(path, roots), owned, metadata);
    },
    async verify(path, { extract = null, signal = null } = {}) {
      signal?.throwIfAborted();
      const ownedPath = confined(path, roots);
      const result = verifyBfp(ownedPath, { extract: extract === null ? null : confined(extract, roots), signal });
      if (nativeVerifier && run) {
        const witness = await run([nativeVerifier, ownedPath], { timeoutMs: 120000, maxOutputBytes: 1024 * 1024 });
        let report;
        try { report = JSON.parse(witness.stdout); } catch { report = null; }
        result.native_witness = { state: witness.exit_code === 0 && report?.verified === true ? 'passed' : 'failed',
          exit_code: witness.exit_code, report };
      }
      return result;
    },
  };
}
