#!/usr/bin/env bun
// Build-tree inventory, not a signature, runtime attestation or quality claim.
import { createHash } from 'node:crypto';
import { constants, readdirSync, statSync, mkdirSync, readFileSync, renameSync, unlinkSync, openSync, closeSync, writeFileSync } from 'node:fs';
import { open } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';

const utc = () => new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
const ordered = rows => rows.sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
function entries(path) {
  try { return ordered(readdirSync(path).filter(name => !name.startsWith('.'))); }
  catch (error) { if (error.code === 'ENOENT') return []; throw error; }
}
function fileStat(path) {
  try { const st = statSync(path); return st.isFile() ? st : null; }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}
async function fileDigest(path) {
  const fd = await open(path, 'r');
  try {
    const before = await fd.stat({ bigint: true });
    if (!before.isFile()) throw new Error(`not a regular artifact: ${path}`);
    const hash = createHash('sha256'), buffer = Buffer.allocUnsafe(1024 * 1024);
    let size = 0n;
    for (;;) {
      const { bytesRead } = await fd.read(buffer, 0, buffer.length, null);
      if (!bytesRead) break;
      hash.update(buffer.subarray(0, bytesRead)); size += BigInt(bytesRead);
    }
    const after = await fd.stat({ bigint: true });
    const named = statSync(path, { bigint: true });
    if (size !== before.size || ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].some(key =>
      before[key] !== after[key] || before[key] !== named[key]))
      throw new Error(`artifact changed while hashing: ${path}`);
    return hash.digest('hex');
  } finally { await fd.close(); }
}
function label(value, name) {
  if (typeof value !== 'string' || !value || /[\r\n\0]/.test(value)) throw new Error(`invalid ${name}`);
  return value;
}
export function atomicWrite(path, content) {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}-${crypto.randomUUID()}`;
  let fd, failure;
  try {
    fd = openSync(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o644);
    writeFileSync(fd, content, 'utf8'); closeSync(fd); fd = undefined;
    renameSync(temporary, path);
  } catch (error) { failure = error; }
  finally {
    try { if (fd !== undefined) closeSync(fd); } catch (error) { failure ??= error; }
    try { unlinkSync(temporary); } catch (error) { if (error.code !== 'ENOENT') failure ??= error; }
  }
  if (failure) throw failure;
}

export async function compileApplianceManifest({ root = process.cwd(), target = 'bun-linux-x64', release = '0.7.0',
  schema = 'bonfyre.deploy.v1', runtimeAbi = 'bonfyre-abi-v1', defaultModelFamily = 'qwen3-8b',
  builtAt = utc() } = {}) {
  root = resolve(root);
  for (const [name, value] of Object.entries({ target, release, schema, runtimeAbi, builtAt })) label(value, name);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(defaultModelFamily)) throw new Error('invalid default model family');
  const operators = [], libraries = [];
  for (const directory of entries(join(root, 'cmd'))) {
    const path = join(root, 'cmd', directory);
    if (!statSync(path).isDirectory()) continue;
    for (const binary of entries(path)) {
      if (binary !== 'bonfyre' && !binary.startsWith('bonfyre-')) continue;
      const absolute = join(path, binary), st = fileStat(absolute);
      if (!st || !(st.mode & 0o111)) continue;
      operators.push({ name: binary.replaceAll('bonfyre-', ''), binary, sha256: await fileDigest(absolute), required: true });
    }
  }
  for (const name of ['libbonfyre.so', 'libbonfyre.dylib']) {
    const path = join(root, 'lib/libbonfyre', name);
    if (fileStat(path)) libraries.push({ name, sha256: await fileDigest(path), abi: runtimeAbi });
  }
  const material = entries(join(root, 'model-memory')).filter(name => fileStat(join(root, 'model-memory', name)));
  function aliases(suffixes, key, fullStem = false) {
    const grouped = new Map();
    for (const suffix of suffixes) for (const name of material.filter(name => name.endsWith(`.${suffix}`))) {
      const stem = fullStem ? name.slice(0, -(suffix.length + 1)) : name.split('.')[0];
      const row = grouped.get(stem) || { [key]: stem, ...(key === 'model_family' ? { required: false } : {}) };
      row[suffix] = `model-memory/${name}`; grouped.set(stem, row);
    }
    // Preserve historical FPQx order: legacy entries first, then new-only aliases.
    return fullStem ? [...grouped.values()] : ordered([...grouped.keys()]).map(stem => grouped.get(stem));
  }
  const model_memory = aliases(['bfmodel', 'akmodel'], 'model_family');
  if (!model_memory.length) model_memory.push({ model_family: defaultModelFamily,
    bfmodel: `model-memory/${defaultModelFamily}.bfmodel`, akmodel: `model-memory/${defaultModelFamily}.akmodel`,
    required: false, presence: 'not_observed' });
  const git = Bun.spawnSync(['git', 'rev-parse', '--short', 'HEAD'], { cwd: root, stdout: 'pipe', stderr: 'pipe' });
  const revision = git.exitCode === 0 ? `git:${new TextDecoder().decode(git.stdout).trim()}` : 'git:unknown';
  return { schema_version: schema, release, revision, built_at: builtAt, target,
    hyper: { path: 'bin/bonfyre-hyper', bun_target: target, version: release },
    runtime: { operator_count: operators.length, ffi_registry_zero_drift: 'unknown', capabilities_runnable: 'unknown' },
    libraries, operators, model_memory, sae_dicts: aliases(['bfsae', 'aksae'], 'name'),
    fpqx_alignments: aliases(['bffpqx', 'akfpqx'], 'stem', true), signature: { alg: 'none', value: 'TODO' } };
}

export function releaseNotes(manifest) {
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) throw new Error('manifest must be an object');
  const text = (object, key, fallback = 'unknown') => object[key] === undefined ? fallback : label(object[key], key);
  const rows = key => {
    const result = manifest[key] ?? [];
    if (!Array.isArray(result) || result.some(row => !row || typeof row !== 'object' || Array.isArray(row)))
      throw new Error(`manifest ${key} must be an array of objects`);
    return result;
  };
  const operators = rows('operators'), libraries = rows('libraries'), models = rows('model_memory');
  const sae = rows('sae_dicts'), fpqx = rows('fpqx_alignments');
  const lines = [`# Bonfyre Release ${text(manifest, 'release')}`, '', `- Target: ${text(manifest, 'target')}`,
    `- Revision: ${text(manifest, 'revision')}`, `- Built at: ${text(manifest, 'built_at', utc())}`,
    `- Manifest schema: ${text(manifest, 'schema_version')}`, '', '## Runtime', '',
    `- Operators: ${operators.length}`, `- Libraries: ${libraries.length}`, ''];
  if (operators.length) {
    lines.push('### Operators', '');
    for (const op of operators) {
      if (op.required !== undefined && typeof op.required !== 'boolean') throw new Error('operator required must be boolean');
      lines.push(`- ${text(op, 'name')} (${text(op, 'binary')}) required=${op.required ?? false}`);
    }
    lines.push('');
  }
  if (libraries.length) {
    lines.push('### Libraries', '', ...libraries.map(lib => `- ${text(lib, 'name')} abi=${text(lib, 'abi')}`), '');
  }
  function section(title, values, key, modern, legacy, always = false) {
    if (!always && !values.length) return;
    lines.push(`## ${title}`, '');
    if (!values.length) lines.push('- none');
    for (const row of values) {
      const path = row[modern] ? text(row, modern) : text(row, legacy, 'n/a');
      const old = row[legacy] ? text(row, legacy) : null;
      lines.push(`- ${text(row, key)}: ${path}${old && old !== path ? ` (legacy: ${old})` : ''}`);
    }
    lines.push('');
  }
  section('Model Memory', models, 'model_family', 'akmodel', 'bfmodel', true);
  section('SAE Dictionaries', sae, 'name', 'aksae', 'bfsae');
  section('FPQx Alignments', fpqx, 'stem', 'akfpqx', 'bffpqx');
  const signature = manifest.signature ?? {};
  if (typeof signature !== 'object' || Array.isArray(signature)) throw new Error('signature must be an object');
  lines.push('## Signature', '', `- alg: ${text(signature, 'alg', 'none')}`, `- value: ${text(signature, 'value', 'TODO')}`, '');
  return lines.join('\n') + '\n';
}

if (import.meta.main) {
  try {
    const [mode, ...args] = process.argv.slice(2);
    if (!['manifest', 'notes'].includes(mode)) throw new Error('usage: appliance-manifest.mjs manifest|notes [--key value]');
    const allowed = mode === 'manifest' ? ['root', 'target', 'out', 'release', 'schema', 'runtime-abi', 'default-model-family'] : ['manifest', 'out'];
    const { values } = parseArgs({ args, options: Object.fromEntries(allowed.map(key => [key, { type: 'string' }])), strict: true });
    let out, content;
    if (mode === 'manifest') {
      out = values.out || 'bonfyre.manifest.json';
      content = JSON.stringify(await compileApplianceManifest({ root: values.root, target: values.target,
        release: values.release, schema: values.schema, runtimeAbi: values['runtime-abi'],
        defaultModelFamily: values['default-model-family'] }), null, 2) + '\n';
    } else {
      if (!values.manifest) throw new Error('--manifest is required');
      if (statSync(values.manifest).size > 16 * 1024 * 1024) throw new Error('manifest exceeds 16 MiB');
      out = values.out || 'dist/release-notes.md'; content = releaseNotes(JSON.parse(readFileSync(values.manifest, 'utf8')));
    }
    atomicWrite(out, content); console.log(`written: ${out}`);
  } catch (error) { console.error(`appliance manifest: ${error.message}`); process.exitCode = 2; }
}
