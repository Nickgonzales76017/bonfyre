#!/usr/bin/env bun
// Read-only verification of wheel RECORD hashes and unrecorded source/binaries.
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';

const venv = process.argv[2];
if (!venv || !path.isAbsolute(venv)) {
  console.error('usage: bun verify-partner-venv.mjs /absolute/path/to/venv');
  process.exit(2);
}
const root = path.resolve(venv);
const site = path.join(root, 'lib', 'python3.12', 'site-packages');
if (!fs.statSync(site).isDirectory()) throw new Error(`missing site-packages: ${site}`);
const within = (file) => file === root || file.startsWith(root + path.sep);
const relative = (file) => path.relative(root, file);

function recordFields(line) {
  const fields = [];
  let field = '';
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (quoted && line[i + 1] === '"') { field += '"'; i++; }
      else quoted = !quoted;
    } else if (ch === ',' && !quoted) {
      fields.push(field); field = '';
    } else field += ch;
  }
  fields.push(field);
  return fields;
}

async function digest(file, algorithm) {
  const hash = createHash(algorithm);
  for await (const chunk of fs.createReadStream(file)) hash.update(chunk);
  return hash.digest('base64url');
}

const owned = new Set();
const result = {
  venv: root,
  distributions: 0,
  record_entries: 0,
  hashed_entries: 0,
  hashed_bytes: 0,
  unhashed_entries: 0,
  missing: [],
  mismatches: [],
  invalid_records: [],
  unrecorded_py_so: [],
};
for (const dist of fs.readdirSync(site).filter((name) => name.endsWith('.dist-info')).sort()) {
  const record = path.join(site, dist, 'RECORD');
  if (!fs.existsSync(record)) { result.invalid_records.push(`${dist}: missing RECORD`); continue; }
  result.distributions++;
  for (const line of fs.readFileSync(record, 'utf8').split(/\r?\n/)) {
    if (!line) continue;
    result.record_entries++;
    const [entry, hashField] = recordFields(line);
    const file = path.resolve(site, entry);
    if (!within(file)) { result.invalid_records.push(`${dist}: path escapes venv`); continue; }
    owned.add(file);
    if (!hashField) { result.unhashed_entries++; continue; }
    const match = /^(sha256|sha384|sha512)=([A-Za-z0-9_-]+)$/.exec(hashField);
    if (!match) { result.invalid_records.push(`${dist}: unsupported hash for ${relative(file)}`); continue; }
    if (!fs.existsSync(file) || !fs.statSync(file).isFile()) { result.missing.push(relative(file)); continue; }
    const observed = await digest(file, match[1]);
    result.hashed_entries++;
    result.hashed_bytes += fs.statSync(file).size;
    if (observed !== match[2]) result.mismatches.push(relative(file));
  }
}

const scan = [path.join(root, 'bin'), site];
while (scan.length) {
  const dir = scan.pop();
  if (!fs.existsSync(dir)) continue;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== '__pycache__') scan.push(file);
    } else if (entry.isFile() && /\.(?:py|so)$/.test(entry.name) && !owned.has(file)) {
      result.unrecorded_py_so.push(relative(file));
    }
  }
}
result.unrecorded_py_so.sort();
console.log(JSON.stringify(result));
if (result.missing.length || result.mismatches.length || result.invalid_records.length || result.unrecorded_py_so.length) process.exitCode = 1;
