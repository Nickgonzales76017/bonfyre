#!/usr/bin/env bun
// Compare sealed, deployed and running bytes. Agreement does not confer trust.
import { createHash } from 'node:crypto';
import { existsSync, openSync, readSync, closeSync, readFileSync, statSync } from 'node:fs';
import { basename } from 'node:path';

export const defaultUnits = ['bonfyre-web-address', 'bonfyre-mcp-gateway', 'bonfyre-oauth-mcp'];
export const sh = (...argv) => {
  try { return Bun.spawnSync(argv, { stdout: 'pipe', stderr: 'ignore', timeout: 30000 }).stdout?.toString().trim() || ''; }
  catch { return ''; }
};
export function sha256(path) {
  let fd;
  try {
    fd = openSync(path, 'r');
    const hash = createHash('sha256'), chunk = Buffer.allocUnsafe(1 << 20);
    for (;;) { const n = readSync(fd, chunk, 0, chunk.length, null); if (!n) break; hash.update(chunk.subarray(0, n)); }
    return hash.digest('hex');
  } catch { return null; }
  finally { if (fd !== undefined) closeSync(fd); }
}
export function artifactKind(path) {
  let fd;
  try { fd = openSync(path, 'r'); const magic = Buffer.alloc(4); return readSync(fd, magic, 0, 4, 0) === 4 && magic.equals(Buffer.from([127, 69, 76, 70])) ? 'elf' : 'script'; }
  catch { return 'unknown'; }
  finally { if (fd !== undefined) closeSync(fd); }
}
export function sourceNewer(path, pid) {
  try { return statSync(path).mtimeMs > statSync(`/proc/${pid}`).mtimeMs + 1000; }
  catch { return false; }
}
export function sealedIndex(manifest) {
  const index = new Map();
  if (!existsSync(manifest)) return index;
  try {
    for (const line of readFileSync(manifest, 'utf8').split(/\r?\n/)) {
      const match = line.match(/^(\S+)\s+(.+)$/);
      if (match) { const name = basename(match[2].trim()); if (!index.has(name)) index.set(name, match[1]); }
    }
  } catch {}
  return index;
}
export function execPathFromUnitText(source, fileExists = path => existsSync(path)) {
  let path = '';
  for (const line of source.split(/\r?\n/)) {
    if (!line.startsWith('ExecStart=')) continue;
    const value = line.slice('ExecStart='.length).trim();
    if (!value) { path = ''; continue; }
    const tokens = value.split(/\s+/);
    const candidates = tokens.filter(t => t.startsWith('/') && fileExists(t));
    if (!candidates.length) { path = tokens.at(-1) || ''; continue; }
    path = candidates[0];
    const head = basename(path);
    // systemd's env wrapper and interpreted service commands carry a script
    // artifact after the executable. Arguments naming other organs are not it.
    const interpreter = ['python', 'python3', 'node', 'bun', 'sh', 'bash', 'perl', 'ruby', 'java'].includes(head);
    const wrapped = head === 'env' && ['python', 'python3', 'node', 'bun', 'sh', 'bash', 'perl', 'ruby', 'java'].includes(tokens[tokens.indexOf(path) + 1]);
    if ((interpreter || wrapped) && candidates.length > 1) path = candidates[1];
  }
  return path;
}
export const unitExecPath = unit => execPathFromUnitText(sh('systemctl', 'cat', unit));
export function attest(units, manifest, { command = sh, kind = artifactKind, hash = sha256, newer = sourceNewer, unitPath = unitExecPath } = {}) {
  const sealed = sealedIndex(manifest), rows = [];
  let fatal = 0;
  for (const unit of units) {
    const pid = command('systemctl', 'show', '-p', 'MainPID', '--value', unit);
    const deployed = unitPath(unit);
    const row = { unit, active: command('systemctl', 'is-active', unit) || 'unknown', deployed_path: deployed, pid,
      kind: kind(deployed), deployed_sha256: deployed ? hash(deployed) : null, sealed_sha256: deployed ? sealed.get(basename(deployed)) || null : null };
    if (pid && pid !== '0') {
      row.running_inode_deleted = command('readlink', `/proc/${pid}/exe`).endsWith('(deleted)');
      if (row.kind === 'elf') row.running_sha256 = hash(`/proc/${pid}/exe`);
      else { row.running_sha256 = null; row.source_newer_than_process = newer(deployed, pid); }
    } else { row.running_sha256 = null; row.running_inode_deleted = false; }
    const run = row.running_sha256, dep = row.deployed_sha256, seal = row.sealed_sha256;
    if (!pid || pid === '0') row.verdict = 'NOT_RUNNING';
    else if (row.kind !== 'elf' && row.source_newer_than_process) { row.verdict = 'SOURCE_CHANGED_SINCE_START'; fatal++; }
    else if (run && dep && run !== dep) { row.verdict = 'RUNNING_DIFFERS_FROM_DEPLOYED'; fatal++; }
    else if (row.kind !== 'elf') row.verdict = 'interpreted; source unchanged since the process started';
    else if (seal && dep && dep !== seal) row.verdict = 'deployed differs from sealed (expected during development)';
    else if (seal && dep && dep === seal) row.verdict = 'matches sealed release';
    else row.verdict = 'running matches deployed; not present in sealed manifest';
    rows.push(row);
  }
  return { rows, fatal };
}
export function main(argv = process.argv.slice(2)) {
  let manifest = '/opt/bonfyre/releases/command-plane-sealed-20260727T163326Z/sha256-manifest.txt';
  let units = defaultUnits, json = false, entrypoint = '';
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--manifest') manifest = argv[++i];
    else if (argv[i] === '--json') json = true;
    else if (argv[i] === '--entrypoint') entrypoint = argv[++i];
    else if (argv[i] === '--units') { units = []; while (i + 1 < argv.length && !argv[i + 1].startsWith('--')) units.push(argv[++i]); }
    else throw Error(`unknown option: ${argv[i]}`);
  }
  if (entrypoint) { console.log(unitExecPath(entrypoint)); return 0; }
  const { rows, fatal } = attest(units, manifest);
  if (json) console.log(JSON.stringify({ schema_version: 'bonfyre.release.attest.v1', sealed_manifest: manifest, units: rows, running_differs_from_deployed: fatal,
    forbidden_inference: 'agreement is not a claim the bytes are correct, tested or authorized; only that sealed, deployed and running name the same bytes' }, null, 2));
  else {
    console.log(`sealed manifest: ${manifest}`);
    const short = value => value ? value.slice(0, 12) : '--------none';
    for (const row of rows) {
      const run = row.kind === 'elf' ? short(row.running_sha256) : row.kind.padEnd(12);
      console.log(`  ${row.unit.padEnd(24)} ${row.active.padEnd(8)} run=${run} dep=${short(row.deployed_sha256)} seal=${short(row.sealed_sha256)}`);
      console.log(`    ${row.verdict}${row.running_inode_deleted ? '  [running inode is deleted]' : ''}`);
    }
    console.log();
    if (fatal) console.error(`${fatal} unit(s) not executing what is on disk; a restart is owed`);
    else console.log('every running unit is executing what is on disk');
  }
  return fatal ? 1 : 0;
}
if (import.meta.main) process.exit(main());
