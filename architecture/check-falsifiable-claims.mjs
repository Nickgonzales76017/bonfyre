#!/usr/bin/env bun
/* Candidate detector for success claims that can never report failure.
 * Only a suite that counts failures but does not return them is a hard failure;
 * other findings require human review. A clean scan is not a correctness proof.
 */
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const rootArg = process.argv.find((arg) => arg.startsWith('--root='));
const root = rootArg ? resolve(rootArg.slice(7)) : repo;
const skip = new Set(['.git', 'node_modules', 'build', 'obj', 'dist', 'vendor',
  'legacy_from_10code', '.bonfyre-runtime', 'releases', '.bonfyre', '.run6',
  '.run8', 'generated', 'demos', 'output', 'e2e-results', 'results',
  'checkpoints', 'epochs', 'source_runs', 'archive', 'site']);
const observed = ['present', 'available', 'reachable', 'installed', 'connected',
  'configured', 'verified', 'measured', 'healthy', 'running', 'exists',
  'enabled_by_probe', 'responded', 'answered', 'reached', 'detected',
  'supported_at_runtime'];
const checks = [
  ['literal-observation', new RegExp(`(?:->|\\.|["'])(\\w*(?:${observed.join('|')}))["']?\\s*[:=]\\s*(?:true|True|1)\\s*[,;}\\n]`, 'g'),
    'a field naming an observable condition is set to a literal truth; if the condition is observable here, observe it'],
  ['literal-status-ok', /["']status["']\s*:\s*["'](?:ok|healthy|ready|active)["']/g,
    'a status is written as a literal; a status that cannot say no is not a status'],
  ['unchecked-exit', /(?:subprocess\.run|subprocess\.Popen|Bun\.spawn|execCommand|check_output|os\.popen)\s*\(/g,
    'a subprocess is run; consult its exit status, or an error and an empty result become the same value'],
  ['counted-not-returned', /fail=\$\(\(fail\s*\+\s*1\)\)/g,
    'a failure counter is incremented; the enclosing recipe must exit on it'],
];
const exitMarkers = ['returncode', 'exitCode', 'exit_code', '.ok', 'check=True',
  'check_call', 'check_output', 'raise_for_status', '$?',
  'CalledProcessError', 'shell=True, check', 'capture_output=True) or'];
const failExit = /(?:\[\s*"?\$\$?fail"?\s*-eq\s*0\s*\]|test\s+"?\$\$?fail"?\s+-eq\s+0|exit\s+\$\{?\$?fail)/;
const args = ['rg', '--files', '--hidden', '--no-ignore', '-0',
  ...['py', 'c', 'h', 'ts', 'sh'].flatMap((ext) => ['-g', `*.${ext}`]),
  ...[...skip].flatMap((name) => ['-g', `!**/${name}/**`]), root];
const listed = Bun.spawnSync(args);
if (listed.exitCode !== 0 && listed.exitCode !== 1) {
  console.error(new TextDecoder().decode(listed.stderr));
  process.exit(listed.exitCode ?? 1);
}
const paths = new TextDecoder().decode(listed.stdout).split('\0').filter(Boolean).sort();
const findings = [];
for (const path of paths) {
  const rel = path.slice(root.length + 1);
  if (rel.split('/').some((part) => skip.has(part))) continue;
  if (path.endsWith('.archive') || path.endsWith('.bak') || path === fileURLToPath(import.meta.url)) continue;
  let source;
  try { source = readFileSync(path, 'utf8'); } catch { continue; }
  const lines = source.split(/\r?\n/);
  for (const [name, regex, why] of checks) {
    regex.lastIndex = 0;
    for (const match of source.matchAll(regex)) {
      const index = match.index;
      const lineNo = source.slice(0, index).split('\n').length;
      const line = (lines[lineNo - 1] ?? '').trim();
      if (/^(#|\/\/|\*|\/\*)/.test(line)) continue;
      if (name === 'unchecked-exit' && exitMarkers.some((marker) => source.slice(Math.max(0, index - 1500), index + 1500).includes(marker))) continue;
      if ((name === 'literal-status-ok' || name === 'literal-observation') &&
          (line.includes(' if ') || source.slice(index + match[0].length, index + match[0].length + 20).includes('?') || line.endsWith('?'))) continue;
      if (name === 'counted-not-returned' && failExit.test(source)) continue;
      findings.push([name, rel, lineNo, line.slice(0, 110), why]);
    }
  }
}
if (!findings.length) {
  console.log('no unfalsifiable-claim patterns found (which is not proof that none exist)');
  process.exit(0);
}
const grouped = Map.groupBy(findings, (row) => row[0]);
for (const name of [...grouped.keys()].sort()) {
  const rows = grouped.get(name);
  console.log(`\n${name}  (${rows.length})`);
  console.log(`  ${rows[0][4]}`);
  for (const [, rel, lineNo, line] of rows.slice(0, 25)) console.log(`    ${rel}:${lineNo}: ${line}`);
  if (rows.length > 25) console.log(`    ... and ${rows.length - 25} more`);
}
console.log(`\n${findings.length} candidate(s) across ${grouped.size} pattern(s)`);
console.log('cannot infer: a flagged line -> a defect, or an unflagged file -> an honest one');
const gated = grouped.get('counted-not-returned') ?? [];
if (gated.length) {
  console.log(`\nFAIL: ${gated.length} suite(s) count failures and do not exit on the count`);
  process.exit(1);
}
