// Shared circuit transport: explicit target, bounded reads, opt-in effects.
import { writeFileSync } from 'node:fs';

export function options(args = process.argv.slice(2)) {
  const result = { base: '', parcel: process.env.BONFYRE_CIRCUIT_PARCEL || '', apply: false, allowUntargetedClaim: false, token: process.env.BONFYRE_CIRCUIT_TOKEN || '', out: '' };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--apply') result.apply = true;
    else if (args[i] === '--allow-untargeted-claim') result.allowUntargetedClaim = true;
    else if (['--base', '--parcel', '--out'].includes(args[i]) && args[i + 1]) result[args[i++].slice(2)] = args[i];
    else throw new Error(`unknown or incomplete argument: ${args[i]}`);
  }
  if (!result.base) throw new Error('--base is required; no production host is implicit');
  const base = new URL(result.base);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) throw new Error('--base must be an uncredentialed HTTP(S) origin');
  result.base = base.origin;
  return result;
}

export function circuit(config) {
  const notes = [], fails = [], skips = [];
  const check = (name, condition, detail = '') => {
    const item = `${condition ? 'PASS' : 'FAIL'}  ${name}${detail ? `  — ${detail}` : ''}`;
    (condition ? notes : fails).push(item);
    return Boolean(condition);
  };
  const skip = (name, reason) => skips.push(`SKIP  ${name}  — ${reason}`);
  const http = async (path, { method = 'GET', body, headers = {}, timeout = 30_000, token = '', effect = false } = {}) => {
    if ((method !== 'GET' && method !== 'HEAD' || effect) && !config.apply) throw new Error(`effectful ${method} requires --apply`);
    const url = new URL(path, `${config.base}/`);
    if (url.origin !== config.base) throw new Error('cross-origin circuit request refused');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetch(url, { method, headers: { ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...(token ? { Authorization: `Bearer ${token}` } : {}), ...headers }, body: body === undefined ? undefined : (typeof body === 'string' ? body : JSON.stringify(body)), signal: controller.signal });
      const text = await response.text();
      let data;
      try { data = JSON.parse(text); } catch { data = text; }
      return { status: response.status, data, text, headers: response.headers };
    } finally { clearTimeout(timer); }
  };
  const finish = () => {
    const report = { passed: notes.length, failed: fails.length, skipped: skips.length, notes, fails, skips };
    if (config.out) writeFileSync(config.out, JSON.stringify(report, null, 2) + '\n');
    console.log([...notes, ...fails, ...skips].join('\n'));
    console.log(`${report.passed} passed, ${report.failed} failed, ${report.skipped} skipped`);
    return report.failed ? 1 : 0;
  };
  return { check, skip, http, finish };
}
