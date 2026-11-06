import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';

export const cmsPath = process.env.BONFYRE_CMS_DB ?? '/var/lib/bonfyre/cms/bonfyre_cms.db';
export const binDir = process.env.BONFYRE_BIN_DIR ?? '/usr/local/bin';
export const trialsDir = process.env.BONFYRE_TRIAL_RECEIPTS ?? '/var/lib/bonfyre/gym/trials';
export const receiptsDir = process.env.BONFYRE_GYM_RECEIPTS ?? '/var/lib/bonfyre/gym/receipts';
export const windows = Object.freeze({ minutes: 900, hours: 21600, days: 86400, weeks: 604800, months: 2592000 });

export function digestCohort(urls, gym) {
  return createHash('sha256').update(`${[...urls].sort().join('\n')}\n--\ngym=${gym}`).digest('hex').slice(0, 32);
}

export function openCMS(path = cmsPath) {
  return new Database(path, { readonly: true, create: false });
}

export function contractFor(db, gym) {
  const row = db.query("SELECT * FROM live_gym WHERE gym=? AND status='published' LIMIT 1").get(gym);
  if (!row) throw new Error(`no published gym contract named ${gym}`);
  return row;
}

export function providerRoutes(db) {
  return db.query("SELECT route, capability, kind, COALESCE(endpoint,'') AS endpoint, COALESCE(credential_env,'') AS credential_env, COALESCE(unit_cost_usd,'') AS unit_cost_usd FROM provider_route WHERE status='published' ORDER BY route").all();
}

export function routeCloses(route, capability) {
  return route.capability === capability || route.capability.startsWith(`${capability}.`);
}

export async function organ(name, args, timeout = 120_000) {
  try {
    const result = Bun.spawnSync([join(binDir, name), ...args], { stdout: 'pipe', stderr: 'pipe', timeout });
    return { code: result.exitCode ?? (result.signalCode ? 124 : 1), stdout: result.stdout.toString(), stderr: result.stderr.toString() };
  } catch (error) {
    return { code: 127, stdout: '', stderr: String(error) };
  }
}

export function selectTwins(doc, size) {
  const raw = Array.isArray(doc) ? doc : doc?.twins;
  if (!Array.isArray(raw)) return [];
  return raw.filter(t => typeof t.url === 'string' && /^https?:\/\//.test(t.url))
    .sort((a, b) => String(a.last_seen ?? '').localeCompare(String(b.last_seen ?? '')) || a.url.localeCompare(b.url))
    .slice(0, size);
}

export async function liveCohort(size) {
  const result = await organ('bonfyre-project', ['foreign', 'twins'], 90_000);
  if (result.code !== 0 || !result.stdout.trim()) throw new Error('the twin owner returned no cohort');
  return selectTwins(JSON.parse(result.stdout), size);
}

export async function trialReceipts(gym, digest, dir = trialsDir) {
  const result = [];
  let names;
  try { names = await readdir(dir); } catch (error) { if (error.code === 'ENOENT') return result; throw error; }
  for (const name of names.filter(n => n.endsWith('.json')).sort()) {
    try {
      const receipt = JSON.parse(await readFile(join(dir, name), 'utf8'));
      if (receipt.gym === gym && receipt.cohort_digest === digest) result.push(receipt);
    } catch (error) { if (error instanceof SyntaxError || error.code === 'ENOENT') continue; throw error; }
  }
  return result;
}

export function parseOptions(argv, valueFlags = []) {
  const values = new Set(valueFlags), flags = new Set(), opts = {}, positional = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith('--')) { positional.push(arg); continue; }
    if (values.has(arg)) { if (!argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error(`${arg} needs a value`); opts[arg.slice(2)] = argv[++i]; }
    else flags.add(arg);
  }
  return { opts, flags, positional };
}
