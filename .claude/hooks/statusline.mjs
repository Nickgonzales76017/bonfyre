#!/usr/bin/env bun
/** Bounded, cached host vital signs for Claude's per-turn status line. */
import { lstatSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { REPO, SURFACE, run, deskQueues, atlasSummary, diskFree } from './estate-probes.mjs';

const repoKey = createHash('sha256').update(REPO).digest('hex').slice(0,12);
const cachePath = join(process.env.TMPDIR || tmpdir(),`aurekai-statusline-${repoKey}.json`);
const ttl = 45;
const cavemanPath = join(process.env.CLAUDE_CONFIG_DIR || join(homedir(),'.claude'),'.caveman-active');
const DIM='\x1b[2m', OFF='\x1b[0m', AMBER='\x1b[38;5;172m', TEAL='\x1b[38;5;37m';
const modes = new Set(['lite','full','ultra','wenyan-lite','wenyan','wenyan-full',
  'wenyan-ultra','commit','review','compress']);

export function surfaceProbe(fetch = run) {
  const html = fetch(['curl','-fsS','-m','3','-H','Accept: text/html',`${SURFACE}/studio/`]);
  if (!html) return [false,''];
  const open = deskQueues(html).filter(([,count]) => count !== '0').map(([,count]) => count);
  return [true,open.join('/')];
}
export function atlasProbe(command = run) {
  const raw = command(['bun','architecture/atlas.mjs','summary'],20);
  const {heading} = atlasSummary(raw);
  const counts = heading ? `${heading[1]}a/${heading[3]}i` : '?';
  let drift = false;
  try {
    const index = statSync(join(REPO,'architecture','atlas.index.json')).mtimeMs;
    const glob = new Bun.Glob('**/*.yaff');
    for (const path of glob.scanSync({cwd:join(REPO,'architecture')}))
      if (statSync(join(REPO,'architecture',path)).mtimeMs > index) { drift = true; break; }
  } catch {}
  return [counts,drift];
}
export function collect(command = run) {
  const [up,desk] = surfaceProbe(command), [atlas,drift] = atlasProbe(command);
  const branch = command(['git','rev-parse','--abbrev-ref','HEAD']) || '?';
  const dirty = command(['git','status','--porcelain']).split('\n').filter(Boolean).length;
  return {up,desk,atlas,drift,free:diskFree(command(['df','-h','/System/Volumes/Data'])),branch,dirty};
}
export function cached(command = run, now = Date.now(), path = cachePath) {
  try { const doc = JSON.parse(readFileSync(path,'utf8'));
    if (now / 1000 - doc.at < ttl) return doc.v;
  } catch {}
  const value = collect(command);
  try { writeFileSync(path,JSON.stringify({at:now/1000,v:value})); } catch {}
  return value;
}
export function cavemanBadge(path = cavemanPath) {
  try {
    const info = lstatSync(path);
    if (!info.isFile() || info.isSymbolicLink()) return '';
    const mode = readFileSync(path).subarray(0,64).toString('utf8').toLowerCase().replace(/[^a-z0-9-]/g,'');
    if (!modes.has(mode)) return '';
    const label = mode === 'full' ? 'CAVEMAN' : `CAVEMAN:${mode.toUpperCase()}`;
    return `  ${AMBER}[${label}]${OFF}`;
  } catch { return ''; }
}
export function render(event, value, badge = cavemanBadge()) {
  const model = event.model?.display_name || 'claude';
  const dot = value.up ? '\x1b[32m●\x1b[0m' : '\x1b[31m○\x1b[0m';
  const parts = [`${TEAL}[AUREKAI]${OFF} ${dot}`,`\x1b[36m${value.branch}\x1b[0m`];
  if (value.dirty) parts.push(`\x1b[33m±${value.dirty}\x1b[0m`);
  const atlas = `atlas ${value.atlas}${value.drift ? ' \x1b[33m⚠drift\x1b[0m' : ''}`;
  parts.push(`${DIM}${atlas}${OFF}`);
  if (value.desk) parts.push(`${DIM}desk ${value.desk}${OFF}`);
  parts.push(`${DIM}disk ${value.free}${OFF}`,`${DIM}${model}${OFF}`);
  return parts.join('  ') + badge;
}
if (import.meta.main) {
  let event = {};
  try { event = JSON.parse(await Bun.stdin.text()); } catch {}
  process.stdout.write(`${render(event,cached())}\n`);
}
