#!/usr/bin/env bun
/** SessionStart/PreCompact institution state, bounded and explicitly fallible. */
import { REPO, SURFACE, run, deskQueues, atlasSummary } from './estate-probes.mjs';

export function probeSurface(command = run) {
  const code = command(['curl','-sS','-m','5','-o','/dev/null','-w','%{http_code}',`${SURFACE}/studio/`]);
  if (code !== '200') return [`- surface: ${SURFACE} did not answer 200 (${code || 'unreachable'})`];
  const html = command(['curl','-sS','-m','8','-H','Accept: text/html',`${SURFACE}/studio/`],10);
  const queues = deskQueues(html);
  if (!queues.length) return ['- surface: live, desk did not render its queues'];
  const open = queues.filter(([,count]) => count !== '0')
    .map(([title,count]) => `${count} ${title[0].toLowerCase()}${title.slice(1)}`).join(', ');
  return [open ? `- desk: ${open}` : '- desk: nothing open'];
}
export function probeAtlas(command = run) {
  const {heading,maturity} = atlasSummary(command(['bun','architecture/atlas.mjs','summary'],25));
  if (!heading) return ['- atlas: summary unavailable; run `bun architecture/atlas.mjs validate`'];
  return [`- atlas: ${heading[1]} architectures, ${heading[3]} interactions${maturity ? ` (${maturity})` : ''}`];
}
export function probeDisk(command = run) {
  const raw = command(['df','-h','/System/Volumes/Data']);
  const cells = raw ? raw.split('\n').at(-1).trim().split(/\s+/) : [];
  if (cells.length < 5) return [];
  const percentage = Number.parseInt(cells[4],10);
  const note = Number.isFinite(percentage) && percentage >= 90 ? '  <- the operator paused itself over disk once' : '';
  return [`- disk here: ${cells[3]} free (${cells[4]} used)${note}`];
}
// The native ratchet's figures: JS shrinking toward C is the direction of travel.
export function probeRatchet(command = run) {
  const raw = command(['sh', `${REPO}/scripts/native-ratchet.sh`, 'measure'], 20);
  let m; try { m = JSON.parse(raw); } catch { return []; }
  return [`- native ratchet: ${m.authored_mjs_files} .mjs (${m.authored_mjs_lines} lines), ${m.other_script_files} other scripts, ` +
    `${m.js_runtime_bins ?? '?'} JS-runtime programs installed. Budgets in architecture/native-ratchet.cue only go down; ` +
    'new behaviour goes to C over YAFF/CUE (docs/engineering/mjs-retirement-20261022/PLAN.md)'];
}
export function contextFor(event, command = run) {
  const hook = event.hook_event_name || 'SessionStart';
  const lines = [...probeAtlas(command),...probeSurface(command),...probeDisk(command),...probeRatchet(command)];
  const dirty = command(['git','-C',REPO,'status','--porcelain'],20);
  if (dirty) {
    const paths = dirty.split('\n').filter(Boolean);
    const yaff = paths.filter(path => path.endsWith('.yaff')).length;
    lines.push(`- working tree: ${paths.length} changed paths${yaff ? `, ${yaff} of them atlas entries` : ''}`);
  }
  const context = `Aurekai, as it stands right now:\n${lines.join('\n')}\n\n`+
    'The atlas is canonical: consult it with `bun architecture/atlas.mjs get|search|loss` rather than re-deriving the system, and never read atlas.index.json into context. Skills are available for the estate, the carrier protocols, density work and the desk.';
  return {hookSpecificOutput:{hookEventName:hook,additionalContext:context}};
}
if (import.meta.main) {
  let event = {};
  try { event = JSON.parse(await Bun.stdin.text()); } catch {}
  process.stdout.write(`${JSON.stringify(contextFor(event))}\n`);
}
