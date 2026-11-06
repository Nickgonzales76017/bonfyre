// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { sessionIdentity, observation, recordEvent } from '../.claude/hooks/agent-session.mjs';
import { surfaceProbe, atlasProbe, collect, cached, cavemanBadge, render } from '../.claude/hooks/statusline.mjs';
import { probeSurface, probeAtlas, probeDisk, contextFor } from '../.claude/hooks/institution-state.mjs';

const html = '<h3>Decisions</h3><span class="desk-n">3</span><h3>Notices</h3><span class="desk-n">0</span>';
const summary = 'atlas: 443 architectures, 11 views, 333 interactions\n  maturity: measured:109';
function fake(args) {
  if (args[0] === 'curl' && args.includes('%{http_code}')) return '200';
  if (args[0] === 'curl') return html;
  if (args[0] === 'bun') return summary;
  if (args[0] === 'df') return 'Filesystem Size Used Avail Capacity\n/dev/disk 1T 9G 901G 1%';
  if (args[0] === 'git' && args.includes('rev-parse')) return 'feature/native';
  if (args[0] === 'git') return ' M architecture/x.yaff\n M source.c';
  return '';
}
const isolated = run => { const dir = mkdtempSync(join(tmpdir(),'bonfyre-hooks-test-'));
  try { run(dir); } finally { rmSync(dir,{recursive:true,force:true}); } };

test('session identity is stable and not provider reference', () => {
  const id = sessionIdentity('claude-123');
  expect(id).toMatch(/^agent-session:[0-9a-f]{32}$/);
  expect(id).toBe(sessionIdentity('claude-123'));
  expect(id).not.toContain('claude-123');
});
test('boundary is not promoted to model event', () => {
  const row = observation({hook_event_name:'SessionStart',session_id:'s',cwd:'/repo'},undefined,new Date('2026-09-30T00:00:00Z'));
  expect(row.boundary).toBe('sessionstart'); expect(row.sequence).toBeUndefined();
  expect(row.truth).toBe('host_origin_observation_not_receipt_evidence');
});
test('model turns and subagents retain attribution without receipt claim', () => {
  const event = {hook_event_name:'SubagentStop',session_id:'s',model:{id:'claude-model'}};
  const row = observation(event,2,new Date('2026-09-30T00:00:00Z'));
  expect(row.kind).toBe('model_turn'); expect(row.sequence).toBe(2);
  expect(row.provider_identity).toBe('provider:anthropic'); expect(row.model_identity).toBe('claude-model');
  expect(row.subagent).toBe(true); expect(row.cannot_infer).toHaveLength(4);
  expect(row).not.toHaveProperty('producer_verifier_ref');
});
test('compaction names distinct contexts and reason', () => {
  const row = observation({hook_event_name:'PreCompact',session_id:'s',trigger:'auto'},3);
  expect(row.context_before_ref).toBe('context:s:2');
  expect(row.context_after_ref).toBe('context:s:3');
  expect(row.reason_ref).toBe('reason:compaction:auto');
});
test('session event stream is append-only and sequence contiguous', () => isolated(dir => {
  recordEvent({hook_event_name:'SessionStart',session_id:'s'},dir);
  recordEvent({hook_event_name:'Stop',session_id:'s',model:{display_name:'Claude'}},dir);
  recordEvent({hook_event_name:'PreCompact',session_id:'s',trigger:'manual'},dir);
  recordEvent({hook_event_name:'SessionEnd',session_id:'s'},dir);
  const stem = sessionIdentity('s').split(':')[1];
  const rows = readFileSync(join(dir,`${stem}.jsonl`),'utf8').trim().split('\n').map(JSON.parse);
  expect(rows).toHaveLength(4); expect(rows[0].boundary).toBe('sessionstart');
  expect(rows[1].sequence).toBe(1); expect(rows[2].sequence).toBe(2);
  expect(rows[3].boundary).toBe('sessionend');
  expect(readFileSync(join(dir,`${stem}.seq`),'utf8')).toBe('2');
}));
test('status surface probe counts only open desk queues', () => {
  expect(surfaceProbe(fake)).toEqual([true,'3']);
  expect(surfaceProbe(() => '')).toEqual([false,'']);
});
test('status atlas probe uses native summary', () => {
  expect(atlasProbe(fake)[0]).toBe('443a/333i');
});
test('status collection retains all vital signs', () => {
  const value = collect(fake);
  expect(value.up).toBe(true); expect(value.desk).toBe('3');
  expect(value.atlas).toBe('443a/333i'); expect(value.free).toBe('901G');
  expect(value.branch).toBe('feature/native'); expect(value.dirty).toBe(2);
});
test('status cache avoids repeat probes inside TTL', () => isolated(dir => {
  const path = join(dir,'status.json'); let calls=0;
  const command = args => { calls++; return fake(args); };
  const first = cached(command,1_000_000,path), before = calls;
  expect(cached(command,1_010_000,path)).toEqual(first);
  expect(calls).toBe(before);
  cached(command,1_050_000,path); expect(calls).toBeGreaterThan(before);
}));
test('caveman badge refuses symlink and untrusted bytes', () => isolated(dir => {
  const path = join(dir,'flag'); writeFileSync(path,'full');
  expect(cavemanBadge(path)).toContain('[CAVEMAN]');
  writeFileSync(path,'evil\x1b[31m'); expect(cavemanBadge(path)).toBe('');
  writeFileSync(path,'lite'); const link = join(dir,'link'); symlinkSync(path,link);
  expect(cavemanBadge(link)).toBe('');
}));
test('status render preserves model, branch, drift and badge', () => {
  const line = render({model:{display_name:'Claude Opus'}},
    {up:true,desk:'3',atlas:'443a/333i',drift:true,free:'901G',branch:'main',dirty:2},' badge');
  expect(line).toContain('Claude Opus'); expect(line).toContain('main');
  expect(line).toContain('⚠drift'); expect(line).toContain('desk 3'); expect(line).toEndWith(' badge');
});
test('institution probe reports desk, atlas, disk and dirty state', () => {
  expect(probeSurface(fake)).toEqual(['- desk: 3 decisions']);
  expect(probeAtlas(fake)[0]).toContain('443 architectures, 333 interactions');
  expect(probeDisk(fake)).toEqual(['- disk here: 901G free (1% used)']);
  const doc = contextFor({hook_event_name:'PreCompact'},fake);
  expect(doc.hookSpecificOutput.hookEventName).toBe('PreCompact');
  expect(doc.hookSpecificOutput.additionalContext).toContain('2 changed paths, 1 of them atlas entries');
  expect(doc.hookSpecificOutput.additionalContext).toContain('bun architecture/atlas.mjs get|search|loss');
});
test('institution probe degrades to explicit unknowns', () => {
  expect(probeSurface(() => '')[0]).toContain('unreachable');
  expect(probeAtlas(() => '')[0]).toContain('summary unavailable');
  expect(probeDisk(() => '')).toEqual([]);
});
