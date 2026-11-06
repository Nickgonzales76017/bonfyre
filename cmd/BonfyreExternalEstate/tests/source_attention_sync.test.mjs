import { test, expect } from 'bun:test';
import { syncSourceAttention } from '../tools/sync_source_attention.mjs';

test('attention sync validates plans and keeps dry runs effect-free', () => {
  const rows = [
    { schema: 'bonfyre.source-attention-plan.v1', watch_id: 'source-root:one', subject: 'root-one',
      temperature: 'warm', reheat_on: 'source-change:one', basis: 'pressure',
      evidence: { candidate_occurrences: 4, affected_closures: 2, open_gaps: 1 } },
    { schema: 'bonfyre.source-attention-plan.v1', watch_id: 'bad"watch', subject: 'root-two', temperature: 'hot' },
    { schema: 'other', watch_id: 'other' },
  ];
  const calls = [];
  const options = { attentionBin: '/not-executed', db: '/not-executed.db', dryRun: true, limit: 0 };
  const result = syncSourceAttention(rows, options, undefined, argv => { calls.push(argv); return { exitCode: 0 }; });
  expect(result).toMatchObject({ planned: 2, applied: 0, failed: 1, dry_run: true,
    commands: [{ watch_id: 'source-root:one', temperature: 'warm', reheat_on: 'source-change:one' }] });
  expect(calls).toEqual([]);
  const applied = syncSourceAttention(rows.slice(0, 1), { ...options, dryRun: false }, undefined,
    argv => { calls.push(argv); return { exitCode: 0 }; });
  expect(applied).toMatchObject({ planned: 1, applied: 1, failed: 0, dry_run: false, commands: [] });
  expect(calls[0]).toEqual(['/not-executed', 'schedule', '--db', '/not-executed.db', '--watch', 'source-root:one',
    '--subject', 'root-one', '--temperature', 'warm', '--detail', 'SourceFabric:pressure:p4:i2:g1',
    '--reheat-on', 'source-change:one']);
});
