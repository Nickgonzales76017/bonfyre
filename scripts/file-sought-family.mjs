#!/usr/bin/env bun
// Provisionally file admitted channels by authored search provenance.
import { Database } from 'bun:sqlite';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

const args = process.argv.slice(2);
let estate = join(homedir(), '.bonfyre/estate-local');
let apply = false;
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--estate' && args[i + 1]) estate = args[++i];
  else if (args[i] === '--apply') apply = true;
  else {
    process.stderr.write('usage: bun scripts/file-sought-family.mjs [--estate DIR] [--apply]\n');
    process.exit(2);
  }
}
estate = resolve(estate);
if (!existsSync(join(estate, 'frontier.db')) || (apply && !existsSync(join(estate, 'cms.db')))) {
  process.stderr.write(`file-sought-family: required estate databases are absent under ${estate}\n`);
  process.exit(2);
}
const policy = Bun.spawnSync(['cue', 'export', new URL('./sought-families.cue', import.meta.url).pathname,
  '-e', 'soughtFamilies', '--out', 'json'], { stdout: 'pipe', stderr: 'pipe' });
if (policy.exitCode !== 0) {
  process.stderr.write(policy.stderr);
  process.exit(policy.exitCode ?? 1);
}
const families = JSON.parse(policy.stdout.toString());
const ledger = new Database(join(estate, 'frontier.db'), { readonly: !apply, strict: true });
let attached = false;
try {
  if (apply) {
    ledger.query('ATTACH DATABASE ? AS cms').run(join(estate, 'cms.db'));
    attached = true;
    ledger.exec('BEGIN IMMEDIATE');
  }
  const report = {};
  const changes = new Map();
  let already = 0;
  let waiting = 0;
  let measuredConflict = 0;
  for (const { family, labels } of families) {
    if (!family || !Array.isArray(labels) || labels.length === 0) throw new Error('invalid sought-family declaration');
    const placeholders = labels.map(() => '?').join(',');
    const rows = ledger.query(`SELECT DISTINCT c.channel_id, COALESCE(c.census_source,'') AS census_source,
      COALESCE(c.material,'') AS material, COALESCE(c.contributions,0) AS contributions
      FROM channel c JOIN channel_origin o ON o.channel_id=c.channel_id
      WHERE o.label IN (${placeholders}) AND c.state IN ('eligible','admitted','active')
      ORDER BY c.channel_id`).all(...labels);
    report[family] = { admitted: rows.length };
    for (const row of rows) {
      if (row.material === family) { already++; continue; }
      if (!row.census_source) { waiting++; continue; }
      if (row.contributions > 0) { measuredConflict++; continue; }
      const previous = changes.get(row.channel_id);
      if (previous && previous.family !== family)
        throw new Error(`ambiguous search family for ${row.channel_id}: ${previous.family}, ${family}`);
      changes.set(row.channel_id, { family, censusSource: row.census_source });
    }
  }
  if (apply) {
    const updateChannel = ledger.query('UPDATE channel SET material=? WHERE channel_id=?');
    const updateCms = ledger.query("UPDATE cms.internet_source SET family=? WHERE source=? AND status='published'");
    for (const [channel, change] of changes) {
      updateChannel.run(change.family, channel);
      updateCms.run(change.family, change.censusSource);
    }
    ledger.exec('COMMIT');
  }
  process.stdout.write(JSON.stringify({ families: report, filed: changes.size,
    already_filed: already, admitted_without_a_census_row: waiting,
    observed_carriage_preserved: measuredConflict, applied: apply,
    cannot_infer: 'a search label provisionally files what this estate asked for; it is not a measurement of carriage, and observed contributions outrank it',
  }, null, 2) + '\n');
} catch (error) {
  if (apply) { try { ledger.exec('ROLLBACK'); } catch {} }
  process.stderr.write(`file-sought-family: ${error.message}\n`);
  process.exitCode = 1;
} finally {
  if (attached) { try { ledger.exec('DETACH DATABASE cms'); } catch {} }
  ledger.close();
}
