#!/usr/bin/env node
/** Optional owner readback for the existing no-loss review. No execution or writes.
 * Node 22+ node:sqlite is needed only for this opt-in command; the original
 * scripts/performance_equivalence.mjs remains independent of SQLite.
 */
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { reviewPerformance } from './performance_equivalence.mjs';

const isName = x => typeof x === 'string' && x.length > 0 && x.length <= 512 && !x.includes('\0');
const counters = ['bytes_read','bytes_borrowed','bytes_copied','bytes_decompressed',
  'bytes_reencoded','bytes_hydrated','allocations','serialize_count',
  'deserialize_count','materialization_count','borrowed_boundaries','fused_boundaries','useful_bytes'];
const directMetrics = ['bytes_copied','bytes_hydrated','allocations'];
const sql = 'SELECT r.receipt_id,p.identity AS passport,p.subject_family,p.semantic_digest,'
  + 'p.exactness_class,p.measurement_state,'
  + counters.map(k => 'r.' + k).join(',')
  + ' FROM compression_receipt AS r'
  + ' JOIN representation_passport AS p ON p.handle=r.passport_handle'
  + ' WHERE r.receipt_id=?';

export function reviewPhysicalReceipts(record, dbPath) {
  const reviewed = reviewPerformance(record);
  const problems = [...(reviewed.problems ?? reviewed.errors ?? [])];
  const sides = ['baseline','candidate'];
  const ids = sides.map(k => record?.[k]?.physical_receipt_id);
  if (!ids.every(isName) || ids[0] === ids[1])
    problems.push('distinct-native-physical-receipt-ids-required');
  if (!isName(dbPath)) problems.push('physical-registry-path-required');
  let db, rows;
  if (!problems.includes('distinct-native-physical-receipt-ids-required') &&
      !problems.includes('physical-registry-path-required')) {
    try {
      // Read-only opens refuse missing paths instead of inventing empty stores.
      db = new DatabaseSync(dbPath, { readOnly: true });
      const get = db.prepare(sql);
      rows = ids.map(id => get.get(id));
      for (let i = 0; i < 2; i++) {
        const side = sides[i], row = rows[i];
        if (!row) { problems.push(side + ':native-physical-receipt-not-found'); continue; }
        for (const key of counters)
          if (!Number.isSafeInteger(row[key]) || row[key] < 0)
            problems.push(side + ':invalid-native-counter-' + key);
        for (const key of directMetrics)
          if (record[side]?.metrics?.[key] !== row[key])
            problems.push(side + ':claimed-' + key + '-differs-from-native-receipt');
        if (isName(record[side]?.physical_passport) &&
            record[side].physical_passport !== row.passport)
          problems.push(side + ':physical-passport-mismatch');
      }
      if (rows.every(Boolean)) {
        const [before, after] = rows;
        if (!isName(before.subject_family) || before.subject_family !== after.subject_family ||
            !isName(before.semantic_digest) || before.semantic_digest !== after.semantic_digest)
          problems.push('physical-passports-differ-in-semantic-family-or-digest');
        if (before.exactness_class !== 'exact')
          problems.push('baseline-physical-representation-not-exact');
        if (after.exactness_class === 'latent' && record.candidate?.kind !== 'approximate-prefilter')
          problems.push('latent-representation-without-approximation-gate');
        if (!['exact','latent'].includes(after.exactness_class))
          problems.push('candidate-unknown-physical-exactness');
        if (!before.useful_bytes || before.useful_bytes !== after.useful_bytes)
          problems.push('unequal-or-empty-useful-work');
      }
    } catch {
      problems.push('native-physical-registry-unavailable-or-schema-incompatible');
    } finally { try { db?.close(); } catch { /* missing registry stays held */ } }
  }
  const physical = rows?.every(Boolean) ? {
    source: 'read-only BonfyreCompress compression_receipt JOIN representation_passport',
    baseline: { receipt_id:rows[0].receipt_id,passport:rows[0].passport,
      semantic_digest:rows[0].semantic_digest,subject_family:rows[0].subject_family,
      exactness_class:rows[0].exactness_class,
      counters:Object.fromEntries(counters.map(k=>[k,rows[0][k]])) },
    candidate: { receipt_id:rows[1].receipt_id,passport:rows[1].passport,
      semantic_digest:rows[1].semantic_digest,subject_family:rows[1].subject_family,
      exactness_class:rows[1].exactness_class,
      counters:Object.fromEntries(counters.map(k=>[k,rows[1][k]])) }
  } : null;
  return { ...reviewed,
    decision:problems.length?'hold-baseline':reviewed.decision,problems,physical,
    admitted_for_execution:false,no_effect:true,
    cannot_infer:[
      ...reviewed.cannot_infer,
      'persisted counters were independently instrumented rather than owner-reported',
      'local physical records authenticate external authority, results or provider rates',
      'equal useful-byte counts alone establish end-to-end functional equivalence'
    ]
  };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv.length !== 4) throw Error(
      'usage: node scripts/performance_physical_receipts.mjs paired-owner-receipts.json physical.sqlite');
    const verdict = reviewPhysicalReceipts(
      JSON.parse(readFileSync(process.argv[2],'utf8')),process.argv[3]);
    console.log(JSON.stringify(verdict,null,2));
    if (verdict.decision !== 'eligible-for-human-promotion-review') process.exitCode=1;
  } catch(err) { console.error(err.message); process.exitCode=2; }
}
