#!/usr/bin/env bun
// The seal is an observation of deployed bytes, not authorization or quality.
import { hostname } from 'node:os';
const [name, count, entries, excluded] = process.argv.slice(2);
if (!name || [count, entries, excluded].some(x => !/^\d+$/.test(x || ''))) throw Error('usage: release_metadata.mjs NAME ORGAN_COUNT ENTRYPOINTS EXCLUDED');
console.log(JSON.stringify({
  schema_version: 'bonfyre.organ.release.v1', release: name, sealed_at: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z'), host: hostname(),
  organ_count: Number(count), unit_entrypoints_included: Number(entries), excluded_backup_copies: Number(excluded),
  manifest: 'sha256-manifest.txt', manifest_format: 'sha256sum; the format the existing sealed release uses',
  forbidden_inference: 'a sealed artifact set is not a tested, authorized or correct one; it records which bytes were deployed at a moment',
}, null, 2));
