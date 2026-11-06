#!/usr/bin/env bun
import { readFileSync, writeFileSync } from 'node:fs';
import { normalizeProviderRecord } from './collect_public_carrier.mjs';

const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
  if (!process.argv[i]?.startsWith('--') || !process.argv[i + 1]) throw new Error('expected --key value arguments');
  args[process.argv[i].slice(2)] = process.argv[i + 1];
}
try {
  const record = normalizeProviderRecord(args.provider, JSON.parse(readFileSync(args.input, 'utf8')),
    args['source-ref'], args['receipt-ref'], args['observed-at']);
  writeFileSync(args.out, JSON.stringify(record) + '\n');
  console.log(JSON.stringify({ kind: record.kind, private_fields: 'excluded' }));
} catch (error) { console.error(`incumbent provider normalizer: ${error.message}`); process.exitCode = 2; }
