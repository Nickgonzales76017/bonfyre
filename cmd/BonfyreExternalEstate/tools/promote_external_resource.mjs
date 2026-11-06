#!/usr/bin/env bun
import { readFileSync, writeFileSync } from 'node:fs';

export function promote(record) {
  if (!record || record.kind !== '211_resource' || record.provider !== '211')
    throw new Error('only normalized 211 resources may promote');
  if (!record.payload || typeof record.payload.functions !== 'string' || !record.payload.functions.trim())
    throw new Error('public 211 service classification required');
  return { ...record, kind: '211_household_opportunity' };
}

if (import.meta.main) {
  try {
    const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, i, all) => {
      if (i % 2 === 0) pairs.push([value.slice(2), all[i + 1]]);
      return pairs;
    }, []));
    if (!args.input || !args.out) throw new Error('usage: --input FILE --out FILE');
    const result = promote(JSON.parse(readFileSync(args.input, 'utf8')));
    writeFileSync(args.out, JSON.stringify(result) + '\n');
    console.log(JSON.stringify({ kind: result.kind, truth: 'public service classification only' }));
  } catch (error) { console.error(`external resource promotion: ${error.message}`); process.exitCode = 2; }
}
