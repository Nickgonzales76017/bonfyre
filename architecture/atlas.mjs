#!/usr/bin/env bun
// The Architecture Atlas is native: cmd/BonfyreAtlas (C over YAFF and CUE).
// This name stays so `bun architecture/atlas.mjs ...` keeps working.
import { spawnSync } from 'node:child_process';
const run = spawnSync(new URL('./atlas-native.sh', import.meta.url).pathname, process.argv.slice(2), { stdio: 'inherit' });
process.exitCode = run.status ?? 1;
