#!/usr/bin/env node
// Bonfyre npm test — checks that key binaries built and respond to 'status'
const { execFileSync } = require('child_process');
const path = require('path');

const binDir = path.join(__dirname, '..', 'bin');
const bins = ['bonfyre-cms', 'bonfyre-api', 'bonfyre-pipeline', 'bonfyre-ingest', 'bonfyre-brief'];

let pass = 0, fail = 0;
const integrationTests = [
  'integrations/wordpress/bridge.test.mjs',
  'integrations/wordpress/native_cms_client.test.mjs',
];
for (const test of integrationTests) {
  try {
    execFileSync(process.execPath, [path.join(__dirname, '..', test)], { timeout: 5000, stdio: 'inherit' });
    console.log(`  ✓ ${test}`);
    pass++;
  } catch {
    console.log(`  ✗ ${test}`);
    fail++;
  }
}

for (const bin of bins) {
  const p = path.join(binDir, bin);
  try {
    execFileSync(p, ['status'], { timeout: 5000, stdio: 'pipe' });
    console.log(`  ✓ ${bin}`);
    pass++;
  } catch {
    console.log(`  ✗ ${bin} (not found or status failed)`);
    fail++;
  }
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
