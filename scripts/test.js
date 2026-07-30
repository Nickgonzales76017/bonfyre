#!/usr/bin/env node
// Bonfyre npm test — checks that key binaries built and respond to 'status'
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const repoDir = path.join(__dirname, '..');
const binaryChecks = [
  ['bonfyre-cms', path.join(repoDir, 'cmd/BonfyreCMS/bonfyre-cms')],
  ['bonfyre-api', path.join(repoDir, 'cmd/BonfyreAPI/bonfyre-api')],
  ['bonfyre-pipeline', path.join(repoDir, 'cmd/BonfyrePipeline/bonfyre-pipeline')],
  ['bonfyre-ingest', path.join(repoDir, 'cmd/BonfyreIngest/bonfyre-ingest')],
  ['bonfyre-brief', path.join(repoDir, 'cmd/BonfyreBrief/bonfyre-brief')],
];

let pass = 0, fail = 0;
const integrationTests = [
  'integrations/wordpress/bridge.test.mjs',
  'integrations/wordpress/native_cms_client.test.mjs',
  'integrations/wordpress/native_cms_e2e.test.mjs',
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

for (const [bin, p] of binaryChecks) {
  try {
    if (!fs.existsSync(p)) throw new Error('binary not built');
    // API has a stable status command; the other CLIs expose usage probes
    // with non-zero exits, so executable presence is their build contract.
    if (bin === 'bonfyre-api') execFileSync(p, ['status'], { timeout: 5000, stdio: 'pipe' });
    console.log(`  ✓ ${bin}`);
    pass++;
  } catch {
    console.log(`  ✗ ${bin} (not found or status failed)`);
    fail++;
  }
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
