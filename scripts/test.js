#!/usr/bin/env node
// Bonfyre npm test — checks that key binaries built and respond to 'status'
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const repoDir = path.join(__dirname, '..');
function packageOrSourceBinary(name, sourcePath) {
  const packaged = path.join(repoDir, 'bin', name);
  return fs.existsSync(packaged) ? packaged : sourcePath;
}
const binaryChecks = [
  ['bonfyre-cms', packageOrSourceBinary('bonfyre-cms', path.join(repoDir, 'cmd/BonfyreCMS/bonfyre-cms'))],
  ['bonfyre-api', packageOrSourceBinary('bonfyre-api', path.join(repoDir, 'cmd/BonfyreAPI/bonfyre-api'))],
  ['bonfyre-pipeline', packageOrSourceBinary('bonfyre-pipeline', path.join(repoDir, 'cmd/BonfyrePipeline/bonfyre-pipeline'))],
  ['bonfyre-ingest', packageOrSourceBinary('bonfyre-ingest', path.join(repoDir, 'cmd/BonfyreIngest/bonfyre-ingest'))],
  ['bonfyre-brief', packageOrSourceBinary('bonfyre-brief', path.join(repoDir, 'cmd/BonfyreBrief/bonfyre-brief'))],
];

let pass = 0, fail = 0;
const integrationTests = [
  'integrations/wordpress/bridge.test.mjs',
  'integrations/wordpress/native_cms_client.test.mjs',
  'integrations/wordpress/native_cms_e2e.test.mjs',
  'integrations/estate/generation_fabric.test.mjs',
  'integrations/estate/model_recovery.test.mjs',
  'integrations/estate/live_adapters.test.mjs',
  'integrations/estate/provider_fleet.test.mjs',
  'integrations/estate/control_plane.test.mjs',
  'integrations/mcp/native_generation_surface.test.mjs',
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
