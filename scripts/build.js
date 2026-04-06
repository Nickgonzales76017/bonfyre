#!/usr/bin/env node
// Bonfyre npm postinstall — compiles C binaries from source
const { execSync } = require('child_process');
const { existsSync, mkdirSync } = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const binDir = path.join(root, 'bin');

console.log('🔥 Building Bonfyre from source...');

if (!existsSync(binDir)) mkdirSync(binDir, { recursive: true });

try {
  execSync('make all', { cwd: root, stdio: 'inherit' });

  // Copy key binaries to bin/ for npm bin links
  const bins = [
    'bonfyre-cms', 'bonfyre-api', 'bonfyre-pipeline', 'bonfyre-ingest',
    'bonfyre-transcribe', 'bonfyre-brief', 'bonfyre-pack', 'bonfyre-auth',
    'bonfyre-cli', 'bonfyre-hash', 'bonfyre-gate', 'bonfyre-meter'
  ];

  const { readdirSync } = require('fs');
  const cmdDir = path.join(root, 'cmd');
  for (const dir of readdirSync(cmdDir)) {
    const full = path.join(cmdDir, dir);
    for (const file of readdirSync(full)) {
      if (file.startsWith('bonfyre-')) {
        const src = path.join(full, file);
        const dst = path.join(binDir, file);
        require('fs').copyFileSync(src, dst);
        require('fs').chmodSync(dst, 0o755);
      }
    }
  }

  console.log('✓ Bonfyre built successfully');
} catch (e) {
  console.error('Build failed. Ensure you have a C11 compiler and SQLite3 headers installed.');
  console.error('  macOS:  xcode-select --install');
  console.error('  Ubuntu: sudo apt install build-essential libsqlite3-dev');
  process.exit(1);
}
