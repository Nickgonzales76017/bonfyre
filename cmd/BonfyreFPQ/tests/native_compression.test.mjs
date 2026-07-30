import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const run = promisify(execFile);
const commandDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repo = dirname(dirname(commandDir));
const binary = join(commandDir, 'bonfyre-fpq');
const temp = await mkdtemp(join(tmpdir(), 'bonfyre-fpq-native-'));

try {
  const rows = 256;
  const cols = 256;
  const values = new Float32Array(rows * cols);
  for (let i = 0; i < values.length; i++) values[i] = Math.sin(i * 0.01) * 0.5;
  const header = JSON.stringify({ tensor: { dtype: 'F32', shape: [rows, cols], data_offsets: [0, values.byteLength] } });
  const padded = header + ' '.repeat((8 - (header.length % 8)) % 8);
  const input = Buffer.alloc(8 + Buffer.byteLength(padded) + values.byteLength);
  input.writeBigUInt64LE(BigInt(Buffer.byteLength(padded)), 0);
  input.write(padded, 8);
  Buffer.from(values.buffer).copy(input, 8 + Buffer.byteLength(padded));
  const source = join(temp, 'fixture.safetensors');
  const compressed = join(temp, 'compressed.fpq');
  const lossless = join(temp, 'lossless.fpq');
  await writeFile(source, input);

  await run(binary, ['convert-fpq', source, compressed], { cwd: repo });
  await run(binary, ['convert-fpq', source, lossless], {
    cwd: repo,
    env: { ...process.env, BONFYRE_FPQ_LOSSLESS: '1' },
  });
  const compressedBytes = (await stat(compressed)).size;
  const losslessBytes = (await stat(lossless)).size;
  assert.ok(compressedBytes < losslessBytes, `compressed=${compressedBytes} lossless=${losslessBytes}`);
  assert.ok(compressedBytes < values.byteLength, `compressed=${compressedBytes} raw=${values.byteLength}`);
  console.log(`native FPQ compression passed: ${compressedBytes} < ${losslessBytes} bytes`);
} finally {
  await rm(temp, { recursive: true, force: true });
}
