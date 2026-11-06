import { test, expect } from 'bun:test';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { runRecursiveNativeWitness } from '../../../tests/recursive_native_witness.mjs';

const tokenBinary = process.env.BONFYRE_TOKEN_FABRIC_BIN || join(homedir(), '.local/bin/bonfyre-token-fabric');
const nativeTest = existsSync(tokenBinary) ? test : test.skip;

nativeTest('disposable native recursive substrate exercises all 22 tools and reopens verified chains', async () => {
  const witness = await runRecursiveNativeWitness({ tokenBinary });
  expect(witness.runtime).toBe('bun');
  expect(witness.unique_tool_count).toBe(22);
  expect(witness.token_delta.operation_count).toBeGreaterThan(0);
  expect(witness.token_delta.reconstructed_sha256).toBe(witness.target.digest);
  expect(witness.ir_types).toHaveLength(7);
  expect(witness.provider_bid).toEqual({ uncached_tokens: 7, compatible: false });
  expect(witness.candidate_only).toBe(true);
  expect(witness.mission).toMatchObject({ bootstrap_state: 'passed', finalize_state: 'passed' });
  expect(witness.receipt_chain.verified).toBe(true);
  expect(witness.flux_chains.verified).toBe(true);
});
