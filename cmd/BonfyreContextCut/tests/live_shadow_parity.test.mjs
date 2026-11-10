import { test, expect } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { contextCasesFromManifest } from '../../../components/BonfyreDecision/frontier-adapters.mjs';

const estate = process.env.BONFYRE_ESTATE ?? '/Users/nickgonzales/.bonfyre/estate';
const tokenizer = process.env.GIGATOKEN_TOKENIZER ?? '/Users/nickgonzales/BonfyreModels/small/qwen2.5-coder-0.5b-instruct/tokenizer.json';
const native = process.env.GIGATOKEN_BIN ?? resolve('cmd/BonfyreContextCut/bonfyre-gigatoken-native');
const admission = resolve('cmd/BonfyreAdmission/bonfyre-admission');
const fixture = join(estate, 'bitnet-shadow-context.json');

test.skipIf(![fixture, tokenizer, native, admission].every(existsSync))('native GigaToken replays the 92-candidate SHADOW context byte-for-byte', () => {
  const temporary = mkdtempSync(join(tmpdir(), 'bonfyre-context-live-'));
  try {
    const result = Bun.spawnSync(['bun', resolve('cmd/BonfyreContextCut/context_cut.mjs'), 'compile', '--shadow-diagnostic',
      '--rank-db', join(estate, 'rank.db'), '--run-id', 'bitnet:1789271781', '--address-db', join(estate, 'estate.db'),
      '--material-manifest', join(estate, 'context-material.jsonl'), '--tokenizer', tokenizer,
      '--tokenizer-passport', 'qwen2.5-coder-0.5b-instruct', '--gigatoken-bin', native,
      '--token-cache-db', join(temporary, 'tokens.db'), '--information-policy-class', 'public',
      '--purpose', 'Explain the retained public bodies',
      '--token-budget', '200000', '--max-candidates', '32', '--out', join(temporary, 'context.json'),
      '--admission-bin', admission, '--admission-db', join(temporary, 'admission.db'), '--admission-plane', 'context',
      '--admission-volume', temporary, '--admission-estimated-bytes', '65536', '--admission-free-bytes', '20000000000']);
    expect(result.exitCode, new TextDecoder().decode(result.stderr)).toBe(0);
    const actual = JSON.parse(readFileSync(join(temporary, 'context.json'), 'utf8'));
    const expected = JSON.parse(readFileSync(fixture, 'utf8'));
    for (const field of ['context_sha256', 'token_sha256', 'selected_tokens', 'unique_bodies', 'rank_gate_state', 'truth', 'token_spans']) expect(actual[field]).toEqual(expected[field]);
    expect(actual.selected_candidates.map(row => row.identity)).toEqual(expected.selected_candidates.map(row => row.identity));
    expect(actual.resource_admission.grant_released).toBe(true);
    const decisionCases = contextCasesFromManifest(actual);
    const materials = new Map(readFileSync(join(estate, 'context-material.jsonl'), 'utf8')
      .trim().split('\n').map(line => JSON.parse(line)).map(row => [row.candidate_identity, row]));
    expect(actual.decision_frontier.state).toBe('reconstructable');
    expect(actual.decision_frontier.resident_case_pack_bytes).toBe(0);
    expect(actual.decision_frontier.provider_calls).toBe(0);
    expect(() => contextCasesFromManifest({ ...actual, purpose: 'different purpose' }))
      .toThrow('recipe changed');
    expect(decisionCases.length).toBeGreaterThan(actual.selected_candidates.length);
    for (const selected of actual.selected_candidates) {
      const chunks = decisionCases.filter(item => item.state.passage.source_candidate_id === selected.identity);
      expect(chunks.length).toBeGreaterThan(1);
      expect(chunks.map(item => item.state.passage.text).join(''))
        .toBe(readFileSync(materials.get(selected.identity).body_path, 'utf8'));
    }
    // The local material manifest has since been reduced to the two retained
    // bodies; omission counts are input-relative, not historical constants.
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});
