import { expect, test } from 'bun:test';
import { join } from 'node:path';
import { CommitmentGraph, OPEN } from '../commitment-graph.mjs';
import { ObligationWork, LoweringError, queue } from '../obligation-work.mjs';
import { enact, get } from '../protocol-library.mjs';
import { installedDecisionOwner } from '../../BonfyreDecision/installed-runtime.mjs';

function fixture(callback) {
  const commitments = new CommitmentGraph();
  const work = new ObligationWork(commitments);
  const queueDb = join(commitments.tempDir, 'work.db');
  const contract = get('economic', 'escrow');
  enact(contract, commitments, 'e1', { payer: 'buyer', agent: 'escrow-co',
    deliverer: 'seller', releaser: 'escrow-co' });
  return Promise.resolve().then(() => callback({ commitments, work, queueDb, contract }))
    .finally(() => commitments.close());
}
const localEvaluator = () => ({ model: 'untrained-test-fixture-not-clef',
  artifact_digest: 'f'.repeat(64), question_semantics: 'joint_schema', deterministic: true,
  evaluate: ({ schema }) => new Float32Array(schema.width) });

test('ordinary enactment lowering exposes a reconstructable typed frontier without changing queue ownership', async () =>
  fixture(async ({ commitments, work, queueDb, contract }) => {
    const lowered = work.lowerEnactment(contract, 'e1', { queue_db: queueDb });
    expect(lowered).toHaveLength(2);
    for (const row of lowered) {
      expect(row).toHaveLength(3);
      const [id, mission, node] = row;
      expect(Array.isArray(row)).toBe(true);
      expect(row.decision_frontier.state).toBe('planned');
      expect(row.decision_frontier.action).toBe('none');
      expect(row.decision_frontier.case_digest).toMatch(/^[a-f0-9]{64}$/);
      expect(row.decision_frontier.mask_digest).toMatch(/^[a-f0-9]{64}$/);
      expect(Object.keys(row)).toEqual(['0', '1', '2']);
      const compiled = work.decisionFrontier(contract, id,
        { expected_mask_digest: row.decision_frontier.mask_digest });
      expect(compiled.case_digest).toBe(row.decision_frontier.case_digest);
      expect(compiled.contract).toMatchObject({ owner: 'Flow', gate_state: 'ADMIT',
        information_policy_class: 'private' });
      expect(compiled.contract.frontier.map(candidate => candidate.id)).toEqual(
        contract.roles.map(role => role.name));
      expect(compiled.state.work).toMatchObject({ id, queue_status: 'ready',
        information_policy_class: 'private' });
      expect(work.workStatus(id)).toBe('ready');
      expect(commitments.status(id)).toBe(OPEN);
      expect(mission).toBeTruthy(); expect(node).toBeTruthy();
    }
    const [id,, , planned] = lowered[0];
    expect(planned).toBeUndefined();
    const receipt = await work.assessDecisionFrontier(contract, id,
      lowered[0].decision_frontier, { localEvaluator: localEvaluator() });
    expect(receipt).toMatchObject({ state: 'diagnostic', provider: 'local', action: 'none',
      case_digest: lowered[0].decision_frontier.case_digest,
      proposal: { action: 'none' } });
    expect(receipt.proposal).toHaveProperty('suggested_candidate');
    expect(commitments.status(id)).toBe(OPEN);
    expect(work.workStatus(id)).toBe('ready');
  }));

test('queue and commitment changes invalidate an old planned frontier before model execution', async () =>
  fixture(async ({ commitments, work, queueDb, contract }) => {
    const lowered = work.lowerEnactment(contract, 'e1', { queue_db: queueDb });
    const [id] = lowered[0], planned = lowered[0].decision_frontier;
    let calls = 0;
    const local = localEvaluator();
    local.evaluate = ({ schema }) => { calls++; return new Float32Array(schema.width); };
    const claim = queue(['claim-next', 'worker', '--lease-ms', '60000'], queueDb);
    expect(claim.mission_id).toBeTruthy();
    await expect(work.assessDecisionFrontier(contract, id, planned,
      { localEvaluator: local })).rejects.toThrow('changed');
    expect(calls).toBe(0);
    const current = work.decisionFrontier(contract, id);
    expect(current.case_digest).not.toBe(planned.case_digest);
    commitments.violate(id, { actor: 'observer', detail: 'unmet' });
    expect(() => work.decisionFrontier(contract, id,
      { expected_mask_digest: current.contract.mask_digest })).toThrow('changed');
  }));

test('Flow plans through the default installed native owner without injection or model execution', async () =>
  fixture(async ({ commitments, work, queueDb, contract }) => {
    const [row] = work.lowerEnactment(contract, 'e1', { queue_db: queueDb });
    const receipt = await work.assessDecisionFrontier(contract, row[0], row.decision_frontier,
      { execute: false });
    expect(receipt).toMatchObject({ state: 'planned', provider: 'local', action: 'none',
      model: installedDecisionOwner.model, model_artifact_digest: installedDecisionOwner.artifact_digest,
      question_semantics: 'joint_schema', contract: { owner: 'Flow', information_policy_class: 'private' } });
    expect(receipt.inference).toBeUndefined();
    expect(receipt.proposal).toBeUndefined();
    expect(work.workStatus(row[0])).toBe('ready');
    expect(commitments.status(row[0])).toBe(OPEN);
    const cancel = new AbortController(); cancel.abort(new Error('cancel before native startup'));
    await expect(work.assessDecisionFrontier(contract, row[0], row.decision_frontier,
      { signal: cancel.signal })).rejects.toThrow('cancel before native startup');
    queue(['claim-next', 'worker', '--lease-ms', '60000'], queueDb);
    await expect(work.assessDecisionFrontier(contract, row[0], row.decision_frontier,
      { execute: false })).rejects.toThrow('changed');
  }));

test('inference-time state changes refuse stale diagnostics after the shared evaluator returns', async () =>
  fixture(async ({ work, queueDb, contract }) => {
    const lowered = work.lowerEnactment(contract, 'e1', { queue_db: queueDb });
    const [id] = lowered[0], planned = lowered[0].decision_frontier;
    const local = localEvaluator();
    local.evaluate = ({ schema }) => {
      queue(['claim-next', 'worker', '--lease-ms', '60000'], queueDb);
      return new Float32Array(schema.width);
    };
    await expect(work.assessDecisionFrontier(contract, id, planned,
      { localEvaluator: local })).rejects.toThrow('changed');
  }));

test('decision compilation refusal is visible but never withholds exact native queue lowering', async () =>
  fixture(async ({ commitments, work, queueDb, contract }) => {
    const malformed = structuredClone(contract);
    malformed.roles.push({ ...malformed.roles[0] });
    const lowered = work.lowerEnactment(malformed, 'e1', { queue_db: queueDb });
    expect(lowered).toHaveLength(2);
    for (const row of lowered) {
      expect(row).toHaveLength(3);
      expect(row.decision_frontier).toMatchObject({ state: 'unavailable', action: 'none' });
      expect(row.decision_frontier.reason).toContain('duplicate');
      expect(work.workStatus(row[0])).toBe('ready');
      expect(commitments.status(row[0])).toBe(OPEN);
      await expect(work.assessDecisionFrontier(malformed, row[0], row.decision_frontier,
        { localEvaluator: localEvaluator() })).rejects.toThrow(LoweringError);
    }
  }));
