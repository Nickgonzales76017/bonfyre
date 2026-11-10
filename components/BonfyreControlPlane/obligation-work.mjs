import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { Database } from 'bun:sqlite';
import { TERMINAL, DISCHARGED } from './commitment-graph.mjs';
import { outstandingAfter } from './protocol-library.mjs';
import { flowWorkCase } from '../BonfyreDecision/frontier-adapters.mjs';
import { assessDecision, decisionDigest } from '../BonfyreDecision/decision-plane.mjs';

export const WORK_TYPE = 'obligation';
export const QUEUE_BIN = process.env.BONFYRE_QUEUE_BIN || resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue');
export class LoweringError extends Error {}

export function queue(args,queueDb) {
  if (!existsSync(QUEUE_BIN)) throw new LoweringError(`native bonfyre-queue not present at ${QUEUE_BIN}`);
  const result = Bun.spawnSync([QUEUE_BIN,...args,'--db',queueDb]);
  if (result.exitCode !== 0) throw new LoweringError(`bonfyre-queue ${args[0]} failed (${result.exitCode}): ${result.stderr.toString().trim() || result.stdout.toString().trim()}`);
  try { return JSON.parse(result.stdout.toString()); }
  catch { throw new LoweringError(`bonfyre-queue ${args[0]} returned non-JSON: ${result.stdout.toString().slice(0,200)}`); }
}

export class ObligationWork {
  constructor(commitments) {
    this.commitments = commitments;
    this.dbPath = commitments.dbPath;
    const db = new Database(this.dbPath);
    try { db.exec(`CREATE TABLE IF NOT EXISTS obligation_work(
      commitment_id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, node_id TEXT NOT NULL,
      queue_db TEXT NOT NULL, lowered_at TEXT NOT NULL DEFAULT '')`); }
    finally { db.close(); }
  }
  read(sql,...args) {
    const db = new Database(this.dbPath,{readonly:true});
    try { return db.query(sql).all(...args); } finally { db.close(); }
  }
  lower(commitmentId,{queue_db,at=''}={}) {
    const state = this.commitments.status(commitmentId);
    if (state === null) throw new LoweringError(`unknown commitment '${commitmentId}'`);
    if (TERMINAL.has(state)) throw new LoweringError(`commitment '${commitmentId}' is ${state}: a settled obligation has no work to claim`);
    const prior = this.read('SELECT mission_id,node_id FROM obligation_work WHERE commitment_id=?',commitmentId)[0];
    if (prior) return [prior.mission_id,prior.node_id];
    const resource = this.read('SELECT resource FROM commitments WHERE commitment_id=?',commitmentId)[0]?.resource || '';
    const receipt = queue(['enqueue',WORK_TYPE,`${commitmentId} :: ${resource}`],queue_db);
    const db = new Database(this.dbPath);
    try { db.query('INSERT INTO obligation_work(commitment_id,mission_id,node_id,queue_db,lowered_at) VALUES(?,?,?,?,?)')
      .run(commitmentId,receipt.mission_id,receipt.node_id,queue_db,at); }
    finally { db.close(); }
    return [receipt.mission_id,receipt.node_id];
  }
  lowerEnactment(contract,enactmentId,{queue_db,at=''}={}) {
    const lowered = outstandingAfter(contract,this.commitments,enactmentId).map(([id]) =>
      [id,...this.lower(id,{queue_db,at})]);
    // Queue admission can update sibling nodes. Describe the settled frontier
    // only after every exact-owner lowering has completed.
    for (const row of lowered) {
      const [id] = row;
      let frontier;
      try {
        const compiled = this.decisionFrontier(contract,id);
        frontier = { state:'planned', action:'none', case_digest:compiled.case_digest,
          mask_digest:compiled.contract.mask_digest };
      } catch (error) {
        // A diagnostic cannot withhold an exact owner's already-admitted work.
        frontier = { state:'unavailable', action:'none', reason:String(error.message).slice(0,240) };
      }
      Object.defineProperty(row,'decision_frontier',{value:frontier});
    }
    return lowered;
  }
  decisionFrontier(contract,commitmentId,{expected_mask_digest}={}) {
    const row = this.read(`SELECT c.commitment_id,c.committer,c.beneficiary,c.resource,
      c.conditions,c.expires_at,c.authority,c.origin,c.enactment,
      (SELECT e.seq FROM commitment_events e WHERE e.commitment_id=c.commitment_id
        ORDER BY e.seq DESC LIMIT 1) AS event_seq,
      (SELECT e.state FROM commitment_events e WHERE e.commitment_id=c.commitment_id
        ORDER BY e.seq DESC LIMIT 1) AS commitment_state,
      w.mission_id,w.node_id,w.queue_db
      FROM commitments c LEFT JOIN obligation_work w ON w.commitment_id=c.commitment_id
      WHERE c.commitment_id=?`,commitmentId)[0];
    if (!row || !row.commitment_state || TERMINAL.has(row.commitment_state))
      throw new LoweringError(`no outstanding commitment frontier for '${commitmentId}'`);
    if (!row.mission_id || !row.node_id || !row.queue_db)
      throw new LoweringError(`commitment '${commitmentId}' has no lowered queue work`);
    if (!Array.isArray(contract?.commitments) || !contract.commitments.some(spec =>
      `${row.enactment}:${spec.key}` === commitmentId))
      throw new LoweringError(`contract does not own commitment '${commitmentId}'`);
    // These are authored phenotype labels, not alternative legal committers.
    const candidateRoles = (contract.roles || []).map(role => ({ id:role.name,
      description:`${role.name} in ${contract.name}`, owner_ref:`ProtocolContract:${contract.family || 'local'}/${contract.name}` }));
    const queueDb = new Database(row.queue_db,{readonly:true});
    let node;
    try { node = queueDb.query(`SELECT status,attempt,updated_at_ms,receipt_id,effect_state
      FROM workgraph_nodes WHERE mission_id=? AND node_id=?`).get(row.mission_id,row.node_id); }
    finally { queueDb.close(); }
    if (!node) throw new LoweringError(`lowered queue node missing for '${commitmentId}'`);
    const exactReadDigest = decisionDigest({ contract, commitment:row, queue_node:node });
    if (expected_mask_digest !== undefined && expected_mask_digest !== exactReadDigest)
      throw new LoweringError(`decision frontier changed for '${commitmentId}'`);
    const work = { id:commitmentId, information_policy_class:'private',
      resource:row.resource, committer:row.committer, beneficiary:row.beneficiary,
      conditions:row.conditions, authority:row.authority,
      commitment_state:row.commitment_state, queue_status:node.status,
      queue_attempt:node.attempt, contract:contract.name };
    return flowWorkCase(work,candidateRoles,exactReadDigest);
  }
  async assessDecisionFrontier(contract,commitmentId,planned,{localEvaluator,signal,timeoutMs,execute=true}={}) {
    if (planned?.state !== 'planned' || !planned.case_digest ||
        !planned.mask_digest)
      throw new LoweringError('a planned frontier with exact case and mask digests is required');
    if (typeof execute !== 'boolean') throw new LoweringError('decision execution must be boolean');
    const compiled = this.decisionFrontier(contract,commitmentId,
      {expected_mask_digest:planned.mask_digest});
    if (compiled.case_digest !== planned.case_digest)
      throw new LoweringError(`decision frontier changed for '${commitmentId}'`);
    // Omission selects the common installed native owner, never a hosted
    // fallback for this private commitment state. Injection remains useful
    // for an embedded owner; it does not weaken either freshness check.
    const receipt = await assessDecision(compiled,{execute,localEvaluator,signal,timeoutMs});
    const current = this.decisionFrontier(contract,commitmentId,
      {expected_mask_digest:planned.mask_digest});
    if (current.case_digest !== planned.case_digest)
      throw new LoweringError(`decision frontier changed for '${commitmentId}'`);
    return receipt;
  }
  workStatus(commitmentId) {
    const row = this.read('SELECT mission_id,node_id,queue_db FROM obligation_work WHERE commitment_id=?',commitmentId)[0];
    if (!row) return null;
    const db = new Database(row.queue_db,{readonly:true});
    try { return db.query('SELECT status FROM workgraph_nodes WHERE mission_id=? AND node_id=?')
      .get(row.mission_id,row.node_id)?.status ?? null; }
    finally { db.close(); }
  }
  lowered() {
    return this.read('SELECT commitment_id,mission_id FROM obligation_work ORDER BY commitment_id')
      .map(row => [row.commitment_id,row.mission_id]);
  }
  settle(commitmentId,{actor='',at=''}={}) {
    const work = this.workStatus(commitmentId);
    if (work === null) throw new LoweringError(`commitment '${commitmentId}' was never lowered`);
    const state = this.commitments.status(commitmentId);
    if (work !== 'complete' || TERMINAL.has(state)) return state;
    this.commitments.discharge(commitmentId,{actor,detail:`work item completed (${work})`,at});
    return this.commitments.status(commitmentId);
  }
  settleAll(options={}) {
    return this.lowered().flatMap(([id]) => {
      const before = this.commitments.status(id), after = this.settle(id,options);
      return before !== after && after === DISCHARGED ? [id] : [];
    }).sort();
  }
}
