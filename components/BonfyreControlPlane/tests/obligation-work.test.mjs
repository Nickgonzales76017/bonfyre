import { test, expect } from 'bun:test';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { CommitmentGraph, OPEN, DISCHARGED, VIOLATED } from '../commitment-graph.mjs';
import { ObligationWork, LoweringError, QUEUE_BIN, WORK_TYPE, queue } from '../obligation-work.mjs';
import { get, enact, outstandingAfter, closed } from '../protocol-library.mjs';

const run = existsSync(QUEUE_BIN) ? test : test.skip;
function fixture(fn) {
  const c = new CommitmentGraph(), o = new ObligationWork(c), queueDb = join(c.tempDir,'work.db');
  try { fn(c,o,queueDb); } finally { c.close(); }
}
function commitment(c,id='c1',resource='the quoted capability') {
  c.record({commitment_id:id,committer:'seller',beneficiary:'buyer',resource,enactment:'ex1'});
  return id;
}
function claimAnd(action,queueDb,mission,node,failureClass='transient') {
  for (let i=0;i<8;i++) {
    const claim = queue(['claim-next','w1','--lease-ms','60000'],queueDb);
    if (claim.mission_id !== mission || claim.node_id !== node) continue;
    const args = [action,mission,node,'w1',claim.claim_token];
    if (action === 'fail') args.push('--class',failureClass);
    return queue(args,queueDb);
  }
  throw new Error(`never claimed ${mission}/${node}`);
}
function escrow(c) {
  const contract = get('economic','escrow');
  enact(contract,c,'e1',{payer:'buyer',agent:'escrow-co',deliverer:'seller',releaser:'escrow-co'});
  return contract;
}

run('lowering creates claimable work without settling', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  expect(mission).toBeTruthy(); expect(node).toBeTruthy();
  expect(o.workStatus(id)).toBe('ready'); expect(c.status(id)).toBe(OPEN);
}));
run('claiming work does not settle obligation', () => fixture((c,o,q) => {
  const id = commitment(c); o.lower(id,{queue_db:q});
  queue(['claim-next','w1','--lease-ms','60000'],q);
  expect(o.workStatus(id)).toBe('running'); expect(c.status(id)).toBe(OPEN);
  expect(o.settle(id)).toBe(OPEN);
}));
run('completed work discharges obligation', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  claimAnd('ack',q,mission,node);
  expect(o.workStatus(id)).toBe('complete'); expect(o.settle(id)).toBe(DISCHARGED);
  expect(c.delivered(id)).toBe(true);
}));
run('settle all discharges only completed work', () => fixture((c,o,q) => {
  const done = commitment(c,'done'), pending = commitment(c,'pending');
  const [mission,node] = o.lower(done,{queue_db:q}); o.lower(pending,{queue_db:q});
  claimAnd('ack',q,mission,node);
  expect(o.settleAll()).toEqual(['done']); expect(c.status(done)).toBe(DISCHARGED);
  expect(c.status(pending)).toBe(OPEN);
}));
run('failed attempt does not violate obligation', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  claimAnd('fail',q,mission,node);
  expect(o.workStatus(id)).toBe('retry_wait'); expect(c.status(id)).toBe(OPEN);
  expect(c.wasEver(id,VIOLATED)).toBe(false); expect(o.settle(id)).toBe(OPEN);
}));
run('permanent work failure still does not violate obligation', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  claimAnd('fail',q,mission,node,'permanent');
  expect(['dead_letter','failed','retry_wait']).toContain(o.workStatus(id));
  expect(c.status(id)).toBe(OPEN); expect(c.wasEver(id,VIOLATED)).toBe(false);
}));
run('violation remains a standing observation', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  claimAnd('fail',q,mission,node);
  c.violate(id,{actor:'institution',detail:'two attempts, no delivery'});
  expect(c.status(id)).toBe(VIOLATED);
}));
run('lowering twice is idempotent', () => fixture((c,o,q) => {
  const id = commitment(c), first = o.lower(id,{queue_db:q});
  expect(o.lower(id,{queue_db:q})).toEqual(first); expect(o.lowered()).toHaveLength(1);
}));
run('settled obligation has no work to claim', () => fixture((c,o,q) => {
  const id = commitment(c); c.discharge(id,{detail:'delivered directly'});
  expect(() => o.lower(id,{queue_db:q})).toThrow(LoweringError);
  expect(() => o.lower(id,{queue_db:q})).toThrow('no work to claim');
}));
run('unknown commitment cannot be lowered', () => fixture((c,o,q) => {
  expect(() => o.lower('never-promised',{queue_db:q})).toThrow('unknown commitment');
}));
run('settling never-lowered work is refused', () => fixture((c,o) => {
  const id = commitment(c); expect(() => o.settle(id)).toThrow('never lowered');
}));
run('direct discharge needs no work item', () => fixture((c,o) => {
  const id = commitment(c); c.discharge(id,{detail:'handed over in person'});
  expect(c.status(id)).toBe(DISCHARGED); expect(o.workStatus(id)).toBeNull();
}));
run('enactment lowers each outstanding obligation', () => fixture((c,o,q) => {
  const contract = escrow(c), lowered = o.lowerEnactment(contract,'e1',{queue_db:q});
  expect(lowered.map(([id]) => id).sort()).toEqual(['e1:deliver_the_agreement','e1:release_the_held_funds']);
  expect(closed(contract,c,'e1')).toBe(false);
}));
run('enactment closes after all lowered work completes', () => fixture((c,o,q) => {
  const contract = escrow(c);
  for (const [,mission,node] of o.lowerEnactment(contract,'e1',{queue_db:q})) claimAnd('ack',q,mission,node);
  expect(o.settleAll()).toEqual(['e1:deliver_the_agreement','e1:release_the_held_funds']);
  expect(closed(contract,c,'e1')).toBe(true);
}));
run('partial completion leaves enactment open', () => fixture((c,o,q) => {
  const contract = escrow(c), lowered = o.lowerEnactment(contract,'e1',{queue_db:q});
  for (const [id,mission,node] of lowered)
    if (id.endsWith('release_the_held_funds')) claimAnd('ack',q,mission,node);
  o.settleAll(); expect(closed(contract,c,'e1')).toBe(false);
  expect(outstandingAfter(contract,c,'e1')).toEqual([['e1:deliver_the_agreement',OPEN]]);
}));
run('work status is read from queue owner database', () => fixture((c,o,q) => {
  const id = commitment(c), [mission,node] = o.lower(id,{queue_db:q});
  const db = new Database(q,{readonly:true});
  try { const row = db.query('SELECT status,input_uri,operator_id FROM workgraph_nodes WHERE mission_id=? AND node_id=?')
    .get(mission,node);
    expect(row.status).toBe('ready'); expect(row.input_uri).toContain(id);
    expect(row.input_uri).toContain('the quoted capability');
    expect(row.operator_id).toBe(`queue.${WORK_TYPE}`);
    expect(o.workStatus(id)).toBe(row.status);
  } finally { db.close(); }
}));
