import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { library, get, coverage, conformance, causalOrder, enact, outstandingAfter,
  closed, UnknownProtocol } from '../protocol-library.mjs';
import { ProtocolContract, ProtocolError } from '../protocol-contract.mjs';
import { CommitmentGraph, OPEN, VIOLATED } from '../commitment-graph.mjs';

const withGraph = run => { const g = new CommitmentGraph(); try { run(g); } finally { g.close(); } };
const role = (c,name) => c.roles.find(r => r.name === name);
const entitlement = r => new Set(r.reads ?? r.requires ?? []);
function parties(g,c,id,actors) { return enact(c,g,id,actors); }
function partiesOf(g,id) {
  const db = new Database(g.dbPath,{readonly:true});
  try { const row = db.query('SELECT committer,beneficiary FROM commitments WHERE commitment_id=?').get(id);
    return [row.committer,row.beneficiary]; }
  finally { db.close(); }
}

for (const contract of library) {
  const key = `${contract.family}/${contract.name}`;
  test(`${key} is conformant`, () => expect(conformance(contract)).toEqual({liveness:{},safety:{},integrity:{}}));
  test(`${key} orders and opens obligations`, () => {
    const waves = causalOrder(contract);
    expect(waves.length).toBeGreaterThan(0);
    expect(waves.flat()).toHaveLength(contract.roles.length);
    expect(contract.commitments?.length).toBeGreaterThan(0);
  });
  test(`${key} roles declare authority`, () => {
    for (const entry of contract.roles) expect(entry.authority).toBeTruthy();
  });
}
test('contract net call precedes proposal and award', () => {
  expect(causalOrder(get('work','contract-net'))).toEqual([['caller'],['participant'],['awarder'],['awardee']]);
});
test('collapsed manager cycle is reported', () => {
  const p = new ProtocolContract('contract-net-collapsed',{given:['task','deadline']})
    .role('manager',{requires:['task','deadline','proposal'],produces:['call_for_proposals','award']})
    .role('participant',{requires:['call_for_proposals'],produces:['proposal']});
  expect(Object.keys(p.livenessErrors()).sort()).toEqual(['manager','participant']);
  expect(() => p.causalOrder()).toThrow(ProtocolError);
});
test('contract net obliges both directions', () => {
  expect(new Set(get('work','contract-net').commitments.map(c => c.key)))
    .toEqual(new Set(['perform_awarded_task','honor_the_award']));
});
test('delegation obliges delegate to principal', () => withGraph(g => {
  parties(g,get('authority','scoped-delegation'),'d1',
    {principal:'estate:parent',delegate:'estate:child',performer:'estate:child'});
  expect(g.status('d1:perform_within_scope')).toBe(OPEN);
  expect(partiesOf(g,'d1:perform_within_scope')).toEqual(['estate:child','estate:parent']);
}));
test('onward delegation leaves original accountable', () => withGraph(g => {
  const c = get('authority','scoped-delegation');
  parties(g,c,'d1',{principal:'estate:parent',delegate:'estate:child',performer:'estate:child'});
  const id = 'd1:perform_within_scope';
  g.delegate(id,{to_committer:'estate:grandchild',delegated_id:`${id}:sub`});
  expect(g.delegationChain(`${id}:sub`)).toEqual(['estate:child','estate:grandchild']);
  expect(closed(c,g,'d1')).toBe(false);
}));
test('escrow obligations remain opposed', () => withGraph(g => {
  const c = get('economic','escrow');
  parties(g,c,'e1',{payer:'buyer',agent:'escrow-co',deliverer:'seller',releaser:'escrow-co'});
  g.discharge('e1:release_the_held_funds',{detail:'funds released'});
  expect(closed(c,g,'e1')).toBe(false);
  expect(outstandingAfter(c,g,'e1')).toEqual([['e1:deliver_the_agreement',OPEN]]);
}));
test('escrow closes only when both directions settle', () => withGraph(g => {
  const c = get('economic','escrow');
  parties(g,c,'e1',{payer:'buyer',agent:'escrow-co',deliverer:'seller',releaser:'escrow-co'});
  g.discharge('e1:release_the_held_funds');
  g.violate('e1:deliver_the_agreement',{detail:'never shipped'});
  expect(closed(c,g,'e1')).toBe(false);
  g.compensate('e1:deliver_the_agreement',{detail:'refunded from release'});
  expect(closed(c,g,'e1')).toBe(true); expect(g.wasEver('e1:deliver_the_agreement',VIOLATED)).toBe(true);
}));
test('mission lease permits honest escalation', () => withGraph(g => {
  const c = get('work','mission-lease');
  parties(g,c,'m1',{offerer:'institution',claimant:'worker:1',worker:'worker:1',reviewer:'institution'});
  g.discharge('m1:complete_or_escalate',{detail:'escalated: two sources unreachable'});
  expect(closed(c,g,'m1')).toBe(true);
  expect(g.wasEver('m1:complete_or_escalate',VIOLATED)).toBe(false);
}));
test('adjudicator reads evidence rather than claim', () => {
  const c = get('evidence','evidence-challenge');
  expect(entitlement(role(c,'adjudicator'))).toEqual(new Set(['evidence']));
  expect(entitlement(role(c,'adjudicator')).has('claim')).toBe(false);
  expect(conformance(c).integrity).toEqual({});
});
test('reservation obliges availability only', () => {
  const c = get('physical','reservation');
  expect(new Set(c.commitments.map(spec => spec.resource))).toEqual(new Set(['the resource, available through the reserved window']));
  expect(c.commitments.every(spec => spec.committer === 'holder')).toBe(true);
});
test('no family is stub-only', () => {
  const cov = coverage();
  expect(Object.keys(cov).sort()).toEqual([...new Set(library.map(c => c.family))].sort());
  expect(Object.values(cov).every(n => n >= 1)).toBe(true);
  expect(cov.work).toBe(4);
  expect(library.every(c => c.roles.length > 0)).toBe(true);
});
test('unknown lookups name available patterns', () => {
  expect(() => get('work','auction')).toThrow(UnknownProtocol);
  expect(() => get('work','auction')).toThrow('contract-net');
  expect(() => get('nonsense','x')).toThrow('unknown family');
  expect(() => get('meta','agent-ensemble')).toThrow('agent-delegation');
});
test('library has twenty-one protocols across ten families', () => {
  expect(library).toHaveLength(21);
  expect(coverage()).toEqual({identity:2,authority:2,work:4,evidence:1,economic:4,
    data_compute:2,foreign_world:2,physical:1,social:2,meta:1});
  expect(Object.values(coverage()).reduce((a,b) => a+b,0)).toBe(21);
});
test('introduction stakes introducer reputation', () => withGraph(g => {
  parties(g,get('identity','subject-introduction'),'i1',{introducer:'person:nick',recipient:'person:ana'});
  expect(partiesOf(g,'i1:stand_behind_the_introduction')).toEqual(['person:nick','person:ana']);
}));
test('relationship needs admission', () => withGraph(g => {
  const c = get('identity','relationship-admission');
  expect(causalOrder(c)).toEqual([['proposer'],['admitter']]);
  parties(g,c,'r1',{proposer:'a',admitter:'b'});
  expect(g.status('r1:honour_the_relationship')).toBe(OPEN);
  expect(closed(c,g,'r1')).toBe(false);
}));
test('dataset slice is bounded before release', () => {
  const c = get('data_compute','minimum-dataset-slice');
  expect(causalOrder(c)).toEqual([['requester'],['bounder'],['holder']]);
  expect(entitlement(role(c,'holder'))).toEqual(new Set(['slice_bound']));
  expect(entitlement(role(c,'holder')).has('question')).toBe(false);
});
test('compute executor owes the admitter', () => withGraph(g => {
  parties(g,get('data_compute','compute-placement'),'p1',
    {planner:'planner',admitter:'resource-admission',executor:'provider:x'});
  expect(partiesOf(g,'p1:stay_within_the_admitted_ceiling')).toEqual(['provider:x','resource-admission']);
}));
test('foreign observation preflights before fetch', () => {
  expect(causalOrder(get('foreign_world','foreign-observation')))
    .toEqual([['preflight'],['observer'],['receipting']]);
});
test('carrier upgrade verifies evidence before adoption', () => {
  const c = get('foreign_world','carrier-upgrade');
  expect(causalOrder(c)).toEqual([['prober'],['verifier'],['adopter']]);
  expect(role(c,'adopter').requires).toEqual(['upgrade_evidence']);
});
test('agent owes principal, not counterparty', () => withGraph(g => {
  const c = get('meta','agent-delegation');
  parties(g,c,'a1',{principal:'person:nick',agent:'agent:acme',actor:'agent:acme',auditor:'estate:aurekai'});
  expect(partiesOf(g,'a1:act_only_within_the_envelope')).toEqual(['agent:acme','person:nick']);
  expect(entitlement(role(c,'auditor'))).toEqual(new Set(['agent_result','envelope']));
}));
test('group buy commits everybody together', () => withGraph(g => {
  const c = get('economic','collective-purchase');
  expect(causalOrder(c)).toEqual([['initiator'],['participant'],['threshold_check'],['seller']]);
  parties(g,c,'g1',{initiator:'person:nick',participant:'person:ana',threshold_check:'estate:aurekai',seller:'merchant:x'});
  expect(outstandingAfter(c,g,'g1').map(([id]) => id).sort())
    .toEqual(['g1:honour_the_group_price','g1:pay_the_pledged_amount']);
}));
test('ownership transfer is independent of delivery', () => {
  const params = get('economic','ownership-transfer').roles.flatMap(r => r.produces || []);
  expect(params.some(p => p.includes('deliver'))).toBe(false);
  expect(params).toContain('record');
});
test('reviewer reads application only', () => {
  const c = get('work','job-application');
  expect(entitlement(role(c,'reviewer'))).toEqual(new Set(['application']));
  expect(entitlement(role(c,'decider'))).toEqual(new Set(['assessment']));
  expect(conformance(c).integrity).toEqual({});
});
test('quorum is counted by answerable teller', () => {
  const c = get('work','quorum-decision');
  expect(causalOrder(c)).toEqual([['proposer'],['voter'],['teller'],['recorder']]);
  expect(role(c,'teller').produces).toEqual(['quorum_result']);
  expect(entitlement(role(c,'teller'))).toEqual(new Set(['vote','quorum_rule']));
});
test('mutual aid has no money parameter', () => {
  const c = get('social','mutual-aid');
  const params = c.roles.flatMap(r => [...(r.requires || []),...(r.produces || [])]);
  for (const word of ['price','amount','payment','invoice','fee'])
    expect(params.some(p => p.includes(word))).toBe(false);
  expect(new Set(c.commitments.map(spec => spec.committer))).toEqual(new Set(['helper']));
});
