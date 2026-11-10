import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { ProtocolContract, ProtocolError, quoteAccept } from '../protocol-contract.mjs';
import { conformance, conformant, causalOrder, enact, outstandingAfter, closed } from '../protocol-library.mjs';
import { CommitmentGraph, OPEN, VIOLATED } from '../commitment-graph.mjs';
import { InformationPolicy } from '../information-policy.mjs';

const actors = {buyer:'agent:acme',seller:'aurekai.life',settlement:'aurekai.life',deliverer:'aurekai.life'};
const withGraph = run => { const g = new CommitmentGraph(); try { run(g); } finally { g.close(); } };

test('ordering derives from information dependencies', () => {
  expect(causalOrder(quoteAccept)).toEqual([['buyer'],['seller'],['settlement'],['deliverer']]);
});
test('independent roles share wave', () => {
  const p = new ProtocolContract('dual-check',{given:['request']})
    .role('credit',{requires:['request'],produces:['credit_ok']})
    .role('identity',{requires:['request'],produces:['identity_ok']})
    .role('decision',{requires:['credit_ok','identity_ok'],produces:['decision']});
  expect(p.causalOrder()).toEqual([['credit','identity'],['decision']]);
});
test('role needing nothing starts immediately', () => {
  const p = new ProtocolContract('kickoff').role('opener',{produces:['request']})
    .role('responder',{requires:['request'],produces:['reply']});
  expect(p.causalOrder()).toEqual([['opener'],['responder']]);
});
test('liveness catches unsatisfied role', () => {
  const p = new ProtocolContract('broken',{given:['request']})
    .role('seller',{requires:['request'],produces:['quote']})
    .role('shipper',{requires:['payment_confirmation'],produces:['shipment']});
  expect(p.livenessErrors()).toEqual({shipper:['payment_confirmation']});
});
test('ordering refuses contract that waits forever', () => {
  const p = new ProtocolContract('broken',{given:['request']})
    .role('shipper',{requires:['payment_confirmation'],produces:['shipment']});
  expect(() => p.causalOrder()).toThrow(ProtocolError);
  expect(() => p.causalOrder()).toThrow('payment_confirmation');
  expect(() => p.causalOrder()).toThrow('waits forever');
});
test('cycle is liveness, not a hang', () => {
  const p = new ProtocolContract('cyclic').role('a',{requires:['from_b'],produces:['from_a']})
    .role('b',{requires:['from_a'],produces:['from_b']});
  expect(Object.keys(p.livenessErrors()).sort()).toEqual(['a','b']);
});
test('healthy contract has no liveness error', () => expect(conformance(quoteAccept).liveness).toEqual({}));
test('safety detects racing producers', () => {
  const p = new ProtocolContract('racy',{given:['request']})
    .role('seller',{requires:['request'],produces:['price']})
    .role('repricer',{requires:['request'],produces:['price']});
  expect(p.safetyErrors()).toEqual({price:['repricer','seller']});
});
test('healthy contract has no safety errors', () => expect(conformance(quoteAccept).safety).toEqual({}));
test('integrity catches unentitled input', () => {
  const p = new ProtocolContract('leaky',{given:['account','owner_mailing_address']})
    .role('scorer',{requires:['account','owner_mailing_address'],produces:['contactability_score'],reads:['account']});
  expect(p.integrityErrors()).toEqual({scorer:['owner_mailing_address']});
});
test('integrity consults disclosure policy', () => {
  const policy = new InformationPolicy('Parcel')
    .disclose('account',{to:['public','institution']})
    .disclose('owner_mailing_address',{to:['institution'],reason:'contactable'});
  const p = new ProtocolContract('scored',{given:['account','owner_mailing_address']})
    .role('scorer',{requires:['account','owner_mailing_address'],produces:['score'],
      reads:['account','owner_mailing_address']});
  expect(p.integrityErrors()).toEqual({});
  expect(p.integrityErrors(policy,'public')).toEqual({scorer:['owner_mailing_address']});
  expect(p.integrityErrors(policy,'institution')).toEqual({});
});
test('role producing nothing cannot fail integrity', () => {
  const p = new ProtocolContract('reader',{given:['secret']}).role('auditor',{requires:['secret'],reads:[]});
  expect(p.integrityErrors()).toEqual({});
});
test('healthy conformance reports all three clean', () => {
  expect(conformant(quoteAccept)).toBe(true);
  expect(conformance(quoteAccept)).toEqual({liveness:{},safety:{},integrity:{}});
});
test('structural conformance is not actor permission', () => {
  expect(conformant(quoteAccept)).toBe(true);
  expect(quoteAccept.roles.find(r => r.name === 'deliverer').authority).toBe('deliver:capability');
  expect(JSON.stringify(conformance(quoteAccept))).not.toContain('agent:acme');
});
test('enactment opens obligations in commitment graph', () => withGraph(g => {
  expect(enact(quoteAccept,g,'ex1',actors)).toEqual(['ex1:delivery','ex1:payment']);
  expect(g.status('ex1:delivery')).toBe(OPEN); expect(g.status('ex1:payment')).toBe(OPEN);
}));
test('completed messages do not settle obligations', () => withGraph(g => {
  enact(quoteAccept,g,'ex1',actors);
  expect(causalOrder(quoteAccept).at(-1)).toEqual(['deliverer']);
  expect(closed(quoteAccept,g,'ex1')).toBe(false);
  expect(outstandingAfter(quoteAccept,g,'ex1')).toEqual([['ex1:delivery',OPEN],['ex1:payment',OPEN]]);
}));
test('enactment closes only after every obligation is terminal', () => withGraph(g => {
  enact(quoteAccept,g,'ex1',actors); g.discharge('ex1:delivery',{detail:'capability returned'});
  expect(closed(quoteAccept,g,'ex1')).toBe(false);
  g.violate('ex1:payment',{detail:'x402 settlement never verified'});
  expect(closed(quoteAccept,g,'ex1')).toBe(false);
  g.compensate('ex1:payment',{detail:'written off'});
  expect(closed(quoteAccept,g,'ex1')).toBe(true); expect(g.wasEver('ex1:payment',VIOLATED)).toBe(true);
}));
test('commitments carry originating enactment', () => withGraph(g => {
  enact(quoteAccept,g,'ex1',actors);
  const db = new Database(g.dbPath,{readonly:true});
  try { expect(db.query('SELECT enactment FROM commitments WHERE commitment_id=?').get('ex1:delivery').enactment).toBe('ex1'); }
  finally { db.close(); }
}));
test('two enactments do not collide', () => withGraph(g => {
  enact(quoteAccept,g,'ex1',actors); enact(quoteAccept,g,'ex2',actors);
  g.discharge('ex1:delivery'); expect(g.status('ex2:delivery')).toBe(OPEN);
  expect(closed(quoteAccept,g,'ex2')).toBe(false);
}));
test('nonconformant enactment refuses before opening obligations', () => withGraph(g => {
  const p = new ProtocolContract('broken',{given:['request']})
    .role('buyer',{requires:['request'],produces:['order']})
    .role('shipper',{requires:['payment_confirmation'],produces:['shipment']})
    .commits('ship',{committer:'shipper',beneficiary:'buyer',resource:'the goods'});
  expect(() => p.enact(g,'ex1',{buyer:'a',shipper:'b'})).toThrow('non-conformant');
  expect(g.status('ex1:ship')).toBeNull();
}));
test('commitment cannot name undeclared role', () => {
  expect(() => new ProtocolContract('typo').role('buyer',{produces:['order']})
    .commits('ship',{committer:'shipper',beneficiary:'buyer',resource:'goods'})).toThrow('does not declare');
});
test('every role needs an actor at enactment', () => withGraph(g => {
  expect(() => enact(quoteAccept,g,'ex1',{buyer:'agent:acme'})).toThrow('no actor for role');
}));
test('role cannot be declared twice', () => {
  expect(() => new ProtocolContract('dup').role('buyer').role('buyer')).toThrow(ProtocolError);
});
