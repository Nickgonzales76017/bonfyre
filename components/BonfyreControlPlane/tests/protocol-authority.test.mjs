import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { AuthorityGraph, AuthorityRefused, DelegationRefused } from '../protocol-authority.mjs';
import { CommitmentGraph } from '../commitment-graph.mjs';
import { quoteAccept } from '../protocol-library.mjs';

const subject = 'capability:parcel-underwrite';
const actors = {buyer:'agent:acme',seller:'aurekai.life',settlement:'aurekai.life',deliverer:'aurekai.life'};
const withGraph = run => { const g = new AuthorityGraph(); try { run(g); } finally { g.close(); } };
function everyRole(g,opts={}) {
  for (const role of quoteAccept.roles)
    g.grant({edge_id:`e:${role.name}`,actor:actors[role.name],permission:role.authority,subject,...opts});
}
const has = (g,actor,permission,subj='s',options={}) => g.hasAuthority(actor,permission,subj,options);
const basic = (g,edge_id='e1',options={}) => g.grant({edge_id,actor:'a',permission:'p',subject:'s',...options});

test('conformant contract is refused without grants', () => withGraph(g => {
  const lacks = g.roleLacks(quoteAccept,actors,{subject});
  expect(lacks).toHaveLength(4); expect(new Set(lacks.map(l => l.role))).toEqual(new Set(quoteAccept.roles.map(r => r.name)));
}));
test('granting every role authorizes enactment', () => withGraph(g => {
  everyRole(g); expect(g.authorized(quoteAccept,actors,{subject})).toBe(true);
}));
test('refusal names role and grant', () => withGraph(g => {
  everyRole(g); g.revoke('e:deliverer'); const lacks = g.roleLacks(quoteAccept,actors,{subject});
  expect(lacks).toHaveLength(1); expect(lacks[0].role).toBe('deliverer');
  expect(String(lacks[0])).toContain('deliver:capability');
}));
test('refused enactment opens no obligations', () => withGraph(g => {
  const c = new CommitmentGraph();
  try {
    expect(() => g.enactAuthorized(c,quoteAccept,{enactment_id:'ex1',actors,subject})).toThrow(AuthorityRefused);
    expect(() => g.enactAuthorized(c,quoteAccept,{enactment_id:'ex1',actors,subject})).toThrow('deliver:capability');
    expect(c.status('ex1:delivery')).toBeNull();
  } finally { c.close(); }
}));
test('authorized enactment opens named obligations', () => withGraph(g => {
  everyRole(g); const c = new CommitmentGraph();
  try { expect(g.enactAuthorized(c,quoteAccept,{enactment_id:'ex1',actors,subject}))
    .toEqual(['ex1:delivery','ex1:payment']); }
  finally { c.close(); }
}));
test('authority never crosses subjects', () => withGraph(g => {
  everyRole(g); expect(g.authorized(quoteAccept,actors,{subject})).toBe(true);
  expect(g.authorized(quoteAccept,actors,{subject:'capability:owner-concentration'})).toBe(false);
}));
test('revocation closes grant immediately and reports second revoke', () => withGraph(g => {
  basic(g); expect(has(g,'a','p')).toBe(true); expect(g.revoke('e1')).toBe(true);
  expect(has(g,'a','p')).toBe(false); expect(g.revoke('e1')).toBe(false);
}));
test('window excludes moments outside it', () => withGraph(g => {
  basic(g,'e1',{effective_from:'2026-01-01T00:00:00Z',expires_at:'2026-06-30T00:00:00Z'});
  expect(has(g,'a','p','s',{at_iso:'2026-03-01T00:00:00Z'})).toBe(true);
  expect(has(g,'a','p','s',{at_iso:'2025-12-01T00:00:00Z'})).toBe(false);
  expect(has(g,'a','p','s',{at_iso:'2026-09-01T00:00:00Z'})).toBe(false);
}));
test('purpose-bound grant cannot generalize', () => withGraph(g => {
  basic(g,'e1',{purpose:'underwriting'});
  expect(has(g,'a','p','s',{purpose:'underwriting'})).toBe(true);
  expect(has(g,'a','p','s',{purpose:'marketing'})).toBe(false);
  g.grant({edge_id:'e2',actor:'b',permission:'p',subject:'s'});
  expect(has(g,'b','p','s',{purpose:'marketing'})).toBe(true);
}));
test('role without declared authority is ungated', () => withGraph(g => {
  expect(g.roleLacks({roles:[{name:'reader',requires:['subject'],produces:['view']}]},
    {reader:'anyone'},{subject})).toEqual([]);
}));
test('unassigned roles are reported', () => withGraph(g => {
  everyRole(g); const lacks = g.roleLacks(quoteAccept,{buyer:'agent:acme'},{subject});
  expect(new Set(lacks.map(l => l.role))).toEqual(new Set(['seller','settlement','deliverer']));
  expect(lacks.every(l => l.actor === '<unassigned>')).toBe(true);
}));
test('native owner matches an independent SQL rule on every case', () => withGraph(g => {
  basic(g,'plain');
  g.grant({edge_id:'win',actor:'w',permission:'p',subject:'s',effective_from:'2026-01-01T00:00:00Z',expires_at:'2026-06-30T00:00:00Z'});
  g.grant({edge_id:'pur',actor:'pu',permission:'p',subject:'s',purpose:'underwriting'});
  g.grant({edge_id:'gen',actor:'gen',permission:'p',subject:'s'});
  g.grant({edge_id:'gone',actor:'rev',permission:'p',subject:'s'}); g.revoke('gone');
  const cases = [
    ['a','p','s','',''],['a','p','other','',''],['nobody','p','s','',''],['a','other','s','',''],
    ['w','p','s','2026-03-01T00:00:00Z',''],['w','p','s','2025-12-01T00:00:00Z',''],
    ['w','p','s','2026-09-01T00:00:00Z',''],['pu','p','s','','underwriting'],
    ['pu','p','s','','marketing'],['gen','p','s','','marketing'],['rev','p','s','',''],
  ];
  const db = new Database(g.dbPath,{readonly:true});
  try { for (const [actor,permission,subj,at_iso,purpose] of cases) {
    const rows = db.query('SELECT purpose,effective_from,expires_at FROM authority_edges WHERE actor=? AND permission=? AND subject=? AND revoked=0')
      .all(actor,permission,subj);
    const at = at_iso || new Date().toISOString();
    const expected = rows.some(row => (!purpose || !row.purpose || row.purpose === purpose) &&
      (!row.effective_from || at >= row.effective_from) && (!row.expires_at || at <= row.expires_at));
    expect(g.hasAuthority(actor,permission,subj,{at_iso,purpose})).toBe(expected);
  }} finally { db.close(); }
}));
test('parity witness detects revoked-edge drift', () => withGraph(g => {
  basic(g); g.revoke('e1');
  expect(has(g,'a','p')).toBe(false);
  const db = new Database(g.dbPath,{readonly:true});
  try { expect(db.query('SELECT 1 FROM authority_edges WHERE actor=? AND permission=? AND subject=?')
    .get('a','p','s')).not.toBeNull(); } finally { db.close(); }
}));
test('non-delegable grant refuses derivation', () => withGraph(g => {
  basic(g,'root',{actor:'parent'});
  expect(() => g.delegateAuthority({parent_edge_id:'root',to_actor:'child',new_edge_id:'c1'}))
    .toThrow('not delegable');
  expect(has(g,'child','p')).toBe(false);
}));
test('delegable grant yields child', () => withGraph(g => {
  basic(g,'root',{actor:'parent',delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'child',new_edge_id:'c1'});
  expect(has(g,'child','p')).toBe(true); expect(g.delegationChain('c1')).toEqual(['root','c1']);
}));
test('revoked parent grants no delegation', () => withGraph(g => {
  basic(g,'root',{delegable:1}); g.revoke('root');
  expect(() => g.delegateAuthority({parent_edge_id:'root',to_actor:'child',new_edge_id:'c1'})).toThrow('revoked');
}));
test('child may narrow purpose but never widen', () => withGraph(g => {
  basic(g,'general',{actor:'parent',delegable:1});
  g.delegateAuthority({parent_edge_id:'general',to_actor:'child',new_edge_id:'c1',purpose:'underwriting'});
  expect(has(g,'child','p','s',{purpose:'underwriting'})).toBe(true);
  expect(has(g,'child','p','s',{purpose:'marketing'})).toBe(false);
  basic(g,'bound',{actor:'parent2',purpose:'underwriting',delegable:1});
  expect(() => g.delegateAuthority({parent_edge_id:'bound',to_actor:'child2',new_edge_id:'c2',purpose:'marketing'}))
    .toThrow('narrows and never widens');
}));
test('child never outlives parent', () => withGraph(g => {
  basic(g,'root',{actor:'parent',expires_at:'2026-06-30T00:00:00Z',delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'child',new_edge_id:'c1',expires_at:'2026-03-01T00:00:00Z'});
  expect(has(g,'child','p','s',{at_iso:'2026-02-01T00:00:00Z'})).toBe(true);
  expect(has(g,'child','p','s',{at_iso:'2026-05-01T00:00:00Z'})).toBe(false);
  expect(() => g.delegateAuthority({parent_edge_id:'root',to_actor:'child2',new_edge_id:'c2',expires_at:'2027-01-01T00:00:00Z'}))
    .toThrow('never outlives');
}));
test('delegation is one hop absent explicit regrant', () => withGraph(g => {
  basic(g,'root',{delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'child',new_edge_id:'c1'});
  expect(() => g.delegateAuthority({parent_edge_id:'c1',to_actor:'grandchild',new_edge_id:'c2'}))
    .toThrow(DelegationRefused);
}));
test('cascade closes every descendant', () => withGraph(g => {
  basic(g,'root',{actor:'parent',delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'a',new_edge_id:'c1'});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'b',new_edge_id:'c2'});
  expect(has(g,'a','p')).toBe(true); expect(has(g,'b','p')).toBe(true);
  expect(g.revokeCascade('root')).toEqual(['c1','c2','root']);
  expect(has(g,'a','p')).toBe(false); expect(has(g,'b','p')).toBe(false);
  expect(has(g,'parent','p')).toBe(false);
}));
test('plain revoke does not cascade', () => withGraph(g => {
  basic(g,'root',{actor:'parent',delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'a',new_edge_id:'c1'});
  g.revoke('root'); expect(has(g,'parent','p')).toBe(false); expect(has(g,'a','p')).toBe(true);
}));
test('descendants and chain are transitive', () => withGraph(g => {
  basic(g,'root',{actor:'p',permission:'x',delegable:1});
  g.delegateAuthority({parent_edge_id:'root',to_actor:'a',new_edge_id:'c1'});
  g.grant({edge_id:'c1',actor:'a',permission:'x',subject:'s',delegable:1,originating_authority:'root'});
  g.delegateAuthority({parent_edge_id:'c1',to_actor:'b',new_edge_id:'c1a'});
  expect(g.descendants('root')).toEqual(['c1','c1a']);
  expect(g.delegationChain('c1a')).toEqual(['root','c1','c1a']);
  expect(g.revokeCascade('root')).toEqual(['c1','c1a','root']);
}));
