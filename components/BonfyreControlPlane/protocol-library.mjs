import { resolve } from 'node:path';
import { TERMINAL } from './commitment-graph.mjs';

const repo = resolve(import.meta.dir, '../..');
function authored(expression) {
  const result = Bun.spawnSync(['cue', 'export', 'schemas/control/protocol-library.cue',
    'schemas/control/protocol-contract.cue', '-e', expression, '--out', 'json'], { cwd: repo });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return JSON.parse(result.stdout.toString());
}
export const library = Object.freeze(authored('protocolLibrary'));
export const quoteAccept = Object.freeze(authored('quoteAcceptContract'));

export class UnknownProtocol extends Error {}
export class ProtocolError extends Error {}

export function get(family, name) {
  const families = [...new Set(library.map(c => c.family))].sort();
  if (!families.includes(family)) throw new UnknownProtocol(`unknown family ${family}; known: ${families}`);
  const contract = library.find(c => c.family === family && c.name === name);
  if (!contract) throw new UnknownProtocol(`unknown protocol ${name} in ${family}; have: ${library.filter(c => c.family === family).map(c => c.name).sort()}`);
  return contract;
}
export function allProtocols() { return library; }
export function coverage() {
  return Object.fromEntries([...new Set(library.map(c => c.family))].sort().map(f => [f, library.filter(c => c.family === f).length]));
}

export function conformance(contract, policy = null, audience = 'public') {
  const roles = contract.roles || [];
  const available = new Set([...(contract.given || []), ...(contract.keys || [])]);
  const remaining = new Map(roles.map(r => [r.name, r]));
  let progressed;
  do {
    progressed = false;
    for (const [name, role] of remaining) {
      if (!(role.requires || []).every(value => available.has(value))) continue;
      for (const value of role.produces || []) available.add(value);
      remaining.delete(name);
      progressed = true;
    }
  } while (progressed);
  const liveness = Object.fromEntries([...remaining].map(([name, role]) =>
    [name, (role.requires || []).filter(value => !available.has(value)).sort()]));
  const producers = new Map();
  for (const role of roles) for (const value of role.produces || []) {
    if (!producers.has(value)) producers.set(value, []);
    producers.get(value).push(role.name);
  }
  const safety = Object.fromEntries([...producers].filter(([, names]) => names.length > 1)
    .map(([value, names]) => [value, names.sort()]));
  const integrity = {};
  for (const role of roles) {
    if (!(role.produces || []).length) continue;
    const entitled = new Set(role.reads ?? role.requires ?? []);
    const unentitled = (role.requires || []).filter(value => !entitled.has(value) ||
      (policy && !policy.fieldDecision(value, audience)[0]));
    if (unentitled.length) integrity[role.name] = unentitled.sort();
  }
  return { liveness, safety, integrity };
}
export function conformant(contract, policy, audience) {
  return Object.values(conformance(contract, policy, audience)).every(section => Object.keys(section).length === 0);
}
export function causalOrder(contract) {
  if (!conformant(contract)) throw new ProtocolError(`${contract.name} waits forever: ${JSON.stringify(conformance(contract).liveness)}`);
  const available = new Set([...(contract.given || []), ...(contract.keys || [])]);
  const remaining = new Map(contract.roles.map(r => [r.name, r]));
  const waves = [];
  while (remaining.size) {
    const wave = [...remaining].filter(([, role]) => (role.requires || []).every(p => available.has(p)))
      .map(([name]) => name).sort();
    if (!wave.length) throw new ProtocolError(`${contract.name} stalled`);
    waves.push(wave);
    for (const name of wave) {
      for (const value of remaining.get(name).produces || []) available.add(value);
      remaining.delete(name);
    }
  }
  return waves;
}

export function enact(contract, commitments, enactmentId, actors, at = '') {
  if (!conformant(contract)) throw new ProtocolError(`refusing to enact non-conformant contract ${contract.name}: ${JSON.stringify(conformance(contract))}`);
  const missing = (contract.roles || []).map(role => role.name).filter(name => !Object.hasOwn(actors,name));
  if (missing.length) throw new ProtocolError(`enactment ${enactmentId} has no actor for roles ${missing.sort()}`);
  const roles = new Map(contract.roles.map(role => [role.name,role]));
  return (contract.commitments || []).map(spec => {
    const id = `${enactmentId}:${spec.key}`;
    commitments.record({commitment_id:id,committer:actors[spec.committer],
      beneficiary:actors[spec.beneficiary],resource:spec.resource,
      conditions:spec.discharged_by || '',authority:roles.get(spec.committer)?.authority || '',
      enactment:enactmentId,at});
    return id;
  });
}
export function outstandingAfter(contract, commitments, enactmentId) {
  return (contract.commitments || []).map(spec => {
    const id = `${enactmentId}:${spec.key}`;
    return [id,commitments.status(id)];
  }).filter(([,state]) => state && !TERMINAL.has(state))
    .sort(([a],[b]) => a.localeCompare(b));
}
export function closed(contract, commitments, enactmentId) {
  return outstandingAfter(contract,commitments,enactmentId).length === 0;
}
