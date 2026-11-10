import { conformant, conformance } from './protocol-library.mjs';

export class ProgramError extends Error {}

export class SemanticProgram {
  constructor(contract, enactment) {
    if (!conformant(contract)) throw new ProgramError(`refusing to lower non-conformant contract ${contract.name}: ${JSON.stringify(conformance(contract))}`);
    if (!enactment) throw new ProgramError('a program is addressed per enactment; without one, two runs of a contract would share a closure');
    this.enactment = enactment;
    this.contract = contract.name;
    this.given = new Set(contract.given || []);
    this.steps = [...contract.roles].sort((a, b) => a.name.localeCompare(b.name)).map(role => ({
      role: role.name, reads: new Set(role.requires || []), writes: new Set(role.produces || []),
    }));
  }
  readClosure() { return new Set(this.steps.flatMap(step => [...step.reads])); }
  writeClosure() { return new Set(this.steps.flatMap(step => [...step.writes])); }
  producerOf(parameter) { return this.steps.find(step => step.writes.has(parameter))?.role ?? null; }
  readersOf(parameter) { return this.steps.filter(step => step.reads.has(parameter)).map(step => step.role).sort(); }
  affectedBy(parameter) {
    const dirty = new Set(), changed = new Set([parameter]);
    let grew;
    do {
      grew = false;
      for (const step of this.steps) {
        if (dirty.has(step.role) || ![...step.reads].some(read => changed.has(read))) continue;
        dirty.add(step.role);
        for (const written of step.writes) changed.add(written);
        grew = true;
      }
    } while (grew);
    return [...dirty].sort();
  }
  invalidates(parameter) { return this.affectedBy(parameter).length > 0; }
  unaffectedBy(parameter) {
    const affected = new Set(this.affectedBy(parameter));
    return this.steps.filter(step => !affected.has(step.role)).map(step => step.role).sort();
  }
}

export function lower(contract, enactment) { return new SemanticProgram(contract, enactment); }
export function affectedPrograms(programs, parameter) {
  return Object.fromEntries(programs.flatMap(program => {
    const roles = program.affectedBy(parameter);
    return roles.length ? [[program.enactment, roles]] : [];
  }));
}
export function factDelta(parameter, enactment = null) {
  if (!parameter) throw new ProgramError('a FactDelta names the address that changed');
  return { parameter, enactment, global_invalidation: false };
}
