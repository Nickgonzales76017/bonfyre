import { conformance, conformant, causalOrder, enact, outstandingAfter, closed,
  ProtocolError, quoteAccept } from './protocol-library.mjs';

export { ProtocolError, quoteAccept };

// Runtime builder for ad-hoc contracts; curated production contracts are authored
// in schemas/control/protocol-library.cue and loaded through protocol-library.mjs.
export class ProtocolContract {
  constructor(name,{keys=[],given=[],evidence_required=[]}={}) {
    this.name = name; this.keys = [...keys]; this.given = [...given];
    this.evidence_required = [...evidence_required]; this.roles = []; this.commitments = [];
  }
  role(name,{requires=[],produces=[],reads,authority=''}={}) {
    if (this.roles.some(role => role.name === name)) throw new ProtocolError(`role ${name} declared twice`);
    this.roles.push({name,requires:[...requires],produces:[...produces],
      ...(reads === undefined ? {} : {reads:[...reads]}),authority});
    return this;
  }
  commits(key,{committer,beneficiary,resource,discharged_by=''}={}) {
    if (!this.roles.some(role => role.name === committer) ||
        !this.roles.some(role => role.name === beneficiary))
      throw new ProtocolError(`${this.name} does not declare role ${committer} or ${beneficiary}`);
    this.commitments.push({key,committer,beneficiary,resource,discharged_by});
    return this;
  }
  livenessErrors() { return conformance(this).liveness; }
  safetyErrors() { return conformance(this).safety; }
  integrityErrors(policy,audience='public') { return conformance(this,policy,audience).integrity; }
  conformance(policy,audience='public') { return conformance(this,policy,audience); }
  conformant(policy,audience='public') { return conformant(this,policy,audience); }
  causalOrder() { return causalOrder(this); }
  enact(commitments,enactmentId,actors,at='') { return enact(this,commitments,enactmentId,actors,at); }
  outstandingAfter(commitments,enactmentId) { return outstandingAfter(this,commitments,enactmentId); }
  closed(commitments,enactmentId) { return closed(this,commitments,enactmentId); }
}
