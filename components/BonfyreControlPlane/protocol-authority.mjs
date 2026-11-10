import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, renameSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Database } from 'bun:sqlite';
import { enact } from './protocol-library.mjs';

const repo = resolve(import.meta.dir, '../..');
const source = resolve(import.meta.dir, 'native/authority_cli.c');
const core = resolve(repo, 'engine/core/src/authority.c');
const header = resolve(repo, 'engine/core/include/bf_authority.h');
const hash = createHash('sha256').update(readFileSync(source)).update(readFileSync(core))
  .update(readFileSync(header)).digest('hex').slice(0, 16);
const cli = process.env.BONFYRE_AUTHORITY_CLI || join(tmpdir(), `bonfyre-authority-${hash}`);
if (!existsSync(cli)) {
  const built = `${cli}.${process.pid}`;
  const result = Bun.spawnSync(['cc','-O2','-Wall','-Wextra','-Werror','-std=c11',
    '-Iengine/core/include',source,core,'-lsqlite3','-o',built], {cwd:repo});
  if (result.exitCode !== 0) throw new Error(`native AuthorityGraph build failed: ${result.stderr}`);
  renameSync(built,cli);
}

export class AuthorityRefused extends Error {}
export class DelegationRefused extends Error {}
export class AuthorityError extends Error {}

export class AuthorityGraph {
  constructor(path = null) {
    this.tempDir = path ? null : mkdtempSync(join(tmpdir(),'bonfyre-authority-'));
    this.dbPath = path || join(this.tempDir,'graph.db');
    this.hasAuthority('__schema_probe__','probe','probe');
  }
  close() { if (this.tempDir) rmSync(this.tempDir,{recursive:true,force:true}); this.tempDir = null; }
  call(operation,...args) {
    const result = Bun.spawnSync([cli,this.dbPath,operation,...args.map(v => String(v ?? ''))]);
    const value = Number(result.stdout.toString().trim());
    if (!Number.isInteger(value) || value < 0 || result.exitCode !== 0)
      throw new AuthorityError(`native AuthorityGraph ${operation} failed: ${result.stderr}`);
    return value;
  }
  read(sql, ...args) {
    const db = new Database(this.dbPath,{readonly:true});
    try { return db.query(sql).all(...args); } finally { db.close(); }
  }
  grant({edge_id,actor,permission,subject,purpose='',scope='',effective_from='',
    expires_at='',delegable=0,evidence='',originating_authority='',created_at=''}) {
    this.call('grant',edge_id,actor,permission,subject,purpose,scope,effective_from,
      expires_at,delegable ? 1 : 0,evidence,originating_authority,created_at);
  }
  revoke(edgeId) { return this.call('revoke',edgeId) === 1; }
  hasAuthority(actor,permission,subject,{at_iso='',purpose=''}={}) {
    return this.call('has',actor,permission,subject,at_iso,purpose) === 1;
  }
  edge(edgeId) {
    return this.read('SELECT * FROM authority_edges WHERE edge_id=?',edgeId)[0] ?? null;
  }
  delegateAuthority({parent_edge_id,to_actor,new_edge_id,purpose,expires_at}) {
    const parent = this.edge(parent_edge_id);
    if (!parent) throw new DelegationRefused(`unknown parent edge ${parent_edge_id}`);
    if (parent.revoked) throw new DelegationRefused(`edge ${parent_edge_id} is revoked`);
    if (!parent.delegable) throw new DelegationRefused(`edge ${parent_edge_id} is not delegable`);
    if (purpose !== undefined && parent.purpose && purpose !== parent.purpose)
      throw new DelegationRefused(`purpose narrows and never widens ${parent.purpose}`);
    if (expires_at !== undefined && parent.expires_at && expires_at > parent.expires_at)
      throw new DelegationRefused(`child never outlives parent ${parent.expires_at}`);
    try { this.call('delegate',parent_edge_id,to_actor,new_edge_id,purpose,expires_at); }
    catch (error) { throw new DelegationRefused(error.message); }
  }
  delegationChain(edgeId) {
    const chain = [edgeId], seen = new Set(chain);
    for (let n=0;n<64;n++) {
      const parent = this.edge(chain[0])?.originating_authority;
      if (!parent || seen.has(parent)) break;
      chain.unshift(parent); seen.add(parent);
    }
    return chain;
  }
  descendants(edgeId) {
    const found = new Set(), frontier = [edgeId];
    for (let n=0;n<64 && frontier.length;n++) {
      const next = this.read(`SELECT edge_id FROM authority_edges WHERE originating_authority IN (${frontier.map(() => '?').join(',')})`,...frontier)
        .map(row => row.edge_id).filter(id => id !== edgeId && !found.has(id));
      for (const id of next) found.add(id);
      frontier.splice(0,frontier.length,...next);
    }
    return [...found].sort();
  }
  revokeCascade(edgeId) {
    const ids = [edgeId,...this.descendants(edgeId)];
    const live = ids.filter(id => this.edge(id)?.revoked === 0);
    this.call('cascade',edgeId);
    return live.sort();
  }
  roleLacks(contract,actors,{subject,at_iso,purpose}={}) {
    return [...(contract.roles || [])].filter(role => role.authority).sort((a,b) => a.name.localeCompare(b.name))
      .flatMap(role => {
        const actor = actors[role.name] ?? '<unassigned>';
        if (actor !== '<unassigned>' && this.hasAuthority(actor,role.authority,subject,{at_iso,purpose})) return [];
        return [{role:role.name,actor,permission:role.authority,subject,purpose:purpose ?? '',
          toString() { return `${this.role} (${this.actor}) lacks '${this.permission}' over '${this.subject}'${this.purpose ? ` for purpose '${this.purpose}'` : ''}`; }}];
      });
  }
  authorized(contract,actors,options) { return this.roleLacks(contract,actors,options).length === 0; }
  enactAuthorized(commitments,contract,{enactment_id,actors,subject,at_iso,purpose,at=''}) {
    const lacks = this.roleLacks(contract,actors,{subject,at_iso,purpose});
    if (lacks.length) throw new AuthorityRefused(`enactment '${enactment_id}' of '${contract.name}' refused: ${lacks.join('; ')}`);
    return enact(contract,commitments,enactment_id,actors,at);
  }
}
