#!/usr/bin/env bun
// Account-scoped preparation over the existing ProgramFabric compiler.
// WebAddress authenticates every call and supplies the admitted binding in argv.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, readSync, writeSync } from 'node:fs';
import { dirname, isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { homedir } from 'node:os';
import { encode, decode, maxPacket } from '../integrations/browser/workspace-msgpack.mjs';
import { compilePrograms, policy } from './compile-program-fabric.mjs';
import { declaredDemands, resolveCollection } from '../cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs';

const LIMIT = 128;
const here = dirname(fileURLToPath(import.meta.url));
const writeOperations = new Set(['prepare','change']);
const privateBoundary = 'This is a private prepared program. Its requirements remain with their native owners; preparation does not publish a page, perform work, connect a provider, grant authority or spend money.';
const operations = new Set(['catalog', 'read', 'review', 'prepare','change','change-receipt']);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Uint8Array);
const canonical = value => Array.isArray(value) ? value.map(canonical) : object(value)
  ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const digest = value => createHash('sha256').update(encode(canonical(value))).digest('hex');
const templateSlug = (value,cap=64) => value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0,cap).replace(/-$/g,'');

export class ProgramRefusal extends Error {
  constructor(code, message, exitCode = 3) { super(message); this.code = code; this.exitCode = exitCode; }
}
const refuse = (condition, code, message, exitCode) => { if (!condition) throw new ProgramRefusal(code, message, exitCode); };
const token = (value, max = 160) => typeof value === 'string' && value.length > 0 && value.length <= max && /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(value);
const sameBinding = (left,right) => object(left) && Object.keys(left).sort().join(',') === 'actor,scope,subject'
  && left.actor === right.actor && left.subject === right.subject && left.scope === right.scope;

export function admittedBinding({actor, subject, scope}) {
  refuse(typeof actor === 'string' && /^user:[1-9][0-9]{0,17}$/.test(actor), 'invalid_binding', 'A current account actor is required.');
  refuse(token(subject) && ['self','institution','household'].includes(scope), 'invalid_binding', 'A current workspace destination is required.');
  refuse(scope === 'self' ? subject === actor : subject.startsWith(scope + ':') && subject.length > scope.length + 1,
    'invalid_binding', 'The account and destination do not agree.');
  return {actor,subject,scope};
}
export function derivedSite(binding) { return 'pws-' + digest([1, policy.namespace, binding.subject, binding.scope]).slice(0,32); }
function rowsBounded(db,sql,args,limit=LIMIT) {
  const rows=db.query(sql).all(...args,limit+1);
  refuse(rows.length<=limit,'capacity_exceeded','This program listing exceeds its declared bound. Refine the selection before continuing.',6);
  return rows;
}
function listInstances(db,binding) {
  return rowsBounded(db,`SELECT i.instance_id,i.template_id,i.site_id,i.status,i.opened_at,
    COALESCE(t.name,i.template_id) AS name FROM program_instance i
    LEFT JOIN program_template t ON t.namespace=i.namespace AND t.template_id=i.template_id
    WHERE i.namespace=? AND i.owner_subject=? ORDER BY i.opened_at DESC,i.instance_id LIMIT ?`,[policy.namespace,binding.subject]);
}
function arrayField(row,key,limit=64) {
  let value;try { value=JSON.parse(row[key]||'[]'); } catch { throw new ProgramRefusal('invalid_template','A program template contains invalid declared fields.',6); }
  refuse(Array.isArray(value)&&value.length<=limit,'invalid_template','A program template exceeds its declared field bound.',6);
  return value;
}
function templateView(row) {
  refuse(token(row.template_id,160)&&typeof row.name==='string'&&row.name.length<=500,'invalid_template','The program template has no usable identity.',6);
  const value={template_id:row.template_id,name:row.name,archetype:row.archetype||'',description:row.description||'',
    semantic_slots:arrayField(row,'semantic_slots'),capability_families:arrayField(row,'capability_families',16),
    owner_bindings:arrayField(row,'owner_bindings'),funding_candidates:arrayField(row,'funding_candidates',32),
    subject_kinds:arrayField(row,'subject_kinds'),surface_policy:row.surface_policy||'',page_policy:row.page_policy||''};
  refuse(typeof value.description==='string'&&value.description.length<=16384,'invalid_template','The program description exceeds its declared bound.',6);
  for(const key of ['semantic_slots','capability_families','funding_candidates','subject_kinds'])
    refuse(value[key].every(x=>typeof x==='string'&&x.length>0&&x.length<=500),'invalid_template','The program declaration contains an invalid item.',6);
  // Encode the full published row for the review identity, including fields the
  // frontdoor does not render. Neither excerpts nor names identify a template.
  value.digest=digest([1,policy.namespace,policy,row]);
  return value;
}
function exactTemplate(db,id) {
  refuse(token(id,160),'invalid_template_id','Choose a published program template.');
  const rows=db.query("SELECT * FROM program_template WHERE namespace=? AND template_id=? AND status='published'").all(policy.namespace,id);
  refuse(rows.length===1,'template_unavailable','That published program template is unavailable.',4);
  return {row:rows[0],view:templateView(rows[0])};
}
function tableExists(db,name) { return !!db.query("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?").get(name); }
function receiptTableExists(db) { return tableExists(db,'_program_workspace_preparation'); }
function receipt(db,id) { return receiptTableExists(db) ? db.query('SELECT * FROM _program_workspace_preparation WHERE namespace=? AND instance_id=?').get(policy.namespace,id) : null; }
function requireNewDeclarations(db,view,id) {
  const slots=view.semantic_slots.map(role=>id+'-'+templateSlug(role,24));
  const funding=view.funding_candidates.map((_,index)=>id+'-funding-'+(index+1));
  const planned=[['semantic_slot','slot_id',slots],['application_binding','binding_id',slots.map(slot=>slot+'-app')],
    ['program_binding','binding_id',slots],['source_collection','collection_id',[id+'-sources']],
    ['program_requirement','requirement_id',[...view.capability_families.map(family=>id+'-needs-'+templateSlug(family,24)),...funding.map(key=>key+'-scope')]],
    ['funding_work_package','package_id',funding]];
  for(const [table,key,identities] of planned) if(identities.length) {
    const found=db.query(`SELECT 1 AS present FROM ${table} WHERE namespace=? AND ${key} IN (${identities.map(()=>'?').join(',')}) LIMIT 1`).get(policy.namespace,...identities);
    refuse(!found,'identity_collision','A declaration already occupies part of this program identity. Its existing owner state was preserved.',4);
  }
  for(const table of ['program_binding','program_requirement','funding_work_package'])
    refuse(!db.query(`SELECT 1 AS present FROM ${table} WHERE namespace=? AND instance_id=? LIMIT 1`).get(policy.namespace,id),
      'identity_collision','Existing declarations name this missing program. Reconcile that owner state before preparing it.',4);
}
function reviewAt(db,binding,id) {
  const {view}=exactTemplate(db,id),site=derivedSite(binding),slug=templateSlug(id);
  const subjectKind=binding.scope==='self'?'person':binding.scope;
  refuse(view.subject_kinds.includes(subjectKind),'subject_kind_unavailable','This template does not declare use by this kind of workspace.',4);
  refuse(!!slug,'invalid_template_id','The program template has no usable local identity.',4);
  const instance=site+'-'+slug;
  const prior=db.query('SELECT owner_subject,site_id,template_id,status FROM program_instance WHERE namespace=? AND instance_id=?').get(policy.namespace,instance);
  const retained=receipt(db,instance);
  if(prior) {
    refuse(prior.owner_subject===binding.subject&&prior.site_id===site&&prior.template_id===id,'identity_collision','This program identity belongs to different work. Nothing was transferred.',4);
    refuse(retained&&retained.subject===binding.subject&&retained.scope===binding.scope&&retained.template_id===id,
      'unreviewed_instance','This existing program needs its original owner to reconcile its preparation.',4);
    refuse(retained.template_digest===view.digest,'template_changed','This program was prepared from an earlier template. Its existing work is preserved; review it before updating.',4);
    refuse(prior.status==='draft','instance_changed','This program has changed lifecycle state. Open its existing work before preparing it again.',4);
  } else {
    refuse(!retained,'incomplete_preparation','A retained preparation no longer agrees with its program. Reconcile that work before continuing.',4);
    requireNewDeclarations(db,view,instance);
  }
  return {template:view,instance_id:instance,review_digest:digest([1,policy.namespace,binding,site,view.digest]),existing:!!prior};
}
function storedObject(value) {
  let result;try { result=JSON.parse(value||'{}'); } catch { throw new ProgramRefusal('invalid_configuration','The stored program configuration could not be read.',6); }
  refuse(object(result),'invalid_configuration','The stored program configuration is not an object.',6);
  return result;
}
function sourceCollection(db,binding,id) {
  const rows=db.query('SELECT * FROM source_collection WHERE namespace=? AND collection_id=?').all(policy.namespace,id+'-sources');
  refuse(rows.length<=1,'invalid_configuration','The program source collection has conflicting identities.',6);
  if(!rows.length)return null;
  const row=rows[0];
  refuse(row.subject_address===binding.subject,'source_owner_changed','The program source collection belongs to another workspace.',4);
  return {...row,source_selector:storedObject(row.source_selector),material_kind:arrayField(row,'material_kind',64)};
}
function linkRows(db,binding,id) {
  if(!tableExists(db,'_program_workspace_link'))return [];
  return rowsBounded(db,`SELECT link_id,target_kind,target_id,label,workspace_binding,record_key,record_revision,
    release,record_digest,linked_by,linked_at FROM _program_workspace_link
    WHERE namespace=? AND subject=? AND scope=? AND instance_id=? ORDER BY target_kind,target_id,link_id LIMIT ?`,[policy.namespace,binding.subject,binding.scope,id])
    .map(row=>({...row,href:'/apps/'+row.workspace_binding+'/_views?'+new URLSearchParams({key:row.record_key,revision:String(row.record_revision),release:row.release}).toString()}));
}
function readInstance(db,binding,id) {
  refuse(token(id,200),'invalid_instance_id','Choose a program from this workspace.');
  const row=db.query('SELECT * FROM program_instance WHERE namespace=? AND owner_subject=? AND instance_id=?').get(policy.namespace,binding.subject,id);
  refuse(!!row,'instance_unavailable','That program is unavailable in this workspace.',4);
  const args=[policy.namespace,id];
  const bindingRows=rowsBounded(db,'SELECT * FROM program_binding WHERE namespace=? AND instance_id=? ORDER BY binding_id LIMIT ?',args);
  const requirementRows=rowsBounded(db,'SELECT * FROM program_requirement WHERE namespace=? AND instance_id=? ORDER BY requirement_id LIMIT ?',args);
  const fundingRows=rowsBounded(db,'SELECT * FROM funding_work_package WHERE namespace=? AND instance_id=? ORDER BY package_id LIMIT ?',args,32);
  const source=sourceCollection(db,binding,id),links=linkRows(db,binding,id);
  const template=db.query('SELECT * FROM program_template WHERE namespace=? AND template_id=?').get(policy.namespace,row.template_id);
  let demands=[];
  if(source) {
    let inherited=null;try { inherited=template?.address_demands?JSON.parse(template.address_demands):null; } catch { throw new ProgramRefusal('invalid_configuration','The source demands could not be read.',6); }
    try { demands=declaredDemands(source.source_selector,inherited); } catch { throw new ProgramRefusal('invalid_configuration','The program source demands exceed their declared contract.',6); }
  }
  // Full native rows, including acceptance and logical timestamps, identify the
  // editable snapshot. Operation receipts never contribute to this revision.
  const revision=digest([2,policy.namespace,binding.subject,binding.scope,row,bindingRows,requirementRows,fundingRows,source,links,template]);
  const instance={};
  for(const field of ['instance_id','template_id','owner_subject','site_id','archetype','opened_at','status','cannot_infer'])instance[field]=row[field]??'';
  instance.bindings=bindingRows.map(value=>Object.fromEntries(['binding_id','slot_id','program_role','lowered_by','continuity_key','status'].map(field=>[field,value[field]??''])));
  instance.requirements=requirementRows.map(value=>Object.fromEntries(['requirement_id','kind','statement','owner','state','status','deadline','evidence_ref','satisfied_by','cannot_infer'].map(field=>[field,value[field]??''])));
  instance.funding=fundingRows.map(value=>({...Object.fromEntries(['package_id','funding_program','distinctness_rule','status','cannot_infer'].map(field=>[field,value[field]??''])),scope:arrayField(value,'scope',256),cost_codes:arrayField(value,'cost_codes',256)}));
  instance.source_collection=source?Object.fromEntries(['collection_id','subject_address','source_selector','material_kind','refresh_policy','status','updated_at'].map(field=>[field,source[field]??''])):null;
  instance.source_demands=demands;
  instance.work_links=links;
  instance.revision=revision;
  return instance;
}
function mutableInstance(db,binding,id) {
  const instance=readInstance(db,binding,id),retained=receipt(db,id);
  refuse(retained&&retained.subject===binding.subject&&retained.scope===binding.scope&&retained.template_id===instance.template_id&&instance.site_id===derivedSite(binding),
    'unreviewed_instance','This program needs its original preparation before it can be changed here.',4);
  refuse(instance.status==='draft','instance_changed','Open this program with its lifecycle owner before changing its configuration.',4);
  return instance;
}
export function readProgramSourceContext(db,binding,id,{write=false}={}) {
  const admitted=admittedBinding(binding),instance=write?mutableInstance(db,admitted,id):readInstance(db,admitted,id),retained=receipt(db,id);
  refuse(retained&&retained.subject===admitted.subject&&retained.scope===admitted.scope&&retained.template_id===instance.template_id&&instance.site_id===derivedSite(admitted),
    'unreviewed_instance','This program needs its original preparation before opening its source work.',4);
  refuse(!!instance.source_collection,'source_collection_unavailable','The program source collection is unavailable.',4);
  if(write) {
    const template=exactTemplate(db,instance.template_id);
    refuse(template.view.digest===retained.template_digest,'template_changed','The template changed. Reconcile its source demands before creating source work.',4);
  }
  return {instance,source_collection:instance.source_collection,source_demands:instance.source_demands};
}
function exactFields(value,fields,code='invalid_change') {
  refuse(object(value)&&Object.keys(value).sort().join(',')===[...fields].sort().join(','),code,'The program change contains unsupported or missing fields.');
}
function boundedText(value,max,empty=false) {
  return typeof value==='string'&&value.length<=max&&(empty||value.trim().length>0)&&!/[\u0000-\u001f\u007f]/.test(value);
}
function dateValue(value) {
  return value===''||typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value+'T00:00:00Z'))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;
}
const normalized=value=>value.normalize('NFKC').trim().replace(/\s+/g,' ').toLocaleLowerCase('en-US');
function declaredList(values,max) {
  refuse(Array.isArray(values)&&values.length<=32&&values.every(value=>boundedText(value,max)&&value===value.trim()),'invalid_change','Use up to 32 nonempty entries without leading or trailing spaces.');
  const keys=values.map(normalized);
  refuse(new Set(keys).size===keys.length,'duplicate_declaration','Each declared scope and cost code must be distinct.');
  return values;
}
function changeContract(value) {
  refuse(object(value),'invalid_change','Choose a supported program change.');
  if(value.kind==='requirement-plan') {
    exactFields(value,['kind','requirement_id','deadline']);
    refuse(token(value.requirement_id,240)&&dateValue(value.deadline),'invalid_change','Choose a requirement and a valid date, or clear the date.');
  } else if(value.kind==='funding-plan') {
    exactFields(value,['kind','package_id','scope','cost_codes']);
    refuse(token(value.package_id,240),'invalid_change','Choose a funding work package.');
    declaredList(value.scope,400);declaredList(value.cost_codes,120);
  } else if(value.kind==='sources-refresh') exactFields(value,['kind']);
  else if(value.kind==='work-link') {
    exactFields(value,['kind','target_kind','target_id','workspace_binding','record_key','record_revision','label']);
    refuse(['program','binding','requirement'].includes(value.target_kind)&&token(value.target_id,240)&&/^ws-[a-f0-9]{40}$/.test(value.workspace_binding)
      &&boundedText(value.record_key,511)&&Number.isSafeInteger(value.record_revision)&&value.record_revision>0&&value.record_revision<2147483647&&boundedText(value.label,200),
      'invalid_change','Choose an exact saved record from an installed workspace.');
  } else if(value.kind==='work-unlink') {
    exactFields(value,['kind','link_id']);refuse(/^pwl-[a-f0-9]{40}$/.test(value.link_id),'invalid_change','Choose an attached work reference.');
  } else throw new ProgramRefusal('invalid_change','Choose a supported program change.');
  return value;
}
function changeTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS _program_workspace_change(
    namespace TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,instance_id TEXT NOT NULL,request_id TEXT NOT NULL,
    actor TEXT NOT NULL,request_digest TEXT NOT NULL,receipt BLOB NOT NULL,
    PRIMARY KEY(namespace,subject,scope,instance_id,request_id))`);
}
function changeReceipt(db,binding,id,requestId) {
  return tableExists(db,'_program_workspace_change')?db.query(`SELECT actor,request_digest,receipt FROM _program_workspace_change
    WHERE namespace=? AND subject=? AND scope=? AND instance_id=? AND request_id=?`).get(policy.namespace,binding.subject,binding.scope,id,requestId):null;
}
function decodedReceipt(row,binding) {
  refuse(row.actor===binding.actor,'request_unavailable','That change receipt belongs to another account actor.',4);
  let value;try { value=decode(new Uint8Array(row.receipt)); } catch { throw new ProgramRefusal('invalid_receipt','The program change receipt could not be read.',6); }
  refuse(object(value),'invalid_receipt','The program change receipt is invalid.',6);return value;
}
function nativeCmsRun(argv) {
  const result=spawnSync(argv[0],argv.slice(1),{timeout:5000,maxBuffer:maxPacket+1});
  return {exitCode:result.status,stdout:result.stdout??new Uint8Array(),stderr:result.stderr??new Uint8Array()};
}
function verifiedWorkspaceRecord(binding,change,{dbPath,cmsBinary,cmsRunFn=nativeCmsRun,workspacePackages}) {
  refuse(typeof cmsBinary==='string'&&isAbsolute(cmsBinary)&&existsSync(cmsBinary),'owner_unavailable','The native saved-work owner is unavailable.',6);
  const called=cmsRunFn([cmsBinary,'pack-workspace','--operation','continuation','--binding',change.workspace_binding,
    '--subject',binding.subject,'--scope',binding.scope,'--actor',binding.actor,'--prefix',change.record_key,'--db',dbPath]);
  refuse(called.exitCode===0,'work_unavailable','The native workspace could not reopen this saved record. Refresh its editor before attaching it.',4);
  let packet;try { packet=decode(new Uint8Array(called.stdout)); } catch { throw new ProgramRefusal('invalid_work_receipt','The workspace returned an invalid record receipt.',6); }
  refuse(Array.isArray(packet)&&packet.length===4&&packet[0]===1,'invalid_work_receipt','The workspace returned an incomplete record receipt.',6);
  const [version,context,row,candidates]=packet;
  refuse(Array.isArray(context)&&context.length===9&&context[0]===1&&context[1]===change.workspace_binding&&context[2]===binding.subject&&context[3]===binding.scope
    &&typeof context[4]==='string'&&/^[a-f0-9]{64}$/.test(context[5])&&typeof context[6]==='string'&&typeof context[7]==='string'&&typeof context[8]==='string',
    'work_binding_changed','The saved record belongs to a different workspace destination.',4);
  refuse(Array.isArray(row)&&row.length===3&&row[0]===change.record_key&&row[1]===change.record_revision&&row[2] instanceof Uint8Array,
    'work_revision_changed','The saved record changed. Read its current version before attaching it.',4);
  refuse(Array.isArray(candidates)&&candidates.length<=64,'invalid_work_receipt','The workspace returned an incomplete record projection.',6);
  const exactSource=candidates.some(candidate=>Array.isArray(candidate)&&candidate.length===2&&Array.isArray(candidate[0])
    &&candidate[0].length===context.length&&candidate[0].every((value,index)=>value===context[index]));
  refuse(exactSource,'work_view_unavailable','The saved record has no current installed editor.',4);
  refuse(typeof workspacePackages==='string'&&isAbsolute(workspacePackages),'owner_unavailable','The retained workspace package root is unavailable.',6);
  const projected=spawnSync(process.execPath,['--no-env-file','--no-install',join(here,'program-work-reference.mjs'),'--root',workspacePackages],
    {input:encode([1,context[5],row[0],row[1],row[2]]),timeout:5000,maxBuffer:32768});
  refuse(projected.status===0,'work_view_unavailable','The saved record has no available verified editor view.',4);
  let projection;try {projection=decode(new Uint8Array(projected.stdout));}catch{throw new ProgramRefusal('invalid_work_projection','The editor view could not be verified.',6);}
  const recordDigest=createHash('sha256').update(row[2]).digest('hex'),view=projection?.[2];
  refuse(Array.isArray(projection)&&projection.length===3&&projection[0]===1&&projection[1]===true&&object(view)
    &&view.release===context[5]&&view.key===row[0]&&view.revision===row[1]&&view.record_digest===recordDigest
    &&Array.isArray(view.views)&&view.views.length>0&&view.views.length<=16&&view.views.every(value=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,63}$/.test(value)),
    'invalid_work_projection','The editor view changed while verifying this record.',6);
  return {release:context[5],record_namespace:context[8],record_digest:recordDigest,views:view.views};
}
function updateConfiguration(db,binding,instance,change,options) {
  const stamp=new Date().toISOString();
  if(change.kind==='requirement-plan') {
    const requirement=instance.requirements.find(row=>row.requirement_id===change.requirement_id);
    refuse(!!requirement,'requirement_unavailable','Choose a requirement from this program.',4);
    if(requirement.deadline!==change.deadline)db.query('UPDATE program_requirement SET deadline=?,updated_at=? WHERE namespace=? AND instance_id=? AND requirement_id=?')
      .run(change.deadline,stamp,policy.namespace,instance.instance_id,change.requirement_id);
  } else if(change.kind==='funding-plan') {
    const target=instance.funding.find(row=>row.package_id===change.package_id);
    refuse(!!target,'package_unavailable','Choose a funding work package from this program.',4);
    for(const other of instance.funding)if(other.package_id!==change.package_id)for(const field of ['scope','cost_codes']) {
      const seen=new Set(other[field].map(value=>{refuse(typeof value==='string','invalid_configuration','A retained funding declaration could not be compared.',6);return normalized(value);}));
      refuse(!change[field].some(value=>seen.has(normalized(value))),'funding_overlap','This scope or cost code already belongs to another work package in this program.',4);
    }
    if(digest([target.scope,target.cost_codes])!==digest([change.scope,change.cost_codes]))db.query('UPDATE funding_work_package SET scope=?,cost_codes=?,updated_at=? WHERE namespace=? AND instance_id=? AND package_id=?')
      .run(JSON.stringify(change.scope),JSON.stringify(change.cost_codes),stamp,policy.namespace,instance.instance_id,change.package_id);
  } else if(change.kind==='sources-refresh') {
    const collection=instance.source_collection;
    refuse(!!collection,'source_collection_unavailable','The program source collection is unavailable.',4);
    const template=exactTemplate(db,instance.template_id),retained=receipt(db,instance.instance_id);
    refuse(template.view.digest===retained.template_digest,'template_changed','The template changed. Reconcile its source demands before refreshing this program.',4);
    let inherited=null;try { inherited=template.row.address_demands?JSON.parse(template.row.address_demands):null; } catch { throw new ProgramRefusal('invalid_configuration','The source demands could not be read.',6); }
    const resolved=resolveCollection(collection.source_selector,inherited,{cms:db,namespace:policy.namespace,index:options.addressIndex,
      binary:join(here,'../cmd/BonfyreExternalEstate/bonfyre-external-estate'),runFn:options.addressRunFn});
    if(digest(resolved.selector)!==digest(collection.source_selector))db.query('UPDATE source_collection SET source_selector=?,updated_at=? WHERE namespace=? AND collection_id=? AND subject_address=?')
      .run(JSON.stringify(resolved.selector),stamp,policy.namespace,collection.collection_id,binding.subject);
    return {owner:'AddressPlane',owner_queries:resolved.local.owner_queries,requirements_satisfied:0,resolutions:resolved.local.resolutions.map(row=>({...row,coverage:row.coverage.map(value=>Object.fromEntries(Object.entries(value).filter(([,item])=>item!==undefined)))}))};
  } else if(change.kind==='work-link') {
    const target=change.target_kind==='program'?change.target_id===instance.instance_id:change.target_kind==='binding'
      ?instance.bindings.some(row=>row.binding_id===change.target_id):instance.requirements.some(row=>row.requirement_id===change.target_id);
    refuse(target,'work_target_unavailable','Choose a part or requirement from this program.',4);
    const admitted=verifiedWorkspaceRecord(binding,change,options);
    const linkId='pwl-'+digest([1,policy.namespace,binding.subject,binding.scope,instance.instance_id,change.target_kind,change.target_id,admitted.record_namespace,change.record_key]).slice(0,40);
    refuse(instance.work_links.some(row=>row.link_id===linkId)||instance.work_links.length<LIMIT,'capacity_exceeded','This program has reached its work reference limit.',6);
    db.exec(`CREATE TABLE IF NOT EXISTS _program_workspace_link(
      namespace TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,instance_id TEXT NOT NULL,link_id TEXT NOT NULL,
      target_kind TEXT NOT NULL,target_id TEXT NOT NULL,label TEXT NOT NULL,workspace_binding TEXT NOT NULL,
      record_key TEXT NOT NULL,record_revision INTEGER NOT NULL,release TEXT NOT NULL,record_digest TEXT NOT NULL,
      linked_by TEXT NOT NULL,linked_at TEXT NOT NULL,PRIMARY KEY(namespace,subject,scope,instance_id,link_id))`);
    db.query(`INSERT INTO _program_workspace_link VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(namespace,subject,scope,instance_id,link_id) DO UPDATE SET label=excluded.label,workspace_binding=excluded.workspace_binding,
      record_revision=excluded.record_revision,release=excluded.release,record_digest=excluded.record_digest,linked_by=excluded.linked_by,linked_at=excluded.linked_at`)
      .run(policy.namespace,binding.subject,binding.scope,instance.instance_id,linkId,change.target_kind,change.target_id,change.label,
        change.workspace_binding,change.record_key,change.record_revision,admitted.release,admitted.record_digest,binding.actor,stamp);
    return {link_id:linkId,views:admitted.views};
  } else if(change.kind==='work-unlink') {
    refuse(instance.work_links.some(row=>row.link_id===change.link_id),'work_link_unavailable','This work reference is no longer attached to the program.',4);
    db.query('DELETE FROM _program_workspace_link WHERE namespace=? AND subject=? AND scope=? AND instance_id=? AND link_id=?')
      .run(policy.namespace,binding.subject,binding.scope,instance.instance_id,change.link_id);
    return {link_id:change.link_id};
  }
  return {};
}
function changeAt(db,binding,request,options) {
  const instance=mutableInstance(db,binding,request.instance_id),requestDigest=digest([1,request]),prior=changeReceipt(db,binding,request.instance_id,request.request_id);
  if(prior) {
    refuse(prior.request_digest===requestDigest,'request_conflict','That change identity was already used for different work.',4);
    return {instance,receipt:decodedReceipt(prior,binding),replayed:true};
  }
  refuse(instance.revision===request.expected_revision,'revision_changed','This program changed in another view. Review its current work before saving.',4);
  const outcome=updateConfiguration(db,binding,instance,request.change,options),current=readInstance(db,binding,request.instance_id);
  const receiptValue={request_id:request.request_id,kind:request.change.kind,instance_id:request.instance_id,
    before_revision:instance.revision,after_revision:current.revision,committed_at:new Date().toISOString(),result:outcome};
  const receiptPacket=encode(receiptValue);
  changeTables(db);
  db.query('INSERT INTO _program_workspace_change VALUES(?,?,?,?,?,?,?,?)').run(policy.namespace,binding.subject,binding.scope,request.instance_id,request.request_id,binding.actor,requestDigest,receiptPacket);
  return {instance:current,receipt:receiptValue,replayed:false};
}

function requestFor(operation,body,binding) {
  if(operation==='catalog'||operation==='read') {
    refuse(body===undefined||body===null||body.byteLength===0,'unexpected_body','This program read does not accept a request body.');
    return {};
  }
  let packet;try { packet=body instanceof Uint8Array?decode(body):body; } catch { throw new ProgramRefusal('invalid_packet','The program request could not be read.'); }
  refuse(Array.isArray(packet)&&packet.length===2&&packet[0]===1&&object(packet[1]),'invalid_packet','Use the current program request format.');
  const request=packet[1];
  const fields=operation==='review'?['expectedBinding','template_id']:operation==='prepare'?['expectedBinding','review_digest','template_id']
    :operation==='change'?['expectedBinding','instance_id','request_id','expected_revision','change']:['expectedBinding','instance_id','request_id'];
  exactFields(request,fields,'invalid_fields');
  refuse(sameBinding(request.expectedBinding,binding),'binding_changed','The account destination changed. Review the program in the current workspace.',4);
  if(operation==='prepare')refuse(typeof request.review_digest==='string'&&/^[a-f0-9]{64}$/.test(request.review_digest),'invalid_review','Review this program before preparing it.');
  if(operation==='change'||operation==='change-receipt') {
    refuse(token(request.instance_id,200)&&token(request.request_id,80),'invalid_request','Choose a program and a bounded change identity.');
    if(operation==='change') {
      refuse(typeof request.expected_revision==='string'&&/^[a-f0-9]{64}$/.test(request.expected_revision),'invalid_revision','Read the current program before changing it.');
      changeContract(request.change);
    }
  }
  return request;
}

export function operate({operation,actor,subject,scope,db:dbPath,instance='',addressIndex=process.env.BONFYRE_ADDRESS_INDEX??join(homedir(),'.bonfyre/estate/estate.db'),cmsBinary=process.env.BONFYRE_PROGRAM_CMS_BINARY??process.env.BONFYRE_CMS_BINARY??join(here,'../cmd/BonfyreCMS/bonfyre-cms'),workspacePackages=process.env.BONFYRE_WORKSPACE_PACKAGES??join(process.env.HOME??'/var/lib/bonfyre','.bonfyre/workspace-packages')},body,{addressRunFn,cmsRunFn}={}) {
  refuse(operations.has(operation),'invalid_operation','Choose a supported program operation.');
  const binding=admittedBinding({actor,subject,scope}),request=requestFor(operation,body,binding);
  refuse(!instance||operation==='read','invalid_instance','An instance may be selected only when opening existing work.');
  refuse(typeof dbPath==='string'&&isAbsolute(dbPath)&&existsSync(dbPath),'owner_unavailable','The canonical program store is unavailable.',6);
  refuse(typeof addressIndex==='string'&&isAbsolute(addressIndex),'owner_unavailable','The local source owner is unavailable.',6);
  refuse(typeof cmsBinary==='string'&&isAbsolute(cmsBinary),'owner_unavailable','The native workspace owner is unavailable.',6);
  const db=new Database(dbPath,writeOperations.has(operation)?{readwrite:true,create:false}:{readonly:true,create:false});
  db.exec('PRAGMA busy_timeout=1500');
  try {
    const work=()=>{
      let result;
      if(operation==='catalog') {
        const templates=rowsBounded(db,"SELECT * FROM program_template WHERE namespace=? AND status='published' ORDER BY ordinal,name,template_id LIMIT ?",[policy.namespace]).map(templateView);
        result={templates,instances:listInstances(db,binding)};
      } else if(operation==='read') {
        result={instances:instance?[readInstance(db,binding,instance)]:listInstances(db,binding).map(row=>readInstance(db,binding,row.instance_id))};
      } else if(operation==='change') {
        result=changeAt(db,binding,request,{dbPath,addressIndex,addressRunFn,cmsBinary,cmsRunFn,workspacePackages});
      } else if(operation==='change-receipt') {
        const current=readInstance(db,binding,request.instance_id),prior=changeReceipt(db,binding,request.instance_id,request.request_id);
        result={found:!!prior,instance:current,receipt:prior?decodedReceipt(prior,binding):null};
      } else {
        const review=reviewAt(db,binding,request.template_id);
        if(operation==='review')result=review;
        else {
          refuse(request.review_digest===review.review_digest,'review_changed','The template or destination changed. Review the current program before preparing it.',4);
          if(!review.existing) {
            db.exec(`CREATE TABLE IF NOT EXISTS _program_workspace_preparation(
              namespace TEXT NOT NULL,instance_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,
              template_id TEXT NOT NULL,template_digest TEXT NOT NULL,actor TEXT NOT NULL,prepared_at TEXT NOT NULL,
              PRIMARY KEY(namespace,instance_id))`);
            const compiled=compilePrograms(db,{subject:binding.subject,siteId:derivedSite(binding),programs:[request.template_id],
              addressIndex,newStatus:'draft',instanceBoundary:privateBoundary,addressRunFn});
            refuse(compiled.compiled.length===1&&compiled.no_such_template.length===0&&compiled.compiled[0].instance===review.instance_id,
              'compilation_incomplete','The program compiler did not confirm this exact preparation.',6);
            db.query('INSERT INTO _program_workspace_preparation(namespace,instance_id,subject,scope,template_id,template_digest,actor,prepared_at) VALUES(?,?,?,?,?,?,?,?)')
              .run(policy.namespace,review.instance_id,binding.subject,binding.scope,request.template_id,review.template.digest,binding.actor,new Date().toISOString());
          }
          result={instance:readInstance(db,binding,review.instance_id),created:!review.existing};
        }
      }
      const value={binding,operation,...result};
      // Packet capacity is part of admission and is checked before commit.
      encode([1,true,value]);
      return value;
    };
    const transaction=db.transaction(work);
    return writeOperations.has(operation)?transaction.immediate():transaction();
  } finally { db.close(); }
}

function argumentsFor(argv) {
  const fields=new Map([['--operation','operation'],['--actor','actor'],['--subject','subject'],['--scope','scope'],['--db','db'],['--instance','instance'],['--address-index','addressIndex']]);
  const options={};
  while(argv.length){const key=argv.shift(),value=argv.shift();refuse(fields.has(key)&&typeof value==='string'&&(value.length>0||key==='--instance')&&!Object.hasOwn(options,fields.get(key)),'invalid_arguments','The program owner arguments are incomplete or duplicated.');options[fields.get(key)]=value;}
  return options;
}
function boundedInput() {
  const chunks=[];let total=0;
  for(;;){const chunk=Buffer.alloc(Math.min(65536,maxPacket+1-total)),n=readSync(0,chunk,0,chunk.length,null);if(!n)break;total+=n;refuse(total<=maxPacket,'packet_too_large','The program request exceeds its size limit.');chunks.push(chunk.subarray(0,n));}
  return Buffer.concat(chunks);
}
function writePacket(packet) {
  const bytes=encode(packet);let offset=0;
  while(offset<bytes.length) { const written=writeSync(1,bytes,offset,bytes.length-offset);if(!written)throw new Error('The native program response could not be written.');offset+=written; }
}
if(import.meta.main) {
  try {
    const options=argumentsFor(process.argv.slice(2)),body=boundedInput();
    const result=operate(options,body);
    writePacket([1,true,result]);
  } catch(error) {
    const refusal=error instanceof ProgramRefusal?error:new ProgramRefusal('owner_unavailable','The program owner could not confirm this operation. Existing work is preserved.',6);
    writePacket([1,false,{code:refusal.code,message:refusal.message}]);
    process.exitCode=refusal.exitCode;
  }
}
