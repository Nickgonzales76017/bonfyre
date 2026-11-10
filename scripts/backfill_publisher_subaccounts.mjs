#!/usr/bin/env bun
/**
 * Backfill observed source contributors as subordinate publisher accounts.
 * No BonfyreAuth user, organization grant, personal identity, or inferred
 * employment/membership is created. CMS owns both typed records.
 *
 * Usage: bun scripts/backfill_publisher_subaccounts.mjs [--apply] [--estate PATH]
 * Dry run writes a bounded native CMS import set; --apply uses the BonfyreCMS
 * schema owner and its transactional import-jsonl operation.
 */
import { Database } from 'bun:sqlite';
import { spawnSync } from 'bun';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';

const repo = resolve(import.meta.dir, '..');
const hash = text => createHash('sha256').update(text).digest('hex');
const parentId = source => 'publisher-account:' + hash(source).slice(0, 32);
const childId = (source, byline) => 'publisher-subaccount:' + hash(source + '\0' + byline).slice(0, 40);
const safeSlug = text => String(text || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 48);
const parentWarning = 'Source publication and a publisher workspace are not legal ownership, employment, user admission, verified affiliation, authority or consent.';
const childWarning = 'A printed byline is not a claimed identity, authenticated account, employee, community member, publication permission, or writer of an Aurekai post.';

const parse = argv => {
  const o = { apply: false, estate: join(homedir(), '.bonfyre/estate-local') };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--apply') o.apply = true;
    else if (argv[i] === '--estate' && argv[i+1]) o.estate = resolve(argv[++i]);
    else throw new Error('Usage: backfill_publisher_subaccounts.mjs [--apply] [--estate DIR]');
  }
  return o;
};

function run(argv, maxBuffer = 16 * 1024 * 1024) {
  const r = spawnSync(argv, { cwd: repo, stdout: 'pipe', stderr: 'pipe',
    timeout: 120000, maxBuffer });
  if (r.exitCode !== 0) throw new Error(argv.slice(0, 3).join(' ') + ': ' + r.stderr.toString().slice(-800));
  return r.stdout.toString();
}

export function buildPlan(manifest, lookupSource, lookupPublishedHost) {
  const fields = manifest.split('\0');
  if (fields.at(-1) !== '' || (fields.length - 1) % 2) throw new Error('Incomplete profile-host manifest');
  const parents = new Map(), children = new Map(), usedHosts = new Map();
  for (let i = 0; i + 1 < fields.length; i += 2) {
    const coordinate = fields[i], leaf = fields[i+1];
    const url = new URL(coordinate, 'https://aurekai.work');
    const byline = url.searchParams.get('who'), source = url.searchParams.get('source');
    if (url.pathname !== '/people' || url.searchParams.get('profile') !== '1'
        || !byline || !source || /[\x00-\x1f]/.test(byline + source)) throw new Error('Invalid source coordinate');
    const match = /^hosts\/(credited-[a-z0-9-]+-[a-f0-9]{20}\.aurekai\.work)\/index\.html$/.exec(leaf);
    if (!match) throw new Error('Unsafe profile host leaf: ' + leaf);
    const existingHost = usedHosts.get(match[1]);
    if (existingHost && existingHost !== source + '\0' + byline) throw new Error('Profile hostname collision');
    usedHosts.set(match[1], source + '\0' + byline);
    const src = lookupSource(source);
    if (!src) throw new Error('Missing published publisher source: ' + source);
    if (!parents.has(source)) {
      const name = src.publisher_name || src.label || source;
      const candidate = src.publisher_name ? lookupPublishedHost(src.publisher_name) : null;
      parents.set(source, {
        account_id: parentId(source),
        account_subject: 'institution:source:' + hash(source).slice(0, 32),
        source, name,
        publisher_subject: candidate?.subject || '',
        publisher_profile_url: candidate?.url || '',
        source_url: src.url || '',
        state: 'observed-publisher',
        cannot_infer: parentWarning,
        status: 'published'
      });
    }
    const key = childId(source, byline);
    if (children.has(key)) throw new Error('Duplicate subordinate account');
    children.set(key, {
      subaccount_id: key,
      account_subject: 'subaccount:publisher:' + hash(source + '\0' + byline).slice(0, 40),
      publisher_account_id: parentId(source),
      source, display_name: byline, host: match[1],
      evidence_path: coordinate,
      identity_state: 'unclaimed',
      claimed_actor_subject: '',
      cannot_infer: childWarning,
      status: 'published'
    });
  }
  if (children.size === 0) throw new Error('No admitted contributor subaccounts');
  return { parents: [...parents.values()].sort((a,b) => a.account_id.localeCompare(b.account_id)),
    children: [...children.values()].sort((a,b) => a.subaccount_id.localeCompare(b.subaccount_id)) };
}

function selectMissing(db, table, key, rows) {
  const present = new Map();
  if (db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table)) {
    for (const row of db.query('SELECT * FROM ' + table + " WHERE namespace='root'").all()) {
      present.set(row[key], row);
    }
  }
  const missed = [];
  for (const row of rows) {
    const saved = present.get(row[key]);
    if (!saved) { missed.push(row); continue; }
    for (const [field,value] of Object.entries(row))
      if ((saved[field] ?? '') !== value) throw new Error('Existing ' + table + ' differs: ' + row[key] + ' ' + field);
  }
  return missed;
}

function main(argv = process.argv.slice(2)) {
  const args = parse(argv), cmsPath = join(args.estate, 'cms.db'),
    itemsPath = join(args.estate, 'items.db'),
    binary = join(repo, 'cmd/BonfyreCMS/bonfyre-cms'),
    manifestTool = join(repo, 'cmd/BonfyreWebAddress/tools/people-static-manifest'),
    schemas = join(repo, 'cmd/BonfyreCMS/content-types');
  if (![cmsPath, itemsPath, binary, manifestTool].every(existsSync)) throw new Error('Canonical source or owner unavailable');
  const manifest = run([manifestTool, '--archive', itemsPath, '--cms', cmsPath, '--profile-hosts', '--null']);
  const db = new Database(cmsPath, { readonly: true });
  const querySource = db.query("SELECT label, COALESCE(publisher_name,'') publisher_name, COALESCE(url,'') url FROM internet_source WHERE namespace='root' AND status='published' AND source=? ORDER BY id DESC LIMIT 1");
  const queryParent = db.query("SELECT subject_address subject,host FROM site_presentation WHERE namespace='root' AND status='published' AND profile='newsroom' AND subject_address=? ORDER BY id DESC LIMIT 1");
  const plan = buildPlan(manifest, source => querySource.get(source), name => {
    const slug = safeSlug(name);
    const found = slug ? queryParent.get('institution:publisher:' + slug) : null;
    return found?.host && /^[a-z0-9-]+\.aurekai\.(life|work|live|shop)$/.test(found.host) ?
      { subject: found.subject, url: 'https://' + found.host + '/' } : null;
  });
  if (args.apply) {
    db.close();
    run([binary, 'schema', 'migrate', '--db', cmsPath, '--schemas', schemas], 4 * 1024 * 1024);
  } else db.close();
  const current = new Database(cmsPath, { readonly: true });
  const missingParents = selectMissing(current, 'publisher_account', 'account_id', plan.parents);
  const missingChildren = selectMissing(current, 'publisher_subaccount', 'subaccount_id', plan.children);
  current.close();
  const output = join(args.estate, 'publisher-account-backfill');
  mkdirSync(output, { recursive: true });
  const parentFile = join(output, 'publisher-accounts.jsonl'), childFile = join(output, 'publisher-subaccounts.jsonl');
  writeFileSync(parentFile, missingParents.map(x => JSON.stringify(x)).join('\n') + (missingParents.length ? '\n' : ''), { mode: 0o600 });
  writeFileSync(childFile, missingChildren.map(x => JSON.stringify(x)).join('\n') + (missingChildren.length ? '\n' : ''), { mode: 0o600 });
  if (args.apply) {
    if (missingParents.length) run([binary, 'entry', 'import-jsonl', 'publisher_account', parentFile,
      '--ns', 'root', '--db', cmsPath, '--schemas', schemas], 4 * 1024 * 1024);
    if (missingChildren.length) run([binary, 'entry', 'import-jsonl', 'publisher_subaccount', childFile,
      '--ns', 'root', '--db', cmsPath, '--schemas', schemas], 4 * 1024 * 1024);
    const check = new Database(cmsPath, { readonly: true });
    const parentsLeft = selectMissing(check, 'publisher_account', 'account_id', plan.parents);
    const childrenLeft = selectMissing(check, 'publisher_subaccount', 'subaccount_id', plan.children);
    check.close();
    if (parentsLeft.length || childrenLeft.length) throw new Error('CMS subaccount closure incomplete');
  }
  console.log(['PUBLISHER_SUBACCOUNTS', args.apply ? 'APPLIED' : 'DRY_RUN',
    'publishers=' + plan.parents.length, 'subaccounts=' + plan.children.length,
    'new_publishers=' + missingParents.length, 'new_subaccounts=' + missingChildren.length,
    'parent_links=' + plan.parents.filter(x => x.publisher_subject).length].join('\t'));
}
if (import.meta.main) main();
