#!/usr/bin/env bun
// Retained public material to exact native CMS owner rows; no authority promotion.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

const boundary = 'a retained public provider record does not establish private membership, role, payment, enrollment, property right, authority, or effect';
const ident = (prefix, ...parts) => `${prefix}-${createHash('sha256').update(parts.map(String).join('\x1f')).digest('hex').slice(0, 24)}`;
const row = (type, body) => ({ type, namespace: 'root', body: { ...body, status: 'published' } });

export function materialize(e) {
  if (!e || typeof e !== 'object' || !['provider', 'source_ref', 'receipt_ref', 'observed_at', 'payload'].every(k => e[k]))
    throw new Error('provider, source receipt, observation time, and payload required');
  const p = e.payload;
  if (!p || typeof p !== 'object' || Array.isArray(p) || !p.id || !p.title ||
      typeof p.url !== 'string' || !p.url.startsWith('https://'))
    throw new Error('payload needs exact id, title, HTTPS URL');
  const source = e.source_ref, receipt = e.receipt_ref, at = e.observed_at;
  switch (e.kind) {
    case 'github_repository':
      return [row('project', { project_id: ident('project', e.provider, p.id), subject_ref: `external:github:${p.id}`,
        title: p.title, pitch: p.description || 'Public repository observed from GitHub.', stage: 'observed',
        goal_stated: '', recurring_stated: '', operating_stated: '', currency: '', opened_at: p.created_at || at,
        cannot_infer: boundary })];
    case 'reddit_community':
    case 'discord_server':
      return [row('community_body', { community: ident('community', e.provider, p.id),
        subject_address: `external:${e.provider}:${p.id}`, title: p.title, description: p.description || '',
        visibility: 'public_observed', governance_scope: '', moderation_policy: '', created_by_subject: '',
        cannot_infer: boundary })];
    case 'meetup_event':
    case 'eventbrite_event':
      return [row('publication_occurrence', { occurrence_id: ident('occurrence', e.provider, p.id),
        source_twin: source, source_ref: source, canonical_href: p.url, title: p.title,
        summary: p.description || '', published_at: p.starts_at || '', observed_at: at, receipt_ref: receipt,
        rights_basis: 'public_observation', retention: 'metadata', visibility: 'public',
        assertion_state: 'observed', cannot_infer: boundary })];
    case '211_household_opportunity':
      if (e.provider !== '211' || typeof p.functions !== 'string' || !p.functions.trim())
        throw new Error('211 public service classification required for household opportunity');
      return [row('household_opportunity', { opportunity_id: ident('opportunity', e.provider, p.id),
        resource_ref: ident('resource', e.provider, p.id), title: p.title, description: p.description || '',
        canonical_href: p.url, service_kinds: p.functions, source_ref: source, receipt_ref: receipt,
        observed_at: at, cannot_infer: 'a public service classification identifies an observed opportunity only; it does not establish household eligibility, application, enrollment, provider notice, service delivery, authority, or a private relationship' })];
    case '211_resource':
    case 'clever_institution':
    case 'property_record':
    case 'nextdoor_place':
    case 'patreon_creator':
      return [row('external_resource', { ext_id: ident('resource', e.provider, p.id), name: p.title,
        url: p.url, says: p.description || '', estate: e.provider, grouping: e.kind,
        functions: p.functions || '', related: source, cannot_infer: boundary })];
    default: throw new Error('unsupported incumbent record kind');
  }
}

if (import.meta.main) {
  try {
    const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, i, all) => {
      if (i % 2 === 0) pairs.push([value.slice(2), all[i + 1]]);
      return pairs;
    }, []));
    if (!args.input || !args.out) throw new Error('usage: --input FILE --out JSONL');
    const rows = materialize(JSON.parse(readFileSync(args.input, 'utf8')));
    writeFileSync(args.out, rows.map(r => JSON.stringify(r)).join('\n') + '\n');
    console.log(JSON.stringify({ rows: rows.length, truth: 'retained public record only' }));
  } catch (error) { console.error(`incumbent materializer: ${error.message}`); process.exitCode = 2; }
}
