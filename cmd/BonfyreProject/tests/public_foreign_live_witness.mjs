#!/usr/bin/env bun
// Read the actual WebAddress public catalogue via the Project MCP owner.
// This check never observes an external source, publishes, or writes CMS.
import { homedir } from 'node:os';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createProjectNative } from '../../../integrations/mcp/project_native.mjs';

const root = resolve(fileURLToPath(new URL('../../..', import.meta.url)));
const project = createProjectNative({
  root, home: homedir(),
  env: { ...process.env, BONFYRE_PUBLIC_ORIGIN: 'https://aurekai.work' },
  run: async () => { throw new Error('no native execution allowed during public release readback'); },
});
const report = await project.call('foreign_twin_release', {
  case_urls: [
    'https://www.dallascitynews.net/feed',
    'https://dallastx.new.swagit.com/videos/402001',
    'https://cityofdallas.legistar.com/MeetingDetail.aspx?GUID=C4FABFDE-D9D8-493D-A742-4D392E055CA8&ID=1362423&Search=',
  ],
  related_queries: ['dallas', 'grants', 'district'],
  grant_terms: ['SS4A'],
  civic_event_id: 1362423,
});
if (!report.generation || !Number.isInteger(report.estate_total) || report.estate_total < 1)
  throw new Error('public source release missing a generation or source population');
if (!report.case || !Array.isArray(report.case.exact_sources) || !Array.isArray(report.case.missing))
  throw new Error('missing source case proof');
if (!report.case.exact_sources.some(x => x.url === 'https://www.dallascitynews.net/feed'))
  throw new Error('known Dallas news source did not resolve through live WebAddress');
if (!report.readback.length || !report.readback.every(x => /^[a-f0-9]{64}$/.test(x.sha256)))
  throw new Error('public release reads lack exact response digests');
if (!report.civic_agenda || report.civic_agenda.state !== 'observed' ||
    !Number.isInteger(report.civic_agenda.item_count) || report.civic_agenda.item_count < 50 ||
    !/^[a-f0-9]{64}$/.test(report.civic_agenda.response_sha256 || '') ||
    report.civic_agenda.vote_verification !== 'not_performed' ||
    report.civic_agenda.video_alignment !== 'not_verified')
  throw new Error('official 2026-09-23 council agenda did not yield verifiable bounded item evidence');
if (report.case.selection_effect !== 'none' || report.context_candidates.some(x => x.relation !== 'candidate_only'))
  throw new Error('public discovery was incorrectly promoted to effects or proven relations');
console.log(JSON.stringify({
  schema: report.schema_version, live_origin: report.public_owner,
  generation: report.generation, source_contracts: report.estate_total,
  case_source_refs: report.case.source_refs, unreleased_case_urls: report.case.missing.map(x => x.url),
  context: report.context_candidates.map(x => ({ query: x.query, total: x.total, shown: x.matching_released_sources.length, truncated: x.truncated })),
  grants: report.funding_observations.map(x => ({ query: x.query, state: x.observation_state, reported: x.provider_hit_count, returned: x.returned?.length, response_digest: x.response_digest || null, reason: x.reason || null })),
  readback_sha256: report.readback.map(x => x.sha256),
  civic: { source_url:report.civic_agenda.source_url ?? report.civic_agenda.api_url,
    observation_route:report.civic_agenda.evidence_route || 'legistar_api',
    count:report.civic_agenda.item_count, response_sha256:report.civic_agenda.response_sha256,
    agenda_ids:report.civic_agenda.items.slice(0,6).map(x=>({file:x.file,number:x.agenda_number,
      funding_mentions:x.funding_amount_mentions.length,location_candidates:x.location_text_candidates.length})),
    vote_verified:false,video_aligned:false },
  effects: 'none', case_state: report.case.state,
}, null, 2));
