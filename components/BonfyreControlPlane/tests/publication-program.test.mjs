import { test, expect } from 'bun:test';
import { InformationPolicy } from '../information-policy.mjs';
import { PublicationProgram, PublicationRefused, socialClipProfile } from '../publication-program.mjs';
import { AuthorityGraph } from '../protocol-authority.mjs';

const subject = 'presence:creator:neon-june';
const clip = { subject, title: 'Friday at Trees', body: "we're playing Trees again Friday",
  creator: 'Neon June', url: 'https://aurekai.live/clip/friday-announcement',
  published_at: '2026-09-04T21:40:00Z', location: "Reno's, Deep Ellum" };
function policy() {
  const p = new InformationPolicy('SocialClip');
  for (const field of ['subject', 'title', 'body', 'creator', 'url', 'published_at']) {
    p.disclose(field, { to: ['public', 'community', 'institution'] });
  }
  return p.disclose('location', { to: ['community', 'institution'],
    reason: 'a live location is not published to the open web' });
}
function program(p = policy()) {
  return new PublicationProgram({ subject, principal: 'person:nick', agent: 'estate:aurekai',
    purpose: 'creator distribution', profile: socialClipProfile(p) })
    .to('mastodon', { carrier: 'activitypub' })
    .to('bluesky', { carrier: 'atproto' })
    .to('shortvideo-net', { carrier: 'shortvideo' });
}
function authority() {
  const grants = new Map();
  return {
    grant(target, purpose = 'creator distribution', expiresAt = null) {
      grants.set(`publish:${target}`, { purpose, expiresAt, revoked: false });
    },
    revoke(target) { grants.get(`publish:${target}`).revoked = true; },
    hasAuthority(_actor, permission, _subject, { at_iso, purpose }) {
      const grant = grants.get(permission);
      return !!grant && !grant.revoked && grant.purpose === purpose &&
        (!at_iso || !grant.expiresAt || at_iso <= grant.expiresAt);
    },
  };
}
function grantAll(a, p, expiresAt = null) {
  for (const target of p.targets) a.grant(target.platform, 'creator distribution', expiresAt);
}
function adapters() {
  return Object.fromEntries(['mastodon', 'bluesky', 'shortvideo-net'].map(platform => {
    const calls = [];
    const adapter = rendered => { calls.push(rendered); return { status: 200, id: `remote-${calls.length}` }; };
    adapter.calls = calls;
    return [platform, adapter];
  }));
}

test('consent is per platform, purpose, expiry and revocation; adapters stay untouched on refusal', () => {
  const p = program(), a = authority(), calls = adapters();
  expect(() => p.publish(a, clip, { adapters: calls })).toThrow(PublicationRefused);
  expect(Object.values(calls).every(adapter => !adapter.calls.length)).toBe(true);
  expect(Object.keys(p.consentGaps(a)).sort()).toEqual(['bluesky', 'mastodon', 'shortvideo-net']);
  expect(p.consentGaps(a).mastodon).toContain('publish:mastodon');
  a.grant('mastodon');
  expect(Object.keys(p.consentGaps(a)).sort()).toEqual(['bluesky', 'shortvideo-net']);
  expect(() => p.publish(a, clip, { adapters: calls })).toThrow('delegation');
  expect(Object.values(calls).every(adapter => !adapter.calls.length)).toBe(true);
  grantAll(a, p, '2026-06-30T00:00:00Z');
  expect(p.consented(a, { atIso: '2026-03-01T00:00:00Z' })).toBe(true);
  expect(p.consented(a, { atIso: '2026-09-04T00:00:00Z' })).toBe(false);
  expect(() => p.publish(a, clip, { adapters: calls, atIso: '2026-09-04T00:00:00Z' })).toThrow('delegation');
  grantAll(a, p);
  a.revoke('bluesky');
  expect(Object.keys(p.consentGaps(a))).toEqual(['bluesky']);
  expect(() => p.publish(a, clip, { adapters: calls })).toThrow('delegation');
});

test('disclosure precedes projection and projection precedes adapter', () => {
  const p = program(), a = authority(), calls = adapters();
  grantAll(a, p);
  const results = p.publish(a, clip, { adapters: calls });
  for (const result of Object.values(results)) expect(JSON.stringify(result.sent)).not.toContain("Reno's, Deep Ellum");
  expect(results.mastodon.sent.fields.content).toBe(clip.body);
  expect(results.bluesky.sent.fields.text).toBe(clip.body);
  expect(results['shortvideo-net'].sent.fields.caption).toBe(clip.title);
  expect(results['shortvideo-net'].sent.fields).not.toHaveProperty('context');
  expect(policy().fieldDecision('location', 'public')[0]).toBe(false);
  expect(policy().fieldDecision('location', 'community')[0]).toBe(true);
  const partial = { mastodon: calls.mastodon };
  calls.mastodon.calls.length = 0;
  expect(() => p.publish(a, clip, { adapters: partial })).toThrow('adapter');
  expect(calls.mastodon.calls).toHaveLength(0);
});

test('adapter report is not round-trip observation; receipt required to confirm each target', () => {
  const p = program(), a = authority();
  grantAll(a, p);
  const results = p.publish(a, clip, { adapters: adapters() });
  expect(Object.values(results).every(result => result.adapter_reported.status === 200)).toBe(true);
  expect(p.roundTripPending(results)).toEqual(['bluesky', 'mastodon', 'shortvideo-net']);
  expect(() => p.observe(results, 'mastodon', { twinReceipt: '' })).toThrow('confirming its own publication');
  expect(() => p.observe(results, 'somewhere-else', { twinReceipt: 'twin:x' })).toThrow('was not published');
  for (const platform of Object.keys(results)) p.observe(results, platform, { twinReceipt: `twin:${platform}:abc` });
  expect(p.roundTripPending(results)).toEqual([]);
  expect(results.mastodon.twin_receipt).toBe('twin:mastodon:abc');
});

test('target grammar is checked at declaration', () => {
  const p = new PublicationProgram({ subject, principal: 'person:nick', agent: 'estate:aurekai',
    purpose: 'creator distribution', profile: socialClipProfile(policy()) });
  expect(() => p.to('some-network', { carrier: 'rss' })).toThrow(PublicationRefused);
  expect(p.targets).toEqual([]);
});

test('publication gates on the compiled AuthorityGraph, not a local grant approximation', () => {
  const graph = new AuthorityGraph();
  try {
    const p = program(), calls = adapters();
    expect(() => p.publish(graph, clip, { adapters: calls })).toThrow('delegation');
    for (const target of p.targets) {
      graph.grant({ edge_id: `g:${target.platform}`, actor: p.agent,
        permission: `${target.operation}:${target.platform}`, subject: p.subject,
        purpose: p.purpose });
    }
    expect(p.consented(graph)).toBe(true);
    expect(Object.keys(p.publish(graph, clip, { adapters: calls }))).toHaveLength(3);
    graph.revoke('g:bluesky');
    expect(Object.keys(p.consentGaps(graph))).toEqual(['bluesky']);
    expect(() => p.publish(graph, clip, { adapters: calls })).toThrow('delegation');
  } finally { graph.close(); }
});
