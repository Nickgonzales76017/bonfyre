import { test, expect } from 'bun:test';
import { PatternError, pattern, atExtent, referenceSource, PatternCatalogue,
  referenceEstate, referencesByUse, referenceSources, permissiveLicenses,
  territoryActions, actionExtents } from '../surface-pattern.mjs';

const estate = referenceEstate();
const entry = name => estate.get(name);
const rejects = (fn, text) => {
  expect(fn).toThrow(PatternError);
  if (text) expect(fn).toThrow(text);
};

test('only page and workspace carry actions', () => {
  expect(entry('LiveCreatorRoom').extent).toBe('workspace');
  expect(entry('LiveCreatorRoom').actions.size).toBeGreaterThan(0);
  expect(entry('LiveMoment').extent).toBe('chip');
  expect(entry('LiveMoment').actions.size).toBe(0);
});
test('card offering action is refused', () => rejects(() => pattern({ name:'BookFromThumbnail', reads:'cms_type', source:'Event', extent:'card', actions:['BOOK'], authority:'book:event', territory:'work' }), 'no room to act inside a reference'));
test('workspace shown as card loses actions, not subject', () => {
  const room = entry('LiveCreatorRoom'), card = atExtent(room, 'card');
  expect(card.source).toBe(room.source);
  expect(card.actions.size).toBe(0);
  expect(card.authority).toBe('');
});
test('returning to page restores nothing', () => {
  expect(atExtent(atExtent(entry('LiveCreatorRoom'), 'card'), 'page').actions.size).toBe(0);
  expect(entry('LiveCreatorRoom').actions.size).toBeGreaterThan(0);
});
test('unknown extent is refused', () => rejects(() => atExtent(entry('LiveMoment'), 'billboard')));
test('pattern declares one owner', () => {
  expect(entry('InterestCommunity').reads).toBe('cms_type');
  expect(entry('LiveCreatorRoom').reads).toBe('native_organ');
});
test('neither owner is refused', () => rejects(() => pattern({ name:'Vague', reads:'whatever', source:'Thing', extent:'page' }), 'two patterns sharing a name'));
test('pattern must name source', () => rejects(() => pattern({ name:'Empty', reads:'cms_type', source:'', extent:'page' }), 'names no source'));
test('action layer states required authority', () => {
  expect(entry('PlacePage').authority).toBe('participate:place');
  expect(entry('ProfessionalPresence').authority).toBe('engage:professional');
});
test('action layer without authority is refused', () => rejects(() => pattern({ name:'Open', reads:'cms_type', source:'Event', extent:'page', actions:['GO'], territory:'life' }), 'nothing can refuse it'));
test('authority requirement is not a grant', () => {
  const p = entry('PlacePage');
  expect(p.actions.size).toBeGreaterThan(0);
  expect(p.authority).toBeTruthy();
  expect(p).not.toHaveProperty('granted');
});
test('unknown action is refused', () => rejects(() => pattern({ name:'Odd', reads:'cms_type', source:'Event', extent:'page', actions:['SCROLL'], authority:'x' }), 'does not carry'));
test('catalogue records borrowed behaviour', () => {
  const lineages = estate.byLineage();
  for (const name of ['short-video feeds', 'live streaming', 'imageboards', 'Pinterest', 'Google Maps', 'Nextdoor', 'Reddit', 'Shopify', 'eBay', 'Walmart', 'LinkedIn', 'Zulip', 'Linear', 'Notion', 'Salesforce']) expect(lineages).toHaveProperty(name);
  expect(lineages['short-video feeds']).toContain('VerticalDiscovery');
});
test('bidirectional compiler patterns are native', () => {
  for (const name of ['LiveMoment', 'LiveSemanticOverlay', 'LiveTimeline', 'ActionCard', 'PlacePage', 'WorkInbox', 'Checkout', 'ApprovalQueue', 'AgentWorkmate']) expect(estate.byLineage().native).toContain(name);
});
test('catalogue answers which patterns offer an action', () => {
  expect(estate.offering('BOOK')).toEqual(['CalendarWorkspace']);
  expect(estate.offering('SELL')).toEqual(['SellFlow']);
  expect(estate.offering('REMEMBER')).toEqual(['MemoryTimeline']);
  expect(new Set(estate.offering('GO').map(name => entry(name).territory))).toEqual(new Set(['live','life']));
});
test('duplicate pattern is refused', () => rejects(() => estate.add({ name:'PlacePage', reads:'cms_type', source:'Event', extent:'page', territory:'life' }), 'already declared'));
test('unknown pattern lookup names what exists', () => rejects(() => estate.get('TikTokClone'), 'VerticalDiscovery'));
test('every action pattern sits at action extent', () => {
  for (const p of estate.patterns.values()) if (p.actions.size) {
    expect(actionExtents.has(p.extent)).toBe(true);
    expect(p.authority).toBeTruthy();
  }
});
test('surface commons covers all four territories', () => {
  const counts = Object.fromEntries(Object.entries(estate.byTerritory()).map(([t, rows]) => [t, rows.length]));
  expect(Object.keys(counts).sort()).toEqual(Object.keys(territoryActions).sort());
  expect(Object.values(counts).every(n => n > 0)).toBe(true);
  expect(Object.values(counts).reduce((a,b) => a+b, 0)).toBe(41);
  expect(estate.patterns.size).toBe(41);
});
test('pattern cannot offer verb territory does not carry', () => rejects(() => pattern({ name:'BuyFromTheFeed', reads:'cms_type', source:'Product', extent:'page', actions:['BUY'], authority:'x', territory:'live' }), 'does not carry'));
test('each territory answers its own question', () => {
  expect(estate.byTerritory().live).toContain('VerticalDiscovery');
  expect(estate.byTerritory().work).toContain('WorkInbox');
  expect(estate.offering('DO').length).toBeGreaterThan(0);
  expect(estate.offering('DO').every(name => entry(name).territory === 'work')).toBe(true);
  expect(estate.offering('WATCH').every(name => entry(name).territory === 'live')).toBe(true);
});
test('narrowing extent keeps territory', () => {
  expect(entry('CollectionBoard').territory).toBe('life');
  expect(atExtent(entry('CollectionBoard'), 'chip').territory).toBe('life');
});
test('unknown territory is refused', () => rejects(() => pattern({ name:'X', reads:'cms_type', source:'Y', extent:'page', territory:'metaverse' }), 'belongs to one of'));
test('permissive project may be donor', () => expect(referenceSource('Pinry','BSD-2-Clause','donor','collecting intent').use).toBe('donor'));
test('copyleft project cannot be donor', () => {
  rejects(() => referenceSource('Plane','AGPL-3.0','donor','issue interaction'), 'network copyleft would extend to this estate');
  rejects(() => referenceSource('Plane','AGPL-3.0','donor','issue interaction'), 'reference instead');
});
test('field-of-use license cannot be donor', () => rejects(() => referenceSource('Saleor','FSL','donor','storefront design'), 'forbids competing use'));
test('mixed license needs review', () => rejects(() => referenceSource('Mattermost webapp','mixed','donor','conversation'), 'file-level review'));
test('unknown license defaults to refusing donorship', () => {
  rejects(() => referenceSource('Something','Custom-1.0','donor','x'), 'not on the permissive list');
  expect(referenceSource('Something','Custom-1.0','reference','x')).toBeTruthy();
});
test('every declared reference is classified', () => {
  for (const r of referenceSources) {
    if (['donor', 'implementation'].includes(r.use)) expect(permissiveLicenses.has(r.license)).toBe(true);
    expect(r.solves).toBeTruthy();
  }
});
test('copyleft projects are references', () => {
  const uses = referencesByUse();
  for (const name of ['Lemmy', 'Pixelfed', 'Immich', 'Plane', 'AppFlowy', 'Twenty', 'Cal.com', 'Saleor storefront']) expect(uses.reference).toContain(name);
  expect(uses.donor).toContain('Spree Storefront');
  expect(uses.donor).toContain('Zulip');
  expect(uses.implementation).toContain('Fedify');
});
test('reference records what project solved', () => expect(referenceSources.find(r => r.project === 'Zulip').solves).toContain('topic-within-stream'));
