import { test, expect } from 'bun:test';
import { InformationPolicy } from '../information-policy.mjs';
import { Opportunity, StandingAddress, ACTIONS, territoryActions, attentionDelta, institutionalDelta } from '../standing-address.mjs';

const tonight = () => [
  new Opportunity('live:neon-june', 'WATCH', 'live', { relevance: 0.9, reason: 'playing two blocks away' }),
  new Opportunity('event:dj-set-2230', 'GO', 'live', { relevance: 0.7 }),
  new Opportunity('creator:neon-june', 'FOLLOW', 'live', { relevance: 0.4 }),
  new Opportunity('post:trees-line', 'SAVE', 'live', { relevance: 0.2 }),
  new Opportunity('offer:guest-list', 'JOIN', 'live', { relevance: 0.6, available: false }),
];
const address = () => new StandingAddress({ actor: 'person:nick', territory: 'live' });

test('the unit is an addressed action and entertainment may win', () => {
  expect(new Opportunity('creator:neon-june', 'WATCH', 'live').subject).toBe(new Opportunity('creator:neon-june', 'FOLLOW', 'live').subject);
  const ranked = address().compile(tonight());
  expect(ranked.slice(0, 2).map(o => [o.action, o.subject])).toEqual([['WATCH', 'live:neon-june'], ['GO', 'event:dj-set-2230']]);
  expect(ranked.at(-1).subject).toBe('offer:guest-list');
});

test('territory actions have one CUE-authored vocabulary and reject category errors', () => {
  expect(territoryActions.life).toContain('MEET');
  expect(territoryActions.live).toContain('WATCH');
  expect(territoryActions.shop).toContain('BUY');
  expect(territoryActions.work).toContain('APPLY');
  expect(Object.entries(territoryActions).filter(([, verbs]) => verbs.includes('JOIN')).map(([name]) => name)).toEqual(['life', 'live', 'shop']);
  expect(ACTIONS).toContain('ASSIGN');
  expect(() => new Opportunity('tickets', 'BUY', 'live')).toThrow('category error');
  expect(() => new Opportunity('s', 'SCROLL', 'live')).toThrow('unknown action');
  expect(() => new Opportunity('s', 'WATCH', 'metaverse')).toThrow('unknown territory');
  expect(new StandingAddress({ actor: 'person:nick', territory: 'work' }).compile(tonight())).toEqual([]);
});

test('attention reorders only; unavailable remains below available and no obligation opens', () => {
  const standing = address();
  for (let i = 0; i < 50; i++) attentionDelta(standing, 'event:dj-set-2230', 'watched');
  const ranked = standing.compile(tonight());
  expect(ranked[0].subject).toBe('event:dj-set-2230');
  expect(ranked[0].action).toBe('GO');
  expect(standing.attention.get('event:dj-set-2230')).toBe(50);
  for (let i = 0; i < 50; i++) standing.observeAttention('offer:guest-list', 'dwelled');
  expect(standing.compile(tonight()).at(-1).subject).toBe('offer:guest-list');
  for (const action of ['BUY', 'BOOK', 'APPLY', 'JOIN']) expect(() => standing.observeAttention('s', action)).toThrow('may never open an obligation');
  expect(attentionDelta(standing, 's', 'reacted')).toEqual({ loop: 'attention', subject: 's', signal: 'reacted', invalidates: ['standing:person:nick:live'], opens_obligation: false });
});

test('the institutional loop needs enactment and occurrence, and policy gates before ranking', () => {
  expect(() => institutionalDelta('event:x', 'BOOK', { enactment: '', occurrence: '' })).toThrow("Action's name");
  expect(institutionalDelta('event:dj-set-2230', 'GO', { enactment: 'ef1', occurrence: 'effect:rsvp-1' })).toEqual({ loop: 'institutional', subject: 'event:dj-set-2230', action: 'GO', enactment: 'ef1', occurrence: 'effect:rsvp-1', invalidates: ['subject:event:dj-set-2230', 'enactment:ef1'], opens_obligation: true });
  const hidden = new InformationPolicy('Opportunity').disclose('subject', { to: ['institution'], reason: 'not public' });
  expect(new StandingAddress({ actor: 'person:nick', territory: 'live', policy: hidden }).compile(tonight())).toEqual([]);
  const shown = new InformationPolicy('Opportunity').disclose('subject', { to: ['public', 'institution'] });
  const visible = new StandingAddress({ actor: 'person:nick', territory: 'live', policy: shown });
  expect(visible.top(tonight(), 2)).toEqual(visible.compile(tonight()).slice(0, 2));
});
