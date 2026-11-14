import { test, beforeAll, afterAll, expect } from 'bun:test';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fixture } from './auth_continuation_fixture.mjs';

// Full native RSS -> material -> anonymous page path. Test feeds remain local;
// no publisher is contacted and no account, installed draft or production row changes.
let f, page;
const xml = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;');
const sources = [
  ['qa-show-a', 'Signal Notes', true, -3600, 'A shared episode headline'],
  ['qa-show-b', 'Signal Notes', true, -3700, 'A shared episode headline'],
  ['qa-show-c', 'Notes without artwork', false, -3800, 'A conversation without a supplied picture'],
  ['qa-show-future', 'Not released yet', true, 86400, 'Tomorrow is not a released episode'],
  ['qa-show-archive', 'Earlier season', true, -60 * 86400, 'An archival conversation'],
];
beforeAll(async () => {
  f = await fixture();
  const now = Date.now();
  for (const [source, label, image, seconds, title] of sources) {
    f.run(f.cms, ['entry', 'create', 'internet_source', JSON.stringify({
      family:'creators', source, label, says:'Explicit local RSS presentation fixture',
      url:'https://' + source + '.example.test/feed', twin:source, state:'observed',
      extract:'rss:item', title_field:'title', meta_field:'', link_field:'feed:auto',
      image_field:'', image_base:'', sub_field:'', body_regime:'retained-public',
      status:'published', tier:'warm', kind:'audio'
    }), '--ns', 'root', '--db', f.db, '--schemas', join(f.home, 'schemas')]);
    const body = join(f.home, 'foreign/bodies', source + '.xml');
    const items = [0,1].map(index => `<item><title>${xml(index ? title + ' earlier' : title)}</title><link>https://${source}.example.test/episode-${index}</link><guid>${source}-${index}</guid><pubDate>${new Date(now + (seconds - index * 1800) * 1000).toUTCString()}</pubDate><description>Retained source words for ${xml(label)}.</description>${image ? `<media:thumbnail url="https://${source}.example.test/cover.png"/>` : ''}<enclosure url="https://${source}.example.test/episode-${index}.mp3" type="audio/mpeg" length="1024"/></item>`).join('');
    writeFileSync(body, `<?xml version="1.0"?><rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/"><channel><title>${xml(label)}</title>${items}</channel></rss>`);
    writeFileSync(join(f.home, 'foreign/receipts', source + '.json'), JSON.stringify({body_path:body}));
  }
  page = await f.request('/', {host:'aurekai.live'});
}, 30000);
afterAll(async () => { await f?.close(); });
// The front's lead and its Listen list together are the current lineup.
const shows = () => page.body.match(/<div class="fd fd-discover"[\s\S]*?<section class="fd-standard"/)?.[0] || '';

test('anonymous front retains two distinct feeds whose episode and show titles agree', () => {
  expect(page.status).toBe(200);
  expect(shows()).toContain('/o/qa-show-a/');
  expect(shows()).toContain('/o/qa-show-b/');
  expect(shows().match(/data-by="Signal Notes"/g)?.length).toBe(2);
});
test('one source without artwork does not remove other sources supplied covers', () => {
  expect(shows()).toContain('qa-show-a.example.test/cover.png');
  expect(shows()).toContain('qa-show-b.example.test/cover.png');
  expect(shows()).toContain('Notes without artwork');
  expect(shows()).not.toContain('qa-show-c.example.test/cover.png');
});
test('future and old episodes are not described as the current show lineup', () => {
  expect(shows()).not.toContain('Not released yet');
  expect(shows()).not.toContain('Earlier season');
});
test('same-heading episodes retain separately openable native item addresses', async () => {
  for (const source of ['qa-show-a', 'qa-show-b']) {
    const href = shows().match(new RegExp('href="(/o/' + source + '/[a-f0-9]{24})"'))?.[1];
    expect(Boolean(href)).toBe(true);
    const detail = await f.request(href, {host:'aurekai.live'});
    expect(detail.status).toBe(200);
    expect(detail.body.includes('A shared episode headline')).toBe(true);
    expect(detail.body.includes(source + '.example.test/episode-0')).toBe(true);
  }
});
test('the corrected front uses ordinary links and the existing stylesheet', () => {
  expect(shows()).toContain('data-ak-track');
  expect(page.body).toContain('/assets/composition.css');
  expect(shows()).not.toContain('autoplay');
});
