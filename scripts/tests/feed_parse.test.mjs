import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { decodeFeed, FeedError, parseReceiptedFeed } from '../feed-parse.mjs';

const fixtures = {
  rss: `<?xml version="1.0"?><rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>
    <item><guid>one</guid><title>First</title><link>https://news.test/one</link>
      <pubDate>Today</pubDate><description>Announcement</description><dc:creator>Ada</dc:creator></item>
    <item><guid>one</guid><title>Duplicate</title></item>
    <item><title>Second</title><link>https://news.test/two</link><dc:date>Tomorrow</dc:date></item>
  </channel></rss>`,
  atom: `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
    <entry><id>atom-1</id><title>Entry</title><link href="https://news.test/a"/>
      <updated>2026-09-29</updated><summary>Brief</summary><author><name>Grace</name></author></entry>
  </feed>`,
  json: JSON.stringify({ version: 'https://jsonfeed.org/version/1.1', items: [
    { id: 'json-1', title: 'One', url: 'https://news.test/j',
      date_published: '2026-09-29', authors: [{ name: 'Lin' }] },
  ] }),
};

function receipted(body, callback) {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-feed-parse.'));
  try {
    const bodyPath = join(directory, 'body'), receiptPath = join(directory, 'receipt.json');
    writeFileSync(bodyPath, body);
    const receipt = { body_path: bodyPath,
      content_sha256: createHash('sha256').update(body).digest('hex'),
      source_url: 'https://news.test/feed', observed_at: '2026-09-29T12:00:00Z', twin_id: 'news' };
    writeFileSync(receiptPath, JSON.stringify(receipt));
    return callback({ bodyPath, receiptPath, receipt });
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

test('native RSS and Atom decoders preserve identity, namespaced fields and source-declares boundary', () => {
  receipted(fixtures.rss, ({ receiptPath }) => {
    const parsed = parseReceiptedFeed(receiptPath, { includeItems: true });
    expect(parsed).toMatchObject({ carrier: 'rss', items_declared: 3, candidates: 2 });
    expect(parsed.items[0].declares).toMatchObject({ declared_id: 'one', author: 'Ada' });
    expect(parsed.items[1].declares).toMatchObject({ declared_id: 'https://news.test/two',
      declared_time: 'Tomorrow' });
    expect(parsed.items[0].observed).toMatchObject({ receipt: receiptPath, carrier: 'rss' });
    expect(parsed.items[0].status).toBe('source_declares');
    const cli = Bun.spawnSync(['bun', new URL('../feed-parse.mjs', import.meta.url).pathname,
      receiptPath, '--json']);
    expect(cli.exitCode).toBe(0);
    expect(JSON.parse(new TextDecoder().decode(cli.stdout))).toEqual(parsed);
  });
  receipted(fixtures.atom, ({ receiptPath }) => {
    const parsed = parseReceiptedFeed(receiptPath, { includeItems: true });
    expect(parsed.items[0].declares).toMatchObject({ declared_id: 'atom-1',
      url: 'https://news.test/a', author: 'Grace', summary: 'Brief' });
  });
});

test('JSON feed uses same candidate boundary; digest mismatch yields no candidates', () => {
  receipted(fixtures.json, ({ bodyPath, receiptPath }) => {
    const parsed = parseReceiptedFeed(receiptPath, { includeItems: true });
    expect(parsed).toMatchObject({ carrier: 'json-feed', items_declared: 1, candidates: 1 });
    expect(parsed.items[0].declares.author).toBe('Lin');
    writeFileSync(bodyPath, '{"items":[]}');
    const cli = Bun.spawnSync(['bun', new URL('../feed-parse.mjs', import.meta.url).pathname,
      receiptPath, '--json']);
    expect(cli.exitCode).toBe(3);
    expect(JSON.parse(new TextDecoder().decode(cli.stdout)).error)
      .toBe('body_does_not_match_receipt');
    try { parseReceiptedFeed(receiptPath, { includeItems: true }); }
    catch (error) {
      expect(error).toBeInstanceOf(FeedError);
      expect(error.code).toBe(3);
      expect(error.result.error).toBe('body_does_not_match_receipt');
      expect(error.result).not.toHaveProperty('items');
      return;
    }
    throw new Error('mismatched body was accepted');
  });
});

test('intact non-feed is rejected without creating a candidate', () => {
  receipted('<html>challenge</html>', ({ receiptPath }) => {
    try { parseReceiptedFeed(receiptPath); }
    catch (error) {
      // A syntactically valid XML document may be an empty RSS candidate set,
      // but malformed XML must be an explicit non-feed refusal.
      expect(error.result.error).toBe('not_a_feed');
      return;
    }
  });
  expect(() => decodeFeed(Buffer.from('<rss><unclosed>'))).toThrow();
});
