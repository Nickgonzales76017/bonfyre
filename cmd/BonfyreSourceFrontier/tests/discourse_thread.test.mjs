import { test, expect, afterAll } from 'bun:test';
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { readDiscourseThread, readDiscourseBoard } from '../tools/discourse_thread.mjs';

const dir = mkdtempSync(join(tmpdir(), 'discourse-retained-'));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
let serial = 0;
const post = (id, number, user = 7, html = '<p>A request</p>') => ({ id, post_number: number, user_id: user, username: user === 7 ? 'buyer' : 'applicant', topic_id: 81, created_at: '2026-10-01T00:00:00Z', cooked: html });
const topic = () => ({ id: 81, title: 'Project', closed: false, archived: false,
  details: { created_by: { id: 7, username: 'buyer' } },
  post_stream: { stream: [101, 102, 103], posts: [post(101, 1), post(102, 2, 8, 'I can help; my client budget is $5000')] } });
function retain(data, url = 'https://forum.example/t/81.json', patch = {}) {
  const path = join(dir, `${serial++}.json`), body = path + '.body', bytes = JSON.stringify(data);
  writeFileSync(body, bytes);
  writeFileSync(path, JSON.stringify({ schema: 'bonfyre-foreign-observation.v1', http_status: 200,
    observation_kind: 'source_observed', policy_decision: 'allow', source_url: url, body_path: body,
    content_sha256: createHash('sha256').update(bytes).digest('hex'), ...patch }));
  return path;
}
const boardUrl = 'https://forum.example/c/jobs/13/l/latest.json?order=created';
const boardTopic = (id, created, title) => ({ id, title, category_id: 13, created_at: created,
  last_posted_at: '2026-10-08T20:00:00Z', posts_count: 30, closed: false, archived: false });
const board = () => ({ topic_list: { more_topics_url: '/c/jobs/13/l/latest?order=created&page=1', topics: [
  boardTopic(1, '2025-07-22T07:24:00Z', 'Old buyer request with new applicant replies'),
  boardTopic(2, '2026-10-08T03:47:38.827Z', 'Technical account manager looking for work')
] } });
test('board preserves original dates, sorts by creation and does not invent buyer demand', () => {
  const input = board(), before = JSON.stringify(input), result = readDiscourseBoard(retain(input, boardUrl));
  expect(result.topics.map(t => t.topic_id)).toEqual([2, 1]);
  expect(result.topics[1].topic_created_at).toBe('2025-07-22T07:24:00Z');
  expect(result.topics[1].topic_last_posted_at).toBe('2026-10-08T20:00:00Z');
  expect(result.topics[0].observation_url).toBe('https://forum.example/t/2.json');
  expect(result.retained_topics).toBe(2);
  expect(result.cannot_infer).toContain('not the complete board');
  expect(result.source_more_topics_url).toBe(input.topic_list.more_topics_url);
  expect(result.topics[0].hiring).toBeUndefined();
  expect(JSON.stringify(input)).toBe(before);
  expect(readDiscourseBoard(retain({ topic_list: { topics: [] } }, boardUrl)).topics).toEqual([]);
});
test('nested category boards retain the leaf category identity', () => {
  const input = board();
  for (const topic of input.topic_list.topics) topic.category_id = 74;
  const url = 'https://community.make.com/c/hire-a-pro/hire-help/74/l/latest.json?order=created';
  const result = readDiscourseBoard(retain(input, url));
  expect(result.category_id).toBe(74);
  expect(result.source).toBe(url);
  expect(result.topics.map(t => t.topic_id)).toEqual([2, 1]);
  expect(result.topics[0].observation_url).toBe('https://community.make.com/t/2.json');
  expect(() => readDiscourseBoard(retain(board(), url))).toThrow('invalid');
  for (const path of ['/c/hire-a-pro//74/l/latest.json', '/c/hire-a-pro/hire-help/l/latest.json',
    '/c/hire-a-pro/hire-help/74/other/latest.json'])
    expect(() => readDiscourseBoard(retain(input, 'https://community.make.com' + path + '?order=created'))).toThrow('category board');
});
test('board refuses malformed dates, duplicate and foreign topics, and unrelated source routes', () => {
  for (const patch of [{ created_at: 'yesterday' }, { created_at: '2026-02-30T00:00:00Z' },
    { created_at: '2026-10-09T00:00:00Z' }, { category_id: 14 }, { id: 1.5 },
    { archived: null }, { posts_count: -1 }]) {
    const b = board(); Object.assign(b.topic_list.topics[0], patch);
    expect(() => readDiscourseBoard(retain(b, boardUrl))).toThrow('invalid');
  }
  const repeated = board(); repeated.topic_list.topics.push(repeated.topic_list.topics[0]);
  expect(() => readDiscourseBoard(retain(repeated, boardUrl))).toThrow('duplicate');
  for (const url of ['https://forum.example/latest.json?order=created', boardUrl.replace('created', 'activity'),
    boardUrl + '&order=created', boardUrl + '&page=-1', boardUrl + '&target=https://other.example'])
    expect(() => readDiscourseBoard(retain(board(), url))).toThrow('category board');
  expect(() => readDiscourseBoard(retain(board(), boardUrl, { policy_decision: 'deny' }))).toThrow('permitted');
  const altered = retain(board(), boardUrl), receipt = JSON.parse(readFileSync(altered));
  writeFileSync(receipt.body_path, '{}');
  expect(() => readDiscourseBoard(altered)).toThrow('digest');
});
test('missing later posts remain explicit and applicants cannot become buyer evidence', () => {
  const result = readDiscourseThread(retain(topic()));
  expect(result.complete_for_retained_stream).toBe(false);
  expect(result.missing_post_ids).toEqual([103]);
  expect(result.original_poster_posts.map(p => p.id)).toEqual([101]);
  expect(result.next_observation_urls).toEqual(['https://forum.example/t/81/posts.json?post_ids%5B%5D=103']);
});
test('later buyer closure survives even when forum remains open and post counts differ', () => {
  const t = topic(); t.posts_count = 2;
  const base = retain(t), supplement = retain({ post_stream: { posts: [post(103, 12, 7, 'Position closed; stop emailing.')] } }, 'https://forum.example/t/81/posts.json?post_ids[]=103');
  const result = readDiscourseThread(base, [supplement]);
  expect(result.closed).toBe(false);
  expect(result.complete_for_retained_stream).toBe(true);
  expect(result.next_observation_urls).toEqual([]);
  expect(result.original_poster_posts.at(-1).html).toBe('Position closed; stop emailing.');
  expect(result.original_poster_posts.at(-1).url).toBe('https://forum.example/t/81/12');
  expect(result.original_poster_posts.at(-1).receipt).toBe(supplement);
});
test('altered, denied, wrong-topic and cross-origin observations are refused', () => {
  const base = retain(topic()), receipt = JSON.parse(readFileSync(base));
  writeFileSync(receipt.body_path, '{}');
  expect(() => readDiscourseThread(base)).toThrow('digest');
  expect(() => readDiscourseThread(retain(topic(), undefined, { policy_decision: 'deny' }))).toThrow('permitted');
  expect(() => readDiscourseThread(retain(topic(), 'https://forum.example/t/82.json'))).toThrow('identity');
  const valid = retain(topic());
  for (const url of ['https://other.example/t/81/posts.json?post_ids[]=103', 'https://forum.example/t/82/posts.json?post_ids[]=103'])
    expect(() => readDiscourseThread(valid, [retain({ post_stream: { posts: [post(103, 3)] } }, url)])).toThrow('another topic');
});
test('unrequested posts, missing identity and duplicate observations cannot fill coverage', () => {
  const base = retain(topic());
  const wrong = post(103, 3); wrong.topic_id = 82;
  expect(() => readDiscourseThread(base, [retain({ post_stream: { posts: [wrong] } }, 'https://forum.example/t/81/posts.json?post_ids[]=103')])).toThrow('foreign post');
  expect(() => readDiscourseThread(base, [retain({ post_stream: { posts: [post(104, 4)] } }, 'https://forum.example/t/81/posts.json?post_ids[]=103')])).toThrow('foreign post');
  expect(() => readDiscourseThread(base, [retain({ post_stream: { posts: [post(102, 2)] } }, 'https://forum.example/t/81/posts.json?post_ids[]=102')])).toThrow('duplicate');
  const t = topic(); delete t.details;
  expect(() => readDiscourseThread(retain(t))).toThrow('creator');
});
test('source HTML stays inert and exact reads reveal truncated original-poster text', () => {
  const t = topic(); t.post_stream.posts[0].cooked = '<script>globalThis.discourseExecuted=true</script>' + 'x'.repeat(5000);
  const base = retain(t), result = readDiscourseThread(base);
  expect(result.original_poster_posts[0].truncated).toBe(true);
  expect(result.original_poster_posts[0].html.length).toBe(4000);
  expect(readDiscourseThread(base, [], { post: 101 }).original_poster_posts[0].html).toBe(t.post_stream.posts[0].cooked);
  expect(globalThis.discourseExecuted).toBeUndefined();
  expect(() => readDiscourseThread(base, [], { post: 102 })).toThrow('original-poster');
});

test('installed native reader forwards exact paths and preserves receipt failures without opening a ledger', () => {
  const owner = join(import.meta.dir, '..'), prefix = join(dir, 'managed reader with spaces');
  const install = spawnSync('make', ['install-discourse-review', `MANAGED_PREFIX=${prefix}`], { cwd: owner, encoding: 'utf8' });
  expect(install.status).toBe(0);
  const binary = join(prefix, 'bin/bonfyre-source-frontier');
  const script = join(prefix, 'lib/source-frontier/discourse_thread.mjs');
  expect(readFileSync(script)).toEqual(readFileSync(join(owner, 'tools/discourse_thread.mjs')));
  const env = { ...process.env, BONFYRE_DISCOURSE_READER: script, BONFYRE_BUN_BINARY: process.execPath };
  const call = (args, patch = {}) => spawnSync(binary, args, { encoding: 'utf8', cwd: dir, env: { ...env, ...patch } });
  const base = retain(topic());
  const supplement = retain({ post_stream: { posts: [post(103, 12, 7, 'Position closed; stop emailing.')] } }, 'https://forum.example/t/81/posts.json?post_ids[]=103');
  const result = call(['discourse-review', '--receipt', base, '--supplement', supplement, '--post', '103']);
  expect(result.status).toBe(0);
  const review = JSON.parse(result.stdout);
  expect(review.complete_for_retained_stream).toBe(true);
  expect(review.original_poster_posts.map(p => p.id)).toEqual([103]);
  expect(review.original_poster_posts[0].html).toContain('stop emailing');
  const boardReceipt = retain(board(), boardUrl);
  const boardResult = call(['discourse-review', '--board', '--receipt', boardReceipt]);
  expect(boardResult.status).toBe(0);
  expect(JSON.parse(boardResult.stdout).topics.map(t => t.topic_id)).toEqual([2, 1]);
  const mixed = call(['discourse-review', '--receipt', boardReceipt, '--board', '--post', '101']);
  expect(mixed.status).toBe(1);
  expect(mixed.stderr).toContain('cannot select thread posts');
  const ledger = join(dir, 'must-not-exist.db');
  const bad = call(['discourse-review', '--receipt', base, '--db', ledger]);
  expect(bad.status).toBe(1);
  expect(bad.stdout).toBe('');
  expect(existsSync(ledger)).toBe(false);
  const altered = JSON.parse(readFileSync(base));
  writeFileSync(altered.body_path, '{}');
  const refused = call(['discourse-review', '--receipt', base]);
  expect(refused.status).toBe(1);
  expect(refused.stderr).toContain('digest mismatch');
  const missing = call(['discourse-review', '--receipt', base], { BONFYRE_DISCOURSE_READER: join(dir, 'missing.mjs') });
  expect(missing.status).toBe(2);
  expect(missing.stderr).toContain('not installed');
  const unknown = call(['discourse-reveiw', '--db', ledger]);
  expect(unknown.status).toBe(2);
  expect(unknown.stderr).toContain('discourse-review');
  expect(existsSync(ledger)).toBe(false);
});
