import { readFileSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';

const positive = n => Number.isSafeInteger(n) && n > 0;
function bounded(path, limit) {
  const stat = statSync(path);
  if (!stat.isFile() || stat.size > limit) throw Error('observation exceeds file budget');
  const bytes = readFileSync(path);
  if (bytes.length > limit) throw Error('observation changed beyond file budget');
  return bytes;
}
function observation(path) {
  const receipt = JSON.parse(bounded(path, 256 * 1024));
  if (receipt.schema !== 'bonfyre-foreign-observation.v1' || receipt.policy_decision !== 'allow' ||
      receipt.http_status !== 200 || receipt.observation_kind !== 'source_observed') throw Error('successful permitted observation required');
  const bytes = bounded(receipt.body_path, 8 * 1024 * 1024);
  if (createHash('sha256').update(bytes).digest('hex') !== receipt.content_sha256) throw Error('observation digest mismatch');
  const url = new URL(receipt.source_url);
  if (url.protocol !== 'https:' || url.username || url.password || url.hash) throw Error('public HTTPS source required');
  return { path, receipt, url, data: JSON.parse(bytes) };
}

// A board is a reading queue, not evidence that a topic offers paid work.
export function readDiscourseBoard(receiptPath) {
  const { path, receipt, url, data } = observation(receiptPath);
  const identity = url.pathname.match(/^\/c\/(?:[^/]+\/)+([1-9]\d*)\/l\/latest\.json$/);
  const params = [...url.searchParams];
  if (!identity || !positive(Number(identity[1])) ||
      params.some(([key, value]) => !((key === 'order' && value === 'created') ||
        (key === 'page' && /^(0|[1-9]\d*)$/.test(value) && Number.isSafeInteger(Number(value))))) ||
      new Set(params.map(([key]) => key)).size !== params.length || url.searchParams.get('order') !== 'created')
    throw Error('category board ordered by creation is required');
  const category = Number(identity[1]), list = data.topic_list;
  if (!Array.isArray(list?.topics) || list.topics.length > 1000) throw Error('invalid or oversized topic list');
  const stamp = value => typeof value === 'string' &&
    /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{3})?Z$/.test(value) &&
    Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value.replace(/(?<=:\d\d)Z$/, '.000Z');
  const seen = new Set();
  const topics = list.topics.map(topic => {
    if (!positive(topic.id) || seen.has(topic.id) || topic.category_id !== category ||
        typeof topic.title !== 'string' || !topic.title.length || topic.title.length > 4096 ||
        !stamp(topic.created_at) || !(topic.last_posted_at === null || stamp(topic.last_posted_at)) ||
        (topic.last_posted_at !== null && Date.parse(topic.last_posted_at) < Date.parse(topic.created_at)) ||
        typeof topic.closed !== 'boolean' || typeof topic.archived !== 'boolean' || !positive(topic.posts_count))
      throw Error('invalid or duplicate board topic');
    seen.add(topic.id);
    return { topic_id: topic.id, title: topic.title, topic_created_at: topic.created_at,
      topic_last_posted_at: topic.last_posted_at, posts_count: topic.posts_count,
      closed: topic.closed, archived: topic.archived,
      observation_url: `${url.origin}/t/${topic.id}.json` };
  }).sort((a, b) => Date.parse(b.topic_created_at) - Date.parse(a.topic_created_at) || b.topic_id - a.topic_id);
  // Retain pagination as evidence only: a server-provided link is not a fetch instruction.
  return { schema: 'bonfyre.discourse-board-review.v1', source: url.href, category_id: category,
    receipt: path, observed_at: receipt.observed_at, sha256: receipt.content_sha256,
    retained_topics: topics.length, topics, source_more_topics_url: list.more_topics_url ?? null,
    cannot_infer: 'A board title, creation date, reply count or open topic does not establish buyer demand, current hiring, budget or permission to contact. This is one retained page, not the complete board. Observe each relevant topic and read its original poster updates with discourse-review.' };
}

// Read retained public observations only. Missing posts produce a reading plan,
// never a network request or an inference that the buyer is still hiring.
export function readDiscourseThread(receiptPath, supplementPaths = [], { post = null } = {}) {
  if (supplementPaths.length > 100) throw Error('too many supplements');
  if (post !== null && !positive(post)) throw Error('post must be a positive post ID');
  const base = observation(receiptPath), topic = base.data;
  const identity = base.url.pathname.match(/^\/t\/(?:[^/]+\/)?([1-9]\d*)\.json$/);
  if (!identity || base.url.search || !positive(topic.id) || Number(identity[1]) !== topic.id) throw Error('topic identity mismatch');
  const creator = topic.details?.created_by;
  const stream = topic.post_stream?.stream;
  if (!positive(creator?.id) || typeof creator.username !== 'string' || !Array.isArray(stream) || !stream.length ||
      stream.length > 100000 || stream.some(id => !positive(id)) || new Set(stream).size !== stream.length) throw Error('invalid topic creator or post stream');
  if (typeof topic.closed !== 'boolean' || typeof topic.archived !== 'boolean') throw Error('topic standing is absent');
  const expected = new Set(stream), posts = new Map(), receipts = [];
  for (const source of [base, ...supplementPaths.map(observation)]) {
    let requested = expected;
    if (source !== base) {
      if (source.url.origin !== base.url.origin || source.url.pathname !== `/t/${topic.id}/posts.json`)
        throw Error('supplement belongs to another topic or origin');
      const params = [...source.url.searchParams];
      if (!params.length || params.some(([k, v]) => k !== 'post_ids[]' || !/^[1-9]\d*$/.test(v) || !expected.has(Number(v))))
        throw Error('supplement must request retained stream IDs');
      requested = new Set(params.map(([, v]) => Number(v)));
    }
    if (!Array.isArray(source.data.post_stream?.posts)) throw Error('posts are absent');
    for (const item of source.data.post_stream.posts) {
      if (!positive(item.id) || !expected.has(item.id) || !requested.has(item.id) || item.topic_id !== topic.id ||
          !positive(item.user_id) || !positive(item.post_number) || typeof item.cooked !== 'string' ||
          typeof item.username !== 'string' || typeof item.created_at !== 'string') throw Error('invalid or foreign post');
      if (posts.has(item.id)) throw Error('duplicate post observation; use nonoverlapping supplements');
      posts.set(item.id, { id: item.id, post_number: item.post_number, user_id: item.user_id,
        username: item.username, created_at: item.created_at, updated_at: item.updated_at ?? null,
        html: item.cooked, receipt: source.path, url: `${base.url.origin}/t/${topic.id}/${item.post_number}` });
    }
    receipts.push({ path: source.path, observed_at: source.receipt.observed_at, sha256: source.receipt.content_sha256 });
  }
  const first = [...posts.values()].find(p => p.post_number === 1);
  if (!first || first.user_id !== creator.id) throw Error('original post does not establish the topic creator');
  const missing = stream.filter(id => !posts.has(id));
  const next = [];
  for (let i = 0; i < missing.length; i += 20) {
    const url = new URL(`/t/${topic.id}/posts.json`, base.url);
    for (const id of missing.slice(i, i + 20)) url.searchParams.append('post_ids[]', String(id));
    next.push(url.href);
  }
  const authored = [...posts.values()].filter(p => p.user_id === creator.id).sort((a, b) => a.post_number - b.post_number);
  if (post !== null && !authored.some(p => p.id === post)) throw Error('requested post is not a retained original-poster post');
  return { schema: 'bonfyre.discourse-thread-review.v1', source: base.url.href, topic_id: topic.id,
    title: topic.title, creator, closed: topic.closed, archived: topic.archived,
    topic_created_at: topic.created_at, topic_last_posted_at: topic.last_posted_at,
    stream_posts: stream.length, retained_posts: posts.size, complete_for_retained_stream: missing.length === 0,
    missing_post_ids: missing, next_observation_urls: next, receipts,
    original_poster_posts: authored.filter(p => post === null || p.id === post).map(p => ({ ...p,
      html: post === null ? p.html.slice(0, 4000) : p.html, truncated: post === null && p.html.length > 4000 })),
    cannot_infer: 'Forum-open state, recent replies, complete retained stream coverage or other applicants\' claims do not establish an open job, buyer interest, our credentials or permission to contact. Read the original poster\'s updates. HTML is inert source text.' };
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), supplements = [];
    let receipt, post = null, board = false;
    for (let i = 0; i < args.length;) {
      if (args[i] === '--board' && !board) { board = true; i++; continue; }
      if (args[i + 1] === undefined) throw Error('missing argument');
      if (args[i] === '--receipt' && !receipt) receipt = args[i + 1];
      else if (args[i] === '--supplement') supplements.push(args[i + 1]);
      else if (args[i] === '--post' && post === null) post = Number(args[i + 1]);
      else throw Error('usage: --receipt FILE [--board | --supplement FILE ... --post ID]');
      i += 2;
    }
    if (!receipt) throw Error('--receipt is required');
    if (board && (supplements.length || post !== null)) throw Error('--board cannot select thread posts or supplements');
    console.log(JSON.stringify(board ? readDiscourseBoard(receipt) : readDiscourseThread(receipt, supplements, { post }), null, 2));
  } catch (error) { console.error(`discourse thread: ${error.message}`); process.exitCode = 1; }
}
