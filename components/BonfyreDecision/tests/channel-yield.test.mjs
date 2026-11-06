// CheapCognition over SourceFrontier candidates: the replay never lets a record
// see outcomes recorded after it, the owner is bound to its weight bytes, a
// shadow receipt is an ordinary validated DecisionPlane receipt, and the value
// of skipping is measured against the qualifier's own timed outcomes.
import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { addOutcome, channelCase, emptyHistory, evaluateShadow, features, frontierHistory, logisticOwner,
  ordering, rankWork, refusalClass, replay, trainLogistic, yielded } from '../channel-yield.mjs';
import { assessDecisionCases, validateDecisionReceipt } from '../decision-plane.mjs';

test('refusals fold into outcome classes, and a page that declares channels still yielded', () => {
  expect(refusalClass('answered nothing')).toBe('silent');
  expect(refusalClass('robots or observe policy: robots_disallow')).toBe('policy');
  expect(refusalClass('fewer than three items; it declares 4 channels of its own')).toBe('thin');
  expect(refusalClass('published in de')).toBe('language');
  expect(yielded({ refused: 'fewer than three items; it declares 2 channels of its own' })).toBe(true);
  expect(yielded({ admitted: 'feed-x' })).toBe(true);
  expect(yielded({ refused: 'answered nothing' })).toBe(false);
});

test('the replay judges each record with only the history that existed before it', () => {
  const rows = new Map();
  const records = [{ address: 'https://a.example/feed', refused: 'answered nothing' },
    { address: 'https://a.example/rss', admitted: 'feed-a' }, { address: 'https://a.example/atom', refused: 'answered nothing' }];
  const out = replay(records, rows);
  expect(out.map(item => item.state.evidence.publisher_observed)).toEqual([0, 1, 2]);
  expect(out.map(item => item.state.evidence.publisher_yielded)).toEqual([0, 0, 1]);
  expect(out[2].state.evidence.publisher_outcomes).toEqual({ silent: 1, yield: 1 });
});

test('weights trained twice on the same examples are the same bytes, and they separate a dead publisher', () => {
  const examples = [];
  for (let i = 0; i < 400; i++) {
    const dead = i % 2 === 0;
    examples.push({ features: features({ channel: { address: `https://${dead ? 'dead' : 'live'}${i}.example/feed`, protocol: 'rss',
      material: 'publication', truth_level: 'listed', discovered_from: '' },
      evidence: { publisher_observed: 5, publisher_yielded: dead ? 0 : 3, publisher_outcomes: dead ? { silent: 5 } : { yield: 3, thin: 2 } } }),
      label: !dead && i % 4 !== 1 });
  }
  const a = JSON.stringify(trainLogistic(examples)), b = JSON.stringify(trainLogistic(examples));
  expect(a).toBe(b);
  const model = JSON.parse(a), score = keys => 1 / (1 + Math.exp(-keys.reduce((z, k) => z + (model.weights[k] || 0), model.bias)));
  expect(score(examples[0].features)).toBeLessThan(score(examples[1].features));
  expect(ordering([{ p: 0.9, label: true }, { p: 0.1, label: false }, { p: 0.2, label: false }, { p: 0.8, label: true }], [0.5]))
    .toEqual([{ observation_budget_share: 0.5, yields_found_ranked: 1, yields_found_as_run: 0.5 }]);
});

test('a shadow assessment is a validated DecisionPlane receipt bound to the weight file, and its value is measured on timed outcomes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'channel-yield-'));
  try {
    const db = new Database(join(dir, 'frontier.db'));
    db.run(`CREATE TABLE channel(channel_id TEXT PRIMARY KEY, exact_address TEXT, protocol TEXT, material TEXT, truth_level TEXT,
      discovered_from TEXT, state TEXT, refusal TEXT DEFAULT '', census_source TEXT DEFAULT '')`);
    db.run("INSERT INTO channel VALUES ('old','https://dead.example/a','rss','publication','listed','','candidate','answered nothing','')");
    db.run("INSERT INTO channel VALUES ('c1','https://dead.example/b','rss','publication','listed','','candidate','','')");
    db.run("INSERT INTO channel VALUES ('c2','https://live.example/feed','rss','publication','listed','','shadow','','')");
    const history = frontierHistory(db);
    expect(history.get('dead.example')).toMatchObject({ tried: 1, yielded: 0 });
    const rows = db.query("SELECT * FROM channel WHERE channel_id IN ('c1','c2') ORDER BY channel_id").all();
    db.close();
    const model = trainLogistic([{ features: ['pub_mostly=silent'], label: false }, { features: ['pub_observed=0'], label: true },
      { features: ['pub_mostly=silent'], label: false }]);
    writeFileSync(join(dir, 'model.json'), JSON.stringify(model));
    const owner = logisticOwner(join(dir, 'model.json'));
    const cases = rows.map(row => channelCase(row, history, { historyDigest: 'h' }));
    const { receipts } = await assessDecisionCases(cases, { localEvaluator: owner, execute: true, maxCalls: 2 });
    for (const [index, receipt] of receipts.entries()) {
      expect(receipt).toMatchObject({ state: 'diagnostic', action: 'none', provider: 'local', model: 'channel-yield-logistic',
        model_artifact_digest: owner.artifact_digest });
      expect(() => validateDecisionReceipt(cases[index], receipt)).not.toThrow();
    }
    expect(cases[0].contract).toMatchObject({ owner: 'SourceFrontier', gate_state: 'SHADOW', information_policy_class: 'public' });
    const [dead, live] = receipts.map(receipt => receipt.answers.yields.noul);
    expect(dead).toBeLessThan(live);
    const shadows = cases.map((item, index) => ({ channel_id: item.contract.frontier[0].id, case: item, receipt: receipts[index] }));
    const evidence = [{ channel_id: 'c1', observe_ms: 3000, refused: 'answered nothing' },
      { channel_id: 'c2', observe_ms: 900, admitted: 'feed-live' }];
    const report = evaluateShadow({ shadows, evidence, threshold: (dead + live) / 2, minimumCases: 2, maximumErrorRate: 0.99 });
    expect(report.matched).toBe(2);
    expect(report.selective_value).toMatchObject({ cases: 2, accepted: 1, errors: 0, baseline_ms: 4700 });
    expect(report.selective_value.net_saved_ms).toBeGreaterThan(3000);
    expect(report.outcomes.groups[0].cases).toBe(2);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('ranking rewrites only WorkGraph priority and keeps every seed line', () => {
  const lines = ['source-qualify:a\tsource.qualify\tbonfyre://source-frontier/channel/a\tsource-qualify\t15000\t4\t30000',
    'source-qualify:b\tsource.qualify\tbonfyre://source-frontier/channel/b\tsource-qualify\t15000\t4\t30000',
    'source-qualify:unknown\tsource.qualify\tx\tsource-qualify\t12000\t4\t30000'];
  const ranked = rankWork(lines, new Map([['a', 0.002], ['b', 0.4]]));
  expect(ranked.length).toBe(3);
  expect(ranked.map(line => line.split('\t')[4])).toEqual(['12000', '410000', '12000']);
  expect(ranked.map(line => line.split('\t').filter((_, i) => i !== 4).join('\t')))
    .toEqual(lines.map(line => line.split('\t').filter((_, i) => i !== 4).join('\t')));
});
