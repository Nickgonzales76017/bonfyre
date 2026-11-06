import assert from "node:assert/strict";
import { discover, hostOf, PaymentRequired, search } from "../tools/discover_with_exa.mjs";

const args = { out: "/not-used", family: "", subjects: "city library events", queries: 10, perQuery: 25, pause: 0 };
const off = await discover(args, { env: {}, searchFn: () => { throw Error("must not call provider"); } });
assert.equal(off.report.asked, 0);
assert.equal(off.write, false);
const noKey = await discover(args, { env: { BONFYRE_EXTERNAL_READS_ENABLED: "1" }, searchFn: () => { throw Error("must not call provider"); } });
assert.equal(noKey.write, false);
assert.equal(hostOf("https://www.example.org/feed"), "example.org");
assert.equal(hostOf("https://bad host/"), "");

const queries = [];
const fixture = async (_key, query) => {
  queries.push(query);
  return [
    { url: "https://www.example.org/feed.xml", title: "RSS" },
    { url: "https://www.example.org/about", title: "Publisher" },
    { url: "https://elsewhere.test/calendar.ics", title: "Calendar" },
    { url: "https://elsewhere.test/login", title: "Private" },
    { url: "https://elsewhere.test/photo.jpg", title: "Photo" },
    { url: "not a URL", title: "Invalid" },
  ];
};
const result = await discover(args, { env: { BONFYRE_EXTERNAL_READS_ENABLED: "1", EXAAI_API_KEY: "test",
  BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN: "25" }, searchFn: fixture, sleepFn: async () => {} });
assert.deepEqual(queries, ["city library events"]);
assert.equal(result.report.queries_asked, 1);
assert.deepEqual(result.rows.map(row => [row.address, row.protocol, row.truth_level]), [
  ["https://www.example.org/feed.xml", "rss", "observed_index_candidate"],
  ["https://example.org/", "unknown", "listed"],
  ["https://elsewhere.test/calendar.ics", "ics", "observed_index_candidate"],
]);
assert.ok(result.rows.every(row => row.discovered_from.includes("city library events")));

const cappedQueries = [];
const capped = await discover({ ...args, subjects: "first; second; third", queries: 3, perQuery: 25 }, {
  env: { BONFYRE_EXTERNAL_READS_ENABLED: "1", EXAAI_API_KEY: "test", BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN: "25" },
  searchFn: async (_key, query) => { cappedQueries.push(query); return []; }, sleepFn: async () => {},
});
assert.equal(capped.report.queries_asked, 1);
assert.deepEqual(cappedQueries, ["first"]);

const paid = await discover(args, { env: { BONFYRE_EXTERNAL_READS_ENABLED: "1", EXAAI_API_KEY: "test" },
  searchFn: async () => { throw new PaymentRequired("credit exhausted"); } });
assert.equal(paid.report.provider_state, "payment_required");
assert.equal(paid.report.queries_asked, 1);
assert.equal(paid.report.addresses_listed, 0);
assert.equal(paid.write, true);
await assert.rejects(search("test", "q", 1, "keyword", async () => new Response("pay", { status: 402 })), PaymentRequired);
console.log("Exa discovery governance and candidate truth: PASS");
