// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import { afterEach, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { priceFloatRepr, routeCandidate } from "../tools/economic-views.mjs";

const project = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const temporary = [];
afterEach(() => { for (const path of temporary.splice(0)) rmSync(path, { recursive: true, force: true }); });
const sha = value => createHash("sha256").update(value).digest("hex");
function estate() {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-economics-"));
  temporary.push(root);
  const dir = join(root, "estate"), bodies = join(root, "bodies");
  mkdirSync(dir); mkdirSync(bodies);
  const db = new Database(join(dir, "estate.db"));
  db.exec("CREATE TABLE occurrence(id TEXT PRIMARY KEY, primary_url TEXT, source_kind TEXT); CREATE TABLE occurrence_term(occurrence_id TEXT, axis TEXT, term TEXT);");
  return { root, dir, bodies, db };
}
function shell(name, env, args = []) {
  const result = Bun.spawnSync([join(project, "tools", name), ...args], { env: { ...process.env, ...env } });
  if (result.exitCode !== 0) throw new Error(`${name} failed: ${result.stderr.toString()}`);
  return JSON.parse(result.stdout.toString());
}

test("price-book shell entrypoint reads only retained bytes, keeps units and evidence identity", () => {
  const { dir, bodies, db } = estate();
  const occurrences = ["quote", "base", "free", "prose", "missing"];
  for (const id of occurrences) db.query("INSERT INTO occurrence VALUES (?, ?, 'x402')").run(id, `https://${id}.example`);
  db.close();
  const quote = Buffer.from('{"price":"$0.005","humanAmount":"0.02","amount":"20000","free":{"price":"0"}}');
  writeFileSync(join(bodies, "quote.body"), quote);
  writeFileSync(join(bodies, "base.body"), '{"amount":"20000"}');
  writeFileSync(join(bodies, "free.body"), '{"price":"0"}');
  writeFileSync(join(bodies, "prose.body"), 'From $0.005 to $0.10 per call');
  const out = join(dir, "prices.jsonl");
  const summary = shell("price-book.sh", { ESTATE_DIR: dir, BODIES: bodies }, [out]);
  expect(summary).toMatchObject({ metered_sources: 5, quoted: 2, publishes_a_free_tier: 2,
    prose_price_not_parsed: 1, no_published_price: 1, spend: 0 });
  const rows = readFileSync(out, "utf8").trim().split("\n").map(JSON.parse);
  const byId = Object.fromEntries(rows.map(row => [row.occurrence_id, row]));
  expect(Object.keys(byId).sort()).toEqual(["base", "quote"]);
  expect(byId.quote).toMatchObject({ min_price_usd: 0.005, max_price_usd: 0.02,
    free_tier_published: true, basis_sha256: sha(quote), spend: 0 });
  expect(byId.quote.evidence_sha256).toBe(sha("quote\x1f[0.005, 0.02]\x1fquoted"));
  expect(byId.base.min_price_usd).toBe(0.02);
  expect(byId.base.evidence_sha256).toBe(sha("base\x1f[0.02]\x1fquoted"));
  expect(byId.quote.cannot_infer).toContain("quoted -> committed, paid, settled or reconciled");
});

test("route-arbitrage shell entrypoint registers only non-thin proven-free native routes", () => {
  const { root, dir, db } = estate();
  for (const term of ["search", "thin", "bad", "failed"])
    for (let i = 0; i < 40; i++) db.query("INSERT INTO occurrence_term VALUES (?, 'affordance', ?)").run(`${term}-${i}`, term);
  db.close();
  const plane = join(root, "plane"), finance = join(root, "finance"), log = join(root, "finance.log");
  writeFileSync(plane, `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = --need ]; then need=$2; break; fi
  shift
done
case "$need" in
  affordance:search) printf '%s\\n' '{"free_and_proven":1,"metered":4,"thin_market":false,"route":"external:free","avoided_cost_usd":{"median":0.005}}' ;;
  affordance:thin) printf '%s\\n' '{"free_and_proven":1,"metered":3,"thin_market":true,"route":"external:free","avoided_cost_usd":{"median":149}}' ;;
  affordance:bad) printf '%s\\n' '{"free_and_proven":0,"metered":4,"thin_market":false,"route":"external:free","avoided_cost_usd":{"median":0.5}}' ;;
  *) exit 1 ;;
esac
`);
  writeFileSync(finance, `#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_FINANCE_LOG"
`);
  chmodSync(plane, 0o755); chmodSync(finance, 0o755);
  const summary = shell("route-arbitrage.sh", { ESTATE_DIR: dir, BONFYRE_EXTERNAL_ESTATE_BINARY: plane,
    BONFYRE_FINANCE_BINARY: finance, MOCK_FINANCE_LOG: log });
  expect(summary).toMatchObject({ needs_considered: 4, services_registered: 1, skipped: 2, failed_route_reads: 1,
    truth: "spreads_registered_for_ranking_not_revenue" });
  const calls = readFileSync(log, "utf8").trim().split("\n");
  expect(calls).toHaveLength(1);
  expect(calls[0]).toContain("--name affordance-search --buy 0 --sell 0.005000 --source external:free");
  expect(summary.cannot_infer).toContain("a spread -> revenue; nobody was charged and nobody paid");
});

test("price-book entrypoint refuses unsafe base-unit amounts instead of misquoting", () => {
  const { dir, bodies, db } = estate();
  db.query("INSERT INTO occurrence VALUES ('huge', 'https://huge.example', 'x402')").run();
  db.close();
  writeFileSync(join(bodies, "huge.body"), '{"amount":"9007199254740992"}');
  const out = join(dir, "prices.jsonl");
  const result = Bun.spawnSync([join(project, "tools/price-book.sh"), out], {
    env: { ...process.env, ESTATE_DIR: dir, BODIES: bodies },
  });
  expect(result.exitCode).not.toBe(0);
  expect(result.stderr.toString()).toContain("base-unit price exceeds exact integer range");
});

test("price-book refuses symlink and FIFO bodies without replacing a prior report", () => {
  const { root, dir, bodies, db } = estate();
  db.query("INSERT INTO occurrence VALUES ('unsafe', 'https://unsafe.example', 'x402')").run();
  db.close();
  const out = join(dir, "price-book.jsonl"), body = join(bodies, "unsafe.body");
  writeFileSync(out, "prior-valid-report\n");
  const target = join(root, "target.body");
  writeFileSync(target, '{"price":"$0.02"}');
  symlinkSync(target, body);
  const run = () => Bun.spawnSync([join(project, "tools/price-book.sh"), out], {
    env: { ...process.env, ESTATE_DIR: dir, BODIES: bodies },
  });
  const symlink = run();
  expect(symlink.exitCode).not.toBe(0);
  expect(readFileSync(out, "utf8")).toBe("prior-valid-report\n");
  rmSync(body);
  const fifo = Bun.spawnSync(["mkfifo", body]);
  expect(fifo.exitCode).toBe(0);
  const pipe = run();
  expect(pipe.exitCode).not.toBe(0);
  expect(pipe.stderr.toString()).toContain("not a regular file");
  expect(readFileSync(out, "utf8")).toBe("prior-valid-report\n");
});

test("candidate validation and legacy float spellings refuse malformed or unpriced routes", () => {
  const good = { free_and_proven: 1, metered: 4, thin_market: false,
    route: "external:free", avoided_cost_usd: { median: 0.02 } };
  expect(routeCandidate(good)).toEqual({ source: "external:free", sell: "0.020000" });
  expect(routeCandidate({ ...good, thin_market: true })).toBeNull();
  expect(routeCandidate({ ...good, metered: 3 })).toBeNull();
  expect(routeCandidate({ ...good, avoided_cost_usd: { median: 0 } })).toBeNull();
  expect(routeCandidate({ ...good, avoided_cost_usd: { median: 0.0000001 } })).toBeNull();
  expect(routeCandidate({ ...good, route: "" })).toBeNull();
  expect(priceFloatRepr(1)).toBe("1.0");
  expect(priceFloatRepr(0.00001)).toBe("1e-05");
  expect(priceFloatRepr(1e16)).toBe("1e+16");
});
