import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { frontierUrls, statedPrice, sweep } from "../sweep-rates.mjs";

test("only positive, stated HTTP 402 prices are considered and base units cannot become rates", () => {
  expect(statedPrice(200, new Headers(), '{"amount":"2","currency":"USD"}')).toBeNull();
  expect(statedPrice(402, new Headers(), '{"amount":"0","currency":"USD"}')).toBeNull();
  expect(statedPrice(402, new Headers(), '{"amount":"2","currency":"USD"}')).toMatchObject({
    amount: 2, currency: "USD", field: "amount", admissible: true });
  expect(statedPrice(402, new Headers(), '{"maxAmountRequired":"1000000","asset":"0xabcdef"}')).toMatchObject({
    amount: 1000000, field: "maxAmountRequired", admissible: false });
  expect(statedPrice(402, new Headers(), '{"amount":"1000000","currency":"USDC"}').admissible).toBe(false);
});

test("one shared robots gate scopes 402 discovery and Meter refusal is not counted as admission", async () => {
  const urls = ["https://one.test/api/paid", "https://one.test/api/unpriced", "https://blocked.test/api/paid"];
  const base = mkdtempSync(join(tmpdir(), "bonfyre-rate-native-"));
  try {
    let robots = 0, meterCalls = 0;
    const fetcher = async url => {
      const target = new URL(url);
      if (target.pathname === "/robots.txt") {
        robots++;
        return new Response(target.hostname === "blocked.test" ? "User-agent: *\nDisallow: /api/\n" : "User-agent: *\nAllow: /\n");
      }
      if (target.pathname === "/api/paid") return new Response('{"amount":"2","currency":"USD"}', { status: 402 });
      return new Response("not priced", { status: 200 });
    };
    const result = await sweep({ urls, admit: true, meterBinary: join(base, "meter"), fetcher,
      runner: () => { meterCalls++; return { exitCode: 0 }; }, today: "2026-09-29" });
    expect(robots).toBe(2);
    expect(result.asked).toBe(2);
    expect(result.declined_by_robots).toBe(1);
    expect(result.answered_without_a_stated_price).toBe(1);
    expect(result.stated_a_price).toBe(1);
    expect(result.admitted).toBe(0);
    expect(result.admission_refused[0].reason).toBe("native Meter unavailable");
    expect(meterCalls).toBe(0);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("the frontier query is protocol-scoped and parameterized", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-rate-frontier-"));
  try {
    const path = join(base, "frontier.db"), db = new Database(path);
    db.exec("CREATE TABLE channel(protocol TEXT, exact_address TEXT, state TEXT)");
    db.query("INSERT INTO channel VALUES(?,?,?)").run("x402", "https://paid.test/api/", "admitted");
    db.query("INSERT INTO channel VALUES(?,?,?)").run("rss", "https://feed.test/rss", "admitted");
    db.close();
    expect(frontierUrls(path, 10)).toEqual(["https://paid.test/api/"]);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("Meter admission count follows the native command result, not the 402 count", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-rate-meter-"));
  try {
    const meterBinary = join(base, "meter");
    writeFileSync(meterBinary, "fixture");
    const fetcher = async url => new URL(url).pathname === "/robots.txt"
      ? new Response("User-agent: *\nAllow: /\n")
      : new Response('{"amount":3,"currency":"USD"}', { status: 402 });
    const args = { urls: ["https://one.test/api/paid"], admit: true, meterBinary, fetcher };
    let command;
    const failed = await sweep({ ...args, runner: argv => { command = argv; return { exitCode: 4, stderr: Buffer.from("not admitted") }; } });
    expect(failed.stated_a_price).toBe(1);
    expect(failed.admitted).toBe(0);
    expect(failed.admission_refused[0].reason).toBe("not admitted");
    expect(command.slice(0, 2)).toEqual([meterBinary, "admit-rate"]);
    const accepted = await sweep({ ...args, runner: () => ({ exitCode: 0 }) });
    expect(accepted.admitted).toBe(1);
  } finally { rmSync(base, { recursive: true, force: true }); }
});
