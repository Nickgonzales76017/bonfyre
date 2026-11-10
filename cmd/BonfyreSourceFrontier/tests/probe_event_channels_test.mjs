import { test, expect } from "bun:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { judge, probe } from "../tools/probe_event_channels.mjs";

const calendar = "BEGIN:VCALENDAR\n" + [1, 2, 3].map(i => `BEGIN:VEVENT\nDTSTART:2026100${i}\nSUMMARY:Event ${i}\nEND:VEVENT\n`).join("") + "END:VCALENDAR";

test("calendar requires dated titled upcoming events", () => {
  expect(judge(calendar, "20260929")[0]).toMatchObject({ events: 3, upcoming: 3 });
  expect(judge(calendar, "20270101")[1]).toContain("nothing upcoming");
  expect(judge("BEGIN:VCALENDAR\nBEGIN:VEVENT\nEND:VEVENT", "20260929")[1]).toBe("fewer than three events");
});

test("CMS event row exists only after canonical ForeignTwin body is retained", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-event-channel-"));
  try {
    const foreign = join(dir, "foreign"), hosts = join(dir, "hosts.txt"), out = join(dir, "rows.json"),
      channels = join(dir, "channels.jsonl"), evidence = join(dir, "evidence.jsonl");
    mkdirSync(join(foreign, "receipts"), { recursive: true }); mkdirSync(join(foreign, "bodies"), { recursive: true });
    writeFileSync(hosts, "calendar.test|City Library|Q123\n");
    const seen = [];
    const observeFn = async (_project, _foreign, twin, url) => {
      seen.push(twin);
      const receiptPath = join(foreign, "receipts", `${twin}.fresh.json`), bodyPath = join(foreign, "bodies", `${twin}.fresh.body`);
      const receipt = { policy_decision: "allow", http_status: 200, body_path: bodyPath };
      writeFileSync(receiptPath, JSON.stringify(receipt) + "\n"); writeFileSync(bodyPath, calendar);
      return { receipt, text: calendar, receiptPath, bodyPath };
    };
    const result = await probe({ project: "project", foreign, hosts, out, channels, evidence, limit: 1 },
      { observeFn, sleepFn: async () => {}, today: "20260929" });
    expect(result).toEqual({ hosts: 1, calendars: 1 });
    expect(seen).toHaveLength(2);
    expect(seen[1]).toBe("events-calendar-test");
    const row = JSON.parse(readFileSync(out, "utf8"))[0];
    expect(row).toMatchObject({ source: "events-calendar-test", twin: "events-calendar-test", publisher_ident: "Q123" });
    const canonical = JSON.parse(readFileSync(join(foreign, "receipts", "events-calendar-test.json"), "utf8"));
    expect(readFileSync(canonical.body_path, "utf8")).toBe(calendar);
    expect(JSON.parse(readFileSync(channels, "utf8"))).toMatchObject({ state: "eligible", census_source: row.source });
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("failed canonical observation cannot admit a probe-only calendar", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-event-channel-refusal-"));
  try {
    const foreign = join(dir, "foreign"), hosts = join(dir, "hosts.txt"), out = join(dir, "rows.json"), evidence = join(dir, "evidence.jsonl");
    writeFileSync(hosts, "calendar.test\n");
    const result = await probe({ project: "project", foreign, hosts, out, evidence, limit: 1 }, {
      observeFn: async (_project, _foreign, twin) => ({ receipt: { policy_decision: "allow", http_status: 200 },
        text: twin.startsWith("calendar-") ? calendar : "", receiptPath: join(dir, "missing.json"), bodyPath: join(dir, "missing.body") }),
      sleepFn: async () => {}, today: "20260929",
    });
    expect(result.calendars).toBe(0);
    expect(JSON.parse(readFileSync(out, "utf8"))).toEqual([]);
    expect(readFileSync(evidence, "utf8")).toContain("canonical observation");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
