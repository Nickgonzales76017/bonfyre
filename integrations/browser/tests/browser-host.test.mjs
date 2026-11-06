// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { test, expect } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHost, frame, LIMITS, SELECTION_BYTES, twinFor, VERSION } from "../host/bonfyre-browser-host.mjs";

const hostPath = join(import.meta.dir, "../host/bonfyre-browser-host.mjs");
const project = join(import.meta.dir, "../../../components/BonfyreProject/bonfyre-project");

// Speaks the browser's side of the wire to a real host process.
async function session(env, messages, { raw } = {}) {
  const proc = Bun.spawn(["bun", hostPath], { stdin: "pipe", stdout: "pipe", stderr: "ignore", env: { ...process.env, ...env } });
  for (const message of messages) proc.stdin.write(frame(message));
  if (raw) proc.stdin.write(raw);
  proc.stdin.end();
  const bytes = Buffer.from(await new Response(proc.stdout).arrayBuffer());
  await proc.exited;
  const replies = [];
  for (let at = 0; at + 4 <= bytes.length;) { const size = bytes.readUInt32LE(at); replies.push(JSON.parse(bytes.subarray(at + 4, at + 4 + size).toString("utf8"))); at += 4 + size; }
  return { replies, code: proc.exitCode };
}

test("the host frames replies as the browser expects and says what it carries", async () => {
  const { replies } = await session({}, [{ id: 1, operation: "hello" }, { id: 2, operation: "delete_everything" }, "not an object"]);
  expect(replies[0]).toMatchObject({ id: 1, ok: true, host: VERSION });
  expect(Object.keys(replies[0].operations)).toEqual(["hello", "provider_admit", "collected", "kept", "history", "observe", "capture", "hold", "enroll_status", "enroll_list", "enroll_inspect", "enroll_continue", "enroll_resume", "enroll_record", "enroll_wait", "enroll_decide", "enroll_seal", "enroll_done"]);
  for (const name of ["provider_admit", "observe", "capture", "hold", "enroll_continue", "enroll_record", "enroll_wait", "enroll_decide", "enroll_seal", "enroll_done"]) expect(replies[0].operations[name].needs_confirmation).toBe(true);
  expect(replies[0].operations.observe.needs_confirmation).toBe(true);
  expect(replies[1]).toMatchObject({ id: 2, ok: false, refused: "unknown_operation" });
  expect(replies[2]).toMatchObject({ ok: false, refused: "not_an_envelope" });
});

test("only a public https address is carried, and keeping a page needs the person's ask", async () => {
  const calls = [];
  const host = createHost({ tools: { call: async (name, args) => { calls.push([name, args]); return { schema_version: "stub" }; } } });
  for (const url of ["http://example.com/", "file:///etc/passwd", "https://user:secret@example.com/", "chrome://settings", "", "javascript:alert(1)"])
    expect((await host.answer({ id: 1, operation: "kept", url })).ok).toBe(false);
  expect(await host.answer({ id: 2, operation: "observe", url: "https://example.com/a" })).toMatchObject({ ok: false, refused: "not_confirmed" });
  expect(await host.answer({ id: 3, operation: "observe", url: "https://example.com/a", confirmed: "yes" })).toMatchObject({ ok: false, refused: "not_confirmed" });
  expect(calls).toHaveLength(0);
  expect(await host.answer({ id: 4, operation: "observe", url: "https://www.Example.com/a", confirmed: true })).toMatchObject({ ok: true });
  expect(calls).toEqual([["foreign_twin_observe", { twin_id: "example.com", url: "https://www.example.com/a" }]]);
  expect(twinFor("https://shop.example.co.uk/x?y=1")).toBe("shop.example.co.uk");
});

test("a session is bounded and an owner's refusal is carried, not hidden", async () => {
  const host = createHost({ tools: { call: async () => { throw new Error("twin_id must be a short slug"); } } });
  expect(await host.answer({ id: 1, operation: "kept", url: "https://example.com/" })).toMatchObject({ ok: false, refused: "owner_refused", reason: "twin_id must be a short slug" });
  const quiet = createHost({ tools: { call: async () => ({}) } });
  for (let i = 0; i < LIMITS.operations; i++) expect((await quiet.answer({ id: i, operation: "hello" })).ok).toBe(true);
  expect(await quiet.answer({ id: 99, operation: "hello" })).toMatchObject({ ok: false, refused: "session_spent" });
});

test("a reply the browser would refuse is cut to its receipt, and an oversized envelope ends the session", async () => {
  const host = createHost({ tools: { call: async () => ({ schema_version: "big", receipt_path: "/r.json", retention: { retained: true }, body: "x".repeat(LIMITS.outbound_bytes + 10) }) } });
  const reply = await host.answer({ id: 1, operation: "kept", url: "https://example.com/" });
  expect(reply).toMatchObject({ ok: true, truncated: true, result: { receipt_path: "/r.json", retention: { retained: true } } });
  expect(Buffer.byteLength(JSON.stringify(reply))).toBeLessThan(4096);
  const head = Buffer.alloc(4); head.writeUInt32LE(LIMITS.inbound_bytes + 1);
  const { replies, code } = await session({}, [{ id: 1, operation: "hello" }], { raw: head });
  expect(replies.at(-1)).toMatchObject({ ok: false, refused: "envelope_too_large" });
  expect(code).toBe(1);
});

// The real owner, over the wire, against receipts already on disk: no page is fetched.
test.skipIf(!existsSync(project))("what was kept from a page is answered by the estate's own owner through the host", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-browser-host-"));
  try {
    mkdirSync(join(root, "receipts"), { recursive: true });
    const env = { BONFYRE_FOREIGN_ROOT: root, BONFYRE_FOREIGN_RECEIPTS: join(root, "receipts"), BONFYRE_PROJECT_BINARY: project };
    const none = await session(env, [{ id: 1, operation: "history", url: "https://example.com/never-read" }]);
    expect(none.replies[0]).toMatchObject({ id: 1, ok: true, operation: "history" });
    expect(none.replies[0].result.native_owner).toBe("BonfyreProject");
    expect(none.replies[0].result.result?.history ?? []).toHaveLength(0);
    expect(readdirSync(join(root, "receipts"))).toHaveLength(0);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

// One real page read through the host, opt-in because it reaches the network.
test.skipIf(!existsSync(project) || !process.env.BONFYRE_BROWSER_HOST_LIVE)("a confirmed ask keeps a copy and the page's history then shows it", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-browser-host-live-"));
  try {
    const env = { BONFYRE_FOREIGN_ROOT: root, BONFYRE_FOREIGN_RECEIPTS: join(root, "receipts"), BONFYRE_PROJECT_BINARY: project };
    const { replies } = await session(env, [{ id: 1, operation: "observe", url: "https://example.com/", confirmed: true }, { id: 2, operation: "history", url: "https://example.com/" }]);
    expect(replies[0]).toMatchObject({ id: 1, ok: true });
    expect(replies[0].result.retention.retained).toBe(true);
    expect(replies[1].result.result.history).toHaveLength(1);
    expect(replies[1].result.result.history[0].retained).toBe(true);
  } finally { rmSync(root, { recursive: true, force: true }); }
}, 60_000);

const queue = join(import.meta.dir, "../../../cmd/BonfyreQueue/bonfyre-queue");
test.skipIf(!existsSync(queue))("a selection is held once in the real work queue, and only when the person asked", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-browser-hold-"));
  try {
    const db = join(root, "queue.db");
    const host = createHost({ env: { ...process.env, BONFYRE_BROWSER_QUEUE_DB: db, BONFYRE_QUEUE_BINARY: queue }, tools: { call: async () => ({}) } });
    const ask = { operation: "hold", url: "https://school.example.org/notices/42", selection: "Bake sale Friday 3pm, bring nut-free items", note: "for the room parents" };
    expect(await host.answer({ id: 1, ...ask })).toMatchObject({ ok: false, refused: "not_confirmed" });
    expect(existsSync(db)).toBe(false);
    const held = await host.answer({ id: 2, ...ask, confirmed: true });
    expect(held).toMatchObject({ ok: true, result: { native_owner: "BonfyreQueue", family: "held-selection" } });
    const again = await host.answer({ id: 3, ...ask, confirmed: true });
    expect(again.result.mission).toBe(held.result.mission);
    const read = Bun.spawnSync([queue, "mission", held.result.mission, "--db", db]);
    const node = JSON.parse(read.stdout.toString());
    expect(node).toMatchObject({ family: "held-selection", status: "ready", operator_id: "queue.page.selection" });
    expect(JSON.parse(node.input_uri)).toMatchObject({ url: ask.url, selection: ask.selection, note: ask.note });
    const depth = Bun.spawnSync([queue, "depth", "--family", "held-selection", "--db", db]).stdout.toString();
    expect(depth).toContain("1");
    expect(await host.answer({ id: 4, ...ask, selection: "  ", confirmed: true })).toMatchObject({ ok: false, refused: "owner_refused", reason: "nothing was selected" });
    expect((await host.answer({ id: 5, ...ask, selection: "x".repeat(SELECTION_BYTES + 1), confirmed: true })).ok).toBe(false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("a capture is kept under the foreign root and the reply carries references, not bodies", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-browser-capture-"));
  try {
    const seen = [];
    const fake = async (url, out) => { seen.push([url, out]); return { schema: "bonfyre.page-capture.v1", capture_id: "pc-abc", final_url: url, representations: { "dom.html": { sha256: "d", bytes: 9, what: "x" } } }; };
    const host = createHost({ env: { ...process.env, BONFYRE_FOREIGN_ROOT: root, BONFYRE_CAPTURE: fake }, tools: { call: async () => ({}) } });
    expect(await host.answer({ id: 1, operation: "capture", url: "https://shop.example.com/tins" })).toMatchObject({ ok: false, refused: "not_confirmed" });
    const reply = await host.answer({ id: 2, operation: "capture", url: "https://shop.example.com/tins", confirmed: true });
    expect(reply).toMatchObject({ ok: true, result: { capture_id: "pc-abc", representations: { "dom.html": { sha256: "d", bytes: 9 } } } });
    expect(seen[0][1].startsWith(join(root, "captures", "shop.example.com"))).toBe(true);
    const none = createHost({ env: { PATH: process.env.PATH }, tools: { call: async () => ({}) } });
    expect(await none.answer({ id: 3, operation: "capture", url: "https://shop.example.com/tins", confirmed: true })).toMatchObject({ ok: false, refused: "owner_refused" });
  } finally { rmSync(root, { recursive: true, force: true }); }
});
