#!/usr/bin/env bun
// Native-messaging host: the carrier between a browser extension and the
// estate's existing native operations. It owns no store and no operation of
// its own. A page's address arrives in an envelope; what is asked of it is
// answered by the same owner-backed tools the MCP surface calls.
//
// Wire: each message is a 4-byte little-endian length and that many bytes of
// JSON, in both directions, as Chrome's native messaging defines it. Chrome
// refuses a reply over 1 MiB, so replies carry receipts and references and
// never a retained body.
//
// A session is one connection (chrome.runtime.connectNative). It ends when
// the browser closes the pipe, when it has been idle, or after a bounded
// number of operations.
import { createHash } from "node:crypto";
import { admitProviderPacket } from './provider-admission.mjs';
import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { createRecursiveTools, nativeRun } from "../../mcp/recursive_overlay_tools.mjs";
import { capturePage } from "../capture/page-capture.mjs";
import * as enrollment from "../onboarding/enrollment.mjs";

export const LIMITS = { inbound_bytes: 256 * 1024, outbound_bytes: 1024 * 1024 - 4096, operations: 64, idle_ms: 5 * 60_000 };
export const VERSION = "bonfyre.browser-host.v1";
const repo = join(import.meta.dir, "..", "..", "..");

// Each operation is an existing owner's. Three keep something -- `observe`
// fetches a public page, `capture` reads it in a browser and keeps how it was
// rendered, `hold` leaves a selection in the work queue for somebody to take
// up -- and each of those runs only when the person in the side panel asked
// for it, which the envelope says with `confirmed: true`.
const OPERATIONS = {
  hello: { effect: "none" },
  provider_admit: { effect: "validates current native provider continuation; never completes work", needs_confirmation: true, provider: true },
  collected: { effect: "none", local: "collected" },
  kept: { effect: "none", tool: "foreign_resolution", args: message => ({ action: "resolve", source_url: message.url }) },
  history: { effect: "none", tool: "foreign_resolution", args: message => ({ action: "history", source_url: message.url }) },
  observe: { effect: "retains a public page", tool: "foreign_twin_observe", needs_confirmation: true,
    args: message => ({ twin_id: twinFor(message.url), url: message.url }) },
  capture: { effect: "retains a page as rendered", local: "capture", needs_confirmation: true },
  hold: { effect: "leaves a selection in the work queue", local: "hold", needs_confirmation: true },
  // Account enrollment is the queue's and the credential boundary's work. These carry no page address: the item
  // names its own provider page, and a closed tab or panel loses nothing because the item lives in the queue.
  enroll_status: { effect: "none", enroll: c => enrollment.status(c) },
  enroll_list: { effect: "none", enroll: (c, m) => enrollment.list(m.identity, {filter:m.filter,search:m.search,cursor:m.cursor}, c) },
  enroll_inspect: { effect: "none", enroll: (c, m) => enrollment.inspect(m.identity, m.work_id, c) },
  enroll_continue: { effect: "holds exactly the reviewed setup item when the native queue admits it", needs_confirmation: true, enroll: (c, m) => enrollment.continueWork(m.identity, m.expected, c) },
  enroll_resume: { effect: "holds this identity's current item, or takes the next one", enroll: (c, m) => enrollment.resume(m.identity, c) ?? enrollment.next(m.identity, c) },
  enroll_record: { effect: "records a stage on the item's checkpoint", needs_confirmation: true, enroll: (c, m) => enrollment.record(m.identity, m.stage, m.note, c, m.expected) },
  enroll_wait: { effect: "sets the item aside until an email, code or review arrives", needs_confirmation: true, enroll: (c, m) => enrollment.wait(m.identity, m.why, m.note, c, m.expected) },
  enroll_decide: { effect: "sets the item aside for a person's decision", needs_confirmation: true, enroll: (c, m) => enrollment.decide(m.identity, m.why, m.note, c, m.expected) },
  enroll_seal: { effect: "seals a key under the identity's account", needs_confirmation: true, enroll: (c, m) => enrollment.seal(m.identity, m.provider, m.field, m.value, c, m.expected) },
  enroll_done: { effect: "finishes the item against its checkpoint", needs_confirmation: true, enroll: (c, m) => enrollment.done(m.identity, c, m.expected) },
};
export const SELECTION_BYTES = 8 * 1024;

// A twin is named for the host it was read from, so one site's pages share one.
export function twinFor(address) {
  const host = new URL(address).hostname.toLowerCase().replace(/^www\./, "");
  return host.replace(/[^a-z0-9.-]/g, "-").slice(0, 96) || "page";
}

export function createHost({ env = process.env, tools } = {}) {
  const scoped = { ...env, BONFYRE_PROJECT_BINARY: env.BONFYRE_PROJECT_BINARY ?? join(repo, "components/BonfyreProject/bonfyre-project") };
  const owner = tools ?? createRecursiveTools({ env: scoped });
  let served = 0;
  // The two operations that are not ForeignTwin's. Each is still somebody
  // else's: the capture organ reads the page, the queue holds the work.
  const local = {
    async collected(message) {
      const address=new URL(message.url),match=/^\/@([A-Za-z0-9_.]{1,64})\/video\/([1-9][0-9]{0,18})\/?$/.exec(address.pathname);
      if(!['www.tiktok.com','tiktok.com'].includes(address.hostname)||!match||address.port||address.search||address.hash)
        throw new Error("name the exact collected TikTok video address");
      if(typeof message.collection!=="string"||!/^[a-f0-9]{64}$/.test(message.collection))throw new Error("name the exact collection identity");
      const home=env.HOME??process.env.HOME;
      const db=env.BONFYRE_SOURCE_FRONTIER_DB??join(home,".bonfyre/estate-local/frontier.db");
      const binary=env.BONFYRE_SOURCE_FRONTIER_BINARY??join(home,".bonfyre/bin/bonfyre-source-frontier");
      const bodies=env.BONFYRE_TRAKTOK_BODIES??join(dirname(db),"foreign/bodies/traktok");
      const r=await nativeRun([binary,"traktok","read","--db",db,"--video",match[2],"--collection",message.collection,"--bodies",bodies],{env,timeoutMs:30000});
      if(r.exit_code!==0)throw new Error((r.stderr||"retained observation unavailable").slice(0,300));
      if(Buffer.byteLength(r.stdout)>128*1024)throw new Error("collected projection exceeds the browser envelope");
      const capture=JSON.parse(r.stdout);
      const expected="https://www.tiktok.com/@"+match[1]+"/video/"+match[2];
      if(capture.schema!=="aurekai.collected-page-capture.v1"||capture.url!==expected||capture.source_receipt?.collection!==message.collection)
        throw new Error("collected source identity disagrees with the requested page");
      return {native_owner:"BonfyreSourceFrontier",capture,network_requests:0};
    },
    async capture(message) {
      const root = env.BONFYRE_FOREIGN_ROOT;
      if (!root) throw new Error("no foreign root is configured, so there is nowhere to keep a capture");
      const record = await (env.BONFYRE_CAPTURE ?? capturePage)(message.url, join(root, "captures", twinFor(message.url), `reading-${Date.now()}`));
      const { representations, ...rest } = record;
      return { ...rest, representations: Object.fromEntries(Object.entries(representations).map(([name, row]) => [name, { sha256: row.sha256, bytes: row.bytes }])) };
    },
    async hold(message) {
      const queue = env.BONFYRE_QUEUE_BINARY ?? join(repo, "cmd/BonfyreQueue/bonfyre-queue");
      const db = env.BONFYRE_BROWSER_QUEUE_DB;
      if (!db) throw new Error("no work queue is configured for held selections");
      if (!existsSync(queue)) throw new Error("the work queue is not present");
      const text = String(message.selection ?? "").trim();
      if (!text) throw new Error("nothing was selected");
      if (Buffer.byteLength(text) > SELECTION_BYTES) throw new Error(`a held selection is at most ${SELECTION_BYTES} bytes; keep a copy of the page for more`);
      const note = String(message.note ?? "").slice(0, 400);
      const input = JSON.stringify({ url: message.url, selection: text, note, held_from: "browser side panel" });
      // The same selection from the same page is one piece of held work, not two.
      const mission = "page-selection-" + createHash("sha256").update(message.url + "\n" + text).digest("hex").slice(0, 24);
      const result = await nativeRun([queue, "enqueue", "page.selection", input, "--family", "held-selection", "--mission", mission, "--if-absent", "--db", db]);
      if (result.exit_code !== 0) throw new Error((result.stderr || result.stdout || "the queue refused it").trim().slice(0, 300));
      return { schema_version: "bonfyre.browser.held-selection.v1", native_owner: "BonfyreQueue", mission, family: "held-selection", bytes: Buffer.byteLength(text),
        forbidden_inference: "held work is waiting for somebody; nothing has been sent, booked, bought or decided" };
    },
  };
  return {
    get served() { return served; },
    async answer(message) {
      const id = message?.id ?? null;
      const refuse = (code, reason) => ({ id, ok: false, refused: code, reason });
      if (!message || typeof message !== "object" || Array.isArray(message)) return refuse("not_an_envelope", "a message is one JSON object naming an operation");
      const operation = OPERATIONS[message.operation];
      if (!operation) return refuse("unknown_operation", `this host carries ${Object.keys(OPERATIONS).join(", ")} and nothing else`);
      if (served >= LIMITS.operations) return refuse("session_spent", `a session carries at most ${LIMITS.operations} operations; open another`);
      served++;
      if (message.operation === "hello")
        return { id, ok: true, host: VERSION, limits: LIMITS, operations: Object.fromEntries(Object.entries(OPERATIONS).map(([name, row]) => [name, { effect: row.effect, needs_confirmation: Boolean(row.needs_confirmation) }])),
          cannot_infer: ["a connected host -> authority to act for the person browsing", "a page observed -> a true statement about what it describes"] };
      if (operation.provider) {
        if (message.confirmed !== true) return refuse("not_confirmed", "provider continuation requires the person's current browser gesture");
        try { return { id, ok: true, operation: message.operation, result: admitProviderPacket(message) }; }
        catch (why) { return {...refuse("owner_refused", String(why?.message ?? why)),...(why?.code==='binding_changed'?{code:why.code}:{})}; }
      }
      if (operation.enroll) {
        if (operation.needs_confirmation && message.confirmed !== true) return refuse("not_confirmed", "this changes enrollment work and runs only when the person asked for it");
        try {
          const result = await operation.enroll(enrollment.config(env), message);
          // A sealed value came in on the envelope; it never goes back out.
          return { id, ok: true, operation: message.operation, result };
        } catch (why) { return refuse("owner_refused", String(why?.message ?? why)); }
      }
      let address;
      try { address = new URL(String(message.url ?? "")); } catch { return refuse("no_address", "name the page's address"); }
      if (address.protocol !== "https:" || address.username || address.password) return refuse("not_public_https", "only a public https address is carried; this one is not");
      if (operation.needs_confirmation && message.confirmed !== true) return refuse("not_confirmed", "this operation keeps a copy of a page and runs only when the person asked for it");
      try {
        const result = operation.local ? await local[operation.local]({ ...message, url: address.href })
          : await owner.call(operation.tool, operation.args({ ...message, url: address.href }));
        const reply = { id, ok: true, operation: message.operation, url: address.href, result };
        // A reply the browser would refuse is cut to what names the result.
        if (Buffer.byteLength(JSON.stringify(reply)) > LIMITS.outbound_bytes)
          return { id, ok: true, operation: message.operation, url: address.href, truncated: true, result: { schema_version: result?.schema_version, receipt_path: result?.receipt_path, retention: result?.retention } };
        return reply;
      } catch (why) { return refuse("owner_refused", String(why?.message ?? why)); }
    },
  };
}

export const frame = message => {
  const body = Buffer.from(JSON.stringify(message));
  const head = Buffer.alloc(4);
  head.writeUInt32LE(body.length);
  return Buffer.concat([head, body]);
};

if (import.meta.main) {
  const host = createHost();
  let pending = Buffer.alloc(0), idle;
  const rest = () => { clearTimeout(idle); idle = setTimeout(() => process.exit(0), LIMITS.idle_ms); };
  rest();
  for await (const chunk of Bun.stdin.stream()) {
    pending = Buffer.concat([pending, Buffer.from(chunk)]);
    while (pending.length >= 4) {
      const size = pending.readUInt32LE(0);
      // An oversized envelope cannot be skipped safely: the stream is no longer framed.
      if (size > LIMITS.inbound_bytes) {
        process.stdout.write(frame({ id: null, ok: false, refused: "envelope_too_large", reason: `an envelope is at most ${LIMITS.inbound_bytes} bytes; send references, not bodies` }));
        process.exit(1);
      }
      if (pending.length < 4 + size) break;
      const body = pending.subarray(4, 4 + size);
      pending = pending.subarray(4 + size);
      rest();
      let message;
      try { message = JSON.parse(body.toString("utf8")); } catch { message = null; }
      process.stdout.write(frame(await host.answer(message)));
    }
  }
  // The browser closed the pipe: the session is over.
  clearTimeout(idle);
  process.exit(0);
}
