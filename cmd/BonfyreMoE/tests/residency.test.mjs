// A served model is a tenant: it loads on the first request after a memory grant,
// unloads when idle and gives the grant back, and a load that cannot fit is refused
// with the reason instead of pushing the machine into swap. llama-server is
// replaced by a stand-in so the contract runs without a GPU or a checkpoint.
import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync, truncateSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const binary = resolve(dirname(fileURLToPath(import.meta.url)), "../bonfyre-moe");
const sleep = ms => new Promise(done => setTimeout(done, ms));
const GiB = 2 ** 30, MiB = 2 ** 20, FLOOR = 1.5 * GiB;

// A real GGUF header: the front reads layers, KV heads and head size from it to
// price the KV cache, and the chat template to tell a chat model from an
// embedder. The file is then made sparse to the size the test wants.
function gguf(path, { bytes = 4096, chat = true, layers = 64, kvHeads = 8, heads = 8, embedding = 1024, vocab = 10 } = {}) {
  const parts = [], str = text => { const b = Buffer.from(text); const n = Buffer.alloc(8); n.writeBigUInt64LE(BigInt(b.length)); return Buffer.concat([n, b]); };
  const u32 = v => { const b = Buffer.alloc(4); b.writeUInt32LE(v); return b; }, u64 = v => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(v)); return b; };
  const kv = [["general.architecture", 8, str("llama")], ["llama.block_count", 4, u32(layers)], ["llama.attention.head_count_kv", 4, u32(kvHeads)],
    ["llama.attention.head_count", 4, u32(heads)], ["llama.embedding_length", 4, u32(embedding)], ["llama.context_length", 4, u32(32768)],
    ["tokenizer.ggml.tokens", 9, Buffer.concat([u32(8), u64(vocab), ...Array.from({ length: vocab }, (_, i) => str(`t${i}`))])]];
  if (chat) kv.push(["tokenizer.chat_template", 8, str("{{ messages }}")]);
  parts.push(Buffer.from("GGUF"), u32(3), u64(0), u64(kv.length));
  for (const [key, type, value] of kv) parts.push(str(key), u32(type), value);
  writeFileSync(path, Buffer.concat(parts)); truncateSync(path, bytes);
  return path;
}
// The same price the front computes: file + logits buffer + 64 MiB + context x q8_0 KV.
const kvPerToken = ({ layers = 64, kvHeads = 8, heads = 8, embedding = 1024 } = {}) => Math.floor(2 * layers * kvHeads * (embedding / heads) * 1.0625);
const price = (bytes, context, shape = {}) => bytes + 512 * 10 * 4 + 64 * MiB + context * kvPerToken(shape);

function standIn(dir) {
  const path = join(dir, "llama-server");
  writeFileSync(path, `#!/usr/bin/env bun
const args = process.argv.slice(2), port = Number(args[args.indexOf("--port") + 1]);
if (args[args.indexOf("-m") + 1].includes("broken")) process.exit(1);
Bun.serve({ hostname: "127.0.0.1", port, fetch(request) {
  const url = new URL(request.url);
  if (url.pathname === "/health") return Response.json({ status: "ok" });
  return Response.json({ choices: [{ message: { role: "assistant", content: "stand-in answer" } }] });
} });
`);
  chmodSync(path, 0o755);
  return path;
}

async function start(dir, model, port, extra = []) {
  const child = Bun.spawn([binary, "serve", "--model", model, "--port", String(port), "--admission-db", join(dir, "admission.db"), ...extra],
    { env: { ...process.env, BONFYRE_LLAMA_SERVER_BIN: standIn(dir) }, stdout: "pipe", stderr: "pipe" });
  for (let i = 0; i < 50; i++) { try { await fetch(`http://127.0.0.1:${port}/bonfyre/residency`); return child; } catch { await sleep(100); } }
  child.kill(); throw new Error("residency front never listened");
}
// The front relays whole connections, so its own routes answer on a fresh one.
const residency = port => fetch(`http://127.0.0.1:${port}/bonfyre/residency`, { keepalive: false }).then(r => r.json());
const ask = port => fetch(`http://127.0.0.1:${port}/v1/chat/completions`, { keepalive: false, method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ messages: [{ role: "user", content: "hi" }] }) });

test("help answers without loading anything", () => {
  const out = Bun.spawnSync([binary, "--help"], { stdout: "pipe", stderr: "pipe", timeout: 5000 });
  expect(out.exitCode).toBe(0);
  expect(out.stdout.toString() + out.stderr.toString()).toContain("--idle-unload");
});

test("the model loads on the first request and unloads when idle, returning its grant", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), model = join(dir, "tiny.gguf"), port = 17400 + Math.floor(Math.random() * 400);
  gguf(model);
  const server = await start(dir, model, port, ["--idle-unload", "2", "--memory-available", String(8 * 2 ** 30)]);
  try {
    expect((await residency(port)).resident).toBe(false);
    expect(await (await fetch(`http://127.0.0.1:${port}/health`, { keepalive: false })).json()).toMatchObject({ resident: false });
    const answer = await ask(port);
    expect(answer.status).toBe(200);
    expect(JSON.stringify(await answer.json())).toContain("stand-in answer");
    const loaded = await residency(port);
    expect(loaded).toMatchObject({ resident: true, loads: 1 });
    const db = new Database(join(dir, "admission.db"));
    // Resident: still held by this plane, but settled, so it is not counted a
    // second time against memory it has already taken.
    expect(db.query("SELECT volume, plane, kind, released_at FROM resource_grants").all())
      .toEqual([{ volume: "memory", plane: `moe-serve:${port}`, kind: "model-resident:settled", released_at: null }]);
    for (let i = 0; i < 60 && (await residency(port)).resident; i++) await sleep(100);
    expect(await residency(port)).toMatchObject({ resident: false, unloads: 1 });
    expect(db.query("SELECT count(*) AS open FROM resource_grants WHERE released_at IS NULL").get().open).toBe(0);
    expect((await ask(port)).status).toBe(200);
    expect((await residency(port)).loads).toBe(2);
    db.close();
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

test("a checkpoint that does not fit is refused with the reason and never started", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), model = join(dir, "huge.gguf"), port = 17800 + Math.floor(Math.random() * 400);
  gguf(model, { bytes: 2 ** 40 });
  const server = await start(dir, model, port);
  try {
    const refused = await ask(port);
    expect(refused.status).toBe(503);
    expect(refused.headers.get("retry-after")).toBeNull();
    expect((await refused.json()).error).toMatchObject({
      type: "resource_unavailable", retryable: false,
      message: expect.stringMatching(/^reject: huge.gguf needs \d+ MiB, more than this machine can give one model/)
    });
    expect(await residency(port)).toMatchObject({ resident: false, loads: 0, refusals: 1 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

test("with fallbacks, the first checkpoint that fits loads and the status names it", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 18200 + Math.floor(Math.random() * 400);
  const huge = join(dir, "huge.gguf"), small = join(dir, "small.gguf");
  gguf(huge, { bytes: 2 ** 40 }); gguf(small);
  const server = Bun.spawn([binary, "serve", "--model", huge, "--model", small, "--port", String(port),
    "--admission-db", join(dir, "admission.db"), "--memory-available", String(8 * 2 ** 30)],
    { env: { ...process.env, BONFYRE_LLAMA_SERVER_BIN: standIn(dir) }, stdout: "pipe", stderr: "pipe" });
  try {
    for (let i = 0; i < 50; i++) { try { await residency(port); break; } catch { await sleep(100); } }
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ resident: true, model: small, choices: 2, refusals: 0 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

function serveRoot(dir, port, extra) {
  return Bun.spawn([binary, "serve", "--model-root", join(dir, "models"), "--port", String(port), "--admission-db", join(dir, "admission.db"), ...extra],
    { env: { ...process.env, BONFYRE_LLAMA_SERVER_BIN: standIn(dir) }, stdout: "pipe", stderr: "pipe" });
}
async function listening(port) { for (let i = 0; i < 50; i++) { try { return await residency(port); } catch { await sleep(100); } } throw new Error("never listened"); }

test("with no --model, every chat model on disk is a candidate, strongest first, and the strongest that fits loads", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 18600 + Math.floor(Math.random() * 300), models = join(dir, "models");
  mkdirSync(join(models, "nested"), { recursive: true });
  gguf(join(models, "small.gguf"), { bytes: 1 * GiB }); gguf(join(models, "nested/mid.gguf"), { bytes: 3 * GiB });
  gguf(join(models, "huge.gguf"), { bytes: 9 * GiB }); gguf(join(models, "embedder.gguf"), { bytes: 512 * MiB, chat: false });
  const server = serveRoot(dir, port, ["--memory-available", String(FLOOR + 4 * GiB)]);
  try {
    const before = await listening(port);
    expect(before.candidates.map(row => row.model)).toEqual(["huge.gguf", "mid.gguf", "small.gguf"]);
    expect((await ask(port)).status).toBe(200);
    const after = await residency(port);
    expect(after).toMatchObject({ resident: true, model: join(models, "nested/mid.gguf"), choices: 3 });
    // 4 GiB spendable: the 3 GiB model gets every 1024 tokens of context that still fit.
    const fits = Math.floor((4 * GiB - price(3 * GiB, 0)) / kvPerToken() / 1024) * 1024;
    expect(after.context).toBe(Math.min(16384, fits));
    expect(after.context).toBeGreaterThan(4096);
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

test("a resident fallback is replaced by a stronger model between requests once there is room", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 18900 + Math.floor(Math.random() * 300), models = join(dir, "models");
  const memory = join(dir, "memory");
  mkdirSync(models); gguf(join(models, "small.gguf"), { bytes: 1 * GiB }); gguf(join(models, "mid.gguf"), { bytes: 3 * GiB });
  writeFileSync(memory, String(FLOOR + 2 * GiB));
  const server = serveRoot(dir, port, ["--memory-available", `@${memory}`]);
  try {
    await listening(port);
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ model: join(models, "small.gguf"), promotions: 0 });
    // Something else let go of memory: now the stronger model fits on its own.
    writeFileSync(memory, String(FLOOR + price(3 * GiB, 4096) + 64 * MiB));
    // Back to back, the request is still served by the model already loaded.
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ model: join(models, "small.gguf"), promotions: 0 });
    await sleep(31000);
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ model: join(models, "mid.gguf"), promotions: 1, loads: 2 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 60000);

test("a model this llama-server cannot run is set aside and the next one answers", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 19200 + Math.floor(Math.random() * 300), models = join(dir, "models");
  mkdirSync(models); gguf(join(models, "broken.gguf"), { bytes: 2 * GiB }); gguf(join(models, "small.gguf"), { bytes: 1 * GiB });
  const server = serveRoot(dir, port, ["--memory-available", String(8 * GiB)]);
  try {
    await listening(port);
    expect((await ask(port)).status).toBe(200);
    const after = await residency(port);
    expect(after.model).toBe(join(models, "small.gguf"));
    expect(after.candidates).toEqual([expect.objectContaining({ model: "broken.gguf", usable: false }), expect.objectContaining({ model: "small.gguf", usable: true })]);
    const db = new Database(join(dir, "admission.db"));
    expect(db.query("SELECT count(*) AS open FROM resource_grants WHERE released_at IS NULL").get().open).toBe(1);
    db.close();
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

test("a locally unsupported checkpoint is not reported as a retryable memory deferral", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), model = join(dir, "broken.gguf"), port = 19300 + Math.floor(Math.random() * 300);
  gguf(model, { bytes: 1 * GiB });
  const server = await start(dir, model, port, ["--memory-available", String(8 * GiB)]);
  try {
    const refused = await ask(port);
    expect(refused.status).toBe(503);
    expect(refused.headers.get("retry-after")).toBeNull();
    expect((await refused.json()).error).toEqual({
      type: "model_unsupported", retryable: false,
      message: expect.stringContaining("broken.gguf is not supported")
    });
    expect(await residency(port)).toMatchObject({ resident: false, loads: 0, refusals: 1 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);

// A batch holds the model by lease, not by keeping requests close together.
const lease = (port, query) => fetch(`http://127.0.0.1:${port}/bonfyre/lease?${query}`, { keepalive: false, method: "POST" });

test("a leased batch keeps its model past any quiet spell; once released, the stronger model takes over", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 19500 + Math.floor(Math.random() * 200), models = join(dir, "models");
  const memory = join(dir, "memory");
  mkdirSync(models); gguf(join(models, "small.gguf"), { bytes: 1 * GiB }); gguf(join(models, "mid.gguf"), { bytes: 3 * GiB });
  writeFileSync(memory, String(FLOOR + 2 * GiB));
  const server = serveRoot(dir, port, ["--memory-available", `@${memory}`]);
  try {
    await listening(port);
    const held = await (await lease(port, "ttl=120")).json();
    expect(held).toMatchObject({ model_path: join(models, "small.gguf"), ttl: 120, preferred: null });
    writeFileSync(memory, String(FLOOR + price(3 * GiB, 4096) + 64 * MiB));
    await sleep(31000);
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ model: join(models, "small.gguf"), promotions: 0, leases: 1 });
    expect((await lease(port, `renew=${held.lease}&ttl=60`)).status).toBe(200);
    expect((await lease(port, `release=${held.lease}`)).status).toBe(200);
    expect((await lease(port, `release=${held.lease}`)).status).toBe(404);
    await sleep(31000);
    expect((await ask(port)).status).toBe(200);
    expect(await residency(port)).toMatchObject({ model: join(models, "mid.gguf"), promotions: 1, leases: 0 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 90000);

test("a lease can name the model its program wants, which need not be the largest, and a held lease is never swapped by another", async () => {
  const dir = mkdtempSync(join(tmpdir(), "moe-residency-")), port = 19700 + Math.floor(Math.random() * 200), models = join(dir, "models");
  mkdirSync(models); gguf(join(models, "general-3b.gguf"), { bytes: 1 * GiB }); gguf(join(models, "coder-7b.gguf"), { bytes: 3 * GiB });
  const server = serveRoot(dir, port, ["--memory-available", String(8 * GiB)]);
  try {
    await listening(port);
    const first = await (await lease(port, "ttl=60&prefer=GENERAL-3B,coder")).json();
    expect(first).toMatchObject({ model_path: join(models, "general-3b.gguf"), preferred: true });
    // Another program wants the coder, but the first batch holds the model.
    const second = await (await lease(port, "ttl=60&prefer=coder-7b")).json();
    expect(second).toMatchObject({ model_path: join(models, "general-3b.gguf"), preferred: false });
    await lease(port, `release=${first.lease}`); await lease(port, `release=${second.lease}`);
    const third = await (await lease(port, "ttl=1&prefer=coder-7b")).json();
    expect(third).toMatchObject({ model_path: join(models, "coder-7b.gguf"), preferred: true });
    // An unrenewed lease ends by itself.
    await sleep(2100);
    expect(await residency(port)).toMatchObject({ leases: 0, leases_granted: 3 });
  } finally { server.kill(); await server.exited; rmSync(dir, { recursive: true, force: true }); }
}, 30000);
