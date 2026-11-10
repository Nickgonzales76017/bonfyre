import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { choose, serve } from "../tools/serve.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");

test("only a proven readable route can return bytes with immutable body and signed provenance", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-served-native-"));
  const estate = join(base, "estate"), outDir = join(base, "out"), identityDir = join(base, "identity"), receipts = join(base, "receipts");
  mkdirSync(estate); mkdirSync(identityDir); mkdirSync(receipts);
  try {
    const db = new Database(join(estate, "estate.db"));
    db.exec("CREATE TABLE occurrence(id TEXT PRIMARY KEY,primary_url TEXT)");
    db.query("INSERT INTO occurrence VALUES(?,?)").run("pub'one", "https://example.test/answer");
    db.close();
    writeFileSync(join(receipts, "pub_one.json"), '{"content_sha256":"older"}\n');
    writeFileSync(join(identityDir, "institution.ed25519.secret"), "fixture only");
    const signer = join(base, "signer.mjs");
    writeFileSync(signer, `#!/usr/bin/env bun\nimport { readFileSync } from "node:fs";\nimport { createHash } from "node:crypto";\nconst file = process.argv[process.argv.indexOf("--file") + 1];\nprocess.stdout.write(createHash("sha256").update(readFileSync(file)).digest("hex"));\n`);
    chmodSync(signer, 0o755);
    const route = { candidates: [{ identity: "unread", name: "Unread", basis: "unproven", cost_usd: 0 },
      { identity: "pub'one", name: "Publisher One", basis: "proven_permitted_read", cost_usd: 1.25 }],
      avoided_cost_usd: { median: 2.5 }, frontier: 3, metered: 2 };
    expect(choose(route).identity).toBe("pub'one");
    let fetched = 0;
    const fetcher = async (url, options) => {
      fetched++;
      expect(url.href).toBe("https://example.test/answer");
      expect(options.headers["User-Agent"]).toBe("BonfyreProject/1.0");
      return new Response("answer bytes", { status: 200 });
    };
    const result = await serve({ need: "answer:one", estate, outDir, identityDir, foreignReceipts: receipts, signer, route, fetcher });
    expect(fetched).toBe(1);
    expect(result.served).toBe(true);
    expect(result.answer.bytes).toBe(12);
    expect(result.answer.content_sha256).toBe(sha("answer bytes"));
    expect(result.freshness).toEqual({ state: "changed_since_observation", observed_sha256: "older" });
    expect(readFileSync(result.answer.body_path, "utf8")).toBe("answer bytes");
    expect(result.answer.body_path).toContain(sha("answer bytes"));
    const answer = readdirSync(outDir).find(name => name.endsWith(".answer.json"));
    expect(answer).toBeTruthy();
    const encoded = readFileSync(join(outDir, answer));
    expect(JSON.parse(encoded.toString())).toEqual(result);
    expect(readFileSync(join(outDir, `${answer}.sig`), "utf8")).toBe(sha(encoded));
    const repeated = await serve({ need: "answer:one", estate, outDir, identityDir, foreignReceipts: receipts, signer, route, fetcher });
    expect(repeated.answer.body_path).toBe(result.answer.body_path);
    expect(readdirSync(outDir).filter(name => name.endsWith(".body"))).toHaveLength(1);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("unproven candidate and failed signer never become a served receipt", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-served-refuse-"));
  const estate = join(base, "estate"), outDir = join(base, "out"), identityDir = join(base, "identity");
  mkdirSync(estate); mkdirSync(identityDir);
  try {
    const db = new Database(join(estate, "estate.db"));
    db.exec("CREATE TABLE occurrence(id TEXT PRIMARY KEY,primary_url TEXT); INSERT INTO occurrence VALUES('one','https://example.test/')");
    db.close();
    const refused = await serve({ need: "answer:none", estate, outDir, route: { candidates: [{ identity: "one", name: "One", basis: "unproven", cost_usd: 0 }] },
      fetcher: async () => { throw new Error("should not fetch"); } });
    expect(refused.served).toBe(false);
    expect(readdirSync(outDir)).toHaveLength(0);
    writeFileSync(join(identityDir, "institution.ed25519.secret"), "fixture only");
    const signer = join(base, "refuse.mjs");
    writeFileSync(signer, "#!/usr/bin/env bun\nprocess.exit(3);\n");
    chmodSync(signer, 0o755);
    await expect(serve({ need: "answer:one", estate, outDir, identityDir, signer,
      route: { candidates: [{ identity: "one", name: "One", basis: "proven_permitted_read", cost_usd: 0 }],
        avoided_cost_usd: { median: 0 }, frontier: 1, metered: 0 },
      fetcher: async () => new Response("body", { status: 200 }) })).rejects.toThrow("signer refused");
    expect(readdirSync(outDir).some(name => name.endsWith(".answer.json"))).toBe(false);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("the native route-cost plane is invoked with the estate inputs, not bypassed", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-served-plane-"));
  const estate = join(base, "estate"), outDir = join(base, "out"), identityDir = join(base, "identity");
  mkdirSync(estate); mkdirSync(identityDir);
  try {
    const db = new Database(join(estate, "estate.db"));
    db.exec("CREATE TABLE occurrence(id TEXT PRIMARY KEY,primary_url TEXT)");
    db.query("INSERT INTO occurrence VALUES(?,?)").run("publisher", "https://example.test/resource");
    db.close();
    writeFileSync(join(identityDir, "institution.ed25519.secret"), "fixture only");
    const plane = join(base, "route-plane.mjs"), signer = join(base, "signer.mjs");
    writeFileSync(plane, `#!/usr/bin/env bun\nimport { writeFileSync } from "node:fs";\nwriteFileSync(${JSON.stringify(join(base, "argv.json"))}, JSON.stringify(process.argv.slice(2)));\nconsole.log(JSON.stringify({ candidates: [{ identity: "publisher", name: "Publisher", basis: "proven_permitted_read", cost_usd: 0.5 }], avoided_cost_usd: { median: 2 }, frontier: 1, metered: 1 }));\n`);
    writeFileSync(signer, `#!/usr/bin/env bun\nprocess.stdout.write("fixture-signature");\n`);
    chmodSync(plane, 0o755); chmodSync(signer, 0o755);
    const result = await serve({ need: "answer:publisher", estate, outDir, identityDir, plane, signer,
      fetcher: async () => new Response("routed bytes", { status: 200 }) });
    expect(result.served).toBe(true);
    expect(result.route.source).toBe("publisher");
    expect(JSON.parse(readFileSync(join(base, "argv.json"), "utf8"))).toEqual([
      "route-cost", "--db", join(estate, "estate.db"), "--need", "answer:publisher",
      "--price-book", join(estate, "price-book.jsonl"), "--free-proof", join(estate, "enactment-evidence.jsonl")]);
    expect(readdirSync(outDir).some(name => name.endsWith(".answer.json.sig"))).toBe(true);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("a readable route without an institutional signer cannot publish an answer", async () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-served-no-key-"));
  try {
    await expect(serve({ need: "answer:one", outDir: base, signer: join(base, "missing"), identityDir: base,
      route: { candidates: [{ identity: "one", name: "One", basis: "proven_permitted_read", cost_usd: 0 }] },
      fetcher: async () => { throw new Error("must not fetch"); } })).rejects.toThrow("require the native signer");
    expect(readdirSync(base)).toHaveLength(0);
  } finally { rmSync(base, { recursive: true, force: true }); }
});
