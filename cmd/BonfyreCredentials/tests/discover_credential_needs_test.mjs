import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { candidateHosts, described, discover, observeDescriptor, paymentProtocols, securitySummary } from "../tools/discover_credential_needs.mjs";

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-credential-discovery-"));
  const cms = join(dir, "cms.sqlite"), frontierDb = join(dir, "frontier.sqlite");
  const c = new Database(cms);
  c.exec("CREATE TABLE internet_source (url TEXT, status TEXT)");
  c.query("INSERT INTO internet_source VALUES (?, ?)").run("https://api.test/start", "published");
  c.query("INSERT INTO internet_source VALUES (?, ?)").run("https://ignored.test", "draft");
  c.close();
  const f = new Database(frontierDb);
  f.exec("CREATE TABLE root (root_id TEXT, host TEXT); CREATE TABLE channel (root_id TEXT, protocol TEXT, truth_level TEXT, refusal TEXT, credential TEXT)");
  const root = f.query("INSERT INTO root VALUES (?, ?)"), channel = f.query("INSERT INTO channel VALUES (?, ?, ?, ?, ?)");
  root.run("a", "api.test"); channel.run("a", "openapi", "observed", "", "token");
  root.run("b", "wordpress.test"); channel.run("b", "json_api", "derived_candidate", "", "");
  root.run("c", "blocked.test"); channel.run("c", "openapi", "observed", "robots_disallow", "token");
  root.run("d", "candidate.test"); channel.run("d", "mcp", "derived_candidate", "", "");
  f.close();
  return { dir, cms, frontierDb };
}

test("candidate priorities require descriptor evidence and respect refusals", () => {
  const x = fixture();
  try {
    const hosts = candidateHosts(x.cms, x.frontierDb);
    expect(hosts.get("api.test")).toBe(25100);
    expect(hosts.get("wordpress.test")).toBe(100);
    expect(hosts.get("blocked.test")).toBe(100);
    expect(hosts.get("candidate.test")).toBe(850);
    expect(hosts.has("ignored.test")).toBe(false);
  } finally { rmSync(x.dir, { recursive: true, force: true }); }
});

test("payment presentation stays outside credential import", () => {
  const doc = { openapi: "3.0.0", components: { securitySchemes: {
    ApiKey: { type: "apiKey", name: "X-Api-Key" },
    X402Payment: { type: "apiKey", name: "PAYMENT-SIGNATURE" },
    L402Auth: { type: "http", scheme: "bearer" },
    MPPAuth: { type: "http", scheme: "bearer" },
  } } };
  expect(described(JSON.stringify(doc))).toEqual(doc);
  expect(securitySummary(doc)).toEqual({ credential_schemes: ["ApiKey", "L402Auth", "MPPAuth"], economic_schemes: ["X402Payment"] });
  expect(paymentProtocols(doc)).toEqual(["l402", "mpp", "x402"]);
});

test("exact leased host retains descriptor and imports only genuine schemes", async () => {
  const x = fixture();
  try {
    const foreign = join(x.dir, "foreign"), evidence = join(x.dir, "evidence.jsonl"), hostFile = join(x.dir, "hosts.txt");
    writeFileSync(hostFile, "api.test\t25100\napi.test\n");
    const doc = { openapi: "3.0.0", components: { securitySchemes: {
      ApiKey: { type: "apiKey", name: "X-Api-Key" }, X402Payment: { type: "apiKey", name: "PAYMENT-SIGNATURE" },
    } } };
    const seen = [], imported = [];
    const report = await discover({ cms: x.cms, frontierDb: x.frontierDb, project: "unused", credentials: "credential-binary", db: "credential.sqlite",
      foreign, evidence, hostFile, hostsPerRun: 40, listHosts: false }, {
      observeFn: async (_project, _foreign, _twin, url) => {
        seen.push(url);
        if (url.endsWith("api-catalog")) return [{ http_status: 200, policy_decision: "allow" }, JSON.stringify({ linkset: [{ "service-desc": [{ href: "https://api.test/spec.json" }] }] })];
        if (url.endsWith("spec.json")) return [{ http_status: 200, policy_decision: "allow" }, JSON.stringify(doc)];
        throw new Error(`unexpected request: ${url}`);
      },
      importFn: argv => { imported.push(argv); return { exitCode: 0, stderr: "" }; },
      sleepFn: async () => {},
    });
    expect(seen).toEqual(["https://api.test/.well-known/api-catalog", "https://api.test/spec.json"]);
    expect(report.report).toMatchObject({ asked: 1, described: 1, declared: 1, economic_hosts: 1, payment_protocol_hosts: 1, selection: "workgraph" });
    expect(imported).toHaveLength(1);
    expect(imported[0].slice(0, 2)).toEqual(["credential-binary", "openapi"]);
    const records = readFileSync(evidence, "utf8").trim().split("\n").map(JSON.parse);
    expect(records.at(-1)).toMatchObject({ descriptor_url: "https://api.test/spec.json", credential_schemes: ["ApiKey"], economic_schemes: ["X402Payment"], payment_protocols: ["x402"] });
    expect(readFileSync(records.at(-1).descriptor_path, "utf8")).toBe(JSON.stringify(doc));
  } finally { rmSync(x.dir, { recursive: true, force: true }); }
});

test("observer uses a fresh coordinate for every request and cannot replay stale body", async () => {
  const x = fixture();
  try {
    const observer = join(x.dir, "observe.sh"), foreign = join(x.dir, "foreign");
    writeFileSync(observer, "#!/bin/sh\nexit 1\n", { mode: 0o755 });
    const [receipt, body] = await observeDescriptor(observer, foreign, "descriptor-fixture", "https://api.test/openapi.json");
    expect(receipt).toEqual({});
    expect(body).toBe("");
  } finally { rmSync(x.dir, { recursive: true, force: true }); }
});
