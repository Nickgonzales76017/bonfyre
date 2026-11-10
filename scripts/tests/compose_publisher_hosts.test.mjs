import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compose } from "../compose-publisher-hosts.mjs";

const hash = text => createHash("sha256").update(text).digest("hex");

test("admitted publisher material becomes a scoped subject and host, never adoption", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-publisher-host-"));
  const estate = join(base, "estate");
  const repo = join(base, "empty-repo");
  mkdirSync(estate);
  mkdirSync(repo);
  try {
    const ledger = new Database(join(estate, "frontier.db"));
    ledger.exec("CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT); CREATE TABLE channel(channel_id TEXT PRIMARY KEY,root_id TEXT,census_source TEXT,material TEXT,protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT)");
    ledger.query("INSERT INTO root VALUES(?,?,?)").run("root-1", "www.example.com", "Café News");
    ledger.query("INSERT INTO root VALUES(?,?,?)").run("root-2", "unread.example", "Unread");
    ledger.query("INSERT INTO root VALUES(?,?,?)").run("root-3", "example-com", "Collision");
    ledger.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?)").run("ch-1", "root-1", "source:one", "article", "rss", "admitted", "", 4, "2024-01-01");
    ledger.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?)").run("ch-2", "root-1", "source:two", "audio", "rss", "active", "", 2, "2024-01-02");
    ledger.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?)").run("ch-3", "root-2", "source:three", "article", "rss", "refused", "", 0, "");
    ledger.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?)").run("ch-4", "root-3", "source:four", "article", "rss", "active", "", 1, "");
    ledger.close();

    const result = compose({ estate, repo, territory: "life" });
    expect(result.publishers).toBe(2);
    expect(result.composed).toBe(1);
    expect(result.label_collisions).toBe(1);
    expect(result.hosts_presented).toBe(0);
    const fabric = new Database(join(estate, "subject_fabric.db"), { readonly: true });
    const bundle = fabric.query("SELECT * FROM subject_bundle").get();
    expect(bundle.subject).toBe("institution:publisher:example-com");
    expect(bundle.scope).toBe("public");
    expect(bundle.closure_digest).toBe(hash("source:one|source:two"));
    const payload = JSON.parse(bundle.payload_json);
    expect(payload.host).toBe("example-com.aurekai.life");
    expect(payload.publisher).toBe("Café News");
    expect(payload.cannot_infer).toContain("not anything it agreed to");
    expect(bundle.bundle_digest).toBe(hash(JSON.stringify({ cannot_infer: payload.cannot_infer, host: payload.host, publisher: payload.publisher, subject: payload.subject }).replace(/"([^"\\]*)":/g, '"$1": ').replace(/,(?=")/g, ", ").replace(/é/g, "\\u00e9")));
    expect(fabric.query("SELECT census_source FROM subject_reader ORDER BY channel_id").all().map(row => row.census_source)).toEqual(["source:one", "source:two"]);
    fabric.close();

    const again = compose({ estate, repo, territory: "life" });
    expect(again.composed).toBe(1);
    const reread = new Database(join(estate, "subject_fabric.db"), { readonly: true });
    expect(reread.query("SELECT COUNT(*) AS n FROM subject_reader").get().n).toBe(2);
    reread.close();
    const stage = join(base, "staged");
    const installed = Bun.spawnSync(["make", "-C", join(import.meta.dir, ".."), "install-publisher-hosts", `MEMBRANE_ROOT=${stage}`]);
    expect(installed.exitCode).toBe(0);
    expect(existsSync(join(stage, "scripts/publisher-host.sql"))).toBe(true);
    const staged = Bun.spawnSync(["bun", join(stage, "scripts/compose-publisher-hosts.mjs"), "--estate", estate, "--repo", repo]);
    expect(staged.exitCode).toBe(0);
    expect(JSON.parse(staged.stdout.toString()).composed).toBe(1);

  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("missing frontier ledger is refused", () => {
  const estate = mkdtempSync(join(tmpdir(), "bonfyre-publisher-missing-"));
  try { expect(() => compose({ estate })).toThrow(); }
  finally { rmSync(estate, { recursive: true, force: true }); }
});
