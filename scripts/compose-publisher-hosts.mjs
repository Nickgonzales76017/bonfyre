#!/usr/bin/env bun
// Read-model compiler: admitted publisher channels, never publisher adoption.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const compiled = Bun.spawnSync(["cue", "export", join(here, "publisher-host.cue"), "-e", "publisherHost", "--out", "json"]);
if (compiled.exitCode !== 0) throw new Error(compiled.stderr.toString());
export const policy = JSON.parse(compiled.stdout.toString());
const sha = value => createHash("sha256").update(value).digest("hex");
const stamp = () => new Date().toISOString().replace(/\.\d{3}Z$/, "Z");

export function labelFor(host) {
  return String(host).toLowerCase().replace(/^www\./, "").replace(/[^a-z0-9]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "").slice(0, 48);
}

function pythonJson(value, sorted = false) {
  const encode = item => JSON.stringify(item).replace(/[^\x00-\x7f]/g, character => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
  const entries = Object.entries(value);
  if (sorted) entries.sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  return `{${entries.map(([key, item]) => `${encode(key)}: ${encode(item)}`).join(", ")}}`;
}

export function compose({ estate, repo = resolve(here, ".."), territory = "life", limit = 400, cmsPath = null } = {}) {
  if (!estate) throw new Error("estate directory required");
  if (!/^[a-z][a-z0-9-]*$/.test(territory) || !Number.isInteger(limit) || limit < 1) throw new Error("invalid territory or limit");
  const ledger = new Database(join(estate, "frontier.db"), { readonly: true, create: false });
  const fabricPath = join(estate, "subject_fabric.db");
  const fabric = new Database(fabricPath, { create: true });
  try {
    fabric.exec(readFileSync(join(here, "publisher-host.sql"), "utf8"));
    const states = policy.admitted_states;
    if (!Array.isArray(states) || !states.length || states.some(state => typeof state !== "string")) throw new Error("publisher-host policy has no admitted states");
    const placeholders = states.map(() => "?").join(",");
    const publishers = ledger.query(`SELECT r.root_id, r.host, COALESCE(NULLIF(r.name,''), r.host) AS name FROM root r WHERE EXISTS (SELECT 1 FROM channel c WHERE c.root_id=r.root_id AND c.state IN (${placeholders})) ORDER BY (SELECT COUNT(*) FROM channel c WHERE c.root_id=r.root_id AND c.state IN (${placeholders})) DESC, r.root_id LIMIT ?`).all(...states, ...states, limit);
    const readersFor = ledger.query(`SELECT c.channel_id, c.root_id, COALESCE(c.census_source,'') AS census_source, COALESCE(c.material,'') AS material, COALESCE(c.protocol,'') AS protocol, c.state, COALESCE(c.last_contributed,'') AS last_contributed, COALESCE(c.contributions,0) AS contributions, COALESCE(c.first_seen,'') AS first_seen FROM channel c WHERE c.root_id=? AND c.state IN (${placeholders}) AND COALESCE(c.census_source,'')<>'' ORDER BY c.channel_id`);
    const deleteReaders = fabric.query("DELETE FROM subject_reader WHERE subject=? AND scope=?");
    const insertReader = fabric.query("INSERT OR REPLACE INTO subject_reader(subject,scope,census_source,root_id,channel_id,material,protocol,state,last_contributed,contributions,first_seen) VALUES(?,?,?,?,?,?,?,?,?,?,?)");
    const insertBundle = fabric.query("INSERT OR REPLACE INTO subject_bundle(subject,scope,bundle_digest,closure_digest,scene_digest,compiled_at,counts_json,payload_json) VALUES(?,?,?,?,'',?,?,?)");
    const cms = cmsPath ?? join(repo, "cmd/BonfyreCMS/bonfyre-cms");
    const seen = new Map();
    let composed = 0, withoutReaders = 0, presented = 0, collisions = 0;
    for (const publisher of publishers) {
      const label = labelFor(publisher.host);
      if (!label) continue;
      if (seen.has(label) && seen.get(label) !== publisher.root_id) { collisions++; continue; }
      seen.set(label, publisher.root_id);
      const subject = `${policy.subject_prefix}${label}`;
      const host = `${label}.${policy.host_middle}.${territory}`;
      const readers = readersFor.all(publisher.root_id, ...states);
      if (!readers.length) { withoutReaders++; continue; }
      const payload = { subject, host, publisher: publisher.name, cannot_infer: policy.bundle_cannot_infer };
      const counts = { readers: readers.length, roots: 1 };
      fabric.transaction(() => {
        deleteReaders.run(subject, policy.scope);
        for (const reader of readers) insertReader.run(subject, policy.scope, reader.census_source, reader.root_id, reader.channel_id, reader.material, reader.protocol, reader.state, reader.last_contributed, reader.contributions, reader.first_seen);
        insertBundle.run(subject, policy.scope, sha(pythonJson(payload, true)), sha(readers.map(reader => reader.census_source).join("|")), stamp(), pythonJson(counts), pythonJson(payload));
      })();
      composed++;
      if (existsSync(cms)) {
        const entry = { site_id: `${policy.site_prefix}${label}`, host, subject_address: subject, profile: policy.presentation_profile, territory, navigation: policy.navigation, status: "published" };
        const run = Bun.spawnSync([cms, "entry", "create", "site_presentation", JSON.stringify(entry), "--ns", "root", "--db", join(estate, "cms.db"), "--schemas", join(estate, "schemas")], { env: { ...process.env } });
        if (run.exitCode === 0) presented++;
      }
    }
    return { publishers: publishers.length, composed, hosts_presented: presented, without_readers: withoutReaders, label_collisions: collisions,
      fabric: fabricPath, cannot_infer: policy.cannot_infer };
  } finally { fabric.close(); ledger.close(); }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const argv = process.argv.slice(2);
    const arg = (name, fallback) => argv.includes(name) ? argv[argv.indexOf(name) + 1] : fallback;
    const result = compose({ estate: resolve(arg("--estate", join(homedir(), ".bonfyre/estate-local"))),
      repo: resolve(arg("--repo", resolve(here, ".."))), territory: arg("--territory", "life"), limit: Number(arg("--limit", 400)) });
    console.log(JSON.stringify(result));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
