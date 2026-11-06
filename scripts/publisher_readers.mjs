#!/usr/bin/env bun
// Bind every addressed publisher to the sources that carry it, in a subject
// fabric: the sources whose retained items name it as publisher, and the census
// sources of every channel its frontier root has. address_every_publisher.mjs
// writes these once when it creates an address; a fabric rebuilt later starts
// empty, so this restores them from the same owners and nothing else.
//
//   bun scripts/publisher_readers.mjs --fabric-db FILE [--estate DIR] [--carry-from OLD_FABRIC] [--apply]
//
// --carry-from copies the readers an earlier fabric held for any subject the
// new one left with none (a territory whose demand store is not present on
// this host, say), so a rebuilt projection never answers with less than the
// fabric it replaces.
//
// A dry run reports what would be written. Readers are the publisher's own
// sources; this never infers consent, control or that the publisher agreed.
import { Database } from "bun:sqlite";
import { homedir } from "node:os";
import { join } from "node:path";

const slug = text => String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "-")
  .replace(/-+/g, "-").replace(/^-|-$/g, "").slice(0, 48).replace(/-$/g, "");

export function main(argv = process.argv.slice(2)) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), fabric: "", carry: "", apply: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--apply") { args.apply = true; continue; }
    if (argv[i] === "--fabric-db" && argv[i + 1]) { args.fabric = argv[++i]; continue; }
    if (argv[i] === "--estate" && argv[i + 1]) { args.estate = argv[++i]; continue; }
    if (argv[i] === "--carry-from" && argv[i + 1]) { args.carry = argv[++i]; continue; }
    throw new Error(`unknown or incomplete argument: ${argv[i]}`);
  }
  if (!args.fabric) throw new Error("--fabric-db is required");
  const cms = new Database(join(args.estate, "cms.db"), { readonly: true });
  const archive = new Database(join(args.estate, "items.db"), { readonly: true });
  const frontier = new Database(join(args.estate, "frontier.db"), { readonly: true });
  const fabric = args.apply ? new Database(args.fabric) : new Database(args.fabric, { readonly: true });
  try {
    if (!fabric.query("SELECT 1 FROM sqlite_master WHERE name='subject_reader'").get())
      throw new Error("the fabric has no subject_reader table; run the fabric precompute first");
    const subjects = cms.query("SELECT DISTINCT subject_address AS subject FROM site_presentation WHERE status='published' AND subject_address LIKE 'institution:publisher:%'").all();
    // The archive's publishers and the frontier's roots, keyed by the same slug the address was made from.
    const bySlug = new Map();
    const add = (label, row) => { if (!label) return; const list = bySlug.get(label) ?? []; list.push(row); bySlug.set(label, list); };
    // A reader is a source the estate currently reads for this publisher: a
    // published census source whose items name it, or an admitted or active
    // frontier channel of its root.  Its state is recorded as it is, never
    // promoted, and a candidate or refused channel is not a reader.
    const published = new Set(cms.query("SELECT DISTINCT source FROM internet_source WHERE status='published'").all().map(r => r.source));
    for (const row of archive.query("SELECT publisher, source, COALESCE(family,'') AS family, COALESCE(kind,'') AS kind, COUNT(*) AS n, MAX(COALESCE(at,0)) AS latest FROM item WHERE COALESCE(publisher,'')<>'' GROUP BY publisher, source").all())
      if (published.has(row.source)) add(slug(row.publisher), { source: row.source, root: "", channel: row.source, material: row.family, protocol: row.kind, state: "admitted", n: row.n, latest: row.latest });
    for (const row of frontier.query("SELECT r.host, r.root_id, c.channel_id, c.census_source, COALESCE(c.material,'') AS material, COALESCE(c.protocol,'') AS protocol, c.state, c.contributions FROM channel c JOIN root r ON r.root_id=c.root_id WHERE COALESCE(c.census_source,'')<>'' AND c.state IN ('admitted','active')").all())
      add(slug(String(row.host).replace(/^www\./, "")), { source: row.census_source, root: row.root_id, channel: row.channel_id, material: row.material, protocol: row.protocol, state: row.state, n: row.contributions || 0, latest: 0 });
    const write = args.apply ? fabric.query("INSERT OR REPLACE INTO subject_reader(subject,scope,census_source,root_id,channel_id,material,protocol,state,last_contributed,contributions,first_seen) VALUES(?,'public',?,?,?,?,?,?,?,?,'')") : null;
    // Reconciled, not appended: a publisher's readers are exactly what holds
    // now, so a binding that went away leaves the projection with it.
    const before = fabric.query("SELECT COUNT(*) AS n FROM subject_reader WHERE subject=? AND scope='public'");
    const clear = args.apply ? fabric.query("DELETE FROM subject_reader WHERE subject=? AND scope='public'") : null;
    let bound = 0, readers = 0, unbound = 0, retired = 0;
    const tx = fabric.transaction(() => {
      for (const { subject } of subjects) {
        const rows = bySlug.get(subject.slice("institution:publisher:".length)) ?? [];
        const seen = new Set();
        let kept = 0;
        const had = before.get(subject)?.n ?? 0;
        if (clear) clear.run(subject);
        for (const r of rows) {
          if (seen.has(r.source)) continue;
          seen.add(r.source); kept++;
          if (write) write.run(subject, r.source, r.root, r.channel, r.material, r.protocol, r.state,
            r.latest ? new Date(r.latest * 1000).toISOString().replace(/\.\d{3}Z$/, "Z") : "", r.n);
        }
        retired += Math.max(0, had - kept);
        if (kept) { bound++; readers += kept; } else unbound++;
      }
    });
    tx();
    // The estate's own territories read every source the frontier currently
    // admits, under the same rule: a reader is what is admitted or active now,
    // so a source linked from the census today reaches the fronts on the next
    // projection instead of waiting on readers carried from an older fabric.
    let territoryReaders = 0;
    const channels = frontier.query("SELECT c.root_id, c.channel_id, c.census_source, COALESCE(c.material,'') AS material, COALESCE(c.protocol,'') AS protocol, c.state, c.contributions FROM channel c WHERE COALESCE(c.census_source,'')<>'' AND c.state IN ('admitted','active') ORDER BY c.census_source, c.channel_id").all()
      .filter(row => published.has(row.census_source));
    fabric.transaction(() => {
      for (const subject of ["life", "live", "shop", "work", "internet"].map(t => `institution:aurekai:${t}`)) {
        if (clear) clear.run(subject);
        const seen = new Set();
        for (const r of channels) {
          if (seen.has(r.census_source)) continue;
          seen.add(r.census_source); territoryReaders++;
          if (write) write.run(subject, r.census_source, r.root_id, r.channel_id, r.material, r.protocol, r.state, "", r.contributions || 0);
        }
      }
    })();
    let carried = 0, carriedSubjects = 0;
    if (args.carry) {
      const old = new Database(args.carry, { readonly: true });
      try {
        const have = new Set(fabric.query("SELECT DISTINCT subject FROM subject_reader").all().map(r => r.subject));
        const columns = "subject,scope,census_source,root_id,channel_id,material,protocol,state,last_contributed,contributions,first_seen";
        const rows = old.query(`SELECT ${columns} FROM subject_reader WHERE state IN ('admitted','active')`).all().filter(r => !have.has(r.subject));
        carriedSubjects = new Set(rows.map(r => r.subject)).size; carried = rows.length;
        if (args.apply) {
          const put = fabric.query(`INSERT OR IGNORE INTO subject_reader(${columns}) VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
          fabric.transaction(() => { for (const r of rows) put.run(...columns.split(",").map(c => r[c] ?? "")); })();
        }
      } finally { old.close(); }
    }
    console.log(JSON.stringify({ publishers: subjects.length, bound, unbound, readers, territory_readers: territoryReaders, retired, carried_subjects: carriedSubjects, carried_readers: carried, applied: args.apply }));
    return 0;
  } finally { cms.close(); archive.close(); frontier.close(); fabric.close(); }
}

if (import.meta.main) {
  try { process.exitCode = main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
