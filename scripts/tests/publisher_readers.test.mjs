// A publisher's readers are what the estate reads for it now: admitted or
// active channels of its root and published sources whose items name it, each
// with its own state; a candidate or refused channel is not a reader, and a
// binding that goes away leaves the fabric with it.
import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "../publisher_readers.mjs";

test("readers carry real standing and are reconciled, not appended", () => {
  const dir = mkdtempSync(join(tmpdir(), "publisher-readers-"));
  try {
    const cms = new Database(join(dir, "cms.db"));
    cms.exec(`CREATE TABLE site_presentation(status TEXT, subject_address TEXT);
      INSERT INTO site_presentation VALUES('published','institution:publisher:lab-test');
      CREATE TABLE internet_source(source TEXT, status TEXT);
      INSERT INTO internet_source VALUES('feed-lab','published'),('feed-gone','retired');`); cms.close();
    const items = new Database(join(dir, "items.db"));
    items.exec(`CREATE TABLE item(source TEXT, key TEXT, family TEXT, kind TEXT, publisher TEXT, at INTEGER);
      INSERT INTO item VALUES('feed-lab','a','news','story','lab.test',1),('feed-gone','b','news','story','lab.test',1);`); items.close();
    const frontier = new Database(join(dir, "frontier.db"));
    frontier.exec(`CREATE TABLE root(root_id TEXT, host TEXT); INSERT INTO root VALUES('r1','lab.test');
      CREATE TABLE channel(channel_id TEXT, root_id TEXT, census_source TEXT, material TEXT, protocol TEXT, state TEXT, contributions INTEGER);
      INSERT INTO channel VALUES('c1','r1','feed-active','news','rss','active',3),('c2','r1','feed-candidate','news','rss','candidate',0),('c3','r1','feed-refused','news','rss','refused',0);`);
    frontier.close();
    const fabricPath = join(dir, "fabric.db");
    const fabric = new Database(fabricPath);
    fabric.exec(`CREATE TABLE subject_reader(subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT,PRIMARY KEY(subject,scope,census_source,channel_id,material));
      INSERT INTO subject_reader VALUES('institution:publisher:lab-test','public','feed-stale','','feed-stale','news','rss','admitted','',0,'');`); fabric.close();
    expect(main(["--fabric-db", fabricPath, "--estate", dir, "--apply"])).toBe(0);
    const rows = new Database(fabricPath, { readonly: true }).query("SELECT census_source, state FROM subject_reader ORDER BY census_source").all();
    expect(rows).toEqual([{ census_source: "feed-active", state: "active" }, { census_source: "feed-lab", state: "admitted" }]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
