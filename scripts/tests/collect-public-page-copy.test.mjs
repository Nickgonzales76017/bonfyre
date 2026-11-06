import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { normalize, sentences, publicUrl, targets, copyCanonical, argsOf, openCanonicalCms } from "../collect-public-page-copy.mjs";

test("sentence boundaries and UI label retention",()=>{
  expect(normalize(" A \n B ")).toBe("A B");
  const result=sentences("First sentence. Second sentence? Read more");
  expect(result.map(x=>x.text)).toEqual(["First sentence.","Second sentence?","Read more"]);
  expect(result.map(x=>x.kind)).toEqual(["sentence","sentence","phrase"]);
});
test("safe same-origin public routes",()=>{
  expect(publicUrl("/posts?page=2#more","nick.aurekai.work")).toBe("https://nick.aurekai.work/posts?page=2");
  expect(publicUrl("https://example.com","nick.aurekai.work")).toBe(null);
  expect(publicUrl("/account","nick.aurekai.work")).toBe(null);
  expect(publicUrl("/posts?token=secret","nick.aurekai.work")).toBe(null);
});
test("all published site hosts and per-host routes with subject provenance",()=>{
  const src=new Database(":memory:"),out=new Database(":memory:");
  src.exec("CREATE TABLE site_presentation(id INTEGER,site_id TEXT,host TEXT,subject_address TEXT,status TEXT);"+
    "CREATE TABLE site_page(id INTEGER,namespace TEXT,route TEXT,status TEXT,title TEXT,heading TEXT,lede TEXT);"+
    "CREATE TABLE site_program(id INTEGER,namespace TEXT,site_id TEXT,status TEXT,actor_presentations TEXT);");
  out.exec("CREATE TABLE canonical_copy(type TEXT,entry_id INTEGER,namespace TEXT,field TEXT,host TEXT,route TEXT,subject TEXT,raw TEXT,normalized TEXT,digest TEXT,PRIMARY KEY(type,entry_id,namespace,field));");
  src.query("INSERT INTO site_presentation VALUES(1,'person-a-work','a.aurekai.work','person:a','published')").run();
  src.query("INSERT INTO site_presentation VALUES(2,'person-b-life','b.aurekai.life','person:b','published')").run();
  src.query("INSERT INTO site_page VALUES(1,'person-a-work','/posts','published','Welcome','A heading','A sentence.')").run();
  src.query("INSERT INTO site_page VALUES(2,'person-b-life','/secret','draft','Hidden','Hidden','Private')").run();
  src.query("INSERT INTO site_program VALUES(3,'root','person-a-work','published','{\"anonymous\":\"Aurekai observation, not authored by publisher.\"}')").run();
  const c=targets(src);
  expect(c.hosts.size).toBe(7);
  expect([...c.hosts.get("a.aurekai.work").routes]).toEqual(["/","/posts"]);
  expect(copyCanonical(src,out,c)).toBe(4);
  expect(out.query("SELECT COUNT(*) AS n FROM canonical_copy").get().n).toBe(4);
  expect(out.query("SELECT subject FROM canonical_copy WHERE field='lede'").get().subject).toBe("person:a");
  src.close();out.close();
});
test("output refuses canonical CMS overwrite",()=>{
  expect(()=>argsOf(["--cms-db","/tmp/x.db","--out","/tmp/x.db"])).toThrow();
});
test("compatible CMS handles cannot write canonical rows", () => {
  const dir = mkdtempSync(join(tmpdir(), "public-copy-query-only-"));
  const path = join(dir, "canonical.db");
  const writer = new Database(path, { create: true });
  writer.exec("CREATE TABLE fixture(id INTEGER PRIMARY KEY)");
  writer.close();
  const input = openCanonicalCms(path, { forceCompat: true });
  try {
    expect(input.query("PRAGMA query_only").get().query_only).toBe(1);
    expect(input.query("SELECT COUNT(*) AS n FROM fixture").get().n).toBe(0);
    expect(() => input.exec("INSERT INTO fixture VALUES(1)")).toThrow();
  } finally {
    input.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
