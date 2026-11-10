import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Database } from "bun:sqlite";
import { declaredLaw, declaredPack, sourceAmounts } from "../product_close_consequence.mjs";

const root = mkdtempSync(join(tmpdir(), "close-consequence-"));
const store = join(root, "obligations.sqlite3"), credentials = join(root, "credentials.env"), mysql = join(root, "mysql");
const trace = join(root, "mysql-call.json");
const script = fileURLToPath(new URL("../product_close_consequence.mjs", import.meta.url));
const namespace = "upper90/managed/ai-loss";
afterAll(() => rmSync(root, { recursive: true, force: true }));

const db = new Database(store);
db.exec("CREATE TABLE target_obligations(id TEXT, state TEXT)");
for (const id of ["reconcile-payment:1", "reconcile-payment:2", "reconcile-payment:3", "reconcile-payment:4"])
  db.query("INSERT INTO target_obligations VALUES (?, 'open')").run(id);
db.close();
writeFileSync(credentials, "TEST_DB_HOST=fixture\nTEST_DB_USER=reader\nTEST_DB_PASS=private-secret\nTEST_DB_NAME=ledger\n");
writeFileSync(mysql, `#!/usr/bin/env bun
import { writeFileSync } from "node:fs";
writeFileSync(process.env.MYSQL_TRACE, JSON.stringify({args:process.argv.slice(2), hasPassword:!!process.env.MYSQL_PWD}));
if (process.env.MYSQL_FAIL === "1") { console.error("ERROR 2003 (HY000): Can't connect to fixture"); process.exit(1); }
if (process.env.MYSQL_MALFORMED === "1") { console.log("not a row"); process.exit(0); }
console.log("1\\t5\\t5\\n2\\t10\\t7\\n3\\t1\\t2");
`);
chmodSync(mysql, 0o755);
const invoke = extraEnv => spawnSync("bun", [script, "--namespace", namespace, "--obligations", store, "--json"], {
  encoding: "utf8", env: { ...process.env, PATH: `${root}${delimiter}${process.env.PATH}`,
    AI_LOSS_ENV: credentials, MYSQL_TRACE: trace, ...extraEnv } });

test("the namespace selects the authored pack and identifiers stay in the declaration", () => {
  const pack = declaredPack(namespace);
  expect(pack).toEndWith("packs/upper90/product-contract-ai-loss.yaff");
  expect(declaredLaw(pack)).toMatchObject({ obligation_prefix: "reconcile-payment:",
    reconcile_entity: "tblpayincoming", reconcile_allocation_amount: "PaySpltAIAmt" });
});

test("read-only consequence distinguishes zero, under, over and missing source rows", () => {
  const child = invoke();
  expect(child.status).toBe(0);
  const receipt = JSON.parse(child.stdout);
  expect(receipt).toMatchObject({ effect_class: "PURE", effect_performed: false, obligations: 4,
    resolved_against_source: 3, unresolvable: 1, unreconciled_under: 1,
    unreconciled_over: 1, net_amount_left: 2,
    source_of_record: { reached: true, status: "the system of record answered" } });
  expect(receipt.detail.map(item => item.verdict)).toEqual(["reconciled", "unreconciled", "unreconciled", "unresolvable"]);
  const call = JSON.parse(readFileSync(trace, "utf8"));
  expect(call.hasPassword).toBe(true);
  expect(call.args.at(-1)).toContain("IN (1,2,3,4)");
  expect(call.args.at(-1)).toContain("a.PaySpltAIAmt");
  expect(JSON.stringify(call)).not.toContain("private-secret");
});

test("an unreachable source never masquerades as a source that returned no row", () => {
  const child = invoke({ MYSQL_FAIL: "1" });
  expect(child.status).toBe(0);
  const receipt = JSON.parse(child.stdout);
  expect(receipt.source_of_record.reached).toBe(false);
  expect(receipt.unresolvable).toBe(4);
  expect(receipt.detail.every(item => item.because.includes("could not be reached"))).toBe(true);
  expect(child.stdout).not.toContain("private-secret");
});

test("a successful client exit with malformed rows cannot manufacture an empty answer", () => {
  const child = invoke({ MYSQL_MALFORMED: "1" });
  expect(child.status).toBe(0);
  const receipt = JSON.parse(child.stdout);
  expect(receipt.source_of_record).toMatchObject({ reached: false,
    status: "the system of record returned a malformed row" });
  expect(receipt.unresolvable).toBe(4);
});

test("declaration identifiers cannot inject SQL", () => {
  const law = declaredLaw(declaredPack(namespace));
  law.reconcile_entity = "tblpayincoming;DROP TABLE payments";
  const [amounts, reason] = sourceAmounts(law, ["1"], { ...process.env, AI_LOSS_ENV: credentials });
  expect(amounts).toEqual({});
  expect(reason).toContain("not a plain identifier");
});
