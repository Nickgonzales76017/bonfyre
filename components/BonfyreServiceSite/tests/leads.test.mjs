import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { syncLeads } from "../leads.mjs";

const dirs = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const site = id => ({ site_id: id, pages_project: id, cloud_account: id === "second" ? "proton" : undefined, business: { name: id }, contact: { email: `${id}@example.test` } });
const lead = (site_id, key = `lead:${site_id}`) => ({ key, site_id, form: "request", received_at: "2026-10-07T00:00:00Z", fields: [{ label: "Name", value: "Test inquiry" }] });
function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "service-lead-recovery-")); dirs.push(dir);
  const binary = process.env.BONFYRE_CMS_BINARY ?? join(homedir(), ".bonfyre/bin/bonfyre-cms");
  const schemas = resolve(import.meta.dir, "../../../cmd/BonfyreCMS/content-types");
  const cms = args => {
    const result = Bun.spawnSync([binary, ...args, "--db", join(dir, "cms.db"), "--schemas", schemas], { stdout: "pipe", stderr: "pipe" });
    if (result.exitCode !== 0) throw new Error(result.stderr.toString());
    const body = JSON.parse(result.stdout.toString().trim() || "{}");
    if (body.error) throw new Error(body.error);
    return body;
  };
  for (const type of ["service_lead", "outbound_message"]) cms(["schema", "apply", join(schemas, `${type}.json`)]);
  return { cms, rows: type => cms(["entry", "list", type, "--ns", "service-site", "--limit", "100"]).data };
}

test("an interrupted message write recovers retained fields and brief before a remote outage", async () => {
  const { cms, rows } = fixture(), content = site("first"), incoming = lead("first");
  const failMessage = args => {
    if (args[1] === "create" && args[2] === "outbound_message") throw new Error("injected write interruption");
    return cms(args);
  };
  const first = await syncLeads({ sites: [content], cms: failMessage, heldLeads: async () => [incoming], enrich: async () => ({ brief: "Retained source observation", facts: { observed: true } }) });
  expect(first.brought).toBe(1); expect(first.prepared).toBe(0); expect(first.errors[0].phase).toBe("import");
  expect(rows("service_lead")).toHaveLength(1); expect(rows("outbound_message")).toHaveLength(0);
  const retry = await syncLeads({ sites: [content], cms, heldLeads: async () => { throw new Error("account unavailable"); }, enrich: async () => { throw new Error("must use retained facts"); } });
  expect(retry.recovered).toBe(1); expect(retry.brought).toBe(0); expect(retry.errors[0].phase).toBe("fetch");
  const message = rows("outbound_message")[0].attributes;
  expect(message.recipient).toBe("first@example.test"); expect(message.body).toContain("Name: Test inquiry"); expect(message.body).toContain("Retained source observation");
  expect(message.state).toBe("composed"); expect(message.status).toBe("draft");
  const again = await syncLeads({ sites: [content], cms, heldLeads: async () => [incoming, incoming] });
  expect(again.errors).toEqual([]); expect(again.prepared).toBe(0); expect(again.brought).toBe(0);
  expect(rows("service_lead")).toHaveLength(1); expect(rows("outbound_message")).toHaveLength(1);
});

test("one account failure does not block another account, and foreign site leads are ignored", async () => {
  const { cms, rows } = fixture(), calls = [];
  const result = await syncLeads({ sites: [site("first"), site("second")], cms, heldLeads: async (project, account) => {
    calls.push([project, account]);
    if (project === "first") throw new Error("first account unavailable");
    return [lead("first", "foreign"), lead("second"), lead("second")];
  } });
  expect(calls).toEqual([["first", undefined], ["second", "proton"]]);
  expect(result.brought).toBe(1); expect(result.prepared).toBe(1); expect(result.errors).toHaveLength(1);
  expect(rows("service_lead")[0].attributes.site_id).toBe("second");
  expect(rows("outbound_message")[0].attributes.recipient).toBe("second@example.test");
});

test("retries preserve reviewed messages and do not recreate messages for handled leads", async () => {
  const { cms, rows } = fixture(), content = site("first");
  await syncLeads({ sites: [content], cms, heldLeads: async () => [lead("first")] });
  const id = rows("outbound_message")[0].id;
  cms(["entry", "update", "outbound_message", String(id), "--ns", "service-site", JSON.stringify({ state: "withheld", body: "Reviewed by operator" })]);
  const before = rows("outbound_message");
  cms(["entry", "create", "service_lead", "--ns", "service-site", JSON.stringify({ lead_key: "handled", site_id: "first", form: "request", received_at: "2026-10-07", fields_json: "[]", handled: "yes" })]);
  const result = await syncLeads({ sites: [content], cms, heldLeads: async () => [] });
  expect(result.prepared).toBe(0); expect(result.errors).toEqual([]); expect(rows("outbound_message")).toEqual(before);
});

test("a failed lead write is retried without creating an orphan message", async () => {
  const { cms, rows } = fixture();
  const refuseLead = args => args[1] === "create" && args[2] === "service_lead" ? { error: "refused" } : cms(args);
  const options = { sites: [site("first")], heldLeads: async () => [lead("first")] };
  const failed = await syncLeads({ ...options, cms: refuseLead });
  expect(failed.errors).toHaveLength(1); expect(failed.brought).toBe(0); expect(rows("outbound_message")).toHaveLength(0);
  const retried = await syncLeads({ ...options, cms });
  expect(retried.errors).toEqual([]); expect(retried.brought).toBe(1); expect(retried.prepared).toBe(1);
});
