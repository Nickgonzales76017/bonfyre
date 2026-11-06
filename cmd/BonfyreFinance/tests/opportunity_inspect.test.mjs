import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir, homedir } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";

const finance = resolve(import.meta.dir, "../bonfyre-finance");
const hash = value => createHash("sha256").update(value).digest("hex");
const header = "id\tsource_kind\tsource_ref\tsource_digest\tobserved_at\ttitle\tdesired_state\tcompensation_cents\tcurrency\tdeadline\tcontact_boundary\texpected_compute_cents\texpected_external_cents\thuman_minutes\treuse_points\trisk_points\n";
function run(args) {
  return spawnSync(finance, args, { encoding: "utf8", timeout: 30000 });
}
test("opportunity inspect reads the three owners but invents no agreement or money", () => {
  const dir = mkdtempSync(join(tmpdir(), "bf-opportunity-inspect-"));
  const id = "india:partner:real-source-reference";
  const fdb = join(dir, "finance.db"), cdb = join(dir, "cms.db"), odb = join(dir, "outreach.db");
  try {
    const file = join(dir, "record.tsv"), ref = "gmail:verify-the-transport-separately";
    writeFileSync(file, header + [id, "gmail_sent", ref, hash(ref),
      "2026-10-10T10:00:00Z", "Export workflow", "Await an accepted scope",
      "45000", "USD", "-", "email_partner", 0, 0, 0, 0, 0].join("\t") + "\n");
    expect(run(["--db", fdb, "opportunity", "import", "--file", file]).status).toBe(0);
    const cms = new Database(cdb);
    cms.run("CREATE TABLE frappe_crm_crm_deal(id INTEGER,namespace TEXT,source TEXT,organization_name TEXT,email TEXT,communication_status TEXT,next_step TEXT)");
    cms.query("INSERT INTO frappe_crm_crm_deal VALUES (?,?,?,?,?,?,?)").run(4,"aurekai-commercial",id,"Partner","partner@example.test","Gmail sent","Read actual response");
    cms.close();
    const outreach = new Database(odb);
    outreach.run("CREATE TABLE sends(id INTEGER,target TEXT,offer_name TEXT,reply_type TEXT)");
    outreach.query("INSERT INTO sends VALUES (?,?,?,NULL)").run(11,"partner@example.test",id+"|"+ref);
    outreach.close();

    const cmd = ["--db", fdb, "opportunity", "inspect", "--id", id,
      "--cms-db", cdb, "--outreach-db", odb];
    const result = run(cmd);
    expect(result.status, result.stderr).toBe(0);
    const view = JSON.parse(result.stdout);
    expect(view.kind).toBe("bonfyre.finance.opportunity_projection.v1");
    expect(view.id).toBe(id);
    expect(view.finance_stage).toBe("specified");
    expect(view.listed_compensation_cents).toBe(45000);
    expect(view.cost_basis_confirmed).toBe(false);
    expect(view.estimated_margin_cents).toBeNull();
    expect(view.actual_revenue_cents).toBeNull();
    expect(view.crm).toHaveLength(1);
    expect(view.crm[0].organization).toBe("Partner");
    expect(view.outreach).toHaveLength(1);
    expect(view.outreach[0].local_entry_id).toBe(11);
    expect(view.outreach[0].reply_type).toBeNull();
    expect(view.limitations.join(" ")).toContain("not transport proof");
    expect(run(cmd.map(x=>x===id?"other:unknown":x)).status).not.toBe(0);
    const missing = join(dir, "must-remain-absent.db");
    expect(run(cmd.map(x=>x===cdb?missing:x)).status).not.toBe(0);
    expect(existsSync(missing)).toBe(false);
  } finally {
    rmSync(dir, { recursive:true, force:true });
  }
});

test("real estate: three cross-border proposals use the same Finance CRM Outreach projection", () => {
  const home = homedir();
  const fdb = join(home, ".bonfyre/estate-local/finance.db");
  const cdb = join(home, ".bonfyre/estate-local/cms.db");
  const odb = join(home, ".local/share/bonfyre/outreach.db");
  if (![fdb,cdb,odb].every(existsSync)) return; // No real-store claim on other machines.
  const cases = [
    ["india:8848:dgft-coo-erpnext-mapping-20261010","8848 Digital","1a12522937dfb5e3",45000,5],
    ["brazil:amplialabs:duimp-erpnext-20261010","Amplia Labs","1a1256ad6eef602b",45000,5],
    ["mexico:geeksolutions:vucem-pedimento-erpnext-20261010","GeekSolutions","1a12570efd16121f",45000,5],
    ["partner:tzcode:erpnext-us-latam-integration-20261010","TZCode","1a12715b3c1f250f",null,3],
    ["partner:indictrans:dgft-sapb1-erpnext-20261010","Indictrans","1a12715e4cda8229",null,3]
  ];
  for (const [id, organization, gmail, listedComp, reqCount] of cases) {
    const result = run(["--db", fdb, "opportunity", "inspect", "--id", id,
      "--cms-db", cdb, "--outreach-db", odb]);
    expect(result.status, result.stderr).toBe(0);
    const view = JSON.parse(result.stdout);
    expect(view.id).toBe(id);
    expect(view.listed_compensation_cents).toBe(listedComp);
    expect(view.finance_stage).toBe("specified");
    expect(view.cost_basis_confirmed).toBe(false);
    expect(view.estimated_margin_cents).toBeNull();
    expect(view.requirements.total).toBe(reqCount);
    expect(view.crm.some(x => x.organization === organization)).toBe(true);
    expect(view.outreach.some(x => x.offer_ref === id || x.offer_ref.endsWith("gmail:" + gmail))).toBe(true);
  }
});
