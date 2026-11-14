// The deterministic Finance/CRM/Outreach fixture now lives in native
// tests/vectors/opportunity_inspect.json. This is observed-store evidence only:
// never silently report a local integration as passed when stores are absent.
import { test, expect } from "bun:test";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const finance = resolve(import.meta.dir, "../bonfyre-finance");
const evidencePaths = [
  join(homedir(), ".bonfyre/estate-local/finance.db"),
  join(homedir(), ".bonfyre/estate-local/cms.db"),
  join(homedir(), ".local/share/bonfyre/outreach.db"),
];
function run(args) { return spawnSync(finance, args, { encoding: "utf8", timeout: 30000 }); }

test.skipIf(!evidencePaths.every(existsSync))("real estate: five cross-border opportunities use their observed Finance CRM Outreach records", () => {
  const [fdb,cdb,odb] = evidencePaths;
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
