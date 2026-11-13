import { test, expect } from "bun:test";
import { writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadDatasets, main } from "./install_authored_rows.mjs";

test("independent tenant presentation source overrides engine fixture without CMS writes", () => {
  const dir = mkdtempSync(join(tmpdir(), "bf-tenant-fronts-"));
  try {
    const original = loadDatasets().find(x => x.type === "territory_presentation").rows;
    expect(original).toHaveLength(4);
    const file = join(dir, "custom-doors.jsonl");
    const newRows = original.map(row => row.territory === "work"
      ? { ...row, head: "An independent institution work front" }
      : row);
    const encode = rows => rows.map(row => JSON.stringify(row)).join("\n")+"\n";
    writeFileSync(file, encode(newRows));
    const tenant = loadDatasets({ territory_presentation:file }).find(x => x.type === "territory_presentation");
    expect(tenant.path).toBe(file);
    expect(tenant.rows.find(row=>row.territory==="work").head).toContain("independent institution");
    expect(loadDatasets().find(x => x.type === "territory_presentation").rows).toEqual(original);
    expect(main(["--type","territory_presentation","--dataset-file","territory_presentation="+file])).toBe(0);
    expect(() => main(["--dataset-file","invented_type=/tmp/x"])).toThrow("unrecognized source owner");
    expect(() => main(["--dataset-file","territory_presentation="])).toThrow("must bind one declared type");
    expect(() => main(["--dataset-file","territory_presentation="+file,"--dataset-file","territory_presentation="+file]))
      .toThrow("must bind one declared type");
    const duplicate=join(dir,"duplicate.jsonl");
    writeFileSync(duplicate,encode([newRows[0],newRows[0],newRows[1],newRows[2]]));
    expect(() => loadDatasets({territory_presentation:duplicate})).toThrow("invalid or duplicate");
    const malformed=join(dir,"invalid.jsonl");
    writeFileSync(malformed,encode(newRows.map((row,i)=>i===0?{...row,unexpected_property:"unsafe"}:row)));
    expect(() => loadDatasets({territory_presentation:malformed})).toThrow("unknown unexpected_property");
  } finally {
    rmSync(dir, {recursive:true,force:true});
  }
});
