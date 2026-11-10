import { expect, test } from "bun:test";
import { classify,editOwner,ROOTS,KINDS,render } from "./repository_maturity.mjs";
test("all five mixed roots have an identified owner",()=>{
  expect(ROOTS).toHaveLength(5);
  expect(new Set(ROOTS.map(r=>r[1])).size).toBe(5);
  expect(KINDS).toEqual(["author","vendor","generated","runtime"]);
});
test("generated and vendor content is not authored code",()=>{
  const cases=[
    ["integrations/frappe-bench/apps/crm/crm/doctype/lead.json","vendor"],
    ["integrations/frappe-bench/env/lib/python3/x.py","runtime"],
    ["integrations/frappe-bench/apps/bonfyre_erpnext/a.py","author"],
    ["components/BonfyreServiceSite/node_modules/react/index.js","vendor"],
    ["components/BonfyreServiceSite/sections/hero.mjs","author"],
    ["tools/creator-ingest/node_modules/puppeteer/a.js","vendor"],
    ["deploy/worker/edge-assets/assets/index.js","generated"],
    ["deploy/worker/.wrangler/cache/a","runtime"],
    ["site/matrix/vgpu/node_modules/vgpu/index.js","vendor"],
    ["site/matrix/DONOR-LICENSES/lemmy/LICENSE","vendor"],
    ["site/matrix/matrix-vgpu.js","generated"],
    ["site/matrix/surfaces/board.html","author"],
  ];
  for(const [path,expected]of cases)expect(classify(path)).toBe(expected);
  expect(()=>classify("site/matrix/../../private")).toThrow();
});
test("editor discoverability is stable",()=>{
  expect(editOwner("components/BonfyreServiceSite/site-cms.mjs")).toBe("native CMS adapter");
  expect(editOwner("components/BonfyreServiceSite/sections/hero.mjs")).toBe("section definition");
  expect(editOwner("site/matrix/surfaces/board.html")).toContain("PageComposition");
});
test("reports are explicit about non-deletion",()=>{
  const rows=ROOTS.map(([path,owner])=>({path,owner,totals:Object.fromEntries(KINDS.map(k=>[k,{files:1,bytes:10}]))}));
  expect(render(rows)).toContain("never authorizes deletion");
});
