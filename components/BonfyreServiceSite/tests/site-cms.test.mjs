// CMS adapter semantics must not require a live database or executable.
import { expect, test } from "bun:test";
import { createSiteCMS, SITE_NS, SITE_TYPE } from "../site-cms.mjs";
const fixtures = () => {
  const calls = [];
  const adapter = createSiteCMS({
    binary: "/mock/bonfyre-cms",
    db: "/mock/estate.db",
    schemas: "/mock/schemas",
    exists: path => path === "/mock/bonfyre-cms",
    spawn: argv => {
      calls.push(argv);
      const data = argv[1] === "entry" ? { data: [{ id: 11, attributes: { site_id: "sample", status: "public" } }] } : {};
      return { exitCode: 0, stdout: JSON.stringify(data), stderr: "" };
    },
  });
  return { calls, adapter };
};
test("explicit CMS owner preserves schema and namespace", () => {
  const { calls, adapter } = fixtures();
  expect(SITE_NS).toBe("service-site");
  expect(SITE_TYPE).toBe("service_site");
  expect(adapter.rows()).toEqual([{ id: 11, site_id: "sample", status: "public" }]);
  expect(calls).toEqual([
    ["/mock/bonfyre-cms", "schema", "apply", "/mock/schemas/service_site.json", "--db", "/mock/estate.db", "--schemas", "/mock/schemas"],
    ["/mock/bonfyre-cms", "schema", "apply", "/mock/schemas/service_lead.json", "--db", "/mock/estate.db", "--schemas", "/mock/schemas"],
    ["/mock/bonfyre-cms", "entry", "list", "service_site", "--ns", "service-site", "--limit", "500", "--db", "/mock/estate.db", "--schemas", "/mock/schemas"],
  ]);
});
test("binary and native refusal remain explicit", () => {
  const absent = createSiteCMS({ binary: "/none", exists: () => false });
  expect(() => absent.cms(["entry", "list"])).toThrow("no CMS binary");
  const failed = createSiteCMS({
    binary: "/test", exists: () => true,
    spawn: () => ({ exitCode: 4, stderr: "denied", stdout: "" }),
  });
  expect(() => failed.cms(["entry", "list"])).toThrow("CMS refused: denied");
});
