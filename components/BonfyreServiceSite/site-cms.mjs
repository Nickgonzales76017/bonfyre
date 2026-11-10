// The only ServiceSite-to-native-CMS command boundary.
// External authority and schema validation still belong to BonfyreCMS.
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export const SITE_NS = "service-site";
export const SITE_TYPE = "service_site";

export function createSiteCMS({
  binary = process.env.BONFYRE_CMS_BINARY ?? join(homedir(), ".bonfyre/bin/bonfyre-cms"),
  db = process.env.BONFYRE_CMS_DB ?? join(homedir(), ".bonfyre/estate-local/cms.db"),
  schemas = join(import.meta.dir, "../../cmd/BonfyreCMS/content-types"),
  exists = existsSync,
  spawn = argv => Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" }),
} = {}) {
  const cms = args => {
    if (!exists(binary)) throw new Error(`no CMS binary at ${binary}`);
    const out = spawn([binary, ...args, "--db", db, "--schemas", schemas]);
    if (out.exitCode !== 0)
      throw new Error(`CMS refused: ${out.stderr.toString().trim() || out.stdout.toString().trim()}`);
    const text = out.stdout.toString().trim();
    try { return JSON.parse(text); } catch { return text; }
  };
  const rows = () => {
    // Preserve existing schema installation on the same native CMS owner.
    cms(["schema", "apply", join(schemas, SITE_TYPE + ".json")]);
    cms(["schema", "apply", join(schemas, "service_lead.json")]);
    return (cms(["entry", "list", SITE_TYPE, "--ns", SITE_NS, "--limit", "500"]).data ?? [])
      .map(e => ({ id: e.id, ...(e.attributes ?? e) }));
  };
  return { cms, rows, schemas, ns: SITE_NS, type: SITE_TYPE };
}
