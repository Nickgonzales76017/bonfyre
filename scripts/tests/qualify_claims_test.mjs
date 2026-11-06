import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { projectClaims } from "../qualify_claims.mjs";

const rows = [
  { mission_id: "source-qualify:channel-1", node_id: "probe", worker_id: "worker", claim_token: "lease-1" },
  { mission_id: "source-qualify:channel-2", worker_id: "worker", claim_token: "lease-2" },
  { mission_id: "wrong:channel-3", worker_id: "worker", claim_token: "lease-3" },
  { mission_id: "source-qualify:channel-4", worker_id: "worker", claim_token: "" },
  { mission_id: "source-qualify:bad\tchannel", worker_id: "worker", claim_token: "lease-5" },
].map(JSON.stringify).join("\n") + "\nnot-json\n";
const projected = projectClaims(rows);
assert.deepEqual(projected.ids, ["channel-1", "channel-2"]);
assert.deepEqual(projected.finish, [
  "source-qualify:channel-1\tprobe\tworker\tlease-1\tbonfyre://source-frontier/channel/channel-1",
  "source-qualify:channel-2\tjob\tworker\tlease-2\tbonfyre://source-frontier/channel/channel-2",
]);
const temp = mkdtempSync(join(tmpdir(), "bonfyre-qualify-claims-"));
try {
  const claims = join(temp, "claims.jsonl"), ids = join(temp, "ids.txt"), finish = join(temp, "finish.tsv");
  writeFileSync(claims, rows);
  const result = Bun.spawnSync(["bun", join(import.meta.dir, "../qualify_claims.mjs"), "--claims", claims, "--ids", ids, "--finish", finish]);
  assert.equal(result.exitCode, 0);
  assert.equal(readFileSync(ids, "utf8"), "channel-1\nchannel-2\n");
  assert.equal(readFileSync(finish, "utf8"), projected.finish.join("\n") + "\n");
  console.log("WorkGraph source-qualify claim projection: PASS");
} finally { rmSync(temp, { recursive: true, force: true }); }
