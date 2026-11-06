// Fresh ForeignTwin observation coordinates prevent stale receipt/body replay.
// A canonical receipt is published only after a caller verifies the new body.
import { createHash, randomBytes } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { closeSync, copyFileSync, existsSync, mkdirSync, openSync, readFileSync, readSync, unlinkSync } from "node:fs";
import { join } from "node:path";

const execFileAsync = promisify(execFile);
export function readObservationReceipt(path) {
  if (!existsSync(path)) return {};
  const lines = readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.startsWith("{"));
  try {
    const receipt = JSON.parse(lines.at(-1) ?? "{}");
    return receipt && typeof receipt === "object" && !Array.isArray(receipt) ? receipt : {};
  } catch { return {}; }
}
export const successfulPublicObservation = receipt => receipt?.policy_decision === "allow"
  && Number(receipt.http_status ?? 0) >= 200 && Number(receipt.http_status ?? 0) < 300;
export async function observePublic(project, foreign, twin, url,
  { maxBytes = 8_000_000, timeout = 90000, sampleBytes = 0, rangeStart = 0, readText = true,
    fullMaxBytes = 0, env = process.env } = {}) {
  if(!Number.isSafeInteger(fullMaxBytes)||fullMaxBytes<0||fullMaxBytes>12_000_000
    ||(fullMaxBytes>0&&(sampleBytes>0||rangeStart!==0)))throw Error('invalid bounded full observation');
  const receiptDir = join(foreign, "receipts"), bodyDir = join(foreign, "bodies");
  mkdirSync(receiptDir, { recursive: true }); mkdirSync(bodyDir, { recursive: true });
  const coordinate = `${twin}-${createHash("sha256").update(url).digest("hex").slice(0, 12)}-${randomBytes(6).toString("hex")}`;
  const receiptPath = join(receiptDir, `${coordinate}.json`), bodyPath = join(bodyDir, `${coordinate}.body`);
  const argv = ["foreign", "observe", twin, url, "--receipt", receiptPath, "--body", bodyPath, "--observe-right", "public"];
  if (sampleBytes > 0) argv.push("--max-bytes", String(sampleBytes), "--range-start", String(rangeStart));
  if (fullMaxBytes > 0) argv.push("--full-max-bytes", String(fullMaxBytes));
  let exitCode=0;
  try {
    await execFileAsync(project, argv,
      { timeout, maxBuffer: 1024 * 1024, env });
  } catch (error) {
    exitCode=Number.isInteger(error.code)?error.code:-1;
    if (error.killed) return { receipt: { policy_decision: "timeout" }, text: "", receiptPath, bodyPath, exitCode };
  }
  const receipt = readObservationReceipt(receiptPath);
  let text = "";
  if (readText && successfulPublicObservation(receipt) && existsSync(bodyPath)) {
    const fd = openSync(bodyPath, "r");
    try {
      const bytes = Buffer.alloc(maxBytes);
      text = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8");
    } finally { closeSync(fd); }
  }
  return { receipt, text, receiptPath, bodyPath, exitCode };
}
export function discardProbeBody(observation) {
  if (existsSync(observation.bodyPath)) unlinkSync(observation.bodyPath);
}
export function publishCanonicalObservation(observation, foreign, twin) {
  if (!existsSync(observation.receiptPath) || !existsSync(observation.bodyPath)
    || !successfulPublicObservation(observation.receipt) || !observation.text) return false;
  copyFileSync(observation.receiptPath, join(foreign, "receipts", `${twin}.json`));
  return true;
}
