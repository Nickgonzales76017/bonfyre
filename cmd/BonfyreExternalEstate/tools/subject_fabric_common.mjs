import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

export function canon(value) {
  if (Array.isArray(value)) return `[${value.map(canon).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canon(value[key])}`).join(",")}}`;
  return JSON.stringify(value ?? null);
}
export const digest = value => createHash("sha256").update(canon(value)).digest("hex");
export function parsed(value, fallback) {
  if (value == null) return fallback;
  if (typeof value === "object") return value;
  try { return JSON.parse(value); } catch { return fallback; }
}
export function tableExists(db, name) {
  return !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
}
export function runJson(argv) {
  try {
    const stdout = execFileSync(argv[0], argv.slice(1), { encoding: "utf8", timeout: 300000, maxBuffer: 64 * 1024 * 1024 });
    for (const line of stdout.split(/\r?\n/).reverse()) {
      try { return [JSON.parse(line), null]; } catch {}
    }
    return [null, "command returned no JSON"];
  } catch (error) { return [null, String(error.stderr || error.stdout || error.message).trim().slice(-800)]; }
}
