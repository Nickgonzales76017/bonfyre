import { appendFile, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";

export const projectRoot = path.resolve(import.meta.dir, "..");
export const outputRoot = path.join(projectRoot, "output");
export const baseUrl = process.env.BONFYRE_BASE_URL ?? "http://127.0.0.1:3045";
export const UNMEASURED_VALUE_STATUS = "unmeasured_no_realized_financial_receipt";

export type CommandResult = {
  ok: boolean;
  exitCode: number;
  stdout: string;
  stderr: string;
  command: string[];
};

export type CandidateRecord = {
  candidate_id: string;
  source_url: string;
  source_name: string;
  title: string;
  category: string;
  setup_fit: string[];
  subusecase_fit: string[];
  value_type:
    | "credits"
    | "grant"
    | "bounty"
    | "procurement"
    | "free_tier"
    | "dataset"
    | "api"
    | "model"
    | "open_source_tool"
    | "marketplace"
    | "local_resource";
  estimated_value_usd: number;
  estimated_cost_to_pursue_usd: number;
  expected_net_value_usd: number;
  actionability_score: number;
  friction_score: number;
  deadline_or_expiration: string | null;
  eligibility_requirements: string[];
  required_account: string | null;
  no_outreach_required: boolean;
  external_write_required: boolean;
  evidence_digest: string;
  recommended_next_action: string;
  snippet?: string;
  source_kind: string;
  provider_enriched?: boolean;
  rank_score?: number;
};

export async function ensureDir(dir: string) {
  await mkdir(dir, { recursive: true });
}

export async function writeJson(file: string, value: unknown) {
  await ensureDir(path.dirname(file));
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export async function writeText(file: string, text: string) {
  await ensureDir(path.dirname(file));
  await writeFile(file, text, "utf8");
}

export async function appendJsonl(file: string, value: unknown) {
  await ensureDir(path.dirname(file));
  await appendFile(file, `${JSON.stringify(value)}\n`, "utf8");
}

export async function readJsonIfExists<T>(file: string, fallback: T): Promise<T> {
  try {
    return JSON.parse(await readFile(file, "utf8")) as T;
  } catch {
    return fallback;
  }
}

export async function readJsonlIfExists<T>(file: string): Promise<T[]> {
  try {
    const text = await readFile(file, "utf8");
    return text
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line) as T);
  } catch {
    return [];
  }
}

export async function fileExists(file: string) {
  try {
    await stat(file);
    return true;
  } catch {
    return false;
  }
}

export async function execCommand(
  command: string[],
  cwd = projectRoot,
  env: Record<string, string | undefined> = process.env
): Promise<CommandResult> {
  // A missing executable is a result, not an exception. Bun.spawn throws ENOENT
  // synchronously, so every caller that reasonably expected the declared
  // CommandResult with ok:false got a thrown error instead -- and four cloud
  // readiness tools died on `aws` not being installed, taking three systemd
  // units to failed with nothing in the journal to say why.
  //
  // 127 is the shell's own code for "command not found", which is what this is.
  let proc;
  try {
    proc = Bun.spawn(command, { cwd, env, stdout: "pipe", stderr: "pipe" });
  } catch (error: any) {
    const missing = error?.code === "ENOENT";
    return {
      ok: false,
      exitCode: missing ? 127 : 126,
      stdout: "",
      stderr: missing
        ? `${command[0]} is not on PATH; the command could not be run, which is not the same as the command failing`
        : String(error?.message ?? error),
      command
    };
  }
  const [stdout, stderr, exited] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited
  ]);
  return {
    ok: exited === 0,
    exitCode: exited,
    stdout,
    stderr,
    command
  };
}

export async function fetchJson<T = any>(
  url: string,
  init?: RequestInit
): Promise<{ ok: boolean; status: number; body: T | string }> {
  const response = await fetch(url, init);
  const text = await response.text();
  try {
    return { ok: response.ok, status: response.status, body: JSON.parse(text) as T };
  } catch {
    return { ok: response.ok, status: response.status, body: text };
  }
}

export async function fetchText(url: string, init?: RequestInit) {
  const response = await fetch(url, init);
  return {
    ok: response.ok,
    status: response.status,
    text: await response.text()
  };
}

export function redactEnv(name: string, value: string | undefined) {
  if (!value) return { env_name: name, present: false, redacted_preview: "missing" };
  if (name.includes("SECRET") || name.includes("KEY") || name.includes("TOKEN") || name.includes("PAT")) {
    return { env_name: name, present: true, redacted_preview: "present_secret_only" };
  }
  if (name.includes("PROFILE")) {
    return { env_name: name, present: true, redacted_preview: `profile:${value}` };
  }
  if (name.includes("ENABLED")) {
    return { env_name: name, present: true, redacted_preview: `flag:${value}` };
  }
  return { env_name: name, present: true, redacted_preview: "present_ref_only" };
}

export function shaDigest(input: string) {
  const hash = new Bun.CryptoHasher("sha256");
  hash.update(input);
  return hash.digest("hex");
}

export function slug(input: string) {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);
}

export function titleFromHtml(html: string) {
  const match = html.match(/<title[^>]*>([^<]+)<\/title>/i);
  return (match?.[1] ?? "").trim();
}

export function stripHtml(html: string) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function extractLinks(html: string, base: string) {
  const urls = new Set<string>();
  const regex = /href=["']([^"'#]+)["']/gi;
  for (const match of html.matchAll(regex)) {
    const raw = match[1];
    try {
      const url = new URL(raw, base).toString();
      if (url.startsWith("http://") || url.startsWith("https://")) urls.add(url);
    } catch {}
  }
  return [...urls];
}

export function isUsefulResourceUrl(url: string) {
  try {
    const parsed = new URL(url);
    if (!/^https?:$/.test(parsed.protocol)) return false;
    const host = parsed.hostname.toLowerCase();
    const lower = parsed.pathname.toLowerCase();
    if (
      /(^|\.)((fonts|font)\.googleapis\.com|fonts\.gstatic\.com|github\.githubassets\.com|avatars\.githubusercontent\.com|x\.com|twitter\.com)$/.test(
        host
      )
    ) {
      return false;
    }
    if (
      /\.(css|js|mjs|map|png|jpe?g|gif|svg|webp|ico|woff2?|ttf|eot|mp4|webm|zip|gz|tgz)$/i.test(lower)
    ) {
      return false;
    }
    if (/\/css2($|[/?])/.test(lower)) return false;
    if (/\/_nuxt\//.test(lower)) return false;
    if (/(^|\/)(_next|assets|static|dist|build|fonts?|images?)\//i.test(lower)) return false;
    return true;
  } catch {
    return false;
  }
}

export function dedupeCandidates(rows: CandidateRecord[]) {
  const byKey = new Map<string, CandidateRecord>();
  for (const row of rows) {
    const key = `${row.source_url}|${row.setup_fit.join(",")}`;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, row);
      continue;
    }
    const existingScore = (existing.rank_score ?? 0) + (existing.title.length > row.title.length ? 1 : 0);
    const nextScore = (row.rank_score ?? 0) + (row.title.length > existing.title.length ? 1 : 0);
    if (nextScore > existingScore) byKey.set(key, row);
  }
  return [...byKey.values()];
}

export function rankCandidate(row: CandidateRecord) {
  return row.expected_net_value_usd * 0.55 + row.actionability_score * 25 - row.friction_score * 15;
}

export function boardPath(name: string) {
  return path.join(outputRoot, "setup-resource-boards", name);
}

export function summarizeTopResources(rows: CandidateRecord[], limit: number) {
  return rows
    .sort((a, b) => (b.rank_score ?? 0) - (a.rank_score ?? 0))
    .slice(0, limit);
}

export function finiteNumberOrNull(value: unknown): number | null {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function firstFiniteNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const number = finiteNumberOrNull(value);
    if (number != null) return number;
  }
  return null;
}

export function measureMoney(row: Record<string, any>) {
  const realized_value_usd = firstFiniteNumber(
    row.realized_value_usd,
    row.realized_amount_usd,
    row.redeemed_amount_usd,
    row.value_realized_preview_usd,
    row.actual_credit_usd
  );
  const source_documented_amount_usd = firstFiniteNumber(
    row.face_value_usd,
    row.documented_amount_usd,
    row.grant_amount_usd,
    row.credit_amount_usd,
    row.listed_amount_usd,
    row.quoted_amount_usd,
    row.provider_credit_amount_usd,
    row.tracked_budget_usd,
    row.amount_usd
  );
  const observed_cost_usd = firstFiniteNumber(
    row.provider_cost_usd,
    row.expected_cost_usd,
    row.cost_to_capture_usd,
    row.tracked_spend_estimate_usd
  );
  const source_budget_usd = firstFiniteNumber(
    row.tracked_budget_usd,
    row.max_budget_usd,
    row.budget_amount_usd
  );
  const measurement_status =
    realized_value_usd != null
      ? "realized_receipt_amount"
      : source_documented_amount_usd != null
        ? "source_documented_amount"
        : observed_cost_usd != null || source_budget_usd != null
          ? "cost_or_budget_observed_only"
          : UNMEASURED_VALUE_STATUS;

  return {
    realized_value_usd,
    source_documented_amount_usd,
    observed_cost_usd,
    source_budget_usd,
    measurement_status
  };
}

export function measuredSortNumber(row: Record<string, any>, keys: string[]) {
  for (const key of keys) {
    const number = finiteNumberOrNull(row?.[key]);
    if (number != null) return number;
  }
  return Number.NEGATIVE_INFINITY;
}

export function markdownTable(rows: CandidateRecord[]) {
  const lines = [
    "| Title | Category | Value Type | Expected Net USD | Actionability | Friction | URL |",
    "| --- | --- | --- | ---: | ---: | ---: | --- |"
  ];
  for (const row of rows) {
    lines.push(
      `| ${escapePipe(row.title)} | ${escapePipe(row.category)} | ${row.value_type} | ${row.expected_net_value_usd.toFixed(
        0
      )} | ${row.actionability_score.toFixed(1)} | ${row.friction_score.toFixed(1)} | ${row.source_url} |`
    );
  }
  return `${lines.join("\n")}\n`;
}

function escapePipe(input: string) {
  return input.replace(/\|/g, "\\|");
}
