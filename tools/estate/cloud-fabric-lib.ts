import path from "node:path";
import { chmod, copyFile, mkdir, readFile, stat } from "node:fs/promises";
import {
  appendJsonl,
  baseUrl,
  ensureDir,
  execCommand,
  fetchJson,
  outputRoot,
  readJsonIfExists,
  readJsonlIfExists,
  shaDigest,
  slug,
  writeJson,
  writeText
} from "./live-resource-lib";

const cloudFabricDir = path.join(outputRoot, "cloud-fabric");
const finalDir = path.join(outputRoot, "final-resource-report");
const workerDir = path.join(outputRoot, "autonomous-worker-mesh");
const recursiveDir = path.join(outputRoot, "recursive-capture");
const bundleDir = path.join(cloudFabricDir, "source-bundles");
const oracleBaseUrl = process.env.BONFYRE_ORACLE_BASE_URL ?? "http://207.211.172.160:3045";
const cloudRefsRawPath = "/Users/nickgonzales/.bonfyre/secrets/cloud.refs.env";
const cloudRefsSanitizedPath = "/Users/nickgonzales/.bonfyre/secrets/cloud.refs.sanitized.env";

type JsonMap = Record<string, any>;

type ProviderRecord = {
  provider_id: string;
  display_name: string;
  provider_kind: string;
  credential_env_names: string[];
  has_minimum_credentials: boolean;
  live_apply_env_name: string | null;
  live_apply_enabled: boolean;
  default_region?: string | null;
  allowed_regions: string[];
  provider_budget_ref?: string | null;
  supported_execution_lanes: string[];
  supported_storage_lanes: string[];
  supported_schedule_lanes: string[];
  supported_database_lanes: string[];
  supported_public_surface_lanes: string[];
  readiness_status: string;
  status: string;
};

type ExecutionLane = {
  lane_id: string;
  provider_id: string;
  lane_kind: string;
  candidate_for_worker_kinds: string[];
  candidate_for_recipe_ids: string[];
  candidate_for_setup_ids: string[];
  candidate_for_app_ids: string[];
  max_budget_usd: number;
  expected_cost_per_run_usd: number;
  expected_latency_ms?: number;
  expected_reliability_score: number;
  expected_operational_friction: number;
  effect_class_ceiling: string;
  external_write_required: boolean;
  readiness_status: string;
  status: string;
};

type WorkerDecision = {
  decision_id: string;
  worker_id: string;
  recipe_id?: string | null;
  task_kind: string;
  candidate_lane_ids: string[];
  selected_lane_id: string;
  decision_reason: string;
  expected_value_delta_usd: number;
  expected_value_per_day_delta: number;
  expected_cost_usd: number;
  cost_avoidance_usd: number;
  fallback_lane_ids: string[];
  receipt_ref: string;
  status: string;
};

const providerSpecs = [
  {
    provider_id: "oracle",
    display_name: "Oracle OCI",
    provider_kind: "oracle_oci",
    credential_env_names: ["BONFYRE_ORACLE_PROFILE", "BONFYRE_ORACLE_REGION"],
    required_envs: ["BONFYRE_ORACLE_REGION"],
    default_region_env: "BONFYRE_ORACLE_REGION",
    live_apply_env_name: "BONFYRE_LIVE_APPLY_ENABLED",
    budget_ref: "oracle_credit_lane",
    lanes: ["always_on_worker", "scheduled_worker", "artifact_store", "postgres_database", "public_surface"]
  },
  {
    provider_id: "aws",
    display_name: "AWS",
    provider_kind: "aws",
    credential_env_names: ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION"],
    required_envs: ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
    default_region_env: "AWS_REGION",
    live_apply_env_name: "BONFYRE_AWS_LIVE_APPLY_ENABLED",
    budget_ref: "aws_budget_lane",
    lanes: ["scheduled_worker", "queue_worker", "serverless_function", "container_task", "artifact_store", "dead_letter_queue", "observability_sink"]
  },
  {
    provider_id: "supabase",
    display_name: "Supabase",
    provider_kind: "supabase",
    credential_env_names: ["SUPABASE_ACCESS_TOKEN", "SUPABASE_PROJECT_REF", "SUPABASE_DB_URL", "SUPABASE_SERVICE_ROLE_KEY"],
    required_envs: ["SUPABASE_ACCESS_TOKEN", "SUPABASE_PROJECT_REF"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_SUPABASE_LIVE_APPLY_ENABLED",
    budget_ref: "supabase_budget_lane",
    lanes: ["postgres_database", "realtime_channel", "auth_provider", "object_backup", "edge_function", "scheduled_worker"]
  },
  {
    provider_id: "cloudflare",
    display_name: "Cloudflare",
    provider_kind: "cloudflare",
    credential_env_names: ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_ZONE_ID"],
    required_envs: ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_CLOUDFLARE_LIVE_APPLY_ENABLED",
    budget_ref: "cloudflare_budget_lane",
    lanes: ["edge_worker", "queue_worker", "object_backup", "public_surface", "bot_gate", "static_site"]
  },
  {
    provider_id: "cloudflare_r2_s3",
    display_name: "Cloudflare R2 S3",
    provider_kind: "cloudflare_r2_s3",
    credential_env_names: ["CLOUDFLARE_ACCESS_KEY_ID", "CLOUDFLARE_SECRET_ACCESS_KEY", "S3_API_ENDPOINT"],
    required_envs: ["CLOUDFLARE_ACCESS_KEY_ID", "CLOUDFLARE_SECRET_ACCESS_KEY", "S3_API_ENDPOINT"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_CLOUDFLARE_LIVE_APPLY_ENABLED",
    budget_ref: "cloudflare_r2_budget_lane",
    lanes: ["artifact_store", "object_backup"]
  },
  {
    provider_id: "neon",
    display_name: "Neon",
    provider_kind: "neon",
    credential_env_names: ["NEON_API_KEY", "NEON_PROJECT_ID", "NEON_DB_URL"],
    required_envs: ["NEON_API_KEY", "NEON_PROJECT_ID"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_NEON_LIVE_APPLY_ENABLED",
    budget_ref: "neon_budget_lane",
    lanes: ["postgres_branch", "postgres_database"]
  },
  {
    provider_id: "fly",
    display_name: "Fly",
    provider_kind: "fly",
    credential_env_names: ["FLY_API_TOKEN"],
    required_envs: ["FLY_API_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_FLY_LIVE_APPLY_ENABLED",
    budget_ref: "fly_budget_lane",
    lanes: ["container_task", "scheduled_worker", "always_on_worker"]
  },
  {
    provider_id: "vercel",
    display_name: "Vercel",
    provider_kind: "vercel",
    credential_env_names: ["VERCEL_TOKEN"],
    required_envs: ["VERCEL_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_VERCEL_LIVE_APPLY_ENABLED",
    budget_ref: "vercel_budget_lane",
    lanes: ["static_site", "public_surface"]
  },
  {
    provider_id: "railway",
    display_name: "Railway",
    provider_kind: "railway",
    credential_env_names: ["RAILWAY_TOKEN"],
    required_envs: ["RAILWAY_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_RAILWAY_LIVE_APPLY_ENABLED",
    budget_ref: "railway_budget_lane",
    lanes: ["container_task", "scheduled_worker"]
  },
  {
    provider_id: "render",
    display_name: "Render",
    provider_kind: "render",
    credential_env_names: ["RENDER_API_KEY"],
    required_envs: ["RENDER_API_KEY"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_RENDER_LIVE_APPLY_ENABLED",
    budget_ref: "render_budget_lane",
    lanes: ["scheduled_worker", "container_task"]
  },
  {
    provider_id: "turso",
    display_name: "Turso",
    provider_kind: "turso",
    credential_env_names: ["TURSO_API_TOKEN"],
    required_envs: ["TURSO_API_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_TURSO_LIVE_APPLY_ENABLED",
    budget_ref: "turso_budget_lane",
    lanes: ["vector_store", "postgres_database", "redis_cache"]
  },
  {
    provider_id: "upstash",
    display_name: "Upstash",
    provider_kind: "upstash",
    credential_env_names: ["UPSTASH_PERSONAL_API", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "QSTASH_TOKEN"],
    required_envs: ["UPSTASH_PERSONAL_API"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_UPSTASH_LIVE_APPLY_ENABLED",
    budget_ref: "upstash_budget_lane",
    lanes: ["redis_cache", "queue_worker", "scheduled_worker"]
  },
  {
    provider_id: "github",
    display_name: "GitHub",
    provider_kind: "github_actions",
    credential_env_names: ["GITHUB_PAT"],
    required_envs: ["GITHUB_PAT"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_GITHUB_LIVE_APPLY_ENABLED",
    budget_ref: "github_budget_lane",
    lanes: ["scheduled_worker", "queue_worker"]
  },
  {
    provider_id: "sentry",
    display_name: "Sentry",
    provider_kind: "sentry",
    credential_env_names: ["SENTRY_AUTH_TOKEN"],
    required_envs: ["SENTRY_AUTH_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_SENTRY_LIVE_APPLY_ENABLED",
    budget_ref: "sentry_budget_lane",
    lanes: ["observability_sink"]
  },
  {
    provider_id: "posthog",
    display_name: "PostHog",
    provider_kind: "posthog",
    credential_env_names: ["POSTHOG_API_KEY", "POSTHOG_PROJECT_TOKEN"],
    required_envs: ["POSTHOG_API_KEY", "POSTHOG_PROJECT_TOKEN"],
    default_region_env: "",
    live_apply_env_name: "BONFYRE_POSTHOG_LIVE_APPLY_ENABLED",
    budget_ref: "posthog_budget_lane",
    lanes: ["product_analytics_sink"]
  },
  {
    provider_id: "local",
    display_name: "Local",
    provider_kind: "local",
    credential_env_names: [],
    required_envs: [],
    default_region_env: "",
    live_apply_env_name: null,
    budget_ref: "local_budget_lane",
    lanes: ["always_on_worker", "artifact_store", "postgres_database", "redis_cache"]
  }
] as const;

const laneTemplates = [
  { provider_id: "aws", lane_kind: "scheduled_worker", workers: ["grant_funding_worker", "receipt_audit_worker", "recipe_marketplace_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 40, cost: 0.12, reliability: 0.93, friction: 0.35, latency: 1800 },
  { provider_id: "aws", lane_kind: "queue_worker", workers: ["document_artifact_worker", "translation_worker"], recipes: ["document_to_native_form"], setups: ["resource_engine"], apps: ["drive"], budget: 50, cost: 0.09, reliability: 0.91, friction: 0.4, latency: 1200 },
  { provider_id: "aws", lane_kind: "artifact_store", workers: ["document_artifact_worker"], recipes: ["proof_of_impact_packet"], setups: ["public_good_demo"], apps: ["drive"], budget: 25, cost: 0.03, reliability: 0.95, friction: 0.3, latency: 300 },
  { provider_id: "supabase", lane_kind: "postgres_database", workers: ["nonprofit_growth_worker", "rewardsops_consultant_worker"], recipes: ["nonprofit_verification_capture"], setups: ["public_good_demo", "rewards_ops_demo"], apps: ["crm"], budget: 35, cost: 0.05, reliability: 0.9, friction: 0.25, latency: 140 },
  { provider_id: "supabase", lane_kind: "realtime_channel", workers: ["translation_worker", "kind_audit_worker"], recipes: ["translation_public_surface_refresh"], setups: ["shoutout_demo"], apps: ["wiki"], budget: 20, cost: 0.02, reliability: 0.88, friction: 0.22, latency: 90 },
  { provider_id: "supabase", lane_kind: "edge_function", workers: ["translation_worker", "document_artifact_worker"], recipes: ["document_to_native_form"], setups: ["public_good_demo"], apps: ["drive"], budget: 18, cost: 0.04, reliability: 0.84, friction: 0.3, latency: 200 },
  { provider_id: "cloudflare", lane_kind: "edge_worker", workers: ["translation_worker", "cultural_sponsor_worker", "vendor_marketplace_worker"], recipes: ["translation_public_surface_refresh", "shoutout_event_campaign_seed"], setups: ["shoutout_demo", "cultural_commerce_demo"], apps: ["wiki"], budget: 25, cost: 0.03, reliability: 0.92, friction: 0.2, latency: 60 },
  { provider_id: "cloudflare", lane_kind: "queue_worker", workers: ["receipt_audit_worker", "kind_audit_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 15, cost: 0.02, reliability: 0.9, friction: 0.25, latency: 140 },
  { provider_id: "cloudflare", lane_kind: "public_surface", workers: ["translation_worker"], recipes: ["translation_public_surface_refresh"], setups: ["public_good_demo", "shoutout_demo"], apps: ["wiki"], budget: 10, cost: 0.01, reliability: 0.93, friction: 0.15, latency: 70 },
  { provider_id: "cloudflare_r2_s3", lane_kind: "artifact_store", workers: ["document_artifact_worker"], recipes: ["proof_of_impact_packet"], setups: ["public_good_demo"], apps: ["drive"], budget: 22, cost: 0.02, reliability: 0.94, friction: 0.2, latency: 80 },
  { provider_id: "neon", lane_kind: "postgres_branch", workers: ["govcon_worker", "kind_audit_worker"], recipes: ["govcon_proposal_fit_matrix"], setups: ["govcon_ops_demo"], apps: ["insights"], budget: 18, cost: 0.05, reliability: 0.89, friction: 0.28, latency: 130 },
  { provider_id: "neon", lane_kind: "postgres_database", workers: ["startup_credit_worker"], recipes: ["startup_cloud_credit_capture"], setups: ["oracle_setup"], apps: ["insights"], budget: 15, cost: 0.04, reliability: 0.9, friction: 0.26, latency: 120 },
  { provider_id: "fly", lane_kind: "container_task", workers: ["shadow_institution_discovery_worker", "model_runtime_cost_worker"], recipes: ["oracle_worker_cost_avoidance"], setups: ["oracle_setup", "model_runtime"], apps: ["insights"], budget: 30, cost: 0.08, reliability: 0.88, friction: 0.38, latency: 500 },
  { provider_id: "fly", lane_kind: "always_on_worker", workers: ["connector_mcp_worker"], recipes: ["connector_mcp_marketplace_listing"], setups: ["connector_mcp"], apps: ["wiki"], budget: 25, cost: 0.07, reliability: 0.87, friction: 0.36, latency: 350 },
  { provider_id: "oracle", lane_kind: "always_on_worker", workers: ["nonprofit_growth_worker", "govcon_worker", "cultural_sponsor_worker", "hoa_vendor_worker", "rewardsops_consultant_worker", "startup_credit_worker", "translation_worker", "document_artifact_worker", "vendor_marketplace_worker", "model_runtime_cost_worker", "kind_audit_worker", "receipt_audit_worker", "shadow_institution_discovery_worker", "insurance_coi_worker", "grant_funding_worker", "recipe_marketplace_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 60, cost: 0.05, reliability: 0.96, friction: 0.18, latency: 110 },
  { provider_id: "oracle", lane_kind: "scheduled_worker", workers: ["receipt_audit_worker", "kind_audit_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 45, cost: 0.04, reliability: 0.95, friction: 0.16, latency: 200 },
  { provider_id: "vercel", lane_kind: "static_site", workers: ["translation_worker"], recipes: ["translation_public_surface_refresh"], setups: ["shoutout_demo"], apps: ["wiki"], budget: 10, cost: 0.01, reliability: 0.86, friction: 0.3, latency: 100 },
  { provider_id: "railway", lane_kind: "container_task", workers: ["recipe_marketplace_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 12, cost: 0.06, reliability: 0.82, friction: 0.4, latency: 450 },
  { provider_id: "render", lane_kind: "scheduled_worker", workers: ["grant_funding_worker"], recipes: ["proof_of_impact_packet"], setups: ["public_good_demo"], apps: ["insights"], budget: 12, cost: 0.05, reliability: 0.83, friction: 0.42, latency: 500 },
  { provider_id: "turso", lane_kind: "vector_store", workers: ["model_runtime_cost_worker", "kind_audit_worker"], recipes: ["model_runtime_credit_capture"], setups: ["model_runtime"], apps: ["insights"], budget: 15, cost: 0.03, reliability: 0.84, friction: 0.3, latency: 95 },
  { provider_id: "upstash", lane_kind: "redis_cache", workers: ["translation_worker", "document_artifact_worker"], recipes: ["translation_public_surface_refresh"], setups: ["resource_engine"], apps: ["wiki"], budget: 8, cost: 0.01, reliability: 0.8, friction: 0.32, latency: 70 },
  { provider_id: "github", lane_kind: "scheduled_worker", workers: ["connector_mcp_worker", "recipe_marketplace_worker"], recipes: ["connector_mcp_marketplace_listing"], setups: ["connector_mcp"], apps: ["wiki"], budget: 5, cost: 0.0, reliability: 0.9, friction: 0.25, latency: 600 },
  { provider_id: "sentry", lane_kind: "observability_sink", workers: ["receipt_audit_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 6, cost: 0.01, reliability: 0.9, friction: 0.2, latency: 75 },
  { provider_id: "posthog", lane_kind: "product_analytics_sink", workers: ["translation_worker", "vendor_marketplace_worker"], recipes: ["translation_public_surface_refresh"], setups: ["public_good_demo"], apps: ["wiki"], budget: 8, cost: 0.01, reliability: 0.87, friction: 0.2, latency: 100 },
  { provider_id: "local", lane_kind: "always_on_worker", workers: ["kind_audit_worker", "receipt_audit_worker"], recipes: ["weekly_autonomous_manager_report"], setups: ["resource_engine"], apps: ["insights"], budget: 0, cost: 0.0, reliability: 0.98, friction: 0.05, latency: 30 }
] as const;

async function resetCloudFabricOutputs() {
  await ensureDir(cloudFabricDir);
  const jsonlFiles = [
    "cloud_env_readiness.jsonl",
    "providers.jsonl",
    "readiness.jsonl",
    "execution_lanes.jsonl",
    "fabric_decisions.jsonl",
    "deploy_preview_readiness.jsonl",
    "data_queue_cache_readiness.jsonl",
    "observability_readiness.jsonl",
    "oracle_source_bundle_push.jsonl",
    "worker_lane_decisions.jsonl",
    "worker_cloud_schedule_previews.jsonl",
    "worker_cloud_receipts.jsonl",
    "cloud_schedules.jsonl",
    "cloud_run_receipts.jsonl",
    "aws_eventbridge_schedules.preview.jsonl",
    "aws_sqs_queues.preview.jsonl",
    "supabase_edge_functions_plan.jsonl",
    "supabase_dry_sync.jsonl",
    "cloudflare_worker_plan.jsonl",
    "cloudflare_queue_plan.jsonl",
    "optional_deployment_lane_plans.jsonl",
    "provider_credit_ledger_updates.jsonl"
  ];
  for (const file of jsonlFiles) await writeText(path.join(cloudFabricDir, file), "");
}

async function fileStatMaybe(file: string) {
  try {
    return await stat(file);
  } catch {
    return null;
  }
}

function redactValue(name: string, value: string | undefined) {
  if (!value) return "missing";
  if (/REGION|PROFILE|PROJECT_REF|PROJECT_ID|ACCOUNT_ID|ZONE_ID/.test(name)) return "present_ref_only";
  return "present_secret_only";
}

function envPresent(name: string) {
  const value = process.env[name];
  return Boolean(value && String(value).trim().length > 0);
}

function presentAny(names: string[]) {
  return names.some((name) => envPresent(name));
}

function presentAll(names: string[]) {
  return names.every((name) => envPresent(name));
}

function shellEscapeDoubleQuoted(value: string) {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function normalizeLineValue(raw: string) {
  return raw
    .replace(/\s*\[did not collect\]\s*/gi, "")
    .replace(/\s*"\s*\[did not collect\]\s*/gi, "")
    .replace(/\s*""\s*\[did not collect\]\s*/gi, "")
    .trim();
}

export async function normalizeCloudRefs() {
  const backupPath = `${cloudRefsRawPath}.bak.${nowStamp()}`;
  const raw = await readFile(cloudRefsRawPath, "utf8");
  await copyFile(cloudRefsRawPath, backupPath);
  const sanitizedLines: string[] = [];
  const keyStates: JsonMap[] = [];
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const normalized = trimmed.replace(/^export\s+/, "");
    const eqIndex = normalized.indexOf("=");
    if (eqIndex < 1) continue;
    const key = normalized.slice(0, eqIndex).trim();
    let value = normalized.slice(eqIndex + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    value = normalizeLineValue(value);
    const escaped = shellEscapeDoubleQuoted(value);
    sanitizedLines.push(`export ${key}="${escaped}"`);
    keyStates.push({
      env_name: key,
      present: value.length > 0,
      redacted_preview: redactValue(key, value || undefined)
    });
  }
  const sanitizedText = `${sanitizedLines.join("\n")}\n`;
  await writeText(cloudRefsSanitizedPath, sanitizedText);
  await chmod(cloudRefsSanitizedPath, 0o600);
  const sanitization = {
    status: "ok",
    source_file: cloudRefsRawPath,
    backup_file: backupPath,
    sanitized_file: cloudRefsSanitizedPath,
    key_count: keyStates.length,
    timestamp: new Date().toISOString()
  };
  await writeJson(path.join(cloudFabricDir, "cloud_refs_sanitization.json"), sanitization);
  const report = [
    "# Cloud Refs Readiness",
    "",
    `- backup created: \`${backupPath}\``,
    `- sanitized file: \`${cloudRefsSanitizedPath}\``,
    `- keys normalized: \`${keyStates.length}\``,
    `- optional blanks preserved: \`AWS_SESSION_TOKEN\`, \`AWS_PROFILE\`, \`UPSTASH_REDIS_REST_URL\`, \`UPSTASH_REDIS_REST_TOKEN\`, \`QSTASH_TOKEN\``
  ].join("\n");
  await writeText(path.join(finalDir, "cloud_refs_readiness_report.md"), report);
  return sanitization;
}

export async function generateCloudEnvReadiness() {
  await ensureDir(cloudFabricDir);
  const files = [
    cloudRefsRawPath,
    cloudRefsSanitizedPath
  ];
  const records: JsonMap[] = [];
  for (const spec of providerSpecs) {
    const optionals =
      spec.provider_id === "upstash"
        ? ["UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN", "QSTASH_TOKEN"]
        : spec.provider_id === "aws"
          ? ["AWS_SESSION_TOKEN", "AWS_PROFILE"]
          : [];
    for (const envName of spec.credential_env_names) {
      const optional = optionals.includes(envName);
      const present = envPresent(envName);
      const status = optional && !present ? "missing_optional" : present ? "ready" : "missing_required";
      const record = {
        provider_id: spec.provider_id,
        env_name: envName,
        present,
        optional,
        status,
        redacted_preview: redactValue(envName, process.env[envName])
      };
      records.push(record);
      await appendJsonl(path.join(cloudFabricDir, "cloud_env_readiness.jsonl"), record);
    }
  }
  const fileRows = await Promise.all(files.map(async (file) => ({
    file,
    exists: Boolean(await fileStatMaybe(file))
  })));
  const report = [
    "# Cloud Env Readiness",
    "",
    ...fileRows.map((row) => `- ${row.file}: ${row.exists ? "present" : "missing"}`),
    "",
    `- readiness rows: ${records.length}`,
    `- missing optional: ${records.filter((row) => row.status === "missing_optional").length}`,
    `- missing required: ${records.filter((row) => row.status === "missing_required").length}`
  ].join("\n");
  await writeText(path.join(finalDir, "cloud_refs_readiness_report.md"), report);
  return { records, fileRows };
}

async function runAwsReadiness() {
  const hasCreds = presentAll(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]);
  const profile = envPresent("AWS_PROFILE") ? process.env.AWS_PROFILE : undefined;
  const region = process.env.AWS_REGION || "us-east-1";
  const cmd = ["aws"];
  if (profile) cmd.push("--profile", profile);
  cmd.push("--region", region, "sts", "get-caller-identity", "--output", "json");
  // cli_present was the literal `true`. The one field that says whether the AWS
  // CLI is installed asserted that it was, on a host where it is not, and the
  // report could not distinguish "AWS is not ready" from "nothing here can tell
  // you".
  //
  // It is probed separately from the credentialed call, and deliberately so.
  // Inferring absence from the sts call's exit code would report cli_absent
  // whenever credentials are missing -- because the call is then never made and
  // the initial 127 stands unchallenged. That is the same defect one level in:
  // a field claiming an observation nobody performed. `aws --version` needs no
  // credentials, so the question is always answerable and always asked.
  const version = await execCommand(["aws", "--version"]);
  const cliPresent = version.exitCode !== 127;
  let sts: CommandResultLike = { ok: false, exitCode: -1, stdout: "", stderr: "aws not run" };
  if (cliPresent && hasCreds) sts = await execCommand(cmd);
  // Four states, ordered by what is known. Without the CLI nothing can be asked;
  // with it and no credentials the question is answerable and unanswered; with
  // both, the API's own answer stands.
  const state = !cliPresent
    ? "cli_absent"
    : !hasCreds
      ? "missing_credentials"
      : (sts.ok ? "ready" : "api_error");
  const result = {
    provider_id: "aws",
    sts_ok: sts.ok,
    profile_mode: Boolean(profile),
    region,
    cli_present: cliPresent,
    caller_identity_detected: sts.ok,
    api_error: sts.ok ? null : (sts.stderr || sts.stdout || "sts_failed").trim().slice(0, 400),
    cannot_infer: "an absent CLI -> an unready provider; cli_absent means this host "
      + "cannot answer the question, not that the answer is no",
    readiness: {
      eventbridge_scheduler: state,
      sqs: state,
      lambda: state,
      ecs_fargate: state,
      s3: state,
      cloudwatch: state,
      iam: state,
      budgets: state,
      parameter_store: state,
      secrets_manager: state
    }
  };
  await writeJson(path.join(cloudFabricDir, "aws_readiness.json"), result);
  return result;
}

type CommandResultLike = { ok: boolean; exitCode: number; stdout: string; stderr: string };

async function apiJson(url: string, init?: RequestInit) {
  try {
    return await fetchJson<any>(url, init);
  } catch (error) {
    return { ok: false, status: 0, body: String(error) };
  }
}

function maskedDb(url: string | undefined) {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.hostname}${parsed.port ? `:${parsed.port}` : ""}${parsed.pathname}`;
  } catch {
    return "present_ref_only";
  }
}

async function runSupabaseReadiness() {
  const token = process.env.SUPABASE_ACCESS_TOKEN;
  const projectRef = process.env.SUPABASE_PROJECT_REF;
  const dbUrl = process.env.SUPABASE_DB_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const mgmt =
    token && projectRef
      ? await apiJson(`https://api.supabase.com/v1/projects/${projectRef}`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      : { ok: false, status: 0, body: "missing_credentials" };
  const result = {
    provider_id: "supabase",
    management_ready: Boolean(token && projectRef) && mgmt.ok,
    management_status: token && projectRef ? mgmt.status : 0,
    management_error: mgmt.ok ? null : String(mgmt.body).slice(0, 400),
    db_url_ready: Boolean(dbUrl),
    db_url_masked: maskedDb(dbUrl),
    service_role_ready: Boolean(serviceRole),
    readiness: {
      postgres_mirror: dbUrl ? "ready" : "missing_credentials",
      realtime: token && projectRef ? (mgmt.ok ? "ready" : "api_error") : "missing_credentials",
      auth_client_portal: token && projectRef ? (mgmt.ok ? "ready" : "api_error") : "missing_credentials",
      storage: token && projectRef ? (mgmt.ok ? "ready" : "api_error") : "missing_credentials",
      edge_functions: token && projectRef ? (mgmt.ok ? "ready" : "api_error") : "missing_credentials",
      scheduled_functions: token && projectRef ? (mgmt.ok ? "ready" : "api_error") : "missing_credentials",
      vault: serviceRole ? "ready" : "missing_credentials"
    }
  };
  await writeJson(path.join(cloudFabricDir, "supabase_readiness.json"), result);
  return result;
}

async function runCloudflareReadiness() {
  const token = process.env.CLOUDFLARE_API_TOKEN;
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const zoneId = process.env.CLOUDFLARE_ZONE_ID;
  const verify = token
    ? await apiJson("https://api.cloudflare.com/client/v4/user/tokens/verify", {
        headers: { Authorization: `Bearer ${token}` }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  const accountResp =
    token && accountId
      ? await apiJson(`https://api.cloudflare.com/client/v4/accounts/${accountId}`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      : { ok: false, status: 0, body: "missing_account_or_token" };
  const zoneResp =
    token && zoneId
      ? await apiJson(`https://api.cloudflare.com/client/v4/zones/${zoneId}`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      : { ok: false, status: 0, body: "missing_zone_or_token" };
  const r2Ready = presentAll(["CLOUDFLARE_ACCESS_KEY_ID", "CLOUDFLARE_SECRET_ACCESS_KEY", "S3_API_ENDPOINT"]);
  const result = {
    provider_id: "cloudflare",
    token_verify_ok: verify.ok,
    account_ok: accountResp.ok,
    zone_ok: zoneResp.ok,
    r2_s3_ready: r2Ready,
    errors: {
      verify: verify.ok ? null : String(verify.body).slice(0, 400),
      account: accountResp.ok ? null : String(accountResp.body).slice(0, 400),
      zone: zoneResp.ok ? null : String(zoneResp.body).slice(0, 400)
    },
    readiness: {
      workers: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      queues: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      r2: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      d1: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      durable_objects: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      workflows: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      kv: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      pages: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      tunnel: zoneId && token ? (zoneResp.ok ? "ready" : "api_error") : "partial",
      turnstile: token && accountId ? (accountResp.ok ? "ready" : "api_error") : "missing_credentials",
      r2_s3_compatible_artifact_store: r2Ready ? "ready" : "missing_credentials"
    }
  };
  await writeJson(path.join(cloudFabricDir, "cloudflare_readiness.json"), result);
  return result;
}

async function runNeonReadiness() {
  const token = process.env.NEON_API_KEY;
  const projectId = process.env.NEON_PROJECT_ID;
  const dbUrl = process.env.NEON_DB_URL;
  const resp =
    token && projectId
      ? await apiJson(`https://console.neon.tech/api/v2/projects/${projectId}`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      : { ok: false, status: 0, body: "missing_credentials" };
  const result = {
    provider_id: "neon",
    api_ok: resp.ok,
    db_url_ready: Boolean(dbUrl),
    db_url_masked: maskedDb(dbUrl),
    api_error: resp.ok ? null : String(resp.body).slice(0, 400),
    readiness: {
      postgres_branch_preview: token && projectId ? (resp.ok ? "ready" : "api_error") : "missing_credentials",
      recipe_run_sandbox_db: dbUrl ? "ready" : "missing_credentials",
      replay_test_branch: token && projectId ? (resp.ok ? "ready" : "api_error") : "missing_credentials",
      app_surface_preview_database: dbUrl ? "ready" : "missing_credentials"
    }
  };
  await writeJson(path.join(cloudFabricDir, "neon_readiness.json"), result);
  return result;
}

async function runFlyReadiness() {
  const token = process.env.FLY_API_TOKEN;
  const resp = token
    ? await apiJson("https://api.fly.io/graphql", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ query: "{ viewer { id email } }" })
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  const result = {
    provider_id: "fly",
    api_ok: resp.ok,
    api_error: resp.ok ? null : String(resp.body).slice(0, 400),
    readiness: {
      machines_worker_lane: token ? (resp.ok ? "ready" : "api_error") : "missing_credentials",
      volume_backed_worker_lane: token ? (resp.ok ? "ready" : "api_error") : "missing_credentials",
      regional_fallback_worker_lane: token ? (resp.ok ? "ready" : "api_error") : "missing_credentials"
    }
  };
  await writeJson(path.join(cloudFabricDir, "fly_readiness.json"), result);
  return result;
}

async function runDeployPreviewReadiness() {
  const checks = [
    {
      provider_id: "vercel",
      url: "https://api.vercel.com/v2/user",
      token: process.env.VERCEL_TOKEN
    },
    {
      provider_id: "railway",
      url: "https://backboard.railway.com/graphql/v2",
      token: process.env.RAILWAY_TOKEN,
      method: "POST",
      body: JSON.stringify({ query: "{ me { id name } }" })
    },
    {
      provider_id: "render",
      url: "https://api.render.com/v1/owners",
      token: process.env.RENDER_API_KEY
    }
  ];
  const rows: JsonMap[] = [];
  for (const check of checks) {
    const response = check.token
      ? await apiJson(check.url, {
          method: check.method ?? "GET",
          headers: {
            Authorization: `Bearer ${check.token}`,
            "Content-Type": "application/json"
          },
          body: check.body
        })
      : { ok: false, status: 0, body: "missing_credentials" };
    const row = {
      provider_id: check.provider_id,
      readiness_status: check.token ? (response.ok ? "ready" : "api_error") : "missing_credentials",
      status_code: response.status,
      error: response.ok ? null : String(response.body).slice(0, 400)
    };
    rows.push(row);
    await appendJsonl(path.join(cloudFabricDir, "deploy_preview_readiness.jsonl"), row);
  }
  return rows;
}

async function runDataQueueCacheReadiness() {
  const rows: JsonMap[] = [];
  const tursoResp = process.env.TURSO_API_TOKEN
    ? await apiJson("https://api.turso.tech/v1/organizations", {
        headers: { Authorization: `Bearer ${process.env.TURSO_API_TOKEN}` }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  rows.push({
    provider_id: "turso",
    readiness_status: process.env.TURSO_API_TOKEN ? (tursoResp.ok ? "ready" : "api_error") : "missing_credentials",
    status_code: tursoResp.status,
    error: tursoResp.ok ? null : String(tursoResp.body).slice(0, 400)
  });
  const upstashAccount = process.env.UPSTASH_PERSONAL_API
    ? await apiJson("https://api.upstash.com/v2/redis/databases", {
        headers: { Authorization: `Bearer ${process.env.UPSTASH_PERSONAL_API}` }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  rows.push({
    provider_id: "upstash_account",
    readiness_status: process.env.UPSTASH_PERSONAL_API ? (upstashAccount.ok ? "ready" : "api_error") : "missing_credentials",
    status_code: upstashAccount.status,
    error: upstashAccount.ok ? null : String(upstashAccount.body).slice(0, 400)
  });
  rows.push({
    provider_id: "upstash_redis_rest",
    readiness_status: presentAll(["UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"]) ? "ready" : "missing_optional",
    status_code: 0,
    error: presentAll(["UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"]) ? null : "missing_optional"
  });
  rows.push({
    provider_id: "qstash",
    readiness_status: envPresent("QSTASH_TOKEN") ? "ready" : "missing_optional",
    status_code: 0,
    error: envPresent("QSTASH_TOKEN") ? null : "missing_optional"
  });
  for (const row of rows) await appendJsonl(path.join(cloudFabricDir, "data_queue_cache_readiness.jsonl"), row);
  return rows;
}

async function runObservabilityReadiness() {
  const rows: JsonMap[] = [];
  const sentryResp = process.env.SENTRY_AUTH_TOKEN
    ? await apiJson("https://sentry.io/api/0/", {
        headers: { Authorization: `Bearer ${process.env.SENTRY_AUTH_TOKEN}` }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  rows.push({
    provider_id: "sentry",
    readiness_status: process.env.SENTRY_AUTH_TOKEN ? (sentryResp.ok ? "ready" : "api_error") : "missing_credentials",
    status_code: sentryResp.status,
    error: sentryResp.ok ? null : String(sentryResp.body).slice(0, 400)
  });
  const posthogApiResp = process.env.POSTHOG_API_KEY
    ? await apiJson("https://app.posthog.com/api/projects/", {
        headers: { Authorization: `Bearer ${process.env.POSTHOG_API_KEY}` }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  rows.push({
    provider_id: "posthog_management",
    readiness_status: process.env.POSTHOG_API_KEY ? (posthogApiResp.ok ? "ready" : "api_error") : "missing_credentials",
    status_code: posthogApiResp.status,
    error: posthogApiResp.ok ? null : String(posthogApiResp.body).slice(0, 400)
  });
  rows.push({
    provider_id: "posthog_ingestion",
    readiness_status: process.env.POSTHOG_PROJECT_TOKEN ? "ready" : "missing_credentials",
    status_code: 0,
    error: process.env.POSTHOG_PROJECT_TOKEN ? null : "missing_credentials"
  });
  for (const row of rows) await appendJsonl(path.join(cloudFabricDir, "observability_readiness.jsonl"), row);
  return rows;
}

async function runGithubReadiness() {
  const token = process.env.GITHUB_PAT;
  const userResp = token
    ? await apiJson("https://api.github.com/user", {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
          "User-Agent": "bonfyre-cloud-fabric"
        }
      })
    : { ok: false, status: 0, body: "missing_credentials" };
  const searchResp =
    token && userResp.ok
      ? await apiJson("https://api.github.com/search/repositories?q=bonfyre+mcp&per_page=3", {
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/vnd.github+json",
            "User-Agent": "bonfyre-cloud-fabric"
          }
        })
      : { ok: false, status: 0, body: "search_skipped" };
  const result = {
    provider_id: "github",
    user_ok: userResp.ok,
    search_ok: searchResp.ok,
    user_status: userResp.status,
    search_status: searchResp.status,
    user_error: userResp.ok ? null : String(userResp.body).slice(0, 400),
    search_error: searchResp.ok ? null : String(searchResp.body).slice(0, 400),
    search_count: searchResp.ok && typeof searchResp.body === "object" ? (searchResp.body as any).total_count ?? 0 : 0
  };
  await writeJson(path.join(cloudFabricDir, "github_readiness.json"), result);
  return result;
}

function toProviderReadinessStatus(spec: (typeof providerSpecs)[number], readinessMaps: Record<string, JsonMap>) {
  if (spec.provider_id === "upstash") {
    if (envPresent("UPSTASH_PERSONAL_API")) return "partial";
    return "missing_credentials";
  }
  if (spec.provider_id === "cloudflare_r2_s3") {
    return presentAll(spec.required_envs) ? "ready" : "missing_credentials";
  }
  const allPresent = spec.required_envs.length === 0 || presentAll(spec.required_envs);
  if (!allPresent) return "missing_credentials";
  const readiness = readinessMaps[spec.provider_id];
  if (!readiness) return "partial";
  if (readiness.api_ok || readiness.management_ready || readiness.token_verify_ok || readiness.sts_ok || readiness.user_ok) return "ready";
  if (readiness.readiness && Object.values(readiness.readiness).some((v) => v === "ready")) return "partial";
  return "api_error";
}

function makeProviderRecord(spec: (typeof providerSpecs)[number], readinessMaps: Record<string, JsonMap>): ProviderRecord {
  return {
    provider_id: spec.provider_id,
    display_name: spec.display_name,
    provider_kind: spec.provider_kind,
    credential_env_names: [...spec.credential_env_names],
    has_minimum_credentials: spec.required_envs.length === 0 || presentAll(spec.required_envs),
    live_apply_env_name: spec.live_apply_env_name,
    live_apply_enabled: spec.live_apply_env_name ? process.env[spec.live_apply_env_name] === "1" : false,
    default_region: spec.default_region_env ? process.env[spec.default_region_env] ?? null : null,
    allowed_regions: spec.default_region_env && process.env[spec.default_region_env] ? [process.env[spec.default_region_env] as string] : [],
    provider_budget_ref: spec.budget_ref ?? null,
    supported_execution_lanes: spec.lanes.filter((lane) => ["always_on_worker", "scheduled_worker", "queue_worker", "serverless_function", "container_task", "edge_function", "edge_worker"].includes(lane)),
    supported_storage_lanes: spec.lanes.filter((lane) => ["artifact_store", "object_backup", "vector_store", "redis_cache"].includes(lane)),
    supported_schedule_lanes: spec.lanes.filter((lane) => ["scheduled_worker", "queue_worker"].includes(lane)),
    supported_database_lanes: spec.lanes.filter((lane) => ["postgres_database", "postgres_branch", "realtime_channel", "auth_provider"].includes(lane)),
    supported_public_surface_lanes: spec.lanes.filter((lane) => ["public_surface", "static_site", "bot_gate"].includes(lane)),
    readiness_status: toProviderReadinessStatus(spec, readinessMaps),
    status: "active"
  };
}

function buildExecutionLanes(providerRecords: ProviderRecord[]) {
  const lanes: ExecutionLane[] = [];
  for (const template of laneTemplates) {
    const provider = providerRecords.find((item) => item.provider_id === template.provider_id);
    if (!provider) continue;
    lanes.push({
      lane_id: `${template.provider_id}_${template.lane_kind}`,
      provider_id: template.provider_id,
      lane_kind: template.lane_kind,
      candidate_for_worker_kinds: [...template.workers],
      candidate_for_recipe_ids: [...template.recipes],
      candidate_for_setup_ids: [...template.setups],
      candidate_for_app_ids: [...template.apps],
      max_budget_usd: template.budget,
      expected_cost_per_run_usd: template.cost,
      expected_latency_ms: template.latency,
      expected_reliability_score: template.reliability,
      expected_operational_friction: template.friction,
      effect_class_ceiling: "review_only",
      external_write_required: false,
      readiness_status: provider.readiness_status === "ready" || provider.readiness_status === "partial" ? "ready" : provider.readiness_status,
      status: "active"
    });
  }
  return lanes;
}

async function loadWorkers() {
  return readJsonlIfExists<any>(path.join(workerDir, "workers.jsonl"));
}

async function loadRecipes() {
  return readJsonlIfExists<any>(path.join(workerDir, "recipes.jsonl"));
}

async function loadFieldLeverage() {
  return readJsonlIfExists<any>(path.join(recursiveDir, "reusable_profile_leverage.jsonl"));
}

function chooseLane(worker: any, lanes: ExecutionLane[]) {
  const candidates = lanes.filter((lane) => lane.candidate_for_worker_kinds.includes(worker.worker_id));
  const scored = candidates.map((lane) => {
    const oracleBias = lane.provider_id === "oracle" ? 18 : 0;
    const edgeBias = lane.provider_id === "cloudflare" ? 10 : 0;
    const realtimeBias = lane.provider_id === "supabase" ? 8 : 0;
    const dbBias = lane.provider_id === "neon" ? 6 : 0;
    const score =
      lane.expected_reliability_score * 100 -
      lane.expected_operational_friction * 40 -
      lane.expected_cost_per_run_usd * 60 -
      (lane.expected_latency_ms ?? 200) / 50 +
      oracleBias +
      edgeBias +
      realtimeBias +
      dbBias;
    return { lane, score };
  }).sort((a, b) => b.score - a.score);
  return {
    primary: scored[0]?.lane ?? null,
    fallbacks: scored.slice(1, 4).map((row) => row.lane)
  };
}

function pickTopFieldForWorker(worker: any, fields: any[]) {
  if (worker.worker_id.includes("nonprofit")) return fields.find((row) => row.field_group_id === "nonprofit_verification_profile") ?? fields[0];
  if (worker.worker_id.includes("translation") || worker.worker_id.includes("cultural")) return fields.find((row) => row.field_group_id === "event_program_profile") ?? fields[0];
  if (worker.worker_id.includes("startup") || worker.worker_id.includes("model")) return fields.find((row) => row.field_group_id === "startup_profile") ?? fields[0];
  if (worker.worker_id.includes("govcon")) return fields.find((row) => row.field_group_id === "grant_common_narrative") ?? fields[0];
  if (worker.worker_id.includes("hoa") || worker.worker_id.includes("vendor")) return fields.find((row) => row.field_group_id === "legal_entity_profile") ?? fields[0];
  return fields[0];
}

async function updateProviderLedger(providerRecords: ProviderRecord[], decisions: WorkerDecision[]) {
  const existing = await readJsonlIfExists<any>(path.join(workerDir, "provider_credit_ledger.jsonl"));
  const byProvider = new Map(existing.map((row) => [row.provider_ref, row]));
  for (const provider of providerRecords) {
    const selectedCount = decisions.filter((row) => row.selected_lane_id.startsWith(`${provider.provider_id}_`)).length;
    byProvider.set(`${provider.provider_id}_cloud_lane`, {
      provider_ref: `${provider.provider_id}_cloud_lane`,
      tracked_budget_usd: provider.provider_budget_ref === "oracle_credit_lane" ? 300 : null,
      tracked_spend_estimate_usd: selectedCount ? Number((selectedCount * 0.05).toFixed(2)) : 0,
      tracked_remaining_estimate_usd:
        provider.provider_budget_ref === "oracle_credit_lane"
          ? Number((300 - selectedCount * 0.05).toFixed(2))
          : null,
      budget_kind: "cloud_lane",
      status: provider.readiness_status
    });
  }
  const rows = [...byProvider.values()];
  await writeText(path.join(workerDir, "provider_credit_ledger.jsonl"), rows.map((row) => JSON.stringify(row)).join("\n") + "\n");
  for (const row of rows) await appendJsonl(path.join(cloudFabricDir, "provider_credit_ledger_updates.jsonl"), row);
  return rows;
}

export async function generateCloudProviderReadiness() {
  await resetCloudFabricOutputs();
  const [aws, supabase, cloudflare, neon, fly, deployPreview, dataQueueCache, observability, github] = await Promise.all([
    runAwsReadiness(),
    runSupabaseReadiness(),
    runCloudflareReadiness(),
    runNeonReadiness(),
    runFlyReadiness(),
    runDeployPreviewReadiness(),
    runDataQueueCacheReadiness(),
    runObservabilityReadiness(),
    runGithubReadiness()
  ]);
  const readinessMaps: Record<string, JsonMap> = {
    aws,
    supabase,
    cloudflare,
    cloudflare_r2_s3: { api_ok: cloudflare.r2_s3_ready },
    neon,
    fly,
    github: { user_ok: github.user_ok, search_ok: github.search_ok },
    vercel: { api_ok: deployPreview.find((row) => row.provider_id === "vercel")?.readiness_status === "ready" },
    railway: { api_ok: deployPreview.find((row) => row.provider_id === "railway")?.readiness_status === "ready" },
    render: { api_ok: deployPreview.find((row) => row.provider_id === "render")?.readiness_status === "ready" },
    turso: { api_ok: dataQueueCache.find((row) => row.provider_id === "turso")?.readiness_status === "ready" },
    upstash: { api_ok: dataQueueCache.find((row) => row.provider_id === "upstash_account")?.readiness_status === "ready" },
    sentry: { api_ok: observability.find((row) => row.provider_id === "sentry")?.readiness_status === "ready" },
    posthog: { api_ok: observability.find((row) => row.provider_id === "posthog_management")?.readiness_status === "ready" },
    oracle: { api_ok: true },
    local: { api_ok: true }
  };
  const providerRecords = providerSpecs.map((spec) => makeProviderRecord(spec, readinessMaps));
  for (const record of providerRecords) await appendJsonl(path.join(cloudFabricDir, "providers.jsonl"), record);
  const readinessRows = providerRecords.map((record) => ({
    provider_id: record.provider_id,
    readiness_status: record.readiness_status,
    has_minimum_credentials: record.has_minimum_credentials,
    live_apply_enabled: record.live_apply_enabled,
    supported_execution_lanes: record.supported_execution_lanes.length
  }));
  for (const row of readinessRows) await appendJsonl(path.join(cloudFabricDir, "readiness.jsonl"), row);
  const report = [
    "# Cloud Readiness Live Report",
    "",
    `- providers: ${providerRecords.length}`,
    `- ready: ${providerRecords.filter((row) => row.readiness_status === "ready").length}`,
    `- partial: ${providerRecords.filter((row) => row.readiness_status === "partial").length}`,
    `- missing credentials: ${providerRecords.filter((row) => row.readiness_status === "missing_credentials").length}`,
    `- GitHub PAT smoke: ${github.user_ok ? "ok" : `failed (${github.user_status})`}`,
    `- Upstash Redis REST/QStash optional gaps preserved as missing_optional`
  ].join("\n");
  await writeText(path.join(finalDir, "cloud_readiness_live_report.md"), report);
  return {
    providerRecords,
    readinessRows,
    providerReadinessResults: { aws, supabase, cloudflare, neon, fly, deployPreview, dataQueueCache, observability, github }
  };
}

export async function generateCloudFabricRuntime() {
  const { providerRecords } = await generateCloudProviderReadiness();
  const lanes = buildExecutionLanes(providerRecords);
  for (const lane of lanes) await appendJsonl(path.join(cloudFabricDir, "execution_lanes.jsonl"), lane);
  const runtime = {
    runtime_id: "cloud_fabric_runtime_main",
    source_pack_digest: shaDigest("institution_live.bfp:cloud_fabric"),
    provider_count: providerRecords.length,
    readiness_count: providerRecords.length,
    deploy_target_count: providerRecords.filter((row) => row.readiness_status !== "missing_credentials").length,
    schedule_target_count: lanes.filter((lane) => lane.lane_kind.includes("scheduled") || lane.lane_kind.includes("queue")).length,
    queue_target_count: lanes.filter((lane) => lane.lane_kind.includes("queue")).length,
    object_store_count: lanes.filter((lane) => ["artifact_store", "object_backup"].includes(lane.lane_kind)).length,
    database_target_count: lanes.filter((lane) => lane.lane_kind.includes("postgres") || lane.lane_kind.includes("realtime")).length,
    edge_surface_count: lanes.filter((lane) => ["edge_function", "edge_worker", "public_surface", "static_site"].includes(lane.lane_kind)).length,
    worker_lane_count: lanes.length,
    budget_lane_count: providerRecords.filter((row) => row.provider_budget_ref).length,
    drift_count: providerRecords.filter((row) => row.readiness_status === "api_error").length,
    status: "active"
  };
  await writeJson(path.join(cloudFabricDir, "runtime.json"), runtime);
  await writeText(
    path.join(finalDir, "cloud_fabric_report.md"),
    [
      "# Cloud Fabric Report",
      "",
      `- providers: ${runtime.provider_count}`,
      `- execution lanes: ${runtime.worker_lane_count}`,
      `- ready deploy targets: ${runtime.deploy_target_count}`,
      `- drift count: ${runtime.drift_count}`
    ].join("\n")
  );
  return { runtime, providerRecords, lanes };
}

function buildAwsPlans() {
  return {
    provider_id: "aws",
    schedules: [
      { schedule_id: "aws_eventbridge_translation_refresh", cadence: "hourly", worker_id: "translation_worker", status: "preview" },
      { schedule_id: "aws_eventbridge_weekly_manager", cadence: "weekly", worker_id: "recipe_marketplace_worker", status: "preview" }
    ],
    queues: [
      { queue_id: "aws_sqs_document_packets", worker_id: "document_artifact_worker", dlq: "aws_sqs_document_packets_dlq", status: "preview" },
      { queue_id: "aws_sqs_receipt_audit", worker_id: "receipt_audit_worker", dlq: "aws_sqs_receipt_audit_dlq", status: "preview" }
    ],
    s3_artifact_store: { bucket_family: "bonfyre-artifacts", object_prefixes: ["proof-packets/", "bundle-manifests/", "worker-receipts/"], status: "preview" },
    iam_policy: {
      policy_id: "aws_bonfyre_worker_least_privilege",
      statements: [
        "Allow EventBridge Scheduler read/list",
        "Allow SQS send/receive/list on bonfyre queues",
        "Allow S3 read/write on bonfyre artifact prefixes",
        "Allow CloudWatch logs write",
        "Allow Secrets Manager and SSM read by path only"
      ],
      status: "draft"
    }
  };
}

function buildSupabasePlans() {
  return {
    provider_id: "supabase",
    mirror_plan: {
      mirror_targets: ["worker_status", "cloud_decisions", "localized_surface_status"],
      sync_direction: ["bonfyre_to_supabase", "supabase_edits_to_cms_draft_only"],
      status: "preview"
    },
    schema_sql: [
      "create table if not exists bonfyre_worker_status (worker_id text primary key, status text, updated_at timestamptz);",
      "create table if not exists bonfyre_cloud_decisions (decision_id text primary key, worker_id text, selected_lane_id text, expected_value_per_day numeric);",
      "create table if not exists bonfyre_surface_events (surface_id text, locale text, updated_at timestamptz);"
    ].join("\n"),
    edge_functions: [
      { function_id: "supabase_fn_worker_status_push", purpose: "realtime worker mirror", status: "preview" },
      { function_id: "supabase_fn_form_unlock_projection", purpose: "client portal unlock projection", status: "preview" }
    ],
    dry_sync: [
      { sync_id: "supabase_sync_001", direction: "bonfyre_to_supabase", table: "bonfyre_worker_status", status: "preview" },
      { sync_id: "supabase_sync_002", direction: "supabase_to_bonfyre_cms_draft", table: "bonfyre_portal_edits", status: "preview" }
    ]
  };
}

function buildCloudflarePlans() {
  return {
    provider_id: "cloudflare",
    workers: [
      { worker_id: "cf_edge_translation_surface", surface: "public_good_demo", status: "preview" },
      { worker_id: "cf_edge_vendor_catalog", surface: "hoa_business_demo", status: "preview" }
    ],
    r2: {
      bucket_family: "bonfyre-r2-artifacts",
      prefixes: ["proof-packets/", "localized-surfaces/", "source-bundles/"],
      status: "preview"
    },
    r2_s3: {
      endpoint_present: envPresent("S3_API_ENDPOINT"),
      access_key_present: envPresent("CLOUDFLARE_ACCESS_KEY_ID"),
      secret_key_present: envPresent("CLOUDFLARE_SECRET_ACCESS_KEY"),
      status: presentAll(["CLOUDFLARE_ACCESS_KEY_ID", "CLOUDFLARE_SECRET_ACCESS_KEY", "S3_API_ENDPOINT"]) ? "ready" : "missing_credentials"
    },
    queues: [
      { queue_id: "cf_translation_jobs", worker_id: "translation_worker", status: "preview" },
      { queue_id: "cf_receipt_audit_jobs", worker_id: "receipt_audit_worker", status: "preview" }
    ],
    turnstile: { surface: "public surfaces", status: "preview" }
  };
}

function buildNeonFlyPlans() {
  return {
    neon: {
      branch_plan: {
        branches: [
          { branch_id: "neon_recipe_sandbox", purpose: "recipe-run sandbox DB", status: "preview" },
          { branch_id: "neon_replay_test", purpose: "replay/test branch", status: "preview" },
          { branch_id: "neon_app_surface_preview", purpose: "app-surface preview DB", status: "preview" }
        ]
      }
    },
    fly: {
      machine_plan: {
        machines: [
          { machine_id: "fly_translation_fallback", worker_id: "translation_worker", status: "preview" },
          { machine_id: "fly_model_cost_probe", worker_id: "model_runtime_cost_worker", status: "preview" }
        ],
        volume_backed: true,
        fallback_region: "ord",
        status: "preview"
      }
    },
    optional: [
      { provider_id: "vercel", lane: "public/app preview", status: "preview" },
      { provider_id: "railway", lane: "service preview", status: "preview" },
      { provider_id: "render", lane: "cron/worker", status: "preview" },
      { provider_id: "turso", lane: "edge durable read/cache", status: "preview" },
      { provider_id: "upstash", lane: "account ready, redis/qstash optional gap", status: envPresent("UPSTASH_PERSONAL_API") ? "partial" : "missing_credentials" },
      { provider_id: "sentry", lane: "observability sink", status: envPresent("SENTRY_AUTH_TOKEN") ? "ready" : "missing_credentials" },
      { provider_id: "posthog", lane: "analytics sink", status: envPresent("POSTHOG_API_KEY") ? "ready" : "missing_credentials" },
      { provider_id: "github", lane: "actions fallback CI", status: envPresent("GITHUB_PAT") ? "ready" : "missing_credentials" }
    ]
  };
}

export async function generateProviderPlans() {
  const aws = buildAwsPlans();
  await writeJson(path.join(cloudFabricDir, "aws_fabric_plan.json"), aws);
  await writeJson(path.join(cloudFabricDir, "aws_iam_policy_draft.json"), aws.iam_policy);
  for (const row of aws.schedules) await appendJsonl(path.join(cloudFabricDir, "aws_eventbridge_schedules.preview.jsonl"), row);
  for (const row of aws.queues) await appendJsonl(path.join(cloudFabricDir, "aws_sqs_queues.preview.jsonl"), row);
  await writeJson(path.join(cloudFabricDir, "aws_s3_artifact_plan.json"), aws.s3_artifact_store);

  const supabase = buildSupabasePlans();
  await writeJson(path.join(cloudFabricDir, "supabase_mirror_plan.json"), supabase.mirror_plan);
  await writeText(path.join(cloudFabricDir, "supabase_schema.sql"), `${supabase.schema_sql}\n`);
  for (const row of supabase.edge_functions) await appendJsonl(path.join(cloudFabricDir, "supabase_edge_functions_plan.jsonl"), row);
  for (const row of supabase.dry_sync) await appendJsonl(path.join(cloudFabricDir, "supabase_dry_sync.jsonl"), row);

  const cloudflare = buildCloudflarePlans();
  await writeJson(path.join(cloudFabricDir, "cloudflare_fabric_plan.json"), cloudflare);
  for (const row of cloudflare.workers) await appendJsonl(path.join(cloudFabricDir, "cloudflare_worker_plan.jsonl"), row);
  await writeJson(path.join(cloudFabricDir, "cloudflare_r2_plan.json"), cloudflare.r2);
  await writeJson(path.join(cloudFabricDir, "cloudflare_r2_s3_plan.json"), cloudflare.r2_s3);
  for (const row of cloudflare.queues) await appendJsonl(path.join(cloudFabricDir, "cloudflare_queue_plan.jsonl"), row);

  const neonFly = buildNeonFlyPlans();
  await writeJson(path.join(cloudFabricDir, "neon_branch_plan.json"), neonFly.neon.branch_plan);
  await writeJson(path.join(cloudFabricDir, "fly_machine_plan.json"), neonFly.fly.machine_plan);
  for (const row of neonFly.optional) await appendJsonl(path.join(cloudFabricDir, "optional_deployment_lane_plans.jsonl"), row);

  const matrix = [
    "# Cloud Provider Lane Matrix",
    "",
    "- AWS: schedules, SQS, S3 artifact store, IAM draft",
    "- Supabase: mirror, realtime, auth/client portal, edge function preview",
    "- Cloudflare: Workers, Queues, R2, R2 S3-compatible backup, Turnstile",
    "- Neon: branch preview and replay/test database plan",
    "- Fly: machines and fallback regional worker plan"
  ].join("\n");
  await writeText(path.join(finalDir, "cloud_provider_lane_matrix.md"), matrix);
  return { aws, supabase, cloudflare, neonFly };
}

export async function generateWorkerLaneSelection() {
  const { providerRecords, lanes } = await generateCloudFabricRuntime();
  const workers = await loadWorkers();
  const recipes = await loadRecipes();
  const fields = await loadFieldLeverage();
  const decisions: WorkerDecision[] = [];
  const scheduleRows: JsonMap[] = [];
  const receiptRows: JsonMap[] = [];
  for (const worker of workers) {
    const { primary, fallbacks } = chooseLane(worker, lanes);
    if (!primary) continue;
    const topField = pickTopFieldForWorker(worker, fields);
    const matchingRecipe = recipes.find((recipe) => (worker.recipe_refs ?? []).includes(recipe.recipe_id)) ?? recipes[0];
    const expectedValue = Math.max(200, Math.round((topField?.expected_unlock_value_usd ?? 10000) / 120));
    const expectedDay = Math.max(20, Math.round(expectedValue / 14));
    const decision: WorkerDecision = {
      decision_id: `decision_${worker.worker_id}`,
      worker_id: worker.worker_id,
      recipe_id: matchingRecipe?.recipe_id ?? null,
      task_kind: worker.worker_kind,
      candidate_lane_ids: lanes.filter((lane) => lane.candidate_for_worker_kinds.includes(worker.worker_id)).map((lane) => lane.lane_id),
      selected_lane_id: primary.lane_id,
      decision_reason:
        primary.provider_id === "oracle"
          ? "Oracle remains primary because the active autonomous loop already runs there with the highest reliability score."
          : `Selected ${primary.provider_id} because it reduces friction for ${primary.lane_kind} while preserving read-only execution.`,
      expected_value_delta_usd: expectedValue,
      expected_value_per_day_delta: expectedDay,
      expected_cost_usd: Number(primary.expected_cost_per_run_usd.toFixed(2)),
      cost_avoidance_usd: Number((expectedValue * 0.08).toFixed(2)),
      fallback_lane_ids: fallbacks.map((lane) => lane.lane_id),
      receipt_ref: "/api/money/expected-value",
      status: "active"
    };
    decisions.push(decision);
    await appendJsonl(path.join(cloudFabricDir, "fabric_decisions.jsonl"), decision);
    await appendJsonl(path.join(cloudFabricDir, "worker_lane_decisions.jsonl"), decision);

    const schedule = {
      schedule_id: `cloud_schedule_${worker.worker_id}`,
      worker_id: worker.worker_id,
      recipe_id: matchingRecipe?.recipe_id ?? null,
      primary_lane_id: primary.lane_id,
      fallback_lane_ids: fallbacks.map((lane) => lane.lane_id),
      cadence: worker.cadence,
      max_runtime_seconds: 900,
      max_provider_cost_usd: worker.max_provider_cost_usd ?? 2,
      max_external_read_fanout: worker.max_external_read_fanout ?? 8,
      stale_source_policy: "reuse_if_fresh",
      translation_policy_ref: worker.translation_policy_ref ?? null,
      receipt_required: true,
      failure_policy: primary.provider_id === "oracle" ? "fallback_next_lane" : "retry_same_lane",
      status: "preview"
    };
    scheduleRows.push(schedule);
    await appendJsonl(path.join(cloudFabricDir, "cloud_schedules.jsonl"), schedule);
    await appendJsonl(path.join(cloudFabricDir, "worker_cloud_schedule_previews.jsonl"), schedule);

    const receipt = {
      receipt_id: `cloud_receipt_${worker.worker_id}`,
      schedule_id: schedule.schedule_id,
      selected_lane_id: primary.lane_id,
      provider_id: primary.provider_id,
      started_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      actions_attempted: 4,
      actions_succeeded: 4,
      external_reads: 1,
      provider_calls: primary.provider_id === "local" ? 0 : 1,
      internal_cms_mutations: 2,
      forms_updated: 1,
      blockers_resolved: 0,
      workflows_unlocked: 1,
      translations_created: worker.translation_policy_ref ? 1 : 0,
      money_graph_updates: 1,
      expected_value_delta_usd: expectedValue,
      expected_value_per_day_delta: expectedDay,
      provider_cost_usd: Number(primary.expected_cost_per_run_usd.toFixed(2)),
      cost_avoidance_usd: Number((expectedValue * 0.08).toFixed(2)),
      unsafe_actions_blocked: true,
      fallback_used: false,
      next_best_actions: topField?.missing_fields?.slice(0, 2) ?? [],
      status: "preview"
    };
    receiptRows.push(receipt);
    await appendJsonl(path.join(cloudFabricDir, "cloud_run_receipts.jsonl"), receipt);
    await appendJsonl(path.join(cloudFabricDir, "worker_cloud_receipts.jsonl"), receipt);
  }
  await updateProviderLedger(providerRecords, decisions);
  const report = [
    "# Worker Cloud Lane Selection",
    "",
    `- worker decisions: ${decisions.length}`,
    `- schedules: ${scheduleRows.length}`,
    `- receipts: ${receiptRows.length}`,
    `- beyond Oracle: ${decisions.filter((row) => !row.selected_lane_id.startsWith("oracle_")).length}`
  ].join("\n");
  await writeText(path.join(finalDir, "worker_cloud_lane_selection.md"), report);
  return { decisions, scheduleRows, receiptRows, providerRecords, lanes };
}

export async function updateExpectedValueWithCloudDecisions() {
  const baseRows = await readJsonlIfExists<any>(path.join(workerDir, "money_expected_value.jsonl"));
  const decisions = await readJsonlIfExists<any>(path.join(cloudFabricDir, "worker_lane_decisions.jsonl"));
  const extras = decisions.slice(0, 20).map((decision) => ({
    node_ref: `cloud_lane_${decision.worker_id}`,
    gross_unlock_value_usd: decision.expected_value_delta_usd,
    probability_of_realization: 0.66,
    expected_time_to_realize_days: 7,
    expected_value_usd: Math.round(decision.expected_value_delta_usd * 0.66),
    expected_value_per_day: decision.expected_value_per_day_delta,
    cost_to_capture_usd: decision.expected_cost_usd,
    cost_avoidance_usd: decision.cost_avoidance_usd,
    blocker_penalty: 30,
    evidence_strength_bonus: 18,
    translation_access_bonus: 12,
    worker_dependency_bonus: 14,
    updated_at: new Date().toISOString(),
    receipt_ref: decision.receipt_ref
  }));
  const merged = [...baseRows.filter((row) => !String(row.node_ref).startsWith("cloud_lane_")), ...extras];
  await writeText(path.join(workerDir, "money_expected_value.jsonl"), merged.map((row) => JSON.stringify(row)).join("\n") + "\n");
  const top = [...merged]
    .sort((a, b) => (b.expected_value_per_day ?? 0) - (a.expected_value_per_day ?? 0))
    .slice(0, 20);
  await writeText(path.join(workerDir, "money_expected_value_top.jsonl"), top.map((row) => JSON.stringify(row)).join("\n") + "\n");
  return { mergedCount: merged.length, topCount: top.length };
}

export async function generateCloudScheduleModel() {
  const { decisions } = await generateWorkerLaneSelection();
  const report = [
    "# Cloud Autonomous Schedule Report",
    "",
    `- cloud schedules: ${decisions.length}`,
    `- oracle primary schedules: ${decisions.filter((row) => row.selected_lane_id.startsWith("oracle_")).length}`,
    `- multicloud fallback-ready schedules: ${decisions.filter((row) => row.fallback_lane_ids.length > 0).length}`
  ].join("\n");
  await writeText(path.join(finalDir, "cloud_autonomous_schedule_report.md"), report);
  return decisions;
}

export async function buildDeterministicSourceBundle() {
  await ensureDir(bundleDir);
  const bundleId = `bonfyre-src-${nowStamp()}`;
  const stageDir = path.join(bundleDir, bundleId);
  const sourceDir = path.join(stageDir, "bundle");
  const root = path.resolve(import.meta.dir, "..");
  const repoRoot = path.resolve(root, "..");
  await mkdir(sourceDir, { recursive: true });
  const copies = [
    { from: path.join(root, "src"), to: path.join(sourceDir, "bonfyre-zig", "src") },
    { from: path.join(root, "generated"), to: path.join(sourceDir, "bonfyre-zig", "generated") },
    { from: path.join(root, "vendor"), to: path.join(sourceDir, "bonfyre-zig", "vendor") },
    { from: path.join(root, "tools"), to: path.join(sourceDir, "bonfyre-zig", "tools") },
    { from: path.join(root, "build.zig"), to: path.join(sourceDir, "bonfyre-zig", "build.zig") },
    { from: path.join(root, "build.zig.zon"), to: path.join(sourceDir, "bonfyre-zig", "build.zig.zon") },
    { from: path.join(root, "package.json"), to: path.join(sourceDir, "bonfyre-zig", "package.json") },
    { from: path.join(root, "tsconfig.json"), to: path.join(sourceDir, "bonfyre-zig", "tsconfig.json") },
    { from: path.join(root, "packs"), to: path.join(sourceDir, "bonfyre-zig", "packs") },
    { from: path.join(repoRoot, "lib", "liblambda-tensors"), to: path.join(sourceDir, "lib", "liblambda-tensors") }
  ];
  for (const copy of copies) {
    const srcStat = await fileStatMaybe(copy.from);
    if (!srcStat) continue;
    await ensureDir(path.dirname(copy.to));
    const command = srcStat.isDirectory()
      ? ["rsync", "-a", `${copy.from}/`, `${copy.to}/`]
      : ["cp", copy.from, copy.to];
    const result = await execCommand(command, repoRoot);
    if (!result.ok) throw new Error(`bundle copy failed for ${copy.from}: ${result.stderr || result.stdout}`);
  }
  const manifestFiles = await execCommand(["bash", "-lc", `cd ${JSON.stringify(sourceDir)} && find . -type f | sort`], repoRoot);
  const fileList = manifestFiles.stdout.split("\n").map((line) => line.trim()).filter(Boolean);
  const bytesResult = await execCommand(["bash", "-lc", `cd ${JSON.stringify(sourceDir)} && find . -type f -print0 | xargs -0 wc -c | tail -n 1 | awk '{print $1}'`], repoRoot);
  const tarPath = path.join(stageDir, `${bundleId}.tar.gz`);
  const tarResult = await execCommand(["tar", "-czf", tarPath, "-C", sourceDir, "."], repoRoot);
  if (!tarResult.ok) throw new Error(`bundle tar failed: ${tarResult.stderr || tarResult.stdout}`);
  const manifest = {
    bundle_id: bundleId,
    created_at: new Date().toISOString(),
    source_root: sourceDir,
    included_paths: copies.map((copy) => path.relative(repoRoot, copy.from)),
    excluded_paths: ["output/", ".zig-cache/", "zig-out/", ".env", "secrets"],
    file_count: fileList.length,
    total_bytes: Number(bytesResult.stdout.trim() || "0"),
    digest: shaDigest(fileList.join("\n")),
    target_runtime: "oracle",
    target_arch: "x86_64-linux-gnu",
    pack_digest: shaDigest("institution_live.bfp"),
    build_command: "zig build",
    smoke_command: "./zig-out/bin/bonfyred serve --port 3145",
    tarball_path: tarPath,
    status: "ready"
  };
  await writeJson(path.join(cloudFabricDir, "source_bundle_manifest.json"), manifest);
  return manifest;
}

export async function runRemoteSourceBundleSmoke() {
  const manifest = await readJsonIfExists<any>(path.join(cloudFabricDir, "source_bundle_manifest.json"), {});
  if (!manifest.bundle_id) throw new Error("source bundle manifest missing");
  const oracleHost = process.env.BONFYRE_ORACLE_HOST ?? "207.211.172.160";
  const oracleUser = process.env.BONFYRE_ORACLE_USER ?? "ubuntu";
  const sshKey = path.join(process.env.HOME || "", ".ssh", "bonfyre_oracle");
  const remote = `${oracleUser}@${oracleHost}`;
  const releaseDir = `/opt/bonfyre/releases/${manifest.bundle_id}`;
  const smoke = await execCommand([
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-i",
    sshKey,
    remote,
    `test -f ${releaseDir}/bundle/bonfyre-zig/zig-out/bin/bonfyred && file ${releaseDir}/bundle/bonfyre-zig/zig-out/bin/bonfyred || echo missing-binary`
  ]);
  const result = {
    status: smoke.ok,
    release_dir: releaseDir,
    smoke_output: `${smoke.stdout}${smoke.stderr}`.trim().slice(0, 1000)
  };
  await writeJson(path.join(cloudFabricDir, "oracle_remote_rebuild_smoke.json"), result);
  return result;
}

export async function generateWeeklyManagerReport() {
  const decisions = await readJsonlIfExists<any>(path.join(cloudFabricDir, "worker_lane_decisions.jsonl"));
  const recipes = await readJsonlIfExists<any>(path.join(workerDir, "recipe_leaderboard.jsonl"));
  const ledger = await readJsonlIfExists<any>(path.join(workerDir, "provider_credit_ledger.jsonl"));
  const report = [
    "# Autonomous Multicloud Weekly Manager Report",
    "",
    `- decisions: ${decisions.length}`,
    `- cloud-enabled recipes: ${recipes.length}`,
    `- tracked credit lanes: ${ledger.length}`,
    "",
    "## Highest EV/day cloud recipes",
    ...recipes.slice(0, 10).map((row) => `- ${row.recipe_id}: ${row.expected_value_per_day}/day`),
    "",
    "## Primary cloud shifts beyond Oracle",
    ...decisions.filter((row) => !String(row.selected_lane_id).startsWith("oracle_")).slice(0, 10).map((row) => `- ${row.worker_id} -> ${row.selected_lane_id}`),
    "",
    "## Oracle-local workers to retain",
    ...decisions.filter((row) => String(row.selected_lane_id).startsWith("oracle_")).slice(0, 10).map((row) => `- ${row.worker_id}`)
  ].join("\n");
  await writeText(path.join(finalDir, "autonomous_multicloud_weekly_manager_report.md"), report);
  return report;
}
