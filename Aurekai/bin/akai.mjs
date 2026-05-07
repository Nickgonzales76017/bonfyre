#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { VERSION } from "../src/version.mjs";
import {
  runKernelIntegration,
  runKernelSurfaceBatch,
  runKernelStateMachineBatch,
  listKernelTargets,
} from "@aurekai/continuity-core";
import {
  ingestIntegrationBatch,
  ingestIntegrationTarget,
  listEndpointTemplates,
  listConnectorPresets,
  validateIntegrationEvent,
} from "../src/integration-connectors.mjs";
import { scaffoldIntegrationRepos } from "../src/integration-repo-scaffold.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(__dirname);

function printHelp() {
  console.log(`${VERSION.product} CLI v${VERSION.release}`);
  console.log(`legacy codename: ${VERSION.legacyCodename}`);
  console.log("");
  console.log("Usage:");
  console.log("  akai doctor --deep");
  console.log("  akai dashboard");
  console.log("  akai run <recipe> [--input FILE] [--sae-audit] [--semantic-cache]");
  console.log("  akai install --user|--system [--service]");
  console.log("  akai uninstall --user|--system [--service]");
  console.log("  akai sae:activate ...");
  console.log("  akai model:inspect ...");
  console.log("  akai fpqx:align-sae ...");
  console.log("  akai integrations [--json]");
  console.log("  akai forks [--json]");
  console.log("  akai integrate:list [--surface hard-fork|soft-fork|adapter|observer|state-machine] [--group NAME] [--json]");
  console.log("  akai integrate:run --target NAME [--event-json JSON|--event-file FILE] [--enforce-doctrine] [--persist] [--json]");
  console.log("  akai integrate:batch --surface hard-fork|soft-fork|adapter|observer|state-machine [--event-json JSON|--event-file FILE] [--enforce-doctrine] [--persist] [--json]");
  console.log("  akai integrate:state-machine --type TYPE [--event-json JSON|--event-file FILE] [--enforce-doctrine] [--persist] [--json]");
  console.log("  akai integrate:ingest --target NAME [--endpoint URL|--input-file FILE|--event-json JSON] [--use-template ID] [--environment NAME] [--request-body-json JSON|--request-body-file FILE] [--strict] [--persist] [--json]");
  console.log("  akai integrate:ingest-batch --surface SURFACE [--group NAME] [--endpoint URL|--input-file FILE|--event-json JSON] [--use-template ID] [--environment NAME] [--request-body-json JSON|--request-body-file FILE] [--strict] [--persist] [--json]");
  console.log("  akai integrate:presets [--target NAME] [--json]");
  console.log("  akai integrate:templates [--target NAME] [--environment NAME] [--json]");
  console.log("  akai integrate:validate-event --target NAME [--input-file FILE|--event-json JSON] [--json]");
  console.log("  akai integrate:scaffold-repos [--target NAME|--all] [--out-dir DIR] [--json]");
  console.log("");
  console.log("Grouped compatibility commands:");
  console.log("  akai sae activate ...   -> akai sae:activate ...");
  console.log("  akai model inspect ...  -> akai model:inspect ...");
  console.log("  akai fpqx align-sae ... -> akai fpqx:align-sae ...");
  console.log("");
  console.log("Compatibility:");
  console.log("  bonfyre       -> akai");
  console.log("  bonfyre-hyper -> akai");
  console.log("  .akmodel/.aksae/.akfpqx are first-class; .bfmodel/.bfsae/.bffpqx remain supported during migration");
}

function printRegistry(path, asJson) {
  const payload = readFileSync(path, "utf8");
  if (asJson) {
    process.stdout.write(payload.endsWith("\n") ? payload : `${payload}\n`);
    return;
  }
  const data = JSON.parse(payload);
  process.stdout.write(`${JSON.stringify(data, null, 2)}\n`);
}

function getFlagValue(argv, flagName) {
  const eqPrefix = `${flagName}=`;
  const eqArg = argv.find((arg) => arg.startsWith(eqPrefix));
  if (eqArg) return eqArg.slice(eqPrefix.length);

  const idx = argv.indexOf(flagName);
  if (idx >= 0 && idx + 1 < argv.length) return argv[idx + 1];
  return null;
}

function parseEventPayload(argv) {
  const eventJson = getFlagValue(argv, "--event-json");
  const eventFile = getFlagValue(argv, "--event-file");

  if (eventJson) return JSON.parse(eventJson);
  if (eventFile) return JSON.parse(readFileSync(eventFile, "utf8"));

  return {
    event_id: `evt-${Date.now()}`,
    action: "observe",
    source: "akai-cli",
    risk_score: 0,
    deformation_score: 0,
    witness_skew: 0,
  };
}

function parseRequestBodyPayload(argv) {
  const bodyJson = getFlagValue(argv, "--request-body-json");
  const bodyFile = getFlagValue(argv, "--request-body-file");

  if (bodyJson) return JSON.parse(bodyJson);
  if (bodyFile) return JSON.parse(readFileSync(bodyFile, "utf8"));
  return null;
}

function printPayload(payload, asJson) {
  if (asJson) {
    process.stdout.write(`${JSON.stringify(payload)}\n`);
    return;
  }
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
}

function resolveDefaultDict(modelMemoryRoot) {
  const candidates = ["default.aksae", "default.bfsae"];
  for (const candidate of candidates) {
    const dict = join(modelMemoryRoot, candidate);
    if (existsSync(dict)) return dict;
  }
  return null;
}

function normalizeArgs(argv) {
  if (argv.length >= 2 && ["sae", "model", "fpqx", "query", "family", "cache"].includes(argv[0])) {
    return [`${argv[0]}:${argv[1]}`, ...argv.slice(2)];
  }
  return argv;
}

function resolveLegacyBinary(command) {
  const envHyper = process.env.AKAI_HYPER || process.env.AUREKAI_HYPER || process.env.BONFYRE_HYPER;
  if (envHyper) return { bin: envHyper, args: [command] };

  const localCompiled = join(repoRoot, "..", "dist", `bonfyre-hyper-v0.7.0-bun-${process.platform}-${process.arch}`);
  if (existsSync(localCompiled)) return { bin: localCompiled, args: [command] };

  const localHyperTs = join(repoRoot, "..", "bonfyre-hyper", "src", "hyper.ts");
  const bunBin = process.env.BUN_BIN || join(process.env.HOME || "/tmp", ".bun", "bin", "bun");
  if (existsSync(localHyperTs) && existsSync(bunBin)) {
    return { bin: bunBin, args: ["run", localHyperTs, command] };
  }

  return { bin: "bonfyre-hyper", args: [command] };
}

function detectLegacyEnv() {
  const bonfyreRoot = join(repoRoot, "..");
  const packagedRuntime = process.env.AUREKAI_RUNTIME || process.env.BONFYRE_RUNTIME || join(bonfyreRoot, "dist", "bonfyre-appliance");
  const packagedManifest = join(packagedRuntime, "runtime", "bonfyre.manifest.json");
  const packagedModelMemory = process.env.AUREKAI_MODEL_MEMORY || process.env.BONFYRE_MODEL_MEMORY || join(packagedRuntime, "model-memory");
  const env = { ...process.env };

  if (!env.BONFYRE_RUNTIME && existsSync(packagedManifest)) {
    env.BONFYRE_RUNTIME = packagedRuntime;
  }
  if (!env.AUREKAI_RUNTIME && existsSync(packagedManifest)) {
    env.AUREKAI_RUNTIME = packagedRuntime;
  }
  if (!env.BONFYRE_MODEL_MEMORY && existsSync(packagedModelMemory)) {
    env.BONFYRE_MODEL_MEMORY = packagedModelMemory;
  }
  if (!env.AUREKAI_MODEL_MEMORY && existsSync(packagedModelMemory)) {
    env.AUREKAI_MODEL_MEMORY = packagedModelMemory;
  }
  const defaultDict = resolveDefaultDict(packagedModelMemory);
  if (!env.BONFYRE_SAE_DICT) {
    if (defaultDict) env.BONFYRE_SAE_DICT = defaultDict;
  }
  if (!env.AUREKAI_SAE_DICT) {
    if (defaultDict) env.AUREKAI_SAE_DICT = defaultDict;
  }
  return env;
}

const rawArgs = process.argv.slice(2);
if (rawArgs.length === 0 || rawArgs[0] === "--help" || rawArgs[0] === "-h" || rawArgs[0] === "help") {
  printHelp();
  process.exit(0);
}

if (rawArgs[0] === "manifest:print") {
  const manifestPath = join(repoRoot, "aurekai.manifest.json");
  process.stdout.write(readFileSync(manifestPath, "utf8"));
  process.exit(0);
}

if (rawArgs[0] === "integrations") {
  const path = join(repoRoot, "registry", "integrations.json");
  printRegistry(path, rawArgs.includes("--json"));
  process.exit(0);
}

if (rawArgs[0] === "forks") {
  const path = join(repoRoot, "registry", "fork-surfaces.json");
  printRegistry(path, rawArgs.includes("--json"));
  process.exit(0);
}

if (rawArgs[0] === "integrate:list" || (rawArgs[0] === "integrate" && rawArgs[1] === "list")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const surface = getFlagValue(args, "--surface");
  const group = getFlagValue(args, "--group");
  const asJson = args.includes("--json");
  const result = listKernelTargets(repoRoot, { surface, group });
  printPayload({
    schema_version: "aurekai.integration_target_list.v1",
    count: result.length,
    targets: result,
  }, asJson);
  process.exit(0);
}

if (rawArgs[0] === "integrate:run" || (rawArgs[0] === "integrate" && rawArgs[1] === "run")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const asJson = args.includes("--json");
  const persist = args.includes("--persist");
  const enforceDoctrine = args.includes("--enforce-doctrine");

  if (!target) {
    console.error("Missing --target for integrate:run");
    process.exit(2);
  }

  const event = parseEventPayload(args);
  try {
    const result = runKernelIntegration(repoRoot, { target, event, persist, enforceDoctrine });
    printPayload({
      schema_version: "aurekai.integration_run_result.v1",
      target,
      persisted_to: result.persistedTo,
      execution: result.envelope,
    }, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:batch" || (rawArgs[0] === "integrate" && rawArgs[1] === "batch")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const surface = getFlagValue(args, "--surface");
  const asJson = args.includes("--json");
  const persist = args.includes("--persist");
  const enforceDoctrine = args.includes("--enforce-doctrine");

  if (!surface) {
    console.error("Missing --surface for integrate:batch");
    process.exit(2);
  }

  const event = parseEventPayload(args);
  try {
    const result = runKernelSurfaceBatch(repoRoot, { surface, event, persist, enforceDoctrine });
    printPayload(result, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:state-machine" || (rawArgs[0] === "integrate" && rawArgs[1] === "state-machine")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const type = getFlagValue(args, "--type");
  const asJson = args.includes("--json");
  const persist = args.includes("--persist");
  const enforceDoctrine = args.includes("--enforce-doctrine");

  if (!type) {
    console.error("Missing --type for integrate:state-machine");
    process.exit(2);
  }

  const event = parseEventPayload(args);
  try {
    const result = runKernelStateMachineBatch(repoRoot, { type, event, persist, enforceDoctrine });
    printPayload(result, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:ingest" || (rawArgs[0] === "integrate" && rawArgs[1] === "ingest")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const endpoint = getFlagValue(args, "--endpoint");
  const useTemplate = getFlagValue(args, "--use-template");
  const environment = getFlagValue(args, "--environment") || "default";
  const file = getFlagValue(args, "--input-file") || getFlagValue(args, "--event-file");
  const asJson = args.includes("--json");
  const persist = args.includes("--persist");
  const strict = args.includes("--strict");
  const timeoutMs = Number(getFlagValue(args, "--timeout-ms") || 10000);
  const requestBody = parseRequestBodyPayload(args);

  if (!target) {
    console.error("Missing --target for integrate:ingest");
    process.exit(2);
  }

  const event = getFlagValue(args, "--event-json") ? parseEventPayload(args) : null;
  try {
    const result = await ingestIntegrationTarget(repoRoot, {
      target,
      endpoint,
      file,
      event,
      persist,
      strict,
      useTemplate,
      environment,
      requestBody,
      timeoutMs,
    });
    printPayload(result, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:ingest-batch" || (rawArgs[0] === "integrate" && rawArgs[1] === "ingest-batch")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const surface = getFlagValue(args, "--surface");
  const group = getFlagValue(args, "--group");
  const type = getFlagValue(args, "--type");
  const endpoint = getFlagValue(args, "--endpoint");
  const useTemplate = getFlagValue(args, "--use-template");
  const environment = getFlagValue(args, "--environment") || "default";
  const file = getFlagValue(args, "--input-file") || getFlagValue(args, "--event-file");
  const asJson = args.includes("--json");
  const persist = args.includes("--persist");
  const strict = args.includes("--strict");
  const timeoutMs = Number(getFlagValue(args, "--timeout-ms") || 10000);
  const requestBody = parseRequestBodyPayload(args);

  if (!surface && !type) {
    console.error("Missing --surface or --type for integrate:ingest-batch");
    process.exit(2);
  }

  const event = getFlagValue(args, "--event-json") ? parseEventPayload(args) : null;
  try {
    const result = await ingestIntegrationBatch(repoRoot, {
      surface,
      group,
      type,
      endpoint,
      file,
      event,
      persist,
      strict,
      useTemplate,
      environment,
      requestBody,
      timeoutMs,
    });
    printPayload(result, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:validate-event" || (rawArgs[0] === "integrate" && rawArgs[1] === "validate-event")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const file = getFlagValue(args, "--input-file") || getFlagValue(args, "--event-file");
  const asJson = args.includes("--json");

  if (!target) {
    console.error("Missing --target for integrate:validate-event");
    process.exit(2);
  }

  const event = getFlagValue(args, "--event-json") ? parseEventPayload(args) : null;
  try {
    const result = await validateIntegrationEvent(repoRoot, {
      target,
      file,
      event,
    });
    printPayload(result, asJson);
    process.exit(result.valid ? 0 : 3);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:templates" || (rawArgs[0] === "integrate" && rawArgs[1] === "templates")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const environment = getFlagValue(args, "--environment");
  const asJson = args.includes("--json");
  const result = listEndpointTemplates(repoRoot, { target, environment });
  printPayload({
    schema_version: "aurekai.integration_endpoint_templates.v1",
    count: result.length,
    templates: result,
  }, asJson);
  process.exit(0);
}

if (rawArgs[0] === "integrate:scaffold-repos" || (rawArgs[0] === "integrate" && rawArgs[1] === "scaffold-repos")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const outDir = getFlagValue(args, "--out-dir");
  const asJson = args.includes("--json");
  const all = args.includes("--all");

  if (!target && !all) {
    console.error("Missing --target or --all for integrate:scaffold-repos");
    process.exit(2);
  }

  try {
    const result = scaffoldIntegrationRepos(repoRoot, {
      target: all ? null : target,
      outDir,
    });
    printPayload(result, asJson);
    process.exit(0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exit(1);
  }
}

if (rawArgs[0] === "integrate:presets" || (rawArgs[0] === "integrate" && rawArgs[1] === "presets")) {
  const args = rawArgs[0] === "integrate" ? rawArgs.slice(2) : rawArgs.slice(1);
  const target = getFlagValue(args, "--target");
  const asJson = args.includes("--json");
  const presets = listConnectorPresets(repoRoot, target);
  printPayload({
    schema_version: "aurekai.integration_connector_preset_list.v1",
    count: presets.length,
    presets,
  }, asJson);
  process.exit(0);
}

const args = normalizeArgs(rawArgs);
const command = args[0];
const rest = args.slice(1);
const target = resolveLegacyBinary(command);
const proc = spawnSync(target.bin, [...target.args, ...rest], {
  stdio: "inherit",
  env: detectLegacyEnv(),
});
process.exit(proc.status ?? 1);
