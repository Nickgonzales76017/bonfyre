import { readFileSync } from "node:fs";
import { createHmac } from "node:crypto";
import {
  executeIntegration,
  executeStateMachineBatch,
  executeSurfaceBatch,
  listIntegrationTargets,
} from "./integration-engine.mjs";

function normalizeKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function readConnectorPresets(repoRoot) {
  const path = `${repoRoot}/registry/integration-connector-presets.json`;
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    const presets = Array.isArray(parsed?.presets) ? parsed.presets : [];
    return presets.filter((item) => item && typeof item === "object");
  } catch {
    return [];
  }
}

function readValidationProfiles(repoRoot) {
  const path = `${repoRoot}/registry/integration-parser-validation-profiles.json`;
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    return parsed?.profiles && typeof parsed.profiles === "object" ? parsed.profiles : {};
  } catch {
    return {};
  }
}

function readEndpointTemplates(repoRoot) {
  const path = `${repoRoot}/registry/integration-endpoint-templates.json`;
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    const templates = Array.isArray(parsed?.templates) ? parsed.templates : [];
    return templates.filter((item) => item && typeof item === "object");
  } catch {
    return [];
  }
}

function buildPresetIndex(repoRoot) {
  const presets = readConnectorPresets(repoRoot);
  const index = new Map();
  for (const preset of presets) {
    const targetKey = normalizeKey(preset.target);
    const idKey = normalizeKey(preset.id || "");
    if (targetKey && !index.has(targetKey)) index.set(targetKey, preset);
    if (idKey && !index.has(idKey)) index.set(idKey, preset);
  }
  return { presets, index };
}

function buildTemplateIndex(repoRoot) {
  const templates = readEndpointTemplates(repoRoot);
  const byId = new Map();
  for (const template of templates) {
    const idKey = normalizeKey(template.id || "");
    if (idKey && !byId.has(idKey)) byId.set(idKey, template);
  }
  return { templates, byId };
}

function firstDefined(candidates) {
  for (const value of candidates) {
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return null;
}

function fromPath(payload, path) {
  const keys = String(path || "").split(".").filter(Boolean);
  let cur = payload;
  for (const key of keys) {
    if (!cur || typeof cur !== "object" || !(key in cur)) return null;
    cur = cur[key];
  }
  return cur;
}

function parseUsingFieldMap(raw, fieldMap = {}) {
  const normalized = {};
  for (const [targetField, paths] of Object.entries(fieldMap)) {
    const pathList = Array.isArray(paths) ? paths : [paths];
    const values = pathList.map((path) => fromPath(raw, path));
    const found = firstDefined(values);
    if (found !== null) normalized[targetField] = found;
  }
  return normalized;
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function expandEnvValue(value, unresolved) {
  if (typeof value !== "string") return value;
  const matcher = /\$\{([A-Z0-9_]+)\}/g;
  return value.replace(matcher, (_, key) => {
    if (process.env[key] === undefined) {
      unresolved.add(key);
      return "";
    }
    return process.env[key];
  });
}

function expandEnvObject(input, unresolved) {
  if (!input || typeof input !== "object" || Array.isArray(input)) return {};
  const out = {};
  for (const [key, value] of Object.entries(input)) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      out[key] = expandEnvObject(value, unresolved);
      continue;
    }
    out[key] = expandEnvValue(value, unresolved);
  }
  return out;
}

function buildRequestBody(template, unresolved, options = {}) {
  if (options.requestBody !== undefined && options.requestBody !== null) {
    return options.requestBody;
  }
  const body = template?.body;
  if (body === undefined || body === null) return null;
  if (typeof body === "string") {
    return expandEnvValue(body, unresolved);
  }
  if (typeof body === "object") {
    return expandEnvObject(body, unresolved);
  }
  return body;
}

function serializeRequestBody(body) {
  if (body === undefined || body === null) return "";
  if (typeof body === "string") return body;
  return JSON.stringify(body);
}

function requestPathFromUrl(endpoint) {
  try {
    const parsed = new URL(endpoint);
    return `${parsed.pathname || "/"}${parsed.search || ""}`;
  } catch {
    return endpoint;
  }
}

function buildCanonicalSigningString(fields, values) {
  return fields.map((field) => String(values[field] || "")).join("\n");
}

function resolveSignedRequest(template, endpoint, request, unresolved) {
  const signing = template?.signing;
  if (!signing || typeof signing !== "object") {
    return {
      request,
      signing: null,
      unresolved,
    };
  }

  const mode = normalizeKey(signing.mode || "hmac-sha256");
  if (mode !== "hmac-sha256") {
    throw new Error(`Unsupported signing mode: ${signing.mode}`);
  }

  const secretEnv = String(signing.secret_env || "").trim();
  if (!secretEnv) {
    throw new Error("Signing is enabled but signing.secret_env is missing.");
  }

  const secret = process.env[secretEnv];
  if (!secret) {
    unresolved.add(secretEnv);
    return {
      request,
      signing: {
        enabled: true,
        mode,
        required: Boolean(signing.required),
        missing_secret_env: secretEnv,
      },
      unresolved,
    };
  }

  const method = String(request.method || "GET").toUpperCase();
  const bodyText = serializeRequestBody(request.body);
  const timestamp = String(signing.timestamp || new Date().toISOString());
  const path = requestPathFromUrl(endpoint);
  const canonicalFields = Array.isArray(signing.canonical_fields) && signing.canonical_fields.length > 0
    ? signing.canonical_fields.map((field) => String(field))
    : ["method", "path", "body", "timestamp"];
  const canonical = buildCanonicalSigningString(canonicalFields, {
    method,
    path,
    body: bodyText,
    timestamp,
  });

  const encoding = String(signing.encoding || "hex").toLowerCase() === "base64" ? "base64" : "hex";
  const signature = createHmac("sha256", secret).update(canonical).digest(encoding);
  const signatureHeader = String(signing.signature_header || "X-Signature");
  const timestampHeader = String(signing.timestamp_header || "X-Timestamp");

  request.headers = {
    ...(request.headers || {}),
    [signatureHeader]: signature,
    [timestampHeader]: timestamp,
  };

  return {
    request,
    signing: {
      enabled: true,
      mode,
      required: Boolean(signing.required),
      signature_header: signatureHeader,
      timestamp_header: timestampHeader,
      encoding,
      canonical_fields: canonicalFields,
      missing_secret_env: null,
    },
    unresolved,
  };
}

function withQueryParams(url, query) {
  if (!query || typeof query !== "object") return url;
  const u = new URL(url);
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    u.searchParams.set(key, String(value));
  }
  return u.toString();
}

function selectTemplate(repoRoot, descriptor, preset, options = {}) {
  const { templates, byId } = buildTemplateIndex(repoRoot);
  const templateId = normalizeKey(options.templateId || "");
  const envName = normalizeKey(options.environment || "default");

  if (templateId) {
    return byId.get(templateId) || null;
  }

  const targetKey = normalizeKey(descriptor.target);
  const presetKey = normalizeKey(preset?.id || "");
  const scoped = templates.filter((template) => {
    const templateTarget = normalizeKey(template.target || "");
    const templatePreset = normalizeKey(template.preset || "");
    const templateEnv = normalizeKey(template.environment || "default");
    const targetMatch = templateTarget && templateTarget === targetKey;
    const presetMatch = templatePreset && templatePreset === presetKey;
    const envMatch = templateEnv === envName;
    return envMatch && (targetMatch || presetMatch);
  });

  if (scoped.length > 0) return scoped[0];
  return null;
}

function resolveEndpointTemplate(repoRoot, descriptor, preset, options = {}) {
  const template = selectTemplate(repoRoot, descriptor, preset, options);
  if (!template) return { template: null, endpoint: null, request: null, unresolvedEnv: [] };

  const unresolved = new Set();
  const expandedHeaders = expandEnvObject(template.headers || {}, unresolved);
  const expandedQuery = expandEnvObject(template.query || {}, unresolved);
  const rawUrl = expandEnvValue(template.url || "", unresolved);
  const body = buildRequestBody(template, unresolved, options);
  const endpoint = rawUrl ? withQueryParams(rawUrl, expandedQuery) : null;

  const request = {
    method: String(template.method || "GET").toUpperCase(),
    headers: expandedHeaders,
    timeoutMs: Number(template.timeout_ms || options.timeoutMs || 10_000),
    body,
  };

  const signed = resolveSignedRequest(template, endpoint, request, unresolved);

  return {
    template,
    endpoint,
    request: signed.request,
    signing: signed.signing,
    unresolvedEnv: Array.from(unresolved),
  };
}

function isPresentValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function pickValidationProfile(repoRoot, parserName) {
  const profiles = readValidationProfiles(repoRoot);
  const parserKey = normalizeKey(parserName || "generic");
  return profiles[parserKey] || profiles.generic || null;
}

function validateByProfile(normalizedEvent, rawPayload, profile) {
  const errors = [];
  const warnings = [];
  if (!profile) return { valid: true, errors, warnings, profile: null };

  const requiredFields = Array.isArray(profile.required_normalized_fields)
    ? profile.required_normalized_fields
    : [];
  for (const field of requiredFields) {
    if (!isPresentValue(normalizedEvent[field])) {
      errors.push(`missing normalized field: ${field}`);
    }
  }

  const payloadPaths = Array.isArray(profile.required_payload_paths)
    ? profile.required_payload_paths
    : [];
  if (payloadPaths.length > 0) {
    const foundCount = payloadPaths
      .map((path) => fromPath(rawPayload, path))
      .filter((value) => isPresentValue(value)).length;

    const anyMode = Boolean(profile.any_payload_paths);
    if (anyMode && foundCount === 0) {
      errors.push(`missing payload path: expected any of [${payloadPaths.join(", ")}]`);
    }
    if (!anyMode) {
      for (const path of payloadPaths) {
        const value = fromPath(rawPayload, path);
        if (!isPresentValue(value)) {
          warnings.push(`payload path not present: ${path}`);
        }
      }
      if (warnings.length === payloadPaths.length) {
        errors.push(`missing payload path: none of required paths found [${payloadPaths.join(", ")}]`);
      }
    }
  }

  const ranges = profile.numeric_ranges && typeof profile.numeric_ranges === "object"
    ? profile.numeric_ranges
    : {};
  for (const [field, range] of Object.entries(ranges)) {
    if (!Array.isArray(range) || range.length !== 2) continue;
    const value = Number(normalizedEvent[field]);
    if (!Number.isFinite(value)) {
      errors.push(`numeric field invalid: ${field}`);
      continue;
    }
    const min = Number(range[0]);
    const max = Number(range[1]);
    if (value < min || value > max) {
      errors.push(`numeric field out of range: ${field} expected [${min}, ${max}] got ${value}`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    profile,
  };
}

function parseByPreset(raw, preset) {
  const fieldMapped = parseUsingFieldMap(raw, preset?.field_map || {});
  const parser = normalizeKey(preset?.parser || "generic");

  if (parser === "ibc-packet") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.type, "packet_commit"]),
      source: firstDefined([fieldMapped.source, raw.chain_id, "ibc-relayer"]),
      stage: firstDefined([fieldMapped.stage, raw.stage, "packet-lifecycle"]),
    };
  }

  if (parser === "layerzero-packet" || parser === "wormhole-vaa" || parser === "hyperlane-message" || parser === "axelar-gmp" || parser === "stargate-transfer") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.type, "cross_chain_message"]),
      source: firstDefined([fieldMapped.source, raw.endpoint, preset?.source_default]),
      stage: firstDefined([fieldMapped.stage, raw.stage, "message-commit"]),
    };
  }

  if (parser === "rollup-proof") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.proof_status, "proof_accept"]),
      source: firstDefined([fieldMapped.source, raw.rollup, preset?.source_default]),
      stage: firstDefined([fieldMapped.stage, raw.stage, "proof-pipeline"]),
    };
  }

  if (parser === "defi-position" || parser === "uniswap-v4-hook") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.hook_event, raw.position_action, "position_update"]),
      source: firstDefined([fieldMapped.source, raw.protocol, preset?.source_default]),
      stage: firstDefined([fieldMapped.stage, raw.stage, "position-lifecycle"]),
    };
  }

  if (parser === "da-publication") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.publication_type, "blob_publish"]),
      source: firstDefined([fieldMapped.source, raw.network, preset?.source_default]),
      stage: firstDefined([fieldMapped.stage, raw.stage, "data-availability"]),
    };
  }

  if (parser === "workflow-loop" || parser === "agent-loop" || parser === "ide-loop") {
    return {
      ...fieldMapped,
      action: firstDefined([fieldMapped.action, raw.step_action, raw.type, "loop_tick"]),
      source: firstDefined([fieldMapped.source, raw.runtime, preset?.source_default]),
      stage: firstDefined([fieldMapped.stage, raw.loop_stage, "loop-step"]),
    };
  }

  return {
    ...fieldMapped,
    action: firstDefined([fieldMapped.action, raw.action, "observe"]),
    source: firstDefined([fieldMapped.source, raw.source, preset?.source_default]),
    stage: firstDefined([fieldMapped.stage, raw.stage, "mutation-path"]),
  };
}

function baseConnector(surface) {
  if (surface === "hard-fork") {
    return {
      mode: "rpc-or-indexer",
      parser: "protocol-event",
      expected_fields: ["event_id", "action", "risk_score"],
      source_kind: "protocol",
    };
  }
  if (surface === "soft-fork") {
    return {
      mode: "runtime-hook",
      parser: "loop-event",
      expected_fields: ["event_id", "action", "stage"],
      source_kind: "runtime",
    };
  }
  if (surface === "observer") {
    return {
      mode: "observer-feed",
      parser: "observer-event",
      expected_fields: ["event_id", "action", "source"],
      source_kind: "observer",
    };
  }
  if (surface === "state-machine") {
    return {
      mode: "continuity-stream",
      parser: "state-machine-event",
      expected_fields: ["event_id", "action", "continuity_gap"],
      source_kind: "state-machine",
    };
  }
  return {
    mode: "adapter-feed",
    parser: "adapter-event",
    expected_fields: ["event_id", "action"],
    source_kind: "adapter",
  };
}

function categoryHints(category) {
  if (category === "transport") {
    return {
      action_default: "packet_commit",
      source_default: "transport-protocol",
    };
  }
  if (category === "rollup_pipeline") {
    return {
      action_default: "proof_accept",
      source_default: "rollup-prover",
    };
  }
  if (category === "financial_topology") {
    return {
      action_default: "position_update",
      source_default: "financial-protocol",
    };
  }
  if (category === "availability") {
    return {
      action_default: "blob_publish",
      source_default: "da-layer",
    };
  }
  return {
    action_default: "observe",
    source_default: "integration-source",
  };
}

function resolveTargetDescriptor(repoRoot, target) {
  const matches = listIntegrationTargets(repoRoot).filter((item) => normalizeKey(item.target) === normalizeKey(target));
  if (!matches.length) {
    throw new Error(`Unknown integration target: ${target}`);
  }

  const priority = ["hard-fork", "soft-fork", "adapter", "observer", "state-machine"];
  matches.sort((a, b) => priority.indexOf(a.surface) - priority.indexOf(b.surface));
  return matches[0];
}

function ensureObject(payload) {
  if (payload && typeof payload === "object" && !Array.isArray(payload)) return payload;
  return { raw: payload };
}

export function normalizeConnectorEvent(targetDescriptor, payload, meta = {}) {
  const base = baseConnector(targetDescriptor.surface);
  const hints = categoryHints(targetDescriptor.category);
  const raw = ensureObject(payload);
  const preset = meta.preset || null;
  const parsed = parseByPreset(raw, preset);

  return {
    event_id: String(firstDefined([parsed.event_id, raw.event_id, raw.id, `evt-${Date.now()}`])),
    action: String(firstDefined([parsed.action, raw.action, raw.type, preset?.action_default, hints.action_default])),
    source: String(firstDefined([parsed.source, meta.endpoint, raw.origin, preset?.source_default, hints.source_default])),
    stage: String(firstDefined([parsed.stage, raw.stage, targetDescriptor.surface === "soft-fork" ? "loop-step" : "mutation-path"])),
    risk_score: toNumber(firstDefined([parsed.risk_score, raw.risk_score, raw.risk]), 0),
    deformation_score: toNumber(firstDefined([parsed.deformation_score, raw.deformation_score, raw.deformation]), 0),
    witness_skew: toNumber(firstDefined([parsed.witness_skew, raw.witness_skew, raw.skew]), 0),
    continuity_gap: toNumber(firstDefined([parsed.continuity_gap, raw.continuity_gap, raw.gap]), 0),
    window: String(firstDefined([parsed.window, raw.window, "current"])),
    payload: raw,
    connector: {
      mode: base.mode,
      parser: preset?.parser || base.parser,
      source_kind: base.source_kind,
      endpoint: meta.endpoint || null,
      file: meta.file || null,
      preset_id: preset?.id || null,
    },
  };
}

function parseInputText(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) return {};

  try {
    return JSON.parse(trimmed);
  } catch {
    return { raw: trimmed };
  }
}

export function loadEventFromFile(path) {
  return parseInputText(readFileSync(path, "utf8"));
}

export async function loadEventFromEndpoint(endpoint, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 10_000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const requestBody = options.body;
    const method = String(options.method || "GET").toUpperCase();
    const headers = options.headers || {};
    let body;
    if (requestBody !== undefined && requestBody !== null && method !== "GET" && method !== "HEAD") {
      if (typeof requestBody === "string") {
        body = requestBody;
      } else {
        body = JSON.stringify(requestBody);
      }
      if (!headers["content-type"] && !headers["Content-Type"]) {
        headers["Content-Type"] = "application/json";
      }
    }

    const response = await fetch(endpoint, {
      method: options.method || "GET",
      headers,
      body,
      signal: controller.signal,
    });
    const text = await response.text();
    return {
      status: response.status,
      ok: response.ok,
      payload: parseInputText(text),
    };
  } finally {
    clearTimeout(timer);
  }
}

export async function ingestIntegrationTarget(repoRoot, input) {
  const descriptor = resolveTargetDescriptor(repoRoot, input.target);
  const { index } = buildPresetIndex(repoRoot);
  const preset = index.get(normalizeKey(descriptor.target)) || null;

  const resolvedTemplate = resolveEndpointTemplate(repoRoot, descriptor, preset, {
    templateId: input.useTemplate,
    environment: input.environment,
    timeoutMs: input.timeoutMs,
    requestBody: input.requestBody,
  });

  const resolvedEndpoint = input.endpoint || resolvedTemplate.endpoint || preset?.default_endpoint || null;
  const endpointRequest = resolvedTemplate.request || {
    method: "GET",
    headers: {},
    timeoutMs: Number(input.timeoutMs || 10_000),
    body: input.requestBody ?? null,
  };

  const endpointModeRequested = !input.event && !input.file;

  if (endpointModeRequested && resolvedTemplate.template && Boolean(resolvedTemplate.template.strict_env) && resolvedTemplate.unresolvedEnv.length > 0) {
    throw new Error(`Endpoint template missing required env vars: ${resolvedTemplate.unresolvedEnv.join(", ")}`);
  }

  const signingRequired = Boolean(resolvedTemplate.template?.signing?.required);
  if (endpointModeRequested && signingRequired && resolvedTemplate.unresolvedEnv.length > 0) {
    throw new Error(`Endpoint template missing required signing env vars: ${resolvedTemplate.unresolvedEnv.join(", ")}`);
  }

  let rawEvent = {};
  let sourceInfo = {};

  if (input.event) {
    rawEvent = input.event;
    sourceInfo = { mode: "direct" };
  } else if (input.file) {
    rawEvent = loadEventFromFile(input.file);
    sourceInfo = { mode: "file", file: input.file };
  } else if (input.endpoint) {
    const fetched = await loadEventFromEndpoint(input.endpoint, {
      ...endpointRequest,
      timeoutMs: Number(input.timeoutMs || endpointRequest.timeoutMs || 10_000),
    });
    rawEvent = fetched.payload;
    sourceInfo = {
      mode: "endpoint",
      endpoint: input.endpoint,
      http_status: fetched.status,
      http_ok: fetched.ok,
    };
  } else if (resolvedEndpoint) {
    const fetched = await loadEventFromEndpoint(resolvedEndpoint, endpointRequest);
    rawEvent = fetched.payload;
    sourceInfo = {
      mode: "endpoint",
      endpoint: resolvedEndpoint,
      http_status: fetched.status,
      http_ok: fetched.ok,
    };
  } else if (preset?.mock_event) {
    rawEvent = preset.mock_event;
    sourceInfo = { mode: "preset-mock", preset_id: preset.id };
  }

  const event = normalizeConnectorEvent(descriptor, rawEvent, {
    endpoint: resolvedEndpoint,
    file: input.file,
    preset,
  });

  const parserName = event?.connector?.parser || preset?.parser || "generic";
  const profile = pickValidationProfile(repoRoot, parserName);
  const validation = validateByProfile(event, event.payload || {}, profile);
  const strict = Boolean(input.strict);
  if (strict && !validation.valid) {
    throw new Error(`Strict validation failed for ${descriptor.target}: ${validation.errors.join("; ")}`);
  }

  const result = executeIntegration(repoRoot, {
    target: descriptor.target,
    event,
    persist: input.persist,
  });

  return {
    schema_version: "aurekai.integration_ingest_result.v1",
    target: descriptor.target,
    surface: descriptor.surface,
    category: descriptor.category,
    source: sourceInfo,
    endpoint_template: resolvedTemplate.template
      ? {
          id: resolvedTemplate.template.id || null,
          environment: resolvedTemplate.template.environment || "default",
          method: endpointRequest.method,
          has_request_body: endpointRequest.body !== undefined && endpointRequest.body !== null,
          signing: resolvedTemplate.signing,
          unresolved_env: resolvedTemplate.unresolvedEnv,
        }
      : null,
    validation: {
      strict,
      valid: validation.valid,
      errors: validation.errors,
      warnings: validation.warnings,
      profile: profile
        ? {
            required_normalized_fields: profile.required_normalized_fields || [],
            required_payload_paths: profile.required_payload_paths || [],
            any_payload_paths: Boolean(profile.any_payload_paths),
          }
        : null,
    },
    preset: preset
      ? {
          id: preset.id,
          parser: preset.parser,
          default_endpoint: preset.default_endpoint || null,
        }
      : null,
    persisted_to: result.persistedTo,
    execution: result.envelope,
  };
}

function resolveBatchTargets(repoRoot, options) {
  if (options.type) {
    const all = listIntegrationTargets(repoRoot, { surface: "state-machine", group: options.type });
    return all.map((item) => item.target);
  }

  const all = listIntegrationTargets(repoRoot, { surface: options.surface, group: options.group });
  return all.map((item) => item.target);
}

export async function ingestIntegrationBatch(repoRoot, options) {
  const targets = resolveBatchTargets(repoRoot, options);
  const executions = [];

  for (const target of targets) {
    const result = await ingestIntegrationTarget(repoRoot, {
      target,
      endpoint: options.endpoint,
      file: options.file,
      event: options.event,
      persist: options.persist,
      timeoutMs: options.timeoutMs,
      strict: options.strict,
      useTemplate: options.useTemplate,
      environment: options.environment,
    });

    executions.push({
      target: result.target,
      surface: result.surface,
      category: result.category,
      run_id: result.execution.run_id,
      committed_state_hash: result.execution.committed_state.state_hash,
      policy_decision: result.execution.policies[0].decision,
      persisted_to: result.persisted_to,
      source: result.source,
      endpoint_template: result.endpoint_template,
      validation: result.validation,
      preset: result.preset,
    });
  }

  return {
    schema_version: "aurekai.integration_ingest_batch.v1",
    surface: options.surface || null,
    state_machine_type: options.type || null,
    count: executions.length,
    executions,
  };
}

export async function validateIntegrationEvent(repoRoot, options) {
  const descriptor = resolveTargetDescriptor(repoRoot, options.target);
  const { index } = buildPresetIndex(repoRoot);
  const preset = index.get(normalizeKey(descriptor.target)) || null;

  let rawEvent = options.event || {};
  if (!options.event && options.file) {
    rawEvent = loadEventFromFile(options.file);
  }

  const event = normalizeConnectorEvent(descriptor, rawEvent, {
    endpoint: options.endpoint || null,
    file: options.file || null,
    preset,
  });

  const parserName = event?.connector?.parser || preset?.parser || "generic";
  const profile = pickValidationProfile(repoRoot, parserName);
  const validation = validateByProfile(event, event.payload || {}, profile);

  return {
    schema_version: "aurekai.integration_event_validation.v1",
    target: descriptor.target,
    surface: descriptor.surface,
    parser: parserName,
    valid: validation.valid,
    errors: validation.errors,
    warnings: validation.warnings,
    normalized_event: event,
    profile: profile
      ? {
          required_normalized_fields: profile.required_normalized_fields || [],
          required_payload_paths: profile.required_payload_paths || [],
          any_payload_paths: Boolean(profile.any_payload_paths),
          numeric_ranges: profile.numeric_ranges || {},
        }
      : null,
  };
}

export function listEndpointTemplates(repoRoot, input = {}) {
  const { templates } = buildTemplateIndex(repoRoot);
  const target = normalizeKey(input.target || "");
  const environment = normalizeKey(input.environment || "");

  return templates.filter((template) => {
    const targetMatch = !target || normalizeKey(template.target || "") === target || normalizeKey(template.preset || "") === target;
    const envMatch = !environment || normalizeKey(template.environment || "default") === environment;
    return targetMatch && envMatch;
  });
}

export function listConnectorPresets(repoRoot, target) {
  const { presets } = buildPresetIndex(repoRoot);
  if (!target) return presets;

  const key = normalizeKey(target);
  return presets.filter((preset) => normalizeKey(preset.target) === key || normalizeKey(preset.id) === key);
}

export function ingestStateMachineBatch(repoRoot, options) {
  if (!options.type) {
    throw new Error("Missing state-machine type.");
  }

  const event = options.event || {
    event_id: `evt-${Date.now()}`,
    action: "continuity_tick",
    source: "state-machine-stream",
    risk_score: 0,
    continuity_gap: 0,
  };

  return executeStateMachineBatch(repoRoot, {
    type: options.type,
    event,
    persist: options.persist,
  });
}

export function ingestSurfaceBatch(repoRoot, options) {
  const event = options.event || {
    event_id: `evt-${Date.now()}`,
    action: "observe",
    source: "surface-stream",
    risk_score: 0,
  };

  return executeSurfaceBatch(repoRoot, {
    surface: options.surface,
    event,
    persist: options.persist,
  });
}