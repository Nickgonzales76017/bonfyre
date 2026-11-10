#!/usr/bin/env node
/* Public, read-only gateway for the compiled external-estate candidate corpus. */
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  composeExternalWorld,
  compileExternalSurfaceSlice,
  compileExternalWorldClosure,
  compileExternalWorldFormation,
  compileExternalWorldFormationCatalogue,
  compileExternalWorldBridges,
  compileExternalWorldCatalogue,
  compileExternalWorldSurfaceScene,
  externalEstateManifest,
  getExternalOccurrence,
  listExternalEstate,
  presentationCellCatalog,
  probeExternalOccurrence,
  renderExternalOccurrenceHtml,
  renderExternalProbeHtml,
  renderExternalEstateHtml,
  renderExternalWorldHtml,
  renderExternalWorldCatalogueHtml,
} from "./external-estate-runtime.mjs";

const sourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const repoRoot = process.env.AUREKAI_REPO_ROOT || sourceRoot;
const port = Number.parseInt(process.env.AUREKAI_EXTERNAL_ESTATE_PORT || "8792", 10);
const basePath = (process.env.AUREKAI_EXTERNAL_ESTATE_BASE_PATH || "/external-estate").replace(/\/+$/, "") || "/external-estate";
const nativeForwardingOptions = Object.freeze({
  addressIndexPath: process.env.BONFYRE_ADDRESS_INDEX || undefined,
  addressPlaneBinary: process.env.BONFYRE_ADDRESSPLANE_BINARY || undefined,
  requireNativeForwarding: process.env.BONFYRE_WORLD_REQUIRE_NATIVE_FORWARDING === "1",
});

function json(response, status, payload) {
  const body = `${JSON.stringify(payload, null, 2)}\n`;
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": Buffer.byteLength(body), "cache-control": "no-store", "x-content-type-options": "nosniff" });
  response.end(body);
}

function html(response, status, body) {
  response.writeHead(status, { "content-type": "text/html; charset=utf-8", "content-length": Buffer.byteLength(body), "cache-control": "no-store", "x-content-type-options": "nosniff" });
  response.end(body);
}

/* A public World closure is actor-neutral and content-addressed.  Unlike the
 * generic inspection routes, it is safe to cache briefly because the exact
 * closure digest is also carried in both its human and machine projections. */
function publicClosure(response, request, type, body, digest) {
  const etag = `\"${String(digest || "").replace(/^sha256:/, "")}\"`;
  if (etag !== '\"\"' && String(request.headers["if-none-match"] || "").includes(etag.slice(1, -1))) {
    response.writeHead(304, { etag, "cache-control": "public, max-age=60, stale-while-revalidate=300", vary: "Host, Accept" });
    return response.end();
  }
  response.writeHead(200, { "content-type": type, "content-length": Buffer.byteLength(body), etag, "cache-control": "public, max-age=60, stale-while-revalidate=300", vary: "Host, Accept", "x-content-type-options": "nosniff", "x-bonfyre-closure-digest": digest });
  response.end(body);
}

function queryOptions(url) {
  return {
    query: url.searchParams.get("q") || undefined,
    sourceKind: url.searchParams.get("source_kind") || undefined,
    reagentRole: url.searchParams.get("reagent_role") || undefined,
    domain: url.searchParams.get("domain") || undefined,
    world: url.searchParams.get("world") || undefined,
    affordance: url.searchParams.get("affordance") || undefined,
    offset: url.searchParams.get("offset") || undefined,
    limit: url.searchParams.get("limit") || undefined,
  };
}

function surfaceSliceOptions(url, grammar, request) {
  const options = {
    ...queryOptions(url), grammar, host: request.headers.host || "",
    kind: url.searchParams.get("kind") || undefined,
    relation: url.searchParams.get("relation") || undefined,
    near: url.searchParams.get("near") || undefined,
    state: url.searchParams.get("state") || undefined,
    time: url.searchParams.get("time") || undefined,
    context: url.searchParams.get("context") || undefined,
    capability: url.searchParams.get("capability") || undefined,
    extent: url.searchParams.get("extent") || undefined,
    mode: url.searchParams.get("mode") || undefined,
  };
  /* Pass prohibited coordinates through solely so the compiler can reject
   * them. Silently ignoring an authority-shaped query is unsafe ergonomics. */
  for (const key of ["actor", "actor_id", "audience", "authority", "role", "institution", "entitlement", "delegation", "permission", "effect_authority", "financial_authority", "spend_authority"]) {
    if (url.searchParams.has(key)) options[key] = url.searchParams.get(key);
  }
  return options;
}

function surfaceSlice(response, url, grammar, request) {
  try {
    return json(response, 200, compileExternalSurfaceSlice(repoRoot, surfaceSliceOptions(url, grammar, request)));
  } catch (error) {
    return json(response, 400, {
      error: error.code || "invalid_surface_slice", forbidden_coordinates: error.forbidden || [],
      invalid_coordinates: error.invalid || [],
      limit: "Address shape may narrow a public candidate slice; it cannot supply actor, audience, role, authority, or effect authority.",
    });
  }
}

function notFound(response) {
  json(response, 404, { error: "not_found", limit: "This service exposes only the compiled external-estate candidate corpus." });
}

function worldRouteParts(value) {
  try {
    const parts = String(value || "").split("/").filter(Boolean).map((part) => decodeURIComponent(part));
    return parts.length > 0 && parts.length <= 2 ? { world: parts[0], grammar: parts[1] || null } : null;
  } catch {
    return null;
  }
}

function worldClosure(response, request, type, world, grammar) {
  try {
    const options = { host: request.headers.host || "", ...nativeForwardingOptions, ...(grammar ? { grammar } : {}) };
    const closure = compileExternalWorldClosure(repoRoot, world, options);
    const body = type.startsWith("text/html")
      ? renderExternalWorldHtml(repoRoot, world, { basePath, ...options })
      : `${JSON.stringify(closure, null, 2)}\n`;
    return publicClosure(response, request, type, body, closure.closure_digest);
  } catch (error) {
    return json(response, error.code === "native_forwarding_unavailable" ? 503 : 400, {
      error: error.code || "invalid_world_closure", invalid_coordinates: error.invalid || [],
    });
  }
}

/* The Scene is the owner-produced reconciliation representation. It is not a
 * browser recipe: a client receives either an exact successor Scene or 304. */
function worldScene(response, request, world, grammar) {
  try {
    const options = { host: request.headers.host || "", ...nativeForwardingOptions, ...(grammar ? { grammar } : {}) };
    const scene = compileExternalWorldSurfaceScene(repoRoot, world, { ...options, basePath });
    return publicClosure(response, request, "application/json; charset=utf-8", `${JSON.stringify(scene, null, 2)}\n`, scene.source_digest);
  } catch (error) {
    return json(response, error.code === "native_forwarding_unavailable" ? 503 : 400, {
      error: error.code || "invalid_world_scene", invalid_coordinates: error.invalid || [],
    });
  }
}

function worldFormation(response, request, world) {
  const formation = compileExternalWorldFormation(repoRoot, world);
  return publicClosure(response, request, "application/json; charset=utf-8", `${JSON.stringify(formation, null, 2)}\n`, formation.formation_digest);
}

function formationCatalogue(response, request) {
  const catalogue = compileExternalWorldFormationCatalogue(repoRoot);
  return publicClosure(response, request, "application/json; charset=utf-8", `${JSON.stringify(catalogue, null, 2)}\n`, catalogue.closure_digest);
}

function worldBridges(response, request, world) {
  const bridges = compileExternalWorldBridges(repoRoot, world);
  return publicClosure(response, request, "application/json; charset=utf-8", `${JSON.stringify(bridges, null, 2)}\n`, bridges.bridge_digest);
}

function worldCatalogue(response, request, type) {
  const catalogue = compileExternalWorldCatalogue(repoRoot);
  const body = type.startsWith("text/html")
    ? renderExternalWorldCatalogueHtml(repoRoot, { basePath })
    : `${JSON.stringify(catalogue, null, 2)}\n`;
  return publicClosure(response, request, type, body, catalogue.closure_digest);
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url || "/", "http://external-estate.local");
  const path = url.pathname.replace(/\/$/, "") || "/";
  const root = basePath;
  const api = `${root}/v1`;
  if (request.method === "GET" && path === `${root}/healthz`) return json(response, 200, { status: "ok", schema_version: "aurekai.external_estate.health.v1", manifest: externalEstateManifest(repoRoot).counts });
  if (request.method === "GET" && (path === root || path === "/")) return html(response, 200, renderExternalEstateHtml(repoRoot, { ...queryOptions(url), basePath }));
  if (path.startsWith(`${root}/occurrences/`)) {
    const suffix = path.slice(`${root}/occurrences/`.length);
    const isProbe = suffix.endsWith("/probe");
    const id = isProbe ? suffix.slice(0, -"/probe".length) : suffix;
    if (request.method === "POST" && isProbe) {
      const record = getExternalOccurrence(repoRoot, id);
      return record ? html(response, 200, renderExternalProbeHtml(record, await probeExternalOccurrence(record), { basePath })) : notFound(response);
    }
    if (request.method !== "GET" || isProbe) return notFound(response);
    const page = renderExternalOccurrenceHtml(repoRoot, id, { basePath });
    return page ? html(response, 200, page) : notFound(response);
  }
  if (request.method === "GET" && path === `${root}/worlds`) {
    return worldCatalogue(response, request, "text/html; charset=utf-8");
  }
  if (request.method === "GET" && path.startsWith(`${root}/worlds/`)) {
    const parts = worldRouteParts(path.slice(`${root}/worlds/`.length));
    return parts ? worldClosure(response, request, "text/html; charset=utf-8", parts.world, parts.grammar) : notFound(response);
  }
  if (request.method === "GET" && path === `${api}/manifest`) return json(response, 200, externalEstateManifest(repoRoot));
  if (request.method === "GET" && path === `${api}/presentation-cells`) return json(response, 200, presentationCellCatalog());
  if (request.method === "GET" && path === `${api}/occurrences`) return json(response, 200, listExternalEstate(repoRoot, queryOptions(url)));
  if (request.method === "GET" && path === `${api}/formations`) return formationCatalogue(response, request);
  if (request.method === "GET" && path === `${api}/worlds`) return worldCatalogue(response, request, "application/json; charset=utf-8");
  if (request.method === "GET" && path.startsWith(`${api}/surfaces/`)) {
    return surfaceSlice(response, url, decodeURIComponent(path.slice(`${api}/surfaces/`.length)), request);
  }
  if (request.method === "GET" && path.startsWith(`${api}/worlds/`)) {
    const suffix = path.slice(`${api}/worlds/`.length);
    if (suffix.endsWith("/bridges")) {
      const parts = worldRouteParts(suffix.slice(0, -"/bridges".length));
      return parts && !parts.grammar ? worldBridges(response, request, parts.world) : notFound(response);
    }
    if (suffix.endsWith("/formation")) {
      const parts = worldRouteParts(suffix.slice(0, -"/formation".length));
      return parts && !parts.grammar ? worldFormation(response, request, parts.world) : notFound(response);
    }
    if (suffix.endsWith("/scene")) {
      const parts = worldRouteParts(suffix.slice(0, -"/scene".length));
      return parts ? worldScene(response, request, parts.world, parts.grammar) : notFound(response);
    }
    if (suffix.endsWith("/closure")) {
      const parts = worldRouteParts(suffix.slice(0, -"/closure".length));
      return parts ? worldClosure(response, request, "application/json; charset=utf-8", parts.world, parts.grammar) : notFound(response);
    }
    return json(response, 200, composeExternalWorld(repoRoot, decodeURIComponent(suffix), queryOptions(url)));
  }
  if (path.startsWith(`${api}/occurrences/`)) {
    const suffix = path.slice(`${api}/occurrences/`.length);
    const probeSuffix = "/probe";
    const isProbe = suffix.endsWith(probeSuffix);
    const id = isProbe ? suffix.slice(0, -probeSuffix.length) : suffix;
    const record = getExternalOccurrence(repoRoot, id);
    if (!record) return notFound(response);
    if (!isProbe && request.method === "GET") return json(response, 200, record);
    if (isProbe && request.method === "POST") return json(response, 200, await probeExternalOccurrence(record, { timeoutMs: url.searchParams.get("timeout_ms") }));
  }
  return notFound(response);
});

server.listen(port, "127.0.0.1", () => {
  process.stdout.write(`Aurekai external-estate service on 127.0.0.1:${port}${basePath}\n`);
});
