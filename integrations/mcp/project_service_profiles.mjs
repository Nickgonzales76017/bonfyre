#!/usr/bin/env bun
import { existsSync, lstatSync, readFileSync, realpathSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { CAPABILITY_PACKS } from './capability_packs.mjs';

const DECLARED = Object.freeze({
  build: { port: 3061, purpose: 'source delivery, architecture, verification and task reconciliation' },
  operate: { port: 3062, purpose: 'operator, service, task and managed-runtime health' },
  work: { port: 3069, purpose: 'current work, outputs and account-scoped activity/inbox continuation' },
  model: { port: 3063, purpose: 'local model inventory, generation and placement' },
  commerce: { port: 3064, purpose: 'commercial, bookkeeping and capability exchange operations' },
  account: { port: 3065, purpose: 'account/destination context and handoff into authenticated workspace apertures' },
  connect: { port: 3066, purpose: 'external provider readiness, lawful observation and credential/account handoff' },
  evidence: { port: 3067, purpose: 'evidence intake, provenance, replay/parity verification and proof state' },
  institution: { port: 3068, purpose: 'institutional preparation, native projections and participation boundaries' },
});

export const PROJECT_MCP_SERVICE_PROFILES = Object.freeze(Object.fromEntries(
  Object.entries(DECLARED).map(([id, value]) => {
    if (!CAPABILITY_PACKS[id]) throw new Error(`missing capability pack for service profile: ${id}`);
    return [id, Object.freeze({
      id,
      label: `com.bonfyre.project-mcp-${id}`,
      host: '127.0.0.1',
      port: value.port,
      purpose: value.purpose,
      pack: id,
      token_name: `project-${id}.token`,
    })];
  }),
));

export const PROJECT_MCP_SERVICE_PROFILE_NAMES = Object.freeze(Object.keys(PROJECT_MCP_SERVICE_PROFILES));

function safeAbsolute(path, name) {
  if (typeof path !== 'string' || !path.startsWith('/')) throw new Error(`${name} must be absolute`);
  return path;
}

export const PROJECT_MCP_SOURCE_SET = Object.freeze([
  'integrations/mcp/project_node.mjs',
  'integrations/mcp/project_catalog.json',
  'integrations/mcp/project_runtime.mjs',
  'integrations/mcp/http_boundary.mjs',
  'integrations/mcp/project_reads.mjs',
  'integrations/mcp/project_native.mjs',
  'integrations/mcp/project_operator.mjs',
  'integrations/mcp/project_fabric.mjs',
  'integrations/mcp/project_spines.mjs',
  'integrations/mcp/project_v6.mjs',
  'integrations/mcp/project_products.mjs',
  'integrations/mcp/capability_packs.mjs',
  'integrations/mcp/project_service_profiles.mjs',
  'integrations/mcp/control_plane_app.mjs',
  'integrations/mcp/control_plane_app_build.mjs',
  'integrations/mcp/control_plane_app.html',
  'integrations/mcp/control_plane_app_resource.mjs',
]);

export function projectMcpSourceIdentity(root) {
  const entries = PROJECT_MCP_SOURCE_SET.map(relative => {
    const path = join(root, relative);
    const sha256 = createHash('sha256').update(readFileSync(path)).digest('hex');
    return { path: relative, sha256 };
  });
  const aggregate_sha256 = createHash('sha256')
    .update(entries.map(item => `${item.path}\0${item.sha256}\n`).join(''))
    .digest('hex');
  return {
    schema_version: 'bonfyre.project-mcp.source-set.v1',
    aggregate_sha256,
    files: entries,
    limitation: 'This is the declared Project MCP release source set, not a transitive module closure or proof that every dynamic dependency was captured.',
  };
}

export function compileProjectMcpLaunchAgent(profile, { root, home = homedir(), bun = process.execPath, sourceIdentity = null } = {}) {
  const definition = PROJECT_MCP_SERVICE_PROFILES[profile];
  if (!definition) throw new Error('unknown Project MCP service profile');
  root = safeAbsolute(realpathSync(root), 'root');
  home = safeAbsolute(home, 'home');
  bun = safeAbsolute(realpathSync(bun), 'bun');
  const entry = join(root, 'integrations/mcp/project_node.mjs');
  if (sourceIdentity !== null) {
    if (!sourceIdentity || sourceIdentity.schema_version !== 'bonfyre.project-mcp.source-set.v1'
      || !/^[a-f0-9]{64}$/.test(sourceIdentity.aggregate_sha256 || '')
      || !Array.isArray(sourceIdentity.files)) {
      throw new Error('invalid precomputed Project MCP source identity');
    }
  } else sourceIdentity = projectMcpSourceIdentity(root);
  const tokenFile = join(home, '.bonfyre/mcp', definition.token_name);
  const logRoot = join(home, '.bonfyre/logs');
  const plist = {
    Label: definition.label,
    ProgramArguments: [bun, entry],
    EnvironmentVariables: {
      BONFYRE_ROOT: root,
      BONFYRE_PROJECT_MCP_HOST: definition.host,
      BONFYRE_PROJECT_MCP_PORT: String(definition.port),
      BONFYRE_PROJECT_MCP_PACK: definition.pack,
      BONFYRE_MCP_TOKEN_FILE: tokenFile,
      BONFYRE_PROJECT_MCP_SOURCE_SHA256: sourceIdentity.aggregate_sha256,
    },
    RunAtLoad: true,
    KeepAlive: true,
    ProcessType: 'Background',
    StandardOutPath: join(logRoot, `project-mcp-${profile}.out.log`),
    StandardErrorPath: join(logRoot, `project-mcp-${profile}.err.log`),
  };
  return {
    schema_version: 'bonfyre.project-mcp.service-profile.v1',
    profile,
    purpose: definition.purpose,
    endpoint: `http://${definition.host}:${definition.port}`,
    token_file: tokenFile,
    capability_pack: definition.pack,
    source_identity: sourceIdentity,
    launch_agent: plist,
    deployment_state: 'candidate_only',
    listener_availability: 'not_verified',
    authority: {
      allowed: ['compile deterministic local LaunchAgent candidate', 'bind candidate to declared source-set identity'],
      not_granted: ['write token', 'install plist', 'bootstrap service', 'replace full Project MCP', 'publish endpoint'],
    },
  };
}


function xmlText(value) {
  return String(value || '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'").replace(/&amp;/g, '&');
}

function plistStrings(source, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return [...source.matchAll(new RegExp(`<key>\\s*${escaped}\\s*</key>\\s*<string>([^<]*)</string>`, 'g'))]
    .map(match => xmlText(match[1]).trim());
}

function regularFile(path) {
  try {
    const info = lstatSync(path);
    return info.isFile() && !info.isSymbolicLink() ? info : null;
  } catch { return null; }
}

export function inspectProjectMcpDeployment(profile, {
  root, home = homedir(), bun = process.execPath, sourceIdentity = null, candidate = null,
} = {}) {
  const compiled = candidate || compileProjectMcpLaunchAgent(profile, { root, home, bun, sourceIdentity });
  if (!compiled || compiled.profile !== profile) throw new Error('deployment preview candidate/profile mismatch');
  home = safeAbsolute(home, 'home');
  const plistPath = join(home, 'Library', 'LaunchAgents', `${compiled.launch_agent.Label}.plist`);
  const tokenPresent = Boolean(regularFile(compiled.token_file));
  const base = {
    schema_version: 'bonfyre.project-mcp.deployment-preview.v1',
    profile,
    plist_path: plistPath,
    token_file_present: tokenPresent,
    candidate: {
      label: compiled.launch_agent.Label,
      pack: compiled.capability_pack,
      port: String(compiled.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_PORT),
      source_sha256: compiled.source_identity.aggregate_sha256,
      token_file: compiled.token_file,
    },
  };
  if (!existsSync(plistPath)) return {
    ...base,
    state: 'missing',
    installed: null,
    changes: ['launch_agent_missing', ...(tokenPresent ? [] : ['token_file_missing'])],
    forbidden_inference: 'A missing or drifted local LaunchAgent is deployment state only; this preview does not install, bootstrap, stop, restart, authenticate, or grant authority.',
  };
  const plistInfo = regularFile(plistPath);
  if (!plistInfo) return {
    ...base,
    state: 'unreadable',
    installed: null,
    changes: ['launch_agent_not_regular_file'],
    forbidden_inference: 'A symlink, directory, device or unreadable LaunchAgent path is not admitted as deployment metadata.',
  };
  if (plistInfo.size > 1_000_000) return {
    ...base,
    state: 'unreadable',
    installed: null,
    changes: ['launch_agent_exceeds_read_bound'],
    forbidden_inference: 'Deployment preview refuses oversized LaunchAgent metadata instead of consuming it as trusted configuration.',
  };
  let source;
  try { source = readFileSync(plistPath, 'utf8'); }
  catch { return { ...base, state: 'unreadable', installed: null, changes: ['launch_agent_unreadable'], forbidden_inference: 'Unreadable local service metadata is not proof that a service is absent or healthy.' }; }
  if (source.startsWith('bplist00')) return {
    ...base,
    state: 'unreadable',
    installed: { format: 'binary-plist', label: null, pack: null, port: null, source_sha256: null, token_file: null },
    changes: ['binary_plist_requires_external_decoder'],
    forbidden_inference: 'This read-only preview does not invoke plutil merely to reinterpret a binary plist; binary presence is not service health.',
  };
  const keys = ['Label', 'BONFYRE_PROJECT_MCP_PACK', 'BONFYRE_PROJECT_MCP_PORT', 'BONFYRE_PROJECT_MCP_SOURCE_SHA256', 'BONFYRE_MCP_TOKEN_FILE'];
  const parsed = Object.fromEntries(keys.map(key => [key, plistStrings(source, key)]));
  if (Object.values(parsed).some(values => values.length > 1)) return {
    ...base,
    state: 'unreadable',
    installed: { format: 'xml-plist', duplicate_keys: keys.filter(key => parsed[key].length > 1) },
    changes: ['ambiguous_duplicate_metadata'],
    forbidden_inference: 'Duplicate deployment keys are ambiguous and refuse instead of selecting one value by position.',
  };
  const installed = {
    format: 'xml-plist',
    label: parsed.Label[0] || null,
    pack: parsed.BONFYRE_PROJECT_MCP_PACK[0] || null,
    port: parsed.BONFYRE_PROJECT_MCP_PORT[0] || null,
    source_sha256: parsed.BONFYRE_PROJECT_MCP_SOURCE_SHA256[0] || null,
    token_file: parsed.BONFYRE_MCP_TOKEN_FILE[0] || null,
  };
  const changes = [];
  if (installed.label !== base.candidate.label) changes.push('label');
  if (installed.pack !== base.candidate.pack) changes.push('pack');
  if (installed.port !== base.candidate.port) changes.push('port');
  if (installed.source_sha256 !== base.candidate.source_sha256) changes.push('source_sha256');
  if (installed.token_file !== base.candidate.token_file) changes.push('token_file_path');
  if (!tokenPresent) changes.push('token_file_missing');
  const state = !installed.source_sha256 ? 'unattested' : changes.length ? 'drifted' : 'current';
  return {
    ...base,
    state,
    installed,
    changes,
    forbidden_inference: 'Matching LaunchAgent metadata and token-file presence do not prove a listening process, successful authentication, runtime health, or effect authority.',
  };
}

if (import.meta.main) {
  const profile = process.argv[2];
  const rootArg = process.argv.find(arg => arg.startsWith('--root='));
  const homeArg = process.argv.find(arg => arg.startsWith('--home='));
  if (!profile || !PROJECT_MCP_SERVICE_PROFILES[profile] || !rootArg) {
    console.error(`usage: bun project_service_profiles.mjs <${PROJECT_MCP_SERVICE_PROFILE_NAMES.join('|')}> --root=/absolute/repo [--home=/absolute/home]`);
    process.exit(2);
  }
  console.log(JSON.stringify(compileProjectMcpLaunchAgent(profile, {
    root: rootArg.slice('--root='.length),
    ...(homeArg ? { home: homeArg.slice('--home='.length) } : {}),
  }), null, 2));
}
