import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  PROJECT_MCP_SERVICE_PROFILES,
  PROJECT_MCP_SERVICE_PROFILE_NAMES,
  PROJECT_MCP_SOURCE_SET,
  compileProjectMcpLaunchAgent,
  inspectProjectMcpDeployment,
} from '../project_service_profiles.mjs';

test('Project MCP product profiles have unique fixed local identities and deterministic candidates', () => {
  expect(PROJECT_MCP_SERVICE_PROFILE_NAMES).toEqual(['build', 'operate', 'work', 'model', 'commerce', 'account', 'connect', 'evidence', 'institution']);
  const ports = new Set(), labels = new Set(), tokens = new Set();
  for (const profile of PROJECT_MCP_SERVICE_PROFILE_NAMES) {
    const item = PROJECT_MCP_SERVICE_PROFILES[profile];
    expect(item.host).toBe('127.0.0.1');
    expect(item.port).toBeGreaterThanOrEqual(3061);
    expect(item.port).toBeLessThanOrEqual(3069);
    expect(ports.has(item.port)).toBe(false); ports.add(item.port);
    expect(labels.has(item.label)).toBe(false); labels.add(item.label);
    expect(tokens.has(item.token_name)).toBe(false); tokens.add(item.token_name);
  }
});

test('profile compiler emits a candidate-only LaunchAgent and no secret value', () => {
  const base = mkdtempSync(join(tmpdir(), 'bf-project-profile-'));
  const root = join(base, 'repo'), home = join(base, 'home'), fakeBun = join(base, 'bun');
  mkdirSync(join(root, 'integrations/mcp'), { recursive: true });
  mkdirSync(home, { recursive: true });
  for (const relative of PROJECT_MCP_SOURCE_SET) {
    const file = join(root, relative);
    mkdirSync(join(file, '..'), { recursive: true });
    writeFileSync(file, `fixture:${relative}\n`);
  }
  writeFileSync(fakeBun, 'fixture');
  try {
    const result = compileProjectMcpLaunchAgent('operate', { root, home, bun: fakeBun });
    expect(result).toMatchObject({
      schema_version: 'bonfyre.project-mcp.service-profile.v1',
      profile: 'operate',
      endpoint: 'http://127.0.0.1:3062',
      capability_pack: 'operate',
      deployment_state: 'candidate_only',
      listener_availability: 'not_verified',
    });
    expect(result.source_identity.schema_version).toBe('bonfyre.project-mcp.source-set.v1');
    expect(result.source_identity.aggregate_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.source_identity.files).toHaveLength(PROJECT_MCP_SOURCE_SET.length);
    expect(result.launch_agent.Label).toBe('com.bonfyre.project-mcp-operate');
    expect(result.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_PACK).toBe('operate');
    expect(result.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_SOURCE_SHA256).toBe(result.source_identity.aggregate_sha256);
    expect(result.launch_agent.EnvironmentVariables.BONFYRE_MCP_TOKEN_FILE).toBe(join(home, '.bonfyre/mcp/project-operate.token'));
    const missing = inspectProjectMcpDeployment('operate', { root, home, bun: fakeBun, candidate: result });
    expect(missing).toMatchObject({ state: 'missing', token_file_present: false, installed: null });
    expect(missing.changes).toContain('launch_agent_missing');
    expect(missing.changes).toContain('token_file_missing');

    mkdirSync(join(home, '.bonfyre/mcp'), { recursive: true });
    mkdirSync(join(home, 'Library/LaunchAgents'), { recursive: true });
    writeFileSync(result.token_file, 'secret-never-returned');
    const plistPath = join(home, 'Library/LaunchAgents', `${result.launch_agent.Label}.plist`);
    const plist = `<?xml version="1.0"?><plist><dict>
<key>Label</key><string>${result.launch_agent.Label}</string>
<key>BONFYRE_PROJECT_MCP_PACK</key><string>${result.capability_pack}</string>
<key>BONFYRE_PROJECT_MCP_PORT</key><string>${result.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_PORT}</string>
<key>BONFYRE_PROJECT_MCP_SOURCE_SHA256</key><string>${result.source_identity.aggregate_sha256}</string>
<key>BONFYRE_MCP_TOKEN_FILE</key><string>${result.token_file}</string>
</dict></plist>`;
    writeFileSync(plistPath, plist);
    const current = inspectProjectMcpDeployment('operate', { root, home, bun: fakeBun, candidate: result });
    expect(current).toMatchObject({ state: 'current', token_file_present: true, changes: [] });
    expect(JSON.stringify(current)).not.toContain('secret-never-returned');

    writeFileSync(plistPath, plist.replace(result.source_identity.aggregate_sha256, '0'.repeat(64)));
    const drifted = inspectProjectMcpDeployment('operate', { root, home, bun: fakeBun, candidate: result });
    expect(drifted.state).toBe('drifted');
    expect(drifted.changes).toContain('source_sha256');

    writeFileSync(plistPath, plist.replace(
      `<key>BONFYRE_PROJECT_MCP_SOURCE_SHA256</key><string>${result.source_identity.aggregate_sha256}</string>\n`, ''));
    const unattested = inspectProjectMcpDeployment('operate', { root, home, bun: fakeBun, candidate: result });
    expect(unattested.state).toBe('unattested');
    expect(JSON.stringify(result)).not.toContain('Bearer ');
    expect(JSON.stringify(result)).not.toContain('token_value');
    const connect = compileProjectMcpLaunchAgent('connect', { root, home, bun: fakeBun, sourceIdentity: result.source_identity });
    expect(connect.endpoint).toBe('http://127.0.0.1:3066');
    expect(connect.capability_pack).toBe('connect');
    expect(connect.source_identity).toBe(result.source_identity);
    expect(connect.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_SOURCE_SHA256).toBe(connect.source_identity.aggregate_sha256);
    expect(connect.launch_agent.EnvironmentVariables.BONFYRE_MCP_TOKEN_FILE).toBe(join(home, '.bonfyre/mcp/project-connect.token'));
    const work = compileProjectMcpLaunchAgent('work', { root, home, bun: fakeBun, sourceIdentity: result.source_identity });
    expect(work.endpoint).toBe('http://127.0.0.1:3069');
    expect(work.capability_pack).toBe('work');
    const evidence = compileProjectMcpLaunchAgent('evidence', { root, home, bun: fakeBun, sourceIdentity: result.source_identity });
    expect(evidence.endpoint).toBe('http://127.0.0.1:3067');
    expect(evidence.capability_pack).toBe('evidence');
    const institution = compileProjectMcpLaunchAgent('institution', { root, home, bun: fakeBun, sourceIdentity: result.source_identity });
    expect(institution.endpoint).toBe('http://127.0.0.1:3068');
    expect(institution.capability_pack).toBe('institution');
    expect(() => compileProjectMcpLaunchAgent('connect', { root, home, bun: fakeBun,
      sourceIdentity: { schema_version: 'bonfyre.project-mcp.source-set.v1', aggregate_sha256: 'bad', files: [] } })).toThrow('source identity');
    expect(() => compileProjectMcpLaunchAgent('unknown', { root, home, bun: fakeBun })).toThrow('unknown');
  } finally { rmSync(base, { recursive: true, force: true }); }
});
