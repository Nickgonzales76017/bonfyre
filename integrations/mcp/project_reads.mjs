// Mac/project-local read owner. No Python fallback, caller-selected root,
// arbitrary shell, mutation command, model request, or service management.
import { closeSync, constants, existsSync, fstatSync, lstatSync, openSync, opendirSync, readSync, realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { basename, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { summarizeChanges } from './project_delta_watcher.mjs';
import { CAPABILITY_PACKS, CAPABILITY_PACK_NAMES, capabilityPackAllows, capabilityPack as getCapabilityPack } from './capability_packs.mjs';

const EXCLUDES = new Set(['.git', 'node_modules', '.zig-cache', 'zig-out', 'target', '__pycache__', '.DS_Store']);
const FILE_BYTES = 10_000_000, RESPONSE_BYTES = 512 * 1024, TREE_ENTRIES = 20_000;
const same = (a, b) => ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(key => a[key] === b[key]);
const integer = (value, fallback, low, high, field) => {
  const n = value ?? fallback;
  if (!Number.isSafeInteger(n)) throw new Error(`${field} must be an integer`);
  return Math.max(low, Math.min(high, n));
};
const text = (value, fallback, field, max = 4096) => {
  const result = value ?? fallback;
  if (typeof result !== 'string' || result.includes('\0') || result.length > max) throw new Error(`invalid ${field}`);
  return result;
};
const flag = (value, field) => {
  if (value !== undefined && typeof value !== 'boolean') throw new Error(`${field} must be boolean`);
  return value === true;
};
function fields(args, allowed) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments must be an object');
  for (const key of Object.keys(args)) if (!allowed.includes(key)) throw new Error(`unknown argument: ${key}`);
}
function* lines(text) {
  // Python splitlines compatibility without allocating an array per source
  // line (a ten-MB newline-only file otherwise creates millions of entries).
  // eslint-disable-next-line no-control-regex -- These exact control characters are Python's documented line separators.
  const breaks = /\r\n|[\n\r\v\f\x1c-\x1e\u0085\u2028\u2029]/g;
  let start = 0, match;
  while ((match = breaks.exec(text))) { yield text.slice(start, match.index); start = breaks.lastIndex; }
  if (start < text.length) yield text.slice(start);
}

export function createProjectReads({ root, env = process.env, run, catalogTools = [] } = {}) {
  if (typeof root !== 'string' || !root || typeof run !== 'function') throw new Error('project reads require a root and bounded runner');
  if (!Array.isArray(catalogTools)) throw new Error('catalogTools must be an array');
  const project = realpathSync(root);
  if (!lstatSync(project).isDirectory()) throw new Error('project root must be a directory');
  const childEnv = { ...env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0' };
  for (const name of Object.keys(childEnv)) {
    if (['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR', 'GIT_EXTERNAL_DIFF'].includes(name) || name.startsWith('GIT_CONFIG_')) delete childEnv[name];
  }
  Object.freeze(childEnv);

  function confined(value = '.', { missing = false } = {}) {
    const input = text(value, '.', 'project path');
    const path = resolve(project, input), local = relative(project, path);
    if (local === '..' || local.startsWith(`..${sep}`) || isAbsolute(local)) throw new Error('path escapes project root');
    // Reject symlinks, including inward-pointing ones, rather than treating
    // a caller path as permission to follow a second owner's filesystem.
    let cursor = project;
    for (const part of local.split(sep).filter(Boolean)) {
      cursor = join(cursor, part);
      let stat;
      try { stat = lstatSync(cursor); }
      catch (error) { if (missing && error.code === 'ENOENT') return path; throw error; }
      if (stat.isSymbolicLink()) throw new Error('symlink project paths are not admitted');
    }
    return path;
  }

  async function execute(argv, { timeoutMs = 30_000, allowed = [0], env: executionEnv = childEnv, truncate = false } = {}) {
    const receipt = await run(argv, { cwd: project, timeoutMs, env: executionEnv });
    if (!receipt || receipt.timeout || (receipt.overflow && !truncate) || !allowed.includes(receipt.exit_code)) throw new Error(`${basename(argv[0])} read failed${receipt?.timeout ? ': timeout' : receipt?.overflow ? ': output limit' : ''}`);
    if (typeof receipt.stdout !== 'string' || typeof receipt.stderr !== 'string') throw new Error('invalid read receipt');
    if (Buffer.byteLength(receipt.stdout) > RESPONSE_BYTES || Buffer.byteLength(receipt.stderr) > RESPONSE_BYTES) { if (truncate && Buffer.byteLength(receipt.stderr) <= RESPONSE_BYTES) { let bytes = 0; const kept = []; for (const line of receipt.stdout.split('\n')) { if ((bytes += Buffer.byteLength(line) + 1) > RESPONSE_BYTES) break; kept.push(line); } return { ...receipt, stdout: kept.join('\n'), truncated: true }; } throw new Error('read output exceeds 512 KiB'); } // search keeps whole lines within budget
    return receipt;
  }
  const git = argv => execute(['git', '--no-pager', '--literal-pathspecs', '-c', 'core.fsmonitor=false', ...argv]);

  function statusFromPorcelain(source) {
    const entries = source.split('\0');
    let head, branch, upstream = '', ahead = 0, behind = 0, clean = true, headers = true, fallback = false;
    const working = [], staged = [];
    const simpleStatus = new Set(['M', 'A', 'D', 'T']);
    const unsafePath = value => /[\t\r\n]/.test(value);
    const addSimple = (list, code, pathValue) => {
      if (code === '.') return;
      if (!simpleStatus.has(code) || unsafePath(pathValue)) { fallback = true; return; }
      list.push(`${code}\t${pathValue}`);
    };
    for (let index = 0; index < entries.length; index++) {
      const entry = entries[index];
      if (!entry) continue;
      if (headers && entry.startsWith('# ')) {
        if (entry.startsWith('# branch.oid ')) head = entry.slice(13);
        else if (entry.startsWith('# branch.head ')) branch = entry.slice(14);
        else if (entry.startsWith('# branch.upstream ')) upstream = entry.slice(18);
        else if (entry.startsWith('# branch.ab ')) {
          const match = /^# branch\.ab \+(\d+) -(\d+)$/.exec(entry);
          if (!match || !Number.isSafeInteger(Number(match[1])) || !Number.isSafeInteger(Number(match[2]))) throw new Error('invalid Git ahead/behind header');
          ahead = Number(match[1]); behind = Number(match[2]);
        }
        continue;
      }
      headers = false;
      clean = false;
      if (entry.startsWith('? ') || entry.startsWith('! ')) continue;
      if (entry.startsWith('1 ')) {
        const match = /^1 ([^ ]{2}) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) (.*)$/.exec(entry);
        if (!match) { fallback = true; continue; }
        const [, xy,,,,,,, pathValue] = match;
        addSimple(staged, xy[0], pathValue);
        addSimple(working, xy[1], pathValue);
        continue;
      }
      if (entry.startsWith('2 ')) {
        const match = /^2 ([^ ]{2}) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([RC])(\d+) (.*)$/.exec(entry);
        const original = entries[++index];
        if (!match || original === undefined) { fallback = true; continue; }
        const [, xy,,,,,,, renameCode, score, pathValue] = match;
        if (unsafePath(pathValue) || unsafePath(original)) { fallback = true; continue; }
        const add = (list, code) => {
          if (code === '.') return;
          if (code === 'R' || code === 'C') list.push(`${renameCode}${score}\t${original}\t${pathValue}`);
          else addSimple(list, code, pathValue);
        };
        add(staged, xy[0]); add(working, xy[1]);
        continue;
      }
      // Unmerged and unknown future porcelain records retain the old diff-based path.
      fallback = true;
    }
    if (!head || branch === undefined) throw new Error('Git status lacks branch headers');
    return { head, branch, upstream, ahead, behind, clean, fallback, working, staged };
  }

  async function status() {
    const observed = await git(['status', '--porcelain=v2', '--branch', '-z']);
    const parsed = statusFromPorcelain(observed.stdout);
    let workingSummary, stagedSummary;
    if (parsed.fallback) {
      const [working, staged] = await Promise.all([
        git(['diff', '--no-ext-diff', '--no-textconv', '--name-status']),
        git(['diff', '--no-ext-diff', '--no-textconv', '--cached', '--name-status']),
      ]);
      workingSummary = summarizeChanges(working.stdout, 30);
      stagedSummary = summarizeChanges(staged.stdout, 30);
    } else {
      workingSummary = summarizeChanges(parsed.working.join('\n'), 30);
      stagedSummary = summarizeChanges(parsed.staged.join('\n'), 30);
    }
    return { root: project, branch: parsed.branch === '(detached)' ? '' : parsed.branch, upstream: parsed.upstream,
      ahead: parsed.ahead, behind: parsed.behind, head: parsed.head === '(initial)' ? 'HEAD' : parsed.head, clean: parsed.clean,
      working: workingSummary, staged: stagedSummary,
      performance: {
        path: parsed.fallback ? 'porcelain-v2+diff-fallback' : 'porcelain-v2',
        git_processes: parsed.fallback ? 3 : 1,
        fast_path: !parsed.fallback,
        fallback_reason: parsed.fallback ? 'unmerged_or_unrecognized_porcelain_record' : null,
      } };
  }

  function tree(args) {
    const requested = text(args.path, '.', 'project path'), base = confined(requested);
    const depth = integer(args.depth, 3, 1, 8, 'depth'), limit = integer(args.limit, 500, 1, 5000, 'limit');
    const items = []; let entries = 0, truncated = false;
    function visit(directory, level) {
      if (level > depth || truncated) return;
      confined(directory);
      const handle = opendirSync(directory);
      try {
        let item;
        while (!truncated && (item = handle.readSync())) {
          if (++entries > TREE_ENTRIES) { truncated = true; break; }
          if (EXCLUDES.has(item.name) || item.isSymbolicLink()) continue;
          const path = join(directory, item.name);
          confined(path);
          const stat = lstatSync(path);
          if (!stat.isDirectory() && !stat.isFile()) continue;
          if (items.length === limit) { truncated = true; break; }
          items.push({ path: relative(project, path), type: stat.isDirectory() ? 'dir' : 'file', size: stat.isFile() ? stat.size : null });
          if (stat.isDirectory()) visit(path, level + 1);
        }
      } finally { handle.closeSync(); }
    }
    if (lstatSync(base).isDirectory()) visit(base, 1);
    // Only the bounded observed set is sorted, never an unbounded rglob.
    items.sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
    return { root: project, base: requested, items, truncated };
  }

  function read(args, budget = { remaining: FILE_BYTES }) {
    fields(args, ['path', 'start_line', 'end_line', 'if_sha256']);
    if (typeof args.path !== 'string' || !args.path) throw new Error('path is required');
    if (args.if_sha256 !== undefined && (typeof args.if_sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(args.if_sha256)))
      throw new Error('if_sha256 must be a lowercase SHA-256 digest');
    const path = confined(text(args.path, undefined, 'project path'));
    const start = integer(args.start_line, 1, 1, Number.MAX_SAFE_INTEGER - 5000, 'start_line');
    const end = integer(args.end_line, 400, start, start + 5000, 'end_line');
    const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    let bytes;
    try {
      const before = fstatSync(fd, { bigint: true });
      if (!before.isFile() || before.size > BigInt(FILE_BYTES)) throw new Error('file is not a regular file within the 10000000-byte read budget');
      if (before.size > BigInt(budget.remaining)) throw new Error('combined source reads exceed 32 MiB');
      budget.remaining -= Number(before.size);
      const buffer = Buffer.alloc(Number(before.size) + 1); let used = 0, n;
      while (used < buffer.length && (n = readSync(fd, buffer, used, buffer.length - used, null)) > 0) used += n;
      confined(path);
      if (used !== Number(before.size) || !same(before, fstatSync(fd, { bigint: true })) || !same(before, lstatSync(path, { bigint: true }))) throw new Error('project file changed during read');
      bytes = buffer.subarray(0, used);
    } finally { closeSync(fd); }
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    const identity = { path: relative(project, path), sha256, source_bytes: bytes.length };
    if (args.if_sha256 === sha256) return { ...identity, not_modified: true };
    const selected = []; let total = 0, outputBytes = 0;
    for (const line of lines(new TextDecoder().decode(bytes))) {
      total++;
      if (total < start || total > end) continue;
      const entry = { line: total, text: line };
      outputBytes += Buffer.byteLength(JSON.stringify(entry));
      if (outputBytes > RESPONSE_BYTES) throw new Error('selected lines exceed 512 KiB; request a smaller range');
      selected.push(entry);
    }
    return { ...identity, total_lines: total, lines: selected, next_start_line: end < total ? end + 1 : null };
  }

  function readRequest(args) {
    fields(args, ['path', 'start_line', 'end_line', 'if_sha256', 'files']);
    if (args.files === undefined) return read(args);
    if (Object.keys(args).some(key => key !== 'files')) throw new Error('use either path or files, not both');
    if (!Array.isArray(args.files) || args.files.length < 1 || args.files.length > 16)
      throw new Error('files must contain 1–16 read requests');
    const budget = { remaining: 32 * 1024 * 1024 };
    const result = { files: [], consistency: 'Each file is checked during its read; this is not an atomic multi-file snapshot.' };
    for (const file of args.files) {
      result.files.push(read(file, budget));
      if (Buffer.byteLength(JSON.stringify(result)) > RESPONSE_BYTES)
        throw new Error('combined read output exceeds 512 KiB; request smaller ranges');
    }
    return result;
  }

  async function organDriftStatus(args) {
    fields(args, ['target', 'verify']);
    const target = text(args.target, '', 'organ target', 128).trim();
    if (target && !/^bonfyre(?:-[a-z0-9][a-z0-9-]*)?$/.test(target)) throw new Error('invalid organ target');
    const script = confined('scripts/organ_drift.sh');
    if (!lstatSync(script).isFile()) throw new Error('organ drift owner is not a file');
    const argv = [script, ...(args.verify === true ? ['--verify'] : []), '--json', ...(target ? ['--only', target] : [])];
    const result = await execute(argv, { timeoutMs: 60_000, allowed: args.verify === true ? [0, 1] : [0] });
    let report;
    try { report = JSON.parse(result.stdout); } catch { throw new Error('organ drift owner returned malformed JSON'); }
    if (!report || report.schema !== 'bonfyre.organ-drift-report.v2') throw new Error('organ drift owner returned wrong schema');
    return {
      ...report,
      verified: args.verify === true ? result.exit_code === 0 : null,
      forbidden_inference: 'Byte identity proves only that a managed carrier matches the checkout build. It does not prove semantic correctness, dependency health, service liveness, or effect authority.',
    };
  }

  async function repositorySlice(args) {
    fields(args, ['path', 'ref', 'limit']);
    const requested = text(args.path, '.', 'project path', 2048);
    if (/[\x00-\x1f\x7f]/.test(requested)) throw new Error('invalid repository slice path');
    const target = confined(requested, { missing: true });
    const prefix = relative(project, target).split(sep).join('/');
    const ref = text(args.ref, 'HEAD', 'repository ref', 128);
    if (ref !== 'HEAD' && !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(ref))
      throw new Error('repository slice ref requires HEAD or an exact commit identity');
    if (args.limit !== undefined && (!Number.isSafeInteger(args.limit) || args.limit < 1 || args.limit > 1000))
      throw new Error('repository slice limit must be between 1 and 1000');
    const limit = integer(args.limit, 100, 1, 1000, 'limit');
    const resolved = await git(['rev-parse', '--verify', '--end-of-options', ref + '^{commit}']);
    const commit = resolved.stdout.trim();
    if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(commit))
      throw new Error('repository slice could not pin its commit');
    // Read the Git tree only. Neither worktree content nor remote state is read,
    // and an actual commit ID pins both the filtered selection and object IDs.
    const result = await git(['ls-tree', '-r', '-z', '--full-tree', commit, ...(prefix ? ['--', prefix] : [])]);
    const records = result.stdout.split('\0').filter(Boolean);
    const files = [];
    let eligible = 0, omittedKinds = 0;
    for (const record of records) {
      const row = /^(100644|100755|120000|160000) (blob|commit) ([a-f0-9]{40}|[a-f0-9]{64})\t([^\x00]+)$/.exec(record);
      if (!row) throw new Error('repository slice returned an unexpected tree entry');
      const [,mode,kind,oid,path] = row;
      if (prefix && path !== prefix && !path.startsWith(prefix + '/'))
        throw new Error('repository slice escaped its selected subtree');
      if (kind !== 'blob' || !['100644','100755'].includes(mode)) { omittedKinds++; continue; }
      eligible++;
      if (files.length < limit) files.push({path,mode,object_id:oid});
    }
    confined(target, { missing: true });
    return {
      schema: 'bonfyre.project.repository-slice.v1',
      root: project, path: prefix || '.', commit,
      files, eligible, omitted: eligible - files.length,
      excluded_non_regular: omittedKinds,
      tree_sha256: createHash('sha256').update(result.stdout).digest('hex'),
      scope: 'One pinned local commit and selected subtree; regular tracked blobs only.',
      cannot_infer: 'This read-only Git tree is not a dirty-checkout snapshot, a filtered clone, a JOSH proxy, a remote-ref lease, provider availability, or authorization to write or publish.',
    };
  }

  function capabilityLens(args) {
    fields(args, ['query', 'owner', 'effect', 'detail', 'limit', 'pack']);
    const query = text(args.query, '', 'capability query', 256).trim().toLowerCase();
    const owner = text(args.owner, '', 'capability owner', 64).trim();
    const pack = text(args.pack, '', 'capability pack', 32).trim();
    const effect = text(args.effect, 'all', 'capability effect', 16).trim();
    const detail = text(args.detail, 'summary', 'capability detail', 16).trim();
    const limit = integer(args.limit, 20, 1, 50, 'limit');
    if (pack && !Object.hasOwn(CAPABILITY_PACKS, pack)) throw new Error('unknown capability pack');
    if (!['all', 'read', 'effect'].includes(effect)) throw new Error('capability effect must be all, read or effect');
    if (!['summary', 'full'].includes(detail)) throw new Error('capability detail must be summary or full');

    const terms = query.split(/\s+/).filter(Boolean);
    const groups = {};
    for (const tool of catalogTools) {
      const key = tool['x-bonfyre-owner'] || 'unknown';
      const readOnly = tool.annotations?.readOnlyHint === true;
      groups[key] ??= { total: 0, read: 0, effect: 0 };
      groups[key].total += 1;
      groups[key][readOnly ? 'read' : 'effect'] += 1;
    }

    const catalogByName = new Map(catalogTools.map((tool) => [tool.name, tool]));
    const ranked = [];
    for (const tool of catalogTools) {
      const toolOwner = tool['x-bonfyre-owner'] || 'unknown';
      const readOnly = tool.annotations?.readOnlyHint === true;
      if (!capabilityPackAllows(tool, pack)) continue;
      if (owner && toolOwner !== owner) continue;
      if (effect === 'read' && !readOnly) continue;
      if (effect === 'effect' && readOnly) continue;
      const required = Array.isArray(tool.inputSchema?.required) ? tool.inputSchema.required : [];
      const searchable = [tool.name, tool.description || '', toolOwner, ...required].join(' ').toLowerCase();
      if (terms.some((term) => !searchable.includes(term))) continue;
      let score = 0;
      for (const term of terms) {
        if (tool.name.toLowerCase() === term) score += 100;
        else if (tool.name.toLowerCase().includes(term)) score += 25;
        if ((tool.description || '').toLowerCase().includes(term)) score += 5;
        if (toolOwner.includes(term)) score += 3;
        if (required.some((field) => field.toLowerCase().includes(term))) score += 2;
      }
      ranked.push({ tool, score });
    }
    ranked.sort((a, b) => b.score - a.score || a.tool.name.localeCompare(b.tool.name));
    const selected = ranked.slice(0, limit).map(({ tool }) => {
      const summary = {
        name: tool.name,
        owner: tool['x-bonfyre-owner'] || 'unknown',
        read_only: tool.annotations?.readOnlyHint === true,
        destructive: tool.annotations?.destructiveHint === true,
        required: Array.isArray(tool.inputSchema?.required) ? tool.inputSchema.required : [],
        description: tool.description || '',
      };
      return detail === 'full' ? { ...summary, inputSchema: tool.inputSchema, annotations: tool.annotations || {} } : summary;
    });
    const publicTool = (tool) => Object.fromEntries(Object.entries(tool).filter(([key]) => key !== 'x-bonfyre-owner'));
    const apertureTools = catalogTools.filter((tool) => capabilityPackAllows(tool, pack));
    const apertureReadTools = apertureTools.filter((tool) => tool.annotations?.readOnlyHint === true).length;
    const apertureEffectTools = apertureTools.length - apertureReadTools;
    const publicApertureJson = JSON.stringify(apertureTools.map(publicTool));
    const catalogPublicBytes = Buffer.byteLength(publicApertureJson);
    const toolsetSha256 = createHash('sha256').update(publicApertureJson).digest('hex');
    const selectedPayloadBytes = Buffer.byteLength(JSON.stringify(selected));
    const payloadReductionPercent = catalogPublicBytes === 0 ? 0
      : Math.max(0, Math.min(100, Math.round((1 - selectedPayloadBytes / catalogPublicBytes) * 10_000) / 100));
    const packDefinition = pack ? getCapabilityPack(pack) : null;
    const route = (name) => {
      const tool = catalogByName.get(name);
      if (tool) return {
        name,
        declared: true,
        owner: tool['x-bonfyre-owner'] || 'unknown',
        read_only: tool.annotations?.readOnlyHint === true,
        carrier: 'project-mcp',
      };
      const accountScoped = name.startsWith('workspace_') || name.startsWith('bonfyre_admin_');
      return {
        name,
        declared: false,
        owner: accountScoped ? 'AccountWorkspaceMcp/OAuth' : 'external-or-unbound',
        read_only: null,
        carrier: accountScoped ? 'account-mcp' : 'not-in-project-aperture',
      };
    };
    const packPlan = packDefinition ? {
      outcome: packDefinition.outcome || null,
      entry_reads: (packDefinition.entry_reads || []).map(route),
      primary_effects: (packDefinition.primary_effects || []).map(route),
      declared_tools: catalogTools.filter((tool) => capabilityPackAllows(tool, pack)).length,
      handoffs: (packDefinition.handoffs || []).map((item) => ({
        name: item.name,
        declared: false,
        owner: item.owner,
        carrier: item.carrier,
        kind: item.kind,
        required: item.required,
        read_only: item.kind === 'read' ? null : false,
      })),
      required_handoffs: (packDefinition.handoffs || []).filter((item) => item.required).map((item) => item.name),
    } : null;

    return {
      schema_version: 'bonfyre.mcp.capability-lens.v1',
      query,
      pack: pack || null,
      pack_description: pack ? CAPABILITY_PACKS[pack].description : null,
      pack_plan: packPlan,
      available_packs: Object.fromEntries(CAPABILITY_PACK_NAMES.map((name) => [name, {
        description: CAPABILITY_PACKS[name].description,
        contract_sha256: getCapabilityPack(name).contract_sha256,
        outcome: CAPABILITY_PACKS[name].outcome || null,
      }])),
      owner: owner || null,
      effect,
      detail,
      total_tools: catalogTools.length,
      matched: ranked.length,
      returned: selected.length,
      performance: {
        aperture_tools: apertureTools.length,
        aperture_read_tools: apertureReadTools,
        aperture_effect_tools: apertureEffectTools,
        toolset_sha256: toolsetSha256,
        catalog_public_bytes: catalogPublicBytes,
        selected_payload_bytes: selectedPayloadBytes,
        payload_reduction_percent: payloadReductionPercent,
        measurement: 'UTF-8 bytes of JSON model-visible tool definitions versus this lens tool projection; transport framing and host metadata are excluded.',
      },
      groups,
      tools: selected,
      cannot_infer: 'A tool appearing in this catalogue proves only that the MCP surface declares it. It does not establish current service health, account authority, provider admission, spend approval, or permission for an effect.',
    };
  }

  return async function call(name, args = {}) {
    if (name === 'project_tree' && args?.ref !== undefined) return repositorySlice(args);
    switch (name) {
      case 'project_status': fields(args, []); return status();
      case 'project_repository_slice': return repositorySlice(args);
      case 'mcp_capability_lens': return capabilityLens(args);
      case 'organ_drift_status': return organDriftStatus(args);
      case 'project_tree': fields(args, ['path', 'depth', 'limit']); return tree(args);
      case 'project_read': return readRequest(args);
      case 'project_search': {
        fields(args, ['query', 'path', 'glob', 'limit']);
        const query = text(args.query, undefined, 'query', 16_384), requested = text(args.path, '.', 'project path');
        const target = confined(requested), limit = integer(args.limit, 200, 1, 1000, 'limit');
        const argv = ['rg', '--no-config', '--no-follow', '--line-number', '--column', '--no-heading', '--color', 'never', '--max-count', String(limit), '--max-columns', '400', '--max-columns-preview', '--max-filesize', '2M']; // per-file cap; long generated lines trimmed
        argv.push(...(args.glob !== undefined ? ['--glob', text(args.glob, undefined, 'glob')] : ['--glob', '!architecture/atlas.index.json', '--glob', '!site/spaces/**', '--glob', '!**/*.min.js']));
        argv.push('-e', query, '--', target);
        const result = await execute(argv, { allowed: [0, 1], truncate: true });
        confined(target);
        const matches = result.stdout.split(/\r?\n/).filter(Boolean); return { query, path: requested, matches: matches.slice(0, limit), exit_code: result.exit_code, stderr: result.stderr, ...(result.truncated || matches.length > limit ? { truncated: true } : {}) };
      }
      case 'project_diff': {
        fields(args, ['path', 'staged', 'stat_only']);
        const path = confined(text(args.path, '.', 'project path'), { missing: true });
        const argv = ['diff', '--no-ext-diff', '--no-textconv'];
        if (flag(args.staged, 'staged')) argv.push('--cached');
        if (flag(args.stat_only, 'stat_only')) argv.push('--stat');
        argv.push('--', relative(project, path) || '.');
        const [value, diff] = await Promise.all([status(), git(argv)]);
        confined(path, { missing: true });
        return { status: value, diff };
      }
      case 'architecture_atlas': {
        fields(args, ['op', 'id', 'view', 'query']);
        const op = text(args.op, 'summary', 'Atlas operation').replaceAll('-', '_');
        if (!['summary', 'expand', 'get', 'cannot_infer', 'search', 'loss'].includes(op)) throw new Error('unknown Atlas operation');
        const key = op === 'expand' ? 'view' : op === 'search' ? 'query' : ['get', 'cannot_infer'].includes(op) ? 'id' : null;
        const arg = key ? text(args[key], '', `Atlas ${key}`) : '';
        // The owner binary may live here; declarations belong to the selected Project root.
        const owner = env.BONFYRE_ATLAS_BIN || fileURLToPath(new URL('../../cmd/BonfyreAtlas/bonfyre-atlas', import.meta.url));
        if (!existsSync(owner) || !lstatSync(owner).isFile()) throw new Error('Atlas query owner is not installed');
        const result = await execute([owner, 'query', op, arg], { timeoutMs: 120_000, env: Object.freeze({ ...childEnv, BONFYRE_ATLAS_DIR: join(project, 'architecture') }) });
        try { return JSON.parse(result.stdout); } catch { throw new Error('Atlas owner returned invalid JSON'); }
      }
      default: throw new Error(`unsupported project read: ${name}`);
    }
  };
}
