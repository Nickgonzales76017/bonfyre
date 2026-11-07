import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, readFile, readdir, realpath, rename, stat, unlink, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { publicTrainingReleases } from './training_public.mjs';
import { collapseExactPassages } from './recall_results.mjs';
import { createEstateControlPlane } from '../estate/control_plane.mjs';

const sha = (value) => createHash('sha256').update(value).digest('hex');
const clamp = (value, fallback, low, high) => {
  const number = Number(value ?? fallback);
  if (!Number.isSafeInteger(number)) throw new Error('integer option required');
  return Math.max(low, Math.min(high, number));
};
const ident = (value, field) => {
  const text = String(value ?? '').trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/.test(text)) throw new Error(`${field} must be 1-128 safe identifier characters`);
  return text;
};
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const formatUsd = (value, field) => {
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0 || number > 1_000_000) throw new Error(`${field} must be between 0 and 1000000`);
  // Python's legacy f"{float(value):.6f}" rounds the exact binary64 value
  // to nearest, ties to even. JS toFixed chooses upward on an exact tie.
  const bits = Buffer.allocUnsafe(8);
  bits.writeDoubleBE(number);
  const raw = bits.readBigUInt64BE();
  const exponentBits = Number((raw >> 52n) & 0x7ffn);
  const mantissa = (raw & ((1n << 52n) - 1n)) | (exponentBits ? 1n << 52n : 0n);
  const exponent = exponentBits ? exponentBits - 1023 - 52 : -1022 - 52;
  let numerator = mantissa * 1_000_000n;
  let denominator = 1n;
  if (exponent < 0) denominator <<= BigInt(-exponent);
  else numerator <<= BigInt(exponent);
  let scaled = numerator / denominator;
  const remainder = numerator % denominator;
  if (2n * remainder > denominator || (2n * remainder === denominator && scaled % 2n)) scaled++;
  return `${scaled / 1_000_000n}.${String(scaled % 1_000_000n).padStart(6, '0')}`;
};
const validTemperature = (value) => {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number) || number < 0 || number > 2) throw new Error('temperature must be between 0 and 2');
  return number;
};
const assertSuccess = (result, label) => {
  if (result.exit_code !== 0 || result.timeout || result.overflow) throw new Error(`${label} failed: ${String(result.stderr || '').slice(-4000)}`);
  return result;
};
const parseNativeJson = (result, label) => {
  assertSuccess(result, label);
  try { return JSON.parse(result.stdout); } catch { throw new Error(`${label} returned malformed JSON`); }
};
const digestFile = async (file) => { try { return sha(await readFile(file)); } catch (error) { if (error.code === 'ENOENT') return null; throw error; } };
const fileExists = async (file) => { try { return (await stat(file)).isFile(); } catch { return false; } };
// Python's legacy request/verification digests used json.dumps(sort_keys=True).
// The two callers here have fixed request fields or parsed facilitator JSON.
// Refuse number spellings that cannot be reproduced exactly from JS Number.
const codepointCompare = (a, b) => {
  const left = Array.from(a, (char) => char.codePointAt(0));
  const right = Array.from(b, (char) => char.codePointAt(0));
  for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
  return left.length - right.length;
};
const pythonJson = (value, compact = false, depth = 0) => {
  if (depth > 32) throw new Error('JSON nesting exceeds receipt digest bound');
  if (value === null) return 'null';
  if (typeof value === 'string') return JSON.stringify(value).replace(/[\u007f-\uffff]/g, (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`);
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value)) throw new Error('non-integral JSON number cannot be parity-digested');
    return String(value);
  }
  const comma = compact ? ',' : ', ', colon = compact ? ':' : ': ';
  if (Array.isArray(value)) return `[${value.map((item) => pythonJson(item, compact, depth + 1)).join(comma)}]`;
  if (object(value)) return `{${Object.keys(value).sort(codepointCompare).map((key) => `${pythonJson(key, compact, depth + 1)}${colon}${pythonJson(value[key], compact, depth + 1)}`).join(comma)}}`;
  throw new Error('unsupported JSON value for receipt digest');
};
const hasNonIntegralJsonNumber = (source) => {
  let quoted = false, escaped = false;
  for (let index = 0; index < source.length; index++) {
    const char = source[index];
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === '"') quoted = false;
      continue;
    }
    if (char === '"') { quoted = true; continue; }
    if (!/[-0-9]/.test(char)) continue;
    const match = source.slice(index).match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/);
    if (!match) continue;
    if (/[.eE]/.test(match[0])) return true;
    index += match[0].length - 1;
  }
  return false;
};
const isoNow = () => new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');

const NATIVE_COPY_SUFFIX = /\.(?:bak[^/]*|old(?:[._-].*)?|orig(?:[._-].*)?|archive(?:[._-].*)?|patched\d*(?:[._-].*)?|tmp(?:[._-].*)?|disabled(?:[._-].*)?)$/i;
const canonicalNativeName = name => /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(name) && !NATIVE_COPY_SUFFIX.test(name);

// No execution is required to classify a candidate. Shebang scripts are not
// native owners; in particular, an executable Python launcher cannot survive
// retirement merely by being named bonfyre-*. A Bun-compiled executable is
// Mach-O/ELF/PE, while source Bun scripts belong to a different explicit owner.
// The generic argv aperture cannot be an unreviewed effect carrier.
// Only commands with a specific audited pure/read contract can cross it.
// Mutations use destination-owned native tools with their own authority.
function nativeReadIntent(family, argv, home) {
  if (argv.length === 1 && ['--help', '-h', '--version'].includes(argv[0])) return true;
  if (family === 'BonfyreCMS') return argv.length === 2 && argv[0] === 'schema' && argv[1] === 'list';
  if (family === 'BonfyreWitness') return argv.length === 1 && argv[0] === 'status';
  if (family === 'BonfyreExperienceMap') return argv.length === 1 && argv[0] === 'stats';
  // Audited read-only inspector. The separate seeder is not admitted here.
  if (family === 'BonfyreWarehouse') {
    const estate = path.join(home, '.bonfyre', 'estate-local');
    if (argv[1] !== '--estate' || argv[2] !== estate) return false;
    if (argv.length === 3) return ['status', 'integrity', 'cohorts', 'verify'].includes(argv[0]);
    if (argv.length !== 7 || !['head', 'plan'].includes(argv[0])) return false;
    return argv[3] === '--semantic-kind'
      && /^[A-Za-z0-9_.:/-]{1,240}$/.test(argv[4])
      && argv[5] === '--kind' && argv[6] === 'parquet';
  }
  if (family === 'BonfyreProject' && argv[0] === 'foreign') {
    if (argv.length === 2 && argv[1] === 'conflicts') return true;
    if (argv.length === 3 && ['resolve', 'history'].includes(argv[1]))
      return /^https?:\/\/[^\s]+$/.test(argv[2]) && argv[2].length <= 2048;
    if (argv[1] !== 'twins' || argv.length < 2 || (argv.length - 2) % 2) return false;
    const values = Object.fromEntries(Array.from({ length: (argv.length - 2) / 2 }, (_, index) =>
      [argv[2 + index * 2], argv[3 + index * 2]]));
    if (Object.keys(values).length !== (argv.length - 2) / 2 ||
        Object.keys(values).some(key => !['--query', '--page', '--limit'].includes(key))) return false;
    if (values['--query'] !== undefined && (values['--query'].length > 160 ||
        /[\x00-\x1f]/.test(values['--query']))) return false; // oxlint-disable-line no-control-regex -- refuses control characters
    if (values['--page'] !== undefined && !/^(?:0|[1-9]\d{0,5})$/.test(values['--page'])) return false;
    if (values['--limit'] !== undefined && !/^(?:[1-9]|[1-9]\d{1,2})$/.test(values['--limit'])) return false;
    return true;
  }
  return false;
}

async function nativeFormat(file) {
  try {
    const info = await lstat(file);
    if (!info.isFile() || !(info.mode & 0o111)) return null;
    const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
      const bytes = Buffer.alloc(4);
      const { bytesRead } = await handle.read(bytes, 0, 4, 0);
      if (bytesRead < 4) return null;
      const first = bytes.readUInt32BE(0);
      if (bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) return 'ELF';
      if ([0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe, 0xcafebabe, 0xbebafeca].includes(first)) return 'Mach-O';
      if (bytes[0] === 0x4d && bytes[1] === 0x5a) return 'PE';
      return null;
    } finally { await handle.close(); }
  } catch { return null; }
}

async function boundedResponse(response, maximum = 32768) {
  const declared = Number(response.headers?.get?.('content-length'));
  if (Number.isFinite(declared) && declared > maximum) throw new Error('response exceeds byte limit');
  if (!response.body?.getReader) throw new Error('response body is not a bounded byte stream');
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximum) throw new Error('response exceeds byte limit');
      chunks.push(Buffer.from(value));
    }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
  return Buffer.concat(chunks, size);
}

// A narrowly scoped fallback for the official Dallas Legistar agenda table.
// The public API may return 500 while the already-observable meeting HTML
// remains accessible. Row positions are official HTML claims, not API item IDs.
export function parseLegistarAgendaHtml(html) {
  const table = html.match(/<table\b[^>]*\bid=(['"])[^'"]*gridMain_ctl00\1[^>]*>([\s\S]*?)<\/table>/i);
  if (!table) throw new Error('official Legistar gridMain table absent');
  const text = raw => String(raw)
    .replace(/<[^>]*>/g, ' ')
    .replace(/&(?:nbsp|#160);/gi, ' ')
    .replace(/&(?:amp|lt|gt|quot|apos|#39);/gi, entity => ({
      '&amp;':'&','&lt;':'<','&gt;':'>','&quot;':'"','&apos;':"'",'&#39;':"'"
    })[entity.toLowerCase()] ?? entity)
    .replace(/\s+/g, ' ').trim();
  const result = [];
  for (const row of table[2].matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...row[1].matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map(hit => text(hit[1]));
    if (cells.length < 6 || !cells[2] || !cells[5]) continue;
    const title = cells[5].slice(0, 3000);
    const addresses = [...title.matchAll(/\b\d{1,6}\s+(?:(?:[A-Za-z][A-Za-z.'-]*|[NSEW])\s+){1,5}(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Parkway|Pkwy)\b/gi)]
      .slice(0, 8).map(hit => hit[0]);
    result.push({ item_id:null, identity_kind:'official_html_agenda_number_and_file',
      agenda_number:cells[2], file:cells[0], title,
      funding_amount_mentions:[...title.matchAll(/\$\s*[\d,]+(?:\.\d{2})?/g)].slice(0,12).map(hit=>hit[0]),
      location_text_candidates:addresses, official_action:cells[6]||null, official_result:cells[7]||null,
      relation:'official_html_row', geo_state:'place_not_geocoded',
      funding_state:'amount_not_award',timestamp_state:'not_aligned_to_recording' });
    if(result.length > 300)throw new Error('official Legistar table exceeds item bound');
  }
  if(!result.length)throw new Error('official Legistar table has no bounded agenda rows');
  return result;
}

export function createProjectNative({ root, home, env = process.env, run, request = fetch }) {
  if (!path.isAbsolute(root) || !path.isAbsolute(home) || typeof run !== 'function' || typeof request !== 'function') {
    throw new TypeError('root, home, run, and request are required');
  }
  const managed = path.join(home, '.bonfyre', 'bin');
  const control = env.BONFYRE_MOE_CONTROL_HOME || path.join(home, '.bonfyre', 'moe');
  const cmsDb = env.BONFYRE_MOE_CMS_DB || path.join(control, 'cms.db');
  const cmsSchemas = env.BONFYRE_MOE_CMS_SCHEMAS || path.join(control, 'content-types');
  const graphDb = env.BONFYRE_MOE_GRAPH_DB || path.join(control, 'generation-graph.db');
  const flowDb = env.BONFYRE_MOE_FLOW_DB || path.join(control, 'workgraphs.db');
  const receipts = path.join(control, 'receipts');
  const workgraphs = path.join(control, 'workgraphs');
  const moeEndpoint = env.BONFYRE_MOE_SERVER_URL || 'http://127.0.0.1:7000';
  const publicOrigin = env.BONFYRE_PUBLIC_ORIGIN || 'https://aurekai.work';
  const familyManifest = path.join(managed, '.families.json');
  const capabilityManifest = path.join(managed, '.capabilities.json');
  const managedNames = { BonfyreCMS: 'bonfyre-cms', BonfyreArtifactGraph: 'bonfyre-graph', BonfyreFlow: 'bonfyre-flow', BonfyreDisCIPL: 'bonfyre-discipl', BonfyreQwenFPQ: 'bonfyre-qwen-fpq' };
  const expandHome = (value) => String(value || '').startsWith('~/') ? path.join(home, String(value).slice(2)) : String(value || '');
  // Observation and native lookup must address the same retained evidence.
  // Keep the existing local MCP store unless the deployment names another.
  const foreignRoot = expandHome(env.BONFYRE_FOREIGN_ROOT || path.join(home, '.bonfyre', 'foreign'));
  const foreignReceipts = expandHome(env.BONFYRE_FOREIGN_RECEIPTS || path.join(foreignRoot, 'receipts'));
  const foreignBodies = expandHome(env.BONFYRE_FOREIGN_BODIES || path.join(foreignRoot, 'bodies'));
  for (const [name, directory] of Object.entries({ BONFYRE_FOREIGN_ROOT: foreignRoot, BONFYRE_FOREIGN_RECEIPTS: foreignReceipts, BONFYRE_FOREIGN_BODIES: foreignBodies })) {
    if (!path.isAbsolute(directory)) throw new TypeError(`${name} must be absolute`);
  }
  // The public source-contract catalogue must read the same native CMS owner
  // as every MCP CMS write.  Without these exact coordinates Project falls
  // back to /var/lib/bonfyre/cms and reports zero twins even after a valid
  // local source was published. Explicitly configured CMS stores always win.
  const nativeCmsDb = expandHome(env.BONFYRE_CMS_DB || cmsDb);
  const nativeCmsSchemas = expandHome(env.BONFYRE_CMS_SCHEMAS || cmsSchemas);
  for (const [name, directory] of Object.entries({ BONFYRE_CMS_DB: nativeCmsDb, BONFYRE_CMS_SCHEMAS: nativeCmsSchemas })) {
    if (!path.isAbsolute(directory)) throw new TypeError(`${name} must be absolute`);
  }
  const nativeEnv = { ...env, BONFYRE_FOREIGN_ROOT: foreignRoot,
    BONFYRE_CMS_DB: nativeCmsDb, BONFYRE_CMS_SCHEMAS: nativeCmsSchemas };
  // Preserve explicitly separated stores, expanding ~ consistently with writes.
  if (env.BONFYRE_FOREIGN_RECEIPTS) nativeEnv.BONFYRE_FOREIGN_RECEIPTS = foreignReceipts;
  if (env.BONFYRE_FOREIGN_BODIES) nativeEnv.BONFYRE_FOREIGN_BODIES = foreignBodies;

  let estatePlane = null;
  function estateControlPlane() {
    if (!estatePlane) {
      estatePlane = createEstateControlPlane({
        root,
        env,
        modelRoot: expandHome(env.BONFYRE_MODEL_ROOT || path.join(home, 'BonfyreModels')),
        ...(env.BONFYRE_MODEL_BINARY ? { modelBinary: expandHome(env.BONFYRE_MODEL_BINARY) } : {}),
        ...(env.BONFYRE_LLAMA_CPP_BINARY ? { llamaBinary: expandHome(env.BONFYRE_LLAMA_CPP_BINARY) } : {}),
        receiptJournalFile: expandHome(env.BONFYRE_ESTATE_RECEIPT_JOURNAL || path.join(home, '.bonfyre', 'estate', 'control-plane-receipts.jsonl')),
      });
    }
    return estatePlane;
  }

  async function capabilityRegistryReadiness(nativeToolCount = null) {
    const configured = expandHome(env.BONFYRE_CAPABILITY_DB || path.join(home, '.local', 'share', 'bonfyre', 'capability.db'));
    let database = configured;
    try {
      const info = await stat(configured);
      if (info.isDirectory()) database = path.join(configured, 'capability.db');
    } catch { /* handled as unavailable below */ }
    try {
      const { Database } = await import('bun:sqlite');
      const db = new Database(database, { readonly: true, create: false });
      try {
        const summary = db.query(`
          SELECT COUNT(*) AS total,
            SUM(CASE WHEN TRIM(COALESCE(stage_class,'')) <> '' THEN 1 ELSE 0 END) AS stage_classified,
            SUM(CASE WHEN TRIM(COALESCE(artifact_out,'')) NOT IN ('','-') THEN 1 ELSE 0 END) AS artifact_named,
            SUM(CASE WHEN LOWER(TRIM(COALESCE(latency_tier,''))) NOT IN ('','-','unmeasured') THEN 1 ELSE 0 END) AS latency_measured,
            SUM(CASE WHEN cost_estimate >= 0 THEN 1 ELSE 0 END) AS cost_recorded
          FROM capabilities
        `).get();
        const stages = Object.fromEntries(db.query(`
          SELECT COALESCE(NULLIF(TRIM(stage_class),''),'unclassified') AS stage, COUNT(*) AS count
          FROM capabilities GROUP BY 1 ORDER BY count DESC, stage
        `).all().map((row) => [String(row.stage), Number(row.count)]));
        const sources = Object.fromEntries(db.query(`
          SELECT COALESCE(NULLIF(TRIM(source),''),'unknown') AS source, COUNT(*) AS count
          FROM capabilities GROUP BY 1 ORDER BY count DESC, source
        `).all().map((row) => [String(row.source), Number(row.count)]));
        const total = Number(summary?.total || 0);
        const stageClassified = Number(summary?.stage_classified || 0);
        const artifactNamed = Number(summary?.artifact_named || 0);
        const latencyMeasured = Number(summary?.latency_measured || 0);
        const costRecorded = Number(summary?.cost_recorded || 0);
        return {
          schema_version: 'bonfyre.capability.metadata-readiness.v1',
          state: 'available',
          registry_records: total,
          native_tool_count: Number.isInteger(nativeToolCount) ? nativeToolCount : null,
          count_difference: Number.isInteger(nativeToolCount) ? nativeToolCount - total : null,
          metadata: {
            stage_classified: stageClassified,
            stage_unclassified: total - stageClassified,
            artifact_named: artifactNamed,
            artifact_missing: total - artifactNamed,
            latency_measured: latencyMeasured,
            latency_unmeasured: total - latencyMeasured,
            cost_recorded: costRecorded,
            cost_unmeasured: total - costRecorded,
          },
          stages,
          sources,
          cannot_infer: 'Registry metadata completeness is not executable health, benchmark validity, provider admission, authority, or economic truth. Native-tool and capability-registry counts are different identity spaces; count_difference is not coverage.',
        };
      } finally { db.close(); }
    } catch (error) {
      return {
        schema_version: 'bonfyre.capability.metadata-readiness.v1',
        state: 'unavailable',
        registry_records: null,
        native_tool_count: Number.isInteger(nativeToolCount) ? nativeToolCount : null,
        reason: String(error?.message || 'capability registry unavailable').slice(0, 300),
        cannot_infer: 'An unavailable capability registry is not evidence that the corresponding native tools are unavailable.',
      };
    }
  }

  async function estateCapabilities(args = {}) {
    const refresh = args.refresh === true;
    const plane = estateControlPlane();
    const [nativeTools, models, adapters, fleet] = await Promise.all([
      plane.fabric.inventory({ refresh }),
      plane.fabric.modelCatalog({ refresh }),
      plane.adapterRuntime.discover({ refresh }),
      plane.fleet.inspect(),
    ]);
    const capabilityRegistry = await capabilityRegistryReadiness(nativeTools.length);
    return {
      schema_version: 'bonfyre.estate.capabilities.v1',
      refreshed: refresh,
      native_tools: nativeTools,
      models,
      generation_profiles: plane.fabric.profileInventory(),
      adapters,
      fleet,
      capability_registry: capabilityRegistry,
      forbidden_inference: 'Discovery, registry metadata and local health state do not grant provider credentials, account authority, spend approval, publication rights, benchmark truth, or permission for a foreign effect.',
    };
  }

  async function estateAdapterProbe(args = {}) {
    const ids = args.ids === undefined ? undefined : (() => {
      if (!Array.isArray(args.ids) || args.ids.length > 20) throw new Error('ids must contain at most 20 adapter identifiers');
      return args.ids.map((value) => ident(value, 'adapter id'));
    })();
    const timeoutMs = clamp(args.timeout_seconds, 5, 1, 30) * 1000;
    const maxConcurrency = clamp(args.max_concurrency, 4, 1, 8);
    const results = await estateControlPlane().adapterRuntime.probe({ ids, timeoutMs, maxConcurrency });
    return {
      schema_version: 'bonfyre.estate.adapter-probe.v1',
      count: results.length,
      passed: results.filter((item) => item.status === 'passed').length,
      failed: results.filter((item) => item.status !== 'passed').length,
      results,
      forbidden_inference: 'A bounded local adapter probe is health evidence only; it does not admit a provider, authenticate an account, authorize spend, or perform a foreign mutation.',
    };
  }

  async function jsonFile(file, fallback = {}) {
    try { return JSON.parse(await readFile(file, 'utf8')); } catch { return fallback; }
  }
  async function manifestFamily(name, records) {
    const manifest = records ?? await jsonFile(familyManifest);
    return manifest[name] || (name === 'bonfyre-moe' ? 'BonfyreMoE' : name.split('-').map((part) => part[0]?.toUpperCase() + part.slice(1)).join(''));
  }
  // Native checkout candidates use the same strict format check as managed binaries.
  async function checkoutCandidate(family, preferredName = null) {
    const dir = path.join(root, 'cmd', family);
    const entries = await readdir(dir).catch(() => []);
    const ordered = preferredName ? (entries.includes(preferredName) ? [preferredName] : []) : entries.sort();
    for (const name of ordered) {
      if (name.endsWith('.sh') || !canonicalNativeName(name) || ['Makefile', 'makefile'].includes(name)) continue;
      const file = path.join(dir, name);
      const format = await nativeFormat(file);
      if (format) return { file, source: 'checkout', format };
    }
    return null;
  }

  async function candidates(family, snapshot) {
    const manifest = snapshot?.families ?? await jsonFile(familyManifest);
    const names = [...Object.keys(manifest).filter((name) => manifest[name] === family), managedNames[family]].filter(Boolean);
    const installed = snapshot?.entries ?? await readdir(managed).catch(() => []);
    for (const name of new Set([...names, ...installed])) {
      if (!canonicalNativeName(name)) continue;
      if (await manifestFamily(name, manifest) !== family) continue;
      const file = path.join(managed, name);
      const format = await nativeFormat(file);
      if (format) return { file, source: 'managed', format };
    }
    return checkoutCandidate(family);
  }
  // Read back both executable artifacts without running or promoting either.
  // A matching byte digest is not source-build freshness or a loaded-process attestation.
  async function nativeBinaryComparison(candidate, family, { exact = true } = {}) {
    const checkout = await checkoutCandidate(family, path.basename(candidate.file));
    const [binaryInfo, checkoutInfo] = await Promise.all([
      stat(candidate.file).catch(() => null), checkout ? stat(checkout.file).catch(() => null) : Promise.resolve(null)
    ]);
    // Native inventory is a frequent routing operation. Hash a selected binary
    // when it is invoked; cataloguing every binary should not rehash the estate.
    const [binaryDigest, checkoutDigest] = exact ? await Promise.all([
      digestFile(candidate.file), checkout ? digestFile(checkout.file) : Promise.resolve(null)
    ]) : [null, null];
    const comparison = candidate.source === 'checkout' ? 'checkout_only'
      : !checkout ? 'checkout_missing'
      : !exact ? binaryInfo && checkoutInfo && binaryInfo.size !== checkoutInfo.size ? 'different_size' : 'not_compared'
      : !binaryDigest || !checkoutDigest ? 'digest_unavailable'
      : binaryDigest === checkoutDigest ? 'matched' : 'diverged';
    return {
      binary_sha256_at_readback: binaryDigest,
      binary_bytes: binaryInfo?.size ?? null,
      checkout_binary: checkout?.file ?? null,
      checkout_bytes: checkoutInfo?.size ?? null,
      checkout_sha256_at_readback: checkoutDigest,
      checkout_comparison: comparison,
      source_build_freshness: 'not_attested'
    };
  }

  async function managedOwner(family) {
    const candidate = await candidates(family);
    if (!candidate || candidate.source !== 'managed') throw new Error(`managed native owner unavailable: ${family}`);
    return candidate.file;
  }
  async function execute(file, argv, { timeoutMs = 30000, cwd = path.dirname(file), extraEnv } = {}) {
    const result = await run([file, ...argv], { cwd, timeoutMs, env: extraEnv ? { ...nativeEnv, ...extraEnv } : nativeEnv });
    return { ...result, cwd: result.cwd || cwd };
  }
  async function owner(family, argv, { timeoutMs = 120000, extraEnv } = {}) {
    const file = await managedOwner(family);
    const result = await execute(file, argv, { timeoutMs, extraEnv });
    return { ...result, schema_version: 'bonfyre.native.owner.receipt.v1', family, binary: file,
      status: result.exit_code === 0 && !result.timeout && !result.overflow ? 'passed' : result.timeout ? 'timed_out' : 'failed',
      stdout_sha256: sha(result.stdout || ''), stderr_sha256: sha(result.stderr || '') };
  }
  async function controlCommand(name, argv, { timeoutMs = 30000, extraEnv } = {}) {
    const file = path.join(managed, name);
    if (!await nativeFormat(file)) throw new Error(`managed control-plane binary unavailable: ${file}`);
    return execute(file, argv, { cwd: control, timeoutMs, extraEnv });
  }
  async function capabilities(family, snapshot) {
    const records = snapshot ?? await jsonFile(capabilityManifest);
    const entry = records instanceof Map ? records.get(family) || {} : Object.values(records).find((record) => record?.family === family) || {};
    const lower = family.toLowerCase();
    const hints = { 'qwen-fpq': ['generation', 'local-model', 'code', 'compression'], fpqx: ['generation', 'compression', 'local-model'], fpq: ['generation', 'compression', 'local-model'], moe: ['generation', 'mixture-of-experts', 'local-model'], embed: ['embedding', 'multimodal'], vec: ['vector-search'], transcribe: ['transcription', 'audio'], narrate: ['speech', 'audio'], gen: ['generation'], model: ['model-control'] };
    return { entry, list: [...new Set([...Object.entries(hints).flatMap(([key, values]) => lower.includes(key) ? values : []), ...(entry.subcommands || []).map((verb) => `cmd:${verb}`)])].sort() };
  }
  async function mapConcurrent(values, concurrency, worker) {
    const results = Array.from({ length: values.length });
    let cursor = 0;
    async function runWorker() {
      while (cursor < values.length) {
        const index = cursor++;
        results[index] = await worker(values[index], index);
      }
    }
    const workers = Math.min(Math.max(1, concurrency), Math.max(1, values.length));
    await Promise.all(Array.from({ length: workers }, runWorker));
    return results;
  }
  async function inventory(args = {}) {
    const started = Date.now();
    if (!object(args) || Object.keys(args).some(key => !['query', 'detail', 'offset', 'limit', 'surface', 'all'].includes(key)))
      throw new Error('invalid inventory arguments');
    if (args.query !== undefined && (typeof args.query !== 'string' || args.query.length > 256 || args.query.includes('\0')))
      throw new Error('query must be at most 256 characters');
    if (args.surface !== undefined && !['owners', 'commands'].includes(args.surface))
      throw new Error('inventory surface must be owners or commands');
    if (args.all !== undefined && typeof args.all !== 'boolean') throw new Error('inventory all must be boolean');
    if (args.surface === 'commands') {
      if (args.detail !== undefined || args.offset !== undefined || args.limit !== undefined)
        throw new Error('command catalog uses query and all, not owner inventory pagination');
      const configured = env.BONFYRE_CLI_BINARY;
      if (configured && !path.isAbsolute(configured)) throw new Error('CLI catalog owner must be an absolute binary path');
      const managedCli = path.join(managed, 'bonfyre');
      const checkoutCli = path.join(root, 'cmd', 'BonfyreCLI', 'bonfyre');
      const checkoutReady = Boolean(await nativeFormat(checkoutCli));
      const managedReady = Boolean(await nativeFormat(managedCli));
      /* In the source checkout, discovery reads the just-built CLI that the
       * API contract tests. Managed availability is still reported separately. */
      const cli = configured || (checkoutReady ? checkoutCli : managedCli);
      if (!await nativeFormat(cli)) throw new Error('native BonfyreCLI catalog owner unavailable');
      const checkoutSha = checkoutReady ? await digestFile(checkoutCli) : null;
      const managedSha = managedReady ? await digestFile(managedCli) : null;
      const drift = checkoutSha && managedSha ? (checkoutSha === managedSha ? 'matched' : 'diverged')
        : checkoutSha ? 'checkout_only' : 'managed_only';
      const query = (args.query ?? '').trim();
      const argv = ['list', '--json', ...(args.all ? ['--all'] : []), ...(query ? [query] : [])];
      const catalog = parseNativeJson(await execute(cli, argv, { cwd: root, timeoutMs: 15000 }), 'BonfyreCLI catalog');
      if (!object(catalog?.summary) || !Array.isArray(catalog?.sections))
        throw new Error('BonfyreCLI returned an invalid command catalog');
      return { schema_version: 'bonfyre.native.command.catalog.v1',
        surface: 'commands', scope: args.all ? 'all' : 'public', owner: 'BonfyreCLI',
        binary: cli, binary_sha256: await digestFile(cli),
        selected_source: configured ? 'explicit' : checkoutReady ? 'checkout' : 'managed',
        managed_checkout_drift: drift, managed_binary_sha256: managedSha, checkout_binary_sha256: checkoutSha,
        source_build_freshness: 'not_attested', ...catalog,
        cannot_infer: 'A command visible through MCP is not an authenticated or authorized execution right; an unpromoted checkout is not a deployed catalog.' };
    }
    if (args.all !== undefined) throw new Error('all is only valid for the command catalog surface');
    const detail = args.detail ?? 'full';
    if (!['full', 'summary'].includes(detail)) throw new Error('detail must be full or summary');
    for (const [key, maximum] of [['offset', 1000000], ['limit', 200]])
      if (args[key] !== undefined && (!Number.isSafeInteger(args[key]) || args[key] < (key === 'limit' ? 1 : 0) || args[key] > maximum))
        throw new Error(`invalid inventory ${key}`);
    const query = (args.query ?? '').trim().toLowerCase(), offset = args.offset ?? 0;
    const bounded = Object.keys(args).length > 0;
    const limit = args.limit ?? (bounded ? 20 : Number.MAX_SAFE_INTEGER);
    // One request-local metadata snapshot, refreshed on the next request.
    const [families, capabilityRecords] = await Promise.all([jsonFile(familyManifest), jsonFile(capabilityManifest)]);
    const capabilityByFamily = new Map(Object.values(capabilityRecords).filter((record) => record?.family).map((record) => [record.family, record]));
    const tools = new Map();
    const degraded = [];
    const refused = [];
    let entries = [];
    try { entries = await readdir(managed); } catch (error) { degraded.push({ source: managed, error: error.code || 'unavailable' }); }
    const managedStarted = Date.now();
    const managedRows = await mapConcurrent(entries.sort(), 16, async (name) => {
      if (name.startsWith('.') || !canonicalNativeName(name)) return null;
      const file = path.join(managed, name);
      const format = await nativeFormat(file);
      if (!format) {
        const info = await lstat(file).catch(() => null);
        return info?.mode & 0o111 ? { refused: { binary: file, reason: 'not_native_executable' } } : null;
      }
      const family = await manifestFamily(name, families);
      const { entry, list } = await capabilities(family, capabilityByFamily);
      return { tool: { id: family, family, binary: file, capabilities: list, available: true, source: 'managed', format, shape: entry.shape || 'unprobed', synopsis: entry.synopsis || '', estate: entry.estate || 'unclassified' } };
    });
    const managedClassificationMs = Date.now() - managedStarted;
    for (const row of managedRows) {
      if (row?.refused) refused.push(row.refused);
      if (row?.tool) tools.set(row.tool.family, row.tool);
    }
    const cmdRoot = path.join(root, 'cmd');
    let dirs = [];
    try { dirs = await readdir(cmdRoot); } catch (error) { degraded.push({ source: cmdRoot, error: error.code || 'unavailable' }); }
    const checkoutFamilies = dirs.filter((name) => /^Bonfyre[A-Za-z0-9_-]+$/.test(name)).sort().filter((family) => !tools.has(family));
    const checkoutStarted = Date.now();
    const checkoutRows = await mapConcurrent(checkoutFamilies, 16, async (family) => {
      const candidate = await candidates(family, { families, entries });
      if (!candidate) return null;
      const { list } = await capabilities(family, capabilityByFamily);
      return { id: family, family, binary: candidate.file, capabilities: list, available: true, source: candidate.source, format: candidate.format };
    });
    const checkoutClassificationMs = Date.now() - checkoutStarted;
    for (const tool of checkoutRows) {
      if (tool) tools.set(tool.family, tool);
    }
    const matches = [...tools.values()].filter(tool => !query || [tool.family, tool.synopsis, tool.estate, ...tool.capabilities]
      .some(value => String(value ?? '').toLowerCase().includes(query))).sort((a, b) => a.family.localeCompare(b.family));
    const selected = matches.slice(offset, offset + limit);
    for (const tool of selected) {
      if (detail === 'full') Object.assign(tool, await nativeBinaryComparison({ file: tool.binary, source: tool.source }, tool.family, { exact: false }));
    }
    return { schema_version: 'bonfyre.native.tool.inventory.v1', root, count: tools.size,
      ...(bounded ? { query, detail, matched: matches.length, offset, returned: selected.length,
        next_offset: offset + selected.length < matches.length ? offset + selected.length : null } : {}),
      tools: detail === 'full' ? selected : selected.map(({ family, capabilities, source, synopsis, shape }) => ({ family, capabilities, source,
        ...(synopsis ? { synopsis } : {}), ...(shape ? { shape } : {}) })),
      performance: { duration_ms: Date.now() - started, classification_concurrency: 16,
        managed_candidates: entries.length, managed_classification_ms: managedClassificationMs,
        checkout_candidates: checkoutFamilies.length, checkout_classification_ms: checkoutClassificationMs },
      ...(degraded.length ? { degraded } : {}), ...(refused.length ? detail === 'full' ? { refused } : { refused_count: refused.length } : {}),
      ...(detail === 'summary' ? { cannot_infer: 'Discovery hints do not attest source-build freshness, runtime readiness or effect authority.' } : {}) };
  }
  // ExperienceMap already declares its corpus-root environment contract.
  // An installed binary may not carry the generated read-only corpus beside it.
  async function nativeResourceEnvironment(family) {
    if (family !== 'BonfyreExperienceMap' || nativeEnv.BONFYRE_EXPERIENCE_MAP_ROOT)
      return { extraEnv: undefined, corpus: null };
    const corpus = path.join(root, 'cmd', family, 'generated');
    if (!await fileExists(path.join(corpus, 'manifest.json')) ||
        !await fileExists(path.join(corpus, 'all_bindings.tsv')))
      return { extraEnv: undefined, corpus: null };
    return { extraEnv: { BONFYRE_EXPERIENCE_MAP_ROOT: corpus },
      corpus: { path: corpus, source: 'checkout-generated-corpus',
        release_match: 'not_attested', scope: 'read-only reference lookup' } };
  }

  async function invoke(args) {
    const family = String(args.family || '');
    if (!/^Bonfyre[A-Za-z0-9_-]+$/.test(family)) throw new Error('family must be a Bonfyre command family');
    if (!Array.isArray(args.args ?? []) || !(args.args ?? []).every((item) => typeof item === 'string')) throw new Error('args must be a list of strings');
    const candidate = await candidates(family);
    if (!candidate) throw new Error(`no native executable found for ${family}; interpreter launchers are refused`);
    if (!nativeReadIntent(family, args.args || [], home))
      throw new Error('native effect requires a destination-owned ActorClosure, PackGraph, and reviewed execution owner; generic argv is read-only');
    const resources = await nativeResourceEnvironment(family);
    const result = await execute(candidate.file, args.args || [], { cwd: candidate.source === 'managed' ? path.dirname(candidate.file) : home, timeoutMs: clamp(args.timeout_seconds, 60, 5, 600) * 1000, extraEnv: resources.extraEnv });
    const { list } = await capabilities(family);
    const binaryComparison = await nativeBinaryComparison(candidate, family);
    return { ...result, family, binary: candidate.file, source_binary: candidate.source === 'checkout' ? candidate.file : null,
      ...binaryComparison,
      corpus_binding: resources.corpus,
      managed_binary: candidate.source === 'managed', schema_version: 'bonfyre.native.tool.receipt.v1', receipt_id: randomUUID(),
      status: result.exit_code === 0 && !result.timeout && !result.overflow ? 'passed' : result.timeout ? 'timed_out' : 'failed', capabilities: list,
      stdout_sha256: sha(result.stdout || ''), stderr_sha256: sha(result.stderr || ''), stdout_bytes: Buffer.byteLength(result.stdout || ''), stderr_bytes: Buffer.byteLength(result.stderr || '') };
  }

  async function financial(args, internalReady = false) {
    const action = String(args.action || 'status');
    const recipe = action === 'status' ? null : ident(args.recipe, 'recipe');
    const economy = (argv, extraEnv) => owner('BonfyreEconomy', argv, { extraEnv });
    if (action === 'status') return { schema_version: 'bonfyre.financial-membrane.mcp.v1', basis: 'operating-book control; not invoices, settlement, or booked cash', status: await economy(['status']), recent_observed_costs: await economy(['report', '--last', '50']), service_book: await owner('BonfyreFinance', ['service', 'list']) };
    if (action === 'set_cap') return { schema_version: 'bonfyre.financial-membrane.mcp.v1', basis: 'budget cap, not committed capital or a payment', mutation: await economy(['budget', 'set', recipe, formatUsd(args.usd_cap, 'usd_cap')]), after: await economy(['budget', 'show', recipe]) };
    if (action === 'route') return { schema_version: 'bonfyre.financial-membrane.mcp.v1', basis: 'route recommendation, not a provider booking or spend authority', budget: await economy(['budget', 'show', recipe]), route: await economy(['route', recipe], internalReady ? { BONFYRE_ECONOMY_LOCAL_MODEL_READY: '1' } : undefined), estimate: await economy(['cost', 'estimate', recipe]), service_book: await owner('BonfyreFinance', ['service', 'list']) };
    if (action === 'record_observed_cost') return { schema_version: 'bonfyre.financial-membrane.mcp.v1', basis: 'observed operating cost; this does not manufacture an invoice, payment, or settlement', mutation: await economy(['cost', 'record', recipe, ident(args.stage, 'stage'), ident(args.model, 'model'), formatUsd(args.usd, 'usd')]), after: await economy(['budget', 'show', recipe]) };
    throw new Error('action must be status, set_cap, route, or record_observed_cost');
  }

  async function financialRead(args) {
    const action = String(args.action || 'status');
    if (!['status', 'route'].includes(action)) throw new Error('financial_read action must be status or route');
    return financial({ action, ...(args.recipe ? { recipe: args.recipe } : {}) });
  }

  async function cms(args) {
    const action = String(args.action || '').trim();
    if (!['list_types', 'list', 'get', 'create', 'update'].includes(action)) throw new Error('invalid CMS action');
    const common = ['--db', cmsDb, '--schemas', cmsSchemas];
    if (action === 'list_types') {
      const native = await controlCommand('bonfyre-cms', ['schema', 'list', ...common]);
      assertSuccess(native, 'CMS schema list');
      return { schema_version: 'bonfyre.cms.record.receipt.v1', action, content_types: native.stdout.split(/\r?\n/).slice(2).filter(Boolean).map((line) => line.trim().split(/\s+/)[0]), native, forbidden_inference: 'A listed content type is not authority to read or act in every namespace.' };
    }
    const contentType = ident(args.content_type, 'content_type');
    const namespace = ident(args.namespace ?? 'mcp', 'namespace');
    let entryId = args.entry_id;
    if (['get', 'update'].includes(action)) {
      entryId = Number(entryId);
      if (!Number.isSafeInteger(entryId) || entryId <= 0) throw new Error('entry_id must be a positive native CMS entry id');
    }
    let argv;
    if (action === 'list') {
      const limit = Number(args.limit ?? 25);
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error('limit must be between 1 and 100');
      argv = ['entry', 'list', contentType, '--ns', namespace, '--limit', String(limit), ...common];
    } else if (action === 'get') argv = ['entry', 'get', contentType, String(entryId), '--ns', namespace, ...common];
    else {
      if (!object(args.attributes)) throw new Error('attributes must be an object');
      const body = JSON.stringify(args.attributes);
      if (Buffer.byteLength(body) > 256000) throw new Error('attributes must serialize to at most 256,000 bytes');
      argv = action === 'create' ? ['entry', 'create', contentType, '--ns', namespace, body, ...common] : ['entry', 'update', contentType, String(entryId), body, '--ns', namespace, ...common];
    }
    const native = await controlCommand('bonfyre-cms', argv);
    const result = parseNativeJson(native, 'BonfyreCMS');
    return { schema_version: 'bonfyre.cms.record.receipt.v1', action, content_type: contentType, namespace, entry_id: entryId, result, native,
      ...(action === 'create' || action === 'update' ? { authority_boundary: 'This is a native CMS namespace mutation under the authenticated MCP session. It does not publish, invoke Frappe, resolve a foreign reference, book money, or grant external effect authority.' } : { forbidden_inference: action === 'list' ? 'A CMS listing is not publication, a Frappe-site query, or proof of external state.' : 'Reading a CMS record is not a change in publication, authority, or external state.' }) };
  }

  async function cmsRead(args) {
    const action = String(args.action || '').trim();
    if (!['list_types', 'list', 'get'].includes(action)) throw new Error('cms_read action must be list_types, list, or get');
    return cms(args);
  }

  async function frappe(args) {
    const lineages = ['frappe', 'crm', 'helpdesk', 'erpnext', 'hrms', 'lms', 'wiki', 'insights', 'drive', 'bonfyre_erpnext'];
    const lineage = String(args.lineage || '').trim().toLowerCase();
    if (!lineages.includes(lineage)) throw new Error('invalid Frappe lineage');
    const appRoot = await realpath(path.join(root, 'integrations', 'frappe-bench', 'apps'));
    const appPath = await realpath(path.join(appRoot, lineage));
    if (!appPath.startsWith(`${appRoot}${path.sep}`) || !(await stat(appPath)).isDirectory()) throw new Error('installed Frappe lineage unavailable');
    const phase = Number(args.phase ?? 12);
    if (!Number.isSafeInteger(phase) || phase < 0 || phase > 12) throw new Error('phase must be 0 through 12');
    const doctype = String(args.cms_doctype || '').trim();
    if (doctype && (doctype.length > 160 || /[/\\]/.test(doctype) || phase < 1)) throw new Error('invalid cms_doctype or phase');
    const output = path.join(home, '.bonfyre', 'frappe-packs', lineage);
    await mkdir(output, { recursive: true });
    const artifactNames = [['AppPack', 'apppack.json'], ['SchemaIR', 'schema-ir.json'], ['BindingDerivations', 'bindings.json'], ['RuleUniverses', 'rule-universes.json']];
    const artifacts = artifactNames.map(([kind, name]) => ({ kind, path: path.join(output, name) }));
    const argv = ['--output', output, '--phase', String(phase), '--emit-pack', artifacts[0].path, '--emit-schema-ir', artifacts[1].path, '--emit-bindings', artifacts[2].path, '--emit-rule-universes', artifacts[3].path];
    let cmsSchema;
    if (doctype) {
      const slug = doctype.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'doctype';
      await mkdir(path.join(output, 'cms'), { recursive: true });
      cmsSchema = path.join(output, 'cms', `${slug}.json`);
      argv.push('--emit-cms-schema', cmsSchema, '--cms-doctype', doctype);
    }
    argv.push(appPath);
    const compile = await owner('BonfyreFrappeCompiler', argv, { timeoutMs: clamp(args.timeout_seconds, 900, 120, 1800) * 1000 });
    for (const artifact of artifacts) { artifact.exists = await fileExists(artifact.path); artifact.sha256 = await digestFile(artifact.path); }
    let cmsProjection = null;
    if (cmsSchema) {
      if (compile.exit_code !== 0 || !await fileExists(cmsSchema)) throw new Error('Frappe compiler did not produce the requested native CMS schema');
      const applied = assertSuccess(await controlCommand('bonfyre-cms', ['schema', 'apply', cmsSchema, '--db', cmsDb, '--schemas', cmsSchemas], { timeoutMs: 90000 }), 'native CMS schema application');
      artifacts.push({ kind: 'NativeCMSSchema', path: cmsSchema, exists: true, sha256: await digestFile(cmsSchema) });
      cmsProjection = { doctype, schema_path: cmsSchema, schema_apply: applied, state: 'registered_in_native_cms', authority_boundary: 'This registers declarative record grammar only. Per-actor namespace authorization, Frappe controller execution, resolved relations, and Frappe-site write-back remain absent.' };
    }
    return { schema_version: 'bonfyre.frappe.compile.receipt.v1', lineage, phase, compile, artifacts, cms_projection: cmsProjection, forbidden_inference: 'compiled metadata is not controller execution, a running Frappe site, or authority to invoke a Frappe effect' };
  }

  async function foreign(args) {
    const twinId = ident(args.twin_id, 'twin_id');
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(twinId)) throw new Error('twin_id must be a filename-safe identifier');
    const raw = String(args.url || '').trim();
    const url = new URL(raw);
    if (!['http:', 'https:'].includes(url.protocol) || !url.hostname || url.username || url.password) throw new Error('url must be a public http(s) URL without embedded credentials');
    const host = url.hostname.toLowerCase();
    if (['localhost', 'localhost.localdomain', '[::1]'].includes(host) || host.endsWith('.local') || host.startsWith('127.')) throw new Error('local or loopback URLs are not ForeignTwin sources');
    const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
    const stem = `${twinId}-${stamp}-${randomUUID().replaceAll('-', '').slice(0, 10)}`;
    const receiptDir = foreignReceipts;
    const bodyDir = foreignBodies;
    await mkdir(receiptDir, { recursive: true }); await mkdir(bodyDir, { recursive: true });
    const receiptPath = path.join(receiptDir, `${stem}.json`);
    const bodyPath = path.join(bodyDir, `${stem}.body`);
    const native = await owner('BonfyreProject', ['foreign', 'observe', twinId, raw, '--receipt', receiptPath, '--body', bodyPath, '--observe-right', 'public'], { timeoutMs: clamp(args.timeout_seconds, 120, 15, 600) * 1000 });
    const receipt = await jsonFile(receiptPath, null);
    return { schema_version: 'bonfyre.foreign-twin.observe.receipt.v1', twin_id: twinId, source: raw, native_observation: native, receipt_path: receiptPath, receipt_sha256: await digestFile(receiptPath), retained_body_path: await fileExists(bodyPath) ? bodyPath : null, retained_body_sha256: await digestFile(bodyPath), receipt: object(receipt) ? receipt : null, rights: { admitted: 'observe', not_granted: ['bind', 'act', 'claim', 'canonicalize'] }, forbidden_inference: 'a public observation or retained body is not remote truth, availability, an external account, or authority to act against the source' };
  }

  // The public WebAddress owns the release catalogue. The local Project binary
  // may have a different store; never treat its empty census as live absence.
  // This is an anonymous, bounded, read-only public projection.
  async function foreignRelease(args) {
    const origin = new URL(publicOrigin);
    if (origin.protocol !== 'https:' || origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash) {
      throw new Error('public ForeignTwin origin must be an HTTPS origin');
    }
    const query = String(args.query ?? '').trim();
    if (query.length > 160 || /[\u0000-\u001f\u007f]/.test(query)) throw new Error('query exceeds public source filter bound'); // oxlint-disable-line no-control-regex -- refuses control characters
    const offset = Number(args.offset ?? 0);
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100000) throw new Error('offset must be 0..100000');
    const expected = String(args.expected_generation ?? '');
    if (expected && !/^[a-zA-Z0-9._-]{1,96}$/.test(expected)) throw new Error('invalid catalogue generation');
    const caseUrls = args.case_urls ?? [];
    const related = args.related_queries ?? [];
    const grantTerms = args.grant_terms ?? [];
    const civicEvent = args.civic_event_id === undefined ? null : Number(args.civic_event_id);
    if (civicEvent !== null && (!Number.isSafeInteger(civicEvent) || civicEvent < 1 || civicEvent > 10000000))
      throw new Error('civic_event_id must be a positive Legistar event identifier');
    if (!Array.isArray(grantTerms) || grantTerms.length > 4 || grantTerms.some(term =>
        typeof term !== 'string' || !term.trim() || term.length > 100 || /[\u0000-\u001f\u007f]/.test(term))) // oxlint-disable-line no-control-regex -- refuses control characters
      throw new Error('grant terms must be at most 4 printable strings of 1..100 characters');
    if (!Array.isArray(caseUrls) || caseUrls.length > 12 ||
        !Array.isArray(related) || related.length > 4) throw new Error('case URLs (max 12) and related queries (max 4) must be arrays');
    const parseUrl = raw => {
      if (typeof raw !== 'string' || raw.length > 2048) throw new Error('invalid source URL');
      const url = new URL(raw);
      if (url.protocol !== 'https:' || !url.hostname || url.username || url.password || url.port || url.hash)
        throw new Error('case source must be HTTPS without embedded credentials, custom port or fragment');
      return url.href;
    };
    const urls = [...new Set(caseUrls.map(parseUrl))];
    // Avoid unbounded catalogue scans; a case starts from its first source host.
    // An unfiltered anonymous request may be refused by the public HTTP owner.
    if (!query && !urls.length) throw new Error('query or case_urls required for a bounded public source release');
    const effectiveQuery = query || new URL(urls[0]).hostname;
    for (const term of related) {
      if (typeof term !== 'string' || !term.trim() || term.length > 80 || /[\u0000-\u001f\u007f]/.test(term)) // oxlint-disable-line no-control-regex -- refuses control characters
        throw new Error('related query must be 1..80 printable characters');
    }
    const readbacks = [];
    let generation = expected;
    const readPage = async (filter, page) => {
      const pathPart = page ? '/foreign/twins/page/' + page : '/foreign/twins';
      const target = new URL(pathPart, origin);
      if (filter) target.searchParams.set('q', filter);
      if (page && generation) target.searchParams.set('snapshot', generation);
      const response = await request(target.href, { method: 'GET', headers: { Accept: 'application/json' },
        signal: AbortSignal.timeout(20000) });
      if (!response.ok) {
        const diagnostic = await boundedResponse(response, 4096).catch(() => Buffer.alloc(0));
        throw new Error('public ForeignTwin owner unavailable: HTTP ' + response.status +
          ' on ' + target.pathname + (target.search ? '?query' : '') +
          ' (' + diagnostic.toString('utf8').slice(0, 280) + ')');
      }
      const bytes = await boundedResponse(response, 4 * 1024 * 1024);
      let release;
      try { release = JSON.parse(bytes.toString('utf8')); } catch { throw new Error('public ForeignTwin owner returned invalid JSON'); }
      if (release?.state === 'snapshot_mismatch') throw new Error('ForeignTwin release changed during the case; retry against the new generation');
      if (release?.schema !== 'bonfyre.foreign.twins.v2' || release.state !== 'current' ||
          !Array.isArray(release.twins) || !/^[a-zA-Z0-9._-]{1,96}$/.test(release.catalogue_generation || ''))
        throw new Error('public ForeignTwin owner returned an unexpected release contract');
      if (generation && generation !== release.catalogue_generation) throw new Error('ForeignTwin release generation changed during readback');
      generation = release.catalogue_generation;
      if (release.twins.length > 200 || release.offset !== page)
        throw new Error('public ForeignTwin page violates bounded pagination');
      readbacks.push({ url: target.href, sha256: sha(bytes), bytes: bytes.length, offset: page, query: filter });
      return release;
    };
    const first = await readPage(effectiveQuery, offset);
    const caseMembers = [];
    const missing = [];
    const members = new Map();
    const byHost = new Map();
    for (const url of urls) {
      const host = new URL(url).hostname;
      const group = byHost.get(host) ?? [];
      group.push(url); byHost.set(host, group);
    }
    for (const [host, wanted] of byHost) {
      let page = 0;
      const unmatched = new Set(wanted);
      for (let iteration = 0; iteration < 12 && unmatched.size; iteration++) {
        const batch = effectiveQuery === host && page === offset ? first : await readPage(host, page);
        for (const row of batch.twins) {
          if (!row || typeof row.url !== 'string' || !row.release?.source_contract?.source || !row.subject_id) continue;
          if (!unmatched.has(row.url)) continue;
          unmatched.delete(row.url);
          if (!members.has(row.release.source_contract.source))
            members.set(row.release.source_contract.source, {
              source: row.release.source_contract.source, subject_id: row.subject_id, url: row.url,
              source_contract: row.release.source_contract, stages: row.release.stages,
              readiness: row.release.readiness, facets: row.release.facets,
            });
        }
        if (batch.next_offset === null || batch.next_offset === undefined) break;
        const next = Number(batch.next_offset);
        if (!Number.isSafeInteger(next) || next <= page) throw new Error('invalid foreign release pagination');
        page = next;
      }
      for (const url of unmatched) missing.push({ url, state: 'not_in_verified_release',
        blocked_by: 'source_not_admitted_or_census_incomplete' });
    }
    caseMembers.push(...members.values());
    const context = [];
    for (const term of related) {
      const hit = await readPage(term.trim(), 0);
      context.push({ query: term.trim(), total: hit.total, matching_released_sources: hit.twins.slice(0, 25).map(row => ({
        source: row.release?.source_contract?.source ?? null, url: row.url,
        family: row.release?.source_contract?.family ?? null, readiness: row.release?.readiness ?? null
      })), relation: 'candidate_only', truncated: Number(hit.total) > hit.twins.length });
    }
    // Reuse FundingProspectFrontier's Grants.gov reader and response digest.
    // A keyword hit is neither an award nor evidence of meeting/project fit.
    const funding = [];
    if (grantTerms.length) {
      const { observeFundingCandidate } = await import('../../scripts/bootstrap_funding_frontier.mjs');
      for (const term of new Set(grantTerms)) {
        const observed = await observeFundingCandidate(term, { rows: 5, timeout: 20,
          endpoint: 'https://api.grants.gov/v1/api/search2' }, { request });
        funding.push({ ...observed, related_to_case: 'unverified',
          eligibility: 'not_determined', application: 'not_submitted' });
      }
    }
    // Official agenda material and video media remain separate evidence carriers.
    let civic = null;
    if (civicEvent !== null) {
      const endpoint = new URL(`/v1/dallas/events/${civicEvent}/eventitems`, 'https://webapi.legistar.com');
      try {
        const reply = await request(endpoint.href, { method: 'GET', headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(20000) });
        if (!reply.ok) throw new Error(`Legistar HTTP ${reply.status}`);
        const bytes = await boundedResponse(reply, 512 * 1024);
        const items = JSON.parse(bytes.toString('utf8'));
        if (!Array.isArray(items) || items.length > 300) throw new Error('Legistar event items exceed bounded contract');
        const rows = items.map(item => {
          if (!object(item) || !Number.isSafeInteger(Number(item.EventItemId))) throw new Error('invalid Legistar event item');
          const title = String(item.EventItemTitle || '').slice(0, 3000);
          // A monetary amount before an address must not be included in the
          // street candidate; candidates remain unlocated text, not geometry.
          const addresses = [...title.matchAll(/\b\d{1,6}\s+(?:(?:[A-Za-z][A-Za-z.'-]*|[NSEW])\s+){1,5}(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Parkway|Pkwy)\b/gi)]
            .slice(0, 8).map(hit => hit[0]);
          return { item_id: Number(item.EventItemId), agenda_number: String(item.EventItemAgendaNumber || ''),
            file: String(item.EventItemMatterFile || ''), title,
            funding_amount_mentions: [...title.matchAll(/\$\s*[\d,]+(?:\.\d{2})?/g)].slice(0, 12).map(hit => hit[0]),
            location_text_candidates: addresses,
            official_action: item.EventItemActionName || null, official_result: item.EventItemPassedFlag ?? null,
            relation: 'official_item_observation', geo_state: 'place_not_geocoded',
            funding_state: 'amount_not_award', timestamp_state: 'not_aligned_to_recording' };
        });
        civic = { event_id: civicEvent, state: 'observed', api_url: endpoint.href,
          response_sha256: sha(bytes), item_count: rows.length, items: rows,
          vote_verification: 'not_performed', video_alignment: 'not_verified', effect: 'none' };
      } catch (error) {
        // A separately named official MeetingDetail source may still answer
        // while Legistar's JSON API refuses. Never fetch an invented URL or
        // borrow a recording URL as authority for agenda-item identities.
        const detail = urls.find(raw => {
          const u = new URL(raw);
          return u.hostname === 'cityofdallas.legistar.com' &&
            u.pathname.toLowerCase() === '/meetingdetail.aspx' &&
            u.searchParams.get('ID') === String(civicEvent);
        });
        if (!detail) civic = { event_id: civicEvent, state: 'unavailable', items: [],
          reason: String(error.message || error).slice(0,160), effect: 'none' };
        else {
          try {
            const reply = await request(detail, { method:'GET', headers:{Accept:'text/html'}, signal:AbortSignal.timeout(20000) });
            if (!reply.ok) throw new Error(`official meeting HTML HTTP ${reply.status}`);
            const bytes = await boundedResponse(reply, 4 * 1024 * 1024);
            const items = parseLegistarAgendaHtml(bytes.toString('utf8'));
            civic = { event_id: civicEvent, state:'observed', api_url:endpoint.href,
              source_url:detail, evidence_route:'official_meeting_html_fallback',
              api_failure:String(error.message || error).slice(0,120),
              response_sha256:sha(bytes), item_count:items.length, items,
              vote_verification:'not_performed',video_alignment:'not_verified',effect:'none' };
          } catch (fallbackError) {
            civic = { event_id:civicEvent,state:'unavailable',items:[],
              reason:(String(error.message || error) + '; html: ' + String(fallbackError.message || fallbackError)).slice(0,220),
              effect:'none' };
          }
        }
      }
    }
    return {
      schema_version: 'bonfyre.foreign.public-release.v1',
      public_owner: origin.origin, generation, query: effectiveQuery, requested_query: query, offset,
      total: first.total, estate_total: first.estate_total,
      release_summary: first.release_summary, source_release: urls.length ? undefined : first.twins,
      case: urls.length ? { state: missing.length ? 'incomplete' : 'released_sources_resolved',
        source_refs: caseMembers.map(row => row.source), exact_sources: caseMembers,
        missing, source_collection_draft: { source_refs: caseMembers.map(row => row.source) },
        selection_effect: 'none', authority: 'not_requested' } : null,
      context_candidates: context, funding_observations: funding, readback: readbacks,
      civic_agenda: civic,
      local_project_store: 'separate_unless_explicitly_bound',
      cannot_infer: 'A source contract, public catalogue readback, shared host, geographic name or matching funding term does not prove a meeting-video alignment, grant eligibility, spatial relation, transcript accuracy, media rights, published World or external authority.'
    };
  }
  const matrixRequired = 'set BONFYRE_MATRIX_DB for a local owner or the bounded BONFYRE_MATRIX_REMOTE_* read route for the deployed native Matrix owner';
  const matrixHost = (value) => {
    const host = String(value || '').trim().toLowerCase().replace(/\.$/, '');
    if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/.test(host)) throw new Error('host must be a normalized DNS hostname');
    return host;
  };
  const matrixPath = (value, closure = false) => {
    const address = String(value ?? '/').trim();
    const grammar = closure ? /^\/[A-Za-z0-9._~:@/=&%+-]{0,1023}$/ : /^\/[A-Za-z0-9._~:@/?=&%+-]{0,1023}$/;
    if (!grammar.test(address) || (closure && (address.includes('/../') || address === '/..' || address.includes('//')))) throw new Error('path must be one bounded local Matrix address path');
    return address;
  };
  function matrixJson(stdout) {
    for (const line of String(stdout || '').split(/\r?\n/).reverse()) { try { const value = JSON.parse(line); if (object(value)) return value; } catch { /* native diagnostics are not state */ } }
    return null;
  }
  async function matrixRead(command, argv, timeout) {
    const local = expandHome(env.BONFYRE_MATRIX_DB);
    if (local && await fileExists(local)) {
      const binary = await candidates('BonfyreWebAddress');
      if (!binary) throw new Error('configured local Matrix lacks native BonfyreWebAddress owner');
      const result = await execute(binary.file, [command, '--db', local, ...argv], { cwd: binary.source === 'managed' ? path.dirname(binary.file) : root, timeoutMs: timeout * 1000 });
      return [result, { kind: 'local', database: local, binary: binary.file }];
    }
    const target = env.BONFYRE_MATRIX_REMOTE_SSH_TARGET;
    const key = expandHome(env.BONFYRE_MATRIX_REMOTE_SSH_KEY);
    const db = env.BONFYRE_MATRIX_REMOTE_DB;
    const binary = env.BONFYRE_MATRIX_REMOTE_BINARY || '/usr/local/bin/bonfyre-web-address';
    if (!target || !key || !db || !binary || !await fileExists(key) || !/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+$/.test(target) || !/^\/[A-Za-z0-9._/+-]{1,255}$/.test(db) || !/^\/[A-Za-z0-9._/+-]{1,255}$/.test(binary)) return [null, null];
    // OpenSSH transports a remote command through the remote user's shell.
    // The local argv boundary does not survive that hop, so quote every remote
    // token and pass one command string. No caller controls the executable.
    const shellWord = (word) => `'${String(word).replaceAll("'", "'\\''")}'`;
    const remoteCommand = [binary, command, '--db', db, ...argv].map(shellWord).join(' ');
    const result = await run(['ssh', '-i', key, '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', target, remoteCommand], { cwd: root, timeoutMs: timeout * 1000, env });
    return [{ ...result, cwd: root }, { kind: 'configured-remote-native-owner', target, database: db, binary }];
  }
  const matrixReceipt = (result) => ({ exit_code: result.exit_code, duration_ms: result.duration_ms, stdout_sha256: sha(result.stdout || ''), stderr_sha256: sha(result.stderr || '') });
  async function matrixResolve(args) {
    const host = matrixHost(args.host), address = matrixPath(args.path);
    const world = String(args.world ?? 'live').trim().toLowerCase(), audience = String(args.audience ?? 'public').trim().toLowerCase(), representation = String(args.representation ?? 'HTML').trim();
    if (!['live', 'shadow', 'replay', 'candidate'].includes(world) || !['self', 'household', 'group', 'community', 'public', 'market', 'institution'].includes(audience) || !['HTML', 'API', 'MCP', 'A2A', 'BonfyreFS', 'CMS', 'Habitat'].includes(representation)) throw new Error('invalid Matrix coordinate');
    const address_context = { host, path: address, world, audience, representation };
    const [result, backend] = await matrixRead('resolve', ['--host', host, '--path', address, '--world', world, '--audience', audience, '--repr', representation], clamp(args.timeout_seconds, 30, 5, 120));
    if (!result) return { schema_version: 'bonfyre.matrix.address-resolution.receipt.v1', status: 'unconfigured', address_context, required_configuration: matrixRequired, forbidden_inference: 'an unconfigured MCP transport must not bootstrap a temporary Matrix or claim an address resolves' };
    const native_resolution = matrixJson(result.stdout);
    return { schema_version: 'bonfyre.matrix.address-resolution.receipt.v1', status: result.exit_code === 0 && native_resolution?.verdict === 'resolved' ? 'resolved' : 'not_resolved', address_context, native_owner: 'BonfyreWebAddress/BonfyreMatrix', backend, native_resolution, native_receipt: matrixReceipt(result), unresolved_closures: ['actor/session', 'delegation envelope', 'information policy', 'effect authority', 'provider state', 'resource admission', 'economic commitment', 'evidence frontier'], forbidden_inference: 'an address resolution is not a viewer closure, private-read right, publication decision, provider availability, payment state, or effect authority' };
  }
  async function matrixConstellation(args) {
    const subject = ident(args.subject, 'subject');
    const [result, backend] = await matrixRead('constellation', ['--subject', subject], clamp(args.timeout_seconds, 30, 5, 120));
    if (!result) return { schema_version: 'bonfyre.matrix.constellation.receipt.v1', status: 'unconfigured', subject, required_configuration: matrixRequired, forbidden_inference: 'a missing Matrix deployment configuration is not an empty address constellation' };
    const constellation = matrixJson(result.stdout);
    return { schema_version: 'bonfyre.matrix.constellation.receipt.v1', status: result.exit_code === 0 && constellation ? 'available' : 'not_available', subject, native_owner: 'BonfyreWebAddress/BonfyreMatrix', backend, constellation, native_receipt: matrixReceipt(result), forbidden_inference: 'multiple coordinates locate one subject; they do not create copies or grant authority through any carrier' };
  }
  async function matrixClosure(args) {
    const host = matrixHost(args.host), address = matrixPath(args.path, true), format = String(args.format ?? 'ro-crate').trim().toLowerCase();
    if (!['ro-crate', 'zarr'].includes(format)) throw new Error('format must be ro-crate or zarr');
    const [result, backend] = await matrixRead('closure', ['--host', host, '--path', address, '--format', format], clamp(args.timeout_seconds, 30, 5, 120));
    if (!result) return { schema_version: 'bonfyre.matrix.closure.receipt.v1', status: 'unconfigured', host, path: address, format, required_configuration: matrixRequired, forbidden_inference: 'an unconfigured transport must not synthesize a crate or Zarr closure' };
    const closure = matrixJson(result.stdout);
    return { schema_version: 'bonfyre.matrix.closure.receipt.v1', status: result.exit_code === 0 && closure ? 'available' : 'not_available', host, path: address, format, native_owner: 'BonfyreMatrix/RecursiveMatrixClosure', backend, closure, native_receipt: matrixReceipt(result), unresolved_closures: ['actor/session', 'delegation envelope', 'information policy', 'evidence frontier', 'material body', 'latent passport', 'provider/resource state', 'work/effect authority', 'economic commitment'], forbidden_inference: 'a Matrix closure or owner reference is not a viewer closure, readable/current body, model route, financial state, or effect authority' };
  }

  async function moeHttp(endpoint, route, payload, timeoutSeconds) {
    const base = new URL(endpoint);
    if (!['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname) || base.protocol !== 'http:' || base.username || base.password || base.pathname !== '/') throw new Error('managed MoE endpoint must be loopback HTTP');
    const response = await request(new URL(route, base), { method: payload ? 'POST' : 'GET', headers: payload ? { 'Content-Type': 'application/json' } : {}, body: payload ? JSON.stringify(payload) : undefined, redirect: 'error', signal: AbortSignal.timeout(timeoutSeconds * 1000) });
    const bytes = await boundedResponse(response, 2_000_000);
    if (!response.ok) throw new Error(`managed MoE service returned HTTP ${response.status}`);
    try { return JSON.parse(bytes.toString('utf8')); } catch { throw new Error('managed MoE service returned malformed JSON'); }
  }
  async function moeStatus() {
    const started = Date.now();
    const endpoint = moeEndpoint.replace(/\/$/, '');
    const health = await moeHttp(endpoint, '/health', null, 10);
    // The bonfyre-moe front answers residency without loading a model; asking
    // /v1/models there is real work that loads one, and answers 503 when memory
    // is short. A plain llama-server has no residency route, so it falls back.
    const residency = await moeHttp(endpoint, '/bonfyre/residency', null, 10).catch(() => null);
    if (residency) return { schema_version: 'bonfyre.moe.service.status.v1', status: health.status === 'ok' ? 'passed' : 'failed', endpoint, health,
      residency, models: (residency.candidates || []).map(row => ({ id: row.model, usable: row.usable, min_bytes: row.min_bytes })), duration_ms: Date.now() - started };
    const models = await moeHttp(endpoint, '/v1/models', null, 10);
    return { schema_version: 'bonfyre.moe.service.status.v1', status: health.status === 'ok' ? 'passed' : 'failed', endpoint, health, models: models.data || [], duration_ms: Date.now() - started };
  }
  async function writeGenerationReceipt(file, receipt) {
    const temporary = path.join(receipts, `.${receipt.receipt_id}.${randomUUID()}.tmp`);
    try { await writeFile(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { flag: 'wx' }); await rename(temporary, file); }
    finally { await unlink(temporary).catch(() => {}); }
  }
  async function reusableGeneration(fingerprint) {
    const names = await readdir(receipts).catch(() => []);
    if (names.length > 512) return null;
    const retained = [];
    for (const name of names.filter((entry) => /^[0-9a-f-]{36}\.json$|^quic-fpq-[0-9a-f]{32}\.json$/.test(entry))) {
      const file = path.join(receipts, name);
      const metadata = await lstat(file).catch(() => null);
      if (!metadata?.isFile() || metadata.size > 3_000_000) continue;
      const prior = await jsonFile(file, null);
      if (prior?.request_fingerprint !== fingerprint || !['projection_failed', 'projection_pending'].includes(prior.status) || prior.generation_status !== 'passed') continue;
      const createdAt = Date.parse(prior.created_at);
      if (!Number.isFinite(createdAt) || createdAt > Date.now() || Date.now() - createdAt > 3_600_000) continue;
      if (typeof prior.output !== 'string' || sha(prior.output) !== prior.output_sha256) continue;
      retained.push(prior);
    }
    retained.sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at));
    return retained.find((prior) => prior.projection_progress?.attempts?.length) || retained[0] || null;
  }
  async function recordGeneration(receipt) {
    await mkdir(receipts, { recursive: true });
    await mkdir(workgraphs, { recursive: true });
    const receiptPath = path.join(receipts, `${receipt.receipt_id}.json`);
    receipt.generation_status = 'passed';
    receipt.status = 'projection_pending';
    receipt.control_plane_manifest_persisted = false;
    receipt.projection_progress ||= { attempts: [], completed: {} };
    delete receipt.projection_progress.failed;
    await writeGenerationReceipt(receiptPath, receipt);
    let stage = 'cms_schema';
    const tracked = async (name, argv, label, options) => {
      stage = label;
      const attempt = { stage, owner: name, status: 'outcome_uncertain', started_at: isoNow() };
      receipt.projection_progress.attempts.push(attempt);
      // A runner may throw or this process may die after native execution has
      // begun. Durable uncertainty must precede launch, not follow return.
      await writeGenerationReceipt(receiptPath, receipt);
      const result = await controlCommand(name, argv, options);
      Object.assign(attempt, { status: 'completed', exit_code: result.exit_code, timeout: Boolean(result.timeout), overflow: Boolean(result.overflow), stdout_sha256: sha(result.stdout || ''), stderr_sha256: sha(result.stderr || ''), duration_ms: result.duration_ms });
      await writeGenerationReceipt(receiptPath, receipt);
      return result;
    };
    try {
    const common = ['--db', cmsDb, '--schemas', cmsSchemas];
    const schemaFile = path.join(cmsSchemas, 'cms_generation.json');
    if (!await fileExists(schemaFile)) throw new Error(`managed CMS schema is missing: ${schemaFile}`);
    assertSuccess(await tracked('bonfyre-cms', ['schema', 'apply', schemaFile, ...common], 'cms_schema'), 'MoE CMS schema');
    const externalId = `${receipt.model}:${receipt.output_sha256}`;
    const listing = parseNativeJson(await tracked('bonfyre-cms', ['entry', 'list', 'cms_generation', '--ns', 'bonfyre-generation', '--limit', '100', ...common], 'cms_list'), 'MoE CMS list');
    const locator = { database: cmsDb, schemas: cmsSchemas, namespace: 'bonfyre-generation', external_id: externalId };
    let cmsLocator;
    const existing = (listing.data || []).find((entry) => entry?.attributes?.external_id === externalId);
    if (existing) cmsLocator = { ...locator, status: 'already_recorded', entry_id: existing.id };
    else {
      const entry = { title: `Generated by ${receipt.model}`, status: 'generated', output: receipt.output, output_sha256: receipt.output_sha256, model: receipt.model, backend: receipt.backend, prompt_sha256: receipt.prompt_sha256, max_new_tokens: String(receipt.request.max_tokens), source_event: receipt.event || 'moe.generate', source_schema_version: receipt.schema_version, source_received_at: receipt.created_at, external_id: externalId, idempotency_key: `${receipt.schema_version}:${receipt.output_sha256}` };
      const created = parseNativeJson(await tracked('bonfyre-cms', ['entry', 'create', 'cms_generation', '--ns', 'bonfyre-generation', JSON.stringify(entry), ...common], 'cms_create'), 'MoE CMS create');
      cmsLocator = { ...locator, status: 'recorded', entry_id: created.id };
    }
    receipt.projection_progress.completed.cms = cmsLocator;
    await writeGenerationReceipt(receiptPath, receipt);
    assertSuccess(await tracked('bonfyre-graph', ['init', graphDb], 'graph_init'), 'artifact graph init');
    const stem = receipt.receipt_id, event = receipt.event || 'moe.generate';
    const promptAtom = `generation-prompt-${stem}`, outputAtom = `generation-output-${stem}`, operator = `${event}-${stem}`;
    for (const argv of [
      ['add-atom', graphDb, '--id', promptAtom, '--hash', receipt.prompt_sha256, '--type', 'application/vnd.bonfyre.generation-prompt'],
      ['add-atom', graphDb, '--id', outputAtom, '--hash', receipt.output_sha256, '--type', 'application/vnd.bonfyre.generation-completion', '--path', receiptPath],
      ['add-op', graphDb, '--id', operator, '--op', event, '--inputs', promptAtom, '--output', outputAtom, '--params', JSON.stringify({ model: receipt.model, backend: receipt.backend, receipt_id: stem }), '--version', '1.0.0'],
    ]) assertSuccess(await tracked('bonfyre-graph', argv, `graph_${argv[0]}`), 'artifact graph');
    const artifactGraph = { status: 'recorded', database: graphDb, prompt_atom: promptAtom, output_atom: outputAtom, operator };
    receipt.projection_progress.completed.graph = artifactGraph;
    await writeGenerationReceipt(receiptPath, receipt);
    const flowId = `generation-${stem}`;
    const definitionPath = path.join(workgraphs, `${stem}.json`);
    const definition = { id: flowId, name: 'Controlled generation', entry: 'generate', stages: [{ id: 'generate', recipe: event }, { id: 'persist', recipe: 'cms.lambda-tensor.persist' }, { id: 'attest', recipe: 'graph.merkle.attest' }], edges: [{ from: 'generate', to: 'persist', kind: 'seq' }, { from: 'persist', to: 'attest', kind: 'seq' }], receipt_id: stem, output_sha256: receipt.output_sha256 };
    stage = 'workgraph_definition';
    await writeFile(definitionPath, `${JSON.stringify(definition, null, 2)}\n`, { flag: 'wx' });
    assertSuccess(await tracked('bonfyre-flow', ['define', definitionPath], 'workgraph_define', { extraEnv: { BONFYRE_FLOW_DB: flowDb } }), 'MoE WorkGraph define');
    const workgraph = { status: 'defined', flow_id: flowId, definition: definitionPath, database: flowDb };
    receipt.projection_progress.completed.workgraph = workgraph;
    await writeGenerationReceipt(receiptPath, receipt);
    for (const argv of [['init', '--root', root], ['actors', 'import', '--root', root], ['contracts', 'import', '--root', root]]) {
      assertSuccess(await tracked('bonfyre-discipl', argv, `actor_${argv[0]}`, { extraEnv: { BONFYRE_REPO_ROOT: root } }), 'MoE DisCIPL actor graph');
    }
    const actorGraph = { status: 'refreshed', database: path.join(root, 'discipl.db'), moe_contract: 'T_MOE_ROUTER -> T_MOE_EXPERT' };
    receipt.projection_progress.completed.actor_graph = actorGraph;
    await writeGenerationReceipt(receiptPath, receipt);
    receipt.control_plane = { schema_version: 'bonfyre.moe.control-plane.receipt.v1', status: 'passed', receipt_path: receiptPath, cms_lambda_tensor: cmsLocator, artifact_graph: artifactGraph, workgraph, actor_graph: actorGraph };
    receipt.status = 'passed';
    receipt.control_plane_manifest_persisted = true;
    receipt.control_plane_manifest_schema = receipt.control_plane.schema_version;
    await writeGenerationReceipt(receiptPath, receipt);
    return receipt;
    } catch (error) {
      receipt.status = 'projection_failed';
      receipt.control_plane_manifest_persisted = false;
      delete receipt.control_plane_manifest_schema;
      delete receipt.control_plane;
      receipt.projection_progress.failed = { stage, error_sha256: sha(String(error?.message || error)) };
      await writeGenerationReceipt(receiptPath, receipt);
      throw new Error(`generation ${receipt.receipt_id} retained at ${receiptPath}; projection failed at ${stage}`, { cause: error });
    }
  }
  async function moeGenerate(args) {
    const prompt = String(args.prompt || '').trim();
    const systemPrompt = String(args.system_prompt ?? "You are Bonfyre's precise local assistant. Return the requested result directly.").trim();
    if (!prompt || prompt.length > 32000 || systemPrompt.length > 8000) throw new Error('prompt or system_prompt exceeds local MoE bounds');
    const maxTokens = clamp(args.max_tokens, 256, 1, 1024), timeout = clamp(args.timeout_seconds, 180, 5, 600), temperature = validTemperature(args.temperature);
    const fingerprint = sha(JSON.stringify({ route: 'moe', endpoint: moeEndpoint, prompt, systemPrompt, maxTokens, temperature }));
    const reusable = await reusableGeneration(fingerprint);
    if (reusable) {
      if (reusable.projection_progress?.attempts?.length) throw new Error(`generation ${reusable.receipt_id} retained at ${path.join(receipts, `${reusable.receipt_id}.json`)}; manual projection reconciliation required before another model request`);
      return recordGeneration(reusable);
    }
    const started = Date.now();
    const completion = await moeHttp(moeEndpoint, '/v1/chat/completions', { messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: prompt }], max_tokens: maxTokens, temperature, stream: false }, timeout);
    const output = completion.choices?.[0]?.message?.content;
    if (typeof output !== 'string' || !output.trim()) throw new Error('managed MoE service returned no completion content');
    return recordGeneration({ schema_version: 'bonfyre.moe.generation.receipt.v1', receipt_id: randomUUID(), status: 'passed', endpoint: moeEndpoint, model: completion.model, backend: 'llama.cpp-metal', request: { max_tokens: maxTokens, temperature }, request_fingerprint: fingerprint, prompt_sha256: sha(prompt), output, output_sha256: sha(output), usage: completion.usage || {}, duration_ms: Date.now() - started, created_at: isoNow() });
  }
  async function quicGenerate(args) {
    const prompt = String(args.prompt || '').trim();
    if (!prompt || prompt.length > 32000) throw new Error('prompt exceeds local FPQ bounds');
    const maxTokens = clamp(args.max_tokens, 32, 1, 128), timeout = clamp(args.timeout_seconds, 300, 30, 900), temperature = validTemperature(args.temperature);
    const host = env.BONFYRE_QWEN_QUIC_HOST || '127.0.0.1';
    const port = Number(env.BONFYRE_QWEN_QUIC_PORT || 9443);
    if (!['127.0.0.1', 'localhost', '::1'].includes(host) || !Number.isInteger(port) || port < 1 || port > 65535) throw new Error('QUIC FPQ endpoint must be loopback');
    const model = env.BONFYRE_QWEN_QUIC_MODEL || 'qwen2.5-coder-0.5b-instruct-native-fp16.fpq';
    const fingerprint = sha(JSON.stringify({ route: 'quic-fpq', host, port, model, prompt, maxTokens, temperature }));
    const reusable = await reusableGeneration(fingerprint);
    if (reusable) {
      if (reusable.projection_progress?.attempts?.length) throw new Error(`generation ${reusable.receipt_id} retained at ${path.join(receipts, `${reusable.receipt_id}.json`)}; manual projection reconciliation required before another model request`);
      return recordGeneration(reusable);
    }
    const receiptId = `quic-fpq-${randomUUID().replaceAll('-', '')}`;
    const clientPath = path.join(receipts, `${receiptId}.quic-client.json`);
    await mkdir(receipts, { recursive: true });
    const clientRequest = { type: 'generate', request_id: receiptId, prompt, mode: 'code', max_new_tokens: maxTokens, temperature };
    const started = Date.now();
    const native = await controlCommand('bonfyre-qwen-fpq', ['quic-client', '--connect', `${host}:${port}`, '--request-json', JSON.stringify(clientRequest), '--timeout-ms', String(timeout * 1000), '--json', clientPath], { timeoutMs: (timeout + 30) * 1000 });
    assertSuccess(native, 'QUIC FPQ client');
    if (!await fileExists(clientPath)) throw new Error('QUIC FPQ client completed without a receipt');
    const client = await jsonFile(clientPath, null);
    let response;
    try { response = JSON.parse(String(client?.last_response || '')); } catch { throw new Error('QUIC FPQ client returned malformed response JSON'); }
    if (!response.success || typeof response.text !== 'string' || !response.text.trim()) throw new Error(String(response.error || 'QUIC FPQ service returned no completion content'));
    return recordGeneration({ schema_version: 'bonfyre.quic.fpq.generation.receipt.v1', receipt_id: receiptId, status: 'passed', event: 'quic.fpq.generate', endpoint: `quic://${host}:${port}`, model, backend: 'ngtcp2-quic-fpq-metal', request: { max_tokens: maxTokens, temperature }, request_fingerprint: fingerprint, prompt_sha256: sha(prompt), output: response.text, output_sha256: sha(response.text), usage: { completion_tokens: Number(response.tokens_generated || 0), token_streams_opened: Number(response.token_streams_opened || 0) }, quic_client_receipt: clientPath, duration_ms: Date.now() - started, created_at: isoNow() });
  }

  async function institutional(args) {
    const subject = ident(args.subject, 'subject'), objective = String(args.objective || '').trim();
    if (!objective || objective.length > 32000) throw new Error('objective is required and must be at most 32,000 characters');
    const mode = String(args.mode || 'prepare'), recipe = ident(args.recipe || 'institutional-prepare', 'recipe'), modelRoute = String(args.model_route || 'moe');
    if (!['observe', 'prepare'].includes(mode)) throw new Error('invalid institutional mode');
    if (!['moe', 'qwen_fpq'].includes(modelRoute)) throw new Error('model_route must be moe or qwen_fpq; remote fallback is not permitted');
    const nativeRecipe = await owner('BonfyreGen', ['--explain', objective], { timeoutMs: 90000 });
    const localRuntime = modelRoute === 'moe' ? await moeStatus() : null;
    if (localRuntime && localRuntime.status !== 'passed') throw new Error('managed local MoE did not satisfy its health contract');
    const financialRoute = await financial({ action: 'route', recipe }, Boolean(localRuntime));
    const prompt = 'Produce an institutional preparation memo for the exact subject below. Separate observed facts, proposed work, authority blockers, financial unknowns, and evidence needed. Do not claim contact, payment, publication, legal approval, or external completion.\n\n' + `subject: ${subject}\nmode: ${mode}\nobjective: ${objective}\n\n` + 'The native recipe compiler produced this candidate recipe:\n' + String(nativeRecipe.stdout || '').slice(-12000);
    const generation = modelRoute === 'moe' ? await moeGenerate({ prompt, system_prompt: 'You are a bounded institutional planning component. Preserve uncertainty, write concrete next preparation steps, and never convert a proposal into proof or an effect.', max_tokens: args.max_tokens ?? 700, temperature: args.temperature ?? 0, timeout_seconds: args.timeout_seconds ?? 300 }) : await quicGenerate({ prompt, max_tokens: Math.min(Number(args.max_tokens ?? 96), 128), temperature: args.temperature ?? 0, timeout_seconds: args.timeout_seconds ?? 600 });
    return { schema_version: 'bonfyre.institutional.prepare.receipt.v1', request_digest: sha(pythonJson({ subject, objective, mode, recipe }, true)), subject, mode, recipe, native_recipe: nativeRecipe, local_runtime: localRuntime, financial_route: financialRoute, local_generation: generation, authority: { effect_authority: 'not_present', allowed: ['local recipe compilation', 'local model preparation', 'receipt persistence'], refused: ['external outreach', 'foreign mutation', 'payment', 'publication', 'legal submission'] }, evidence_status: 'generated planning text is an attributed artifact, not evidence; it must cross EvidenceGraph/ProofFrontier before it can support a claim', control_plane: generation.control_plane };
  }

  async function publicQuote(capability) {
    const origin = new URL(publicOrigin);
    if (!['https:', 'http:'].includes(origin.protocol) || origin.username || origin.password) throw new Error('invalid configured public origin');
    const url = new URL('/mpp/quote', origin);
    url.searchParams.set('capability', capability);
    const response = await request(url, { headers: { Accept: 'application/json' }, redirect: 'error', signal: AbortSignal.timeout(20000) });
    const raw = await boundedResponse(response, 32768);
    if (!response.ok) throw new Error(`public MPP returned HTTP ${response.status}`);
    let quote;
    try { quote = JSON.parse(raw.toString('utf8')); } catch { throw new Error('owned public MPP quote was not valid JSON'); }
    if (!object(quote) || quote.protocol !== 'MPP' || quote.capability !== capability) throw new Error('owned public MPP quote did not bind the requested capability');
    if (quote.priced !== true || typeof quote.amount !== 'string') throw new Error('capability is not currently priced by the owned public service book');
    return { origin: publicOrigin, resource: url.toString(), quote, quote_sha256: sha(raw), forbidden_inference: 'an MPP quote is not a reservation, payment, or settlement' };
  }
  async function facilitator(capability, paymentReference) {
    const endpoint = String(env.BONFYRE_X402_FACILITATOR || '').trim();
    if (!endpoint) return { verified: false, reason: 'no facilitator configured (BONFYRE_X402_FACILITATOR); nothing has verified this payment and this will not pretend otherwise', facilitator: null };
    let url;
    try { url = new URL(`${endpoint.replace(/\/$/, '')}/verify`); } catch { return { verified: false, reason: 'configured facilitator URL invalid', facilitator: endpoint }; }
    if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) return { verified: false, reason: 'configured facilitator URL invalid', facilitator: endpoint };
    try {
      const response = await request(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ x402Version: 1, capability, payment: paymentReference, network: env.BONFYRE_X402_NETWORK || 'base-sepolia', payTo: env.BONFYRE_X402_PAY_TO || '', asset: env.BONFYRE_X402_ASSET || '' }), redirect: 'error', signal: AbortSignal.timeout(30000) });
      const raw = await boundedResponse(response, 32768);
      if (!response.ok) throw new Error(`facilitator returned HTTP ${response.status}`);
      if (hasNonIntegralJsonNumber(raw.toString('utf8'))) throw new Error('facilitator numeric spelling has no parity digest');
      const answer = JSON.parse(raw.toString('utf8'));
      if (!object(answer)) throw new Error('non-object facilitator answer');
      const verified = [answer.verified, answer.isValid, answer.valid].some((value) => value === true);
      return { verified, facilitator: endpoint, http_status: response.status, answer_digest: sha(pythonJson(answer)), reason: verified ? null : String(answer.reason || 'facilitator did not verify') };
    } catch { return { verified: false, reason: 'facilitator unreachable or refused', facilitator: endpoint }; }
  }
  async function fulfillment(args) {
    const action = String(args.action || 'open_quote'), capability = ident(args.capability, 'capability'), exchange = ident(args.exchange, 'exchange');
    if (capability.length > 48 || exchange.length > 32) throw new Error('capability must be at most 48 and exchange at most 32 characters');
    if (!['open_quote', 'present_payment', 'verify_payment', 'status'].includes(action)) throw new Error('invalid capability action');
    const quote = action === 'open_quote' ? await publicQuote(capability) : null;
    const runId = `cap-run-${capability}-${exchange}`, instanceId = `cap-protocol-${capability}-${exchange}`, definition = 'capability-fulfillment-rail-v3';
    await owner('BonfyreLifecycle', ['lifecycle', 'register-capability-fulfillment']);
    const registration = await owner('BonfyreLifecycle', ['lifecycle', 'register-capability-fulfillment-rail']);
    const worker = action === 'open_quote' ? await owner('BonfyreLifecycle', ['worker-run', 'start', runId, 'mcp-agent', `capability:${capability}`, instanceId]) : await owner('BonfyreLifecycle', ['worker-run', 'status', runId]);
    const instance = action === 'open_quote' ? await owner('BonfyreLifecycle', ['protocol-machine', 'start', instanceId, definition, runId, '--receipt', `mpp-quote-sha256:${quote.quote_sha256}`]) : await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]);
    const base = { schema_version: 'bonfyre.capability.fulfillment.receipt.v1', capability, exchange, worker_run: runId, protocol_instance: instanceId, protocol_definition: definition, native_lifecycle: { registration, worker, instance }, carrier_coordinates: { x402: `/capability/${capability}`, mpp: '/mpp/quote', mcp: 'capability_fulfillment', a2a: '/.well-known/agent.json', cloudevents: '/events', commerce: ['/beckn/search', '/ucp/catalog', '/acp/products'] } };
    if (action === 'open_quote') return { ...base, public_mpp_quote: quote, financial_membrane: await financial({ action: 'route', recipe: capability }), state: await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]), authority: { allowed: ['quote', 'discover', 'compare capability'], not_granted: ['reserve', 'charge', 'verify payment', 'execute', 'deliver'] }, forbidden_inference: 'a quote is not a reservation, payment, execution right, or delivery' };
    if (action === 'present_payment') {
      const reference = String(args.payment_reference || '').trim();
      if (!reference || reference.length > 512) throw new Error('payment_reference is required and must be at most 512 characters');
      const digest = sha(reference);
      return { ...base, payment_claim_digest: digest, transition: await owner('BonfyreLifecycle', ['protocol-machine', 'step', instanceId, 'present-payment', `payment-presented-${digest.slice(0, 24)}`, '--receipt', `payment-claim-sha256:${digest}`]), state: await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]), authority: { allowed: ['record payment presentation claim'], not_granted: ['payment verification', 'settlement', 'execution', 'delivery'] }, next_required: 'a configured and independently exercised payment rail must submit a verification receipt', forbidden_inference: 'a presented payment claim is not verified payment or settlement' };
    }
    if (action === 'verify_payment') {
      const reference = String(args.payment_reference || '').trim();
      if (!reference) throw new Error('payment_reference is required to verify a payment');
      const verification = await facilitator(capability, reference);
      if (!verification.verified) return { ...base, verification, state: await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]), authority: { allowed: ['ask a facilitator'], not_granted: ['settlement', 'execution', 'delivery'] }, forbidden_inference: 'an unverified payment is not a settled one' };
      return { ...base, verification, transition: await owner('BonfyreLifecycle', ['protocol-machine', 'step', instanceId, 'rail-verified', `rail-verified-${verification.answer_digest.slice(0, 24)}`, '--receipt', `facilitator-sha256:${verification.answer_digest}`]), state: await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]), authority: { allowed: ["record a facilitator's verification"], not_granted: ['execution', 'delivery'] }, forbidden_inference: 'a settled payment is not an executed or delivered capability' };
    }
    return { ...base, state: await owner('BonfyreLifecycle', ['protocol-machine', 'status', instanceId]), worker_state: await owner('BonfyreLifecycle', ['worker-run', 'status', runId]), forbidden_inference: 'protocol state alone is not payment verification, external execution, or delivery' };
  }

  // The commercial stores each own one part of a relationship. This reads them
  // side by side through the finance and commerce owners and changes nothing.
  // Public fitness content is a projection of the existing approved CMS
  // release. No member request, training history, coach consent, or Pay
  // record is reachable through this read-only MCP surface.
  async function trainingPublic(args) {
    const studio = String(args.studio ?? '').trim();
    if (!/^[a-z0-9-]{1,64}$/.test(studio)) throw new Error('Invalid studio identity');
    const allowed = String(env.BONFYRE_TRAINING_MCP_PUBLIC_STUDIOS || '')
      .split(',').map(s => s.trim()).filter(Boolean);
    if (!allowed.includes(studio)) throw new Error('Studio has not enabled public MCP programmes');
    const location = String(env.BONFYRE_TRAINING_CMS_HOME || '');
    if (!path.isAbsolute(location)) throw new Error('Public training CMS is not configured');
    const db = path.join(location, 'catalogue.db');
    if (!await fileExists(db)) throw new Error('Public training catalogue is unavailable');
    const result = await owner('BonfyreCMS', ['entry', 'list', 'training_program_release',
      '--ns', 'training-' + studio, '--limit', '101', '--db', db,
      '--schemas', path.join(root, 'cmd/BonfyreCMS/content-types')], { timeoutMs: 15000 });
    const nativeRows = parseNativeJson(result, 'BonfyreCMS')?.data;
    const programs = publicTrainingReleases(nativeRows, studio);
    return { schema_version: 'bonfyre.training.public-programs.mcp.v1',
      studio, programs, owner: 'BonfyreCMS/training_program_release',
      source_readback_sha256: result.stdout_sha256,
      visibility: 'public-approved-programs-only',
      forbidden_inference: 'A published routine is not paid entitlement, coaching consent, private training progress, or a settled payment.' };
  }

  async function commercial(args) {
    const view = String(args.view || 'relationships').trim();
    if (!['relationships', 'books', 'exchange'].includes(view)) throw new Error('view must be relationships, books or exchange');
    const estate = env.BONFYRE_ESTATE_LOCAL || path.join(home, '.bonfyre', 'estate-local');
    const shared = env.BONFYRE_ESTATE_STORES || path.join(home, '.local', 'share', 'bonfyre');
    const stores = {
      finance: env.BONFYRE_ESTATE_FINANCE || path.join(estate, 'finance.db'),
      cms: env.BONFYRE_ESTATE_CMS || path.join(estate, 'cms.db'),
      commerce: env.BONFYRE_COMMERCE_DB || path.join(estate, 'commerce.db'),
      outreach: path.join(shared, 'outreach.db'),
      economy: path.join(shared, 'economy.db'),
      pay: path.join(shared, 'pay.db'),
    };
    const forbidden = 'A price offered is not an accepted scope, a message sent is not a reply, a stage recorded is not the next one, and nothing read here is a payment.';
    if (view === 'exchange') {
      const exchange = String(args.exchange || '').trim();
      if (!exchange || exchange.length > 300) throw new Error('exchange is required, for example opportunity:<finance id>');
      const native = await controlCommand('bonfyre-commerce', ['progress', '--db', stores.commerce, '--exchange', exchange]);
      return { schema_version: 'bonfyre.commercial.state.v1', view, exchange, result: parseNativeJson(native, 'BonfyreCommerce'), stores: { commerce: stores.commerce }, forbidden_inference: forbidden };
    }
    const argv = view === 'books'
      ? ['books', '--pay-db', stores.pay, '--economy-db', stores.economy, '--cms-db', stores.cms, '--finance-db', stores.finance]
      : ['--db', stores.finance, 'relationships', '--outreach-db', stores.outreach, '--cms-db', stores.cms, '--commerce-db', stores.commerce];
    const native = await controlCommand('bonfyre-finance', argv, { timeoutMs: 60000 });
    const result = parseNativeJson(native, 'BonfyreFinance');
    return { schema_version: 'bonfyre.commercial.state.v1', view, result, stores, forbidden_inference: forbidden };
  }

  // Candidate passages from a store Recall built over the estate's own records
  // and documents. Learned proximity only: the owner of a record still decides.
  async function recall(args) {
    const text = String(args.text || '').trim();
    if (!text || text.length > 2000) throw new Error('text is required (up to 2000 characters)');
    const name = String(args.store || 'campaign').trim();
    if (!/^[a-z0-9][a-z0-9_-]{0,63}$/.test(name)) throw new Error('store is a short name such as campaign');
    const top = Number.isInteger(args.top) ? args.top : 8;
    if (top < 1 || top > 50) throw new Error('top must be 1..50');
    const estate = env.BONFYRE_ESTATE_LOCAL || path.join(home, '.bonfyre', 'estate-local');
    const store = path.join(estate, 'recall', `${name}.db`);
    const collapse = args.collapse_exact_text === true;
    // Keep the canonical ranking while allowing more candidates for distinct results.
    const candidateTop = collapse ? Math.min(50, top * 3) : top;
    const native = await controlCommand('bonfyre-recall', ['query', '--store', store, '--text', text, '--top', String(candidateTop), '--hybrid'], { timeoutMs: 60000 });
    const result = parseNativeJson(native, 'BonfyreRecall');
    const projection = collapse ? collapseExactPassages(result.fused?.results ?? [], top) :
      { passages: result.fused?.results };
    return { schema_version: 'bonfyre.estate.recall.v1', store: name, ...projection, word_matches: result.fused?.word_matches, truth: result.truth, forbidden_inference: 'A passage near the question is a place to look, not the current state of the record it came from, and not evidence, authority or permission.' };
  }

  const methods = { native_tool_inventory: inventory, native_tool_invoke: invoke, estate_capabilities: estateCapabilities, estate_adapter_probe: estateAdapterProbe, estate_recall: recall, bonfyre_moe_status: moeStatus, bonfyre_moe_generate: moeGenerate, bonfyre_quic_fpq_generate: quicGenerate, financial_membrane: financial, financial_read: financialRead, frappe_compile: frappe, cms_record: cms, cms_read: cmsRead, commercial_state: commercial, training_public_programs: trainingPublic, foreign_twin_observe: foreign, foreign_twin_release: foreignRelease, institutional_prepare: institutional, capability_fulfillment: fulfillment, matrix_address_resolve: matrixResolve, matrix_subject_constellation: matrixConstellation, matrix_closure_compile: matrixClosure };
  const call = async (name, args = {}) => {
    if (!Object.hasOwn(methods, name)) throw new Error(`unknown native Project tool: ${name}`);
    if (!object(args)) throw new Error('tool arguments must be an object');
    return methods[name](args);
  };
  // Other Project owners are callable; keep that integration shape while also
  // exposing the named call method for direct clients and fixture tests.
  call.call = call;
  return Object.freeze(call);
}
