// Local retained observations only. Neither report grants authority or moves money.
import { constants, openSync, closeSync, fstatSync, readSync, readdirSync,
  writeFileSync, fsyncSync, renameSync, unlinkSync, existsSync } from 'node:fs';
import { dirname, basename, join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

const MAX_FILE_BYTES = 16 * 1024 * 1024;
const MAX_RUN_BYTES = 256 * 1024 * 1024;
const MAX_FILES = 100000;
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const clip = (value, size) => Array.from(value ?? '').slice(0, size).join('');

export function observationReader() {
  let remaining = MAX_RUN_BYTES;
  return (path, { optional = false, limit = MAX_FILE_BYTES } = {}) => {
    let fd;
    try { fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); }
    catch (error) { if (optional && error.code === 'ENOENT') return Buffer.alloc(0); throw error; }
    try {
      const before = fstatSync(fd);
      if (!before.isFile() || !Number.isSafeInteger(before.size) || before.size > limit || before.size > remaining)
        throw new Error(`observation exceeds regular-file budget: ${basename(path)}`);
      remaining -= before.size;
      const bytes = Buffer.alloc(before.size);
      let offset = 0;
      while (offset < bytes.length) {
        const n = readSync(fd, bytes, offset, bytes.length - offset, offset);
        if (!n) throw new Error(`observation changed while reading: ${basename(path)}`);
        offset += n;
      }
      const after = fstatSync(fd);
      if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs)
        throw new Error(`observation changed while reading: ${basename(path)}`);
      return bytes;
    } finally { closeSync(fd); }
  };
}

function names(directory, include, optional = false) {
  let entries;
  try { entries = readdirSync(directory); }
  catch (error) { if (optional && error.code === 'ENOENT') return []; throw error; }
  if (entries.length > MAX_FILES) throw new Error('observation directory exceeds entry budget');
  return entries.filter(include).sort();
}

export function atomicReport(path, produce) {
  const temporary = join(dirname(path), `.${basename(path)}-${randomUUID()}.tmp`);
  let fd = openSync(temporary, 'wx', 0o600);
  try {
    const summary = produce(bytes => writeFileSync(fd, bytes));
    fsyncSync(fd); closeSync(fd); fd = undefined;
    renameSync(temporary, path);
    return summary;
  } catch (error) {
    if (fd !== undefined) closeSync(fd);
    unlinkSync(temporary);
    throw error;
  }
}

export function grantClosure(grants, estate) {
  const read = observationReader();
  const proven = new Set();
  const evidence = read(join(estate, 'enactment-evidence.jsonl'), { limit: 64 * 1024 * 1024 });
  for (const line of evidence.toString('utf8').split('\n')) {
    // Preserve the legacy tag heuristic. This is NOT a state-transition verifier.
    if (!line.includes('"obligations_closed"')) continue;
    const entry = JSON.parse(line);
    if (!object(entry) || typeof entry.occurrence_id !== 'string') throw new Error('invalid evidence occurrence_id');
    proven.add(entry.occurrence_id);
  }
  const attestationPresent = existsSync(join(estate, 'attestation.json'));
  const rows = [], classes = {};
  for (const name of names(grants, name => name.startsWith('opp-') && name.endsWith('.json'), true)) {
    const doc = JSON.parse(read(join(grants, name)).toString('utf8'));
    if (!object(doc)) throw new Error(`invalid opportunity: ${name}`);
    const data = doc.data ?? {}, syn = data.synopsis ?? {};
    if (!object(data) || !object(syn)) throw new Error(`invalid synopsis: ${name}`);
    if (!Object.keys(syn).length) continue;
    const applicants = syn.applicantTypes ?? [], categories = syn.fundingActivityCategories ?? [];
    if (!Array.isArray(applicants) || !Array.isArray(categories)) throw new Error(`invalid opportunity categories: ${name}`);
    for (const field of ['opportunityTitle', 'agencyName'])
      if (syn[field] !== undefined && typeof syn[field] !== 'string') throw new Error(`invalid ${field}: ${name}`);
    if (data.opportunityTitle !== undefined && typeof data.opportunityTitle !== 'string') throw new Error(`invalid opportunityTitle: ${name}`);
    if (syn.responseDate != null && typeof syn.responseDate !== 'string' && !Number.isSafeInteger(syn.responseDate))
      throw new Error(`invalid responseDate: ${name}`);
    const residuals = [];
    if (applicants.length) {
      if (!object(applicants[0]) || (applicants[0].description !== undefined && typeof applicants[0].description !== 'string'))
        throw new Error(`invalid applicant description: ${name}`);
      // No applicant class is asserted for this estate; do not infer eligibility.
      residuals.push({ class: 'PartnerResidual', why: `eligibility: ${clip(applicants[0].description, 44)}` });
    }
    if (syn.costSharing) residuals.push({ class: 'CapitalResidual', why: 'cost sharing or match required' });
    if (syn.responseDate) residuals.push({ class: 'TemporalResidual', why: `closes ${syn.responseDate}` });
    rows.push({ opportunity: name.replaceAll('opp-', '').replaceAll('.json', ''),
      title: clip(syn.opportunityTitle || data.opportunityTitle || '', 60), agency: clip(syn.agencyName, 44),
      evidence_class_covered: Boolean((categories.length || syn.synopsisDesc) && proven.size && attestationPresent),
      residuals, blocking_classes: [...new Set(residuals.map(row => row.class))].sort() });
    for (const residual of residuals) classes[residual.class] = (classes[residual.class] ?? 0) + 1;
  }
  return { schema: 'bonfyre-grant-closure.v1', source: 'retained grants.gov opportunity documents; freshness not verified',
    opportunities_assessed: rows.length, covered_with_no_gaps: rows.filter(row => !row.residuals.length).length,
    evidence_class_covered: rows.filter(row => row.evidence_class_covered).length, blocked_by_class: classes,
    estate_capabilities_applied: { sources_proven_readable: proven.size, signed_attestation: attestationPresent, provenance_chain_depth: 5 },
    assessment_basis: { evidence: 'legacy obligations_closed tag presence; not grant-specific evidence verification',
      signed_attestation: 'legacy field means attestation.json exists; signature and contents not verified',
      provenance_chain_depth: 'legacy declared depth; not measured by this report',
      covered_with_no_gaps: 'absence of listed partner/capital/temporal residuals only; evidence may still be absent',
      eligibility: 'no estate applicant types asserted', deadlines: 'presence only; no open/expired classification' },
    truth: 'coverage_of_requirement_classes_not_of_awards', cannot_infer: [
      'a covered evidence class -> an awarded grant',
      'an eligibility residual -> a fixable one; it is what the institution is', 'assessed -> applied for',
      'attestation file presence -> a verified signature or verified provenance chain',
      'generic evidence presence -> grant-specific evidence sufficiency or eligibility',
      'covered_with_no_gaps -> all grant requirements satisfied' ], opportunities: rows };
}

function envelope(raw) {
  try {
    const doc = JSON.parse(raw.toString('utf8'));
    return object(doc) && Array.isArray(doc.accepts) ? doc.accepts : [];
  } catch { return []; }
}

function publishedQuote(raw, headers) {
  const lines = headers.toString('utf8').split(/\r?\n/);
  // Require an actual status code, not a 402 substring in a reason phrase.
  if (!/^HTTP\/\d(?:\.\d)?\s+402(?:\s|$)/.test(lines[0])) return null;
  let accepts = envelope(raw), source = 'body';
  if (!accepts.length) {
    const line = lines.find(line => /^payment-required:/i.test(line));
    if (!line) return null;
    const encoded = line.slice(line.indexOf(':') + 1).trim();
    if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) || encoded.replace(/=+$/, '').length % 4 === 1) return null;
    const decoded = Buffer.from(encoded, 'base64');
    if (decoded.toString('base64').replace(/=+$/, '') !== encoded.replace(/=+$/, '')) return null;
    accepts = envelope(decoded); source = 'payment-required';
  }
  const first = accepts[0];
  if (!object(first)) return null;
  const extra = object(first.extra) ? first.extra : {};
  const amount = first.amount || first.maxAmountRequired || '';
  const fields = [amount, extra.humanAmount || '', extra.assetSymbol || first.asset || '', first.network || ''];
  // Do not round a price above JS's exact integer range, or stringify objects.
  if (!amount || fields.some(value => typeof value !== 'string' && !(typeof value === 'number' && Number.isSafeInteger(value)))) return null;
  return { amount: String(amount), human_amount: String(fields[1]), asset: String(fields[2]), network: String(fields[3]), envelope_source: source };
}

export function captureQuotes(quotes, out, now = new Date()) {
  const read = observationReader();
  const stamp = now.toISOString().replace(/\.\d{3}Z$/, 'Z');
  return atomicReport(out, write => {
    let written = 0;
    for (const name of names(quotes, name => name.endsWith('.body'))) {
      const raw = read(join(quotes, name));
      const headers = read(join(quotes, `${name.slice(0, -5)}.headers`), { optional: true });
      const quote = publishedQuote(raw, headers);
      if (!quote) continue;
      const ident = `external:${name.slice(0, -5)}`;
      const record = { schema: 'bonfyre-owner-participation.v1', occurrence_id: ident, axis: 'economic', state: 'quoted',
        owner: 'ForeignQuote', from_owner: 'ForeignTwin', to_owner: 'ForeignQuote',
        basis_sha256: sha256(raw), headers_sha256: sha256(headers),
        evidence_sha256: sha256([ident, quote.amount, quote.asset, quote.network, 'quoted'].join('\x1f')),
        produced_by: 'bonfyre-project', recorded_at: stamp, ...quote, spend: 0,
        cannot_infer: ['a quote -> an intention to pay, a budget, or a fair price',
          'a quote -> the source honouring it later',
          'quoted -> committed, paid, settled or reconciled; those move money and this did not'] };
      write(`${JSON.stringify(record)}\n`); written++;
    }
    return { schema: 'bonfyre-quote-capture.v1', quotes_recorded: written, spend: 0,
      truth: 'prices_the_sources_published_when_asked_not_prices_anyone_agreed_to' };
  });
}

if (import.meta.main) {
  try {
    const [mode, ...args] = process.argv.slice(2);
    let summary;
    if (mode === 'grant-closure' && args.length === 3) {
      const report = grantClosure(args[0], args[1]);
      atomicReport(args[2], write => write(`${JSON.stringify(report, null, 2)}\n`));
      const { opportunities: _rows, ...rest } = report; summary = rest;
    } else if (mode === 'quote-capture' && args.length === 2) summary = captureQuotes(...args);
    else throw new Error('usage: economic-reports.mjs grant-closure GRANTS ESTATE OUT | quote-capture QUOTES OUT');
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) { console.error(`economic report: ${error.message}`); process.exitCode = 1; }
}
