// Project exposes the three products as operations a ChatGPT or Claude user can
// run with their own data, instead of argv for native_tool_invoke: the bank
// export, the watch and the seller's listing travel in the call, and the answer
// is the booked lines, the digest and the manifest. The native products
// (cmd/BonfyreDecide) own the decisions; this file only shapes inputs and
// answers and keeps learned books.
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { spawnSync } from 'node:child_process';

const MAX_TEXT = 4_000_000;
const PROFILE = /^[a-z0-9][a-z0-9-]{0,63}$/;
const readOnly = { readOnlyHint: true, destructiveHint: false };
const columns = { type: 'object', additionalProperties: false, description: 'Which header is which, when a file could be read two ways.',
  properties: Object.fromEntries(['date', 'description', 'amount', 'debit', 'credit', 'category'].map(role => [role, { type: 'string' }])) };
const model = { type: 'string', description: 'Prefer this local model (a file name or part of one), e.g. qwen2.5-3b-instruct.' };

export const PRODUCTS_PROJECT_TOOLS = [
  { name: 'books_learn', description: 'Learn a client\'s bookkeeping from lines they have already categorised (CSV text with a description column and a Category or Account column, 20 lines or more) and keep it on this machine as a named profile. books_book then uses it.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['profile', 'history_csv'], properties: {
      profile: { type: 'string', description: 'Short name, lowercase letters, digits and dashes, e.g. acme-2026.' },
      history_csv: { type: 'string', description: 'The categorised history as CSV text.' }, columns } },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true } },
  { name: 'books_profiles', description: 'List the learned books profiles kept on this machine, with their accounts and how many lines each learned from.',
    inputSchema: { type: 'object', additionalProperties: false, properties: {} }, annotations: readOnly },
  { name: 'books_book', description: 'Book a bank or card export (CSV text) to a client\'s accounts. Lines the learned profile is sure of are booked; the rest are held for review with a suggestion. Amounts that do not read as money are kept as written and held, never booked as zero. Give a profile, or a chart of accounts (one per line, "Name: what goes there") to have the local model suggest every line for review. Nothing is posted to any ledger.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['transactions_csv'], properties: {
      transactions_csv: { type: 'string', description: 'The export as CSV text: a description column, and date and amount (or debit and credit) if present.' },
      profile: { type: 'string', description: 'A profile from books_learn.' },
      chart: { type: 'string', description: 'Chart of accounts, one per line, when there is no profile.' },
      columns, model,
      include_csv: { type: 'boolean', default: false, description: 'Also return every line as CSV.' } } },
    annotations: readOnly },
  { name: 'monitor_mentions', description: 'What the news and sources this installation already reads said about one business in recent days: items about it, with namesakes set aside, ranked by whether something needs doing. Uses the local model; if it cannot be loaded the answer says why.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['watch', 'about'], properties: {
      watch: { type: 'array', minItems: 1, maxItems: 8, items: { type: 'string' }, description: 'The name and any other names the business goes by.' },
      about: { type: 'string', description: 'One line saying which business is meant, e.g. "Target Corporation, the US retail chain".' },
      days: { type: 'integer', minimum: 1, maximum: 120, default: 30 },
      limit: { type: 'integer', minimum: 1, maximum: 80, default: 40 }, model } },
    annotations: readOnly },
  { name: 'manifest_lot', description: 'Turn a seller\'s listing text for an equipment lot into an itemised manifest: make, model, count, kind, whether it holds data to wipe, condition, and what to confirm with the seller. Also returns the ITAD desk import, which holds back any unit whose count was not stated rather than pricing it as one.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['listings'], properties: {
      listings: { type: 'array', minItems: 1, maxItems: 20, items: { type: 'object', additionalProperties: false, required: ['name', 'text'],
        properties: { name: { type: 'string' }, text: { type: 'string', description: 'The listing as text or HTML.' } } } },
      lot: { type: 'string', description: 'A name for the lot.' }, model } },
    annotations: readOnly },
].map(tool => ({ ...tool, 'x-bonfyre-owner': 'products' }));

const bounded = (value, name) => {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${name} is empty`);
  if (Buffer.byteLength(value) > MAX_TEXT) throw new Error(`${name} is larger than ${MAX_TEXT} bytes`);
  return value;
};

// The products are native (cmd/BonfyreDecide): each operation runs the
// program with the call's data in private temporary files and reads its JSON.
export function createProjectProducts({ root, home, env = process.env } = {}) {
  const program = name => [env.BONFYRE_DECIDE_BIN && join(env.BONFYRE_DECIDE_BIN, `bonfyre-${name}`),
    join(root, 'cmd/BonfyreDecide', `bonfyre-${name}`), join(home, '.bonfyre/bin', `bonfyre-${name}`)].find(path => path && existsSync(path));
  const run = (name, argv) => {
    const binary = program(name);
    if (!binary) throw new Error(`bonfyre-${name} is not built; run make -C cmd/BonfyreDecide`);
    const out = spawnSync(binary, argv, { env, encoding: 'utf8', maxBuffer: 256 << 20 });
    if (out.status !== 0) throw new Error((out.stderr || `bonfyre-${name} failed`).trim());
    return out.stdout;
  };
  const scratch = fn => { const dir = mkdtempSync(join(tmpdir(), 'bonfyre-products-')); try { return fn(dir); } finally { rmSync(dir, { recursive: true, force: true }); } };
  const put = (dir, name, text) => { const path = join(dir, name); writeFileSync(path, text, { mode: 0o600 }); return path; };
  const columnsArg = columns => columns && Object.keys(columns).length ? ['--columns', Object.entries(columns).map(([role, header]) => `${role}=${header}`).join(',')] : [];
  const modelArg = args => args.model ? ['--model', args.model] : [];
  const profiles = env.BONFYRE_BOOKS_PROFILES || join(home, '.bonfyre/estate-local/books');
  const profilePath = name => {
    if (typeof name !== 'string' || !PROFILE.test(name)) throw new Error('profile must be lowercase letters, digits and dashes (up to 64)');
    return join(profiles, `${name}.json`);
  };
  const cents = value => value === null ? '' : `${value < 0 ? '-' : ''}${Math.floor(Math.abs(value) / 100)}.${String(Math.abs(value) % 100).padStart(2, '0')}`;

  const operations = {
    async books_learn(args) {
      const history = bounded(args.history_csv, 'history_csv');
      const path = profilePath(args.profile);
      mkdirSync(profiles, { recursive: true, mode: 0o700 });
      const temporary = `${path}.${randomUUID()}.tmp`;
      scratch(dir => run('books', ['learn', '--history', put(dir, 'history.csv', history), '--out', temporary, ...columnsArg(args.columns)]));
      chmodSync(temporary, 0o600);
      renameSync(temporary, path);
      const learned = JSON.parse(readFileSync(path, 'utf8'));
      return { profile: args.profile, accounts: learned.accounts, learned_from_lines: learned.learned_from.lines,
        history_sha256: learned.learned_from.sha256, kept_at: path,
        cannot_infer: 'a learned profile -> correct books; it repeats how these lines were categorised, mistakes included' };
    },
    async books_profiles() {
      if (!existsSync(profiles)) return { profiles: [] };
      return { profiles: readdirSync(profiles).filter(name => name.endsWith('.json')).toSorted().map(name => {
        const path = join(profiles, name);
        try { const learned = JSON.parse(readFileSync(path, 'utf8'));
          return { profile: name.slice(0, -5), accounts: learned.accounts, learned_from_lines: learned.learned_from?.lines, updated: statSync(path).mtime.toISOString() }; }
        catch { return { profile: name.slice(0, -5), error: 'unreadable profile' }; }
      }) };
    },
    async books_book(args) {
      if (!args.profile && !args.chart) throw new Error('give a profile (from books_learn) or a chart of accounts');
      const learned = args.profile ? profilePath(args.profile) : null;
      if (learned && !existsSync(learned)) throw new Error(`no books profile named ${args.profile}; see books_profiles`);
      const { report, csv } = scratch(dir => {
        const argv = ['--transactions', put(dir, 'transactions.csv', bounded(args.transactions_csv, 'transactions_csv')),
          ...(learned ? ['--learned', learned] : ['--chart', put(dir, 'chart.txt', args.chart)]), ...columnsArg(args.columns), ...modelArg(args), '--json'];
        if (args.include_csv) argv.push('--out', join(dir, 'booked.csv'));
        const report = JSON.parse(run('books', argv));
        return { report, csv: args.include_csv ? readFileSync(join(dir, 'booked.csv'), 'utf8') : null };
      });
      const line = row => ({ line: row.line, date: row.date, description: row.description, amount: cents(row.amount_cents),
        ...(row.amount_unread ? { amount_as_given: row.amount_text } : {}), account: row.account || undefined, suggested: row.suggested,
        confidence: row.confidence === null ? null : Math.round(row.confidence * 100) / 100, decided_by: row.decided_by, ...(row.review_reason ? { reason: row.review_reason } : {}) });
      const held = report.transactions.filter(row => row.review);
      return { lines: report.lines, booked: report.by_learned, held_for_review: report.to_review, amounts_unread: report.amounts_unread,
        suggested_by_local_model: report.suggested_by_model, local_model: report.model, ...(report.model_error ? { local_model_unavailable: report.model_error } : {}),
        ...(report.checked_against_given_accounts ? { checked_against_given_accounts: report.checked_against_given_accounts } : {}),
        booked_by_account: report.booked_by_account, totals: report.totals, ...(report.totals_unavailable ? { totals_unavailable: report.totals_unavailable } : {}), held: held.slice(0, 200).map(line), ...(held.length > 200 ? { held_not_shown: held.length - 200 } : {}),
        booked_sample: report.transactions.filter(row => !row.review).slice(0, 20).map(line),
        ...(csv !== null ? { csv } : {}), seconds: report.seconds,
        cannot_infer: report.cannot_infer };
    },
    async monitor_mentions(args) {
      const names = args.watch.map(name => String(name).trim()).filter(Boolean);
      if (!names.length) throw new Error('watch needs at least one name');
      const { report, digest } = scratch(dir => {
        const report = JSON.parse(run('monitor', [...names.flatMap(name => ['--watch', name]), '--about', String(args.about || ''),
          '--days', String(args.days ?? 30), '--limit', String(args.limit ?? 40), ...modelArg(args), '--out', join(dir, 'digest.md'), '--json']));
        return { report, digest: readFileSync(join(dir, 'digest.md'), 'utf8') };
      });
      const { receipts, mentions, archive, ...summary } = report;
      return { ...summary, digest,
        mentions: mentions.map(({ receipt, observations, ...item }) => ({ ...item, sources: observations.length })) };
    },
    async manifest_lot(args) {
      const listings = args.listings.map(item => ({ name: String(item.name).slice(0, 120), text: bounded(item.text, `listing ${item.name}`) }));
      const files = scratch(dir => {
        const argv = listings.flatMap((item, i) => { const sub = join(dir, String(i)); mkdirSync(sub);
          return ['--listing', put(sub, basename(item.name) || 'listing', item.text)]; });
        const report = JSON.parse(run('manifest', [...argv, '--lot', args.lot || '', ...modelArg(args),
          '--out', join(dir, 'manifest.tsv'), '--itad', join(dir, 'itad.tsv'), '--json']));
        const held = join(dir, 'itad.held.tsv');
        return { report, manifest: readFileSync(join(dir, 'manifest.tsv'), 'utf8'), itad: readFileSync(join(dir, 'itad.tsv'), 'utf8'),
          held: existsSync(held) ? readFileSync(held, 'utf8') : 'line\tsource\twhy_held\tconfirm_with_seller\n' };
      });
      const { receipts, ...summary } = files.report;
      return { ...summary, rows: files.report.rows.map(({ receipt, ...row }) => row), manifest_tsv: files.manifest,
        itad_tsv: files.itad, held_tsv: files.held };
    },
  };
  return async (name, args) => {
    if (!Object.hasOwn(operations, name)) throw new Error('unknown product operation');
    return operations[name](args || {});
  };
}
