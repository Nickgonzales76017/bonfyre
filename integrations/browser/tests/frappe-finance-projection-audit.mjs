// Cross-app qualification audit. Existing native compiler/CMS, scratch state only.
import { mkdtempSync, readFileSync, rmSync, existsSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { Database } from 'bun:sqlite';
const root = process.env.BONFYRE_ROOT || resolve(import.meta.dir, '../../..');
const compiler = process.env.BONFYRE_FRAPPE_COMPILER_BIN || join(root, 'cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler');
const cms = process.env.BONFYRE_CMS_BIN || join(root, 'cmd/BonfyreCMS/bonfyre-cms');
const cases = [
  ['frappe', 'DocType'], ['crm', 'CRM Deal'], ['helpdesk', 'HD Ticket'],
  ['erpnext', 'Sales Order'], ['hrms', 'Attendance'], ['lms', 'LMS Course'],
  ['wiki', 'Wiki Document'], ['insights', 'Insights Query'], ['drive', 'Drive Permission'],
  ['erpnext', 'Supplier Quotation'], ['erpnext', 'Quotation'],
  ['erpnext', 'Request for Quotation'], ['erpnext', 'Purchase Order'],
  ['erpnext', 'Sales Invoice'], ['erpnext', 'Purchase Invoice'],
  ['erpnext', 'Payment Entry'], ['erpnext', 'Payment Request'],
  ['erpnext', 'Bank Transaction'], ['erpnext', 'Shipment'],
  ['erpnext', 'Landed Cost Voucher'], ['hrms', 'Salary Slip'],
  ['hrms', 'Expense Claim'], ['hrms', 'Employee Advance'],
];
const scratch = mkdtempSync(join(tmpdir(), 'aurekai-frappe-finance-audit-'));
const started = performance.now();
const report = {
  schema: 'aurekai.frappe-finance-projection-audit.v1', observed_at: new Date().toISOString(),
  compiler, cms, scope: 'compiled metadata to scratch native CMS storage',
  bank_access: false, external_requests: 0, production_writes: 0,
  lending_checkout_present: existsSync(join(root, 'integrations/frappe-bench/apps/lending')), records: [],
};
function run(binary, args) {
  const result = Bun.spawnSync([binary, ...args], { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 30000 });
  return { code: result.exitCode, out: result.stdout.toString(), err: result.stderr.toString() };
}
try {
  for (const [app, doctype] of cases) {
    const file = join(scratch, `${app}-${doctype.replace(/\W/g, '_')}.json`);
    const result = run(compiler, ['--root', root, '--phase', '1', '--emit-cms-schema', file,
      '--cms-doctype', doctype, '-o', join(scratch, 'generated'), join(root, 'integrations/frappe-bench/apps', app)]);
    if (result.code !== 0 || !existsSync(file)) {
      report.records.push({ app, doctype, state: 'compiler_refused', exit_code: result.code, detail: result.err.slice(-1500) });
      continue;
    }
    const schema = JSON.parse(readFileSync(file, 'utf8'));
    if (!/^[a-zA-Z0-9_]+$/.test(schema.name)) throw Error('Unexpected generated table name');
    const declared = Object.keys(schema.attributes);
    const dbpath = join(scratch, `${report.records.length}.db`);
    const apply = run(cms, ['schema', 'apply', file, '--db', dbpath]);
    if (apply.code !== 0) {
      report.records.push({ app, doctype, state: 'cms_refused', exit_code: apply.code, declared: declared.length, detail: apply.err.slice(-1500) });
      continue;
    }
    const db = new Database(dbpath, { readonly: true, create: false });
    let columns;
    try { columns = db.query(`PRAGMA table_info("${schema.name}")`).all(); }
    finally { db.close(); }
    const names = new Set(columns.map(row => row.name));
    const missing = declared.filter(name => !names.has(name));
    const references = declared.filter(name => schema.attributes[name].frappe_reference_doctype);
    report.records.push({ app, doctype, table: schema.name,
      state: missing.length ? 'silent_field_loss' : 'projected_fields_preserved',
      declared: declared.length, preserved: declared.length - missing.length, missing,
      reference_fields: references.length,
      references_are_strings: references.every(name => schema.attributes[name].type === 'string'),
      decimal_fields_stored_as_real: columns.filter(row => schema.attributes[row.name]?.type === 'decimal' && row.type === 'REAL').map(row => row.name),
      projection_notice: schema.bonfyre?.forbidden_inference ?? null, apply_exit: apply.code,
    });
  }
  report.summary = {
    requested: cases.length, preserved: report.records.filter(row => row.state === 'projected_fields_preserved').length,
    silent_loss: report.records.filter(row => row.state === 'silent_field_loss').length,
    refused: report.records.filter(row => row.state.endsWith('_refused')).length,
    fields_missing: report.records.reduce((sum, row) => sum + (row.missing?.length ?? 0), 0),
    elapsed_ms: performance.now() - started,
  };
  const text = JSON.stringify(report, null, 2) + '\n';
  const out = process.argv.indexOf('--out');
  if (out !== -1) {
    const destination = resolve(process.argv[out + 1] || '');
    if (!destination.startsWith(join(root, 'packs') + '/')) throw Error('Audit output must be campaign data under packs');
    writeFileSync(destination, text, { flag: 'wx', mode: 0o600 });
  }
  console.log(text);
  process.exitCode = report.summary.silent_loss || report.summary.refused ? 2 : 0;
} finally { rmSync(scratch, { recursive: true, force: true }); }
