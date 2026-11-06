// Opt-in witness for one marked enquiry on the estate's own work-samples site.
// Reuses cloud, lead sync, enrichment and compiled CMS owners. Never sends email.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdtempSync, readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { plannerFiles } from '../../../components/BonfyreServiceSite/planner-material.mjs';
import { validatePlan, assessPlan } from '../../../components/BonfyreServiceSite/planner-model.mjs';
import { heldLeads, dropLead } from '../../../components/BonfyreServiceSite/cloud.mjs';
import { syncLeads } from '../../../components/BonfyreServiceSite/leads.mjs';
import { onLead } from '../../../packs/aurekai-work-samples/site/lead.mjs';

if (!process.argv.includes('--live')) throw Error('Pass --live to submit one marked test enquiry.');
const repo = resolve(import.meta.dir, '../../..');
const origin = 'https://aurekai-work-samples.pages.dev', form = 'planner-red3-fixtures';
const runId = crypto.randomUUID(), marker = 'Aurekai verification ' + runId.slice(0, 8);
const output = process.argv.includes('--out') ? resolve(process.argv[process.argv.indexOf('--out') + 1])
  : mkdtempSync(join(tmpdir(), 'aurekai-planner-handoff-'));
mkdirSync(output, { recursive: true });
const evidence = { schema: 'aurekai.planner.live-handoff.v1', run_id: runId, marker,
  at: new Date().toISOString(), origin, status: 'started', receipt: null, checks: [], external_emails: 0 };
const record = () => writeFileSync(join(output, 'receipt.json'), JSON.stringify(evidence, null, 2) + '\n');
const check = (name, condition) => { assert.ok(condition, name); evidence.checks.push(name); record(); };
const db = join(homedir(), '.bonfyre/estate-local/cms.db');
const cms = args => {
  const result = Bun.spawnSync([join(homedir(), '.bonfyre/bin/bonfyre-cms'), ...args, '--db', db], { stdout: 'pipe', stderr: 'pipe' });
  if (result.exitCode !== 0) throw Error('CMS operation failed: ' + result.stderr.toString().slice(0, 500));
  const value = JSON.parse(result.stdout.toString()); if (value.error) throw Error(value.error); return value;
};
function rows(type) {
  const result = cms(['entry', 'list', type, '--ns', 'service-site', '--limit', '5000']);
  if (!Array.isArray(result.data) || result.data.length >= 5000) throw Error('Incomplete CMS listing.');
  return result.data.map(row => ({ ...row.attributes, _id: row.id }));
}
const siteRow = rows('service_site').find(row => row.site_id === 'aurekai-work-samples');
if (!siteRow) throw Error('Work-samples site is not registered.');
const site = JSON.parse(siteRow.content_json);
if (site.pages_project !== 'aurekai-work-samples' || site.cloud_account || site.leads !== 'server')
  throw Error('Unexpected work-samples deployment scope.');
const local = plannerFiles(JSON.parse(readFileSync(join(repo, 'packs/aurekai-work-samples/site/fixture-planner.json'), 'utf8'))).catalogue;
const puppeteer = createRequire(join(repo, 'tools/creator-ingest/package.json'))('puppeteer');
const profile = mkdtempSync(join(tmpdir(), 'aurekai-planner-live-browser-'));
let browser, requestBody, posts = [], knownLead = null;
record();
try {
  browser = await puppeteer.launch({ headless: true, executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', userDataDir: profile });
  const page = await browser.newPage(); page.setDefaultTimeout(12000);
  await page.setViewport({ width: 1440, height: 1050 });
  page.on('request', request => { if (request.method() === 'POST') posts.push(request.url()); });
  await page.goto(origin + '/red3-planner/', { waitUntil: 'networkidle2', timeout: 30000 });
  await page.waitForSelector('[data-planner][data-ready=true]');
  const cfg = await page.$eval('[data-planner-config]', node => JSON.parse(node.textContent));
  check('Published catalogue matches the native enrichment release', cfg.catalogue.digest === local.digest && cfg.form === form);
  const fill = async (name, value) => { await page.click(`[data-ui="${name}"]`, { clickCount: 3 }); await page.keyboard.press('Backspace'); await page.type(`[data-ui="${name}"]`, value); };
  await fill('title', marker);
  await fill('notes', 'TEST ONLY. Do not contact the merchant or any customer. Check complete selection and requirements. ' + runId);
  await page.click('[data-ui=save]');
  await page.waitForFunction(() => document.querySelector('[data-ui=save-state]').textContent === 'Saved in this browser');
  await page.click('[data-ui=review]');
  await page.type('[data-ui=name]', marker); await page.type('[data-ui=email]', 'verification@example.invalid');
  const pending = page.waitForResponse(response => response.url() === origin + '/api/lead/' + form && response.request().method() === 'POST');
  evidence.status = 'submitting'; record();
  await page.click('[data-ui=submit]');
  const response = await pending, body = await response.json();
  requestBody = JSON.parse(response.request().postData());
  evidence.http_status = response.status(); evidence.response = body;
  evidence.receipt = /^lead:planner:[a-f0-9]{64}$/.test(body.receipt ?? '') ? body.receipt : null; record();
  check('Live intake returns a durable receipt', response.ok() && body.ok === true && Boolean(evidence.receipt));
  const plan = validatePlan(JSON.parse(requestBody.fields.find(field => field.label === 'Fixture plan').value), local);
  check('Submitted plan is explicitly marked and source-valid', plan.title === marker && plan.notes.includes(runId));
  check('Browser sent exactly one enquiry to the intended endpoint', posts.length === 1 && posts[0] === origin + '/api/lead/' + form);
  evidence.status = 'received'; evidence.plan = plan; record();
  await page.waitForFunction(() => document.querySelector('[data-ui=receipt]').textContent.includes('Received by Aurekai'));
  await page.screenshot({ path: join(output, 'received.png'), fullPage: true });
  await page.reload({ waitUntil: 'networkidle2' }); await page.waitForSelector('[data-ready=true]'); await page.click('[data-ui=review]');
  check('Reload restores the receipt without another POST', posts.length === 1 && (await page.$eval('[data-ui=receipt]', node => node.textContent)).includes(evidence.receipt));
  for (let attempt = 0; attempt < 4 && !knownLead; attempt++) {
    knownLead = (await heldLeads(site.pages_project, site.cloud_account)).find(lead => lead.key === evidence.receipt);
    if (!knownLead) await Bun.sleep(1500);
  }
  check('Cloud owner reads back this exact stored enquiry', knownLead?.site_id === site.site_id && knownLead.form === form);
  check('All selected items and customer requirements survived storage', JSON.stringify(knownLead.fields) === JSON.stringify(requestBody.fields));
  // Filter readbacks so this witness cannot prepare unrelated pending messages.
  const scopedCms = args => {
    if (args[0] === 'entry' && args[1] === 'list') {
      const value = cms(args), type = args[2];
      if (value.data.length >= 5000) throw Error('Incomplete witness readback.');
      return { data: value.data.filter(row => type === 'service_lead' ? row.attributes.lead_key === evidence.receipt
        : type === 'outbound_message' && row.attributes.message_id === 'service-site:' + evidence.receipt) };
    }
    if (args[0] !== 'entry' || args[1] !== 'create') throw Error('Unexpected witness mutation.');
    const data = JSON.parse(args.at(-1));
    if (args[2] === 'service_lead' ? data.lead_key !== evidence.receipt : args[2] !== 'outbound_message' || data.message_id !== 'service-site:' + evidence.receipt)
      throw Error('Refusing an unrelated record.');
    return cms(args);
  };
  const options = { sites: [site], cms: scopedCms, heldLeads: async () => [knownLead], enrich: (_, lead) => onLead(lead) };
  evidence.first_sync = await syncLeads(options); evidence.second_sync = await syncLeads(options); record();
  check('Repeated native sync creates no extra message', !evidence.first_sync.errors.length && !evidence.second_sync.errors.length && evidence.second_sync.prepared === 0);
  const leads = rows('service_lead').filter(row => row.lead_key === evidence.receipt);
  const messages = rows('outbound_message').filter(row => row.message_id === 'service-site:' + evidence.receipt);
  check('Exactly one native lead and held message exist', leads.length === 1 && messages.length === 1);
  const facts = JSON.parse(leads[0].facts_json);
  check('Native assessment matches the actual selected options', facts.listed_subtotal_cents === assessPlan(plan, local).subtotal && facts.item_count === plan.items.length);
  check('Human-readable handoff preserves the test requirements', leads[0].brief.includes(runId) && messages[0].body.includes(runId));
  check('No merchant contact, quote or order is inferred', facts.supplier_contacted === false && facts.quote_issued === false && facts.order_placed === false);
  evidence.native = { lead_id: leads[0]._id, message_id: messages[0]._id, facts };
  evidence.status = 'passed'; record();
} catch (error) { evidence.status = 'failed'; evidence.error = error.message; record(); process.exitCode = 1; }
finally {
  if (browser) await browser.close(); rmSync(profile, { recursive: true, force: true });
  if (evidence.receipt) try {
    for (const row of rows('service_lead').filter(row => row.lead_key === evidence.receipt)) cms(['entry', 'update', 'service_lead', String(row._id), '--ns', 'service-site', JSON.stringify({ handled: 'yes' })]);
    for (const row of rows('outbound_message').filter(row => row.message_id === 'service-site:' + evidence.receipt)) cms(['entry', 'update', 'outbound_message', String(row._id), '--ns', 'service-site', JSON.stringify({ state: 'abandoned', state_reason: 'Marked live verification ' + runId + '. Do not contact. Evidence: ' + output })]);
    await dropLead(site.pages_project, site.cloud_account, evidence.receipt);
    evidence.cleanup = 'Test lead handled, test message abandoned, exact remote test copy removed; quota counters unchanged.';
  } catch (error) { evidence.cleanup_error = error.message; process.exitCode = 1; }
  record(); console.log(JSON.stringify({ ...evidence, plan: undefined, response: undefined, output }));
}
