// The planner's package: its code and a sample plan made from this estate's own
// open work, read from the handoff ledger of promised instruments, so the
// example is a plan somebody actually has to finish. Estimates are stated
// assumptions, labelled as such on the page.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { problems } from './plan-network.mjs';

export function samplePlan(ledgerPath) {
  const [head, ...rows] = readFileSync(ledgerPath, 'utf8').trim().split('\n'), cols = head.split('\t');
  const open = rows.map(r => Object.fromEntries(r.split('\t').map((v, i) => [cols[i], v]))).filter(r => !r.operation_implemented);
  const tasks = [
    { id: 'publish', name: 'Publish the work samples', optimistic: 0.1, likely: 0.5, pessimistic: 2, deps: [], owner: 'Nick', note: 'Push the site record, then publish; the agent is not allowed to deploy.' },
    { id: 'verify-live', name: 'Check the public pages in a browser', optimistic: 0.2, likely: 0.5, pessimistic: 1, deps: ['publish'], owner: 'Agent' },
    { id: 'credentials', name: 'Install the account-scoped credential store', optimistic: 0.1, likely: 0.3, pessimistic: 1, deps: [], owner: 'Nick' },
    { id: 'extension', name: 'Reload the browser extension and register its host', optimistic: 0.1, likely: 0.3, pessimistic: 1, deps: [], owner: 'Nick' },
    { id: 'declare', name: 'Declare key fields for the first 20 providers', optimistic: 1, likely: 2, pessimistic: 4, deps: [], owner: 'Agent' },
    { id: 'enroll-20', name: 'Work the first 20 accounts, both identities', optimistic: 2, likely: 4, pessimistic: 9, deps: ['credentials', 'extension', 'declare'], owner: 'Nick' },
    { id: 'verify-keys', name: 'Verify the kept keys with each provider', optimistic: 1, likely: 2, pessimistic: 4, deps: ['enroll-20'], owner: 'Agent' },
    { id: 'native-save', name: 'Native save and continuation for the workspaces', optimistic: 3, likely: 5, pessimistic: 10, deps: ['verify-live'], owner: 'Agent' },
    { id: 'review-40', name: 'Read the next 40 source skills in depth', optimistic: 3, likely: 5, pessimistic: 10, deps: [], owner: 'Agent' },
    ...open.map((r, i) => ({ id: r.id, name: `Build: ${r.title}`, optimistic: 2, likely: 4, pessimistic: 9, deps: i ? [open[i - 1].id] : [], owner: 'Agent', note: `Promised in the 2026-10-19 handoff; now ${r.state}.` })),
    { id: 'batch-2', name: 'Build the second batch of skill instruments', optimistic: 8, likely: 12, pessimistic: 25, deps: ['review-40', ...(open.length ? [open.at(-1).id] : [])], owner: 'Agent' },
  ];
  const found = problems(tasks); if (found.length) throw Error(found[0]);
  return { name: 'Finishing the 19 October handoffs', start: '2026-10-20', seed: 1, tasks,
    assumed: 'The tasks are this estate\'s real open work, read from its handoff ledger. The day estimates are starting assumptions; change them.' };
}

export function planFiles(ledgerPath) {
  const hash = body => createHash('sha256').update(body).digest('hex');
  const files = { 'sample-plan.json': Buffer.from(JSON.stringify(samplePlan(ledgerPath))) };
  for (const name of ['plan-network.mjs', 'plan-network-client.mjs', 'instrument-store.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: { tasks: JSON.parse(files['sample-plan.json']).tasks.length }, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
