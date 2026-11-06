// The five skill instruments as one installable editor release. The same pages
// the public work samples build are rebound to an Aurekai destination: their
// assets become relative, the page carries the destination's record store,
// and each instrument saves under its own record prefix. Continuation views
// open a saved record in the editor that made it.
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { retainWorkspaceFiles } from '../../scripts/materialize_workspace_package.mjs';
import { continuationManifest, workspaceNavigation, workspaceNavigationCSS } from '../../scripts/workspace_continuation.mjs';
import { build } from './build.mjs';

export const INSTRUMENTS = [
  { page: 'queues', view: 'process', label: 'Process simulator', prefix: 'process-model:' },
  { page: 'research', view: 'review', label: 'Research desk', prefix: 'review-project:' },
  { page: 'schema', view: 'schema', label: 'Schema workspace', prefix: 'schema-plan:' },
  { page: 'plan', view: 'plan', label: 'Dependency planner', prefix: 'plan:' },
  { page: 'pricing', view: 'pricing', label: 'Pricing desk', prefix: 'price-work:' },
  { page: 'bids', view: 'opportunity', label: 'Opportunity desk', prefix: 'opportunity:' },
];
const walk = dir => readdirSync(dir).flatMap(n => { const p = join(dir, n); return statSync(p).isDirectory() ? walk(p) : [p]; });

export async function instrumentsPackage({ site = join(import.meta.dir, '../../packs/aurekai-work-samples/site'), packageRoot } = {}) {
  const out = mkdtempSync(join(tmpdir(), 'instruments-package-'));
  try {
    await build(site, out);
    const files = new Map();
    for (const it of INSTRUMENTS) {
      let html = readFileSync(join(out, it.page, 'index.html'), 'utf8');
      const data = html.match(new RegExp(`/data/${it.page}/(v-[a-f0-9]{64})/`));
      if (!data) throw Error(`${it.page} has no versioned package.`);
      // Assets move beside the editor; the site stylesheet and fonts move to the release root.
      html = html.replaceAll(`/data/${it.page}/${data[1]}/`, './').replaceAll('href="/assets/', 'href="../assets/').replaceAll('src="/assets/', 'src="../assets/')
        .replace(/href="\/#[^"]*"/g, 'href="../"').replace(/href="\/(?!\/)([^"]*)"/g, (m, p) => `href="../${p}"`);
      html = html.replace('</head>', `<meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}</style></head>`)
        .replace(/(<body[^>]*>)/, '$1' + workspaceNavigation);
      files.set(`${it.page}/index.html`, Buffer.from(html));
      const dir = join(out, 'data', it.page, data[1]);
      for (const f of walk(dir)) files.set(`${it.page}/${relative(dir, f)}`, readFileSync(f));
    }
    for (const f of walk(join(out, 'assets')).filter(f => /\.(css|woff2?)$/.test(f))) files.set(`assets/${relative(join(out, 'assets'), f)}`, readFileSync(f));
    for (const name of ['workspace-store.mjs', 'workspace-msgpack.mjs', 'workspace-value.mjs', 'workspace-continuation.mjs']) files.set('bridge/' + name, readFileSync(join(import.meta.dir, '../../integrations/browser', name)));
    files.set('instruments/index.html', Buffer.from(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Instruments</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><style>${workspaceNavigationCSS}body{margin:0;background:#0d130f;color:#dde6da;font:16px 'DM Sans',system-ui}main{padding:28px}a{color:#b5df32;display:block;font-size:22px;margin:12px 0}</style></head><body>${workspaceNavigation}<main><h1>Instruments</h1>${INSTRUMENTS.map(i => `<a href="../${i.page}/">${i.label}</a>`).join('')}</main></body></html>`));
    files.set('CONTINUATIONS.tsv', continuationManifest(INSTRUMENTS.map(i => ({ view: i.view, label: i.label, entry: `${i.page}/`, prefix: i.prefix, title: 'name', query: 'record=$key' })), files));
    return { ...await retainWorkspaceFiles(files, packageRoot ? { root: packageRoot } : {}), entry: 'instruments/', family: 'skill-instruments', records: INSTRUMENTS.map(i => i.prefix) };
  } finally { rmSync(out, { recursive: true, force: true }); }
}
if (import.meta.main) console.log(JSON.stringify(await instrumentsPackage({ packageRoot: process.argv[2] })));
