import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
export async function buildWorkbookMcpApp(file = new URL('../../integrations/mcp/dist/workbook-review.html', import.meta.url)) {
  const build = await Bun.build({ entrypoints: [fileURLToPath(new URL('./workbook-mcp-app.mjs', import.meta.url))], target: 'browser', minify: true });
  if (!build.success || build.outputs.length !== 1) throw new AggregateError(build.logs, 'Financial MCP application could not build.');
  const [template, grid, license, code] = await Promise.all([
    readFile(new URL('./workbook-mcp-app.html', import.meta.url), 'utf8'),
    readFile(new URL('./vendor/ag-grid-community-36.2.0/ag-grid-community.min.js', import.meta.url), 'utf8'),
    readFile(new URL('./vendor/ag-grid-community-36.2.0/LICENSE.txt', import.meta.url), 'utf8'), build.outputs[0].text()
  ]);
  const script = source => source.replace(/<\/script/gi, '<\\/script');
  // A function replacement: a string one would read $' $` $& inside the
  // minified bundles as substitution patterns and splice page text into code.
  const html = template.replace('<!-- WORKBOOK_APP_SCRIPTS -->', () => `<script>/* AG Grid Community license\n${license.replaceAll('*/', '* /')}\n*/\n${script(grid)}</script><script type="module">${script(code)}</script>`);
  if (Buffer.byteLength(html) > 4 * 1024 * 1024) throw Error('Financial MCP application exceeds its resource limit.');
  const path = file instanceof URL ? fileURLToPath(file) : file;
  await mkdir(dirname(path), { recursive: true }); await writeFile(path, html);
  return { path, bytes: Buffer.byteLength(html), sha256: createHash('sha256').update(html).digest('hex') };
}
if (import.meta.main) console.log(JSON.stringify(await buildWorkbookMcpApp(process.argv[2])));
