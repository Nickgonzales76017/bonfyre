import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

export async function buildControlPlaneApp({ file = new URL('./dist/project-control-plane.html', import.meta.url), stdout = false } = {}) {
  const extApps = fileURLToPath(new URL('../../components/BonfyreServiceSite/node_modules/@modelcontextprotocol/ext-apps/dist/src/app-with-deps.js', import.meta.url));
  const build = await Bun.build({
    entrypoints: [fileURLToPath(new URL('./control_plane_app.mjs', import.meta.url))],
    target: 'browser',
    minify: true,
    plugins: [{ name: 'bonfyre-existing-ext-apps', setup(builder) {
      builder.onResolve({ filter: /^@modelcontextprotocol\/ext-apps$/ }, () => ({ path: extApps }));
    } }],
  });
  if (!build.success || build.outputs.length !== 1) throw new AggregateError(build.logs, 'Project control-plane MCP application could not build.');
  const [template, code] = await Promise.all([readFile(new URL('./control_plane_app.html', import.meta.url), 'utf8'), build.outputs[0].text()]);
  const script = source => source.replace(/<\/script/gi, '<\\/script');
  const html = template.replace('<!-- CONTROL_PLANE_APP_SCRIPT -->', () => `<script type="module">${script(code)}</script>`).replace(/^[ \\t]+$/gm, '');
  if (Buffer.byteLength(html) > 512 * 1024) throw Error('Project control-plane application exceeds its resource limit.');
  if (!stdout) { const path = file instanceof URL ? fileURLToPath(file) : file; await mkdir(dirname(path), { recursive: true }); await writeFile(path, html); }
  return { html, bytes: Buffer.byteLength(html), sha256: createHash('sha256').update(html).digest('hex') };
}

if (import.meta.main) {
  const result = await buildControlPlaneApp({ stdout: process.argv.includes('--stdout') });
  if (process.argv.includes('--stdout')) process.stdout.write(result.html);
  else console.log(JSON.stringify({ bytes: result.bytes, sha256: result.sha256 }));
}
