// Publication metadata only: the authoritative owners still build every table.
import { closeSync, fsyncSync, lstatSync, openSync, readdirSync, renameSync,
  unlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

export function edgeCatalogManifest(directory, now = new Date()) {
  const tables = readdirSync(directory).filter(name => !name.startsWith('.') && name.endsWith('.parquet'))
    .sort().flatMap(file => {
      const stat = lstatSync(join(directory, file));
      // Do not describe directories or follow links outside the publication cut.
      if (!stat.isFile()) return [];
      if (!Number.isSafeInteger(stat.size)) throw new Error('catalogue file size exceeds JSON integer precision');
      return [{ name: file.slice(0, -8), file, bytes: stat.size }];
    });
  return { schema: 'bonfyre-edge-catalog.v1', published: now.toISOString().replace(/\.\d{3}Z$/, 'Z'), tables,
    cannot_infer: 'every row points at a page this estate already composed; the catalogue chooses among them and is never a second source of truth. A score between two things is a measurement of two texts, not a shared subject. A distance is between two stated points and establishes no relation.' };
}

export function publishEdgeCatalogManifest(directory) {
  const bytes = `${JSON.stringify(edgeCatalogManifest(directory), null, 2)}\n`;
  const temporary = join(directory, `.catalog-${randomUUID()}.tmp`);
  const fd = openSync(temporary, 'wx', 0o644);
  try { writeFileSync(fd, bytes); fsyncSync(fd); }
  catch (error) { closeSync(fd); unlinkSync(temporary); throw error; }
  closeSync(fd);
  try { renameSync(temporary, join(directory, 'catalog.json')); }
  catch (error) { unlinkSync(temporary); throw error; }
}

if (import.meta.main) {
  try {
    if (process.argv.length !== 3) throw new Error('usage: edge-catalog-manifest.mjs DIRECTORY');
    publishEdgeCatalogManifest(process.argv[2]);
  } catch (error) { console.error(`catalogue manifest: ${error.message}`); process.exitCode = 1; }
}
