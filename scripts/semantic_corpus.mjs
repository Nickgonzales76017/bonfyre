#!/usr/bin/env bun
// Bounded text projection for the existing embedding owner. No semantic claims.
import { Database } from 'bun:sqlite';
import { createWriteStream, renameSync, unlinkSync } from 'node:fs';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';

export async function compileCorpus(archive, output, limit) {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error('limit must be a positive integer');
  const db = new Database(archive, { readonly: true });
  const pending = `${output}.pending-${process.pid}-${randomUUID()}`;
  const out = createWriteStream(pending, { flags: 'wx', mode: 0o600 });
  let count = 0;
  try {
    const rows = db.query("SELECT source, key, title, COALESCE(publisher,'') AS publisher, COALESCE(substr(body,1,400),'') AS body FROM item WHERE title <> '' ORDER BY at DESC LIMIT ?").iterate(limit);
    for (const row of rows) {
      const line = JSON.stringify({ id: `o/${row.source}/${row.key}`,
        text: [row.title, row.publisher, row.body].filter(Boolean).join(' ').trim() }) + '\n';
      // Backpressure requires this producer to wait before reading more rows.
      if (!out.write(line)) {
        // eslint-disable-next-line no-await-in-loop
        await once(out, 'drain');
      }
      count++;
    }
    out.end();
    await once(out, 'finish');
    renameSync(pending, output);
    return { corpus: count };
  } catch (error) {
    out.destroy();
    try { unlinkSync(pending); } catch {}
    throw error;
  } finally { db.close(); }
}

if (import.meta.main) {
  const [archive, output, rawLimit] = process.argv.slice(2);
  if (!archive || !output || !rawLimit) throw new Error('usage: semantic_corpus.mjs ITEMS_DB CORPUS_JSONL LIMIT');
  const result = await compileCorpus(archive, output, Number(rawLimit));
  console.log(JSON.stringify(result));
}
