import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

// main.c carries its organs through same-directory #includes now, so a source
// assertion that greps main.c alone misses behavior that still compiles in.
// This returns the translation unit: main.c first (its own matches keep
// priority), then each local header once, in include order. Generated
// ../generated/* headers stay out: they are compiler output, not source.
export function translationUnit(mainPath) {
  const root = readFileSync(mainPath, 'utf8');
  const dir = dirname(mainPath);
  const seen = new Set([resolve(dir, 'main.c')]);
  const parts = [root];
  for (const match of root.matchAll(/^[ \t]*#[ \t]*include[ \t]+"([^"]+)"/gm)) {
    const name = match[1];
    if (name.includes('/')) continue;
    const full = resolve(dir, name);
    if (seen.has(full) || !existsSync(full)) continue;
    seen.add(full);
    parts.push(`\n/* ===== ${name} (inlined for source assertions) ===== */\n` + readFileSync(full, 'utf8'));
  }
  return parts.join('\n');
}
