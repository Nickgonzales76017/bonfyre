// Read-only gate before removing/rebuilding a public asset output directory.
import { existsSync, realpathSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";

function physical(path) {
  let folder=resolve(path), suffix=[];
  while(!existsSync(folder) && folder!==dirname(folder)) {
    suffix.unshift(basename(folder));
    folder=dirname(folder);
  }
  return resolve(realpathSync(folder),...suffix);
}
const within = (part,whole) => part===whole || part.startsWith(whole+sep);
export function guardEdgePaths({export: source,out,maxFiles=20000}, repoRoot) {
  if (!source || !out) throw Error("--export and --out are required");
  if (!Number.isSafeInteger(maxFiles) || maxFiles<1 || maxFiles>20000)
    throw Error("--max-files must be an integer from 1 to 20000");
  const src=physical(source),target=physical(out),repo=physical(repoRoot);
  if (within(repo,target))
    throw Error("public output would delete repository root or an ancestor");
  if (within(src,target) || within(target,src))
    throw Error("public output and export source overlap; refusing destructive rebuild");
  if (!existsSync(join(src,"hosts")))
    throw Error("no hosts/ in "+source);
  return {source:src,out:target,maxFiles};
}
