import { createHash } from "node:crypto";
import { createReadStream, lstatSync, readdirSync } from "node:fs";
import { join, relative, resolve, sep } from "node:path";

async function fileDigest(path) {
  const before = lstatSync(path, { bigint: true });
  if (!before.isFile() || before.isSymbolicLink()) throw new Error(`body member is not a regular file: ${path}`);
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path, { highWaterMark: 4 * 1024 * 1024 })) hash.update(chunk);
  const after = lstatSync(path, { bigint: true });
  if (before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ino !== after.ino)
    throw new Error(`body changed while hashing: ${path}`);
  return { digest: hash.digest("hex"), bytes: Number(after.size) };
}

// A directory body's identity is the sorted path:digest manifest, not its first part.
export async function bodyIdentity(path) {
  const root = resolve(path), body = lstatSync(root);
  if (body.isSymbolicLink()) throw new Error(`body is a symlink: ${root}`);
  if (body.isFile()) return { ...await fileDigest(root), members: 1 };
  if (!body.isDirectory()) throw new Error(`body is not a regular file or directory: ${root}`);
  const files = [], pending = [root];
  while (pending.length) {
    const dir = pending.pop();
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const member = join(dir, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`body member is a symlink: ${member}`);
      if (entry.isDirectory()) pending.push(member);
      else if (entry.isFile()) files.push(member);
      else throw new Error(`body member is not a regular file: ${member}`);
    }
  }
  files.sort((a, b) => relative(root, a) < relative(root, b) ? -1 : relative(root, a) > relative(root, b) ? 1 : 0);
  let bytes = 0;
  const manifest = [];
  for (const file of files) {
    const member = await fileDigest(file);
    bytes += member.bytes;
    manifest.push(`${relative(root, file).split(sep).join("/")}:${member.digest}`);
  }
  return { digest: createHash("sha256").update(manifest.join("\n"), "utf8").digest("hex"),
    bytes, members: files.length };
}

export async function bodyDigest(path) { return (await bodyIdentity(path)).digest; }
