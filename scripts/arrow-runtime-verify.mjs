// CUE-free native runtime verification shared by bind and read-only serving.
import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";

const operations = ["bonfyre-arrow-stream", "bonfyre-arrow-flight",
  "libbonfyre_arrow_c.dylib"];
const libraries = ["arrow", "parquet", "flight", "simdjson"];
const licenses = ["APACHE-ARROW-LICENSE.txt", "APACHE-ARROW-NOTICE.txt",
  "SIMDJSON-LICENSE.txt"];

export async function hashFile(path) {
  const hash = createHash("sha256");
  for await (const chunk of Bun.file(path).stream()) hash.update(chunk);
  return hash.digest("hex");
}

function manifestAt(runtime) {
  const manifestPath = join(runtime, "manifest.json");
  if (!existsSync(manifestPath)) throw new Error(`missing native runtime manifest: ${manifestPath}`);
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (manifest.kind !== "bonfyre-arrow-native-runtime" || manifest.format !== 1)
    throw new Error("unsupported Arrow native runtime manifest");
  for (const required of operations)
    if (!manifest.binaries?.[required]) throw new Error(`missing native operation: ${required}`);
  for (const required of libraries)
    if (!manifest.libraries?.[required]) throw new Error(`missing native library: ${required}`);
  return manifest;
}

function checkedFile(path, record, type, name) {
  if (!record || !Number.isSafeInteger(record.bytes) || record.bytes < 1 ||
      !/^[0-9a-f]{64}$/.test(record.sha256))
    throw new Error(`invalid native ${type} manifest: ${name}`);
  if (!existsSync(path)) throw new Error(`native ${type} drift: ${name}`);
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== record.bytes)
    throw new Error(`native ${type} drift: ${name}`);
}

// Strict bind/service admission hashes every installed byte. Stream can use
// the receipt-bound fast mode; the native IPC process separately hashes its
// selected Parquet source before emitting any Arrow bytes.
async function verify(runtime, operation, strict) {
  const manifest = manifestAt(runtime);
  if (operation && !operations.includes(operation)) throw new Error(`unknown native operation: ${operation}`);
  for (const [name, record] of Object.entries(manifest.binaries ?? {})) {
    if (name !== basename(name) || name === "." || name === "..")
      throw new Error(`invalid native operation coordinate: ${name}`);
    const path = join(runtime, name);
    checkedFile(path, record, "binary", name);
    if ((strict || name === operation) && await hashFile(path) !== record.sha256)
      throw new Error(`native binary drift: ${name}`);
  }
  for (const [name, record] of Object.entries(manifest.libraries ?? {})) {
    if (!record.file || record.file !== String(record.file).split(/[\\/]/).at(-1))
      throw new Error(`invalid native library coordinate: ${name}`);
    const path = join(runtime, "lib", record.file);
    checkedFile(path, record, "library", name);
    if (strict && await hashFile(path) !== record.sha256)
      throw new Error(`native library drift: ${name}`);
  }
  for (const required of licenses) {
    if (!existsSync(join(runtime, "licenses", required)))
      throw new Error(`missing native license: ${required}`);
  }
  return manifest;
}

export const verifyArrowRuntime = runtime => verify(runtime, null, true);
export const verifyArrowRuntimeFast = (runtime, operation) => {
  if (!operations.includes(operation)) throw new Error(`unknown native operation: ${operation}`);
  return verify(runtime, operation, false);
};
