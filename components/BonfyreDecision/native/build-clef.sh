#!/bin/sh
# Build the pinned complete Clef backbone/head. Never downloads model weights.
# Supply a verified source archive to build offline. Output must not yet exist;
# failed builds remain inspectable and are never silently reused or overwritten.
set -eu
set -f
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf '%s\n' 'usage: build-clef.sh NEW_OUTPUT_DIRECTORY [SOURCE_ARCHIVE]' >&2
    exit 2
fi
out=$1
archive=${2:-}
case "$out" in /*) ;; *) printf '%s\n' 'output directory must be absolute' >&2; exit 2 ;; esac
if [ -e "$out" ] || [ -L "$out" ]; then
    printf '%s\n' 'output already exists; refusing to overwrite or reuse unverified build state' >&2
    exit 2
fi
for tool in bun cmake curl tar shasum patch; do
    command -v "$tool" >/dev/null 2>&1 || { printf 'missing build tool: %s\n' "$tool" >&2; exit 2; }
done
if [ "$(uname -s)" != Darwin ]; then
    printf '%s\n' 'this build recipe targets macOS CPU/Metal/Accelerate only' >&2
    exit 2
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# Validate fields before shell splitting; the lock contains no shell program.
pins=$(bun -e '
const lock = JSON.parse(await Bun.file(process.argv[1]).text());
if (lock.schema !== "bonfyre.clef-native-runtime.v1"
    || lock.runtime.repository !== "https://github.com/ggml-org/llama.cpp"
    || !/^[a-f0-9]{40}$/.test(lock.runtime.revision)
    || !/^[a-f0-9]{64}$/.test(lock.runtime.source_archive_sha256)
    || lock.runtime.source_patches?.length !== 1
    || lock.runtime.source_patches[0].file !== "patches/clef-output-embedding.patch"
    || !/^[a-f0-9]{64}$/.test(lock.runtime.source_patches[0].sha256)) throw Error("invalid runtime lock");
console.log(lock.runtime.revision, lock.runtime.source_archive_sha256,
  lock.runtime.source_patches[0].file, lock.runtime.source_patches[0].sha256);
' "$script_dir/clef-runtime.lock.json")
set -- $pins
revision=$1
expected=$2
patch_file=$3
patch_expected=$4
patch_actual=$(shasum -a 256 "$script_dir/$patch_file")
patch_actual=${patch_actual%% *}
if [ "$patch_actual" != "$patch_expected" ]; then
    printf '%s\n' 'source patch digest mismatch; refusing to build' >&2
    exit 1
fi
mkdir -m 700 "$out"
if [ -z "$archive" ]; then
    archive="$out/source.tar.gz"
    curl --fail --location --proto '=https' --tlsv1.2 --max-time 180 \
        --max-filesize 104857600 --output "$archive" \
        "https://codeload.github.com/ggml-org/llama.cpp/tar.gz/$revision"
fi
actual=$(shasum -a 256 "$archive")
actual=${actual%% *}
if [ "$actual" != "$expected" ]; then
    printf '%s\n' 'source archive digest mismatch; refusing to extract or execute its build' >&2
    exit 1
fi
mkdir "$out/source"
tar -xzf "$archive" -C "$out/source" --strip-components 1
patch --batch --forward --fuzz=0 -p1 -d "$out/source" -i "$script_dir/$patch_file"
cmake -S "$out/source" -B "$out/build" \
    -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF \
    -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF \
    -DLLAMA_BUILD_APP=OFF -DLLAMA_BUILD_TOOLS=ON -DLLAMA_BUILD_SERVER=ON \
    -DLLAMA_BUILD_UI=OFF -DLLAMA_USE_PREBUILT_UI=OFF \
    -DLLAMA_OPENSSL=OFF -DLLAMA_SUBPROCESS=OFF -DLLAMA_USE_SYSTEM_GGML=OFF \
    -DLLAMA_BUILD_COMMIT="$revision" -DLLAMA_BUILD_NUMBER=0 \
    -DGGML_CPU=ON -DGGML_METAL=ON -DGGML_METAL_EMBED_LIBRARY=ON \
    -DGGML_BLAS=ON -DGGML_BLAS_VENDOR=Apple -DGGML_OPENMP=OFF \
    -DGGML_OPENMP_FETCH=OFF -DGGML_CPU_KLEIDIAI=OFF
cmake --build "$out/build" --target llama-server --parallel 2
mkdir "$out/bin"
cp "$out/build/bin/llama-server" "$out/bin/llama-server"
cp "$out/source/LICENSE" "$out/LICENSE.llama.cpp"
cp "$script_dir/clef-runtime.lock.json" "$out/clef-runtime.lock.json"
"$out/bin/llama-server" --version
bun -e '
import { createHash } from "node:crypto";
import { stat } from "node:fs/promises";
const [out, revision, archiveDigest, patchFile, patchDigest] = process.argv.slice(1);
const binary = `${out}/bin/llama-server`;
const digest = new Bun.CryptoHasher("sha256");
for await (const part of Bun.file(binary).stream()) digest.update(part);
const receipt = {
  schema: "bonfyre.clef-native-build.v1", source_revision: revision,
  source_archive_sha256: archiveDigest, binary, binary_bytes: (await stat(binary)).size,
  source_patches: [{ file: patchFile, sha256: patchDigest }],
  binary_sha256: digest.digest("hex"), platform: process.platform, arch: process.arch,
  cmake_cache_sha256: createHash("sha256").update(new Uint8Array(await Bun.file(`${out}/build/CMakeCache.txt`).arrayBuffer())).digest("hex"),
  built_at: new Date().toISOString(), inference_verified: false
};
await Bun.write(`${out}/build-receipt.json`, `${JSON.stringify(receipt, null, 2)}\n`);
' "$out" "$revision" "$expected" "$patch_file" "$patch_expected"
printf 'native Clef server: %s\n' "$out/bin/llama-server"
