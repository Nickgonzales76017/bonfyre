#!/bin/sh
# Reclaiming space must never take evidence or a working session's files:
#   - a retained body older than the raw window is compressed, not deleted,
#     and its bytes come back out of the compressed file
#   - a body inside the raw window is left alone
#   - a scratch directory holding a recent write is kept even when the
#     directory's own timestamp is old (this check once deleted live work)
#   - a scratch directory named as kept is never pruned
#   - on APFS, a cold receipt-named file shrinks on disk with its path and bytes
#     unchanged, identical files come to share blocks, and a database or a
#     recently written file is never rewritten
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-reclaim.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/bodies" "$tmp/scratch/project/live/deep" "$tmp/scratch/project/stale" "$tmp/scratch/project/named"
bun - "$tmp" <<'JS'
import { writeFileSync, utimesSync } from "node:fs";
const tmp = process.argv[2];
const old = new Date(Date.now() - 40 * 86400000);
writeFileSync(`${tmp}/bodies/ancient.body`, `<html>${"a retained page ".repeat(400)}</html>`);
writeFileSync(`${tmp}/bodies/fresh.body`, "<html>just observed</html>");
writeFileSync(`${tmp}/bodies/ancient.db`, "retained database bytes");
utimesSync(`${tmp}/bodies/ancient.body`, old, old);
utimesSync(`${tmp}/bodies/ancient.db`, old, old);
writeFileSync(`${tmp}/scratch/project/live/deep/work.txt`, "a session still working");
writeFileSync(`${tmp}/scratch/project/stale/old.txt`, "x".repeat(2_000_000));
writeFileSync(`${tmp}/scratch/project/named/keep.txt`, "y".repeat(2_000_000));
for (const dir of ["live", "stale", "named"]) utimesSync(`${tmp}/scratch/project/${dir}`, old, old);
for (const file of ["stale/old.txt", "named/keep.txt"]) utimesSync(`${tmp}/scratch/project/${file}`, old, old);
JS
bun "$root/scripts/reclaim_space.mjs" --no-prune-caches --no-transparent --no-dedupe --compress-dir "$tmp/bodies" \
    --scratch "$tmp/scratch" --keep-paths "$tmp/scratch/project/named" --json >"$tmp/dry-run.json"
[ -f "$tmp/bodies/ancient.body" ]
[ -f "$tmp/scratch/project/stale/old.txt" ]
bun "$root/scripts/reclaim_space.mjs" --apply --no-prune-caches --no-transparent --no-dedupe --compress-dir "$tmp/bodies" \
    --scratch "$tmp/scratch" --keep-paths "$tmp/scratch/project/named" --json >"$tmp/report.json"
bun - "$tmp" <<'JS'
import { existsSync, readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
const tmp = process.argv[2];
const report = JSON.parse(readFileSync(`${tmp}/report.json`, "utf8"));
const dryRun = JSON.parse(readFileSync(`${tmp}/dry-run.json`, "utf8"));
const problems = [];
const check = (yes, why) => { if (!yes) problems.push(why); };
check(existsSync(`${tmp}/bodies/ancient.body.gz`), "an old retained body is compressed");
check(!existsSync(`${tmp}/bodies/ancient.body`), "and its uncompressed copy no longer takes room");
check(gunzipSync(readFileSync(`${tmp}/bodies/ancient.body.gz`)).toString().includes("a retained page"), "the evidence is still readable");
check(existsSync(`${tmp}/bodies/fresh.body`), "a body inside the raw window is untouched");
check(existsSync(`${tmp}/bodies/ancient.db`) && !existsSync(`${tmp}/bodies/ancient.db.gz`), "a database is never compressed by a broad directory flag");
check(existsSync(`${tmp}/scratch/project/live/deep/work.txt`), "a session still writing keeps its scratch");
check(existsSync(`${tmp}/scratch/project/named/keep.txt`), "a directory named as kept is never pruned");
check(!existsSync(`${tmp}/scratch/project/stale/old.txt`), "an abandoned scratch directory is reclaimed");
check(report.compressed_bytes > 0 && report.pruned_bytes > 0, `the run reports reclaimed bytes (${report.reclaimed_bytes})`);
check(!dryRun.applied && dryRun.compressed_bytes > 0 && dryRun.pruned_bytes > 0, "dry-run measures without changing material");
if (problems.length) { console.error("reclaim space: FAIL\n" + problems.map(p => `  - ${p}`).join("\n")); process.exit(1); }
console.log("reclaim space: PASS");
JS

[ "$(uname)" = Darwin ] || exit 0
mkdir -p "$tmp/cold" "$tmp/dupes"
bun - "$tmp" <<'JS'
import { writeFileSync, utimesSync } from "node:fs";
const tmp = process.argv[2], old = new Date(Date.now() - 40 * 86400000);
const page = JSON.stringify({ receipt: "observed", body: "a retained public record ".repeat(4000) });
for (const name of ["receipt.json", "warm.json", "ledger.db"]) writeFileSync(`${tmp}/cold/${name}`, page);
for (const name of ["receipt.json", "ledger.db"]) utimesSync(`${tmp}/cold/${name}`, old, old);
const same = Buffer.alloc(300000, 7);
for (const name of ["a.bin", "b.bin", "c.bin"]) writeFileSync(`${tmp}/dupes/${name}`, same);
JS
shasum -a 256 "$tmp"/cold/* "$tmp"/dupes/* >"$tmp/before.sha"
before=$(du -k "$tmp/cold/receipt.json" | cut -f1)
bun "$root/scripts/reclaim_space.mjs" --apply --no-prune-caches --scratch "$tmp/none" \
    --transparent-dir "$tmp/cold" --dedupe-dir "$tmp/dupes" --cold-days 1 --json \
    | bun -e 'const r = JSON.parse(await Bun.stdin.text()); const t = r.transparent.find(x => x.path.endsWith("/cold")), d = r.deduped.find(x => x.path.endsWith("/dupes")); if (t?.files !== 1 || d?.files !== 2) { console.error("reclaim space: FAIL physical", JSON.stringify({ t, d })); process.exit(1); }' >/dev/null
shasum -a 256 -c "$tmp/before.sha" >/dev/null || { echo "reclaim space: FAIL bytes changed"; exit 1; }
[ "$(du -k "$tmp/cold/receipt.json" | cut -f1)" -lt "$before" ] || { echo "reclaim space: FAIL cold file did not shrink"; exit 1; }
ls -lO "$tmp/cold/ledger.db" "$tmp/cold/warm.json" | grep -q compressed && { echo "reclaim space: FAIL rewrote a database or warm file"; exit 1; }
# A second pass finds nothing: files already sharing their blocks are not
# re-cloned and reported as savings again.
bun "$root/scripts/reclaim_space.mjs" --no-prune-caches --no-transparent --scratch "$tmp/none" \
    --dedupe-dir "$tmp/dupes" --json \
    | bun -e 'const r = JSON.parse(await Bun.stdin.text()); if (r.deduped[0]?.files !== 0) { console.error("reclaim space: FAIL re-counted clones", JSON.stringify(r.deduped)); process.exit(1); }' >/dev/null
echo "reclaim space physical: PASS"
