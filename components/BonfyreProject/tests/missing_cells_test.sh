#!/bin/sh
# The four cells nothing owned, and the four things they refuse.
#
# Nine interaction grammars were blocked on these. Each is the smallest honest
# version rather than a clone, and the refusal is the part worth protecting:
#
#   version   -> snapshots and diffs; cannot merge, cannot restore, and will not
#                diff two snapshots of different namespaces
#   message   -> composed and held; no state claims transmission, and the compiler
#                refuses a state table where one does
#   signature -> bound to exact bytes; broken the moment they change
#   stream    -> live only while something says so inside the window, and asking
#                does not make it live
set -eu

bin=${1:-./bonfyre-project}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
command -v sqlite3 >/dev/null 2>&1 || { echo "missing cells: SKIP (no sqlite3)"; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-cells.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
cms="$tmp/cms.db"
tree_a="$tmp/tree-a"
tree_b="$tmp/tree-b"

sqlite3 "$cms" "
  CREATE TABLE estate_version(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, version_id TEXT, namespace_root TEXT, label TEXT,
    tree_digest TEXT, file_count TEXT, total_bytes TEXT, taken_by TEXT, taken_at TEXT,
    cannot_infer TEXT);
  CREATE TABLE version_path(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, path_id TEXT, version_id TEXT, path TEXT,
    digest TEXT, bytes TEXT, cannot_infer TEXT);
  CREATE UNIQUE INDEX vp_id ON version_path(namespace, path_id);
  CREATE TABLE outbound_message(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, message_id TEXT, about TEXT, recipient TEXT,
    subject_line TEXT, body TEXT, state TEXT, composed_by TEXT, composed_at TEXT,
    would_be_sent_by TEXT, state_reason TEXT, cannot_infer TEXT);
  CREATE TABLE assent_signature(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, signature_id TEXT, about TEXT, digest TEXT,
    assented_by TEXT, assented_at TEXT, statement TEXT, cannot_infer TEXT);
  CREATE TABLE live_session(namespace TEXT, created_at TEXT, updated_at TEXT,
    published_at TEXT, status TEXT, session_id TEXT, label TEXT, host_actor TEXT,
    opened_at TEXT, last_heartbeat_at TEXT, ended_at TEXT, replay_ref TEXT,
    cannot_infer TEXT);
"

mkdir -p "$tree_a/sub" "$tree_b"
printf 'one\n' >"$tree_a/kept.txt"
printf 'two\n' >"$tree_a/edited.txt"
printf 'three\n' >"$tree_a/sub/deep.txt"
printf 'gone\n' >"$tree_a/removed.txt"
printf 'elsewhere\n' >"$tree_b/other.txt"

run() { BONFYRE_CMS_DB="$cms" BONFYRE_ACTOR="${ACTOR-nick}" "$bin" "$@"; }
jq_field() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

# ------------------------------------------------------------------ version
v1=$(run version take "$tree_a" "first" | jq_field version)
# An identical tree must produce an identical tree digest, so the fold cannot
# depend on the order the directory was walked in.
cp -R "$tree_a" "$tmp/tree-a-copy"
d1=$(sqlite3 "$cms" "SELECT tree_digest FROM estate_version WHERE version_id='$v1';")
v_copy=$(run version take "$tmp/tree-a-copy" "copy" | jq_field version)
d_copy=$(sqlite3 "$cms" "SELECT tree_digest FROM estate_version WHERE version_id='$v_copy';")
[ "$d1" = "$d_copy" ] || { echo "FAIL: identical trees produced different digests"; exit 1; }

printf 'two changed\n' >"$tree_a/edited.txt"
rm "$tree_a/removed.txt"
printf 'new\n' >"$tree_a/added.txt"
sleep 1
v2=$(run version take "$tree_a" "second" | jq_field version)
run version diff "$v1" "$v2" >"$tmp/diff.json"
python3 - "$tmp/diff.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
by = {c["path"]: c["change"] for c in d["changes"]}
assert by.get("added.txt") == "added", by
assert by.get("removed.txt") == "removed", by
assert by.get("edited.txt") == "changed", by
assert "kept.txt" not in by, "an unchanged path was reported as news"
assert "sub/deep.txt" not in by, by
assert (d["added"], d["removed"], d["changed"]) == (1, 1, 1), d
assert d["unchanged"] == 2, d
print("version: 1 added, 1 removed, 1 changed, 2 unchanged, nested paths walked")
PY

# Two snapshots of different namespaces were never the same tree.
v_other=$(run version take "$tree_b" "other namespace" | jq_field version)
run version diff "$v1" "$v_other" >"$tmp/cross.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/cross.json" || { echo "FAIL: a cross-namespace diff was allowed"; exit 1; }
grep -q 'never the same tree' "$tmp/cross.json" || { echo "FAIL: the refusal gave no reason"; exit 1; }

# A root with nothing readable is refused rather than snapshotted as empty.
mkdir -p "$tmp/empty"
run version take "$tmp/empty" "nothing here" >"$tmp/empty.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/empty.json" || { echo "FAIL: an empty root produced a snapshot"; exit 1; }
run version take "$tmp/not-a-dir" "missing" >"$tmp/missing.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/missing.json" || { echo "FAIL: a missing root produced a snapshot"; exit 1; }
echo "version refused: a cross-namespace diff, an empty root, a missing root"

# ------------------------------------------------------------------ message
run message compose "about:something" "someone@example.test" "A line" "A body." >"$tmp/msg.json"
python3 - "$tmp/msg.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["state"] == "composed", d
assert d["transmitted"] is False, "the estate reported transmitting something"
assert "outside this estate" in d["would_be_sent_by"], d
print("message: composed and held, transmitted false, sender named as a person")
PY
# No state this record can hold may claim transmission.
states=$(sqlite3 "$cms" "SELECT DISTINCT state FROM outbound_message;")
[ "$states" = "composed" ] || { echo "FAIL: unexpected message state ($states)"; exit 1; }

# ---------------------------------------------------------------- signature
printf 'the exact bytes\n' >"$tmp/doc.txt"
sig=$(run sign record "a document" "$tmp/doc.txt" "I agree to this" | jq_field signature)
run sign check "$sig" "$tmp/doc.txt" >"$tmp/sig-ok.json"
grep -q '"state":"intact"' "$tmp/sig-ok.json" || { echo "FAIL: an unchanged file did not check out"; exit 1; }
printf 'one more line\n' >>"$tmp/doc.txt"
run sign check "$sig" "$tmp/doc.txt" >"$tmp/sig-broken.json" 2>/dev/null || true
grep -q '"state":"broken"' "$tmp/sig-broken.json" || { echo "FAIL: assent survived a change to the bytes"; exit 1; }
grep -q 'It was not withdrawn; it was outlived' "$tmp/sig-broken.json" || { echo "FAIL: the broken state did not say what it means"; exit 1; }
rm "$tmp/doc.txt"
run sign check "$sig" "$tmp/doc.txt" >"$tmp/sig-gone.json" 2>/dev/null || true
grep -q '"state":"broken"' "$tmp/sig-gone.json" || { echo "FAIL: a missing file checked out"; exit 1; }
run sign record "a document" "$tmp/does-not-exist" "I agree" >"$tmp/sig-none.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/sig-none.json" || { echo "FAIL: assent was recorded for a file that is not there"; exit 1; }
echo "signature: intact on the exact bytes, broken when they change or vanish, refused with no file"

# ------------------------------------------------------------------- stream
run session open s1 "A session" >"$tmp/s-open.json"
grep -q '"standing":"live"' "$tmp/s-open.json" || { echo "FAIL: a just-opened session was not live"; exit 1; }
sqlite3 "$cms" "UPDATE live_session SET last_heartbeat_at = datetime('now','-10 minutes') WHERE session_id='s1';"
run session status s1 >"$tmp/s-silent.json"
grep -q '"standing":"silent"' "$tmp/s-silent.json" || { echo "FAIL: a session with no heartbeat was still live"; exit 1; }
# Reading must not revive it.
run session status s1 >"$tmp/s-silent2.json"
grep -q '"standing":"silent"' "$tmp/s-silent2.json" || { echo "FAIL: asking a session's standing made it live"; exit 1; }
run session beat s1 >"$tmp/s-beat.json"
grep -q '"standing":"live"' "$tmp/s-beat.json" || { echo "FAIL: a heartbeat did not make it live"; exit 1; }
run session end s1 "replay:somewhere" >"$tmp/s-end.json"
grep -q '"standing":"ended"' "$tmp/s-end.json" || { echo "FAIL: an ended session was not ended"; exit 1; }
grep -q '"replay":"replay:somewhere"' "$tmp/s-end.json" || { echo "FAIL: the replay reference was lost"; exit 1; }
run session beat s1 >"$tmp/s-revive.json" 2>/dev/null || true
grep -q '"state":"refused"' "$tmp/s-revive.json" || { echo "FAIL: an ended session was made live again"; exit 1; }
echo "stream: live on a beat, silent without one, reading does not revive, ended stays ended"

echo "missing cells contract: PASS"
