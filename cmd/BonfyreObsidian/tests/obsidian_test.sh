#!/bin/sh
# bonfyre-obsidian over throwaway vaults in a temp directory.
#
# Real: the organ binary, its SQLite store, every native reader and writer,
# path refusal, account isolation, snapshots and diffs, the headless guard and
# the child supervisor's process handling.
#
# Shims, and only for argv construction and supervision: `ob`, `obsidian` and
# `launchctl` below are small sh scripts that record the argv and environment
# they were given and answer the way the real tools answer. They stand in for
# Obsidian's servers and the desktop app, which a test must not touch. No part
# of the organ itself is mocked.
set -u
B=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
T=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-obsidian-test.XXXXXX")
T=$(cd "$T" && pwd -P)
trap 'kill $(cat "$T"/pids 2>/dev/null) 2>/dev/null; rm -rf "$T"' EXIT
PASS=0; FAIL=0
pass() { PASS=$((PASS + 1)); }
fail() { FAIL=$((FAIL + 1)); echo "FAIL: $*"; echo "  rc=$RC out=$OUT" | cut -c1-600; }
run() { OUT=$("$B" "$@" 2>"$T/stderr"); RC=$?; }
rc_is() { [ "$RC" = "$1" ] && pass || fail "$2 (want rc $1)"; }
has() { printf '%s' "$OUT" | jq -e "$1" >/dev/null 2>&1 && pass || fail "$2 :: $1"; }

ROOT="$T/vaults"; mkdir -p "$ROOT" "$T/bin" "$T/home" "$T/appsupport" "$T/outside/vault"
export HOME="$T/home"
export BONFYRE_OBSIDIAN_DB="$T/obsidian.db" BONFYRE_OBSIDIAN_STATE="$T/state" BONFYRE_OBSIDIAN_ROOTS="$ROOT"
export BONFYRE_OBSIDIAN_APP_SUPPORT="$T/appsupport" BONFYRE_OBSIDIAN_APP="$T/NoApp.app"
export BONFYRE_OBSIDIAN_OB="$T/bin/ob" OBSIDIAN_CLI="$T/bin/obsidian-absent" BONFYRE_OBSIDIAN_APP_RUNNING=0
export BONFYRE_OBSIDIAN_SUPERVISOR=child BONFYRE_OBSIDIAN_TIMEOUT=10
unset OBSIDIAN_AUTH_TOKEN BONFYRE_OBSIDIAN_TOKEN_ACCOUNT

# ---------------------------------------------------------------- shims
cat > "$T/bin/ob" <<EOF
#!/bin/sh
# Records argv, HOME and whether a token reached it; never contacts Obsidian.
{ printf 'HOME=%s TOKEN=%s ARGV=' "\$HOME" "\${OBSIDIAN_AUTH_TOKEN:+set}"; printf '%s|' "\$@"; echo; } >> "$T/ob.log"
case "\$1" in
  --version) echo 0.0.14 ;;
  sync-setup|sync-config) echo '{"ok":true}' ;;
  sync-status) if [ -f "$T/ob-configured" ]; then echo '{"vaultId":"v1","mode":"bidirectional"}'; else echo "No sync configuration found" >&2; exit 3; fi ;;
  sync) case " \$* " in *" --continuous "*) while :; do sleep 1; done ;; *) echo synced ;; esac ;;
esac
EOF
cat > "$T/bin/obsidian" <<EOF
#!/bin/sh
# Stands in for the desktop CLI: records argv, answers as the app would.
printf '%s|' "\$@" >> "$T/cli.log"; echo >> "$T/cli.log"
if [ "\$1" = version ]; then echo 1.12.7; exit 0; fi
case "\$2" in
  search) echo '[{"file":"x.md","matches":1}]' ;;
  tags) exit 1 ;;
  sync:status) cat "$T/cli-sync-status" ;;
  version) echo "1.12.7" ;;
  *) echo ok ;;
esac
EOF
cat > "$T/bin/launchctl" <<EOF
#!/bin/sh
printf '%s|' "\$@" >> "$T/launchctl.log"; echo >> "$T/launchctl.log"
case "\$1" in
  bootstrap) touch "$T/launchd-loaded" ;;
  bootout) rm -f "$T/launchd-loaded" ;;
  print) [ -f "$T/launchd-loaded" ] || exit 113; echo "pid = 4242" ;;
esac
EOF
chmod +x "$T/bin/ob" "$T/bin/obsidian" "$T/bin/launchctl"

# ---------------------------------------------------------------- fixture vaults
A="$ROOT/alpha"; mkdir -p "$A/Projects" "$A/.obsidian"
cat > "$A/Home.md" <<'EOF'
---
status: active
tags: [project, Alpha]
aliases:
  - start
  - "front door"
---
# Title
Links: [[Ideas]], [[Projects/Plan|the plan]], [[Missing Note]] and ![[pic.png]].
Also [the plan](Projects/Plan.md) and https://example.com#frag.
#inbox and `#notatag` and #123 alone.
```
#fenced [[Fenced]]
- [ ] fenced task
```
- [ ] buy milk
- [x] done thing
  * [ ] nested task
1. [ ] numbered task
- [/] partial
EOF
printf 'links back [[Home]]\n#inbox #idea/sub\n' > "$A/Ideas.md"
printf '[[home]] in lower case\n- [ ] plan task\n' > "$A/Projects/Plan.md"
printf '\211PNG\r\n' > "$A/pic.png"
echo '{"sync":false,"daily-notes":true}' > "$A/.obsidian/core-plugins.json"
echo '{"folder":"Daily","format":"YYYY-MM-DD"}' > "$A/.obsidian/daily-notes.json"
ln -s /etc/hosts "$A/esc.md"
ln -s /etc "$A/escape"
B2="$ROOT/beta"; mkdir -p "$B2/.obsidian"; echo '{"sync":true}' > "$B2/.obsidian/core-plugins.json"; echo hi > "$B2/n.md"
G="$ROOT/gamma"; mkdir -p "$G"; echo '# g' > "$G/g.md"
H="$ROOT/eta"; mkdir -p "$H/.obsidian"; echo '["file-explorer","sync"]' > "$H/.obsidian/core-plugins.json"
D="$ROOT/delta"; mkdir -p "$D"; echo '- [ ] cli milk' > "$D/d.md"
E="$ROOT/epsilon"; mkdir -p "$E"; echo e > "$E/e.md"
ln -s "$T/outside/vault" "$ROOT/link"
cat > "$T/appsupport/obsidian.json" <<EOF
{"vaults":{"a1pha":{"path":"$A","ts":1},"d3lta":{"path":"$D","ts":2,"open":true},"e5":{"path":"$E","ts":3}}}
EOF

# ---------------------------------------------------------------- binding and isolation
run bind --account a1 --name alpha --path "$A"; rc_is 0 "bind alpha"
has '.binding.vault_id == "a1pha" and .binding.transport == "files" and .desktop_registered and .desktop_sync_plugin == false' "bind reports registry id"
run bind --account a2 --name mine --path "$A"; rc_is 3 "second account cannot bind a bound folder"
has '.error == "already-bound"' "already-bound"
run bind --account a2 --name sub --path "$A/Projects"; rc_is 3 "nested folder refused"
has '.error == "nested"' "nested code"
run bind --account a1 --name escaped --path "$ROOT/link"; rc_is 3 "symlink out of the allowed roots refused"
has '.error == "outside-roots"' "outside-roots code"
run bind --account a1 --name alpha --path "$G"; rc_is 3 "name reuse refused"
run bind --account A1 --name x --path "$G"; rc_is 2 "uppercase account refused"
run bind --account a1 --name ../x --path "$G"; rc_is 2 "bad name refused"
# Real account subjects must not alias a slug, another user or a path.
CANON="$ROOT/canonical"; mkdir -p "$CANON"; echo '# account' > "$CANON/Note.md"
run doctor --account user:19; rc_is 0 "canonical user subject accepted"
run bind --account user:19 --name knowledge --path "$CANON"; rc_is 0 "canonical subject bind"
run doctor --account user:19; rc_is 0 "local-first doctor"
has '.native.local_file_io and .native.snapshots_and_diff and (.native.obsidian_subscription_required | not) and (.native.cross_device_replication_active | not) and .native.cross_device_replication == "not-wired"' "native vault does not require a hosted subscription"
has '([.by_hand[] | select(.why | test("Headless Sync|Obsidian Headless"))] | length) == 0' "local-only account receives no hosted setup prerequisite"
run read --account user:19 --name knowledge --path Note.md; rc_is 0 "canonical subject read"
run read --account user:20 --name knowledge --path Note.md; rc_is 3 "other canonical subject refused"
run read --account user-19 --name knowledge --path Note.md; rc_is 3 "hyphen alias refused"
run doctor --account user:0; rc_is 2 "user zero refused"
run doctor --account user:019; rc_is 2 "leading zero refused"
run doctor --account user:19:extra; rc_is 2 "extra colon refused"
run doctor --account user:../19; rc_is 2 "account traversal refused"
run doctor --account user:19/evil; rc_is 2 "slash refused"
run read --account a2 --name alpha --path Home.md; rc_is 3 "other account cannot read"
has '.error == "not-bound" and (.reason | test("a2"))' "same answer as an unbound name"
run read --account a2 --name nothing --path Home.md; rc_is 3 "unbound name"
run vaults --account a2
has '(.bindings | length) == 0 and ([.desktop_vaults[] | select(.id == "a1pha")][0] | .bound_elsewhere and (.bound_to_this_account | not))' "a2 sees alpha only as bound elsewhere"
run vaults --account a1
has '(.bindings | length) == 1 and ([.desktop_vaults[] | select(.id == "a1pha")][0].bound_to_this_account)' "a1 sees its binding"

# ---------------------------------------------------------------- reading
run read --account a1 --name alpha --path Home.md; rc_is 0 "read"
WANT=$(shasum -a 256 "$A/Home.md" | cut -d' ' -f1)
has ".via == \"native\" and .sha256 == \"$WANT\" and (.content | test(\"buy milk\"))" "read content and sha"
for bad in ../x.md /etc/hosts .obsidian/core-plugins.json .OBSIDIAN/core-plugins.json esc.md escape/hosts 'a//b.md' '.hidden.md'; do
  run read --account a1 --name alpha --path "$bad"; rc_is 3 "read refused: $bad"
done
run read --account a1 --name alpha --path nope.md; rc_is 4 "missing note"
run search --account a1 --name alpha --query MILK; rc_is 0 "search"
has '.answer.shape == "bonfyre-native" and ([.answer.matches[] | select(.path == "Home.md")] | length) == 1 and .answer.matches[0].line == 16' "search finds milk case-insensitively at line 16"
run search --account a1 --name alpha --query plan --prefix Projects/
has '(.answer.file_matches == ["Projects/Plan.md"]) and all(.answer.matches[]; .path == "Projects/Plan.md")' "search prefix"
run tasks --account a1 --name alpha; rc_is 0 "tasks"
has '.answer.counts == {"done":1,"other":1,"todo":4}' "task counts exclude the fenced task"
has '[.answer.tasks[] | select(.status == "todo") | .text] == ["buy milk","nested task","numbered task","plan task"]' "todo texts in order"
has '([.answer.tasks[] | .ref] | index("Home.md:16")) != null' "task ref path:line"
run tasks --account a1 --name alpha --status done
has '[.answer.tasks[].text] == ["done thing"]' "status filter"
run backlinks --account a1 --name alpha --path Home.md; rc_is 0 "backlinks"
has '[.answer.backlinks[].source] == ["Ideas.md","Projects/Plan.md"]' "backlinks to Home, case-insensitive"
run backlinks --account a1 --name alpha --path Projects/Plan.md
has '.answer.count == 2 and ([.answer.backlinks[].kind] | sort) == ["markdown","wikilink"]' "wikilink and markdown backlinks"
run links --account a1 --name alpha --path Home.md
has '([.answer.links[] | select(.target == "pic.png")][0].resolved == "pic.png") and ([.answer.links[] | select(.target == "Missing Note")][0].resolved == null)' "outgoing links resolved"
run unresolved --account a1 --name alpha
has '[.answer.unresolved[].target] == ["Missing Note"]' "only the real unresolved link, not the fenced one"
run tags --account a1 --name alpha; rc_is 0 "tags"
has '[.answer.tags[].tag] == ["alpha","idea/sub","inbox","project"]' "tags from frontmatter and body, none from code"
has '(.answer.tags[] | select(.tag == "inbox")) == {"tag":"inbox","count":2,"files":2}' "tag counts"
run properties --account a1 --name alpha --path Home.md; rc_is 0 "properties"
has '.answer.properties == {"status":"active","tags":["project","Alpha"],"aliases":["start","front door"]}' "frontmatter properties"
run properties --account a1 --name alpha
has '[.answer.keys[].key] == ["aliases","status","tags"]' "property keys"

# ---------------------------------------------------------------- writing
run create --account a1 --name alpha --path Inbox/new --content 'first line'; rc_is 0 "create makes folders and adds .md"
has '.created and .path == "Inbox/new.md" and .via == "native" and (.receipt | startswith("obr-"))' "create receipt"
[ "$(cat "$A/Inbox/new.md")" = "first line" ] && pass || fail "created content"
run create --account a1 --name alpha --path Inbox/new.md --content again; rc_is 3 "create refuses an existing note"
run create --account a1 --name alpha --path Inbox/new.md --content replaced --overwrite; rc_is 0 "overwrite"
printf 'from stdin\n' | { OUT=$("$B" append --account a1 --name alpha --path Inbox/new.md --stdin); RC=$?; rc_is 0 "append from stdin"; }
[ "$(cat "$A/Inbox/new.md")" = "$(printf 'replaced\nfrom stdin')" ] && pass || fail "append content"
run prepend --account a1 --name alpha --path Ideas.md --content 'top'; rc_is 0 "prepend"
[ "$(head -1 "$A/Ideas.md")" = "top" ] && pass || fail "prepend at top without frontmatter"
cp "$A/Home.md" "$T/home-before"
run prepend --account a1 --name alpha --path Home.md --content 'after fm'; rc_is 0 "prepend after frontmatter"
[ "$(sed -n 1p "$A/Home.md")" = "---" ] && [ "$(sed -n 8p "$A/Home.md")" = "after fm" ] && pass || fail "prepend lands after frontmatter"
cp "$T/home-before" "$A/Home.md"
run append --account a1 --name alpha --path missing.md --content x; rc_is 4 "append needs a note"
run create --account a1 --name alpha --path .obsidian/evil.md --content x; rc_is 3 "write into config refused"
run create --account a1 --name alpha --path escape/evil.md --content x; rc_is 3 "write through a symlinked folder refused"
[ ! -e /etc/evil.md ] && pass || fail "nothing written through the link"
run create --account a1 --name alpha --path esc.md --content x --overwrite; rc_is 3 "overwrite of a symlinked note refused"
run create --account a2 --name alpha --path x.md --content x; rc_is 3 "other account cannot write"
run daily-append --account a1 --name alpha --date 2026-10-31 --content 'morning'; rc_is 0 "daily-append creates"
run daily-append --account a1 --name alpha --date 2026-10-31 --content 'evening'; rc_is 0 "daily-append appends"
[ "$(cat "$A/Daily/2026-10-31.md")" = "$(printf 'morning\nevening')" ] && pass || fail "daily note content"
run daily-path --account a1 --name alpha --date 2026-10-31
has '.path == "Daily/2026-10-31.md" and .exists' "daily path"
run daily-append --account a1 --name alpha --date 2026-13-01 --content x; rc_is 2 "bad date refused"
run task --account a1 --name alpha --ref Home.md:16 --done; rc_is 0 "task done"
grep -qx -- '- \[x\] buy milk' "$A/Home.md" && pass || fail "task marked"
run task --account a1 --name alpha --ref Home.md:16 --toggle
grep -qx -- '- \[ \] buy milk' "$A/Home.md" && pass || fail "task toggled back"
run task --account a1 --name alpha --ref Home.md:9; rc_is 3 "non-task line refused"
run receipts --account a1 --name alpha
has '(.receipts | length) > 5 and all(.receipts[]; .binding == "alpha") and ([.receipts[] | select(.outcome == "refused")] | length) > 0' "a1 receipts include refusals"
run receipts --account a2
has 'all(.receipts[]; .binding != "alpha" or .verb == "bind")' "a2 receipts hold none of a1's work"
if sqlite3 "$T/obsidian.db" "select * from receipt" | grep -q 'first line\|from stdin\|morning'; then FAIL=$((FAIL+1)); echo "FAIL: content leaked into receipts"; else pass; fi

# ---------------------------------------------------------------- snapshots
run snapshot --account a1 --name alpha; rc_is 0 "snapshot 1"
S1=$(printf '%s' "$OUT" | jq .snapshot_id)
has '.previous == null and .files == 6 and .symlinks_skipped == 2 and .reused == 0' "first snapshot hashes every file, skips links and .obsidian"
run snapshot --account a1 --name alpha
has ".previous == $S1 and .rehashed == 0 and .reused == 6 and .since_previous == {\"added\":0,\"removed\":0,\"changed\":0}" "unchanged vault reuses every hash"
echo changed >> "$A/Ideas.md"; rm "$A/pic.png"; echo new > "$A/fresh.md"
run snapshot --account a1 --name alpha
has '.since_previous == {"added":1,"removed":1,"changed":1} and .rehashed == 2' "incremental: only touched files rehashed"
run diff --account a1 --name alpha; rc_is 0 "diff"
has '[.added[].path] == ["fresh.md"] and [.removed[].path] == ["pic.png"] and [.changed[].path] == ["Ideas.md"]' "diff names the changes"
run diff --account a1 --name alpha --from "$S1"
has '.from == '"$S1"' and .counts == {"added":1,"removed":1,"changed":1}' "diff from an older snapshot"
run snapshot --account a1 --name alpha --include-config
has '.files == 8' "include-config adds the settings files"
# Two actual local replicas, no provider, no write/transfer and no cross-account leak.
REPLICA="$ROOT/replica"; mkdir -p "$REPLICA/.obsidian"
cp "$A/Home.md" "$REPLICA/Home.md"
cp "$A/Ideas.md" "$REPLICA/Ideas.md"
printf 'different from the source\n' >> "$REPLICA/Ideas.md"
echo peer-only > "$REPLICA/peer.md"
echo '{"sync":false}' > "$REPLICA/.obsidian/core-plugins.json"
ln -s /etc/hosts "$REPLICA/escape.md"
run bind --account a1 --name replica --path "$REPLICA"; rc_is 0 "same account second replica"
BEFORE_REPLICA=$(shasum -a 256 "$REPLICA/Home.md" | cut -d' ' -f1)
run replica-plan --account a1 --name alpha --peer replica; rc_is 0 "compare real replica bytes without a provider"
has '.mode == "read-only" and (.transfer | not) and .merge_base == null and .counts.identical >= 1 and .counts.divergent >= 1 and .counts.source_only >= 1 and .counts.peer_only == 1 and .peer_symlinks_skipped == 1' "per-file digests, divergence, exclusions and symlink refusal"
has '[.files[] | select(.status == "divergent") | .path] | index("Ideas.md") != null' "divergent content is flagged for review"
has '[.files[] | .path] | index(".obsidian/core-plugins.json") == null and index("escape.md") == null' "config and symlinks omitted"
[ "$BEFORE_REPLICA" = "$(shasum -a 256 "$REPLICA/Home.md" | cut -d' ' -f1)" ] && pass || fail "replica plan is nonmutating"
run replica-plan --account a2 --name alpha --peer replica; rc_is 3 "other account cannot inspect replicas"
has '.error == "not-bound"' "other account learns no paths"
run replica-plan --account a1 --name alpha --peer unknown; rc_is 3 "missing peer rejected"
run replica-plan --account a1 --name alpha --peer alpha; rc_is 2 "same replica refused"
# Matching historical content manifests permit ancestor-aware *planning only*.
BASE_A="$ROOT/base-a"; BASE_B="$ROOT/base-b"
mkdir -p "$BASE_A" "$BASE_B"
for note in unchanged source peer both conflict source-delete peer-delete both-delete; do
  printf 'original %s\n' "$note" > "$BASE_A/$note.md"
done
cp "$BASE_A/"*.md "$BASE_B/"
run bind --account a1 --name base-a --path "$BASE_A"; rc_is 0 "bind baseline source"
run bind --account a1 --name base-b --path "$BASE_B"; rc_is 0 "bind baseline peer"
run snapshot --account a1 --name base-a; rc_is 0 "source baseline snapshot"
BASE_S=$(printf '%s' "$OUT" | jq .snapshot_id)
run snapshot --account a1 --name base-b; rc_is 0 "peer baseline snapshot"
BASE_P=$(printf '%s' "$OUT" | jq .snapshot_id)
printf 'source only edit\n' >> "$BASE_A/source.md"
printf 'peer only edit\n' >> "$BASE_B/peer.md"
echo same-change > "$BASE_A/both.md"
echo same-change > "$BASE_B/both.md"
echo from-source > "$BASE_A/conflict.md"
echo from-peer > "$BASE_B/conflict.md"
rm "$BASE_A/source-delete.md" "$BASE_B/peer-delete.md"
rm "$BASE_A/both-delete.md" "$BASE_B/both-delete.md"
echo only-a > "$BASE_A/added-a.md"
echo only-b > "$BASE_B/added-b.md"
BEFORE_A=$(shasum -a 256 "$BASE_A/conflict.md" | cut -d' ' -f1)
BEFORE_B=$(shasum -a 256 "$BASE_B/conflict.md" | cut -d' ' -f1)
run replica-plan --account a1 --name base-a --peer base-b --base-source "$BASE_S" --base-peer "$BASE_P"; rc_is 0 "historical paired snapshots reconcile"
has '.merge_base.status == "matching-recorded-manifests" and .merge_base.source_snapshot == '"$BASE_S"' and .merge_base.peer_snapshot == '"$BASE_P"' and .transfer == false and .counts == {"unchanged":1,"source_changed":3,"peer_changed":3,"both_same":2,"conflict":1}' "all change and delete classifications measured"
has '([.files[] | select(.path == "source.md")][0].status == "source-changed") and
     ([.files[] | select(.path == "peer.md")][0].status == "peer-changed") and
     ([.files[] | select(.path == "both.md")][0].status == "both-same") and
     ([.files[] | select(.path == "conflict.md")][0].status == "conflict")' "new classifications have exact paths"
has '([.files[] | select(.path == "source-delete.md")][0].source_sha256 == null) and
     ([.files[] | select(.path == "peer-delete.md")][0].peer_sha256 == null) and
     ([.files[] | select(.path == "both-delete.md")][0].status == "both-same")' "deletions remain explicit and never auto-apply"
[ "$BEFORE_A" = "$(shasum -a 256 "$BASE_A/conflict.md" | cut -d' ' -f1)" ] &&
[ "$BEFORE_B" = "$(shasum -a 256 "$BASE_B/conflict.md" | cut -d' ' -f1)" ] &&
pass || fail "baseline plan leaves both conflicts untouched"
run replica-plan --account a1 --name base-a --peer base-b --base-source "$BASE_S"; rc_is 2 "paired baseline required"
run replica-plan --account a1 --name base-a --peer base-b --base-source x --base-peer "$BASE_P"; rc_is 2 "nonnumeric snapshot refused"
run replica-plan --account a1 --name base-a --peer base-b --base-source "$BASE_P" --base-peer "$BASE_S"; rc_is 3 "cross-binding baseline refused"
run replica-plan --account a2 --name base-a --peer base-b --base-source "$BASE_S" --base-peer "$BASE_P"; rc_is 3 "cross-account baseline refused"
run snapshot --account a1 --name base-a; rc_is 0 "changed source checkpoint"
CHANGED_S=$(printf '%s' "$OUT" | jq .snapshot_id)
run replica-plan --account a1 --name base-a --peer base-b --base-source "$CHANGED_S" --base-peer "$BASE_P"; rc_is 3 "unequal baseline manifests refused"
run snapshot --account a1 --name base-a --include-config; rc_is 0 "include-config checkpoint"
INCLUDE_S=$(printf '%s' "$OUT" | jq .snapshot_id)
run replica-plan --account a1 --name base-a --peer base-b --base-source "$INCLUDE_S" --base-peer "$BASE_P"; rc_is 3 "config-inclusive snapshot refused"
run bind --account a2 --name g2 --path "$E" ; rc_is 0 "a2 binds another vault"
run diff --account a2 --name g2 --from "$S1" --to "$S1"; rc_is 3 "a2 cannot diff a1's snapshots"

# ---------------------------------------------------------------- headless guard
run bind --account a1 --name beta --path "$B2" --transport headless; rc_is 0 "bind beta headless"
has '.desktop_sync_plugin == true' "bind reports desktop sync"
run guard --account a1 --name beta
has '.guard.decision == "refuse" and .guard.code == "desktop-sync-enabled"' "guard refuses desktop-sync vault"
: > "$T/ob.log"
run sync-once --account a1 --name beta; rc_is 3 "sync-once refused where desktop sync is on"
run sync-start --account a1 --name beta; rc_is 3 "sync-start refused where desktop sync is on"
[ ! -s "$T/ob.log" ] && pass || fail "ob never ran for a refused vault"
run bind --account a1 --name eta --path "$H" --transport headless
run guard --account a1 --name eta
has '.guard.code == "desktop-sync-enabled"' "array-form core-plugins.json is read too"
run guard --account a1 --name alpha
has '.guard.code == "transport"' "files transport cannot headless sync"
run bind --account a1 --name gamma --path "$G" --transport headless --mode pull-only --exclude 'Archive,Private/x' --device-name 'mac mini'; rc_is 0 "bind gamma"
run guard --account a1 --name gamma
has '.guard.decision == "allow" and .guard.code == "clear"' "guard allows a clean vault"
mkdir -p "$T/state/accounts/a2/home/.obsidian-headless/sync/v9"
echo "{\"vaultPath\":\"$G\"}" > "$T/state/accounts/a2/home/.obsidian-headless/sync/v9/config.json"
run guard --account a1 --name gamma
has '.guard.code == "headless-other-account"' "another account's ob setup for the folder refuses"
rm -rf "$T/state/accounts/a2/home/.obsidian-headless"
mkdir -p "$HOME/.obsidian-headless/sync/v8"; echo "{\"vaultPath\":\"$G\"}" > "$HOME/.obsidian-headless/sync/v8/config.json"
run guard --account a1 --name gamma
has '.guard.code == "headless-unscoped"' "an ob setup outside any account refuses"
rm -rf "$HOME/.obsidian-headless"

# ---------------------------------------------------------------- headless argv, account HOME and token scope (ob shim)
: > "$T/ob.log"
run sync-setup --account a1 --name gamma --remote-vault 'Remote X'; rc_is 0 "sync-setup"
AH="$T/state/accounts/a1/home"
grep -q "^HOME=$AH TOKEN= ARGV=sync-setup|--vault|Remote X|--path|$G|--device-name|mac mini|--json|$" "$T/ob.log" && pass || fail "sync-setup argv and account HOME: $(cat "$T/ob.log")"
grep -q "ARGV=sync-config|--path|$G|--mode|pull-only|--conflict-strategy|merge|--excluded-folders|Archive,Private/x|--device-name|mac mini|--json|" "$T/ob.log" && pass || fail "sync-config argv"
grep -q -- '--password\|--email\|--mfa' "$T/ob.log" && fail "a credential flag reached argv" || pass
run status --account a1 --name gamma
has '.vaults[0].binding.state == "sync-configured" and .vaults[0].binding.remote_vault == "Remote X" and .vaults[0].last_sync_run.kind == "sync-config"' "status after setup"
export OBSIDIAN_AUTH_TOKEN=tok-secret-value-123 BONFYRE_OBSIDIAN_TOKEN_ACCOUNT=a2
: > "$T/ob.log"; run sync-once --account a1 --name gamma; rc_is 0 "sync-once"
grep -q "TOKEN= ARGV=sync|--path|$G|$" "$T/ob.log" && pass || fail "token withheld from another account"
export BONFYRE_OBSIDIAN_TOKEN_ACCOUNT=a1
: > "$T/ob.log"; run sync-once --account a1 --name gamma
grep -q "TOKEN=set ARGV=sync|" "$T/ob.log" && pass || fail "token passed to the named account"
printf '%s' "$OUT" | grep -q tok-secret && fail "token in output" || pass
cat "$T/obsidian.db" "$T"/state/accounts/a1/logs/* 2>/dev/null | grep -q tok-secret && fail "token stored" || pass
unset OBSIDIAN_AUTH_TOKEN BONFYRE_OBSIDIAN_TOKEN_ACCOUNT
touch "$T/ob-configured"; run sync-status --account a1 --name gamma
has '.ob.configured and .ob.status.vaultId == "v1" and (.continuous_sync.running | not)' "sync-status reads ob json"
rm -f "$T/ob-configured"

# ---------------------------------------------------------------- child supervisor
run sync-start --account a1 --name gamma; rc_is 0 "sync-start"
PID=$(printf '%s' "$OUT" | jq .continuous_sync.pid); echo "$PID" >> "$T/pids"
kill -0 "$PID" 2>/dev/null && pass || fail "continuous sync process alive"
run sync-start --account a1 --name gamma
has '.continuous_sync.already_running and .continuous_sync.pid == '"$PID" "second start sees the running process"
run status --account a1 --name gamma
has '.vaults[0].continuous_sync.running and .vaults[0].binding.state == "syncing"' "status shows syncing"
run unbind --account a1 --name gamma; rc_is 3 "unbind refused while syncing"
run sync-stop --account a1 --name gamma; rc_is 0 "sync-stop"
has '.was_running' "stop stopped it"
sleep 1; kill -0 "$PID" 2>/dev/null && fail "process still alive after stop" || pass
sleep 300 & STRAY=$!; echo "$STRAY" >> "$T/pids"
echo "$STRAY" > "$T/state/accounts/a1/run/gamma.pid"
run sync-stop --account a1 --name gamma
has '(.was_running | not) and .stale_pidfile' "a reused pid is called stale"
kill -0 "$STRAY" && pass || fail "an unrelated process was signalled"
kill "$STRAY" 2>/dev/null; wait "$STRAY" 2>/dev/null
printf '#!/bin/sh\nexit 7\n' > "$T/bin/ob-dies"; chmod +x "$T/bin/ob-dies"
export BONFYRE_OBSIDIAN_OB="$T/bin/ob-dies"; run sync-start --account a1 --name gamma; export BONFYRE_OBSIDIAN_OB="$T/bin/ob"; rc_is 1 "a sync that dies at once is reported"
has '.error == "start-failed" and (.reason | test("status 7"))' "start failure names the exit status"

# ---------------------------------------------------------------- launchd supervisor (launchctl shim)
export BONFYRE_OBSIDIAN_SUPERVISOR=launchd BONFYRE_OBSIDIAN_LAUNCHCTL="$T/bin/launchctl" BONFYRE_OBSIDIAN_LAUNCH_AGENTS="$T/agents"
run sync-start --account a1 --name gamma; rc_is 0 "launchd start"
P=$(printf '%s' "$OUT" | jq -r '.continuous_sync.plist')
LABEL=$(printf '%s' "$OUT" | jq -r '.continuous_sync.label')
printf '%s' "$LABEL" | grep -Eq '^com[.]bonfyre[.]obsidian[.]a1-[0-9a-f]{20}[.]gamma$' && pass || fail "account digest survives launchd label"
[ -f "$P" ] && grep -q "<string>--continuous</string><string>--path</string><string>$G</string>" "$P" && grep -q "<key>HOME</key><string>$AH</string>" "$P" && pass || fail "plist names ob, the folder and the account HOME"
if command -v plutil >/dev/null; then plutil -lint "$P" >/dev/null && pass || fail "plist is valid"; fi
grep -q "^bootstrap|gui/$(id -u)|$P|" "$T/launchctl.log" && pass || fail "launchctl bootstrap argv"
run status --account a1 --name gamma
has '.vaults[0].continuous_sync.running and .vaults[0].continuous_sync.pid == 4242' "launchd status"
run sync-stop --account a1 --name gamma; rc_is 0 "launchd stop"
grep -q "^bootout|gui/$(id -u)/$LABEL|" "$T/launchctl.log" && [ ! -f "$P" ] && pass || fail "bootout and plist removed"
export BONFYRE_OBSIDIAN_SUPERVISOR=child
run unbind --account a1 --name gamma; rc_is 0 "unbind once stopped"

# ---------------------------------------------------------------- desktop CLI (obsidian shim)
export OBSIDIAN_CLI="$T/bin/obsidian"
run bind --account a1 --name delta --path "$D" --transport cli; rc_is 0 "bind delta cli"
: > "$T/cli.log"
run search --account a1 --name delta --query milk
has '.answer.shape == "bonfyre-native"' "app down: native search"
[ ! -s "$T/cli.log" ] && pass || fail "the desktop CLI was not called while the app is down"
export BONFYRE_OBSIDIAN_APP_RUNNING=1
run search --account a1 --name delta --query milk; rc_is 0 "search through CLI"
has '.shape == "obsidian-cli" and .result == [{"file":"x.md","matches":1}]' "CLI JSON passed through"
grep -q '^vault=d3lta|search|query=milk|format=json|$' "$T/cli.log" && pass || fail "CLI search argv"
run tags --account a1 --name delta
has '.answer.shape == "bonfyre-native" and (.cli_fallback | test("failed"))' "failing CLI falls back to native"
run create --account a1 --name delta --path 'n.md' --content "$(printf 'one\ntwo')"; rc_is 0 "create through CLI"
grep -q '^vault=d3lta|create|path=n.md|content=one\\ntwo|$' "$T/cli.log" && pass || fail "CLI create argv escapes newlines"
has '.via == "cli"' "write went through the app"
run search --account a1 --name delta --query milk --via native
has '.answer.shape == "bonfyre-native"' "--via native"
export BONFYRE_OBSIDIAN_APP_RUNNING=0
run search --account a1 --name delta --query milk --via cli; rc_is 5 "--via cli with the app down"
export BONFYRE_OBSIDIAN_APP_RUNNING=1
run bind --account a1 --name eps --path "$E" --transport headless; rc_is 3 "epsilon is a2's"
run unbind --account a2 --name g2; rc_is 0 "a2 lets epsilon go"
run bind --account a1 --name eps --path "$E" --transport headless; rc_is 0 "bind epsilon headless"
echo "Sync: connected, last synced 2 minutes ago" > "$T/cli-sync-status"
run guard --account a1 --name eps
has '.guard.code == "desktop-sync-reported"' "running app reporting Sync refuses"
echo "Sync is disabled for this vault" > "$T/cli-sync-status"
run guard --account a1 --name eps
has '.guard.code == "clear"' "running app reporting Sync off allows"
grep -q '^vault=e5|sync:status|$' "$T/cli.log" && pass || fail "sync:status argv"

# Bundled CLI: use it while Obsidian is already running, without global registration.
APP="$T/Obsidian.app"; mkdir -p "$APP/Contents/MacOS"
cp "$T/bin/obsidian" "$APP/Contents/MacOS/obsidian-cli"
unset OBSIDIAN_CLI
export BONFYRE_OBSIDIAN_APP="$APP" BONFYRE_OBSIDIAN_APP_RUNNING=1
run doctor --account user:19; rc_is 0 "bundled CLI with canonical account"
has '.cli.reachable == true and (.cli.path | endswith("/obsidian-cli"))' "bundled CLI discovered"
run search --account a1 --name delta --query milk; rc_is 0 "bundled CLI search"
has '.shape == "obsidian-cli" and .result == [{"file":"x.md","matches":1}]' "bundled CLI result"
# A bundled binary may return exit 0 but only print an enablement warning.
cat > "$APP/Contents/MacOS/obsidian-cli" <<'CLI_DISABLED'
#!/bin/sh
echo "Command line interface is not enabled. Please turn it on."
exit 0
CLI_DISABLED
chmod +x "$APP/Contents/MacOS/obsidian-cli"
run doctor --account user:19; rc_is 0 "disabled bundled CLI doctor"
has '.cli.reachable == false and ([.by_hand[].why] | any(test("not enabled")))' "disabled CLI never reports ready"
run search --account a1 --name delta --query milk
has '.answer.shape == "bonfyre-native"' "disabled bundled CLI returns native fallback"
export BONFYRE_OBSIDIAN_APP_RUNNING=0
: > "$T/cli.log"; run search --account a1 --name delta --query milk
has '.answer.shape == "bonfyre-native"' "bundled CLI does not start stopped app"
[ ! -s "$T/cli.log" ] && pass || fail "CLI did not launch app"
export BONFYRE_OBSIDIAN_APP="$T/NoApp.app" OBSIDIAN_CLI="$T/bin/obsidian"
# ---------------------------------------------------------------- doctor
export BONFYRE_OBSIDIAN_APP_RUNNING=0 OBSIDIAN_CLI="$T/bin/obsidian-absent"
run doctor --account a1; rc_is 0 "doctor"
has '.cli.registered == false and .headless.installed and .headless.version == "0.0.14" and .login.token_file_present == false' "doctor facts"
has "[.by_hand[].do] | any(test(\"HOME='$AH' ob login\"))" "doctor names the login command for the account"
has '[.by_hand[].do] | any(test("Command line interface"))' "doctor names the CLI setting"

# Find a user-scoped headless tool even when MCP PATH excludes the installer bin.
mkdir -p "$HOME/.vite-plus/bin"; cp "$T/bin/ob" "$HOME/.vite-plus/bin/ob"
unset BONFYRE_OBSIDIAN_OB
run doctor --account user:19; rc_is 0 "headless fallback doctor"
has '.headless.installed and (.headless.path | endswith("/.vite-plus/bin/ob")) and .headless.version == "0.0.14"' "headless discovered"
export BONFYRE_OBSIDIAN_OB="$T/bin/ob"
echo "obsidian_test: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
