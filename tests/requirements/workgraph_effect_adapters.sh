#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-effect-adapters}/adapters-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state/runtime"

make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null

claim_node() {
    mission=$1
    "$fabric" mission create "$mission" >/dev/null
    "$fabric" work add "$mission" task core.identity >/dev/null
    "$fabric" work claim "$mission" task "worker-$mission" --lease-ms 30000
}

compensate_node() {
    mission=$1
    effect_id=$2
    "$fabric" work cancel-node "$mission" task >/dev/null
    compensation_claim=$("$fabric" work compensation-claim "comp-$mission" \
        --mission "$mission" --lease-ms 30000)
    compensation_token=$(printf '%s\n' "$compensation_claim" | sed -n 's/^claim_token=//p')
    "$fabric" work compensate "$mission" task "comp-$mission" \
        "$compensation_token" "$effect_id" --result success >/dev/null
}

printf 'derive payload\n' > "$state/derive-input.txt"
claim=$(claim_node effect-derive)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare effect-derive task worker-effect-derive "$token" \
    derive-effect "$state/derive-output.txt" --adapter derive-file \
    --input-artifact-uri "$state/derive-input.txt" --verification-policy sha256 \
    --rollback-contract remove-created-target --authority worker-effect-derive >/dev/null
[ ! -e "$state/derive-output.txt" ]
"$fabric" work effect-commit effect-derive task worker-effect-derive \
    "$token" derive-effect >/dev/null
cmp "$state/derive-input.txt" "$state/derive-output.txt"
compensate_node effect-derive derive-effect
[ ! -e "$state/derive-output.txt" ]

printf 'publish payload\n' > "$state/publish-input.txt"
claim=$(claim_node effect-publish)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare effect-publish task worker-effect-publish "$token" \
    publish-effect "$state/publish-output.txt" --adapter publish-local \
    --input-artifact-uri "$state/publish-input.txt" --verification-policy sha256 \
    --rollback-contract remove-created-target --authority worker-effect-publish >/dev/null
"$fabric" work effect-commit effect-publish task worker-effect-publish \
    "$token" publish-effect >/dev/null
cmp "$state/publish-input.txt" "$state/publish-output.txt"
compensate_node effect-publish publish-effect
[ ! -e "$state/publish-output.txt" ]

printf 'archive payload\n' > "$state/archive-input.txt"
claim=$(claim_node effect-archive)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare effect-archive task worker-effect-archive "$token" \
    archive-effect "$state/archive-output.tar" --adapter archive-local \
    --input-artifact-uri "$state/archive-input.txt" --verification-policy exists \
    --rollback-contract remove-created-target --authority worker-effect-archive >/dev/null
"$fabric" work effect-commit effect-archive task worker-effect-archive \
    "$token" archive-effect >/dev/null
tar -tf "$state/archive-output.tar" | grep -qx 'archive-input.txt'
compensate_node effect-archive archive-effect
[ ! -e "$state/archive-output.tar" ]

claim=$(claim_node effect-alias)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
alias_uri='bonfyre://alias/workgraph-proof'
source_uri='bonfyre://artifact/source-proof'
"$fabric" work effect-prepare effect-alias task worker-effect-alias "$token" \
    alias-effect "$alias_uri" --adapter namespace-alias \
    --input-artifact-uri "$source_uri" --verification-policy resolve \
    --rollback-contract remove-owned-alias --authority worker-effect-alias >/dev/null
if "$fabric" namespace show "$alias_uri" >/dev/null 2>&1; then
    echo 'namespace alias existed before commit' >&2
    exit 1
fi
"$fabric" work effect-commit effect-alias task worker-effect-alias \
    "$token" alias-effect >/dev/null
"$fabric" namespace show "$alias_uri" | grep -q "^locator=$source_uri$"
compensate_node effect-alias alias-effect
if "$fabric" namespace show "$alias_uri" >/dev/null 2>&1; then
    echo 'namespace alias survived compensation' >&2
    exit 1
fi

repository="$state/repository"
worktree="$state/worktree"
mkdir -p "$repository"
git -C "$repository" init -q
git -C "$repository" config user.name 'Bonfyre Test'
git -C "$repository" config user.email 'bonfyre-test@local'
printf 'initial\n' > "$repository/tracked.txt"
git -C "$repository" add tracked.txt
git -C "$repository" commit -q -m initial
repository_head=$(git -C "$repository" rev-parse HEAD)

claim=$(claim_node effect-worktree)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare effect-worktree task worker-effect-worktree "$token" \
    worktree-effect "$worktree" --adapter git-worktree \
    --input-artifact-uri "$repository" --verification-policy git-head \
    --rollback-contract git-worktree-remove --authority worker-effect-worktree >/dev/null
[ ! -e "$worktree" ]
"$fabric" work effect-commit effect-worktree task worker-effect-worktree \
    "$token" worktree-effect >/dev/null
[ "$(git -C "$worktree" rev-parse HEAD)" = "$repository_head" ]
compensate_node effect-worktree worktree-effect
[ ! -e "$worktree" ]

printf 'candidate\n' > "$repository/tracked.txt"
claim=$(claim_node effect-candidate)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare effect-candidate task worker-effect-candidate "$token" \
    candidate-effect "$repository" --adapter git-candidate-commit \
    --input-artifact-uri 'Bonfyre candidate commit' --verification-policy git-head-changed \
    --rollback-contract git-reset-prepared-head --authority worker-effect-candidate >/dev/null
"$fabric" work effect-commit effect-candidate task worker-effect-candidate \
    "$token" candidate-effect >/dev/null
candidate_head=$(git -C "$repository" rev-parse HEAD)
[ "$candidate_head" != "$repository_head" ]
[ "$(git -C "$repository" show HEAD:tracked.txt)" = 'candidate' ]
compensate_node effect-candidate candidate-effect
[ "$(git -C "$repository" rev-parse HEAD)" = "$repository_head" ]
[ "$(git -C "$repository" show HEAD:tracked.txt)" = 'initial' ]

for mission in effect-derive effect-publish effect-archive effect-alias effect-worktree effect-candidate; do
    "$fabric" work status "$mission" task | grep -q '^status=cancelled$'
done
