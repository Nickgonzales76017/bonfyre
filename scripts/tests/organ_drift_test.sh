#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SCRIPT="$ROOT/scripts/organ_drift.sh"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-organ-drift-test.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
TREE="$TMP/tree"
MANAGED="$TMP/managed"
mkdir -p "$TREE/cmd/BonfyreFPQ" "$TREE/cmd/BonfyreMoQ" "$TREE/cmd/BonfyreCLI" "$MANAGED"

cat > "$TREE/cmd/BonfyreFPQ/Makefile" <<'EOF'
TARGET := bonfyre-fpq
EOF
printf '#!/bin/sh\necho current\n' > "$TREE/cmd/BonfyreFPQ/bonfyre-fpq"
chmod 755 "$TREE/cmd/BonfyreFPQ/bonfyre-fpq"
cat > "$TREE/cmd/BonfyreMoQ/Makefile" <<'EOF'
TARGET = bonfyre-moq
EOF
printf '#!/bin/sh\necho moq\n' > "$TREE/cmd/BonfyreMoQ/bonfyre-moq"
chmod 755 "$TREE/cmd/BonfyreMoQ/bonfyre-moq"
cat > "$TREE/cmd/BonfyreCLI/Makefile" <<'EOF'
TARGET = bonfyre
EOF
printf '#!/bin/sh\necho cli\n' > "$TREE/cmd/BonfyreCLI/bonfyre"
chmod 755 "$TREE/cmd/BonfyreCLI/bonfyre"
printf '#!/bin/sh\necho stale\n' > "$MANAGED/bonfyre-fpq"
chmod 755 "$MANAGED/bonfyre-fpq"

# Inspection sees drift and never mutates a managed carrier.
report=$("$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --json)
printf '%s' "$report" | grep -q '"behind":1'
printf '%s' "$report" | grep -q '"missing":2'
printf '%s' "$report" | grep -q '"promoted":0'
grep -q stale "$MANAGED/bonfyre-fpq"

# Target spelling comes from Makefile, and sync only promotes after a matching
# staged digest is copied into the managed directory.
report=$("$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync --json)
printf '%s' "$report" | grep -q '"promoted":1'
printf '%s' "$report" | grep -q '"behind":1'
grep -q current "$MANAGED/bonfyre-fpq"
[ ! -e "$MANAGED/bonfyre-moq" ]
[ ! -e "$MANAGED/bonfyre" ]
grep -q '"schema":"bonfyre.organ-runtime-image.v1"' "$MANAGED/.bonfyre-organ-manifest.json"
grep -Eq '"target":"bonfyre-fpq","sha256":"[0-9a-f]{64}","state":"promoted"' "$MANAGED/.bonfyre-organ-manifest.json"
if "$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --verify --json >/dev/null 2>&1; then
    echo "expected an absent managed organ to fail verification" >&2
    exit 1
fi

# A newer checkout invalidates the runtime image. Verify cannot repair it.
printf '#!/bin/sh\necho newer\n' > "$TREE/cmd/BonfyreFPQ/bonfyre-fpq"
chmod 755 "$TREE/cmd/BonfyreFPQ/bonfyre-fpq"
if "$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --verify >/dev/null 2>&1; then
    echo "expected stale managed image to fail verification" >&2
    exit 1
fi
grep -q current "$MANAGED/bonfyre-fpq"
# A narrow promotion leaves every other installed or missing organ alone.
report=$("$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync --only bonfyre-fpq --json)
printf '%s' "$report" | grep -q '"expected":1'
printf '%s' "$report" | grep -q '"promoted":1'
grep -q newer "$MANAGED/bonfyre-fpq"
[ ! -e "$MANAGED/bonfyre-moq" ]
[ ! -e "$MANAGED/bonfyre" ]
# The runtime-image manifest must remain a complete installed estate
manifest="$MANAGED/.bonfyre-organ-manifest.json"
[ "$(grep -o '\"target\":' "$manifest" | wc -l | tr -d '[:space:]')" = 3 ] || { echo 'narrow promotion truncated manifest' >&2; exit 1; }
grep -q '\"target\":\"bonfyre-moq\",\"sha256\":\"\",\"state\":\"missing\"' "$manifest" || { echo 'runtime manifest lost bonfyre-moq missing organ' >&2; exit 1; }
grep -q '\"target\":\"bonfyre\",\"sha256\":\"\",\"state\":\"missing\"' "$manifest" || { echo 'runtime manifest lost bonfyre CLI missing organ' >&2; exit 1; }
if "$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync --only bonfyre-unknown >"$TMP/refused.stdout" 2>"$TMP/refused.stderr"; then
    echo "unknown target was silently accepted" >&2
    exit 1
fi
grep -q 'no built executable' "$TMP/refused.stderr"
if "$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync --only ../unsafe >/dev/null 2>&1; then
    echo "unsafe target unexpectedly accepted" >&2
    exit 1
fi
"$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync >/dev/null
"$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --sync --install-missing --json | grep -q '"installed":2'
"$SCRIPT" --root "$TREE" --managed-dir "$MANAGED" --verify >/dev/null
grep -q newer "$MANAGED/bonfyre-fpq"
grep -q moq "$MANAGED/bonfyre-moq"
grep -q cli "$MANAGED/bonfyre"

echo "organ_drift_test: ok"
