#!/bin/sh
# Golden output captured from the retired JS audit with its missing index
# loader replaced by reading the native-exported fixture index. Only that
# loader and fixture root differ; the audit body remains the original oracle.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
organ=$(dirname "$here")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/architecture" "$tmp/bin"
cp "$here/../../../architecture/atlas-fields.cue" "$tmp/architecture/"
export BONFYRE_ATLAS_DIR="$tmp/architecture" BONFYRE_BIN="$tmp/bin"
for family in Alpha Bravo CMS Delta Echo Empty; do
  mkdir -p "$tmp/cmd/Bonfyre$family/src" "$tmp/components/Bonfyre$family/src"
  printf 'source\n' > "$tmp/cmd/Bonfyre$family/src/main.c"
  printf 'component\n' > "$tmp/components/Bonfyre$family/src/main.c"
done
mkdir -p "$tmp/cmd/BonfyreNoTwin" "$tmp/components/BonfyreNoCommand"
ln -s BonfyreAlpha "$tmp/cmd/BonfyreSymlink"
mkdir -p "$tmp/components/BonfyreSymlink"
printf 'TARGET := bonfyre-bravo\nSRC = ../../components/BonfyreBravo/src/main.c\n' > "$tmp/cmd/BonfyreBravo/Makefile"
printf 'BIN ?= bonfyre-cms\nSOURCES = src/main.c\n' > "$tmp/cmd/BonfyreCMS/Makefile"
printf 'BINARY = bonfyre-echo\nSRCS = src/../src/main.c src/missing.c\n' > "$tmp/cmd/BonfyreEcho/Makefile"
printf 'TARGET = bonfyre-empty\nSOURCE = include/only.h\n' > "$tmp/cmd/BonfyreEmpty/Makefile"
printf 'cmd binary\n' > "$tmp/cmd/BonfyreAlpha/bonfyre-alpha"
cp "$tmp/cmd/BonfyreAlpha/bonfyre-alpha" "$tmp/bin/bonfyre-alpha"
printf 'component binary\n' > "$tmp/components/BonfyreCMS/bonfyre-cms"
cp "$tmp/components/BonfyreCMS/bonfyre-cms" "$tmp/bin/bonfyre-cms"
printf 'old build\n' > "$tmp/bin/bonfyre-bravo"
printf 'another build\n' > "$tmp/bin/bonfyre-echo"
touch -t 200001010000 "$tmp/cmd/BonfyreEcho/src/main.c"
touch -t 199001010000 "$tmp/bin/bonfyre-bravo"
cat > "$tmp/architecture/build-edges.yaff" <<'EOF'
atlas build-edges-fixture
architecture cmd-alpha
  source_path cmd/BonfyreAlpha/src/main.c
architecture cmd-bravo
  source_path cmd/BonfyreBravo/src/main.c
architecture cmd-c-m-s
  source_path cmd/BonfyreCMS/src/main.c
architecture cmd-echo
  source_path cmd/BonfyreEcho/src/main.c
  source_path cmd/BonfyreEcho/src/missing.c
architecture cmd-empty
  source_path cmd/BonfyreEmpty/include/only.h
EOF
"$organ/bonfyre-atlas" export > /dev/null
audit() { "$organ/bonfyre-atlas" build-edges "$@"; }
audit --json > "$tmp/result.json"
diff -u "$here/build-edges.json" "$tmp/result.json"
cue vet "$organ/build-edges.cue" "$tmp/result.json"
audit > "$tmp/text"
diff -u "$here/build-edges.txt" "$tmp/text"
if audit --json --strict > "$tmp/strict"; then exit 1; else status=$?; fi
[ "$status" -eq 1 ]
cmp "$tmp/result.json" "$tmp/strict"
if audit --unknown > "$tmp/out" 2> "$tmp/error"; then exit 1; else status=$?; fi
[ "$status" -eq 2 ]
grep -F 'usage: bonfyre-atlas build-edges' "$tmp/error" > /dev/null
sed 's@source_path cmd/BonfyreBravo/src/main.c@source_path components/BonfyreBravo/src/main.c@' "$tmp/architecture/build-edges.yaff" > "$tmp/fixed"
mv "$tmp/fixed" "$tmp/architecture/build-edges.yaff"
audit --strict > "$tmp/out"
grep -F '0 Atlas entries name a source the build does not use' "$tmp/out" > /dev/null

# A command wrapper may delegate the actual compile to its component owner.
# Its own src/main.c remains visible as a twin, but is not the built source.
mkdir -p "$tmp/cmd/BonfyreOwner/src" "$tmp/components/BonfyreOwner/src"
printf 'unbuilt command twin\n' > "$tmp/cmd/BonfyreOwner/src/main.c"
printf 'owner source\n' > "$tmp/components/BonfyreOwner/src/main.c"
cat > "$tmp/cmd/BonfyreOwner/Makefile" <<'EOF'
TARGET = bonfyre-owner
OWNER_DIR = ../../components/BonfyreOwner
all:
	$(MAKE) -C $(OWNER_DIR) $(TARGET)
EOF
cat >> "$tmp/architecture/build-edges.yaff" <<'EOF'
architecture cmd-owner
  source_path components/BonfyreOwner/src/main.c
