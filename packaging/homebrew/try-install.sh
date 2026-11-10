#!/bin/sh
# Proves the recipe by using it: packs the compiler's source set, installs it
# through Homebrew from a throwaway local tap, runs the formula's own test and
# the installed binary's standalone check, then removes everything it added.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
command -v brew >/dev/null || { echo "SKIP: no Homebrew"; exit 0; }
work=$(mktemp -d); tap=bonfyre-try/recipe
cleanup() { brew uninstall --formula bonfyre-frappe-compiler >/dev/null 2>&1 || true; brew untap "$tap" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT INT TERM
src="$work/bonfyre-0.0.0"
mkdir -p "$src/cmd/BonfyreLex" "$src/lib/libbonfyre/src" "$src/engine/core"
rsync -a --exclude build --exclude bonfyre-frappe-compiler "$repo/cmd/BonfyreFrappeCompiler" "$src/cmd/"
cp -R "$repo/cmd/BonfyreLex/src" "$src/cmd/BonfyreLex/"
cp -R "$repo/lib/libbonfyre/include" "$src/lib/libbonfyre/"
cp "$repo/lib/libbonfyre/bf_jsonw.c" "$repo/lib/libbonfyre/bf_json.c" "$src/lib/libbonfyre/"
cp "$repo/lib/libbonfyre/src/bonfyre_frappe_compiler.c" "$repo/lib/libbonfyre/src/bonfyre_frappe_condition.c" "$repo/lib/libbonfyre/src/bf_sha256.c" "$src/lib/libbonfyre/src/"
cp -R "$repo/engine/core/include" "$src/engine/core/"
cp "$repo/LICENSE" "$src/"
tar -C "$work" -czf "$work/bonfyre-0.0.0.tar.gz" bonfyre-0.0.0
sum=$(shasum -a 256 "$work/bonfyre-0.0.0.tar.gz" | cut -d' ' -f1)
export HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_CLEANUP=1 HOMEBREW_NO_ENV_HINTS=1
brew tap-new --no-git "$tap" >/dev/null
formula="$(brew --repository "$tap")/Formula/bonfyre-frappe-compiler.rb"
sed -e "s|@URL@|file://$work/bonfyre-0.0.0.tar.gz|" -e "s|@SHA256@|$sum|" -e "s|@VERSION@|0.0.0|" "$repo/packaging/homebrew/bonfyre-frappe-compiler.rb.template" > "$formula"
brew install --build-from-source "$tap/bonfyre-frappe-compiler" >"$work/install.log" 2>&1 || { tail -20 "$work/install.log"; echo "FAIL: install"; exit 1; }
brew test "$tap/bonfyre-frappe-compiler" >"$work/test.log" 2>&1 || { tail -20 "$work/test.log"; echo "FAIL: formula test"; exit 1; }
installed=$(brew --prefix)/bin/bonfyre-frappe-compiler
[ -x "$installed" ] || { echo "FAIL: nothing on the prefix's bin"; exit 1; }
sh "$repo/cmd/BonfyreFrappeCompiler/tests/standalone_check.sh" "$installed"
echo "files installed: $(brew list --formula bonfyre-frappe-compiler | wc -l | tr -d ' ')"
otool -L "$installed" | tail -n +2 | awk '{print "links " $1}'
echo "PASS: installed through Homebrew from a $(du -k "$work/bonfyre-0.0.0.tar.gz" | cut -f1) KB source tarball, tested, and removed"
