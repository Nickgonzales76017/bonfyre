#!/usr/bin/env bash
# Pinned public companion repositories. No automatic source promotion or writes.
# Compatible with macOS Bash 3.2. No recursive clone of historical dependencies.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
manifest="$root/.gitmodules"
action="${1:-list}"
group="${2:-all}"

usage() {
  cat <<'HELP'
Usage: scripts/federated_modules.sh list|verify|status|diff [group]
       scripts/federated_modules.sh init GROUP

Groups: products | extractions | examples | aurekai | lineage | upstream | all

These are pinned public companion checkouts, NOT replacement runtime owners.
The normal Bonfyre checkout and build need none of the optional groups.
Use "init products" first. "all" includes older, larger source-history repos.
Init is explicit; no automatic remote-head upgrades or recursive nested clones.
HELP
}

case "$action" in
  list|verify|status|diff|init) ;;
  -h|--help|help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac
case "$group" in
  products|extractions|examples|aurekai|lineage|upstream|all) ;;
  *) printf "Unknown submodule group: %s\n" "$group" >&2; usage >&2; exit 2 ;;
esac
if [[ "$action" == init && $# -lt 2 ]]; then
  printf "Choose one group explicitly (start with: init products).\n" >&2
  exit 2
fi
[[ -r "$manifest" ]] || { printf "Missing %s\n" "$manifest" >&2; exit 2; }

selected() {
  local path="$1"
  case "$group" in
    all) return 0 ;;
    products) [[ "$path" == integrations/federated/products/* ]] ;;
    extractions) [[ "$path" == integrations/federated/extractions/* ]] ;;
    examples) [[ "$path" == examples/federated/* ]] ;;
    aurekai) [[ "$path" == integrations/federated/aurekai/* ]] ;;
    lineage) [[ "$path" == integrations/federated/lineage/* ]] ;;
    upstream) [[ "$path" == vendor/hvm4 ]] ;;
  esac
}

paths=()
keys=()
while IFS=" " read -r key path; do
  [[ -n "$path" ]] || continue
  if selected "$path"; then
    paths+=("$path")
    keys+=("$key")
  fi
done < <(git -C "$root" config -f "$manifest" --get-regexp '^submodule\..*\.path$')

if [[ ${#paths[@]} -eq 0 ]]; then
  printf "No configured submodules in group %s\n" "$group" >&2
  exit 2
fi

# Refuse stale/non-public URLs, missing gitlinks and any mismatch of HEAD pins.
for ((i=0; i<${#paths[@]}; i++)); do
  path="${paths[i]}"
  key="${keys[i]%.path}"
  url="$(git -C "$root" config -f "$manifest" --get "$key.url")"
  case "$url" in
    https://github.com/Nickgonzales76017/*.git|https://github.com/aurekai/*.git|https://github.com/HigherOrderCO/HVM4.git) ;;
    *) printf "Refused unapproved remote for %s: %s\n" "$path" "$url" >&2; exit 2 ;;
  esac
  row="$(git -C "$root" ls-tree -r HEAD -- "$path")"
  read -r mode type revision actual <<< "$row"
  if [[ "$mode" != 160000 || "$type" != commit || ! "$revision" =~ ^[0-9a-f]{40}$ || "$actual" != "$path" ]]; then
    printf "Unpinned or mismatched gitlink: %s\n" "$path" >&2
    exit 2
  fi
done

case "$action" in
  list|verify)
    for ((i=0; i<${#paths[@]}; i++)); do
      path="${paths[i]}"
      row="$(git -C "$root" ls-tree -r HEAD -- "$path")"
      read -r _ _ revision _ <<< "$row"
      printf "%-68s  %.12s\n" "$path" "$revision"
    done
    printf "%s verified pinned gitlinks in %s\n" "${#paths[@]}" "$group"
    ;;
  status)
    git -C "$root" submodule status -- "${paths[@]}"
    ;;
  diff)
    git -C "$root" diff --submodule=log -- "${paths[@]}"
    ;;
  init)
    printf "Initializing %s verified, pinned public submodules in %s\n" "${#paths[@]}" "$group"
    git -C "$root" submodule sync -- "${paths[@]}"
    git -C "$root" submodule update --init --jobs 4 -- "${paths[@]}"
    git -C "$root" submodule status -- "${paths[@]}"
    ;;
esac
