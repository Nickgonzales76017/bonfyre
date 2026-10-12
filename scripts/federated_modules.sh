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

Groups: products | extractions | examples | aurekai | lineage | frappe | interfaces | upstream | all | one PATH

These are pinned public companion checkouts, NOT replacement runtime owners.
The normal Bonfyre checkout and build need none of the optional groups.
Use "init products" first. "one PATH" selects one exact module.
Large donor trees require BONFYRE_ALLOW_LARGE_MODULES=1 when initializing
all, frappe or interfaces. No large group is fetched by default.
Init is explicit; no remote-head upgrades. Only extraction dependencies recurse.
HELP
}

case "$action" in
  list|verify|status|diff|init) ;;
  -h|--help|help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac
case "$group" in
  products|extractions|examples|aurekai|lineage|frappe|interfaces|upstream|one|all) ;;
  *) printf "Unknown submodule group: %s\n" "$group" >&2; usage >&2; exit 2 ;;
esac
if [[ "$action" == init && $# -lt 2 ]]; then
  printf "Choose one group explicitly (start with: init products).\n" >&2
  exit 2
fi
if [[ "$group" == one ]]; then
  [[ $# -eq 3 ]] || { printf "Specify the exact submodule path after one.\n" >&2; exit 2; }
  only_path="$3"
fi
if [[ "$action" == init ]]; then
  case "$group" in
    all|frappe|interfaces)
      [[ "${BONFYRE_ALLOW_LARGE_MODULES:-0}" == 1 ]] || {
        printf "Refused large optional donor checkout. Set BONFYRE_ALLOW_LARGE_MODULES=1 to opt in.\n" >&2
        exit 2
      } ;;
  esac
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
    frappe) [[ "$path" == integrations/federated/frappe/* ]] ;;
    interfaces) [[ "$path" == integrations/federated/interfaces/* ]] ;;
    one) [[ "$path" == "$only_path" ]] ;;
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
    https://github.com/Nickgonzales76017/*.git|https://github.com/aurekai/*.git|https://github.com/HigherOrderCO/HVM4.git|https://github.com/frappe/*.git|https://github.com/anomalyco/opentui.git|https://github.com/modelcontextprotocol/ext-apps.git|https://github.com/a2ui-project/a2ui.git|https://github.com/vercel-labs/json-render.git|https://github.com/charmbracelet/*.git|https://github.com/ratatui/ratatui.git|https://github.com/TryGhost/Ghost.git) ;;
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
    # Only vetted extraction repos need nested pinned core/Lambda source.
    # Never recurse into the older lineage or large upstream donors.
    recursive=false
    case "$group" in
      extractions) recursive=true ;;
      one)
        case "$only_path" in
          integrations/federated/extractions/*) recursive=true ;;
        esac
        ;;
    esac
    if [[ "$recursive" == true ]]; then
      git -C "$root" submodule update --init --recursive --jobs 4 -- "${paths[@]}"
    else
      git -C "$root" submodule update --init --jobs 4 -- "${paths[@]}"
    fi
    git -C "$root" submodule status -- "${paths[@]}"
    ;;
esac
