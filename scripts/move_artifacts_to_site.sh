#!/usr/bin/env bash
set -euo pipefail

usage(){
  cat <<'EOF'
Usage: move_artifacts_to_site.sh --map MAP.json [--dry-run]

Copies demo artifacts from repo root `site/demos/<demo>` into each Pages repo's `site/` folder
based on the mappings in MAP.json. Commits and pushes changes unless --dry-run is provided.
EOF
}

DRY_RUN=0
MAP=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --map) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; MAP="$2"; shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [ -z "$MAP" ]; then
  usage; exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq required" >&2; exit 1
fi

if ! jq -e '(.mappings | type == "array") and
  all(.mappings[]; (.demo_dir | type == "string" and length > 0) and
    (.remote | type == "string" and length > 0) and
    (.paths | type == "array" and all(.[]; type == "string" and length > 0)))' "$MAP" >/dev/null; then
  echo "Invalid artifact mapping: $MAP" >&2
  exit 2
fi
BASE_PROJECTS_DIR="${BONFYRE_PAGES_PROJECTS_DIR:-/Users/$(whoami)/Projects}"

while IFS= read -r m; do
  IFS= read -r -d '' demo_dir < <(printf '%s\n' "$m" | jq -j '.demo_dir, "\u0000"')
  IFS= read -r -d '' remote < <(printf '%s\n' "$m" | jq -j '.remote, "\u0000"')
  repo_basename=$(basename "$remote" .git)
  case "$repo_basename" in ''|'.'|'..') echo "Invalid remote repository name" >&2; exit 2;; esac
  repo_path="$BASE_PROJECTS_DIR/$repo_basename"
  site_dir="$repo_path/site"
  printf '\n== %s -> %s/site ==\n' "$demo_dir" "$repo_basename"

  if [ ! -d "$repo_path" ]; then
    echo "Local repo missing: $repo_path" >&2
    continue
  fi
  if [ ! -d "$site_dir" ]; then
    echo "Site dir missing: $site_dir; creating" 
    if [ "$DRY_RUN" -eq 0 ]; then mkdir -p "$site_dir"; fi
  fi

  # copy listed paths (preserve directory structure)
  changed=0
  while IFS= read -r -d '' p; do
    src="$demo_dir/$p"
    if [ ! -e "$src" ]; then
      echo "Missing source: $src (skipping)"
      continue
    fi
    # if src is a directory, copy its contents into site/<basename(p)>
    if [ -d "$src" ]; then
      dest_dir="$site_dir/$(basename "$p")"
      if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRY RUN: would copy dir $src -> $dest_dir"
        changed=1
        continue
      fi
      mkdir -p "$dest_dir"
      cp -R "$src"/. "$dest_dir/"
      echo "Copied dir $src -> $dest_dir"
      changed=1
    else
      # src is a file; copy to site/<filename>
      filename=$(basename "$p")
      dest_file="$site_dir/$filename"
      if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRY RUN: would copy file $src -> $dest_file"
        changed=1
        continue
      fi
      mkdir -p "$(dirname "$dest_file")"
      cp "$src" "$dest_file"
      echo "Copied file $src -> $dest_file"
      # remove legacy directory created by previous incorrect runs, if present
      legacy_dir="$site_dir/$filename"
      if [ -d "$legacy_dir" ] && [ -f "$legacy_dir/$filename" ]; then
        rm -rf "$legacy_dir"
        echo "Removed legacy directory $legacy_dir"
      fi
      changed=1
    fi
  done < <(printf '%s\n' "$m" | jq -j '.paths[] | ., "\u0000"')

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "DRY RUN: would commit/push in $repo_path"
    continue
  fi

  if [ "$changed" -eq 1 ]; then
    pushd "$repo_path" >/dev/null
    git add site || true
    git commit -m "Add demo artifacts into site/ for Pages" || echo "No changes to commit"
    git push origin HEAD || echo "Push failed for $repo_basename"
    popd >/dev/null
  else
    echo "No artifacts copied for $repo_basename"
  fi
done < <(jq -c '.mappings[]' "$MAP")

echo "Done."
