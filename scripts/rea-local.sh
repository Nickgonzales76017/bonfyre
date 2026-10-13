#!/usr/bin/env bash
# Pinned, isolated REA workstation adapter. Never scans or uploads an estate.
# Keep analyzer state/credentials distinct from Bonfyre's production runtime.
set -euo pipefail
umask 077
if [ "$#" -eq 0 ]; then set -- help; fi
action="$1"; shift
version=6.4.0
node_version=24.11.0
root="$HOME/.local/share/bonfyre/rea"
if [ -n "$*" ] && [ "$action" = install ]; then
  echo "install accepts no implicit analysis targets" >&2; exit 2
fi
if [ -n "$(printenv BONFYRE_REA_ROOT || true)" ]; then root="$BONFYRE_REA_ROOT"; fi
prefix="$root/releases/$version"
cli="$prefix/node_modules/rea-agents/scripts/rea.mjs"

private_node="$root/toolchains/node-v$node_version/bin/node"
rea_node=""

supported_node() {
  [ -f "$1" ] && [ -x "$1" ] && "$1" -e '
    const [major,minor]=process.versions.node.split(".").map(Number);
    if(!((major===22&&minor>=19)||(major===24&&minor>=11)||(major>=26)))
      process.exit(1);
  ' >/dev/null 2>&1
}

# Only the pinned Node runtime is downloaded, into this user's private tools
# directory. The official release's HTTPS SHA-256 manifest is checked before
# extraction; no sudo, global package manager or shell PATH mutation occurs.
bootstrap_node() (
  set -euo pipefail
  if [ -L "$root" ] || [ -L "$root/toolchains" ] || [ -L "${private_node%/bin/node}" ]; then
    echo "refusing symlinked REA Node installation root" >&2; exit 2
  fi
  local os arch platform archive dist stage expected actual node_dir
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Darwin:arm64) platform=darwin-arm64 ;;
    Darwin:x86_64) platform=darwin-x64 ;;
    Linux:aarch64|Linux:arm64) platform=linux-arm64 ;;
    Linux:x86_64) platform=linux-x64 ;;
    *) echo "unsupported private REA Node host: $os/$arch" >&2; exit 2 ;;
  esac
  command -v curl >/dev/null && command -v shasum >/dev/null && command -v tar >/dev/null || {
    echo "curl, shasum and tar are required for private Node bootstrap" >&2; exit 2;
  }
  mkdir -p "$root/toolchains"
  node_dir="$root/toolchains/node-v$node_version"
  if [ -e "$node_dir" ]; then
    echo "private Node prefix exists but is not runnable: $node_dir" >&2; exit 2
  fi
  dist="node-v$node_version-$platform"
  archive="$dist.tar.xz"
  stage="$(mktemp -d "$root/toolchains/.node-stage.XXXXXXXX")"
  trap 'rm -rf -- "$stage"' EXIT
  local url="https://nodejs.org/dist/v$node_version"
  curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 240 \
    "$url/SHASUMS256.txt" -o "$stage/SHASUMS256.txt"
  curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 240 \
    "$url/$archive" -o "$stage/$archive"
  expected="$(awk -v archive="$archive" '$2==archive && length($1)==64 {print $1}' "$stage/SHASUMS256.txt")"
  [ "${#expected}" -eq 64 ] || { echo "Node release checksum unavailable" >&2; exit 2; }
  actual="$(shasum -a 256 "$stage/$archive" | awk '{print $1}')"
  [ "$actual" = "$expected" ] || { echo "Node release SHA-256 mismatch" >&2; exit 2; }
  tar -xJf "$stage/$archive" -C "$stage"
  [ -x "$stage/$dist/bin/node" ] || { echo "incomplete private Node archive" >&2; exit 2; }
  "$stage/$dist/bin/node" -e '
    if(process.version!=="v24.11.0")process.exit(1)
  ' || { echo "unexpected Node version in release archive" >&2; exit 2; }
  mv "$stage/$dist" "$node_dir"
  echo "REA private Node v$node_version verified and installed in $node_dir"
)

need_node() {
  if [ -n "${BONFYRE_REA_NODE_BIN:-}" ]; then
    supported_node "$BONFYRE_REA_NODE_BIN" || {
      echo "BONFYRE_REA_NODE_BIN is not a compatible Node executable" >&2; exit 2;
    }
    rea_node="$BONFYRE_REA_NODE_BIN"
    return
  fi
  # If the isolated distribution exists, keep runtime and npm ABI on that
  # same Node instead of silently returning to a differently packaged PATH node.
  if [ "${BONFYRE_REA_FORCE_PRIVATE_NODE:-0}" = 1 ]; then
    if supported_node "$private_node"; then rea_node="$private_node"; return; fi
  else
    if supported_node "$private_node"; then rea_node="$private_node"; return; fi
    if command -v node >/dev/null; then
      local available
      available="$(command -v node)"
      if supported_node "$available"; then rea_node="$available"; return; fi
    fi
  fi
  if [ "$action" = install ]; then
    bootstrap_node
    supported_node "$private_node" || {
      echo "private Node bootstrap did not produce a runnable binary" >&2; exit 2;
    }
    rea_node="$private_node"
    return
  fi
  echo "REA needs Node 22.19+, 24.11+, or 26+; run: bash scripts/rea-local.sh install" >&2
  exit 2
}

need_rea() {
  need_node
  if [ ! -f "$cli" ] || [ -L "$cli" ]; then
    echo "REA 6.4.0 not installed in isolated prefix. Run: bash scripts/rea-local.sh install" >&2
    exit 2
  fi
}
rea() { auto_native_env; "$rea_node" "$cli" "$@"; }

# Private, opt-in REA native analysis dependencies: no system installations.
# Native decompilation needs substantially more disk than the installed REA
# runtime. An optional existing volume keeps the 4-GiB admission test intact
# without moving/reinstalling REA or touching the dirty Bonfyre checkout.
native_root="${BONFYRE_REA_NATIVE_ROOT:-$root}"
ghidra_dir="$native_root/engines/ghidra_12.1.4_PUBLIC"
jdk_dir="$native_root/toolchains/jdk-21.0.12.1+1"
ghidra_native=mac_arm_64

check_native_root() {
  if [ -n "${BONFYRE_REA_NATIVE_ROOT:-}" ]; then
    case "$native_root" in
      /*) ;;
      *) echo "BONFYRE_REA_NATIVE_ROOT must be an existing absolute directory" >&2; return 2 ;;
    esac
    if [ ! -d "$native_root" ] || [ -L "$native_root" ]; then
      echo "native analysis root absent or symlinked; refusing to create a new volume path" >&2
      return 2
    fi
    # /Volumes/Name can be a plain directory when the real drive is not mounted:
    # never download several gigabytes into such a fallback on the system disk.
    case "$native_root" in
      /Volumes/*)
        local suffix volume root_device home_device
        suffix="${native_root#/Volumes/}"
        volume="/Volumes/${suffix%%/*}"
        if [ ! -d "$volume" ] || [ -L "$volume" ]; then
          echo "selected external volume is not mounted" >&2; return 2
        fi
        root_device="$(stat -f %d "$volume")" || return 2
        home_device="$(stat -f %d "$HOME")" || return 2
        if [ "$root_device" = "$home_device" ]; then
          echo "selected /Volumes path is on the system disk, not a mounted external drive" >&2
          return 2
        fi
        ;;
    esac
  fi
}


auto_native_env() {
  if [ -z "${GHIDRA_INSTALL_DIR:-}" ] && [ -f "$ghidra_dir/Ghidra/application.properties" ]; then
    export GHIDRA_INSTALL_DIR="$ghidra_dir"
  fi
  if [ -z "${JAVA_HOME:-}" ] && [ -x "$jdk_dir/Contents/Home/bin/javac" ]; then
    export JAVA_HOME="$jdk_dir/Contents/Home"
  fi
}

fetch_verified() {
  local url="$1" expected="$2" path="$3" actual
  curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL --retry 3 \
    --connect-timeout 20 --max-time 1200 "$url" -o "$path"
  actual="$(shasum -a 256 "$path" | awk '{print $1}')"
  [ "$actual" = "$expected" ] || {
    echo "REA native dependency SHA-256 mismatch: $(basename "$path")" >&2
    return 2
  }
}

# Ghidra's supported buildNatives task emits into build/os; REA admits both
# that source-owned location and prebuilt os. Never duplicate built binaries.
native_ready() {
  local dir="$1/Ghidra/Features/Decompiler" platform="$ghidra_native"
  { [ -x "$dir/os/$platform/decompile" ] || [ -x "$dir/build/os/$platform/decompile" ]; } &&
  { [ -x "$dir/os/$platform/sleigh" ] || [ -x "$dir/build/os/$platform/sleigh" ]; }
}

# This explicit action downloads pinned vendor releases, then uses Ghidra's
# supported native build task for macOS ARM64. The public zip alone cannot
# satisfy REA's decompiler check on this host.
install_native_ghidra() {
  [ "$(uname -s):$(uname -m)" = Darwin:arm64 ] || {
    echo "private Ghidra setup currently supports macOS arm64" >&2; return 2
  }
  for cmd in curl shasum tar unzip make clang++; do
    command -v "$cmd" >/dev/null || { echo "Ghidra prerequisite missing: $cmd" >&2; return 2; }
  done
  check_native_root || return 2
  if [ -L "$native_root" ] || [ -L "$native_root/toolchains" ] ||
     [ -L "$native_root/engines" ] || [ -L "$ghidra_dir" ] ||
     [ -L "$jdk_dir" ]; then
    echo "refusing symlinked private native-toolchain roots" >&2; return 2
  fi
  mkdir -p "$native_root/toolchains" "$native_root/engines"
  if [ ! -x "$jdk_dir/Contents/Home/bin/javac" ] || [ ! -d "$ghidra_dir" ]; then
    # The GitHub ZIP, full JDK, native build and Gradle workspace expand well
    # beyond download size. Refuse an unsafe low-disk bootstrap before fetching.
    local available_kib
    available_kib="$(df -Pk "$native_root" | awk 'END {print $4}')"
    case "$available_kib" in ''|*[!0-9]*) echo "unable to measure native bootstrap free disk" >&2; return 2;; esac
    if [ "$available_kib" -lt 4194304 ]; then
      echo "native REA setup requires 4 GiB available disk; no downloads started" >&2
      return 2
    fi
  fi
  if [ ! -x "$jdk_dir/Contents/Home/bin/javac" ]; then
    [ ! -e "$jdk_dir" ] || { echo "incomplete private JDK; refusing overwrite" >&2; return 2; }
    (
      set -e
      stage="$(mktemp -d "$native_root/toolchains/.jdk-stage.XXXXXXXX")"
      trap 'rm -rf -- "$stage"' EXIT
      archive=OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.12.1_1.tar.gz
      fetch_verified "https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/$archive" \
        3623232f33a9c3baadf304480b2535f9a3cba8a58d42ecbb438ba267315d9998 "$stage/$archive"
      tar -xzf "$stage/$archive" -C "$stage"
      test -x "$stage/jdk-21.0.12.1+1/Contents/Home/bin/java"
      test -x "$stage/jdk-21.0.12.1+1/Contents/Home/bin/javac"
      "$stage/jdk-21.0.12.1+1/Contents/Home/bin/javac" -version
      mv "$stage/jdk-21.0.12.1+1" "$jdk_dir"
    )
  fi
  auto_native_env
  [ -x "$JAVA_HOME/bin/javac" ] || { echo "full JDK 21 required" >&2; return 2; }
  if [ -e "$ghidra_dir" ]; then
    native_ready "$ghidra_dir" || {
      echo "incomplete private Ghidra; refusing overwrite" >&2; return 2
    }
  else
    (
      set -e
      stage="$(mktemp -d "$native_root/engines/.ghidra-stage.XXXXXXXX")"
      trap 'rm -rf -- "$stage"' EXIT
      archive=ghidra_12.1.4_PUBLIC_20260921.zip
      fetch_verified "https://github.com/NationalSecurityAgency/ghidra/releases/download/Ghidra_12.1.4_build/$archive" \
        ddac49f903da9d5bac833e5cc79395098b9c33cfd3279be5f31bd00387d2d4db "$stage/$archive"
      unzip -q "$stage/$archive" -d "$stage"
      target="$stage/ghidra_12.1.4_PUBLIC"
      test -f "$target/Ghidra/application.properties"
      test -f "$target/support/analyzeHeadless"
      if ! native_ready "$target"; then
        echo "Building macOS arm64 decompiler through supported Ghidra buildNatives"
        (cd "$target/support/gradle"; \
          GRADLE_USER_HOME="$native_root/toolchains/gradle-cache" \
          JAVA_HOME="$JAVA_HOME" bash ./gradlew --no-daemon buildNatives)
      fi
      native_ready "$target" || {
        echo "Ghidra Gradle native build succeeded without a runnable ARM64 decompiler" >&2
        exit 1
      }
      mv "$target" "$ghidra_dir"
    )
  fi
  auto_native_env
  printf 'REA Ghidra native ready: %s\nFull JDK: %s\n' "$ghidra_dir" "$JAVA_HOME"
}


case "$action" in
  install)
    # Check an existing installation before looking for npm. An already
    # installed REA CLI only needs a compatible Node executable to run.
    if [ -L "$root" ] || [ -L "$root/releases" ] || [ -L "$prefix" ]; then
      echo "refusing symlinked REA installation root" >&2; exit 2
    fi
    need_node
    if [ -e "$prefix" ]; then
      need_rea
      echo "REA $version already installed at $prefix"
      exit 0
    fi
    npm_cli="$(dirname "$rea_node")/../lib/node_modules/npm/bin/npm-cli.js"
    if [ ! -f "$npm_cli" ]; then
      # macOS Homebrew, nvm, asdf and custom Node packages need not keep npm
      # next to the selected node binary. Never assume a portable npm path.
      # Switch to the same private, checksum-verified Node distribution already
      # used when PATH has no supported Node; do not replace the host runtime.
      if ! supported_node "$private_node"; then
        echo "Compatible Node lacks adjacent npm; provisioning isolated Node $node_version" >&2
        bootstrap_node
      fi
      rea_node="$private_node"
      npm_cli="$(dirname "$rea_node")/../lib/node_modules/npm/bin/npm-cli.js"
    fi
    if [ ! -f "$npm_cli" ]; then
      echo "REA private Node is missing npm; refusing an incomplete toolchain" >&2; exit 2
    fi
    mkdir -p "$root/releases" "$root/npm-cache"
    stage="$(mktemp -d "$root/releases/.rea-stage.XXXXXXXX")"
    cleanup() { if [ -n "$stage" ] && [ -d "$stage" ]; then rm -rf -- "$stage"; fi; }
    trap cleanup EXIT
    # Exact release, private prefix/cache. No global npm install, sudo, app
    # credentials, REA setup registration, or automatic target analysis.
    npm_config_cache="$root/npm-cache" "$rea_node" "$npm_cli" install --prefix "$stage" \
      --no-audit --no-fund --no-save --no-package-lock \
      "rea-agents@$version"
    test -f "$stage/node_modules/rea-agents/scripts/rea.mjs"
    "$rea_node" "$stage/node_modules/rea-agents/scripts/rea.mjs" --help >/dev/null
    mv "$stage" "$prefix"
    stage=""
    printf 'REA %s installed in %s\n' "$version" "$prefix"
    ;;
  native-preflight)
    need_rea
    check_native_root
    if [ ! -d "$native_root" ]; then
      echo "native analysis root does not exist; no downloads started" >&2; exit 2
    fi
    free_kib="$(df -Pk "$native_root" | awk 'END {print $4}')"
    case "$free_kib" in ''|*[!0-9]*) echo "unable to measure native disk" >&2; exit 2;; esac
    "$rea_node" - "$native_root" "$free_kib" "$ghidra_dir" "$jdk_dir" <<'JS'
const fs=require('node:fs'),[nativeRoot,free,ghidra,jdk]=process.argv.slice(2);
const fullJdk=fs.existsSync(jdk+'/Contents/Home/bin/javac');
const nativeFiles=[
  ghidra+'/Ghidra/Features/Decompiler/os/mac_arm_64/decompile',
  ghidra+'/Ghidra/Features/Decompiler/build/os/mac_arm_64/decompile'
];
const availableBytes=Number(free)*1024;
console.log(JSON.stringify({schema:'bonfyre.rea.native-preflight.v1',native_root:nativeRoot,
  free_bytes:availableBytes,full_jdk_present:fullJdk,
  native_decompiler_present:nativeFiles.some(x=>fs.existsSync(x)),
  conservative_build_requirement_bytes:4194304*1024,
  conservative_space_admitted:Number(free)>=4194304,
  state:'read_only_preflight_no_downloads'},null,2));
JS
    ;;
  install-native)
    need_rea
    install_native_ghidra
    rea providers --json
    ;;
  native-map)
    # Available without Ghidra, Java or license-protected binary providers.
    # It reads exactly the caller-selected Mach-O or application bundle and
    # records an immutable REA static evidence identity (not runtime profiling).
    need_rea
    if [ "$#" -ne 1 ]; then
      echo "usage: rea-local.sh native-map /absolute/authorized/Mach-O-or-App" >&2; exit 2
    fi
    case "$1" in /*) ;; *) echo "native-map requires an absolute target" >&2; exit 2;; esac
    if [ ! -f "$1" ] && [ ! -d "$1" ]; then
      echo "native-map target is not a local file or application directory" >&2; exit 2
    fi
    scratch="$(mktemp -d "${TMPDIR:-/tmp}/rea-native-map.XXXXXXXX")"
    trap 'rm -rf -- "$scratch"' EXIT
    rea trace-dylib-resolution "$1" --json > "$scratch/evidence.json"
    "$rea_node" - "$scratch/evidence.json" <<'JS'
const fs=require('node:fs');
const data=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
if(!data||typeof data.evidence_id!=='string'||!data.evidence_id.startsWith('ev_'))
  throw Error('REA native dylib trace returned no sealed Evidence identity');
const normalized=data.normalized_result??{};
const size=value=>Array.isArray(value)?value.length:null;
console.log(JSON.stringify({schema:'bonfyre.rea.native-map.v1',
  evidence_id:data.evidence_id,inspection:'trace_dylib_resolution',
  target_sha256:normalized.target_sha256??null,
  image_count:size(normalized.images),dependency_edge_count:size(normalized.edges),
  finding_count:size(normalized.findings),
  result_facets:Object.keys(normalized).slice(0,24),
  coverage:normalized.coverage??null,
  state:'static_native_observation_not_runtime_proof'},null,2));
JS
    ;;
  smoke)
    need_rea
    mkdir -p "$root"
    scratch="$(mktemp -d "$root/.rea-smoke.XXXXXXXX")"
    trap 'rm -rf -- "$scratch"' EXIT
    mkdir -p "$scratch/fixture/src"
    printf '%s\n' '{"name":"aurekai-rea-local-fixture","private":true,"type":"module","main":"src/index.mjs"}' > "$scratch/fixture/package.json"
    printf '%s\n' "export function renderWindow(frame){return {schema:'aurekai.creator.render-program.v1',frame};}" > "$scratch/fixture/src/index.mjs"
    rea analyze-javascript-application "$scratch/fixture" --json > "$scratch/evidence.json"
    "$rea_node" -e 'const fs=require("node:fs");let o=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));
      if(!o||typeof o.evidence_id!=="string"||!o.evidence_id.startsWith("ev_"))process.exit(1);
      process.stdout.write(JSON.stringify({rea_version:"6.4.0",state:"static-evidence-verified",
        evidence_id:o.evidence_id,scope:"synthetic-local-only"})+"\n");' "$scratch/evidence.json"
    ;;
  doctor)
    need_rea
    # REA doctor owns health, while providers exposes its large catalog and
    # capability inventory. Preserve both full upstream CLI commands, but make
    # this operator-facing readback a bounded, source-derived projection.
    scratch="$(mktemp -d "${TMPDIR:-/tmp}/rea-doctor.XXXXXXXX")"
    trap 'rm -rf -- "$scratch"' EXIT
    if ! rea doctor --json > "$scratch/doctor.json"; then
      # The REA doctor may return a nonzero status when optional providers are
      # unavailable. We still inspect its actual JSON without inventing health.
      echo "REA doctor reported unavailable capabilities; inspecting its receipt" >&2
    fi
    # REA's doctor and providers are separate contracts. Doctor owns health
    # checks; providers owns native capabilities, server identity and client
    # registration status. Neither can substitute for the other.
    if ! rea providers --json > "$scratch/providers.json"; then
      echo "REA providers did not produce a usable capability inventory" >&2
      exit 1
    fi
    "$rea_node" - "$scratch/doctor.json" "$scratch/providers.json" "$prefix" "$rea_node" <<'JS'
const fs=require('node:fs');
const read=path=>{try{return JSON.parse(fs.readFileSync(path,'utf8'))}
  catch(err){console.error('REA returned invalid JSON: '+err.message);process.exit(1)}};
const doctor=read(process.argv[2]),inventory=read(process.argv[3]);
const identity=inventory.server_identity??inventory.identity??{};
const providers=inventory.analysis_provider_candidates;
const regs=inventory.registrations??[];
const installed=identity.package?.version??identity.server?.version??null;
if(typeof installed!=='string'||!Array.isArray(providers)||!doctor||typeof doctor!=='object'){
  console.error(JSON.stringify({error:'unrecognized REA doctor/providers contracts',
    doctor_keys:Object.keys(doctor??{}).slice(0,12),
    inventory_keys:Object.keys(inventory??{}).slice(0,12)}));
  process.exit(1);
}
const checks=Array.isArray(doctor.checks)?doctor.checks:[];
const summary={
  schema:'bonfyre.rea.local-doctor.v1',
  package_version:installed,selected_node:process.argv[5],
  install_prefix:process.argv[4],
  health:{healthy:doctor.healthy??null,environment_healthy:doctor.environment_healthy??null,
    checked:checks.length,failed:checks.filter(x=>x.ok===false||x.healthy===false).length},
  catalog:{cli_commands:identity.catalog?.counts?.cli_commands??null,
    mcp_tools:identity.catalog?.counts?.mcp_tools??null},
  native_providers:providers.map(x=>({
    id:x.provider?.id??null,status:x.availability?.status??'unknown',
    reason:x.availability?.reason??null
  })),
  agent_registrations:Array.isArray(regs)?regs.map(x=>({
    client:x.client??null,state:x.state??'unknown'
  })):[],
  live_server_state:inventory.live_server?.state??'unknown'
};
console.log(JSON.stringify(summary,null,2));
JS
    ;;
  targets)
    # Discover actual installed creative application bundles by name only.
    # Paths may contain spaces; never launch, open bundles, or read app data.
    # BONFYRE_REA_APP_ROOT scopes this to a disposable synthetic CI fixture.
    if [ -n "${BONFYRE_REA_APP_ROOT:-}" ]; then
      [ -d "$BONFYRE_REA_APP_ROOT" ] || { echo "creative app root missing" >&2; exit 2; }
      app_roots=("$BONFYRE_REA_APP_ROOT")
    else
      app_roots=("/Applications" "$HOME/Applications")
    fi
    matches=0
    for app_root in "${app_roots[@]}"; do
      [ -d "$app_root" ] || continue
      while IFS= read -r -d '' candidate; do
        app_name="$(basename "$candidate" | tr '[:upper:]' '[:lower:]')"
        case "$app_name" in
          "fl studio"*.app|"ableton live"*.app|"bitwig studio"*.app|reaper*.app|\
          "logic pro"*.app|blender*.app|"davinci resolve"*.app|\
          "adobe premiere pro"*.app|"adobe after effects"*.app|\
          obs.app|"obs studio"*.app)
            printf 'available %s\n' "$candidate"
            matches=$((matches+1))
            ;;
        esac
      done < <(find "$app_root" -maxdepth 4 \( -type d -o -type l \) -iname '*.app' -print0 -prune 2>/dev/null)
    done
    if [ "$matches" -eq 0 ]; then
      printf 'no recognized creative application bundles in selected locations\n'
    fi
    ;;
  static-js)
    need_rea
    if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
      echo "usage: bash scripts/rea-local.sh static-js /explicit/app-directory" >&2; exit 2
    fi
    rea analyze-javascript-application "$1" --json
    ;;
  native)
    need_rea
    if [ "$#" -ne 2 ] || [ ! -e "$2" ]; then
      echo "usage: bash scripts/rea-local.sh native ghidra|hopper /explicit/binary-or-app" >&2; exit 2
    fi
    case "$1" in ghidra|hopper) ;; *) echo "provider must be ghidra or hopper" >&2; exit 2;; esac
    # Explicit, caller-owned target: never default to /Applications or private estate.
    rea inspect "$2" --provider "$1" --json
    ;;
  setup-preview)
    need_rea
    rea setup --client codex --skill=false --dry-run --json
    ;;
  agents-preview)
    need_rea
    # One reviewed scope: Codex and Claude Code only, never every detected app.
    rea setup --client codex --client claude_code --dry-run --json
    ;;
  agents-connect)
    need_rea
    # Upstream REA currently registers the absolute script entry with an
    # env-node shebang on POSIX, not process.execPath. Do not claim our private
    # Node can start that MCP in an agent whose PATH exposes only an old Node.
    # Keep upstream's owned/doctor-compatible registration format unchanged.
    if ! command -v node >/dev/null || ! supported_node "$(command -v node)"; then
      echo "REA MCP registration needs compatible Node on agent PATH (22.19+, 24.11+ or 26+)" >&2
      echo "The private REA Node is usable via the helper, but REA's upstream setup currently emits an env-node launcher. No agent configuration changed." >&2
      exit 2
    fi
    # This command itself is an explicit user request. REA owns backups,
    # conflict checks, atomic writes and matching skill installation.
    rea setup --client codex --client claude_code --yes --json
    ;;
  rea)
    need_rea
    if [ "$#" -lt 1 ]; then echo "provide explicit REA CLI arguments" >&2; exit 2; fi
    rea "$@"
    ;;
  help|*)
    cat <<'HELP'
Bonfyre-local REA 6.4.0 (pinned, isolated; no global npm or auto-inspection)
  bash scripts/rea-local.sh install     # provisions private Node 24 if PATH Node lacks npm
  bash scripts/rea-local.sh smoke
  bash scripts/rea-local.sh doctor
  bash scripts/rea-local.sh targets
  bash scripts/rea-local.sh native-preflight  # no downloads; reports disk gate
  bash scripts/rea-local.sh native-map /absolute/path  # no Ghidra/JDK needed
  bash scripts/rea-local.sh install-native   # private full JDK21 + ARM64 Ghidra
  # optional: BONFYRE_REA_NATIVE_ROOT=/Volumes/MOUNT/rea-native (existing dir)
  bash scripts/rea-local.sh static-js /explicit/authorized/source-directory
  bash scripts/rea-local.sh native ghidra|hopper /explicit/authorized/binary-or-app
  bash scripts/rea-local.sh setup-preview
  bash scripts/rea-local.sh agents-preview
  bash scripts/rea-local.sh agents-connect   # explicit Codex + Claude registration
  bash scripts/rea-local.sh rea providers --json
REA static analysis is not behavioral equivalence or execution authorization.
Hopper and Ghidra require independent installation and compatible licenses.
HELP
    ;;
esac
