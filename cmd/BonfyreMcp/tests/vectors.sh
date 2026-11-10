#!/usr/bin/env bash
# Golden vectors for the owner command clients.
#   vectors.sh record   run the JS oracle (while it exists) into tests/vectors
#   vectors.sh check    run bonfyre-mcp and diff against tests/vectors
#   vectors.sh compare  run both now and diff them directly
# Each case runs against `bonfyre-mcp fixture-serve`, which records the request
# (id normalised to @ID@) and answers from tests/scripts.
set -u
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
bin="$here/../bonfyre-mcp"
mode=${1:-check}
only=${2:-}
work=$(cd "$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-mcp-vectors.XXXXXX")" && pwd -P)
trap 'rm -rf "$work"' EXIT

run_case() {   # impl name svc script env argv -> writes $out/{stdout,stderr,exit,record}
    local impl=$1 name=$2 svc=$3 script=$4 envs=$5 argv=$6 out=$7
    local t="$work/$impl-$name"; mkdir -p "$t/home/.bonfyre/mcp" "$t/tmp" "$out"
    printf 'fixture-token\n' > "$t/home/.bonfyre/mcp/token"; : > "$t/empty-token"
    local -a env_args=(HOME="$t/home" TMPDIR="$t/tmp" PATH="$PATH")
    local port="" fpid=""
    if [ "$script" != "-" ]; then
        "$bin" fixture-serve "$t/port" "$here/scripts/$script" "$t/record" & fpid=$!
        for _ in $(seq 1 200); do [ -s "$t/port" ] && break; sleep 0.02; done
        port=$(cat "$t/port")
        local up; up=$(printf '%s' "$svc" | tr a-z A-Z)
        env_args+=("BONFYRE_${up}_URL=http://127.0.0.1:$port")
    fi
    if [ "$envs" != "-" ]; then
        for kv in $envs; do
            case $kv in
                SLASH=1) env_args+=("BONFYRE_$(printf '%s' "$svc" | tr a-z A-Z)_URL=http://127.0.0.1:$port/") ;;
                *) env_args+=("${kv//@EMPTY@/$t/empty-token}") ;;
            esac
        done
    fi
    argv=${argv//@F@/$here/fixtures}
    eval "set -- $argv"
    if [ "$impl" = js ]; then
        env -i "${env_args[@]}" "${BONFYRE_BUN:-$(command -v bun)}" "$root/integrations/mcp/${svc}_cli.mjs" "$@" > "$out/stdout" 2> "$out/stderr"
    else
        env -i "${env_args[@]}" "$bin" "$svc" "$@" > "$out/stdout" 2> "$out/stderr"
    fi
    echo $? > "$out/exit"
    if [ -n "$fpid" ]; then kill "$fpid" 2>/dev/null; wait "$fpid" 2>/dev/null; fi
    [ -f "$t/record" ] && cp "$t/record" "$out/record" || : > "$out/record"
    for f in stdout stderr record; do
        sed -e "s#$here/fixtures#@F@#g" -e "s#$t#@T@#g" -e "s#bonfyre-recursive-smoke-[A-Za-z0-9]\{6\}#bonfyre-recursive-smoke-XXXXXX#g" \
            ${port:+-e "s#127.0.0.1:$port#127.0.0.1:@PORT@#g"} "$out/$f" > "$out/$f.n" && mv "$out/$f.n" "$out/$f"
    done
}

pass=0 fail=0
while IFS=$'\t' read -r name svc script envs argv; do
    case $name in ''|'#'*) continue ;; esac
    [ -n "$only" ] && [ "$only" != "$name" ] && continue
    case $mode in
        record) run_case js "$name" "$svc" "$script" "$envs" "$argv" "$here/vectors/$name" ;;
        check|compare)
            if [ "$mode" = check ]; then expect="$here/vectors/$name"
            else expect="$work/js-out/$name"; run_case js "$name" "$svc" "$script" "$envs" "$argv" "$expect"; fi
            run_case c "$name" "$svc" "$script" "$envs" "$argv" "$work/c-out/$name"
            if diff -r "$expect" "$work/c-out/$name" > "$work/diff" 2>&1; then pass=$((pass + 1))
            else fail=$((fail + 1)); echo "FAIL $name"; head -40 "$work/diff"; fi ;;
    esac
done < "$here/cases.tsv"
[ "$mode" = record ] && { echo "recorded $(ls "$here/vectors" | wc -l | tr -d ' ') vectors"; exit 0; }
echo "bonfyre-mcp vectors: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
