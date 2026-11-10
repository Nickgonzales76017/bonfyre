#!/bin/sh
set -eu

runtime=${1:?runtime binary required}

BONFYRE_QUEUE_BINARY=/usr/bin/true "$runtime" queue delegated-argument
if BONFYRE_QUEUE_BINARY=/usr/bin/false "$runtime" queue delegated-argument; then
    printf '%s\n' 'delegate exit status was not preserved' >&2
    exit 1
fi

BONFYRE_REPRESENTATION_BINARY=/usr/bin/true "$runtime" representation doctor
if BONFYRE_REPRESENTATION_BINARY=/usr/bin/false "$runtime" representation doctor; then
    printf '%s\n' 'representation delegate exit status was not preserved' >&2
    exit 1
fi
"$runtime" doctor --json | grep -Fq '"total":17'
"$runtime" capabilities | grep -Fq '"generator":"bonfyre-runtime capabilities"'

# branch: a counter guest that honours the line contract. Its forks are local
# arithmetic, so a branch never moves the committed position.
case "$runtime" in /*) ;; *) runtime=$PWD/$runtime ;; esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cat > "$work/guest.sh" <<'GUEST'
#!/bin/sh
pos=0
apply() {
    result=$1
    old=$IFS; IFS=';'
    for step in $2; do
        case "$step" in inc) result=$((result + 1)) ;; dec) result=$((result - 1)) ;; esac
    done
    IFS=$old
}
while IFS= read -r line; do
    case "$line" in
        q) exit 0 ;;
        "b "*)
            n=${line#b }; k=1
            while [ "$k" -le "$n" ]; do
                IFS= read -r macro
                apply "$pos" "$macro"
                printf '{"i":%d,"pos":%d}\n' "$k" "$result"
                k=$((k + 1))
            done
            printf '{"done":1}\n' ;;
        m*)
            macro=${line#m}; macro=${macro# }
            apply "$pos" "$macro"; pos=$result
            printf '{"i":0,"pos":%d}\n' "$pos" ;;
    esac
done
GUEST
chmod +x "$work/guest.sh"
printf 'up\tinc\ndown\tdec\n' > "$work/actions.tsv"
rank="sed -n 's/.*\"pos\":\\(-*[0-9]*\\).*/\\1/p' candidates.jsonl"

out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank "$rank" --depth 2 --width 2 \
    --decisions 20 --stop-score 5 --log log -- ./guest.sh)
printf '%s\n' "$out" | grep -Fq '"stopped":"score"'
printf '%s\n' "$out" | grep -Fq '"pos":5'
[ "$(grep -c '^inc$' "$work/log")" = 5 ]
if grep -q '^dec$' "$work/log"; then
    printf '%s\n' 'branch committed a step its ranker scored lower' >&2
    exit 1
fi

# A ranker that fails gives no scores; branch stops instead of guessing.
if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank 'echo 7; exit 3' --decisions 3 -- ./guest.sh); then
    printf '%s\n' 'branch accepted scores from a failed ranker' >&2
    exit 1
fi
printf '%s\n' "$out" | grep -Fq '"stopped":"ranker"'

# Native Query serializes some numeric columns as JSON strings; consume both
# carriers without changing candidate order or inventing scores from text.
for values in '[{"v":"1"},{"v":"0"}]' '[{"v" : 1},{"v" : 0}]'; do
    printf '%s\n' "$values" > "$work/scores.json"
    out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank 'cat scores.json' \
        --depth 1 --width 1 --decisions 1 -- ./guest.sh)
    printf '%s\n' "$out" | grep -Fq '"pos":1'
done

# Too many, too few, non-finite and malformed scores must stop before any
# candidate action reaches the guest. Test the installed carrier as well.
for values in '1 0 999' '1' 'nan 0' 'inf 0' '1e999 0' 'ERROR 500 400' \
    '[{"v":"nan"},{"v":"0"}]' '[{"v":null},{"v":0}]' \
    '[{"v":"1x"},{"v":"0"}]' '[{"v":1},{"v":0},{"v":999}]'; do
    printf '%s\n' "$values" > "$work/scores.json"
    if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank 'cat scores.json' \
        --depth 1 --width 1 --decisions 1 -- ./guest.sh); then
        printf 'branch accepted invalid score sequence: %s\n' "$values" >&2
        exit 1
    fi
    printf '%s\n' "$out" | grep -Fq '"stopped":"ranker"'
    printf '%s\n' "$out" | grep -Fq '"pos":0'
done

# The initial state callback cannot fail and then let a run claim success.
if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank "$rank" \
    --depth 1 --width 1 --decisions 1 --commit 'exit 23' -- ./guest.sh); then
    printf '%s\n' 'branch ignored initial commit failure' >&2
    exit 1
fi
printf '%s\n' "$out" | grep -Fq '"stopped":"commit"'
printf '%s\n' "$out" | grep -Fq '"pos":0'

# A later failed callback stops continuation but cannot erase an action that
# the guest has actually performed. Preserve the exact committed step.
if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank "$rank" \
    --depth 1 --width 1 --decisions 4 --log failed-commit.log \
    --commit "grep -Fq '\"pos\":0' state.json" -- ./guest.sh); then
    printf '%s\n' 'branch ignored post-action commit failure' >&2
    exit 1
fi
printf '%s\n' "$out" | grep -Fq '"stopped":"commit"'
printf '%s\n' "$out" | grep -Fq '"pos":1'
[ "$(grep -c '^inc$' "$work/failed-commit.log")" = 1 ]

# An explicitly requested recording destination cannot silently disappear.
for option in --log --trace; do
    if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank "$rank" \
        --depth 1 --width 1 --decisions 1 "$option" missing/record -- ./guest.sh); then
        printf 'branch ignored unavailable %s destination\n' "$option" >&2
        exit 1
    fi
    printf '%s\n' "$out" | grep -Fq '"stopped":"setup"'
done
rm "$work/state.json"
mkdir "$work/state.json"
if out=$(cd "$work" && "$runtime" branch --actions actions.tsv --rank "$rank" \
    --depth 1 --width 1 --decisions 1 -- ./guest.sh); then
    printf '%s\n' 'branch ignored failed state publication' >&2
    exit 1
fi
printf '%s\n' "$out" | grep -Fq '"stopped":"workdir"'

printf '%s\n' 'runtime dispatch: PASS'
