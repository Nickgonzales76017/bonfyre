#!/bin/sh
# The estate's own semantic index, built by its own organs.
#
# Cloudflare's vector service refuses this estate's token, and the keyword
# index answers only the words a person actually typed. But the estate already
# has an embedding organ and a vector store, and a model cached on this
# machine, so the semantic layer does not need anyone's permission.
#
# The embedding organ refuses to embed a corpus degraded -- if the model it was
# asked for has no readable vocabulary it stops rather than quietly hashing
# words and calling the result meaning. That refusal is the reason this is
# worth building: a degraded index that looks like a working one is worse than
# none.
#
# What it produces is a distance between two texts under one model. It is not
# meaning, not a shared subject, and not comparable to a distance from any
# other model.
#
#   build_semantic_index.sh [--limit 40000] [--publish]
set -eu

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
MODEL="${BONFYRE_EMBED_MODEL:-$HOME/.cache/bonfyre/models/nomic-embed-text-onnx}"
LIMIT=40000
PUBLISH=0
while [ $# -gt 0 ]; do
    case "$1" in
        --limit) LIMIT=$2; shift 2 ;;
        --publish) PUBLISH=1; shift ;;
        *) shift ;;
    esac
done

EMBED="$REPO/cmd/BonfyreEmbed/bonfyre-embed"
VEC="${BONFYRE_VEC:-$HOME/.bonfyre/bin/bonfyre-vec}"
ITEMS="$ESTATE/items.db"
WORK="$ESTATE/semantic"; mkdir -p "$WORK"
LOG="$WORK/semantic.log"
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

[ -x "$EMBED" ] || { echo "the embedding organ is not built at $EMBED" >&2; exit 2; }
[ -f "$MODEL/vocab.txt" ] || { echo "no readable vocabulary at $MODEL" >&2; exit 2; }
[ -f "$ITEMS" ] || { echo "no item archive at $ITEMS" >&2; exit 2; }

LOCK="$WORK/running.lock"
mkdir "$LOCK" 2>/dev/null || exit 0
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT INT TERM

# What each thing says about itself, in the words a reader would recognise.
bun "$REPO/scripts/semantic_corpus.mjs" "$ITEMS" "$WORK/corpus.jsonl" "$LIMIT"

note "embedding $(wc -l < "$WORK/corpus.jsonl" | tr -d ' ') things"
"$EMBED" --input-jsonl "$WORK/corpus.jsonl" --out "$WORK/vectors.json" \
         --backend onnx --model "$MODEL" 2>>"$LOG" | tail -1 >> "$LOG"

# The vector store, if this estate has one built.
if [ -x "$VEC" ]; then
    rm -f "$WORK/vectors.db"
    "$VEC" init "$WORK/vectors.db" >/dev/null 2>&1 || true
    "$VEC" insert "$WORK/vectors.db" "$WORK/vectors.json" >>"$LOG" 2>&1 || note "the vector store refused the batch"
    note "store holds $("$VEC" count "$WORK/vectors.db" 2>/dev/null | tail -1)"
fi

# And as a table, so a reader elsewhere can use the same vectors without
# needing this machine.
if [ "$PUBLISH" = "1" ] && command -v duckdb >/dev/null 2>&1; then
    SQL="$WORK/.semantic.sql"
    cat > "$SQL" <<SQLTEXT
COPY (
  SELECT id AS address, embedding AS vector
  FROM read_json_auto('$WORK/vectors.json')
) TO '$ESTATE/catalog/semantic.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
    duckdb -f "$SQL" >/dev/null 2>&1 && note "semantic table written" || note "semantic table refused"
    rm -f "$SQL"
fi

note "pass complete"
tail -3 "$LOG"
