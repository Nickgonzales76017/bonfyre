#!/bin/sh
# The estate as tables, small enough for a browser to query.
#
# The edge already serves every page this estate composed. What it cannot serve
# is a question: "thirty things I have not seen, about these subjects, not all
# from the same publisher." Answering that from files means one request per
# guess. Answering it from a table means one query.
#
# So the same material is published a second way -- as columnar tables a reader
# can query locally, by range-reading only the columns and row groups it needs.
# Nothing here is new material: every row points at a page the estate already
# composed, and the catalogue is a way to choose among them, never a second
# source of truth.
#
#   publish_catalog.sh [--out DIR] [--upload]
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
OUT="$ESTATE/catalog"
UPLOAD=0
while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT=$2; shift 2 ;;
        --upload) UPLOAD=1; shift ;;
        *) shift ;;
    esac
done
mkdir -p "$OUT"

command -v duckdb >/dev/null 2>&1 || { echo "duckdb is not installed here" >&2; exit 2; }
command -v bun >/dev/null 2>&1 || { echo "bun is not installed here" >&2; exit 2; }

ITEMS="$ESTATE/items.db"
ACTORS_EARLY="$ESTATE/actors.db"
FRONTIER="$ESTATE/frontier.db"
[ -f "$ITEMS" ] || { echo "no item archive at $ITEMS" >&2; exit 2; }

# The SQL lives in a heredoc, not a double-quoted argument: a shell string eats
# the quotes DuckDB needs around at, key and rank, which are its reserved words,
# and the query then fails as syntax rather than as a missing column.
SQL="$OUT/.catalog.sql"
cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$ITEMS' AS archive (TYPE sqlite, READ_ONLY);
ATTACH '$ACTORS_EARLY' AS people (TYPE sqlite, READ_ONLY);

-- What the estate has kept. One row per thing, pointing at the page that
-- already exists for it.
COPY (
  SELECT 'o/' || i.source || '/' || i."key" AS address,
         i.source, i."key" AS item_key, i.kind,
         COALESCE(i.family,'') AS family,
         COALESCE(i.title,'') AS title,
         COALESCE(i.publisher,'') AS publisher,
         COALESCE(i.href,'') AS href,
         COALESCE(i.image,'') AS image,
         CAST(COALESCE(i."at",0) AS BIGINT) AS published_at,
         COALESCE(i.first_read,'') AS first_read,
         -- Who was credited, and what kind of name that is. A feed credits a
         -- masthead as readily as a reporter, and a page that cannot tell them
         -- apart cannot say who is writing about anything.
         COALESCE(trim(i.by),'') AS by_name,
         COALESCE(a.node_kind,'') AS by_kind,
         COALESCE(i.body,'') AS body
  FROM archive.item i
  LEFT JOIN (SELECT display_name, MIN(node_kind) AS node_kind
               FROM people.actor_nodes GROUP BY display_name) a
         ON a.display_name = trim(i.by)
) TO '$OUT/material.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null || { echo "the material catalogue did not build" >&2; exit 3; }

# The vGPU's own output, published so the expensive comparison is spent more
# than once. An estate that has not computed these yet simply has no file.
cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$ITEMS' AS archive (TYPE sqlite, READ_ONLY);
COPY (
  SELECT address, kin_address, chart,
         CAST(score AS FLOAT) AS score,
         CAST("rank" AS INTEGER) AS kin_rank
  FROM archive.kin
) TO '$OUT/kin.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null 2>&1 || true

cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$ITEMS' AS archive (TYPE sqlite, READ_ONLY);
COPY (
  SELECT address, near_address, kind,
         CAST(metres AS FLOAT) AS metres,
         CAST("rank" AS INTEGER) AS near_rank
  FROM archive.neighbour
) TO '$OUT/neighbour.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null 2>&1 || true

# The source estate itself: who publishes, and through what.
if [ -f "$FRONTIER" ]; then
cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$FRONTIER' AS ledger (TYPE sqlite, READ_ONLY);
COPY (
  SELECT root_id, host, COALESCE(NULLIF(name,''), host) AS name,
         COALESCE(identity,'') AS identity
  FROM ledger.root
) TO '$OUT/source_root.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
  SELECT channel_id, root_id, exact_address AS address, protocol,
         COALESCE(material,'') AS material, state,
         COALESCE(truth_level,'') AS truth_level,
         COALESCE(census_source,'') AS census_source,
         COALESCE(carries,'') AS carries,
         CAST(COALESCE(contributions,0) AS INTEGER) AS contributions
  FROM ledger.channel
) TO '$OUT/source_channel.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null 2>&1 || true
fi
# Who is credited on what this estate kept. Six thousand names the estate
# separated into people and mastheads by how each was credited, each carrying
# the provenance of that judgement. Published because "who is writing about
# this" is a question a publisher column cannot answer.
ACTORS="$ESTATE/actors.db"
if [ -f "$ACTORS" ] && [ -f "$ITEMS" ]; then
cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$ACTORS' AS people (TYPE sqlite, READ_ONLY);
ATTACH '$ITEMS' AS archive (TYPE sqlite, READ_ONLY);
COPY (
  SELECT a.display_name AS name,
         a.node_kind AS kind,
         COALESCE(a.role,'') AS role,
         COALESCE(a.confidence,'') AS confidence,
         COALESCE(a.provenance,'') AS provenance,
         CAST(COUNT(i."key") AS INTEGER) AS kept,
         CAST(COUNT(DISTINCT i.publisher) AS INTEGER) AS publishers,
         CAST(COALESCE(MIN(NULLIF(i."at",0)),0) AS BIGINT) AS first_at,
         CAST(COALESCE(MAX(i."at"),0) AS BIGINT) AS last_at
  FROM (SELECT display_name, MIN(node_kind) AS node_kind, MIN(role) AS role,
               MIN(confidence) AS confidence, MIN(provenance) AS provenance
          FROM people.actor_nodes GROUP BY display_name) a
  LEFT JOIN archive.item i ON trim(i.by) = a.display_name
  GROUP BY 1,2,3,4,5
  HAVING COUNT(i."key") > 0
) TO '$OUT/actor.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null 2>&1 || true
fi

# The clothes. This estate has published one thousand three hundred and ninety
# design systems, each a real theme's language reduced to tokens: a ground, an
# ink, three typefaces, a display scale. Every page the estate composes wears
# one, chosen for it. Published here, a reader can choose instead -- which is
# what it means for software to behave like publishing rather than like an app
# with a dark mode.
CMS="$ESTATE/cms.db"
if [ -f "$CMS" ]; then
cat > "$SQL" <<SQLTEXT
INSTALL sqlite; LOAD sqlite;
ATTACH '$CMS' AS cms (TYPE sqlite, READ_ONLY);
COPY (
  SELECT reference_name AS name,
         COALESCE(family,'') AS family,
         COALESCE(hero_mode,'') AS hero_mode,
         COALESCE(typography_mode,'') AS typography_mode,
         COALESCE(density,'') AS density,
         COALESCE(reference_url,'') AS reference_url,
         layout_tokens AS tokens
  FROM cms.presentation_template
  WHERE COALESCE(layout_tokens,'') <> '' AND COALESCE(reference_name,'') <> ''
) TO '$OUT/template.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
duckdb -f "$SQL" >/dev/null 2>&1 || true
fi

# Which route each thing can appear on. Without this a reader can only ask
# "what is next in the list"; with it they can ask "what have I not seen, on
# this route, not all from one publisher" -- which is the difference between
# counting pages and choosing material. The web-address organ is asked, because
# it is the thing that actually decides which sources feed a route.
WEB="${BONFYRE_WEB:-$HOME/Developer/Bonfyre/cmd/BonfyreWebAddress/bonfyre-web-address}"
if [ -x "$WEB" ]; then
    BONFYRE_CMS_DB="$ESTATE/cms.db" BONFYRE_FOREIGN_ROOT="$ESTATE/foreign" \
    BONFYRE_SOURCE_FRONTIER_DB="$ESTATE/frontier.db" BONFYRE_ITEM_ARCHIVE_DB="$ITEMS" \
        "$WEB" page-contributions 2>/dev/null \
        | grep '^{"route"' > "$OUT/.route-source.jsonl" || true
    if [ -s "$OUT/.route-source.jsonl" ]; then
        cat > "$SQL" <<SQLTEXT
COPY (
  SELECT route, source, CAST(items AS INTEGER) AS shown
  FROM read_json_auto('$OUT/.route-source.jsonl')
  WHERE route IS NOT NULL AND source IS NOT NULL
) TO '$OUT/route_source.parquet' (FORMAT parquet, COMPRESSION zstd);
SQLTEXT
        duckdb -f "$SQL" >/dev/null 2>&1 || true
        rm -f "$OUT/.route-source.jsonl" "$SQL"
    fi
fi

# What a reader needs to know before asking anything: what is here, and what it
# is not. A catalogue that does not say what it cannot answer invites being
# read as the estate itself.
bun "$ROOT/scripts/edge-catalog-manifest.mjs" "$OUT"

printf 'catalogue at %s\n' "$OUT"
for f in "$OUT"/*.parquet; do
    [ -f "$f" ] || continue
    printf '  %-26s %6s KB  %s rows\n' "$(basename "$f")" \
        "$(( $(wc -c < "$f") / 1024 ))" \
        "$(duckdb -noheader -list -c "SELECT count(*) FROM '$f'" 2>/dev/null || echo '?')"
done

if [ "$UPLOAD" = "1" ]; then
    [ -f "$HOME/.bonfyre/secrets/cloud.refs.env" ] && . "$HOME/.bonfyre/secrets/cloud.refs.env"
    AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION=auto \
    aws s3 sync "$OUT/" "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/catalog/" \
        --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --only-show-errors --no-progress \
        && printf 'published to the object store\n'
fi
