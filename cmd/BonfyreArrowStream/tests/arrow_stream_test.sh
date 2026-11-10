#!/bin/sh
set -eu

# Proof-only native dependency injection. No production path may assume a
# Python wheel; an Arrow C++/Parquet distribution must be packaged separately.
: "${ARROW_CPP_INCLUDE:?set ARROW_CPP_INCLUDE to native Arrow C++ headers}"
: "${ARROW_CPP_LIBARROW:?set ARROW_CPP_LIBARROW to the native Arrow C++ library}"
: "${ARROW_CPP_LIBPARQUET:?set ARROW_CPP_LIBPARQUET to the native Parquet library}"
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -f "$scratch/fixture.parquet" "$scratch/stream.ipc" "$scratch/writer" "$scratch/verify"; rmdir "$scratch"' EXIT

libdir=$(dirname -- "$ARROW_CPP_LIBARROW")
cxx=${CXX:-c++}
"$cxx" -std=c++20 -Wall -Wextra -Werror -isystem "$ARROW_CPP_INCLUDE" \
  "$repo/cmd/BonfyreArrowStream/src/main.cpp" \
  "$ARROW_CPP_LIBARROW" "$ARROW_CPP_LIBPARQUET" \
  "-Wl,-rpath,$libdir" -o "$scratch/writer"
"$cxx" -std=c++20 -Wall -Wextra -Werror -isystem "$ARROW_CPP_INCLUDE" \
  "$repo/cmd/BonfyreArrowStream/tests/verify.cpp" \
  "$ARROW_CPP_LIBARROW" "$ARROW_CPP_LIBPARQUET" \
  "-Wl,-rpath,$libdir" -o "$scratch/verify"

duckdb -c "COPY (SELECT 1::BIGINT AS id, 'alpha'::VARCHAR AS name,
  [1,2,3]::INTEGER[] AS tags, 12.34::DECIMAL(8,2) AS amount,
  DATE '2024-01-02' AS d UNION ALL SELECT 2, NULL, []::INTEGER[],
  -5.67::DECIMAL(8,2), DATE '2024-01-03')
  TO '$scratch/fixture.parquet' (FORMAT PARQUET)"
"$scratch/writer" "$scratch/fixture.parquet" --batch-size 1 > "$scratch/stream.ipc"
"$scratch/verify" "$scratch/fixture.parquet" "$scratch/stream.ipc"
if "$scratch/writer" "$scratch/fixture.parquet" --batch-size 0 >/dev/null 2>&1; then
  echo 'zero batch size was accepted' >&2
  exit 1
fi
if "$scratch/writer" "$scratch/not-a-file.parquet" >/dev/null 2>&1; then
  echo 'missing input was accepted' >&2
  exit 1
fi
if [ -x "$repo/cmd/BonfyreCMS/bonfyre-cms" ]; then
  sh "$repo/cmd/BonfyreArrowStream/tests/density_index_test.sh" "$scratch/writer"
else
  echo 'density-index: CMS not built here; native cross-owner test deferred'
fi
