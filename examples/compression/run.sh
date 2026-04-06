#!/bin/sh
# Lambda Tensors Compression Demo
# Compresses a sample JSON dataset and shows the results.

set -e

BONFYRE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LT_DIR="$BONFYRE_ROOT/lib/liblambda-tensors"
DEMO_DIR="$(dirname "$0")"

echo "=== Lambda Tensors Compression Demo ==="
echo ""

# Build library if needed
if [ ! -f "$LT_DIR/liblambda-tensors.a" ]; then
    echo "Building liblambda-tensors..."
    make -C "$LT_DIR"
fi

# Build the demo program
echo "Building demo..."
cc -std=c11 -O2 -Wall \
    -I"$LT_DIR/include" \
    -o "$DEMO_DIR/compress-demo" \
    "$DEMO_DIR/compress-demo.c" \
    "$LT_DIR/liblambda-tensors.a" -lm

echo ""

# Generate sample data
echo "Generating 1,000 sample JSON records..."
"$DEMO_DIR/compress-demo" generate 1000 > "$DEMO_DIR/sample.json"

RAW_SIZE=$(wc -c < "$DEMO_DIR/sample.json" | tr -d ' ')
echo "Raw JSON size: $RAW_SIZE bytes"

# Compress with Lambda Tensors
echo ""
echo "Compressing with Lambda Tensors..."
"$DEMO_DIR/compress-demo" compress "$DEMO_DIR/sample.json" "$DEMO_DIR/sample.lt"

LT_SIZE=$(wc -c < "$DEMO_DIR/sample.lt" | tr -d ' ')
RATIO=$(echo "scale=1; $LT_SIZE * 100 / $RAW_SIZE" | bc)

echo ""
echo "=== Results ==="
echo "Raw JSON:        $RAW_SIZE bytes"
echo "Lambda Tensors:  $LT_SIZE bytes ($RATIO% of original)"
echo ""
echo "Key advantage: random access to ANY field in ANY record"
echo "without decompressing the whole dataset."

# Clean up
rm -f "$DEMO_DIR/compress-demo" "$DEMO_DIR/sample.json" "$DEMO_DIR/sample.lt"
