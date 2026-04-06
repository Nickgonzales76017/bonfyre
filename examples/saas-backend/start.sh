#!/bin/sh
# Bonfyre SaaS Backend Demo
# Starts the API gateway and seeds demo data.

set -e

BONFYRE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CMD="$BONFYRE_ROOT/cmd"

# Build if needed
for bin in BonfyreAPI BonfyreAuth BonfyreGate BonfyreMeter BonfyrePay; do
    binary="$CMD/$bin/$(echo "$bin" | sed 's/Bonfyre/bonfyre-/' | tr '[:upper:]' '[:lower:]')"
    # normalize: BonfyreAPI -> bonfyre-api, etc.
    if [ ! -f "$CMD/$bin/"bonfyre-* ] 2>/dev/null; then
        echo "Building $bin..."
        make -C "$BONFYRE_ROOT" "cmd/$bin"
    fi
done

echo "Starting Bonfyre SaaS backend..."
echo ""

# Start API gateway
echo "[1/4] Starting API gateway on port 9090..."
"$CMD/BonfyreAPI/bonfyre-api" --port 9090 --static "$BONFYRE_ROOT/frontend/" serve &
API_PID=$!
sleep 1

# Create demo user
echo "[2/4] Creating demo user..."
"$CMD/BonfyreAuth/bonfyre-auth" signup \
    --email demo@example.com \
    --password demo123 2>/dev/null || echo "  (user may already exist)"

# Issue API key
echo "[3/4] Issuing Pro API key..."
"$CMD/BonfyreGate/bonfyre-gate" issue \
    --email demo@example.com \
    --tier pro 2>/dev/null || echo "  (key may already exist)"

echo "[4/4] Backend running."
echo ""
echo "  Dashboard: http://localhost:9090"
echo "  API:       http://localhost:9090/api/"
echo "  User:      demo@example.com / demo123"
echo ""
echo "Press Ctrl+C to stop."

# Wait for Ctrl+C
trap "echo ''; echo 'Shutting down...'; kill $API_PID 2>/dev/null; exit 0" INT TERM
wait $API_PID
