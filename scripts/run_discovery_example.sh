#!/bin/bash
# 
# REAL USAGE EXAMPLE
# 
# This is what you actually run to USE Phase 17.
# No more building. Just run it and see if it finds anything interesting.
#

set -e

echo "════════════════════════════════════════════════════════════════"
echo "PHASE 17: REAL USAGE EXAMPLE"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "This script shows you how to ACTUALLY USE Phase 17."
echo "No theory. No infrastructure. Just: run it and look at output."
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 1: Set up test corpus
# ──────────────────────────────────────────────────────────────────

echo "[1/3] Creating test corpus..."
mkdir -p /tmp/test-corpus

cat > /tmp/test-corpus/doc1.txt << 'EOF'
Jeffrey Epstein was seen at the party on July 15, 2019.
J.E. arrived by helicopter around 8 PM.
Multiple witnesses saw both Jeffrey Epstein and J.E. that night.
The guest list included Epstein's name.
EOF

cat > /tmp/test-corpus/doc2.txt << 'EOF'
J.E. spoke with several guests about finance.
Jeffrey Epstein left the party around midnight.
J.E. was seen boarding the helicopter.
Witnesses said Epstein seemed in good spirits.
EOF

cat > /tmp/test-corpus/doc3.txt << 'EOF'
Event A occurred before Event B, according to the timeline.
However, other sources claim Event B happened first.
The sequence Event A → Event B is disputed.
Some witnesses say Event A came after Event B.
EOF

echo "  ✓ Created 3 test documents in /tmp/test-corpus/"
echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 2: Run Phase 17 discovery
# ──────────────────────────────────────────────────────────────────

echo "[2/3] Running autonomous discovery..."
echo ""

python3 scripts/hypothesis_discovery.py \
    --corpus /tmp/test-corpus/*.txt \
    --max-hypotheses 5 \
    --min-signal-strength 0.4 \
    --output /tmp/discovery_report.json

echo ""
echo "  ✓ Discovery complete"
echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 3: Look ONLY at key findings
# ──────────────────────────────────────────────────────────────────

echo "[3/3] Key findings (THIS is what matters):"
echo ""

if [ -f /tmp/discovery_report.json ]; then
    echo "  Top hypotheses by investigation score:"
    echo ""
    
    python3 << 'PYEOF'
import json

with open('/tmp/discovery_report.json') as f:
    report = json.load(f)

for i, ranking in enumerate(report.get('rankings', [])[:3], 1):
    print(f"  {i}. {ranking['hypothesis_name']}")
    print(f"     investigation_score: {ranking['investigation_score']:.3f}")
    print(f"     impact: {ranking['impact_score']:.2f}")
    print(f"     structural_leverage: {ranking['structural_leverage']:.2f}")
    print(f"     cost: {ranking['cost']:.1f}")
    print()
PYEOF

    echo "  Full report: /tmp/discovery_report.json"
else
    echo "  ⚠ No report generated (check if claims were extracted)"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "WHAT TO DO NEXT"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "1. Look at the top 3 hypotheses above"
echo "   → Are they interesting?"
echo "   → Do they match real patterns in the documents?"
echo ""
echo "2. If YES: Phase 17 is working!"
echo "   → Use it on real data"
echo "   → Iterate on scoring if needed"
echo ""
echo "3. If NO: The signals/hypotheses need tuning"
echo "   → Check what signals were detected"
echo "   → Adjust min_signal_strength"
echo "   → Add/modify signal detectors"
echo ""
echo "4. DO NOT BUILD MORE INFRASTRUCTURE"
echo "   → Run it"
echo "   → See what it finds"
echo "   → Iterate based on REAL output"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""
