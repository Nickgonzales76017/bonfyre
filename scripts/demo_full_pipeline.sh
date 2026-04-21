#!/bin/bash
#
# END-TO-END EXAMPLE: Documents → Claims → Discovery → Report
#
# This is the COMPLETE workflow. Nothing left to build.
#

set -e

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "BONFYRE: FULL AUTONOMOUS PIPELINE"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "This script runs the COMPLETE pipeline:"
echo "  1. Extract claims from documents"
echo "  2. Detect signals in claim graph"
echo "  3. Generate competing hypotheses"
echo "  4. Rank by investigation_score"
echo "  5. Output top 5 hypotheses"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo ""

# ──────────────────────────────────────────────────────────────────
# SETUP
# ──────────────────────────────────────────────────────────────────

CORPUS_DIR="/tmp/bonfyre-corpus"
MEMORY_DIR="/tmp/bonfyre-demo"
REPORT_FILE="/tmp/bonfyre-report.json"

mkdir -p "$CORPUS_DIR"

# ──────────────────────────────────────────────────────────────────
# CREATE TEST CORPUS
# ──────────────────────────────────────────────────────────────────

echo "[SETUP] Creating test corpus in $CORPUS_DIR"

cat > "$CORPUS_DIR/doc1.txt" << 'EOF'
Jeffrey Epstein attended the event on July 15.
J.E. arrived by helicopter at the same event.
Jeffrey Epstein spoke with Alice about business.
J.E. met with Alice to discuss investments.
Jeffrey Epstein left around midnight.
J.E. departed on the helicopter around midnight.
Multiple guests saw Jeffrey Epstein at the party.
Several witnesses noticed J.E. at the same party.
Jeffrey Epstein wore a blue suit.
J.E. was also wearing a blue suit.
EOF

cat > "$CORPUS_DIR/doc2.txt" << 'EOF'
Alice confirmed meeting Jeffrey Epstein.
Alice also mentioned meeting J.E. that night.
Jeffrey Epstein traveled frequently to the island.
J.E. owns property on that island.
Alice has worked with Jeffrey Epstein for years.
Alice considers J.E. a close business partner.
Jeffrey Epstein and Alice attended multiple events together.
J.E. and Alice were seen together frequently.
EOF

cat > "$CORPUS_DIR/doc3.txt" << 'EOF'
Event A occurred before Event B according to witness testimony.
Event B happened first according to other witnesses.
The timeline clearly shows Event A then Event B.
The timeline actually shows Event B then Event A.
Event A must have come after Event B.
Event B definitely happened before Event A.
EOF

echo "  ✓ Created 3 test documents"
echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 1: EXTRACT CLAIMS
# ──────────────────────────────────────────────────────────────────

echo "[STEP 1/4] Extracting claims from corpus..."
echo ""

python3 scripts/extract_claims.py \
    --corpus "$CORPUS_DIR/*.txt" \
    --memory-dir "$MEMORY_DIR" \
    --clear

echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 2: RUN DISCOVERY
# ──────────────────────────────────────────────────────────────────

echo "[STEP 2/4] Running autonomous discovery..."
echo ""

python3 scripts/hypothesis_discovery.py \
    --memory-dir "$MEMORY_DIR" \
    --models-dir /tmp/bonfyre-models \
    --max-hypotheses 5 \
    --min-signal-strength 0.4 \
    --output "$REPORT_FILE"

echo ""

# ──────────────────────────────────────────────────────────────────
# STEP 3: DISPLAY KEY FINDINGS
# ──────────────────────────────────────────────────────────────────

echo "[STEP 3/4] Key findings (THIS is what matters):"
echo ""

if [ -f "$REPORT_FILE" ]; then
    python3 << PYEOF
import json

with open('$REPORT_FILE') as f:
    report = json.load(f)

print("  📊 PIPELINE SUMMARY:")
print(f"     Signals detected:      {report['n_signals_detected']}")
print(f"     Hypotheses generated:  {report['n_hypotheses_generated']}")
print(f"     After deduplication:   {report['n_hypotheses_deduped']}")
print()
print("  🔍 TOP SIGNALS:")
for i, signal in enumerate(report.get('signals', [])[:3], 1):
    print(f"     {i}. {signal['signal_type']:20s} | strength: {signal['strength']:.2f}")
    print(f"        {signal['description']}")
print()
print("  🎯 TOP HYPOTHESES BY INVESTIGATION SCORE:")
print()
for i, ranking in enumerate(report.get('rankings', [])[:5], 1):
    print(f"     {i}. {ranking['hypothesis_name']}")
    print(f"        investigation_score: {ranking['investigation_score']:.3f}")
    print(f"        impact:              {ranking['impact_score']:.2f}")
    print(f"        leverage:            {ranking['structural_leverage']:.2f}")
    print(f"        cost:                {ranking['cost']:.1f}")
    print()

PYEOF
else
    echo "  ⚠ No report generated"
fi

# ──────────────────────────────────────────────────────────────────
# STEP 4: WHAT TO DO NEXT
# ──────────────────────────────────────────────────────────────────

echo "[STEP 4/4] What to do next:"
echo ""
echo "  1. LOOK AT THE TOP 3 HYPOTHESES ABOVE"
echo "     → Are they interesting?"
echo "     → Do they match patterns in your documents?"
echo ""
echo "  2. IF INTERESTING:"
echo "     → The system is working!"
echo "     → Test top hypothesis with Phase 16.5:"
echo ""
echo "       python3 scripts/hypothesis_engine.py \\"
echo "         --compare <hypothesis_name> \\"
echo "         --with-fragility"
echo ""
echo "  3. IF NOT INTERESTING:"
echo "     → Tune the scoring (see QUICKSTART.md)"
echo "     → Adjust --min-signal-strength"
echo "     → Improve claim extraction (if needed)"
echo ""
echo "  4. DO NOT:"
echo "     → Build more infrastructure"
echo "     → Add more phases"
echo "     → Overthink it"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "FILES CREATED:"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "  Test corpus:   $CORPUS_DIR/"
echo "  Claim database: $MEMORY_DIR/memory.db"
echo "  Report:        $REPORT_FILE"
echo ""
echo "  View full report:"
echo "    cat $REPORT_FILE | python3 -m json.tool"
echo ""
echo "  Query claims:"
echo "    sqlite3 $MEMORY_DIR/memory.db 'SELECT * FROM claims LIMIT 5'"
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "THE SYSTEM IS COMPLETE. USE IT."
echo "════════════════════════════════════════════════════════════════"
echo ""
