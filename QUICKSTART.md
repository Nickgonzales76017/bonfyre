# 🚀 QUICKSTART: How to Actually Use Bonfyre

**Stop building. Start using.**

This guide shows you how to run Bonfyre **TODAY** on real data and see if it produces anything interesting.

---

## 📋 Prerequisites

You need:
- Python 3.8+
- Your documents (text files, transcripts, etc.)
- Nothing else

---

## 🎯 The Workflow (4 Steps)

### 1. **Extract claims from your corpus** (Phase 12)

```bash
# NOT IMPLEMENTED YET - Phase 12 needs corpus integration
# This would run: python3 scripts/hypothesis_swarm.py --corpus data/*.txt
```

**What this does**: Generates multiple interpretations (claims) from your documents using different "lenses"

**Output**: SQLite database with claims table

---

### 2. **Run autonomous discovery** (Phase 17)

```bash
python3 scripts/hypothesis_discovery.py \
    --corpus data/*.txt \
    --max-hypotheses 5 \
    --min-signal-strength 0.5 \
    --output report.json
```

**What this does**:
- Scans your claim graph for anomalies
- Generates competing hypotheses
- Ranks by investigation score
- Returns top 5 worth testing

**Output**: `report.json` with:
```json
{
  "rankings": [
    {
      "hypothesis_name": "alias_same_Jeffrey_Epstein_JE",
      "investigation_score": 0.847,
      "impact_score": 0.87,
      "structural_leverage": 0.94
    }
  ]
}
```

---

### 3. **Look ONLY at these fields**

Ignore everything else. Just look at:

```json
{
  "hypothesis": "alias_same_Jeffrey_Epstein_JE",
  "investigation_score": 0.847,
  "impact": 0.87,
  "leverage": 0.94,
  "cost": 2.0
}
```

**Ask yourself**:
- Is this hypothesis **interesting**?
- Does it match **real patterns** in my documents?
- Would testing this **resolve confusion**?

---

### 4. **If YES → Test top hypothesis** (Phase 16.5)

```bash
python3 scripts/hypothesis_engine.py \
    --compare alias_same_Jeffrey_Epstein_JE \
    --with-fragility
```

**What this does**:
- Runs competing variants (same vs different)
- Tests with orthogonal pressure
- Reports winner + fragility

**Output**:
```
WINNER: same_person (composite score: 0.687)
LOSER:  different_people (composite score: 0.012)
RATIO:  57× stronger

FRAGILITY:
  Strong under: graph_pressure (0.85)
  FRAGILE under: temporal_pressure (0.12)
  Robustness: 0.58
```

---

## 🔥 The Reality Check

### **If Phase 17 produces interesting hypotheses:**

✅ It's working! Keep using it.

**Next steps**:
1. Run on more documents
2. Tune `min_signal_strength` if too many/few signals
3. test top hypotheses with Phase 16.5

---

### **If Phase 17 produces garbage:**

❌ The signal detectors or scoring need tuning.

**Debug**:
1. Check what signals were detected (`report.json → signals`)
2. Are the signals real anomalies in your data?
3. If YES: Adjust hypothesis generation
4. If NO: Adjust signal detection thresholds

---

## 🚫 What NOT to Do

**DON'T**:
- Build more infrastructure
- Add more lenses
- Tweak pressure algorithms
- Design new phases

**DO**:
- Run it on real data
- Look at actual output
- Iterate based on what you SEE

---

## 📊 Expected Output

**Good output** looks like:

```
Top 3 hypotheses by investigation score:

1. alias_same_Jeffrey_Epstein_JE                 | score: 0.847
   (impact=0.87, leverage=0.94, cost=2.0)
   → INTERESTING: High co-occurrence, should test if same person

2. timeline_impossible_Event_A_Event_B           | score: 0.723
   (impact=0.68, leverage=0.73, cost=2.0)
   → INTERESTING: Temporal violations detected, test consistency

3. conflict_resolution_Transaction_Network       | score: 0.689
   (impact=0.73, leverage=0.54, cost=1.0)
   → INTERESTING: High conflict density, worth investigating
```

**Bad output** looks like:

```
Top 3 hypotheses:

1. random_correlation_X_Y                        | score: 0.234
   → NOT INTERESTING: Weak signal, probably noise

2. trivial_observation_Z                         | score: 0.187
   → NOT INTERESTING: Obvious fact, no value in testing

3. impossible_to_test_W                          | score: 0.156
   → NOT INTERESTING: Can't actually verify this
```

If you get bad output, **tune the scoring**, don't build more.

---

## 🎯 The Formula

Phase 17 ranks hypotheses by:

```
investigation_score = (impact × novelty × uncertainty_reduction × structural_leverage) / cost
```

**Tuning knobs** (if needed):

1. **Impact**: Signal strength threshold
   - Too high → misses patterns
   - Too low → noise
   - Start with: `0.5`

2. **Cost**: Penalize expensive tests
   - Competing hypotheses: `2.0`
   - Single hypothesis: `1.0`
   - Adjust if you want to favor/avoid competing sets

3. **Uncertainty reduction**: Bonus for high-value signals
   - Conflicts: `0.9`
   - Unstable regions: `0.9`
   - Other: `0.5`
   - Adjust based on what matters in your domain

---

## 🔧 Minimal Tuning Example

If Phase 17 finds too many hypotheses:

```bash
# Increase signal strength threshold
python3 scripts/hypothesis_discovery.py \
    --min-signal-strength 0.7  # was 0.5
    --max-hypotheses 5
```

If it finds too few:

```bash
# Decrease threshold
python3 scripts/hypothesis_discovery.py \
    --min-signal-strength 0.3  # was 0.5
    --max-hypotheses 10        # test more
```

**That's it.** Don't build more infrastructure.

---

## 💡 One-Line Summary

> Run Phase 17 on your corpus. Look at top 3 hypotheses. If interesting, test them. If not, tune scoring. **Don't build more.**

---

## 🚀 Current Status

**What works**:
- ✅ Phase 17: Autonomous discovery
- ✅ Investigation scoring
- ✅ Hypothesis deduplication
- ✅ Phase 16.5: Adversarial testing

**What's missing** (blockers for real usage):
- ❌ Phase 12: Corpus → Claims extraction
- ❌ Integration: Full pipeline (corpus → claims → discovery → testing)

**To make this actually usable**, you need to connect Phase 12 to real document processing.

---

## 📝 Actual Commands (When Phase 12 Integrated)

```bash
# Full autonomous workflow
cd Bonfyre/

# 1. Extract claims from corpus
python3 scripts/hypothesis_swarm.py \
    --corpus data/transcripts/*.txt \
    --output /tmp/bonfyre-memory

# 2. Discover hypotheses
python3 scripts/hypothesis_discovery.py \
    --memory-dir /tmp/bonfyre-memory \
    --max-hypotheses 5 \
    --output discoveries.json

# 3. Look at top hypotheses
cat discoveries.json | jq '.rankings[:3]'

# 4. Test top hypothesis
python3 scripts/hypothesis_engine.py \
    --compare alias_same_Jeffrey_Epstein_JE \
    --with-fragility
```

---

## 🔥 The Bottom Line

Bonfyre has **all the machinery**. Now:

1. Make Phase 12 process real documents
2. Run the full pipeline
3. **Look at the output**
4. Tune if needed
5. **Stop building**

The system is complete. Use it.
