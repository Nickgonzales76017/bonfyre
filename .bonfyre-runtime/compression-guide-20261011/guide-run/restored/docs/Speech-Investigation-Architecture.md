# Bonfyre Speech Investigation — Complete Architecture

## Overview

**Speech Investigation** is a core Bonfyre capability that transforms conversations into interrogatable knowledge structures through multi-layer analysis and hypothesis testing.

```
Audio/Speech → Structured Knowledge → Autonomous Discovery → Pressure-Tested Insights
```

---

## Core Thesis

Most speech systems output text.  
Bonfyre outputs **competing interpretations ranked by evidentiary pressure**.

**Traditional approach**:
```
audio → transcript → summary → done
```

**Bonfyre approach**:
```
audio → entities → graph → claims → hypotheses → pressure → convergence → insights
      ↓          ↓       ↓       ↓           ↓          ↓              ↓
   fragments   canon  layers  embeddings  adversarial temporal    actionable
                                         testing     layers       recommendations
```

---

## Why Speech Is The Killer App

### 1. Speech Has Higher Signal Density Than Clean Text

**Clean text characteristics**:
- Curated, edited
- Contradictions removed
- Single authoritative voice
- ❌ Low hypothesis discovery potential

**Speech characteristics**:
- Messy, redundant
- Multiple perspectives preserved
- Contradictions remain
- ✅ **High hypothesis discovery potential**

### 2. Bonfyre Stack Is Optimized For Mess

The system **thrives on**:
- Contradictions → Convergence engine classifies stable/fragile/conflict
- Redundancy → Entity canonicalization + graph deduplication
- Structural weirdness → Fragment specialization for anomalies
- Multiple perspectives → Orthogonal pressure testing

### 3. Speech Unlocks Capabilities Text Cannot

**Multi-speaker truth analysis**:  
- Track competing claims across speakers
- Detect alliances/clusters via entity co-occurrence
- Timeline contradiction detection

**Temporal pattern mining**:
- How narratives shift across time
- Strategic emphasis changes
- Memory drift quantification

**Conversational structure extraction**:
- Who talks about what together
- Topic transitions and pivots
- Expertise domain mapping

---

## Full Stack Architecture

### Layer 0: Audio Intake

```bash
# Binary: BonfyreSpeechLoop
# Purpose: Audio segmentation, VAD, normalization

BonfyreSpeechLoop \
  --input raw_audio.wav \
  --vad-threshold 0.3 \
  --segment-max 30 \
  --output segments/
```

**Output**: Normalized audio segments (optimal for transcription)

---

### Layer 1: Transcription

```bash
# Binary: BonfyreTranscribe  
# Purpose: Speech → text with confidence scoring

BonfyreTranscribe \
  --input segments/*.wav \
  --model whisper-large-v3 \
  --vocab custom.bfvocab \
  --speaker-labels \
  --output transcripts/
```

**Output**: 
- `transcript.txt` — Clean text
- `transcript.json` — Detailed (timestamps, confidence, speakers, alternatives)
- `artifact.json` — Bonfyre canonical artifact manifest

**Extensions**:
- `BonfyreTranscriptClean` — Post-process for filler removal, disfluency repair
- `BonfyreTranscriptFamily` — Group related transcripts into families

---

### Layer 2: Entity Extraction (Symbolic Front-End)

```bash
# Binary: BonfyreEntity
# Purpose: Extract entities from unstructured text

BonfyreEntity \
  --input transcripts/*.txt \
  --type conversational \
  --structural-filter \
  --output entities/entities.json
```

**What it does**:
- Extract capitalized sequences (multi-word entities)
- Apply structural scoring (filter function words, contractions)
- Preserve context (sentence, document, position)
- Track frequency and co-occurrence statistics

**Output**: `entities.json` with metadata per entity mention

---

### Layer 3: Canonicalization

```bash
# Binary: BonfyreCanon
# Purpose: Group variant forms → canonical representations

BonfyreCanon \
  --input entities/entities.json \
  --fuzzy-threshold 0.85 \
  --output canon/canon.json
```

**What it does**:
- Plural/singular grouping ("Iraqi" ← "Iraqis")
- Fuzzy matching for typos/variations
- Acronym detection (CIA, FBI, USA)
- Title normalization ("President Bush" → "Bush")
- Speaker name consolidation across documents

**Output**: 
- `canon.json` — Variant → canonical mapping
- `canonical_entities.json` — Deduplicated entity list

---

### Layer 4: Graph Construction

```bash
# Binary: BonfyreGraph
# Purpose: Build entity relationship graph

BonfyreGraph \
  --entities canon/canonical_entities.json \
  --cooccurrence-window 5 \
  --temporal-edges \
  --output graph/graph.json
```

**What it does**:
- Nodes: Canonical entities
- Edges: Co-occurrence relationships (weighted by frequency)
- Temporal edges: Track entity appearance across time
- Document edges: Track entity appearance across documents/speakers

**Output**: 
- `graph.json` — Nodes + edges in standard graph format
- Compatible with graph_to_claims.py bridge

---

### Layer 5: Claims Extraction

```bash
# Binary: graph_to_claims.py (bridge script)
# Purpose: Convert graph → relational claims

python3 scripts/graph_to_claims.py \
  --graph graph/graph.json \
  --memory-dir claims/ \
  --json claims/claims.json
```

**What it does**:
- Convert edges → subject-predicate-object triples
- Compute claim_strength from edge weight
- Store in SQLite (memory.db) for hypothesis discovery
- Alternative JSON export format

**Output**:
- `memory.db` — SQLite database with claims table
- `claims.json` — JSON export

---

### Layer 6: Embedding Layer (Semantic Infrastructure)

```bash
# Binary: BonfyreEmbed
# Purpose: Generate embeddings for semantic search

BonfyreEmbed \
  --input claims/claims.json \
  --model all-MiniLM-L6-v2 \
  --fpq-compress \
  --output embeddings/
```

**What it does**:
- Generate 384-dim embeddings for each claim
- FPQ compression (0.45 B/param, cosine 0.9999+)
- Store in vector index (BonfyreVec)

**Extensions**:
- `BonfyreVec` — Vector index with SQLite-vec
- `BonfyreSLI` — Spectral Lattice Inference (4.4× bandwidth reduction)

**Use cases**:
- Semantic search across claims ("find claims about distribution strategy")
- Clustering related claims
- Detecting paraphrase clusters (same idea, different wording)

---

### Layer 7: Hypothesis Discovery (Autonomous Investigation)

```bash
# Binary: hypothesis_discovery.py  
# Purpose: Detect signals → generate hypotheses

python3 scripts/hypothesis_discovery.py \
  --memory-dir claims/ \
  --max-hypotheses 20 \
  --signal-threshold 0.7 \
  --output discovery/discovery.json
```

**What it does**:
- **Signal detection**: Statistical anomalies in claim patterns
  - Co-occurrence anomalies (unusual entity pairs)
  - Frequency spikes (repeated patterns)
  - Contradiction clusters (competing claims)
  - Temporal discontinuities (timeline breaks)
  
- **Hypothesis generation**: Competing explanations for signals
  - `alias_same`: Two names → same entity
  - `contradiction`: Competing incompatible claims
  - `temporal_drift`: Claims changing over time
  - `cluster_emergence`: New topic/concept cluster

- **Investigation scoring**: Rank by potential value
  - `investigation_score = (impact × leverage) / cost`
  - Impact: How many claims affected
  - Leverage: How much uncertainty resolved
  - Cost: Evidence required to test

**Output**:
- `discovery.json` — Signals, hypotheses, rankings

---

### Layer 8: Adversarial Testing (Phase 16.5)

```bash
# Binary: hypothesis_engine.py
# Purpose: Test hypotheses against contradicting evidence

python3 scripts/hypothesis_engine.py \
  --hypotheses discovery/discovery.json \
  --memory-dir claims/ \
  --adversarial \
  --output tested/tested_hypotheses.json
```

**What it does**:
- For each hypothesis, search for contradicting evidence
- Score supporting vs. refuting claims
- Compute confidence intervals
- Flag hypotheses requiring human review

**Output**:
- `tested_hypotheses.json` — Test results, confidence scores

---

### Layer 9: Convergence + Orthogonal Pressure

```bash
# Binary: convergence_engine.py  
# Purpose: Classify claims by stability

python3 scripts/convergence_engine.py \
  --claims claims/memory.db \
  --tested tested/tested_hypotheses.json \
  --output convergence/
```

**What it does**:
- Classify each claim:
  - **Stable**: `claim_strength > 0.8` (high consensus)
  - **Fragile**: `0.5 < claim_strength ≤ 0.8` (contested)
  - **Conflict**: `claim_strength ≤ 0.5` (contradictory)

- Compute orthogonal_pressure for each claim
- Identify hot zones needing intervention

**Output**:
- `stable_graph.json` — Convergent knowledge
- `fragile_graph.json` — Contested areas
- `conflict_graph.json` — Contradiction clusters

---

### Layer 10: Structural Intervention

```bash
# Binary: structural_intervention.py
# Purpose: Recommend resolution strategies for conflicts

python3 scripts/structural_intervention.py \
  --conflicts convergence/conflict_graph.json \
  --corpus-dir transcripts/ \
  --output interventions/
```

**What it does**:
For each conflict cluster, recommend:

1. **Fragment specialization**: Pull conflicting claims into separate fragments
2. **Layer pull + patch**: Create temporal layer to track evolution
3. **Cross-family composite**: Link speaker-specific interpretations
4. **Human annotation request**: Flag irresolvable conflicts

**Output**:
- `interventions/metadata.json` — Recommendations
- `interventions/patches/` — Structural modifications

---

### Layer 11: Fragment + Layer System (Multi-Dimensional Representation)

#### Fragment Architecture

**Purpose**: Represent competing interpretations simultaneously

```bash
# Binary: BonfyreCMS (Component: fragments)

BonfyreCMS fragment-create \
  --parent main_graph \
  --type speaker_perspective \
  --data speaker_a_claims.json

BonfyreCMS fragment-create \
  --parent main_graph \
  --type speaker_perspective \
  --data speaker_b_claims.json
```

**Use cases**:
- **Speaker perspectives**: Each speaker's view as separate fragment
- **Temporal snapshots**: How understanding evolved over conversation
- **Topic domains**: E-commerce fragment vs. AI fragment vs. product fragment
- **Confidence bands**: High-confidence fragment vs. speculative fragment

**Key operations**:
- `fragment_delta()` — Compare two fragments
- `fragment_merge()` — Combine compatible fragments
- `fragment_project()` — Extract claims matching criteria

---

#### Layer Architecture

**Purpose**: Add dimensions without multiplying fragments

**Four layer types** (from libbonfyre):

1. **Substrate Layer**: Raw transcriptions, audio segments 
2. **Transform Layer**: Entities, canonicalization, claims
3. **Surface Layer**: Hypotheses, tested claims, convergence
4. **Value Layer**: Insights, recommendations, actionable outcomes

**Temporal Layers**: Track evolution

```bash
BonfyreCMS layer-create \
  --type temporal \
  --dimension timeline \
  --data conversation_t0.json

BonfyreCMS layer-create \
  --type temporal \
  --dimension timeline \
  --data conversation_t60.json
```

**Query across layers**:
```sql
SELECT claim, timestamp, speaker, claim_strength
FROM claims
CROSS JOIN _layers AS temporal
WHERE temporal.dimension = 'timeline'
ORDER BY timestamp
```

**Spatial Layers**: Track speaker/document origin

**Epistemic Layers**: Track confidence/evidence quality

---

### Layer 12: Quality Scoring + Metering

```bash
# Binary: BonfyreMeter
# Purpose: Track usage and quality metrics

BonfyreMeter record \
  --operation transcribe \
  --input-size 180 \  # seconds
  --output artifacts/transcripts/artifact.json \
  --cost 0.006 \  # per second
  --quality-score 0.87 \  # confidence
  --ledger ledger.db
```

**What it tracks**:
- Per-operation costs (transcription, entity extraction, embedding)
- Quality metrics (confidence, hallucination rate, RTF)
- Artifact sizes (compressed vs. uncompressed)
- Value creation (hypothesis quality, convergence rate)

**Integration with BonfyreLedger**:
```bash
BonfyreLedger assess \
  --artifacts artifacts/ \
  --output portfolio.json
```

**Output**:
- `portfolio.json` — Value assessment
- Cost vs. value analysis
- Replacement cost estimation

---

### Layer 13: Compression + Storage

```bash
# Binary: BonfyreCompress
# Purpose: Family-aware compression

BonfyreCompress \
  --family speech_investigation \
  --inputs convergence/*.json \
  --codec zstd \
  --output archive/
```

**Lambda Tensor Compression** (BonfyreCMS):

For large-scale transcript archives:

```bash
BonfyreCMS compress \
  --family transcript_en_podcast \
  --members transcripts/*.json \
  --v2-huffman \  # 15% of raw at N=10K
  --output archive.bflam
```

**Results**:
- V2 Huffman: 15% of raw size at 10K members
- 2.8× better than gzip at scale
- Cross-member string dedup (CB_FAMSTR)
- Per-position canonical Huffman from family PMF

---

### Layer 14: Query + API Layer

```bash
# Binary: BonfyreAPI
# Purpose: HTTP gateway to investigation system

BonfyreAPI start \
  --port 9999 \
  --db ~/.local/share/bonfyre/queue.db \
  --static site/ \
  --rate-limit 120
```

**Endpoints**:

1. **POST /jobs/submit** — Submit audio for investigation  
   ```json
   {
     "type": "speech_investigation",
     "input": "s3://bucket/interview.mp3",
     "options": { "speakers": 2, "max_hypotheses": 20 }
   }
   ```
   Returns: `{ "job_id": "abc123", "status": "queued" }`

2. **GET /jobs/{id}** — Poll job status

3. **GET /events** — SSE stream of job progress

4. **POST /search** — Semantic claim search
   ```json
   {
     "query": "distribution strategy",
     "type": "semantic",
     "limit": 20
   }
   ```

5. **GET /graphs/{id}** — Retrieve entity graph

6. **GET /hypotheses/{id}** — Retrieval discovery results

---

### Layer 15: Production Deployment

#### Docker Deployment

```bash
# Build all 70+ binaries
make docker

# Start API + worker
docker-compose up -d
```

**docker-compose.yml**:
```yaml
services:
  api:
    image: bonfyre:latest
    command: ["/usr/local/bin/bonfyre-api", "start", "--port", "9999"]
    ports: ["9999:9999"]
    volumes: ["bonfyre-data:/data"]
    
  worker:
    image: bonfyre:latest
    command: ["/usr/local/bin/bonfyre-queue", "work", "--threads", "4"]
    volumes: ["bonfyre-data:/data"]
    environment:
      WHISPER_MODEL: "large-v3"
```

#### Queue-Based Processing

```bash
# Binary: BonfyreQueue
# Purpose: Async job management

# Enqueue job
BonfyreQueue enqueue \
  --cmd "bash scripts/run_investigation.sh audio.mp3 output/" \
  --priority 5

# Worker daemon
BonfyreQueue work --threads 4
```

**Features**:
- SQLite WAL-backed queue
- Exponential backoff retry
- Webhook notifications
- Event streaming via SSE

---

## Production Use Cases

### 1. Deposition Analysis (Legal)

**Input**: Multi-hour deposition video

**Pipeline**:
```bash
#!/bin/bash
set -e

# Extract audio
ffmpeg -i deposition.mp4 -vn -ar 16000 audio.wav

# Segment + transcribe
BonfyreSpeechLoop --input audio.wav --vad --output segments/
BonfyreTranscribe --input segments/*.wav --speakers --output transcripts/

# Clean + family
BonfyreTranscriptClean --input transcripts/*.txt --legal-mode --output clean/
BonfyreTranscriptFamily --input clean/*.txt --group-by witness --output families/

# Run investigation
bash scripts/run_investigation.sh "families/*/*.txt" investigation/

# Analyze contradictions
python3 scripts/analyze_contradictions.py \
  --conflicts investigation/graphs/conflict_graph.json \
  --speakers families/speaker_map.json \
  --output report/
```

**Output**:
- Timeline of claims per witness
- Contradiction clusters (witness A vs. B on topic X)
- Entity relationship graphs per witness
- Temporal drift analysis (how stories changed)

**Value**: Automates 100+ hours of manual review

---

### 2. Customer Interview Mining (Product)

**Input**: 50 customer interviews (~3 hours each)

**Pipeline**:
```bash
# Batch transcribe
for f in interviews/*.mp3; do
  BonfyreTranscribe --input "$f" --output transcripts/
done

# Run full investigation
bash scripts/run_investigation.sh "transcripts/*.txt" analysis/

# Extract product insights
python3 scripts/extract_product_signals.py \
  --discoveries analysis/reports/discovery.json \
  --entities analysis/symbolic/entities.json \
  --output insights/
```

**Insights Extracted**:
- Pain point clusters (what problems mentioned most)
- Feature request patterns (what solutions suggested)
- Competitor mentions (who's being compared)
- Usage workflow descriptions (how they actually use product)
- Decision criteria (what factors drove purchase)

**Value**: Turn 150 hours of interviews → structured product roadmap

---

### 3. Podcast Series Knowledge Graph (Media)

**Input**: 200 episodes of podcast (~50 hours total)

**Pipeline**:
```bash
# Download series
for url in $(cat urls.txt); do
  yt-dlp -x --audio-format mp3 "$url" -o "podcasts/%(title)s.mp3"
done

# Batch investigation
bash scripts/speech_to_investigation.sh "podcasts/*.mp3" knowledge_graph/

# Build cross-episode graph
python3 scripts/build_podcast_graph.py \
  --episodes knowledge_graph/symbolic/*.json \
  --temporal \
  --output series_graph/
```

**Insights**:
- Guest expertise mapping (who talks about what)
- Topic evolution over series (how themes shift)
- Recurring concepts/entities across episodes
- Citation network (which episodes reference each other)
- Strategic narrative arcs

**Value**: Automated podcast research assistant

---

### 4. Organizational Knowledge Capture (Enterprise)

**Input**: All company meetings for 1 year

**Pipeline**:
```bash
# Continuous ingestion
while true; do
  # Check for new recordings
  new_files=$(find /mnt/recordings -name "*.wav" -mtime -1)
  
  for f in $new_files; do
    # Queue processing
    BonfyreQueue enqueue \
      --cmd "bash scripts/speech_to_investigation.sh '$f' /data/org_knowledge/" \
      --webhook "https://company.com/api/knowledge_update"
  done
  
  sleep 3600  # Check hourly
done
```

**Continuous outputs**:
- Real-time entity graph (who/what company talks about)
- Strategic shift detection (what leadership emphasizes changing)
- Cross-department contradiction tracking
- Organizational memory (searchable across all meetings)
- Expertise directory (who knows what)

**Value**: Institutional knowledge preservation + discovery

---

## Embeddings + Semantic Search Integration

### Setup Vector Index

```bash
# Generate embeddings for all claims
BonfyreEmbed \
  --input claims/claims.json \
  --model all-MiniLM-L6-v2 \
  --batch-size 256 \
  --fpq-compress \
  --output embeddings/

# Build vector index
BonfyreVec create \
  --embeddings embeddings/*.fpq \
  --index-type hnsw \
  --m 16 \
  --ef-construction 200 \
  --output vector.db
```

### Semantic Query API

```python
import requests

# Search for claims about "distribution strategy"
response = requests.post("http://localhost:9999/search", json={
    "query": "distribution strategy for e-commerce products",
    "type": "semantic",
    "limit": 20,
    "min_score": 0.7
})

results = response.json()["claims"]
for claim in results:
    print(f"{claim['score']:.3f}: {claim['text']}")
    print(f"  Speaker: {claim['speaker']}, Time: {claim['timestamp']}")
```

### Spectral Lattice Inference (SLI) Acceleration

**Standard embedding search**:
```
Query → Embed (10ms) → Dense matmul (50ms) → Top-K (5ms) = 65ms
```

**SLI-accelerated search**:
```
Query → Embed (10ms) → SLI lookup (10ms) → Top-K (5ms) = 25ms
```

**Bandwidth**: 4.4× reduction (116 B/block vs 512 B)

**Quality**: Cosine 0.9999+ (lossless for semantic search)

**Integration**:
```bash
# Prepare index for SLI
BonfyreSLI prepare \
  --embeddings embeddings/*.fpq \
  --output embeddings_sli/

# Query via SLI
BonfyreSLI query \
  --index embeddings_sli/ \
  --query "distribution strategy" \
  --top-k 20
```

---

## FPQ Model Compression for On-Device Inference

For running lightweight models **on the investigation results**:

### Use Case: Claim Summarization

```bash
# Compress Qwen 0.5B for summarization
BonfyreFPQ encode \
  --model ~/.local/share/models/qwen-0.5b.safetensors \
  --output models/qwen-0.5b.fpq \
  --version 12 \  # E8 + rANS entropy
  --coord-bits 3

# Result: 265 MB (vs 2 GB BF16) = 7.5× compression
# Quality: PPL 12.07 vs 11.95 baseline (+0.9% degradation)
```

### Inference on Claims

```python
from bonfyre import FPQModel

# Load compressed model
model = FPQModel.load("models/qwen-0.5b.fpq")

# Summarize conflict cluster
prompt = f"""Summarize this contradiction:

Speaker A: {claims_a}
Speaker B: {claims_b}

What's the core disagreement?"""

summary = model.generate(prompt, max_tokens=100)
```

**Value**: Run sophisticated NLP on-device, no API costs

---

## Quality Metrics + Benchmarking

### Per-Operation Metrics

**Transcription Quality**:
- Word Error Rate (WER)
- Confidence score per word/segment
- Hallucination detection (repeated phrases, nonsense)
- RTF (Real-Time Factor): Processing time / audio duration

**Entity Extraction Quality**:
- Precision: % extracted entities that are real
- Recall: % real entities that were extracted
- F1 score
- Structural filter effectiveness (% noise removed)

**Hypothesis Quality**:
- % hypotheses confirmed after testing
- Average investigation score
- Convergence rate (stable vs. fragile vs. conflict)

### Benchmark Suite

```bash
# Run full benchmark
bash scripts/batch_all_apps.sh

# Output: /tmp/bonfyre-bench-full/
#   - Per-operation timing
#   - Quality scores
#   - Compression ratios
#   - Value metrics
```

**Tracked Metrics**:
- Pipeline RTF (total processing / audio duration)
- Average confidence score
- Entity extraction precision/recall
- Hypothesis confirmation rate
- Storage efficiency (compressed / raw)
- Cost per hour of audio processed

---

## Performance Characteristics

### Scalability

**Single conversation** (1 hour audio):
- Transcription: 6 minutes (RTF 0.1 on A40 GPU)
- Entity extraction: 30 seconds
- Graph construction: 15 seconds  
- Hypothesis discovery: 1 minute
- Total: ~8 minutes

**100 conversations** (100 hours audio):
- Parallel processing (4 workers): ~3 hours
- With queue: Asynchronous, incremental

**1000 conversations** (1000 hours = 6 weeks):
- Distributed workers: ~1 day
- Continuous processing: Real-time as recorded

### Resource Requirements

**Minimal** (development):
- CPU: 4 cores
- RAM: 8 GB
- Disk: 100 GB

**Production** (100 concurrent conversations):
- CPU: 32 cores
- RAM: 64 GB
- GPU: 1× RTX 4090 (transcription)
- Disk: 2 TB SSD

**Enterprise** (1000+ conversations):
- Kubernetes cluster
- GPU pool for transcription
- Distributed SQLite (via Turso/LiteFS)
- S3/compatible for audio/artifact storage

---

## Security + Privacy

### Data Handling

**Audio storage**:
- Option 1: Delete after transcription
- Option 2: Encrypt + archive (customer retention requirements)
- Option 3: On-premise only (no cloud)

**Transcript storage**:
- PII detection + redaction (BonfyreEntity with PII mode)
- Speaker pseudonymization (Speaker A, B, C vs. real names)
- Selective disclosure (different fragments for different access levels)

### Access Control

**Fragment-level permissions**:
```json
{
  "fragment_id": "speaker_a_perspective",
  "access": {
    "view": ["legal_team", "researcher_1"],
    "edit": [],
    "admin": ["researcher_lead"]
  }
}
```

**Audit trail**:
```bash
BonfyreAPI audit \
  --resource graphs/conflict_graph.json \
  --since 2026-04-01
```

---

## Next Steps

### Immediate (Week 1)

1. ✅ Replace Python entity stubs with BonfyreEntity binary
2. ✅ Replace Python canon stubs with BonfyreCanon binary  
3. ✅ Replace Python graph stubs with BonfyreGraph binary
4. ⏳ Add BonfyreEmbed integration for semantic search
5. ⏳ Add BonfyreMeter/BonfyreLedger for value tracking

### Short-term (Month 1)

1. Speaker diarization integration (BonfyreSpeechLoop)
2. Fragment-based speaker perspective separation
3. Temporal layer for conversation evolution tracking
4. Quality benchmark suite
5. Production deployment guide (Docker + K8s)

### Medium-term (Quarter 1)

1. SLI-accelerated semantic search
2. Multi-conversation pattern detection
3. Cross-corpus hypothesis discovery
4. FPQ-compressed on-device summarization models
5. Enterprise SSO + RBAC

### Long-term (Year 1)

1. Real-time streaming investigation (WebSocket + incremental processing)
2. Multi-modal analysis (video + slides + transcripts)
3. Predictive hypothesis generation (ML-driven)
4. Cross-lingual investigation (multi-language support)
5. Federated investigation (privacy-preserving multi-party analysis)

---

## Key Insights

### 1. Speech Is Superior Input

Traditional systems treat speech as degraded text.  
Bonfyre treats speech as **richer signal** with preserved contradictions, redundancy, and multi-perspective structure.

### 2. Hypothesis Discovery Is The Core Capability

Not "extract entities" or "build graphs" — those are means.  
The end is: **What competing interpretations exist, and which survive scrutiny?**

### 3. Fragments Enable Simultaneous Truths

Rather than forcing convergence to single truth, fragments preserve:
- Speaker A's perspective  
- Speaker B's perspective
- Temporal snapshots (how understanding evolved)
- Confidence bands (what's certain vs. speculative)

### 4. Layers Add Dimensions Without Fragmentation

Instead of creating fragments for every dimension (speaker × time × topic = explosion), layers provide orthogonal views:
- Substrate: Raw data
- Transform: Structured extraction
- Surface: Tested hypotheses
- Value: Actionable insights

### 5. The System Improves With Scale

**Text systems**: More data = more noise, harder to navigate

**Bonfyre speech investigation**: More conversations = 
- Better entity canonicalization (more variants seen)
- Stronger hypothesis testing (more evidence)
- Richer temporal patterns (longer timelines)
- Higher quality convergence (more pressure applied)

---

## Conclusion

Speech investigation is not a feature.  
It's **Bonfyre's asymmetric advantage**.

Every other system outputs summaries.  
Bonfyre outputs **interrogatable knowledge structures** with:

- Competing hypotheses ranked by evidence
- Convergence metrics (stable vs fragile vs conflict)
- Multi-dimensional analysis (speaker × time × topic)
- Semantic search across all claims
- Autonomous discovery of hidden patterns
- Pressure-tested insights ready for action

**The thesis**: 

> Conversations contain competing interpretations.
> Most systems pick one and discard the rest.
> Bonfyre preserves all, tests all, and ranks all by evidentiary pressure.

That's not text processing.  
That's **conversation interrogation**.

---

**Status**: Architecture complete, production-ready components exist, integration in progress

**Next**: See [Speech-Investigation-Quickstart.md](Speech-Investigation-Quickstart.md) for usage guide
