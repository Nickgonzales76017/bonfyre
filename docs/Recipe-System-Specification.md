# Recipe System Specification

## Executive Summary

The Bonfyre Recipe System provides **declarative pipeline composition** via:
- **JSON schema** for pipeline recipes
- **Composition operators** (⊕ serial,⊗ parallel, ⊕ merge, 🔁 loop)
- **BonfyreRecipe** registry (SQLite + FTS5)
- **BonfyreRun** executor (DAG scheduler)

**Goal**: Replace bash scripts with content-addressed, reproducible recipes.

---

## Recipe Schema (v1)

### Core Structure

```json
{
  "recipe_id": "A3",
  "name": "Full Speech Investigation Pipeline",
  "version": "1.0.0",
  "description": "23-stage pipeline: audio → convergence analysis",
  "hash": "sha256:67f3a2e...",
  "created": "2026-04-20T10:00:00Z",
  
  "inputs": [
    {"name": "audio", "type": "file", "pattern": "*.{wav,mp3,m4a}"}
  ],
  
  "outputs": [
    {"name": "brief", "path": "{out}/brief.md"},
    {"name": "graph", "path": "{out}/graph.db"},
    {"name": "claims", "path": "{out}/claims.db"},
    {"name": "convergence", "path": "{out}/convergence.json"}
  ],
  
  "stages": [
    {
      "id": "s01",
      "name": "VAD Segmentation",
      "operator": "BonfyreSpeechLoop",
      "args": ["--input", "{input}", "--out", "{out}/segments"],
      "inputs": ["{input}"],
      "outputs": ["{out}/segments"],
      "parallel": 1
    },
    {
      "id": "s02",
      "name": "Transcription",
      "operator": "BonfyreTranscribe",
      "args": ["--input", "{out}/segments", "--model", "whisper-base"],
      "inputs": ["{out}/segments"],
      "outputs": ["{out}/transcript.txt"],
      "parallel": 8,
      "depends_on": ["s01"]
    },
    {
      "id": "s03",
      "name": "Entity Extraction",
      "operator": "BonfyreEntity",
      "args": ["--input", "{out}/transcript.txt", "--out", "{out}/entities.json"],
      "inputs": ["{out}/transcript.txt"],
      "outputs": ["{out}/entities.json"],
      "parallel": 1,
      "depends_on": ["s02"]
    }
  ],
  
  "models": [
    {"name": "whisper-base", "hash": "sha256:abc123...", "size_mb": 142}
  ],
  
  "metadata": {
    "category": "speech-investigation",
    "tags": ["audio", "transcription", "knowledge-graph"],
    "author": "bonfyre",
    "complexity": "high",
    "estimated_time_minutes": 11
  }
}
```

---

## Composition Operators

### ⊕ Serial Composition (Pipeline)

**Operation**: `recipe_c = recipe_a ⊕ recipe_b`

**Semantics**: 
- Execute `recipe_a` → produce intermediate outputs
- Feed intermediate outputs as inputs to `recipe_b`
- Final output = `recipe_b` outputs

**JSON Representation**:

```json
{
  "recipe_id": "SERIAL_AB",
  "composition": {
    "op": "serial",
    "recipes": ["A1", "A2"]
  }
}
```

**DAG**:
```
A1_stage1 → A1_stage2 → A2_stage1 → A2_stage2
```

---

### ⊗ Parallel Composition (Fan-out)

**Operation**: `recipe_c = recipe_a ⊗ recipe_b`

**Semantics**:
- Execute `recipe_a` and `recipe_b` concurrently
- Both receive same inputs
- Final output = union of outputs

**JSON Representation**:

```json
{
  "recipe_id": "PARALLEL_AB",
  "composition": {
    "op": "parallel",
    "recipes": ["A1", "M1"],
    "merge_outputs": true
  }
}
```

**DAG**:
```
        ┌─ A1_stage1 → A1_stage2
input ──┤
        └─ M1_stage1 → M1_stage2
```

---

### ⊕ Merge Composition (Fan-in)

**Operation**: `recipe_c = merge([recipe_a, recipe_b], merger_fn)`

**Semantics**:
- Execute `recipe_a` and `recipe_b` in parallel
- Merge outputs using `merger_fn` (e.g., BonfyreMerge)

**JSON Representation**:

```json
{
  "recipe_id": "MERGE_AB",
  "composition": {
    "op": "merge",
    "recipes": ["A1", "A2"],
    "merger": {
      "operator": "BonfyreMerge",
      "args": ["--strategy", "union"]
    }
  }
}
```

**DAG**:
```
        ┌─ A1 → output_a ─┐
input ──┤                  ├→ BonfyreMerge → merged_output
        └─ A2 → output_b ─┘
```

---

### 🔁 Loop Composition (Iterative Refinement)

**Operation**: `recipe_c = loop(recipe_a, condition, max_iterations)`

**Semantics**:
- Execute `recipe_a`
- Check `condition` on outputs
- If not met, feed outputs back as inputs and repeat
- Stop after `max_iterations`

**JSON Representation**:

```json
{
  "recipe_id": "LOOP_A",
  "composition": {
    "op": "loop",
    "recipe": "A1",
    "condition": {
      "type": "file_exists",
      "path": "{out}/converged.flag"
    },
    "max_iterations": 10
  }
}
```

**DAG**:
```
A1 → check → [not met] → A1 → check → ... → [met] → output
```

---

## Recipe Registry (BonfyreRecipe)

### SQLite Schema

```sql
CREATE TABLE recipes (
    recipe_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    hash TEXT UNIQUE NOT NULL,
    json_data TEXT NOT NULL,  -- Full JSON
    created_at INTEGER NOT NULL,
    category TEXT,
    UNIQUE(recipe_id, version)
);

CREATE TABLE recipe_fts (
    recipe_id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    tags TEXT
);
CREATE VIRTUAL TABLE recipe_fts USING fts5(name, description, tags);

CREATE TABLE recipe_tags (
    recipe_id TEXT,
    tag TEXT,
    FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),
    PRIMARY KEY(recipe_id, tag)
);

CREATE TABLE recipe_deps (
    recipe_id TEXT,
    stage_id TEXT,
    dep_recipe_id TEXT,  -- For composed recipes
    dep_stage_id TEXT,   -- For stage dependencies
    FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id)
);

CREATE TABLE models (
    model_name TEXT PRIMARY KEY,
    hash TEXT NOT NULL,
    size_mb INTEGER,
    source_url TEXT
);

CREATE TABLE recipe_models (
    recipe_id TEXT,
    model_name TEXT,
    FOREIGN KEY(recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY(model_name) REFERENCES models(model_name),
    PRIMARY KEY(recipe_id, model_name)
);
```

---

### CLI Commands

#### `bonfyre-recipe init`

Initialize registry database.

```bash
bonfyre-recipe init [--db PATH]
```

**Behavior**:
- Create SQLite database at `~/.bonfyre/recipes.db` (or custom path)
- Create schema
- Insert 10 built-in recipes (A1-A3, M1, P1-P2, V1, R1, L1, G1)

---

#### `bonfyre-recipe list`

List all recipes.

```bash
bonfyre-recipe list [--category CATEGORY] [--tag TAG]
```

**Output**:
```
A1   Audio Quick Brief         audio,brief             0.5 min
A2   Audio Archive             audio,archive           3 min
A3   Full Speech Investigation audio,investigation     11 min
M1   Media/Podcast Pipeline    media,podcast           15 min
P1   Proof Bundle              legal,proof             2 min
...
```

---

#### `bonfyre-recipe show`

Show recipe details.

```bash
bonfyre-recipe show <RECIPE_ID>
```

**Output**: Full JSON + stage DAG visualization

---

#### `bonfyre-recipe add`

Add custom recipe.

```bash
bonfyre-recipe add <RECIPE.json>
```

**Behavior**:
- Validate JSON schema
- Compute SHA-256 hash
- Insert into registry
- Check for conflicts

---

#### `bonfyre-recipe validate`

Validate recipe JSON.

```bash
bonfyre-recipe validate <RECIPE.json>
```

**Checks**:
- Schema validity
- Stage dependency cycles
- Operator existence
- Input/output consistency

---

#### `bonfyre-recipe search`

Full-text search.

```bash
bonfyre-recipe search <QUERY>
```

**Example**:
```bash
$ bonfyre-recipe search "transcription"
A1   Audio Quick Brief
A2   Audio Archive
A3   Full Speech Investigation
M1   Media/Podcast Pipeline
```

---

## Recipe Executor (BonfyreRun)

### Execution Flow

```
1. Load recipe JSON from registry
2. Build dependency DAG (topological sort)
3. Assign stages to parallel levels
4. For each level:
   a. Fork up to 8 stages concurrently
   b. Substitute {input}/{out} variables
   c. Execute operator binaries
   d. Wait for all to complete
5. Write run manifest (signed Ed25519)
6. Return status
```

---

### CLI Commands

#### `bonfyre-run <RECIPE_ID>`

Execute recipe.

```bash
bonfyre-run A3 --input audio.wav --out results/ [--dry-run] [--resume]
```

**Flags**:
- `--input PATH`: Input file/directory
- `--out DIR`: Output directory
- `--dry-run`: Show plan, don't execute
- `--resume`: Resume from failed stage
- `--from-stage ID`: Start from specific stage
- `--to-stage ID`: Stop at specific stage
- `--tier TIER`: Execution tier (local, cluster, cloud)
- `--batch`: Batch mode (no progress bar)

---

#### `bonfyre-run compose`

Execute composed recipe.

```bash
bonfyre-run compose --serial A1 A2 --input audio.wav --out results/
bonfyre-run compose --parallel A1 M1 --input audio.wav --out results/
```

---

### Run Manifest (Signed)

Every execution produces `run-manifest.json`:

```json
{
  "manifest_version": "1.0.0",
  "recipe_id": "A3",
  "recipe_hash": "sha256:67f3a2e...",
  "started_at": "2026-04-20T10:00:00Z",
  "completed_at": "2026-04-20T10:11:30Z",
  "status": "success",
  
  "inputs": {
    "audio": "audio.wav"
  },
  
  "outputs": {
    "brief": "results/brief.md",
    "graph": "results/graph.db"
  },
  
  "stages": [
    {
      "id": "s01",
      "status": "success",
      "started_at": "2026-04-20T10:00:05Z",
      "completed_at": "2026-04-20T10:01:20Z",
      "duration_seconds": 75,
      "operator": "BonfyreSpeechLoop",
      "exit_code": 0
    }
  ],
  
  "signature": {
    "algorithm": "ed25519",
    "public_key": "base64:...",
    "signature": "base64:..."
  }
}
```

---

## Built-In Recipes

### A1 — Audio Quick Brief

**Description**: Minimal pipeline for fast brief generation.

**Stages**: VAD → Transcribe → Brief (3 stages)

**Time**: ~0.5 minutes (1 min audio)

**Output**: `brief.md`

---

### A3 — Full Speech Investigation

**Description**: Complete 23-stage pipeline (see Universal-Investigation-Paradigm.md).

**Stages**: VAD → Transcribe → Entity → Canon → Graph → Claims → Hypotheses → Adversarial Test → Convergence

**Time**: ~11 minutes (1 hr audio)

**Outputs**: `brief.md`, `graph.db`, `claims.db`, `convergence.json`

---

### M1 — Media/Podcast Pipeline

**Description**: Audio → transcript → show notes → clips → distribution.

**Stages**: VAD → Transcribe → MediaPrep → Distribute

**Time**: ~15 minutes

**Outputs**: `shownotes.md`, `clips/`, `rss.xml`

---

## Implementation Priorities

**Phase 3.1** (Current):
- [x] Recipe schema design
- [ ] BonfyreRecipe binary (SQLite registry)
- [ ] 10 built-in recipes (JSON)

**Phase 3.2**:
- [ ] BonfyreRun executor (DAG scheduler)
- [ ] Composition operators
- [ ] Run manifest signing

**Phase 3.3**:
- [ ] Integration with BonfyreStitch
- [ ] Cache management
- [ ] Multi-tier execution

---

## Success Criteria

1. **Zero bash scripts**: All pipelines via `bonfyre-run`
2. **Content-addressed**: Every recipe has SHA-256 hash
3. **Reproducible**: Same inputs → same outputs → same manifest
4. **Composable**: Build complex recipes from primitives
5. **Auditable**: Signed manifests for compliance
