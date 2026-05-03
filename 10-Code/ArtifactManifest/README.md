# ArtifactManifest

Universal typed manifest system for Bonfyre artifact families.

Every artifact family writes an `artifact.json` that describes:
- **Atoms** — irreducible source inputs
- **Operators** — derivation steps (Clean, Summarize, Narrate, Pack, …)
- **Realizations** — concrete outputs (pinned on disk or reconstructable)
- **Realization targets** — what CAN be produced but hasn't been yet
- **Merkle hashes** — SHA-256 operator DAG for lineage, dedup, and change detection

## Quick Start

Validate a manifest:
```bash
python3 validate_artifact.py examples/brief.artifact.json
```

Compute Merkle hashes (print only):
```bash
python3 validate_artifact.py --compute-hashes examples/transcript-family.artifact.json
```

Compute and write hashes into the manifest:
```bash
python3 validate_artifact.py --update-hashes examples/brief.artifact.json
```

Run the smoke test:
```bash
bash tests/smoke.sh
```

## Schema

See [`artifact.schema.json`](artifact.schema.json) for the full JSON Schema (Draft 2020-12).

### Top-level fields

| Field | Required | Description |
|---|---|---|
| `schema_version` | yes | Always `"1.0.0"` |
| `artifact_id` | yes | Unique family instance ID |
| `artifact_type` | yes | `transcript`, `brief`, `proof`, `offer`, `pack`, `distribution`, `narration`, `summary`, `custom` |
| `created_at` | yes | ISO-8601 timestamp |
| `atoms` | yes | Array of base atoms (min 1) |
| `operators` | yes | Array of operator nodes |
| `realizations` | yes | Array of realized outputs |
| `realization_targets` | no | Declarative list of potential outputs |
| `root_hash` | no | SHA-256 Merkle root (computed by CLI) |
| `metadata` | no | Arbitrary artifact-type-specific data |

## Examples

- `examples/transcript-family.artifact.json` — Transcript → Clean → Paragraphize chain
- `examples/brief.artifact.json` — BonfyreBrief output
- `examples/proof.artifact.json` — BonfyreProof bundle
- `examples/offer.artifact.json` — BonfyreOffer output
- `examples/pack.artifact.json` — BonfyrePack assembly

## Architecture

```
Atom (source file)
  → Operator (derivation step)
    → Realization (concrete output, optionally pinned)
      → Realization Target (future potential output)

Every operator node hashes: op + params + child hashes + version
Root hash = SHA-256 of all sorted operator hashes
```

## Related

- [Research - Piper x Lambda Tensors Deep Synthesis](../../03-Research/Research%20-%20Piper%20x%20Lambda%20Tensors%20Deep%20Synthesis.md)
- [Research - Lambda Tensors Compression Direction](../../03-Research/Research%20-%20Lambda%20Tensors%20Compression%20Direction.md)
- [Project - Artifact JSON Universal Manifest](../../02-Projects/Project%20-%20Artifact%20JSON%20Universal%20Manifest.md)
