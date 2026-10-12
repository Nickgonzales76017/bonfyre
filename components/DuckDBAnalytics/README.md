# DuckDBAnalytics

Local analytics + transformation engine over Bonfyre artifact manifests.

## Subcommands
- `query` — run SQL over artifact JSON (requires `pip install duckdb`)
- `family-stats` — compute family metrics from an `artifact.json`
- `bench-compare` — compare two directories (naive vs family storage)

## Usage
```bash
python3 bin/duckdb_analytics.py family-stats examples/brief.artifact.json
python3 bin/duckdb_analytics.py bench-compare /tmp/naive /tmp/family
python3 bin/duckdb_analytics.py query "SELECT count(*) FROM data" --data ./artifacts/
```
