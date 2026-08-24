# SQLiteGraphStore

Merkle-DAG artifact graph backed by SQLite. Designed as the storage layer for BonfyreFamilyStore (Build 3).

## Subcommands
- `init` — create new graph database
- `add-atom` — insert a base atom
- `add-op` — insert an operator node (auto-computes node_hash)
- `lineage` — walk backwards from a node
- `export` — export graph as `artifact.json`

## Usage
```bash
python3 bin/sqlite_graph.py init family.db
python3 bin/sqlite_graph.py add-atom family.db --id src --hash abc123 --media-type text/plain
python3 bin/sqlite_graph.py add-op family.db --id op1 --op Clean --inputs src --output cleaned
python3 bin/sqlite_graph.py lineage family.db --id op1
python3 bin/sqlite_graph.py export family.db --out artifact.json
```
