# FaissLocalSearch

Local embedded-vector search using FAISS or native brute-force fallback.

## Subcommands
- `build-index` — build index from JSON embedding files
- `search` — cosine-similarity search against index

## Usage
```bash
python3 bin/faiss_search.py build-index ./embeddings/ --out idx.faiss --dim 384
python3 bin/faiss_search.py search idx.faiss query.json --k 5
```

Falls back to native brute-force cosine search when FAISS not installed.

## Install (optional)
```bash
pip install faiss-cpu
```
