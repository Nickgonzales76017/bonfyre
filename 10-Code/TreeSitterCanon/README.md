# TreeSitterCanon

Structural parsing and canonicalization operator via tree-sitter.

## Subcommands
- `parse` — produce AST JSON from source file
- `canon` — canonicalize structure (strip comments, normalize identifiers)
- `diff` — structural diff between two files

## Usage
```bash
python3 bin/treesitter_canon.py parse src/main.c --out ast.json
python3 bin/treesitter_canon.py diff old.py new.py
```

Falls back to naive line-level parsing when tree-sitter not installed.
