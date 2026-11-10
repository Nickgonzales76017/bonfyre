Export Vault Concatenation
==========================

This small script concatenates all Markdown files under your vault root into a single
`combined_vault.md` file suitable for upload or bulk analysis.

Usage (from repo root):

```bash
python3 .bonfyre-runtime/export_vault_concat.py --root . --out combined_vault.md
```

Notes:
- The script skips `.git`, `node_modules`, `.obsidian`, and the output file itself.
- Each file is prefixed with an HTML comment `<!-- FILE: path -->` so you can find
  original locations in the combined document.
