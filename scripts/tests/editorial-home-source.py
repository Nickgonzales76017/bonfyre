#!/usr/bin/env python3
"""Require a source-owned index, not a generated index that acts as its own input."""
from pathlib import Path
from hashlib import sha256
import subprocess

root=Path(__file__).resolve().parents[2]
template=(root/"editorial/journal-home.html").read_text(encoding="utf-8")
assert template.count("<!-- JOURNAL_CARD_GRID -->")==1, "Source template missing or duplicated collection"
assert "data-article data-format=" not in template, "Rendered cards leaked into source"
published=root/"site/blog/index.html"
before=sha256(published.read_bytes()).hexdigest()
for _ in range(2):
    subprocess.run(["sh","scripts/render-editorial.sh","--check"],cwd=root,check=True,capture_output=True,text=True)
    assert sha256(published.read_bytes()).hexdigest()==before, "Render/validation modified generated output"
assert published.read_text(encoding="utf-8").count(' data-article data-format=')==66
print("JOURNAL_TEMPLATE_PARITY_PASS source-template and 66 output cards deterministic")
