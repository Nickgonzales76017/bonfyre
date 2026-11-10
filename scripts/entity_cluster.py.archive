#!/usr/bin/env python3
"""
entity_cluster.py — cross-document entity co-occurrence clustering

For each document in the corpus, extract named entities (people, orgs, locations).
Then build a co-occurrence graph: two entities are linked if they both appear
within the same document (or within a configurable sliding window of pages).

Outputs:
  - Ranked co-occurrence pairs (who appears near who, across how many docs)
  - Cluster assignments (connected components by minimum edge weight)
  - Markdown report + optional GraphML for Gephi/Cytoscape

Usage:
    python3 entity_cluster.py \\
        --corpus /path/to/epstein-corpus \\
        --out /tmp/epstein-bench/entity_report \\
        [--window 3] [--min-edge-weight 2] [--top-n 500]

Requirements:
    pip install spacy PyMuPDF
    python -m spacy download en_core_web_lg   # or sm/md for speed
"""

import argparse
import collections
import hashlib
import itertools
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("pip install PyMuPDF")

try:
    import spacy
except ImportError:
    spacy = None


# ── entity extraction ─────────────────────────────────────────────────────────

_NLP = None

NAME_RE = re.compile(r"\b([A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20}){1,3})\b")
ORG_RE = re.compile(r"\b(FBI|DOJ|CIA|NSA|IRS|SDNY|SDFL|USDOJ|UNITED STATES)\b", re.I)

def get_nlp(model: str = "en_core_web_lg"):
    global _NLP
    if _NLP is None:
        if spacy is None:
            print("spaCy not installed; using regex fallback extractor", file=sys.stderr)
            _NLP = False
            return _NLP
        try:
            print(f"Loading spaCy model {model}…", file=sys.stderr)
            _NLP = spacy.load(model, disable=["parser", "lemmatizer"])
        except Exception:
            print(f"spaCy model '{model}' unavailable; using regex fallback extractor", file=sys.stderr)
            _NLP = False
    return _NLP


ACCEPTED_LABELS = {"PERSON", "ORG", "GPE", "FAC", "NORP"}

# Common noisy tokens that should be excluded from entity lists
_NOISE = {
    "the", "a", "an", "i", "mr", "ms", "mrs", "dr", "sir", "lord",
    "government", "court", "united states", "state", "new york",
    "", " ",
}


def extract_entities(text: str, nlp) -> list[str]:
    """Return deduplicated normalised entity strings from text."""
    if not nlp:
        names = [m.lower() for m in NAME_RE.findall(text[:500_000])]
        orgs = [m.upper() for m in ORG_RE.findall(text[:500_000])]
        merged = names + orgs
        seen = set()
        result = []
        for ent in merged:
            norm = ent.strip()
            if norm in _NOISE or len(norm) < 3:
                continue
            if norm not in seen:
                seen.add(norm)
                result.append(norm)
        return result

    doc = nlp(text[:500_000])  # cap to avoid OOM on huge pages
    seen = set()
    result = []
    for ent in doc.ents:
        if ent.label_ not in ACCEPTED_LABELS:
            continue
        norm = ent.text.strip().lower()
        if norm in _NOISE or len(norm) < 3:
            continue
        if norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def pages_to_windows(pages: list[str], window: int) -> list[str]:
    """Yield concatenated text windows of `window` pages."""
    for i in range(0, len(pages), window):
        yield " ".join(pages[i: i + window])


def extract_from_pdf(path: Path, window: int, nlp) -> list[list[str]]:
    """
    Returns list-of-windows, each window = list of entity strings.
    window=0 → treat the whole document as one unit.
    """
    try:
        doc = fitz.open(str(path))
    except Exception as e:
        print(f"  WARN: cannot open {path.name}: {e}", file=sys.stderr)
        return []

    pages = [page.get_text("text") for page in doc]
    doc.close()

    if not pages:
        return []

    if window == 0:
        text = " ".join(pages)
        return [extract_entities(text, nlp)]

    result = []
    for window_text in pages_to_windows(pages, window):
        ents = extract_entities(window_text, nlp)
        if ents:
            result.append(ents)
    return result


# ── co-occurrence graph ───────────────────────────────────────────────────────

def build_graph(
    corpus_windows: dict[str, list[list[str]]],
    min_edge_weight: int,
) -> tuple[dict, dict]:
    """
    corpus_windows: { source_file_name: [[ent, ent, …], …] }
    Returns:
      edge_counts: { frozenset({a, b}): count_across_windows }
      node_doc_counts: { entity: set of source files it appears in }
    """
    edge_counts: dict = collections.Counter()
    node_doc_counts: dict[str, set] = collections.defaultdict(set)

    for fname, windows in corpus_windows.items():
        for window_ents in windows:
            # update node doc membership
            for ent in window_ents:
                node_doc_counts[ent].add(fname)
            # all pairs within this window
            for a, b in itertools.combinations(set(window_ents), 2):
                key = frozenset((a, b))
                edge_counts[key] += 1

    # prune by min weight
    edge_counts = {k: v for k, v in edge_counts.items() if v >= min_edge_weight}
    return edge_counts, node_doc_counts


def connected_components(edges: dict) -> dict[str, int]:
    """Union-Find over all nodes in edges. Returns {node: cluster_id}."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent.get(x, x), x)
            x = parent[x]
        return x

    def union(a: str, b: str):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for pair in edges:
        a, b = tuple(pair)
        if a not in parent:
            parent[a] = a
        if b not in parent:
            parent[b] = b
        union(a, b)

    # assign integer cluster ids
    root_to_id: dict[str, int] = {}
    result: dict[str, int] = {}
    cid = 0
    for node in parent:
        root = find(node)
        if root not in root_to_id:
            root_to_id[root] = cid
            cid += 1
        result[node] = root_to_id[root]
    return result


# ── output writers ─────────────────────────────────────────────────────────────

def write_report(
    out_dir: Path,
    edge_counts: dict,
    node_doc_counts: dict,
    clusters: dict[str, int],
    top_n: int,
    timestamp: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Top co-occurrence pairs
    top_edges = sorted(edge_counts.items(), key=lambda x: -x[1])[:top_n]

    lines = [
        "# Entity Co-occurrence Report",
        "",
        f"**Generated**: {timestamp}",
        f"**Edges (min weight applied)**: {len(edge_counts):,}",
        f"**Unique entities**: {len(node_doc_counts):,}",
        f"**Clusters**: {len(set(clusters.values())):,}",
        "",
        "---",
        "",
        f"## Top {top_n} Co-occurrence Pairs",
        "",
        "| Rank | Entity A | Entity B | Co-occurrences | Shared Docs A | Shared Docs B |",
        "|---|---|---|---|---|---|",
    ]

    for rank, (pair, count) in enumerate(top_edges, 1):
        a, b = sorted(pair)
        docs_a = len(node_doc_counts.get(a, set()))
        docs_b = len(node_doc_counts.get(b, set()))
        lines.append(f"| {rank} | {a} | {b} | {count} | {docs_a} | {docs_b} |")

    lines += [
        "",
        "---",
        "",
        "## Cluster Membership (top 20 clusters by size)",
        "",
    ]

    cluster_members: dict[int, list[str]] = collections.defaultdict(list)
    for entity, cid in clusters.items():
        cluster_members[cid].append(entity)

    sorted_clusters = sorted(cluster_members.items(), key=lambda x: -len(x[1]))[:20]
    for cid, members in sorted_clusters:
        lines.append(f"### Cluster {cid} ({len(members)} entities)")
        lines.append("")
        lines.append(", ".join(sorted(members)[:50]))
        lines.append("")

    report_path = out_dir / "entity_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {report_path}", file=sys.stderr)

    # JSON for programmatic use
    json_data = {
        "generated": timestamp,
        "top_edges": [
            {"a": sorted(p)[0], "b": sorted(p)[1], "count": c}
            for p, c in top_edges
        ],
        "clusters": {str(k): v for k, v in clusters.items()},
    }
    json_path = out_dir / "entity_graph.json"
    json_path.write_text(json.dumps(json_data, indent=2), encoding="utf-8")
    print(f"JSON: {json_path}", file=sys.stderr)

    # GraphML for Gephi/Cytoscape
    write_graphml(out_dir / "entity_graph.graphml", top_edges, node_doc_counts, clusters)


def write_graphml(
    path: Path,
    top_edges: list,
    node_doc_counts: dict,
    clusters: dict,
) -> None:
    root_el = ET.Element("graphml", xmlns="http://graphml.graphdrawing.org/graphml")
    # key declarations
    for kid, name, etype in [
        ("d0", "doc_count", "int"),
        ("d1", "cluster_id", "int"),
        ("d2", "weight", "int"),
    ]:
        k = ET.SubElement(root_el, "key", id=kid, **{"for": "node" if kid != "d2" else "edge"})
        k.set("attr.name", name)
        k.set("attr.type", etype)

    g = ET.SubElement(root_el, "graph", id="G", edgedefault="undirected")
    # gather nodes
    seen_nodes: set[str] = set()
    for pair, _ in top_edges:
        seen_nodes.update(pair)
    for node in sorted(seen_nodes):
        n = ET.SubElement(g, "node", id=node)
        d0 = ET.SubElement(n, "data", key="d0")
        d0.text = str(len(node_doc_counts.get(node, set())))
        d1 = ET.SubElement(n, "data", key="d1")
        d1.text = str(clusters.get(node, -1))
    for i, (pair, weight) in enumerate(top_edges):
        a, b = sorted(pair)
        e = ET.SubElement(g, "edge", id=f"e{i}", source=a, target=b)
        d2 = ET.SubElement(e, "data", key="d2")
        d2.text = str(weight)

    tree = ET.ElementTree(root_el)
    ET.indent(tree, space="  ")
    tree.write(str(path), encoding="unicode", xml_declaration=True)
    print(f"GraphML: {path}", file=sys.stderr)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Cross-document entity co-occurrence clustering")
    ap.add_argument("--corpus", required=True, help="Root directory of the PDF corpus")
    ap.add_argument("--out", required=True, help="Output directory for reports")
    ap.add_argument("--window", type=int, default=3,
                    help="Page window size for co-occurrence (0=whole doc)")
    ap.add_argument("--min-edge-weight", type=int, default=2,
                    help="Minimum co-occurrence count to include an edge")
    ap.add_argument("--top-n", type=int, default=500,
                    help="Top N pairs to include in the report")
    ap.add_argument("--model", default="en_core_web_lg",
                    help="spaCy model name")
    ap.add_argument("--max-docs", type=int, default=0,
                    help="Cap number of PDFs to process (0=all, useful for quick tests)")
    args = ap.parse_args()

    corpus_dir = Path(args.corpus)
    out_dir = Path(args.out)

    if not corpus_dir.exists():
        sys.exit(f"Corpus dir not found: {corpus_dir}")

    pdfs = sorted(corpus_dir.rglob("*.pdf"))
    if args.max_docs > 0:
        pdfs = pdfs[: args.max_docs]

    if not pdfs:
        sys.exit("No PDFs found in corpus directory")

    nlp = get_nlp(args.model)
    timestamp = datetime.now(timezone.utc).isoformat()

    print(f"Processing {len(pdfs)} PDFs (window={args.window})…", file=sys.stderr)

    corpus_windows: dict[str, list[list[str]]] = {}
    for i, pdf in enumerate(pdfs, 1):
        if i % 100 == 0 or i == 1:
            print(f"  {i}/{len(pdfs)}: {pdf.name}", file=sys.stderr)
        windows = extract_from_pdf(pdf, args.window, nlp)
        if windows:
            corpus_windows[pdf.name] = windows

    print("Building co-occurrence graph…", file=sys.stderr)
    edge_counts, node_doc_counts = build_graph(corpus_windows, args.min_edge_weight)

    print("Computing clusters…", file=sys.stderr)
    clusters = connected_components(edge_counts)

    print("Writing reports…", file=sys.stderr)
    write_report(out_dir, edge_counts, node_doc_counts, clusters, args.top_n, timestamp)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
