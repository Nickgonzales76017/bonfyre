#!/usr/bin/env python3
"""
temporal_dag.py — Temporal entity clustering + graph diff primitive.

Reads ingest_manifest.jsonl (from djvu_ingest.py) + entity JSON files
(from entity_cluster.py) to build a time-stamped entity co-occurrence graph
where edges carry the earliest/latest filing dates and co-mention frequency.

Graph diff:
  graph_diff(snapshot_a.graphml, snapshot_b.graphml)
  → added/removed nodes, changed edge weights, new clusters

Two output formats:
  GraphML  — for Gephi / networkx / yEd
  JSONL    — for Bonfyre pipeline / vector search

Temporal clusters:
  Entities that co-occur in documents filed within N days of each other
  form a temporal cluster. Default window: 30 days.

Usage:
  # Build graph from ingest + entity data
  python3 temporal_dag.py build \\
    --manifest /tmp/epstein-bench/ingested/ingest_manifest.jsonl \\
    --entities /tmp/epstein-bench/entities/ \\
    --out /tmp/epstein-bench/temporal/

  # Diff two graph snapshots
  python3 temporal_dag.py diff \\
    --before /tmp/epstein-bench/temporal/graph_checkpoint_a.graphml \\
    --after  /tmp/epstein-bench/temporal/graph_checkpoint_b.graphml

  # Extract temporal clusters
  python3 temporal_dag.py clusters \\
    --graph /tmp/epstein-bench/temporal/graph.graphml \\
    --window-days 30

Outputs:
  build →  graph.graphml, graph.jsonl, cluster_report.json
  diff  →  diff.json   {added_nodes, removed_nodes, changed_edges, new_clusters}
  clusters → clusters.json
"""

import argparse
import collections
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# ── date parsing ──────────────────────────────────────────────────────────────

DATE_PATTERNS = [
    r'\b(\d{4})-(\d{2})-(\d{2})\b',           # 2006-07-14
    r'\b(\w+ \d{1,2}),?\s+(\d{4})\b',          # July 14, 2006
    r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b',        # 7/14/2006
    r'\b(\d{1,2})/(\d{1,2})/(\d{2})\b',        # 7/14/06
]

MONTH_MAP = {m: i+1 for i, m in enumerate([
    'january','february','march','april','may','june',
    'july','august','september','october','november','december'
])}
MONTH_ABBR = {m[:3]: v for m, v in MONTH_MAP.items()}

def _parse_date(s: str) -> Optional[date]:
    s = s.strip()
    # ISO
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError: pass
    # Month name
    m = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', s, re.I)
    if m:
        mon_s = m.group(1).lower()
        mon = MONTH_MAP.get(mon_s) or MONTH_ABBR.get(mon_s[:3])
        if mon:
            try: return date(int(m.group(3)), mon, int(m.group(2)))
            except ValueError: pass
    # Slash
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', s)
    if m:
        yr = int(m.group(3))
        if yr < 100: yr += 2000 if yr < 30 else 1900
        try: return date(yr, int(m.group(1)), int(m.group(2)))
        except ValueError: pass
    return None

def extract_dates_from_text(text: str) -> list[date]:
    dates = []
    for pat in DATE_PATTERNS:
        for m in re.finditer(pat, text, re.I):
            d = _parse_date(m.group(0))
            if d and 1990 <= d.year <= 2030:
                dates.append(d)
    return sorted(set(dates))


# ── entity extraction (lightweight, no spaCy required) ────────────────────────

NAME_RE = re.compile(
    r'\b([A-Z][a-z]{1,15}(?:\s[A-Z][a-z]{1,15}){1,3})\b'
)
ORG_KEYWORDS = {'FBI', 'DOJ', 'USAO', 'SDFL', 'SDNY', 'DOD', 'CIA', 'NSA', 'IRS-CI', 'ATF'}

def extract_entities(text: str) -> list[str]:
    names = NAME_RE.findall(text[:50000])
    # Filter extremely common false positives
    skip = {'The', 'This', 'That', 'United States', 'New York', 'Palm Beach',
            'Virgin Islands', 'New Mexico', 'District Court', 'Grand Jury'}
    entities = [n for n in names if n not in skip and len(n.split()) >= 2]
    orgs = [k for k in ORG_KEYWORDS if k in text]
    return list(dict.fromkeys(entities + orgs))  # dedupe, preserve order


# ── Graph data structure ───────────────────────────────────────────────────────

class EntityGraph:
    """
    Sparse co-occurrence graph.
    node: entity_name → {doctype, mention_count, first_date, last_date}
    edge: (a, b) → {weight, first_date, last_date, docs}
    """
    def __init__(self):
        self.nodes: dict[str, dict] = {}
        self.edges: dict[tuple[str,str], dict] = {}

    def add_mention(self, entity: str, doctype: str, doc_date: Optional[date], doc_id: str):
        if entity not in self.nodes:
            self.nodes[entity] = {
                'doctype': doctype, 'count': 0,
                'first_date': None, 'last_date': None, 'docs': []
            }
        n = self.nodes[entity]
        n['count'] += 1
        n['docs'].append(doc_id)
        if doc_date:
            if n['first_date'] is None or doc_date < n['first_date']: n['first_date'] = doc_date
            if n['last_date']  is None or doc_date > n['last_date']:  n['last_date']  = doc_date

    def add_cooccurrence(self, a: str, b: str, doc_date: Optional[date], doc_id: str):
        key = (min(a, b), max(a, b))
        if key not in self.edges:
            self.edges[key] = {'weight': 0, 'first_date': None, 'last_date': None, 'docs': []}
        e = self.edges[key]
        e['weight'] += 1
        e['docs'].append(doc_id)
        if doc_date:
            if e['first_date'] is None or doc_date < e['first_date']: e['first_date'] = doc_date
            if e['last_date']  is None or doc_date > e['last_date']:  e['last_date']  = doc_date

    def ingest_document(self, entities: list[str], doctype: str,
                        doc_date: Optional[date], doc_id: str):
        for ent in entities:
            self.add_mention(ent, doctype, doc_date, doc_id)
        # Co-occurrence: all pairs (cap at 50 entities to avoid O(n^2) explosion)
        ents = entities[:50]
        for i in range(len(ents)):
            for j in range(i+1, len(ents)):
                self.add_cooccurrence(ents[i], ents[j], doc_date, doc_id)


# ── GraphML serialization ──────────────────────────────────────────────────────

def _d(val) -> str:
    if isinstance(val, date): return val.isoformat()
    return str(val) if val is not None else ''

def to_graphml(graph: EntityGraph) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<graphml xmlns="http://graphml.graphstruct.org/graphml">',
        '  <key id="d0" for="node" attr.name="count"      attr.type="int"/>',
        '  <key id="d1" for="node" attr.name="doctype"    attr.type="string"/>',
        '  <key id="d2" for="node" attr.name="first_date" attr.type="string"/>',
        '  <key id="d3" for="node" attr.name="last_date"  attr.type="string"/>',
        '  <key id="d4" for="node" attr.name="name"       attr.type="string"/>',
        '  <key id="e0" for="edge" attr.name="weight"    attr.type="int"/>',
        '  <key id="e1" for="edge" attr.name="first_date" attr.type="string"/>',
        '  <key id="e2" for="edge" attr.name="last_date"  attr.type="string"/>',
        '  <graph id="G" edgedefault="undirected">',
    ]
    node_ids = {}
    for i, (name, nd) in enumerate(graph.nodes.items()):
        nid = f'n{i}'; node_ids[name] = nid
        lines.append(f'    <node id="{nid}">')
        lines.append(f'      <data key="d4">{name.replace("<","&lt;").replace(">","&gt;")}</data>')
        lines.append(f'      <data key="d0">{nd["count"]}</data>')
        lines.append(f'      <data key="d1">{nd["doctype"]}</data>')
        lines.append(f'      <data key="d2">{_d(nd["first_date"])}</data>')
        lines.append(f'      <data key="d3">{_d(nd["last_date"])}</data>')
        lines.append('    </node>')
    for eidx, ((a, b), ed) in enumerate(graph.edges.items()):
        if a not in node_ids or b not in node_ids: continue
        lines.append(f'    <edge id="e{eidx}" source="{node_ids[a]}" target="{node_ids[b]}">')
        lines.append(f'      <data key="e0">{ed["weight"]}</data>')
        lines.append(f'      <data key="e1">{_d(ed["first_date"])}</data>')
        lines.append(f'      <data key="e2">{_d(ed["last_date"])}</data>')
        lines.append('    </edge>')
    lines += ['  </graph>', '</graphml>']
    return '\n'.join(lines)


def from_graphml(path: Path) -> dict:
    """
    Lightweight GraphML reader.  Returns {'nodes': {id: attrs}, 'edges': [(src, tgt, attrs)]}.
    """
    tree = ET.parse(str(path))
    root = tree.getroot()
    ns = ''
    if root.tag.startswith('{'):
        ns = root.tag.split('}')[0] + '}'

    key_map = {}
    for k in root.findall(f'{ns}key'):
        key_map[k.attrib.get('id')] = k.attrib.get('attr.name', k.attrib.get('id'))

    nodes = {}; edges = []
    for graph in root.findall(f'{ns}graph'):
        for node in graph.findall(f'{ns}node'):
            nid = node.attrib['id']
            attrs = {}
            for d in node.findall(f'{ns}data'):
                k = key_map.get(d.attrib.get('key', ''), d.attrib.get('key'))
                attrs[k] = d.text or ''
            nodes[nid] = attrs
        for edge in graph.findall(f'{ns}edge'):
            attrs = {}
            for d in edge.findall(f'{ns}data'):
                k = key_map.get(d.attrib.get('key', ''), d.attrib.get('key'))
                attrs[k] = d.text or ''
            edges.append((edge.attrib['source'], edge.attrib['target'], attrs))
    return {'nodes': nodes, 'edges': edges}


# ── temporal cluster extraction ────────────────────────────────────────────────

def temporal_clusters(graph: EntityGraph, window_days: int = 30) -> list[dict]:
    """
    Group entities into temporal clusters: entities that co-appear in documents
    filed within window_days of each other.  Uses union-find.
    """
    parent = {n: n for n in graph.nodes}

    def find(x):
        if x not in parent: parent[x] = x
        while parent[x] != x: parent[x] = parent[parent[x]]; x = parent[x]
        return x

    def union(x, y): parent[find(x)] = find(y)

    for (a, b), ed in graph.edges.items():
        da = graph.nodes.get(a, {}).get('first_date')
        db = graph.nodes.get(b, {}).get('first_date')
        if da and db and abs((da - db).days) <= window_days:
            union(a, b)
        elif ed['weight'] >= 3:  # strongly co-occurring → always cluster
            union(a, b)

    clusters: dict[str, list[str]] = collections.defaultdict(list)
    for n in graph.nodes:
        clusters[find(n)].append(n)

    result = []
    for root, members in sorted(clusters.items(), key=lambda x: -len(x[1])):
        if len(members) < 2: continue
        dates = [graph.nodes[m]['first_date'] for m in members if graph.nodes[m]['first_date']]
        result.append({
            'size': len(members),
            'members': sorted(members),
            'earliest': min(dates).isoformat() if dates else None,
            'latest':   max(dates).isoformat() if dates else None,
        })
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def cmd_build(args):
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = Path(args.manifest)
    entities_dir = Path(args.entities) if args.entities else None

    graph = EntityGraph()
    doc_count = 0

    with manifest.open() as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            doc_id  = rec.get('id') or rec.get('doc_id', f'doc_{doc_count}')
            doctype = rec.get('doctype') or rec.get('doc_type') or rec.get('type', 'investigative')
            text    = rec.get('text', '')
            # Load text from output/source paths when not embedded
            if not text:
                for path_key in ('output', 'source'):
                    p = rec.get(path_key)
                    if p and Path(p).exists():
                        try:
                            text = Path(p).read_text(errors='replace')
                        except OSError:
                            pass
                        if text:
                            break

            # Try loading entities from separate file
            ents = []
            if entities_dir:
                ent_path = entities_dir / f'{doc_id}.entities.json'
                if ent_path.exists():
                    ents = json.loads(ent_path.read_text()).get('entities', [])
            if not ents and text:
                ents = extract_entities(text)

            # Extract doc date
            text_for_date = rec.get('header', '') + ' ' + text[:5000]
            dates = extract_dates_from_text(text_for_date)
            doc_date = dates[0] if dates else None

            graph.ingest_document(ents, doctype, doc_date, doc_id)
            doc_count += 1

    print(f'Processed {doc_count} docs → '
          f'{len(graph.nodes)} entities, {len(graph.edges)} edges', file=sys.stderr)

    # GraphML
    gml_path = out_dir / 'graph.graphml'
    gml_path.write_text(to_graphml(graph))
    print(f'GraphML → {gml_path}', file=sys.stderr)

    # JSONL
    jsonl_path = out_dir / 'graph.jsonl'
    with jsonl_path.open('w') as f:
        for name, nd in graph.nodes.items():
            f.write(json.dumps({'type': 'node', 'id': name, **{
                k: v.isoformat() if isinstance(v, date) else v
                for k, v in nd.items() if k != 'docs'
            }}) + '\n')
        for (a, b), ed in graph.edges.items():
            f.write(json.dumps({'type': 'edge', 'src': a, 'dst': b, **{
                k: v.isoformat() if isinstance(v, date) else v
                for k, v in ed.items() if k != 'docs'
            }}) + '\n')
    print(f'JSONL   → {jsonl_path}', file=sys.stderr)

    # Clusters
    clusters = temporal_clusters(graph, window_days=int(args.window_days))
    cluster_path = out_dir / 'cluster_report.json'
    cluster_path.write_text(json.dumps({
        'n_clusters': len(clusters),
        'window_days': args.window_days,
        'clusters': clusters[:100],  # top-100
    }, indent=2))
    print(f'Clusters → {cluster_path}  ({len(clusters)} clusters)', file=sys.stderr)


def cmd_diff(args):
    before = from_graphml(Path(args.before))
    after  = from_graphml(Path(args.after))

    before_nodes = set(n.get('d1', nid) for nid, n in before['nodes'].items())
    after_nodes  = set(n.get('d1', nid) for nid, n in after['nodes'].items())

    added_nodes   = sorted(after_nodes - before_nodes)
    removed_nodes = sorted(before_nodes - after_nodes)

    def edge_key(src, tgt, nodes):
        name = lambda nid: nodes.get(nid, {}).get('d1', nid)
        a, b = sorted([name(src), name(tgt)])
        return (a, b)

    before_edges = {edge_key(s, t, before['nodes']): a.get('weight', '0')
                    for s, t, a in before['edges']}
    after_edges  = {edge_key(s, t, after['nodes']): a.get('weight', '0')
                    for s, t, a in after['edges']}

    changed = []
    for k in set(before_edges) & set(after_edges):
        bw = int(before_edges[k] or 0)
        aw = int(after_edges[k]  or 0)
        if bw != aw:
            changed.append({'edge': list(k), 'before_weight': bw, 'after_weight': aw})

    new_edges = [
        {'edge': list(k), 'weight': int(v or 0)}
        for k, v in after_edges.items() if k not in before_edges
    ]

    print(json.dumps({
        'added_nodes':   added_nodes,
        'removed_nodes': removed_nodes,
        'changed_edges': changed,
        'new_edges':     new_edges,
        'summary': {
            'nodes_added': len(added_nodes),
            'nodes_removed': len(removed_nodes),
            'edges_changed': len(changed),
            'edges_new': len(new_edges),
        }
    }, indent=2))


def cmd_clusters(args):
    # Load graph from GraphML, build lightweight EntityGraph
    g_data = from_graphml(Path(args.graph))
    graph = EntityGraph()
    # Reconstruct just enough for clustering
    for nid, attrs in g_data['nodes'].items():
        name = attrs.get('d1', nid)
        d = _parse_date(attrs.get('first_date', '') or '')
        graph.nodes[name] = {
            'doctype': attrs.get('doctype', ''),
            'count': int(attrs.get('count', 0) or 0),
            'first_date': d, 'last_date': None, 'docs': []
        }
    node_name = {nid: attrs.get('name', attrs.get('d4', nid)) for nid, attrs in g_data['nodes'].items()}
    for src, tgt, attrs in g_data['edges']:
        a, b = node_name.get(src, src), node_name.get(tgt, tgt)
        key = (min(a,b), max(a,b))
        d = _parse_date(attrs.get('first_date', '') or '')
        graph.edges[key] = {
            'weight': int(attrs.get('weight', 1) or 1),
            'first_date': d, 'last_date': None, 'docs': []
        }
    clusters = temporal_clusters(graph, window_days=int(args.window_days))
    print(json.dumps({'n_clusters': len(clusters), 'clusters': clusters}, indent=2))


def main():
    ap = argparse.ArgumentParser(description='Temporal entity DAG + graph diff')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p_b = sub.add_parser('build')
    p_b.add_argument('--manifest',    required=True)
    p_b.add_argument('--entities',    default=None, help='Dir with *.entities.json files')
    p_b.add_argument('--out',         required=True)
    p_b.add_argument('--window-days', default=30, type=int)

    p_d = sub.add_parser('diff')
    p_d.add_argument('--before', required=True)
    p_d.add_argument('--after',  required=True)

    p_c = sub.add_parser('clusters')
    p_c.add_argument('--graph',       required=True)
    p_c.add_argument('--window-days', default=30, type=int)

    args = ap.parse_args()
    {'build': cmd_build, 'diff': cmd_diff, 'clusters': cmd_clusters}[args.cmd](args)


if __name__ == '__main__':
    main()
