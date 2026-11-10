#!/usr/bin/env python3
"""
Bridge: BonfyreGraph output → hypothesis_discovery input

Converts graph.json (nodes + edges) into claims format for Phase 17.
This is the proper symbolic front-end, replacing ad-hoc text extraction.
"""

import json
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Any
import argparse


def load_graph(graph_path: str) -> Dict[str, Any]:
    """Load graph.json from BonfyreGraph or compatible format."""
    with open(graph_path) as f:
        return json.load(f)


def graph_to_claims(graph: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert graph structure to claims.
    
    Graph format (expected):
    {
      "nodes": [
        {"id": "entity_1", "label": "Alice", "type": "person", ...},
        {"id": "entity_2", "label": "Bob", "type": "person", ...}
      ],
      "edges": [
        {"source": "entity_1", "target": "entity_2", "relation": "met", "doc_id": "doc1", ...}
      ]
    }
    
    Claim format (output):
    {
      "doc_id": "doc1",
      "subject": "Alice",
      "predicate": "met",
      "object": "Bob",
      "text": "Alice met Bob",
      "claim_strength": 0.8,
      ...
    }
    """
    claims = []
    
    # Build node lookup
    nodes = {n["id"]: n for n in graph.get("nodes", [])}
    
    # Convert edges to claims
    for edge in graph.get("edges", []):
        source_id = edge.get("source")
        target_id = edge.get("target")
        
        if source_id not in nodes or target_id not in nodes:
            continue
        
        source_label = nodes[source_id].get("label", source_id)
        target_label = nodes[target_id].get("label", target_id)
        relation = edge.get("relation", "related_to")
        
        claim = {
            "doc_id": edge.get("doc_id", "unknown"),
            "subject": source_label,
            "predicate": relation,
            "object": target_label,
            "text": edge.get("text", f"{source_label} {relation} {target_label}"),
            "claim_strength": edge.get("weight", 0.7),
            "orthogonal_pressure": None,
            "temporal_pressure": None
        }
        
        claims.append(claim)
    
    return claims


def store_claims_in_memory_db(claims: List[Dict[str, Any]], memory_dir: str):
    """Store claims in memory.db for hypothesis_discovery.py"""
    memory_path = Path(memory_dir)
    memory_path.mkdir(parents=True, exist_ok=True)
    
    db_path = memory_path / "memory.db"
    
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Create claims table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT,
            subject TEXT,
            predicate TEXT,
            object TEXT,
            text TEXT,
            claim_strength REAL,
            orthogonal_pressure REAL,
            temporal_pressure REAL,
            created_at TEXT
        )
    """)
    
    # Insert claims
    for claim in claims:
        cur.execute("""
            INSERT INTO claims 
            (doc_id, subject, predicate, object, text, claim_strength, 
             orthogonal_pressure, temporal_pressure, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            claim["doc_id"],
            claim["subject"],
            claim["predicate"],
            claim["object"],
            claim["text"],
            claim["claim_strength"],
            claim["orthogonal_pressure"],
            claim["temporal_pressure"],
            time.strftime("%Y-%m-%d %H:%M:%S")
        ))
    
    conn.commit()
    
    # Stats
    n_claims = cur.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    n_subjects = cur.execute("SELECT COUNT(DISTINCT subject) FROM claims").fetchone()[0]
    
    conn.close()
    
    print(f"✓ Stored {n_claims} claims ({n_subjects} unique subjects) in {db_path}")


def write_claims_json(claims: List[Dict[str, Any]], output_path: str):
    """Write claims to JSON (alternative output format)"""
    with open(output_path, 'w') as f:
        json.dump({
            "claims": claims,
            "n_claims": len(claims),
            "n_subjects": len(set(c["subject"] for c in claims))
        }, f, indent=2)
    
    print(f"✓ Wrote {len(claims)} claims to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert BonfyreGraph output to claims for hypothesis discovery"
    )
    parser.add_argument("--graph", required=True,
                        help="Path to graph.json from BonfyreGraph")
    parser.add_argument("--memory-dir", 
                        help="Memory directory for claims (creates memory.db)")
    parser.add_argument("--json", 
                        help="Output path for claims.json (alternative format)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing claims first")
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("GRAPH → CLAIMS BRIDGE")
    print("=" * 70)
    print()
    
    # Load graph
    print(f"Loading graph: {args.graph}")
    graph = load_graph(args.graph)
    
    n_nodes = len(graph.get("nodes", []))
    n_edges = len(graph.get("edges", []))
    print(f"  → {n_nodes} nodes, {n_edges} edges")
    print()
    
    # Convert to claims
    print("Converting graph to claims...")
    claims = graph_to_claims(graph)
    print(f"  → {len(claims)} claims generated")
    print()
    
    # Clear existing if requested
    if args.clear and args.memory_dir:
        db_path = Path(args.memory_dir) / "memory.db"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            conn.execute("DROP TABLE IF EXISTS claims")
            conn.commit()
            conn.close()
            print(f"✓ Cleared existing claims from {db_path}\n")
    
    # Store in memory.db
    if args.memory_dir:
        print("Storing in memory.db...")
        store_claims_in_memory_db(claims, args.memory_dir)
        print()
    
    # Store in JSON
    if args.json:
        print("Writing claims.json...")
        write_claims_json(claims, args.json)
        print()
    
    print("=" * 70)
    print("DONE - Claims ready for hypothesis discovery")
    print("=" * 70)
    print()
    
    if args.memory_dir:
        print("Next step:")
        print(f"  python3 scripts/hypothesis_discovery.py \\")
        print(f"    --memory-dir {args.memory_dir} \\")
        print(f"    --max-hypotheses 5 \\")
        print(f"    --output report.json")
