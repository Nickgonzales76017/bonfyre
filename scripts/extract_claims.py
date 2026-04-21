#!/usr/bin/env python3
"""
Simple claim extractor - the dumbest thing that works.

GOAL: Get documents → claims → memory.db so Phase 17 can run.

NOT trying to be perfect. Just trying to get SIGNAL FLOWING.
"""

import glob
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Any


def extract_claims_from_text(text: str, doc_id: str) -> List[Dict[str, Any]]:
    """
    Extract claims from text.
    
    INTENTIONALLY SIMPLE. Just needs to create claims that Phase 17 can analyze.
    """
    claims = []
    
    # Split into sentences (dumb but works)
    sentences = text.replace("!", ".").replace("?", ".").split(".")
    
    for i, sentence in enumerate(sentences):
        s = sentence.strip()
        if not s or len(s) < 10:
            continue
        
        words = s.split()
        if len(words) < 3:
            continue
        
        # Extract entities (just grab capitalized words)
        entities = [w for w in words if w[0].isupper() and len(w) > 1]
        
        if len(entities) >= 2:
            # Entity-entity relationship
            subject = entities[0]
            obj = entities[-1]
            
            # Try to find predicate (middle words)
            predicate_words = []
            in_middle = False
            for w in words:
                if w == subject:
                    in_middle = True
                    continue
                if w == obj:
                    break
                if in_middle and len(w) > 2:
                    predicate_words.append(w.lower())
            
            predicate = "_".join(predicate_words[:3]) if predicate_words else "related_to"
            
            claims.append({
                "doc_id": doc_id,
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "text": s,
                "claim_strength": 0.7,  # Default
                "orthogonal_pressure": None,
                "temporal_pressure": None
            })
        
        elif len(entities) == 1:
            # Single entity claim
            subject = entities[0]
            
            claims.append({
                "doc_id": doc_id,
                "subject": subject,
                "predicate": "mentioned_in",
                "object": doc_id,
                "text": s,
                "claim_strength": 0.6,
                "orthogonal_pressure": None,
                "temporal_pressure": None
            })
    
    return claims


def load_corpus(path_pattern: str) -> List[Dict[str, Any]]:
    """Load corpus and extract claims."""
    all_claims = []
    
    files = glob.glob(path_pattern)
    
    print(f"Found {len(files)} files")
    
    for filepath in files:
        doc_id = Path(filepath).stem
        
        try:
            with open(filepath) as f:
                text = f.read()
            
            claims = extract_claims_from_text(text, doc_id)
            all_claims.extend(claims)
            
            print(f"  {doc_id}: {len(claims)} claims")
        
        except Exception as e:
            print(f"  ERROR reading {filepath}: {e}")
    
    return all_claims


def store_claims(claims: List[Dict[str, Any]], memory_dir: str):
    """Store claims in memory.db"""
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
    
    print(f"\nStored {n_claims} claims ({n_subjects} unique subjects) in {db_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract claims from corpus (Phase 12 simplified)"
    )
    parser.add_argument("--corpus", required=True,
                        help="Glob pattern for corpus files (e.g., 'data/*.txt')")
    parser.add_argument("--memory-dir", default="/tmp/bonfyre-memory",
                        help="Memory directory for claims")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing claims first")
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("SIMPLE CLAIM EXTRACTION")
    print("=" * 70)
    print()
    
    # Clear existing claims if requested
    if args.clear:
        db_path = Path(args.memory_dir) / "memory.db"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            conn.execute("DROP TABLE IF EXISTS claims")
            conn.commit()
            conn.close()
            print(f"✓ Cleared existing claims from {db_path}\n")
    
    # Load corpus
    print(f"Loading corpus: {args.corpus}")
    claims = load_corpus(args.corpus)
    
    if not claims:
        print("\n⚠ No claims extracted. Check your corpus files.")
        exit(1)
    
    print(f"\nExtracted {len(claims)} total claims")
    
    # Store claims
    print(f"\nStoring claims in {args.memory_dir}...")
    store_claims(claims, args.memory_dir)
    
    print("\n" + "=" * 70)
    print("DONE - Claims ready for Phase 17")
    print("=" * 70)
    print()
    print("Next step:")
    print(f"  python3 scripts/hypothesis_discovery.py \\")
    print(f"    --memory-dir {args.memory_dir} \\")
    print(f"    --max-hypotheses 5 \\")
    print(f"    --output report.json")
    print()
