#!/usr/bin/env python3
"""
Symbolic entity extraction (stub for BonfyreEntity)

Extracts entities from text with proper canonicalization.
This is a minimal implementation - will be replaced by BonfyreEntity binary.
"""

import json
import re
from pathlib import Path
from collections import Counter
from typing import List, Dict, Any
import argparse


# Common function words that should be filtered even if capitalized
COMMON_WORDS = {
    "The", "He", "She", "It", "They", "There", "This", "That", "These", "Those",
    "In", "On", "At", "To", "For", "Of", "With", "From", "By", "As",
    "And", "Or", "But", "If", "When", "Where", "Why", "How", "What", "Which",
    "One", "Two", "Three", "Some", "Many", "All", "Most", "Few", "Each", "Every",
    "Was", "Were", "Been", "Being", "Have", "Has", "Had", "Do", "Does", "Did",
    "Can", "Could", "Will", "Would", "Should", "May", "Might", "Must",
    "Later", "Then", "Now", "Here", "Watch", "Look", "See", "Come", "Go",
    "After", "Before", "During", "Since", "Until", "While",
    "E-mail", "Email",  # Email noise
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
    "American", "British", "French", "German", "Russian", "Chinese", "Japanese",  # Generic demonyms
    "According", "His", "Her", "Their", "Our", "Its", "My", "Your"
}


def structural_score(word: str, corpus_stats: Counter) -> int:
    """
    Score token by structural signal strength (same as extract_claims.py).
    
    This approximates fragment filtering at text level.
    """
    # HARD FILTER: common function words
    if word in COMMON_WORDS:
        return 0
    
    score = 0
    
    # Capitalization (proper noun signal)
    if word and word[0].isupper():
        score += 1
    
    # Length filter (avoid short words)
    if len(word) > 3:
        score += 1
    
    # Corpus frequency (repeated = meaningful)
    if corpus_stats.get(word, 0) > 2:
        score += 1
    
    return score


def extract_entities_from_text(text: str, doc_id: str, corpus_stats: Counter = None) -> List[Dict[str, Any]]:
    """
    Extract entities from text with structural filtering.
    
    Returns list of entity mentions with context.
    """
    if corpus_stats is None:
        corpus_stats = Counter()
    
    entities = []
    
    # Split into sentences
    sentences = re.split(r'[.!?]+', text)
    
    for sent_idx, sentence in enumerate(sentences):
        s = sentence.strip()
        if not s:
            continue
        
        words = s.split()
        
        # Extract capitalized sequences (proper nouns)
        i = 0
        while i < len(words):
            word = words[i]
            
            # Skip if not capitalized or too short
            if not word or not word[0].isupper() or len(word) < 2:
                i += 1
                continue
            
            # STRUCTURAL FILTER: Check if this entity has sufficient signal
            if structural_score(word, corpus_stats) < 2:
                i += 1
                continue
            
            # Collect multi-word entities
            entity_words = [word]
            j = i + 1
            while j < len(words) and words[j] and words[j][0].isupper():
                # Only add if not a common word
                if structural_score(words[j], corpus_stats) >= 2:
                    entity_words.append(words[j])
                    j += 1
                else:
                    break
            
            entity_text = " ".join(entity_words)
            
            entities.append({
                "text": entity_text,
                "doc_id": doc_id,
                "sentence_idx": sent_idx,
                "char_offset": text.find(entity_text),
                "context": s[:100]
            })
            
            i = j if j > i + 1 else i + 1
    
    return entities


def canonicalize_entities(entities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Canonicalize entity variants (stub for BonfyreCanon).
    
    Groups:
    - Plural/singular forms
    - Abbreviations
    - Case variants
    
    Returns canonical mapping.
    """
    # Count occurrences
    entity_counts = Counter(e["text"] for e in entities)
    
    # Build canonical groups
    canonical_groups = {}
    canonical_to_variants = {}
    
    for entity_text in entity_counts:
        # Simple canonicalization rules
        canonical = entity_text
        
        # Use most common form as canonical
        # Check for variants (simple heuristics)
        variants = [entity_text]
        
        # Check for plural
        if entity_text.endswith('s') and entity_text[:-1] in entity_counts:
            singular = entity_text[:-1]
            if entity_counts[singular] >= entity_counts[entity_text]:
                canonical = singular
            variants.append(singular)
        
        # Check if this is singular of a plural
        plural = entity_text + 's'
        if plural in entity_counts:
            if entity_counts[plural] > entity_counts[entity_text]:
                canonical = plural
            variants.append(plural)
        
        # Group all variants under canonical form
        if canonical not in canonical_to_variants:
            canonical_to_variants[canonical] = set()
        
        for v in variants:
            if v in entity_counts:
                canonical_to_variants[canonical].add(v)
                canonical_groups[v] = canonical
    
    return {
        "canonical_groups": canonical_groups,
        "canonical_to_variants": {
            k: list(v) for k, v in canonical_to_variants.items()
        },
        "entity_counts": dict(entity_counts)
    }


def build_graph(entities: List[Dict[str, Any]], canon: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build entity graph (stub for BonfyreGraph).
    
    Creates nodes and edges from entity co-occurrences.
    """
    canonical_groups = canon["canonical_groups"]
    
    # Group entities by document
    doc_entities = {}
    for entity in entities:
        doc_id = entity["doc_id"]
        if doc_id not in doc_entities:
            doc_entities[doc_id] = []
        doc_entities[doc_id].append(entity)
    
    # Build nodes (unique canonical entities)
    canonical_entities = set(canonical_groups.values())
    nodes = []
    for i, entity in enumerate(sorted(canonical_entities)):
        nodes.append({
            "id": f"entity_{i}",
            "label": entity,
            "type": "entity",
            "count": canon["entity_counts"].get(entity, 0),
            "variants": canon["canonical_to_variants"].get(entity, [entity])
        })
    
    # Build node lookup
    label_to_id = {n["label"]: n["id"] for n in nodes}
    
    # Build edges (co-occurrences in same document)
    edges = []
    edge_set = set()  # Deduplicate
    
    for doc_id, doc_ents in doc_entities.items():
        # Canonicalize
        canonical_ents = [canonical_groups.get(e["text"], e["text"]) for e in doc_ents]
        unique_ents = list(set(canonical_ents))
        
        # Create edges for all pairs
        for i in range(len(unique_ents)):
            for j in range(i + 1, len(unique_ents)):
                e1, e2 = unique_ents[i], unique_ents[j]
                
                # Canonical ordering for deduplication
                source, target = (e1, e2) if e1 < e2 else (e2, e1)
                edge_key = (source, target, doc_id)
                
                if edge_key not in edge_set:
                    edge_set.add(edge_key)
                    
                    edges.append({
                        "source": label_to_id[source],
                        "target": label_to_id[target],
                        "relation": "co-occurs_with",
                        "doc_id": doc_id,
                        "weight": 1.0,
                        "text": f"{source} co-occurs with {target} in {doc_id}"
                    })
    
    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": {
            "n_nodes": len(nodes),
            "n_edges": len(edges),
            "n_documents": len(doc_entities)
        }
    }


def process_corpus(corpus_pattern: str, output_dir: str):
    """Process corpus through entity/canon/graph pipeline with structural filtering."""
    import glob
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Step 1a: Build corpus statistics for structural filtering
    print("=" * 70)
    print("STEP 1a: Building Corpus Statistics")
    print("=" * 70)
    print()
    
    files = glob.glob(corpus_pattern)
    print(f"Processing {len(files)} files for statistics...")
    
    corpus_stats = Counter()
    for filepath in files:
        try:
            with open(filepath) as f:
                text = f.read()
            
            # Count all capitalized words
            sentences = re.split(r'[.!?]+', text)
            for sentence in sentences:
                words = sentence.strip().split()
                for word in words:
                    if word and len(word) >= 2 and word[0].isupper():
                        corpus_stats[word] += 1
        except Exception as e:
            continue
    
    print(f"✓ Analyzed {len(corpus_stats)} unique capitalized words\n")
    
    # Step 1b: Extract entities with filtering
    print("=" * 70)
    print("STEP 1b: Entity Extraction (stub for BonfyreEntity)")
    print("=" * 70)
    print()
    
    print(f"Extracting entities with structural filtering...")
    
    all_entities = []
    for filepath in files:
        doc_id = Path(filepath).stem
        
        try:
            with open(filepath) as f:
                text = f.read()
            
            entities = extract_entities_from_text(text, doc_id, corpus_stats)
            all_entities.extend(entities)
            
            if len(entities) > 0:
                print(f"  {doc_id}: {len(entities)} entities")
        
        except Exception as e:
            print(f"  ERROR {doc_id}: {e}")
    
    print(f"\n✓ Extracted {len(all_entities)} entity mentions (after filtering)")
    
    # Show top entities
    entity_counts = Counter(e["text"] for e in all_entities)
    print("\nTop 10 entities after structural filtering:")
    for entity, count in entity_counts.most_common(10):
        print(f"  {entity}: {count}")
    
    # Save entities
    entities_path = output_path / "entities.json"
    with open(entities_path, 'w') as f:
        json.dump({"entities": all_entities}, f, indent=2)
    print(f"\n✓ Saved to {entities_path}\n")
    
    # Step 2: Canonicalize
    print("=" * 70)
    print("STEP 2: Canonicalization (stub for BonfyreCanon)")
    print("=" * 70)
    print()
    
    canon = canonicalize_entities(all_entities)
    n_canonical = len(canon["canonical_to_variants"])
    n_total = len(canon["entity_counts"])
    print(f"✓ Canonicalized {n_total} variants → {n_canonical} canonical entities")
    
    # Save canon
    canon_path = output_path / "canon.json"
    with open(canon_path, 'w') as f:
        json.dump(canon, f, indent=2)
    print(f"✓ Saved to {canon_path}\n")
    
    # Step 3: Build graph
    print("=" * 70)
    print("STEP 3: Graph Construction (stub for BonfyreGraph)")
    print("=" * 70)
    print()
    
    graph = build_graph(all_entities, canon)
    print(f"✓ Built graph: {graph['metadata']['n_nodes']} nodes, {graph['metadata']['n_edges']} edges")
    
    # Save graph
    graph_path = output_path / "graph.json"
    with open(graph_path, 'w') as f:
        json.dump(graph, f, indent=2)
    print(f"✓ Saved to {graph_path}\n")
    
    print("=" * 70)
    print("SYMBOLIC PROCESSING COMPLETE")
    print("=" * 70)
    print(f"\nOutput directory: {output_dir}")
    print(f"  entities.json  - {len(all_entities)} entity mentions")
    print(f"  canon.json     - {n_canonical} canonical entities")
    print(f"  graph.json     - {graph['metadata']['n_nodes']} nodes, {graph['metadata']['n_edges']} edges")
    print()
    print("Next step:")
    print(f"  python3 scripts/graph_to_claims.py --graph {graph_path} --memory-dir /tmp/memory")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Symbolic entity/canon/graph extraction (stub pipeline)"
    )
    parser.add_argument("--corpus", required=True,
                        help="Glob pattern for corpus files")
    parser.add_argument("--output", required=True,
                        help="Output directory for symbolic processing")
    
    args = parser.parse_args()
    
    process_corpus(args.corpus, args.output)
