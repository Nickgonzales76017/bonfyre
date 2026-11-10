# Bonfÿre Compact Semantic AST / Build / Ownership Map v3.2.1

Generated: `2026-09-29T07:51:58.960804+00:00`

## Why this exists

Row cardinality and filename co-reference are downstream symptoms. This map asks which declaration/compiler/symbol actually owns each generated artifact and durable table effect before proposing convergence.

## Census

- Source/build files: **19,864**
- AST/declaration symbols: **888,252**
- Graph edges: **2,131,042**
- SQL accesses: **10,330** across **1,497** tables
- Generated artifacts mapped: **1,019**
- Rebuildable generated artifacts: **26**
- Generated artifacts with no discovered generator/reader: **378**
- Archived peers: **466**
- Strong archive/native structural candidates: **49**

## Highest convergence candidates

### P0 · TABLE_OWNER · `BOM`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `CSV`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `Payment`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `Settings`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_compaction_manifests`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_content_types`
- Evidence: 1 structural writer / 14 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_merkle_tree`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_meta_families`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_meta_members`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_transfer_templates`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `_type_constraints`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `address_bit`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `an`
- Evidence: 1 structural writer / 29 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `and`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `another`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `assignments`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `bitnet_rank_run`
- Evidence: 1 structural writer / 6 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `call`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `candidate_advertisement_mask`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `capital_actions`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `catalog_fts`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `channel_publication`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `closure_dependency`
- Evidence: 1 structural writer / 10 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `command_laws`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `community_membership`
- Evidence: 1 structural writer / 8 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `continuous_assignments`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `costs`
- Evidence: 1 structural writer / 9 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `creator_channel`
- Evidence: 1 structural writer / 7 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `credits`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `database`
- Evidence: 1 structural writer / 8 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `derivations`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `economic_measures`
- Evidence: 1 structural writer / 6 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `embedding_projection`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `feedback`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `financial_positions`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `foreign_twin_observations`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `funding_applications`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `funding_work_package`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `harvest`
- Evidence: 1 structural writer / 6 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `hypotheses`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `index`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `kin`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `layer_index`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `layers`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `lead`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `live`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `machine_reader`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `material_lease`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `matrix_addresses`
- Evidence: 1 structural writer / 14 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `matrix_domains`
- Evidence: 1 structural writer / 14 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `matrix_subject_relations`
- Evidence: 1 structural writer / 10 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `matrix_subjects`
- Evidence: 1 structural writer / 13 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `media_library`
- Evidence: 1 structural writer / 9 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `media_library_membership`
- Evidence: 1 structural writer / 7 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `mesh_export_state`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `model_representation_passport`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `neighbour`
- Evidence: 1 structural writer / 5 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `obligation_work`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `observed_price`
- Evidence: 1 structural writer / 4 readers
- Next: map writer AST to generator/law before row-factorization

### P0 · TABLE_OWNER · `orphan`
- Evidence: 1 structural writer / 3 readers
- Next: map writer AST to generator/law before row-factorization

## Interpretation rule

An AST/build edge is stronger than a regex/name coincidence but still not semantic parity. Mutation still requires an exact behavior/data round trip. The purpose of this graph is to reduce hundreds of apparent cleanup targets into the few generators/owners that actually produce them.
