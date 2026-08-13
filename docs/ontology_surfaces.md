# Ontology Surfaces

Bonfyre's operating truth should not live only in static C tables.

When a compatibility table, contract table, or routing table becomes important
enough that operators keep digging through source to manage it, that table should
also have a first-class CLI inspection surface.

Current native ontology surfaces:

- `bonfyre layer ontology`
  - exports `BF_LAYER_FAMILY_RELATIONS`
  - supports `--family T_FAMILY`
- `bonfyre discipl contracts list`
  - exports the DisCIPL contract table
  - supports `--family T_FAMILY`
- `bonfyre self ontology aliases`
  - exports command alias and install-target mapping
- `bonfyre self ontology registry-projections`
  - exports catalog projection/rebuild rules
- `bonfyre self ontology repair-plans`
  - exports self-optimization repair plan kinds
- `bonfyre precision ontology`
  - exports precision-routing heuristics and goal policies
- `bonfyre capabilities ontology`
  - exports capability tagging rules used to infer capability links

Examples:

```bash
bonfyre layer ontology
bonfyre layer ontology --family T_MOE_ROUTER

bonfyre discipl contracts list
bonfyre discipl contracts list --family T_GEOSPATIAL_EMBED

bonfyre self ontology aliases
bonfyre self ontology registry-projections
bonfyre self ontology repair-plans

bonfyre precision ontology

bonfyre capabilities ontology
bonfyre capabilities ontology --filter transcribe
```

Why this matters:

- avoids code archaeology for core operating truth
- makes compatibility and bridge semantics queryable
- reduces latency when editing family or contract logic
- creates a reusable pattern for other hidden tables

This pattern should continue anywhere Bonfyre still hides operating truth in
static tables or compiled heuristics.
