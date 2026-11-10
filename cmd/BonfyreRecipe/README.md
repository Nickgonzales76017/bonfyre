# Recipe registry and source-bound preparation material

The existing pipeline-recipe registry remains unchanged. The `corpus` operations
add a derived, exact source-record representation and explicit preparation-family
calculations. They do not create a second business-state store or a browser planner.

`cmd/BonfyreCompress/src/record_factor.c` owns exact physical factoring. Equal root
field fragments are stored once, with their original spelling and byte identity.
Native Zstandard uses one bounded, retained dictionary when enough source material
exists. SQLite supplies indexed lookup and text search. Compiler-only indexes are
removed before publication. Record and dictionary corruption is refused.

`src/recipe_material.c` is linked by both this CLI and BonfyreWebAddress. The
existing mixed `/flow` feed opens `/recipes?id=...`; the existing subject-page
layout displays the same source, quantities and variant instructions. A normal
HTML form changes the number of sets through the native owner. No client code
reimplements the arithmetic.

## Operations

```sh
make material-witness
./build/bonfyre-recipe corpus factor SOURCE.jsonl NEW-OUTPUT.db
./build/bonfyre-recipe corpus verify NEW-OUTPUT.db SOURCE.jsonl
./build/bonfyre-recipe corpus export NEW-OUTPUT.db
./build/bonfyre-recipe corpus list NEW-OUTPUT.db prep:oat-bite-variations 2
```

The material path is `BONFYRE_RECIPE_MATERIAL`, or
`~/.bonfyre/estate-local/recipe-material.db`. This is a rebuildable read model;
factoring refuses an existing destination and never removes the original source.
The native browser witness is `cmd/BonfyreWebAddress/tests/recipe_material_web.test.mjs`.

## Meaning and limits

The authored example retains ten distinct variants over one explicitly declared
base. Quantities are exact rational numbers with units; a drop is not converted
to milliliters, and a cup is not silently converted to mass. Capacity determines
how many base batches are required, not how long to heat or chill them. A shared
text fragment is not proof of shared cooking behavior. The example is not food-
tested and does not establish storage life or container suitability.

Current price observation, compatible package units, fulfillment fees, travel,
helper acceptance and delivery execution still belong to FoodCombinationGraph
