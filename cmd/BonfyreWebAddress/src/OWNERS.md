# WebAddress native source owners

The HTTP endpoint, catalogue projection, public cache, account authority, and
publication must not grow into one route function. This table is the edit path,
not a claim that the whole historical main translation unit is already retired.

| Change | Source owner | Test/witness |
|---|---|---|
| Model catalogue index, model graph, representation passports | `model_catalogue_index.c/.h` | `make model-catalogue-index-witness` |
| Model page route, account action and observed prices | `main.c` (yet to extract) | `model_commons_test.sh`, browser readback |
| Public projection caching | `public_projection_cache.c/.h` | `make public-cache-contention-witness` |
| Source graph hierarchy | `source_hierarchy_projection.c/.h` | `make source-hierarchy-witness` |
| Community projection | `community_projection.c/.h` | `community_journey.test.mjs` |
| People projection | `people_projection.c/.h` | `people_projection_test.c` |
| Geographic territory composition | `territory_home.c/.h` | `territory-home-witness` |
| Public site delivery | `site_product.c`, `site_presentation.c` | public release four-apex readback |
| Native HTTP readiness | `http_readiness.c/.h` | bounded readiness regression |

Prefer a new independent `.c/.h` pair with its own fixture and an explicit
`WEB_SOURCES` / Makefile witness, not another included implementation header.
Model catalogue functions return bounded HTML facts from read-only SQLite and
**never** infer model runtime residency, provider price, account access or
permission to execute. Presentation does not grant authority.

The normal and compact builds share `WEB_SOURCES`. A local witness passing is
not a full release: the native ratchet, public/browser and signed-in account
tests must all pass before promoting. Do not raise size budgets to hide
duplication or delete retained donor evidence on a file-count inference.
