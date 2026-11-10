---
name: estate
description: Query the receipted DFW parcel estate — 6.5M observations, hot SQLite shadowing cold Parquet. Use when asked about parcels, owners, land or assessed value, permits, Dallas or Tarrant records, or any question the observed public record could answer.
allowed-tools: Bash, Read
---

# The estate

6.5M receipted observations of the DFW public record. Historical state is
columnar Parquet; recent rows live in SQLite and **shadow** the cold history, so
a query must read the composed view rather than either store alone. Reading only
Parquet silently returns stale rows; reading only SQLite returns almost nothing.

## Ask it

Through MCP, which composes the view for you:

```
estate_query  { "sql": "SELECT ... FROM observation ..." }
```

Tables in the view: `observation`, `page`, `parcel_flat`. SELECT and WITH only —
the tool refuses anything else, and that refusal is deliberate rather than a
limitation to work around.

Locally, the same composition is `_fleming_sql()` in the gateway: attach
`estate-lite.db` as `hot`, scan `estate.parquet/observation.parquet`, exclude
`src_id`s that hot already carries, union.

## What the rows are

`observed` is JSON per source. The Tarrant roll (`src:tad-parcel-view`) carries
`Situs_Addr`, `Owner_Name`, `City`, `Land_Value`, `Total_Valu`, `Account_Nu`,
`Year_Built`, `Living_Are`, `Land_SqFt`. Cast with `TRY_CAST` — these are strings
in the source and a hard cast fails the whole scan on one bad row.

Dallas sources are separate and larger in count:

```
src:dallas-tax-account-points              576,829
src:dallas-tax-parcels-hosted              500,142
src:dallas-pbw-...-permit-location         181,036
src:dallas-basemap-dallasarearoads-streets 161,099
```

## What it cannot tell you

Every row is an **observation of a public record**, not a fact about the world
and not a licence to act. It records what a source published, with a receipt for
when and from where. It is not title, not an appraisal, not consent to contact
an owner, and not evidence that the record was correct. A parcel's assessed
value is what the assessor published, which is a different thing from what it
would sell for.

Numbers arrive as DuckDB doubles. Group them before showing them to a person —
`232174.0` reads as machine output, `$232,174` reads as money.
