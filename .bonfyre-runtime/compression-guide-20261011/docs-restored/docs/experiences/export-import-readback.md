# Move a website and verify the records

Take a customer-owned Bonfÿre site into familiar content formats, import the result into new local sites, and compare native database records in a separate destination. Every operation runs through the existing Site and Bridge owners.

This example uses the local Fernhollow Pottery site, its introduction and its own presentation record.

## Start from a published customer site

Fernhollow has a dedicated host, community content, a classes page and a bookable pottery workshop. Its published page and its underlying records are different things: one is the experience a visitor sees; the other is what a downstream system can import.

```sh Terminal
export SITE_HOME="$HOME/Developer/aurekai-profile-drafts/white-label-native-20261011"

bonfyre site show fernhollow --json --home "$SITE_HOME" --port 8917
bonfyre site check fernhollow --json --home "$SITE_HOME" --port 8917
```

## Export once, deliver through three formats

```sh Terminal
bonfyre site export fernhollow --format html \
  --out "$SITE_HOME/fernhollow.html" --home "$SITE_HOME" --port 8917

bonfyre site export fernhollow --format wordpress \
  --out "$SITE_HOME/fernhollow.xml" --home "$SITE_HOME" --port 8917

bonfyre site export fernhollow --format frappe \
  --out "$SITE_HOME/fernhollow-frappe.json" --home "$SITE_HOME" --port 8917
```

The HTML export is a self-contained site snapshot with the stylesheet inlined and scripts removed. The WordPress export is WXR 1.2. The Frappe export contains Blog Post-shaped documents. The latter two export the site's posts, not every customer setting, offer or page.

## Import into independent local identities

```sh Terminal
bonfyre site import "$SITE_HOME/fernhollow.xml" \
  --as fernhollow-wp-roundtrip --home "$SITE_HOME" --port 8917 --no-open

bonfyre site import "$SITE_HOME/fernhollow-frappe.json" \
  --as fernhollow-frappe-roundtrip --home "$SITE_HOME" --port 8917 --no-open
```

The new namespaces each returned a served site with one imported post. Their title matched the source post. The body matched after trimming trailing whitespace; both imports added three trailing newline characters. Those exports carry the content, not Fernhollow's workshop offer, original domain or site-specific theme.

This also gives agencies a useful handoff: content can be moved independently of a customer's commercial obligations.

## Move native records with exact readback

For data migration, Bonfÿre Bridge compares the actual values from the source and destination rather than treating a generated export file as an imported application.

```sh Move
bonfyre bridge db \
  --from "sqlite:$SITE_HOME/cms.db" \
  --to "sqlite:$SITE_HOME/bridge-readback.sqlite" \
  --tables site_presentation,community_post
```

```sh Verify
bonfyre bridge db-verify \
  --from "sqlite:$SITE_HOME/cms.db" \
  --to "sqlite:$SITE_HOME/bridge-readback.sqlite" \
  --tables site_presentation,community_post
```

## Read the results

The native bridge moved the sample's actual CMS records into a separate local SQLite database and compared every value after arrival.

| Table | Source rows | Destination rows | Value readback |
|---|---:|---:|---|
| `site_presentation` | 5 | 5 | Matched |
| `community_post` | 5 | 5 | Matched |
| **Total** | **10** | **10** | **2 tables matched** |

The subsequent read-only `db-verify` repeated the same result: both tables arrived, ten records read back, no row or value differences. The source was opened read-only.

Bridge intentionally does not carry database constraints, indexes or stored procedures. Migrations that require them need a schema and application-level pass before cutover. The external content export does not replace this database readback.

## Work across other systems

The same Bridge command supports MySQL, PostgreSQL and SQLite URLs. Connection coordinates may be supplied as `env:VARIABLE` so passwords remain outside CLI arguments.

```sh MySQL → Postgres
bonfyre bridge db --from env:SOURCE_DB --to env:TARGET_DB \
  --tables customers,invoices,payments

bonfyre bridge db-verify --from env:SOURCE_DB --to env:TARGET_DB \
  --tables customers,invoices,payments
```

When migrating to WordPress or Frappe, connect to the destination system's own importer or API and read the imported objects back there. A document generated in its format does not by itself constitute a live database change.

## Continue the implementation

Move an independently branded site using [white-label delivery](/white-label-delivery). Follow [one application, several interfaces](/interfaces-and-mcp) to inspect the same content through WebAddress and MCP, or [request to books](/request-to-books) to extend the customer's operation beyond website content.
