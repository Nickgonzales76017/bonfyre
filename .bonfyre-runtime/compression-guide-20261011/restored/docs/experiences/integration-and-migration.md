# Integration and migration

Move websites, business records and databases without asking the client to start over. Bonfÿre connects the source system to a destination and makes the difference between a successful copy and a verified migration visible.

## Choose the movement

| Source | Destination | Native route | What to review |
|---|---|---|---|
| WordPress export | Bonfÿre site | Site import | Posts, authors, media, URLs and publication state |
| Bonfÿre site | WordPress export | Site export | Site material in the supported export format |
| Bonfÿre site | Frappe-shaped documents | Site export | Field mapping and document semantics |
| MySQL/MariaDB | PostgreSQL | Bridge | Every row/value, field types, constraints and procedures |
| Two existing databases | Read-only comparison | Bridge verify | Row counts and content digests |

These are distinct routes. A Frappe-shaped document export is not an API write to a running ERPNext installation. A relational bridge moves table data, not automatically every index, trigger or stored procedure.

## Prepare a local example

Start in a configured Bonfÿre checkout with Site, CMS and the intake commands available. The database example also needs `duckdb`, its SQLite/MySQL/PostgreSQL extensions, `sqlite3` and `jq`. Bridge installs and loads those extensions on invocation; an uncached extension needs network access. All example data below is invented and stays in a fresh directory.

```sh Terminal
make -C cmd/BonfyreWebAddress bonfyre-site
make -C cmd/BonfyreBridge
bonfyre_migration=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-migration-guide.XXXXXX")
bonfyre_port=8915
printf '%s\n' "$bonfyre_migration"
```

Keep the commands in the same shell. Choose another port if 8915 is already occupied.

## Import two published posts

Create a small WordPress export with two published posts and a draft:

```sh Terminal
cat > "$bonfyre_migration/juniper.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:wp="http://wordpress.org/export/1.2/">
<channel><title>Juniper Lane Bakery</title><wp:wxr_version>1.2</wp:wxr_version>
<item><title>Saturday bread</title><content:encoded><![CDATA[<p>Sourdough is ready at nine. Collect your loaf before noon.</p>]]></content:encoded><wp:post_date_gmt>2026-10-10 09:00:00</wp:post_date_gmt><wp:status>publish</wp:status><wp:post_type>post</wp:post_type></item>
<item><title>Custom cakes</title><content:encoded><![CDATA[<p>Order a birthday cake three days ahead. Tell us the size and collection date.</p>]]></content:encoded><wp:post_date_gmt>2026-10-09 10:00:00</wp:post_date_gmt><wp:status>publish</wp:status><wp:post_type>post</wp:post_type></item>
<item><title>Unfinished holiday menu</title><content:encoded><![CDATA[<p>This draft is not published.</p>]]></content:encoded><wp:status>draft</wp:status><wp:post_type>post</wp:post_type></item>
</channel></rss>
XML
bonfyre site import "$bonfyre_migration/juniper.xml" --as juniper --no-open \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
bonfyre site set juniper --title 'Juniper Lane Bakery' \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
bonfyre site check juniper --json \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
```

Import should report two posts and a served page. The draft stays out. Open `http://juniper.localhost:8915/` to read the local site, using your selected port.

This route imports published post and page text. Plan separately for media files, original authors, categories, comments, internal links and redirects when moving a customer's actual site.

## Export for another system

```sh Terminal
bonfyre site export juniper --out "$bonfyre_migration/juniper.html" --format html \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
bonfyre site export juniper --out "$bonfyre_migration/juniper-frappe.json" --format frappe \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
bonfyre site export juniper --out "$bonfyre_migration/juniper-out.xml" --format wordpress \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
jq -e 'length == 2 and all(.[]; .doctype == "Blog Post" and .published == 1)' \
  "$bonfyre_migration/juniper-frappe.json"
```

The HTML export is a self-contained page. The other exports should each report two posts. Frappe's `published_on` field retains the calendar date; the export does not preserve the original time of day in that field.

## Read both formats back

Import each exported format into a separate local site, then compare their titles and short content through MCP:

```sh Terminal
bonfyre site import "$bonfyre_migration/juniper-frappe.json" --as juniper-frappe --no-open \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
bonfyre site import "$bonfyre_migration/juniper-out.xml" --as juniper-wp --no-open \
  --home "$bonfyre_migration/sites" --port "$bonfyre_port"
for bonfyre_site in juniper juniper-frappe juniper-wp; do
  curl --fail --silent --show-error \
    "http://127.0.0.1:$bonfyre_port/mcp/$bonfyre_site" \
    -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_posts","arguments":{}}}' \
    > "$bonfyre_migration/$bonfyre_site-posts.json"
  jq -e '.result.structuredContent.total == 2' \
    "$bonfyre_migration/$bonfyre_site-posts.json"
  jq -S '.result.structuredContent.posts | map({title, excerpt}) | sort_by(.title)' \
    "$bonfyre_migration/$bonfyre_site-posts.json" \
    > "$bonfyre_migration/$bonfyre_site-text.json"
done
cmp "$bonfyre_migration/juniper-text.json" "$bonfyre_migration/juniper-frappe-text.json"
cmp "$bonfyre_migration/juniper-text.json" "$bonfyre_migration/juniper-wp-text.json"
cat "$bonfyre_migration/juniper-text.json"
```

Both comparisons should finish without output. The final result should contain `Custom cakes` and `Saturday bread`. These posts are short enough for the returned excerpts to hold their whole text; use full-post retrieval for longer material.

This checks Bonfÿre's readers and writers for the two formats. Before handing a file to a customer, test its import in the intended WordPress or Frappe installation, including the required author, category and permission records.

## Move relational records with readback

```sh Terminal
sqlite3 "$bonfyre_migration/source.db" <<'SQL'
CREATE TABLE orders(id INTEGER, customer TEXT, amount_cents INTEGER, note TEXT);
INSERT INTO orders VALUES(1,'Juniper Lane Bakery',8500,NULL),(2,'Fernhollow Pottery',12500,'');
CREATE TABLE order_lines(order_id INTEGER, item TEXT, quantity INTEGER);
INSERT INTO order_lines VALUES(1,'Bread box',1),(1,'Bread box',1),(2,'Clay kit',2);
SQL
export BONFYRE_GUIDE_SOURCE="sqlite:$bonfyre_migration/source.db"
export BONFYRE_GUIDE_TARGET="sqlite:$bonfyre_migration/target.db"
bonfyre bridge db --from env:BONFYRE_GUIDE_SOURCE --to env:BONFYRE_GUIDE_TARGET \
  --tables orders,order_lines > "$bonfyre_migration/move.json"
jq -e '.verified and .arrived == 2 and .rows_arrived == 5' "$bonfyre_migration/move.json"
bonfyre bridge db-verify --from env:BONFYRE_GUIDE_SOURCE --to env:BONFYRE_GUIDE_TARGET \
  --tables orders,order_lines > "$bonfyre_migration/verify.json"
jq -e '.verified' "$bonfyre_migration/verify.json"
```

The receipt should show two arrived tables and five rows. The example includes duplicate rows, an empty string and a null value. The bridge compares row counts and aggregate hashes of column values after text conversion. It reports type changes separately; it does not carry indexes, constraints or stored procedures.

For an existing MySQL or PostgreSQL system, put its connection URL in the corresponding environment variable. Supported forms are `mysql://user:password@host:port/database` and `postgres://user:password@host:port/database`. Choose an empty destination or review its existing tables before a move. `--replace` explicitly drops and recreates selected destination tables.

## Detect a changed value

Change one amount in the example destination, then verify again:

```sh Terminal
sqlite3 "$bonfyre_migration/target.db" 'UPDATE orders SET amount_cents=8501 WHERE id=1;'
if bonfyre bridge db-verify --from env:BONFYRE_GUIDE_SOURCE --to env:BONFYRE_GUIDE_TARGET \
  --tables orders,order_lines > "$bonfyre_migration/changed.json"; then
  printf '%s\n' 'Unexpected match after changing an amount.' >&2
  exit 1
fi
jq -e '.verified == false and any(.tables[]; .table == "orders" and .state == "values_differ")' \
  "$bonfyre_migration/changed.json"
sqlite3 "$bonfyre_migration/target.db" 'UPDATE orders SET amount_cents=8500 WHERE id=1;'
bonfyre bridge db-verify --from env:BONFYRE_GUIDE_SOURCE --to env:BONFYRE_GUIDE_TARGET \
  --tables orders,order_lines > "$bonfyre_migration/restored.json"
jq -e '.verified' "$bonfyre_migration/restored.json"
bonfyre site stop --home "$bonfyre_migration/sites" --port "$bonfyre_port"
```

The changed receipt should name `orders` as `values_differ` and leave `order_lines` as `arrived`. Restoring the original value makes verification pass again. Both endpoints are attached read-only during verification, and a missing destination is refused without creating it.

The receipt compares the data observed during that invocation. Coordinate writes or take suitable snapshots when checking a live system; the two endpoints do not share one transaction. Hash agreement is a comparison check, not a collision-free proof. DuckDB's hash can also change between engine versions, so rerun both sides together rather than comparing hashes from unrelated releases. See [DuckDB's hash reference](https://duckdb.org/docs/current/sql/functions/utility#hashvalue) and [read-only attachments](https://duckdb.org/docs/current/sql/statements/attach#options).

## Read the result in three useful forms

**For the customer:** a short migration summary listing moved content, anything that needs their attention, and which URLs or business workflows remain unchanged.

**For the operator:** a SourceDiff table with source and destination, row count, field mapping, differences, refused operations, execution time and readback reference.

**For a developer:** the exact source and target schemas, native command or API request, typed conversion rules, before/after digests and re-run instructions.

All three describe the same move. The result view is a projection of the bridge and content owners, not another data reconciliation database.

## Common acceptance checks

- Source and destination counts agree for every selected table.
- The digest of every value agrees after the intended conversion.
- Nulls, numeric precision, timezones, booleans, encodings and large values survive the mapping.
- Customer-owned relationships, IDs and authorization remain attached to the right subject.
- Indexes, uniqueness, constraints, foreign keys and stored routines receive their own explicit validation.
- A second verification run leaves both databases unchanged.
- A failure preserves enough information to resume or roll back under the destination's own policy.

Do not claim “migration complete” solely because a process exited successfully. Use the readback and application-level checks appropriate to the source.

## Connect WordPress and ERPNext workflows

A company can keep WordPress as a public-facing CMS, Frappe/ERPNext as its internal records, and Bonfÿre as the implementation and composition substrate. Another company can choose a Bonfÿre native application for both. The integration layer is an option, not an architectural dependency.

For applications that need live bidirectional sync, name the external version, authority, API credentials, conflict rules and acknowledgment mechanism. A document export by itself does not establish synchronization.

## Extend into international operations

The same approach applies to supplier catalogs, manufacturing requests, customs documents and regional ERP systems. Partner-specific field maps and permitted exchanges stay separate from the reusable data movement and verification machinery.

Next: [white-label delivery](/white-label-delivery) for multiple independent customers, [request to books](/request-to-books) for financial workflow handoffs, and the native Bridge reference for exact supported database syntax.
