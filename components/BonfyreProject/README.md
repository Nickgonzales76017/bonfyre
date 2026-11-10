# BonfyreProject

Thin compiled fusion binary for:
- `BonfyreCMS`
- `BonfyreIndex`
- `BonfyreStitch`
- rights-limited `ForeignTwin` observations

## Purpose
- give the content graph projection engine one front door

## Build
```bash
make
```

## Usage
```bash
./bonfyre-project cms schema migrate --db /tmp/test.db --schemas ../BonfyreCMS/content-types
./bonfyre-project index build artifacts --db /tmp/index.db
./bonfyre-project refresh artifacts --db /tmp/index.db
./bonfyre-project stitch plan artifacts/family/artifact.json --target deliverable-md
./bonfyre-project foreign observe apartments-dallas https://www.apartments.com/dallas-tx/ --receipt /path/to/source-receipts.jsonl
./bonfyre-project foreign observe cl-dfw-rooms https://www.craigslist.org/search/area/dallas?cat=roo \
  --receipt /path/to/source-receipts.jsonl --body /path/to/bodies/cl-dfw-rooms.html
./bonfyre-project foreign twins
./bonfyre-project foreign twins --receipts   # forensic receipt history; not a surface feed
./bonfyre-project message review --db /path/to/cms.db --ns sales
./bonfyre-project message review --db /path/to/cms.db --ns sales --format html --out /path/to/review.html
```

`message review` reads the existing project and outbound_message records in one
namespace and one SQLite snapshot. It brings the exact draft, recipient, price
options, delivery approach and review notes together. Active membership requires
a project at proposal_review and a draft message that is composed or withheld;
other records remain visible outside that batch. This classification does not
check whether a source is still open or approve an offer.

The JSON and HTML exports carry the same content fingerprint. Changing a draft,
recipient, scope or saved state changes it. The HTML report runs no scripts and
loads no remote resources. `--out` replaces its destination only after a complete
report is available and returns an export receipt. Reports over 1,000 messages or
8 MiB are refused. The command opens the CMS read-only and sends nothing.

`foreign twins` is the compact release catalogue: one row per published source
contract with separate SourceFrontier admission, ItemArchive material,
spatial/media/rights/publisher extraction declarations, readiness paths, and
program-input standing. A readiness path says only that exact independently
owned inputs exist for a later compiler; it is not a derived spatial/media/
rights fact. Program input is resolved only through the authored
`SiteProgram ← SourceCollection ← source_refs` bridge and remains
`delivery_state: not_verified` until a delivery owner proves a route. It reads
the owner tables in bounded scans, rather than enumerating the receipt ledger.
If `BONFYRE_PRICES_DB` names an observed-price ledger, the response also reports
exact source-contract alignment; an unmatched price source stays unbound and
never appears as an offer, current price, payment authority, or settlement.
For bounded carriers, `foreign twins --page OFFSET --limit N` emits a rich
slice with `offset`, `limit`, `total`, and `next_offset`; its
`release_summary` remains a whole-estate result rather than a page-local count.
`--query TEXT` is a literal case-insensitive filter over authored source
contract fields (URL, source identity, family, carries, declared publisher).
It reports the narrowed `total` beside `estate_total`; it is not a semantic
ranking, source classification, or alternate estate census.

`foreign observe` preflights `robots.txt` with the `BonfyreProject/1.0` user
agent before fetching a source. A disallowed URL (or `--observe-right denied`)
records `observation_kind: "policy_excluded"` and `fetch_attempted: false`; it
does not fetch or retain the source body. This is distinct from a
`boundary_response`, where a permitted source fetch was attempted but failed or
returned a non-2xx status. A 404 `robots.txt` permits the source fetch; an
unavailable robots preflight records `policy_indeterminate` and fails closed.

Use `--observe-right public` (the default for authorized public reads) or
`--observe-right denied` to make the local rights decision durable. `--robots URL`
is available for a source-specific preflight endpoint and deterministic
tests. `foreign observe` never claims availability from a readable page (or a
denial); listing and availability verification remain separate downstream work.

`--body FILE` retains the observed body at `FILE` and records `body_path` in the
receipt, so a downstream reader can bind to the exact bytes the receipt hashed.
Without `--body` the body is fetched to a scratch file and discarded. A retained
body is still an observation: it is parsed only while it re-hashes to its
receipt, and it never becomes an availability claim.
