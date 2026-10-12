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
```

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
