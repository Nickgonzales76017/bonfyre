# Native publication membership query

The donor renderer reads published, public, observed publication occurrences
whose exact source appears in a published collection for the requested domain.
Its previous correlated query reparsed collection selectors for each occurrence.
The new query materializes the domain's source set once and uses it for membership
checks. Publication standing, attribution joins, ordering and limits are unchanged.

The shared SQL is in `src/native_publications_query.h`. The native witness compares
complete ordered results against the retained reference query, including duplicate
collection entries and attribution joins, partial dates, missing sources, empty
and unknown domains, and numeric/null selector values. A larger empty-cut fixture
checks SQLite VM work instead of relying on a machine-dependent time threshold.

```sh
make -C cmd/BonfyreWebAddress -f tests/native_publications_query_test.mk test
make -C cmd/BonfyreWebAddress -f tests/native_publications_query_test.mk sanitizer-test
```

The fixture performs 6970019 VM steps with the reference query and 37259 with the
new query on this Mac. Both suites pass 585 checks. The query uses SQLite 3.35+
[`AS MATERIALIZED`](https://www.sqlite.org/lang_with.html#materialization_hints)
to keep the collection expansion outside the occurrence loop.

During the 2026-10-26 read-only production diagnosis, the restored service's two
workers each used about 95% CPU. A three-second sample of worker 19410 spent
1269 of 2512 samples in `native_publications`, predominantly SQLite JSON parsing.
Other samples showed item and route material reads. The sampled worker handled
several requests; this is evidence of competing work, not a measurement of one
LIFE home render.

The production CMS held 65140 publication occurrences and 59 published source
collections. EXPLAIN showed a correlated collection scan for each occurrence.
The old `discover` query did not finish within a three-second diagnostic cap;
the materialized query returned an empty result in 0.036 seconds. These reads
used a read-only SQLite connection and changed no indexes or data.

This addresses a measured source of worker load. It does not prove that LIFE
meets a latency budget under concurrent production traffic. A new release still
needs cold and warm readbacks and load-aware verification.
