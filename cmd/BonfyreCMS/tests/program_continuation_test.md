The native fixture creates a disposable CMS database through the current CMS
binary's schema and entry commands. It then plans, installs and activates the
pack and writes a record through `pack-workspace`. SQL only inspects the admitted
binding, removes the unrelated `_hooks` table to expose unwanted schema repair,
and holds the writer reservation.

From the repository root:

```sh
rtk make -C cmd/BonfyreCMS -f tests/program_continuation_test.mk test
rtk make -C cmd/BonfyreCMS -f tests/program_continuation_test.mk sanitizer-test
```

For release validation, select the frozen native CMS reader:

```sh
rtk env BONFYRE_TEST_PROGRAM_CMS_BINARY=/absolute/release/native/cmd/BonfyreCMS/bonfyre-cms BONFYRE_TEST_PACK_ESTATE_BINARY=/absolute/release/native/cmd/BonfyreExternalEstate/bonfyre-external-estate make -C cmd/BonfyreCMS -f tests/program_continuation_test.mk test
```

Fixture writes still use `cmd/BonfyreCMS/bonfyre-cms` from the checkout. Every
continuation invocation uses the selected reader, including the refusal checks.
The release's `bin/bonfyre-cms` may point to an older installed binary; use its
frozen `native/cmd/BonfyreCMS/bonfyre-cms` path.

The six checks are:

1. The actual continuation reader completes within 2.5 seconds while the fixture
   connection holds `BEGIN IMMEDIATE`, returning the admitted workspace context.
2. The record key, revision and MessagePack body match the saved record exactly.
3. The single compatible workspace context references the same account and
   matches the source context byte for byte.
4. The schema snapshot and SHA-256 hashes of the database and WAL remain unchanged,
   and the reader does not recreate `_hooks`.
5. A different account receives exit code 4 and no response bytes within 2.5 seconds.
6. A missing database is refused within 2.5 seconds without creating the file.

The continuation checks print `PROGRAM_CONTINUATION_PASS` followed by a tab and
`6`. A second pack goes through its own actual plan, install and activate
lifecycle, then receives a separate public activation through CMS. The native
ExternalEstate `emit-cms` command must satisfy five checks:

1. Forward the explicit `reusable-source` public activation.
2. Exclude the private `user:101` account from emitted JSONL.
3. Exclude private `pack-lock:` coordinates from emitted JSONL.
4. Preserve both published activation records for this pack in CMS.
5. Leave the private account's native `current` resolution active.

Every emitted JSONL row is parsed before these assertions. This second group
prints `PACK_LOCK_ADVERTISEMENT_PASS`, a tab and `5`, followed by its summary.
The activation count is scoped to the second pack because the continuation pack
also remains in this shared disposable database. No admission records are
inserted directly with SQL.

`BONFYRE_TEST_PACK_ESTATE_BINARY` selects the frozen emitter; otherwise the fixture
uses `cmd/BonfyreExternalEstate/bonfyre-external-estate` from the checkout. The
emitter is a native command and reads only this fixture's CMS database. Release
validation must retain both binary identities along with the current CMS fixture
writer, fixture source and Makefile, native library dependencies and copied
content-type schemas. The emitter implementation is in
`cmd/BonfyreExternalEstate/src/main.c`.

Captured
output above 1 MiB is refused, fixture commands have a 15-second deadline, and cleanup
stops an outstanding fixture child and removes only the fixture's temporary tree.
This tests native admission and continuation reads; it does not test browser
layout, provider execution or public release activation.
