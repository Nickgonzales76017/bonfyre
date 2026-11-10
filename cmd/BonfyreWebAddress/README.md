# BonfyreWebAddress

The public web estate as a native origin: subjects, addresses, territories,
carriers, and machine-readable projections, served with bounded requests
and verified CMS reads.

## Build, test, install

```sh
make -C cmd/BonfyreWebAddress
make -C cmd/BonfyreWebAddress test
make -C cmd/BonfyreWebAddress install MANAGED_PREFIX=~/.bonfyre
```

The suite is dozens of witness targets over routes, projections, carriers,
and pages. Install places the binary; it serves site roots from the tree.

Known failures (3, community/profile territory): `community_journey` profile
link counts, retained-publication binding, and the CC0 gallery flow fail on
the merged line while passing on converge-20261105. Site content is identical
on both sides and both binaries are fresh builds, so the cause sits in
auto-merged WebAddress sources or fixture reads shared with the CMS binary —
bisect there, not in content.

## Use

```sh
bonfyre-web-address resolve --db FILE --host HOST --path PATH
bonfyre-web-address closure --db FILE --host HOST [--format ro-crate|zarr]
bonfyre-web-address public-projection-lower --cache-db FILE --handoff FILE
```

See a page change in seconds without touching the live service with
`make -C cmd/BonfyreWebAddress preview` (serves on `:28911` against
copies of the stores).

## Contract and limits

- Native HTTP requests stay bounded to 256 KiB; decoded CMS fields to
  64 KiB.
- The command existing never authorizes a read or a mutation; scope
  still applies.
- Native builds and deployed consumer paths carry separate witnesses.

No version is pinned and none is published as a package yet. The live
contract is `cmd-web-address` in the Architecture Atlas.
