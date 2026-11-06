# ExperienceMap

Look up retained workflow definitions and the tools, schemas and references associated with them. These records describe possible interactions; they do not authorize execution or prove that a customer workflow is connected.

```sh
bonfyre-experience-map find training 10
bonfyre-experience-map resolve 'member training creator program request publication'
bonfyre-experience-map resolve training-creator-request 1
bonfyre-experience-map chain-def creator-publish
bonfyre-experience-map chain creator-publish 10
bonfyre-experience-map record-def cms_type:training-program-release
bonfyre-experience-map record-def architecture:training-creator-request
bonfyre-experience-map protocol-def websocket
```

`find` searches record text. `chain-def`, `protocol-def` and `home-def` require an exact, case-sensitive ID and return its original JSON record. A word in another definition's stages cannot select that definition. Unknown IDs return exit code 1; malformed or ambiguous definition files return 2 with no selected record. `url-def` retains term search.

`record-def` retrieves a local or reference record by the ID shown in search results. Architecture records retain their source paths, witnesses, interaction references and limits. Use the existing atlas lookup for a referenced architecture's full contract.

`resolve` searches every exported architecture by words in its ID, question and domain. It returns up to five matching owners (set 1–10 with the second argument), their incoming and outgoing declared interactions, and the workflow definitions associated with those owners or their immediate neighbours. Exact architecture IDs rank first. The response retains each owner's complete exported contract and the atlas declaration digest. It reads the compiled `owner_graph.json` through an in-memory SQLite connection and writes no routing database.

This is candidate discovery. Scores are lexical matches; the one-hop expansion is not a complete execution plan. The response explicitly leaves current binaries, actor authority, evidence readback, resource admission, paid entitlement and ordered fact flow unresolved. No-match objectives return a structured `no_match` response and exit code 1; invalid input or damaged graph data returns 2 without a result.

An architecture can declare repeatable `experience_chain` IDs in its canonical YAFF entry. The compiler verifies the atlas export and joins those declarations to exact source paths. A schema's workflow does not transfer to other files in the same directory. Records without an explicit source mapping retain name-based classification, marked `name-hint-or-fallback`. Changing or removing a declaration changes the next compiled projection; it does not execute work or grant authority.

The executable reads its sibling `generated` directory or the installed `share/bonfyre/experience-map/current` corpus. `BONFYRE_EXPERIENCE_MAP_ROOT` explicitly selects another corpus; an invalid override fails without falling back. `stats` returns the corpus manifest and build digest.

Build and test from this directory:

```sh
make native-definition-witness
sh tests/native_corpus_resolution.test.sh
```

The native command uses system SQLite's JSON reader in memory. It writes no database. `make all` also recompiles and checks the broader reference corpus. The definition suite reads every compiled workflow, protocol and home record through the executable, then checks ambiguous IDs, escaped strings and damaged inputs.

After changing atlas declarations, run `bun architecture/atlas.mjs export` from the repository root before rebuilding this corpus. An outdated export or a declared workflow ID absent from `data/experience_chains.tsv` stops compilation.
