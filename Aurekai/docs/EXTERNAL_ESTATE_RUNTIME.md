# External estate runtime

`/external-estate/` is the usable public surface for the supplied 4,891
external source occurrences. It is deliberately a **candidate** surface: each
entry retains its raw supplied line, source document and source reference, then
offers cross-domain composition candidates for Worlds such as a farm cold chain,
water district, elder-care network, port township, school, film production and
household.

The generated corpus lives at:

`estate/generated/external-estate-v4/observed-external-estate.jsonl`

It is rebuilt from the supplied master documents by:

```sh
python3 architecture/compile_external_estate.py
```

The Python program is a reproducible source compiler only. It is not a provider
runtime or institutional owner. The live edge is Node because it is a bounded
HTTP/query carrier; it owns no durable external truth.

## Product routes

The public estate is not one catalogue screen. The current measured corpus has
30 declared Worlds and all 15 compiled interaction grammars have material in
each, yielding 450 focused public lens routes. They are generated from the
same World/lens closure compiler, rather than maintained as route-specific
applications:

```text
/external-estate/worlds
/external-estate/v1/worlds
/external-estate/worlds/{world}
/external-estate/worlds/{world}/{grammar}
/external-estate/v1/worlds/{world}/closure
/external-estate/v1/worlds/{world}/{grammar}/closure
/external-estate/v1/worlds/{world}/formation
/external-estate/v1/worlds/{world}/bridges
/external-estate/v1/formations
```

When `BONFYRE_ADDRESS_INDEX` and `BONFYRE_ADDRESSPLANE_BINARY` are configured,
each public World closure also carries a read-only native AddressPlane witness
for `world:{resolved-world}`. It reports only deterministic mask survival
(`eligible_for_further_consideration`) and never substitutes for an exact
owner, rights, evidence, provider, or effect decision. Set
`BONFYRE_WORLD_REQUIRE_NATIVE_FORWARDING=1` in production to fail a World
route closed when that witness is unavailable or mismatched.

The first pair is server-rendered for people; the second pair is the matching
machine carrier. Every closure has a distinct digest and cache validator.

Available manual-test routes:

- `GET /external-estate/` — human search/browse surface.
- `GET /external-estate/v1/manifest` — counts and source digests.
- `GET /external-estate/v1/occurrences?domain=weather&world=farm-cold-chain-household`
  — machine query.
- `GET /external-estate/v1/occurrences/EXT-00001` — exact occurrence, raw source
  line, candidate projection and limits.
- `GET /external-estate/occurrences/EXT-00001` — human inspection page for one
- `POST /external-estate/occurrences/EXT-00001/probe` — an explicitly requested
  human no-body transport check; it never follows redirects or creates provider,
  evidence, authority, payment, or effect state.
  supplied occurrence, including its candidate Worlds and an explicit probe form.
- `GET /external-estate/v1/worlds/port-township` — a cross-domain composition.
- `GET /external-estate/worlds/farm-cold-chain-household` — a server-rendered
  public World with bounded interaction lenses over the real candidate
  reservoir.
- `GET /external-estate/v1/worlds/farm-cold-chain-household/closure` — the
  machine carrier for that exact World closure. Its `closure_digest` is also
  embedded in the HTML page, so a human and machine do not receive separately
  assembled candidate sets.
- `GET /external-estate/v1/worlds/farm-cold-chain-household/formation` — a
  content-addressed formation projection over that same observed World
  reservoir. It names the exact observed material, observation, comparison,
  schedule, communication, market, identity and place signals that make a
  living publication, decision service, coordination space, market, place
  service or small-institution worth *designing*. It also names missing
  signals. It creates none of those things: no project, public page, member
  list, budget, governance process, provider relationship, payment, or effect
  is implied by a complete candidate.
- `GET /external-estate/v1/formations` — the whole-estate, content-addressed
  census of all declared Worlds and the formation kinds their observed signal
  coverage can support for human design. It deliberately does not rank Worlds
  by worth, need, demand or priority.
- `GET /external-estate/v1/worlds/school-township/bridges` — exact overlap of
  observed source-candidate membership with other declared Worlds. It is a
  cross-density index for possible future composition, never a claim that the
  Worlds have a relationship, shared participants, proximity, consent, or a
  right to combine their data.
- `GET /external-estate/worlds/farm-cold-chain-household/listings` — one
  focused interaction lens of that World. Every declared grammar with material
  becomes a first-class product route rather than a hidden filter.
- `GET /external-estate/v1/worlds/farm-cold-chain-household/listings/closure`
  — the matching focused machine closure. A lens has its own digest because it
  is a smaller addressed projection, never an HTML-only tab.
- `POST /external-estate/v1/occurrences/EXT-00001/probe` — one explicitly
  requested HTTP(S) `HEAD`; redirects are not followed and no body is stored.

The CLI uses the same runtime:

```sh
node Aurekai/bin/akai.mjs external-estate:list --world farm-cold-chain-household --json
node Aurekai/bin/akai.mjs external-estate:inspect --id EXT-00001 --json
node Aurekai/bin/akai.mjs external-estate:probe --id EXT-00001 --json
```

No route establishes a provider relationship, verifies a vendor claim, admits a
capability, combines personal data, authorizes a payment, settles a transaction,
or performs an external effect. A positive transport probe is only a timestamped
transport observation candidate. The next lawful step is always the existing
CapabilityClosure, AuthorityGraph, InformationPolicy, EvidenceGraph,
ProtocolContract and native effect-gate path.

## From source density to candidate worlds

The external estate is not a feed monitor. A source can contribute material,
identity, spatial context, temporal coordination, comparison evidence, market
affordances, or communication capacity to more than one addressed World. The
World compiler preserves those contributions separately and can expose their
joint coverage as a formation candidate:

```text
observed external occurrences
        -> addressed World reservoir
        -> counted heterogeneous signals
        -> formation candidate for human design
        -> existing owner / policy / authority / evidence gates
        -> only then a possible project, service, market, publication or institution
```

For example, a school World with public schedules, map/place material, public
communications, comparison/observation sources and identity references may be
legible as a family-school coordination service. That does **not** establish a
school relationship, authority over student data, an approved participant,
eligibility, a shared need, or permission to operate. The formation carrier is
therefore useful as a design and operator aperture, while refusing to turn
internet adjacency into an institutional fact.

Formation recipes also name only **possible operator archetypes** and project
facets from the existing native ProjectPrimitive grammar. This lets a school,
market, neighborhood, or household formation arrive with a legible social
shape—who might recognise the work and whether it needs recruiting, governance,
a public ledger, recurring support, a campaign, or bounties—without inventing a
person, role assignment, engagement, funding commitment, or authority. The
runtime test checks those archetype IDs against the generated native operator
declaration, so a UI-friendly formation recipe cannot quietly diverge from the
institutional grammar.
