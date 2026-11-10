# Composition circuits

Endpoint tests pass while the product is broken. These run whole workflows
across several protocols at once, because that is where this design either holds
together or does not.

Run read-only circuits against a host you explicitly choose:

    bun tests/circuits/fleming_close_circuit.mjs --base https://example.test
    bun tests/circuits/cross_protocol_circuit.mjs --base https://example.test --parcel 04111877
    bun tests/circuits/interaction_stack_circuit.mjs --base https://example.test --parcel 00226440

The write circuit requires an isolated estate, explicit `--apply`, a
write-scoped `BONFYRE_CIRCUIT_TOKEN`, and `--allow-untargeted-claim` because the
current `claim-next` operation can lease unrelated work. The interaction stack
uses the same token and `--apply` before its create assertions; without them it
reports those assertions as skipped. Cross-protocol WebSub POST checks require
`--apply`, `BONFYRE_CIRCUIT_WEBSUB_TOKEN`, and
`BONFYRE_CIRCUIT_WEBSUB_SECRET`. No script embeds login credentials, chooses
production implicitly, or treats a skipped effect as a passing assertion.

What each one holds to account:

- **fleming_close_circuit** — an outside agent discovers this world (A2A), reads
  the priced catalogue (x402), compares it to a quote (MPP), is refused without
  payment, falls back to the open projections, and checks that RESO and OGC
  describe *the same parcels*.
- **marketplace_circuit** — an operator lists a capability, which must change the
  x402 price, the quote and the 402 behaviour together; work is opened from a
  parcel found through the projections, claimed under lease, completed only by
  the lease holder, and the membrane books must agree afterwards.
- **cross_protocol_circuit** — one price through five commerce grammars, every
  protocol document still spec-shaped, deep paging over 6.5M receipted rows, and
  an oversized page refusing rather than truncating.

- **interaction_stack_circuit** — Source E's laws, exercised as a person meets
  them: a subject answers with its own operations; the aperture differs by
  viewer and stays within 2–5 actions; relations are directions that carry real
  counts, with the ones this source cannot support named rather than hidden; a
  lens varies selection and representation independently; a room is shaped by
  its subject and says what it cannot supply; and creation is read into a typed
  proposal before anything exists.

Defects these found that endpoint tests did not: every projection silently
ignored `limit` and returned 10 rows; a 1000-row page truncated mid-JSON and was
served as 200; and a process-global HTML flag leaked across requests, so a
browser visit made the next machine request receive HTML instead of JSON.
