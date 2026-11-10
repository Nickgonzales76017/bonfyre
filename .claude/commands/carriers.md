---
description: Verify every carrier protocol answers and agrees on price
allowed-tools: Bash(curl:*), Bash(bun:*)
argument-hint: "[parcel subject, required for circuit]"
---

Check all eighteen carriers over the same state, and assert they cannot disagree.

The circuit checks every priced capability and needs a parcel subject for
cross-carrier identity assertions. Use the parcel supplied in `$ARGUMENTS`;
if none was supplied, ask for one before running the circuit.

!`for c in capability mpp/quote .well-known/agent.json events ogc reso/Property mismo/subject-property ap/actor beckn/search ucp/catalog acp/products wot/thing cap/alerts epcis/events openrtb/bidrequest; do printf "%-28s %s\n" "$c" "$(curl -sS -m 12 -o /dev/null -w '%{http_code}' https://aurekai.life/$c)"; done`

Then run the circuit that proves price agreement per capability:

From the project root run `bun tests/circuits/cross_protocol_circuit.mjs
--parcel <subject>` and preserve its actual exit status and full failure count.

Report any carrier that did not answer 200, and any price divergence. An
empty-by-design carrier answering with its reason is a pass, not a gap.
