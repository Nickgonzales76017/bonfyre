# Product components and transitional cutover

This folder owns independently buildable Bonfÿre product components that do not
already have a more specific native owner. Contracts belong in `schemas/`,
architecture in `architecture/`, generated outputs in `generated/`, and
historical notebook material in `evidence/origins/`.

The Generation-10 cutover is still removing compatibility children from this
tree. Their current fates are measured rather than maintained as a hand-written
build list:
`generated/projections/estate/transitional-root-absorption.json`; placement
under this root does not promote their maturity.

Regenerate or verify that projection with:

```sh
python3 tools/transitional_root_absorption.py
python3 tools/transitional_root_absorption.py --verify
```

The fate names are operational:

- `absorb_required` and `retain_until_native_parity` stay until their capability
  fences close.
- `absorb_to_current_runtime` needs a named runtime owner before moving.
- `absorb_docs_and_fixtures` and `absorb_to_evidence` leave production paths
  once their non-executable material has an evidence owner.
- `superseded_by_cmd` is deleted under a DeletionProof; the `cmd/` owner remains.
- `repair_required` must register, vendor, or remove the gitlink under an
  identity-bound disposition proof rather than treating an empty checkout as
  disposable or substituting a similarly named remote HEAD.

## Rule

Product implementation lives here only when it is not a native kernel, shared
library, command surface, integration boundary, or service with a more specific
owner. Physical placement never grants semantic authority.
