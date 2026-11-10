# Contributing to Bonfÿre

Bonfÿre is the native execution and capability engine beneath Aurekai.
Contributions should make an existing capability more correct, reusable,
observable or easy to adopt—not add another isolated project implementation.

[Operating model](docs/aurekai-bonfyre-model.md) ·
[Architecture Atlas](architecture/README.md) · [Quickstart](QUICKSTART.md)

## Constitutional rule: where semantics may live

**Do not add Python source.** New Bonfÿre runtime, tooling, tests, and
conformance code belongs in C, Zig, C++, or Bun/TypeScript. CI rejects every
new .py file.

Existing Python is a shrinking compatibility boundary, not a place to extend
the product. A migration may alter an existing Python oracle only to compare it
with a native owner while that owner is being absorbed. It is never the
authoritative implementation of work lifecycle, occurrences, receipts, actor
and relationship semantics, capability closure, provider state, resource
admission, structural cooling, context-cut compilation, plane scheduling,
authority transitions, or projection and replay. Those are kernel concepts.

**A campaign is data, not a source tree.** Instantiating a workload should add a
directory under `packs/`, never a module. If a campaign needs new runtime code,
the kernel is missing a primitive — that is the bug to fix, not the campaign.
`run4/`, `run5/`, `run6/`, `capitalgym/` and similar are scopes; their state
belongs in packs and databases, not in a new implementation tree each time.

## The basics

- Fork, branch, PR. Keep PRs focused on one thing.
- All C code: C11 standard, `-Wall -Wextra`, no warnings.
- Follow the canonical native owner and its existing file layout; don't create a new binary for a workload already expressible as a pack/recipe.
- Run the relevant owner's tests and independent evidence checks. Do not claim the whole estate passed because one component compiled.

## What we want help with

### High priority
- **Language bindings** — independently scoped interoperability adapters for reusable native libraries (without growing the core Python runtime)
- **Package manager support** — Homebrew formula, Debian/Ubuntu packages, AUR, Nix
- **Docker image** — Multi-stage build, scratch-based, minimal
- **Bug reports** — Especially on Linux distros we haven't tested

### Always welcome
- Documentation improvements
- Performance optimizations (with benchmarks)
- New pipeline binaries (propose in an issue first)
- Test coverage
- CI/CD setup

### Before adding a native capability

1. Query `bun architecture/atlas.mjs search <concept>` and identify the current owner and source/witness contracts.
2. Check whether the need is a **workload instance** (put it in a pack) or a **missing reusable primitive**.
3. Implement the primitive in the owner, using native C and its actual YAFF/CUE and typed artifact boundaries; do not create parallel authority, work or evidence stores.
4. Add a test that exercises the expected result and a refusal/denial path. Keep the test data independent of private customer state.
5. Update the Atlas entry with exact paths, evidence and a truthful maturity level; update relevant docs without promising universal release.
6. Open a focused PR describing affected consumers, release risks, licensing and rollback/readback steps.

If an external adapter requires JSON or another wire format, confine that encoding to the declared compatibility boundary. Internal canonical semantics and artifacts should not be implemented as ad hoc JSON contracts.

## Code style

- C11 (`-std=c11`)
- 4-space indentation (no tabs)
- `snake_case` for functions and variables
- `UPPER_CASE` for constants and macros
- Keep binaries self-contained — minimize cross-binary dependencies
- SQLite for any persistent state
- Typed native contracts and retained artifacts (YAFF/CUE and owner-defined formats) for internal coordination; JSON only at explicit compatibility/API boundaries

## Building

```bash
make              # Build everything
make clean        # Clean all build artifacts
make test         # Run tests
make install      # Install to ~/.local/bin
```

## Reporting bugs

Open an issue with:
- Your OS and compiler version (`cc --version`)
- The binary and command that failed
- Expected vs actual output
- Minimal reproduction steps

## Writing

Use the shared [Humanizer skill](.agents/skills/humanizer/SKILL.md) when writing or editing prose for people: site copy, interface labels and messages, documentation, release notes, proposals, support replies, and agent updates. Read the source and identify the audience before editing. Use embedded mode for finished copy.

Lead with what the reader needs to know or do. Prefer specific verbs and familiar words. Keep technical names when the reader needs them, and explain an unfamiliar term where it first matters. Remove process narration, repeated caveats, generic praise and claims of importance that add no information. Let the purpose of the text determine its length and tone; sales copy, instructions and technical explanations should not all sound alike.

Keep facts, necessary qualifications, quantities, citations and supported commercial claims. Edit current authored prose at its source. Leave source quotations, archived conversations, receipts, historical evidence, formal contracts, identifiers, commands, URLs and stored state unchanged. Do not apply a word blacklist or rewrite source material to make it sound less like AI. A readability edit does not verify a technical claim or authorize publication.

For customer-facing commercial work, follow its separate approval, evidence, publication and outreach policies. Repository contribution rights do not grant permission to send messages, access customer information or perform financial effects.

## README acceptance contract

Every new or materially revised README must let an unfamiliar reader answer, without sales prose or internal shorthand:

1. **Business:** who uses it, the concrete outcome, and how it fits Aurekai's reusable-software investment model.
2. **Project:** what this repository actually owns, what it consumes or produces, and whether it is a public experience, active source, experimental component, scaffold or historical extraction.
3. **Technical:** the runnable entry point, dependencies, source/contract owner, and a reproducible test or witness.
4. **Maturity:** distinguish declared, implemented, measured, locally tested and publicly deployed. Quote the specific revision or environment whenever numbers are included.
5. **Limits:** do not turn a provider configuration into a signed partnership, a demo into a customer deployment, an agent proposal into an approved action, or a quote into revenue.

Retain domain-specific detail: Bonfÿre repositories should lead with technical ownership and verification; Aurekai products should lead with audience, workflow and observable outcome. Reference the [shared business/project/technical model](docs/aurekai-bonfyre-model.md). Do not paste a generic company pitch into every README.

## License

Review the repository [license](LICENSE) and any component-specific notices before contributing. This guide does not create a separate contributor license or override an existing license.
