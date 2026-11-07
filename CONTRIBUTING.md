# Contributing to Bonfÿre

Thanks for your interest. Here's how you can help.

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
- Each binary is one `src/main.c` + one `Makefile`. Keep it that way.
- Test your changes: `make clean && make && make test` in the binary's directory.

## What we want help with

### High priority
- **Language bindings** — Node.js, Python, Rust, Go wrappers for `liblambda-tensors`
- **Package manager support** — Homebrew formula, Debian/Ubuntu packages, AUR, Nix
- **Docker image** — Multi-stage build, scratch-based, minimal
- **Bug reports** — Especially on Linux distros we haven't tested

### Always welcome
- Documentation improvements
- Performance optimizations (with benchmarks)
- New pipeline binaries (propose in an issue first)
- Test coverage
- CI/CD setup

### How to add a new binary

1. Create `cmd/BonfyreYourThing/src/main.c`
2. Create `cmd/BonfyreYourThing/Makefile` (copy from any existing binary)
3. Follow the pattern: `status` command, `--help`, `--version`, JSON output
4. Add it to `install.sh`
5. Add it to the README table
6. Open a PR

## Code style

- C11 (`-std=c11`)
- 4-space indentation (no tabs)
- `snake_case` for functions and variables
- `UPPER_CASE` for constants and macros
- Keep binaries self-contained — minimize cross-binary dependencies
- SQLite for any persistent state
- JSON for inter-binary communication

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

For client tools and outreach, also use [client-sales](.agents/skills/client-sales/SKILL.md). Introductory phone and video calls are free; the current 15-draft batch awaits Nick's approval.

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
