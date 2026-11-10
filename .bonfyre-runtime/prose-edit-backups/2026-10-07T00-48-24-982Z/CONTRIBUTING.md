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

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
