# Contributing to Bonfyre

Thanks for your interest. Here's how you can help.

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
