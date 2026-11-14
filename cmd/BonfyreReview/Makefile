BUN ?= $(HOME)/.bun/bin/bun
.PHONY: all test
all: test
test:
	$(BUN) test ./tests/*.test.mjs
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/native-command-web.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) -std=c11 -D_DARWIN_C_SOURCE -Wall -Wextra -Werror -I../../lib/libbonfyre/include tests/native_command_web_test.c ../../lib/libbonfyre/bf_msgpack.c -o "$$witness"; "$$witness"
	$(BUN) test ../../scripts/release/tests/public_surface_rollout.test.mjs
