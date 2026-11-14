CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-finance
SRC = src/main.c
DEPS = src/books.inc src/opportunity_inspect.inc src/relationships.inc

all: $(TARGET)

$(TARGET): $(SRC) $(DEPS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Isolated contract tests only; not external customer evidence.
smoke: $(TARGET)
	@bash ../../scripts/verification/test-gate.sh run --suite finance_vectors --suite finance_unit --fast

test: $(TARGET)
	@bash ../../scripts/verification/test-gate.sh run --suite finance_vectors --suite finance_unit --suite finance_browser
	@$(MAKE) -s opportunity-inspect-mutation-witness

# The native contract must separate the real owner binary from a broken one.
.PHONY: opportunity-inspect-mutation-witness
opportunity-inspect-mutation-witness:
	@log=$(mktemp); trap 'rm -f "$log"' EXIT; \
	if BONFYRE_FINANCE_BIN=/usr/bin/false ../../tests/vectors/bonfyre-vectors --root ../.. tests/vectors/opportunity_inspect.json >"$log" 2>&1; then \
	  echo 'REFUSE: Finance command mutation escaped vector checks' >&2; exit 1; \
	fi; \
	if ! grep -q '^  FAIL' "$log"; then cat "$log" >&2; exit 1; fi; \
	echo 'PASS Finance mutation refusal'

# Broader Feed -> installed apps -> Source Library ownership belongs to
# account-work integration. Never run the whole unrelated surface when
# verifying Finance changes; release acceptance still executes that suite.
.PHONY: test-integration
test-integration: test
	$(HOME)/.bun/bin/bun test --reporter=dots --timeout 90000 ../BonfyreWebAddress/tests/installed_subtools_test.mjs

# A 100,000-opportunity scale witness is explicit, not on routine test paths.
.PHONY: test-scale
test-scale: $(TARGET)
	$(HOME)/.bun/bin/bun test tests/zero_close_scale.test.mjs

install-bin: $(TARGET)
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(TARGET) $(HOME)/.bonfyre/bin/$(TARGET)
	@if [ "$$(uname -s)" = Darwin ]; then codesign --force --sign - $(HOME)/.bonfyre/bin/$(TARGET); fi

clean:
	rm -f $(TARGET)

.PHONY: all smoke test test-integration test-scale install-bin clean
