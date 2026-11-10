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

all: $(TARGET) smoke

$(TARGET): $(SRC) $(DEPS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Isolated contract tests only; not external customer evidence.
smoke: $(TARGET)
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreFinance/tests/vectors/*.json
	$(HOME)/.bun/bin/bun test tests/opportunity_inspect.test.mjs
	$(HOME)/.bun/bin/bun test tests/crm_finance_auth.test.mjs

test: $(TARGET)
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreFinance/tests/vectors/*.json
	$(HOME)/.bun/bin/bun test tests/zero_close_scale.test.mjs tests/opportunity_inspect.test.mjs
	$(HOME)/.bun/bin/bun test tests/crm_finance_auth.test.mjs
	$(HOME)/.bun/bin/bun test --timeout 90000 tests/crm_finance_browser.test.mjs
	# The observed Feed -> installed apps -> CRM and Source Library side is a product dependency.
	$(HOME)/.bun/bin/bun test --timeout 90000 ../BonfyreWebAddress/tests/installed_subtools_test.mjs

install-bin: $(TARGET)
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(TARGET) $(HOME)/.bonfyre/bin/$(TARGET)
	@if [ "$$(uname -s)" = Darwin ]; then codesign --force --sign - $(HOME)/.bonfyre/bin/$(TARGET); fi

clean:
	rm -f $(TARGET)

.PHONY: all smoke test install-bin clean
