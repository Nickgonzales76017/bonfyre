# BonfyreRun — Recipe Executor

CC ?= cc
SQLITE_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null || echo "-I/opt/homebrew/opt/sqlite/include")
SQLITE_LIBS   := $(shell pkg-config --libs   sqlite3 2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11 $(SQLITE_CFLAGS)
CFLAGS += -I../../lib/libbonfyre/include
CFLAGS += -I../BonfyreLex/src
LDFLAGS := -L../../lib/libbonfyre -lbonfyre $(SQLITE_LIBS)

SRC := src/main.c
BUILD := build
BIN := bonfyre-run
STAGE ?= $(BUILD)/staged

.PHONY: all clean install test test-staged-install

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

# Stage an executable for the managed release owner to attest and install.
# Never write to ~/bin or ~/.bonfyre/bin as a source-build side effect.
install: $(BUILD)/$(BIN)
	mkdir -p "$(STAGE)/bin"
	install -m 0755 $(BUILD)/$(BIN) "$(STAGE)/bin/$(BIN)"
	@cmp $(BUILD)/$(BIN) "$(STAGE)/bin/$(BIN)"
	@echo "Staged $(STAGE)/bin/$(BIN) for managed release admission"

test: $(BUILD)/$(BIN) test-staged-install
	@set -eu; \
	tmp=$$(mktemp -d /tmp/bonfyre-run-test.XXXXXX); \
	trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	sqlite3 "$$tmp/recipes.db" "CREATE TABLE recipes(recipe_id TEXT PRIMARY KEY, json_data TEXT NOT NULL); INSERT INTO recipes VALUES ('A1', '{\"name\":\"Fixture recipe\",\"version\":\"1\",\"stages\":[{\"id\":\"inspect\",\"name\":\"Inspect\",\"operator\":\"true\"}]}');"; \
	./$(BUILD)/$(BIN) A1 --input "$$tmp/input.fixture" --out "$$tmp/out" --db "$$tmp/recipes.db" --dry-run > "$$tmp/plan"; \
	grep -Fq "[DRY RUN] Execution plan:" "$$tmp/plan"; \
	grep -Fq "[inspect] Inspect (true)" "$$tmp/plan"; \
	test ! -e "$$tmp/out"; \
	if ./$(BUILD)/$(BIN) missing --input "$$tmp/input.fixture" --out "$$tmp/out" --db "$$tmp/recipes.db" --dry-run > "$$tmp/missing" 2>&1; then echo "FAIL: missing recipe was accepted" >&2; exit 1; fi; \
	grep -Fq "Recipe not found: missing" "$$tmp/missing"; \
	if ./$(BUILD)/$(BIN) missing --input "$$tmp/input.fixture" --out "$$tmp/out" --db "$$tmp/absent.db" --dry-run > "$$tmp/absent.log" 2>&1; then echo "FAIL: absent recipe database was accepted" >&2; exit 1; fi; \
	test ! -e "$$tmp/absent.db"; \
	echo "BonfyreRun: isolated native dry-run and missing-recipe refusal PASS"

# The packaging witness exercises exact source bytes in an isolated destination.
test-staged-install: $(BUILD)/$(BIN)
	@set -eu; \
	tmp=$$(mktemp -d /tmp/bonfyre-run-stage.XXXXXX); \
	trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	$(MAKE) --no-print-directory install STAGE="$$tmp" >/dev/null; \
	cmp $(BUILD)/$(BIN) "$$tmp/bin/$(BIN)"; \
	test -x "$$tmp/bin/$(BIN)"; \
	"$$tmp/bin/$(BIN)" --help >/dev/null; \
	echo "BonfyreRun: isolated staged-install parity PASS"
