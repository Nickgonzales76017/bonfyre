# BonfyreRun — Recipe Executor

CC ?= cc
SQLITE_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null || echo "-I/opt/homebrew/opt/sqlite/include")
SQLITE_LIBS   := $(shell pkg-config --libs   sqlite3 2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
CFLAGS ?= -O2 -Wall -Wextra -std=c11 $(SQLITE_CFLAGS) -I../../lib/libbonfyre/include
LDFLAGS := -L../../lib/libbonfyre -lbonfyre $(SQLITE_LIBS)

SRC := src/main.c
BUILD := build
BIN := bonfyre-run

.PHONY: all clean install test

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

install: $(BUILD)/$(BIN)
	mkdir -p ~/bin
	cp $(BUILD)/$(BIN) ~/bin/
	@echo "Installed to ~/bin/$(BIN)"

test: $(BUILD)/$(BIN)
	@echo "Testing BonfyreRun..."
	./$(BUILD)/$(BIN) A1 --input test.wav --out /tmp/bonfyre-test --db /tmp/test-recipes.db --dry-run
	@echo "✓ Dry-run test passed"
